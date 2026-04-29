"""
jira_client.py - Jira MCP 클라이언트

MCP 프록시(mcp.sginfra.net)를 통해 Jira에 접근합니다.
wiki_client.py, gdi_client.py 와 동일한 패턴으로, mcp_session.McpSession 을 공유합니다.

캐시 계층 (Phase 3):
  L1: 인메모리 dict (_JIRA_MEM_CACHE) — 5분 TTL
  L2: SQLite (mcp-cache-layer) — 이슈 10분, 프로젝트 1시간, 목록 24시간 TTL
  L3: MCP HTTP 호출 (폴백)

사용 가능한 Jira MCP 도구:
  jql_search, get_issue, get_all_projects, get_project,
  get_project_components, get_project_versions, get_all_project_issues,
  get_issue_transitions, get_issue_status, issue_get_comments, myself

환경변수:
  JIRA_MCP_URL  : MCP 서버 URL (기본: http://mcp.sginfra.net/confluence-jira-mcp)
  JIRA_USERNAME : Jira 사용자명
  JIRA_TOKEN    : Jira API 토큰
"""

import os
import re
import json
import logging
import time
import datetime as _dt
from dataclasses import dataclass, field, replace
from typing import Optional

from mcp_session import McpSession
from game_aliases import resolve_game, detect_game_in_text

logger = logging.getLogger(__name__)

# ── MCP 캐시 레이어 (옵셔널 — 임포트 실패 시 캐시 없이 동작) ──────────
_JIRA_CACHE_ENABLED = False
_jira_cache = None
_ops_log = None
_perf = None
_JIRA_ISSUE_TTL = 0.17      # 기본값 (~10분)
_JIRA_PROJECT_TTL = 1        # 기본값 (1시간)
_JIRA_PROJECTS_TTL = 24      # 기본값 (24시간)
_JIRA_MEM_TTL = 300           # 기본값 (5분)

try:
    import sys as _sys
    _cache_path = "D:/Vibe Dev/QA Ops/mcp-cache-layer"
    if _cache_path not in _sys.path:
        _sys.path.insert(0, _cache_path)
    from src.cache_manager import CacheManager as _CacheManager
    from src.cache_logger import ops_log as _ops_log_mod, perf as _perf_mod
    from src import config as _cache_config
    _jira_cache = _CacheManager()
    _ops_log = _ops_log_mod
    _perf = _perf_mod
    _JIRA_ISSUE_TTL = getattr(_cache_config, "JIRA_ISSUE_TTL_HOURS", 0.17)
    _JIRA_PROJECT_TTL = getattr(_cache_config, "JIRA_PROJECT_TTL_HOURS", 1)
    _JIRA_PROJECTS_TTL = getattr(_cache_config, "JIRA_PROJECTS_TTL_HOURS", 24)
    _JIRA_MEM_TTL = getattr(_cache_config, "JIRA_MEM_TTL_SEC", 300)
    _JIRA_CACHE_ENABLED = True
    logger.info("[jira] 캐시 레이어 로드 완료 (issue TTL=%.2fh, project TTL=%dh, "
                "projects TTL=%dh, mem TTL=%ds)",
                _JIRA_ISSUE_TTL, _JIRA_PROJECT_TTL, _JIRA_PROJECTS_TTL, _JIRA_MEM_TTL)
except Exception as _e:
    logger.info("[jira] 캐시 레이어 미사용: %s", _e)

# ── Jira 미러 fallback (옵셔널 — task-108) ─────────────────────────────────
_mirror_search = None    # scripts.jira_mirror.search_mirror
_mirror_age_fn = None    # scripts.jira_mirror.get_mirror_age_str
try:
    _mirror_cache_path = "D:/Vibe Dev/QA Ops/mcp-cache-layer"
    if _mirror_cache_path not in _sys.path:
        _sys.path.insert(0, _mirror_cache_path)
    from scripts.jira_mirror import (
        search_mirror as _mirror_search,
        get_mirror_age_str as _mirror_age_fn,
    )
    logger.info("[jira] 미러 fallback 로드 완료")
except Exception as _me:
    logger.info("[jira] 미러 fallback 미사용: %s", _me)

# ── L1 인메모리 캐시 ─────────────────────────────────────────────────────
_JIRA_MEM_CACHE: dict = {}  # {key: (data, timestamp)}


def _mem_get(key: str):
    """L1 메모리 캐시 조회. TTL 초과 시 None."""
    entry = _JIRA_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < _JIRA_MEM_TTL:
        return entry[0]
    return None


def _mem_set(key: str, data):
    """L1 메모리 캐시 저장."""
    _JIRA_MEM_CACHE[key] = (data, time.time())


def _jql_to_query_text(jql: str) -> str:
    """JQL에서 검색어 텍스트를 추출한다 (미러 fallback용)."""
    import re as _re
    m = _re.search(r'(?:text|summary)\s*~\s*["\'](.+?)["\']', jql, _re.IGNORECASE)
    if m:
        return m.group(1)
    return jql


def _extract_project_from_jql(jql: str) -> str:
    """JQL에서 project 키를 추출한다."""
    import re as _re
    m = _re.search(r'project\s*=\s*["\']?([A-Z][A-Z0-9_-]+)["\']?', jql, _re.IGNORECASE)
    return m.group(1).upper() if m else ""


# NOTE: JIRA_MCP_URL은 기본값이 있어 모듈 레벨 평가 OK.
# JIRA_USERNAME/JIRA_TOKEN은 .env 로드 전 import 시 빈 값이 되므로
# _get_mcp()에서 lazy 평가한다 (2026-04-29 fix).
JIRA_MCP_URL = os.getenv(
    "JIRA_MCP_URL", "http://mcp.sginfra.net/confluence-jira-mcp"
)
if JIRA_MCP_URL.startswith("http://"):
    logger.warning("[jira] JIRA_MCP_URL이 평문 HTTP입니다 — 토큰이 평문 전송될 수 있습니다.")

# ── Jira 조회 전용 로거 (logs/jira_query.log) ────────────────────────────
_jira_query_logger: "logging.Logger | None" = None


def _get_jira_query_logger() -> logging.Logger:
    """Jira 조회 전용 로거를 반환합니다."""
    global _jira_query_logger
    if _jira_query_logger is not None:
        return _jira_query_logger

    _jira_query_logger = logging.getLogger("jira_query")
    _jira_query_logger.setLevel(logging.INFO)
    _jira_query_logger.propagate = False

    bot_dir  = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(os.path.dirname(bot_dir), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, "jira_query.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _jira_query_logger.addHandler(fh)
    return _jira_query_logger


def log_jira_query(*, user_id: str = "", user_name: str = "",
                   action: str, query: str, result: str = "",
                   error: str = "", elapsed_ms: int = 0,
                   cache_status: str = ""):
    """
    /jira 조회 내역을 logs/jira_query.log 에 기록합니다.

    cache_status: HIT_MEM, HIT_DB, MISS, MISS_STALE, STORE, DISABLED
    """
    gl     = _get_jira_query_logger()
    status = "ERROR" if error else "OK"
    user   = f"{user_name}({user_id})" if user_id else (user_name or "unknown")

    msg = f"{status} | {action} | user={user} | query={query}"
    if result:
        msg += f" | result={result}"
    if error:
        msg += f" | error={error}"
    if cache_status:
        msg += f" | cache={cache_status}"
    if elapsed_ms > 0:
        msg += f" | {elapsed_ms}ms"

    if error:
        gl.error(msg)
    else:
        gl.info(msg)


# ── 싱글톤 MCP 세션 ─────────────────────────────────────────────────────

_mcp_session: "McpSession | None" = None


def _get_mcp() -> McpSession:
    global _mcp_session
    if _mcp_session is None:
        username = os.getenv("JIRA_USERNAME", "")
        token = os.getenv("JIRA_TOKEN", "")
        if not username:
            logger.warning("[jira] JIRA_USERNAME 환경변수가 설정되지 않았습니다.")
        if not token:
            logger.warning("[jira] JIRA_TOKEN 환경변수가 설정되지 않았습니다.")
        _mcp_session = McpSession(
            url=JIRA_MCP_URL,
            headers={
                "x-confluence-jira-username": username,
                "x-confluence-jira-token": token,
            },
            label="jira",
        )
    return _mcp_session


# ── JQL 자동 변환 헬퍼 ───────────────────────────────────────────────────

_JQL_KEYWORDS = re.compile(
    r'\b(project|status|assignee|reporter|priority|issuetype|created|updated|'
    r'resolution|fixversion|component|label|sprint|text|summary|description|'
    r'AND|OR|ORDER\s+BY|NOT\s+IN|IN|IS|WAS|CHANGED)\b',
    re.IGNORECASE,
)

_ISSUE_KEY_RE = re.compile(r'^[A-Z][A-Z0-9]+-\d+$')


def is_jql(text: str) -> bool:
    """텍스트가 JQL 구문인지 판별합니다."""
    return bool(_JQL_KEYWORDS.search(text))


def to_jql(text: str) -> str:
    """단순 텍스트를 JQL로 변환합니다. 이미 JQL이면 그대로 반환."""
    if is_jql(text):
        return text
    safe = text.replace('"', '\\"')
    return f'summary ~ "{safe}" ORDER BY updated DESC'


# 자연어 질문에서 제거할 한국어 지시어/조사
_STOP_WORDS = {
    "알려줘", "보여줘", "찾아줘", "검색해줘", "조회해줘", "요약해줘",
    "설명해줘", "확인해줘", "정리해줘", "분석해줘",
    "이슈", "관련", "관한", "대한", "어떤", "최근", "현재",
    "내용", "정보", "목록", "리스트", "뭐야", "뭐가",
    "있는지", "있나", "있어", "어때", "좀", "해줘", "줘",
    "어떻게", "무엇이", "무슨", "모든", "전체", "중에서",
}


def _extract_keywords(text: str) -> list:
    """자연어에서 불용어를 제거한 핵심 키워드 목록 반환."""
    words = text.split()
    keywords = [w for w in words if w not in _STOP_WORDS]
    return keywords if keywords else words[:3]


# ── 날짜 의도 감지 ──────────────────────────────────────────────────────

# "N월 N일" 패턴
_DATE_ABS_RE = re.compile(r'(\d{1,2})월\s*(\d{1,2})일')

# "YYYY-MM-DD" 또는 "YYYY.MM.DD" 또는 "YYYY/MM/DD"
_DATE_ISO_RE = re.compile(r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})')

# "최근 N일/주/달" 패턴
_DATE_RECENT_RE = re.compile(r'최근\s*(\d+)\s*(일|주|주일|개월|달)')

# "N일/주/달 전/이내" 패턴
_DATE_AGO_RE = re.compile(r'(\d+)\s*(일|주|주일|개월|달)\s*(전|이내)')

# 상대 날짜 키워드 → JQL 값
_DATE_RELATIVE_MAP = [
    ('이번 주', 'startOfWeek()'),
    ('이번주',  'startOfWeek()'),
    ('지난 주', 'startOfWeek(-1)'),
    ('지난주',  'startOfWeek(-1)'),
    ('이번 달', 'startOfMonth()'),
    ('이번달',  'startOfMonth()'),
    ('지난 달', 'startOfMonth(-1)'),
    ('지난달',  'startOfMonth(-1)'),
    ('올해',    'startOfYear()'),
    ('어제',    'startOfDay(-1)'),
    ('오늘',    'startOfDay()'),
]

# created vs updated 판별
_CREATED_KW = re.compile(r'(생성|등록|올라온|발생|만든|접수|작성)')
_UPDATED_KW = re.compile(r'(수정|업데이트|변경|갱신)')

# 날짜 관련 불용어 (키워드에서 제거)
_DATE_NOISE = re.compile(
    r'(부터|까지|이후|이전|사이|동안|생성된|등록된|올라온|발생된|발생한|'
    r'수정된|업데이트된|변경된|현재까지|지금까지|오늘까지|에서)',
)


def _detect_date_filter(question: str) -> "tuple[str | None, str]":
    """자연어 질문에서 날짜 의도를 감지하여 JQL 날짜 조건을 반환.

    Returns
    -------
    (date_clause, cleaned_question)
        date_clause     : JQL 날짜 조건 (예: 'created >= "2026-04-27"'), 없으면 None
        cleaned_question: 날짜 관련 텍스트가 제거된 질문 (키워드 추출용)
    """
    date_field = "created"
    if _UPDATED_KW.search(question):
        date_field = "updated"

    date_value = None
    cleaned = question

    # 1. YYYY-MM-DD 패턴
    m = _DATE_ISO_RE.search(question)
    if m:
        date_value = f'"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"'
        cleaned = question[:m.start()] + question[m.end():]

    # 2. "N월 N일" 패턴
    if not date_value:
        m = _DATE_ABS_RE.search(question)
        if m:
            month, day = int(m.group(1)), int(m.group(2))
            year = _dt.date.today().year
            date_value = f'"{year}-{month:02d}-{day:02d}"'
            cleaned = question[:m.start()] + question[m.end():]

    # 3. "최근 N일/주/달" 패턴
    if not date_value:
        m = _DATE_RECENT_RE.search(question)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            if unit in ('주', '주일'):
                date_value = f'"-{n}w"'
            elif unit in ('개월', '달'):
                date_value = f'startOfMonth(-{n})'
            else:
                date_value = f'"-{n}d"'
            cleaned = question[:m.start()] + question[m.end():]

    # 4. "N일/주/달 전/이내" 패턴
    if not date_value:
        m = _DATE_AGO_RE.search(question)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            if unit in ('주', '주일'):
                date_value = f'"-{n}w"'
            elif unit in ('개월', '달'):
                date_value = f'startOfMonth(-{n})'
            else:
                date_value = f'"-{n}d"'
            cleaned = question[:m.start()] + question[m.end():]

    # 5. 상대 날짜 키워드 (오늘, 어제, 이번주 등)
    if not date_value:
        for kw, jql_val in _DATE_RELATIVE_MAP:
            if kw in question:
                date_value = jql_val
                cleaned = cleaned.replace(kw, '', 1)
                break

    if not date_value:
        return None, question

    # 날짜 관련 노이즈 제거 후 정리
    cleaned = _DATE_NOISE.sub('', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return f'{date_field} >= {date_value}', cleaned


def _inject_before_order(jql: str, clause: str) -> str:
    """ORDER BY 앞에 AND 절을 삽입합니다.

    예: _inject_before_order(
        'text ~ "foo" ORDER BY updated DESC',
        'AND priority IN (Critical)'
    ) → 'text ~ "foo" AND priority IN (Critical) ORDER BY updated DESC'
    """
    upper = jql.upper()
    if " ORDER BY " in upper:
        idx = upper.index(" ORDER BY ")
        return f"{jql[:idx]} {clause}{jql[idx:]}"
    return f"{jql} {clause}"


def question_to_jql(question: str, project_key: str = "") -> str:
    """자연어 질문에서 검색 키워드를 추출하여 JQL로 변환합니다.

    '/jira 카제나 \\ 접속 불가 이슈 알려줘' 같은 파이프 질문에서
    핵심 키워드만 추출 → text ~ "키워드" (전체 텍스트 필드 검색).

    to_jql()과 달리 summary가 아닌 text 필드를 사용하여 더 넓은 범위 검색.

    Parameters
    ----------
    question    : 자연어 질문
    project_key : Jira 프로젝트 키 (있으면 project = KEY 조건 추가)
    """
    if is_jql(question):
        return question

    # ── 키워드 규칙 매칭 ─────────────────────────────────────────
    from keyword_rules import match_jira_keyword_rule
    kw_rule = match_jira_keyword_rule(question, project_key=project_key)

    # ── 날짜 의도 감지 ────────────────────────────────────────────
    date_clause, question_cleaned = _detect_date_filter(question)

    # ── 상태 의도 감지 → 상태 필터 JQL 생성 ──────────────────────
    intent_jql = _detect_status_intent(question)
    if intent_jql:
        if project_key:
            jql = f"project = {project_key} AND {intent_jql}"
        else:
            jql = intent_jql
        if date_clause:
            jql = _inject_before_order(jql, f'AND {date_clause}')
        if kw_rule:
            jql = _inject_before_order(jql, kw_rule["jql_append"])
        return jql

    # 날짜 제거 후 남은 키워드 추출
    keywords = _extract_keywords(question_cleaned)
    keyword_text = " ".join(keywords)
    safe = keyword_text.replace('"', '\\"')

    # 키워드가 있으면 text ~ + 날짜, 없으면 날짜만
    if safe.strip():
        jql = f'text ~ "{safe}" ORDER BY updated DESC'
    elif date_clause:
        jql = 'ORDER BY updated DESC'
    else:
        jql = f'text ~ "{safe}" ORDER BY updated DESC'

    if project_key:
        if " ORDER BY " in jql.upper():
            idx = jql.upper().index(" ORDER BY ")
            jql = f"project = {project_key} AND {jql[:idx]}{jql[idx:]}"
        else:
            jql = f"project = {project_key} AND {jql}"

    if date_clause:
        jql = _inject_before_order(jql, f'AND {date_clause}')
    if kw_rule:
        jql = _inject_before_order(jql, kw_rule["jql_append"])
    return jql


# ── 상태 의도 감지 패턴 ──────────────────────────────────────────
# "액티브 이슈 몇개", "열린 이슈", "활성 이슈", "미완료 이슈" 등

_ACTIVE_PATTERNS = re.compile(
    r'(액티브|활성|열린|오픈|진행\s*중|미완료|미해결|open|active|in\s*progress)',
    re.IGNORECASE,
)

_CLOSED_PATTERNS = re.compile(
    r'(완료|종료|닫힌|해결|closed|done|resolved)',
    re.IGNORECASE,
)

# "이슈 수", "이슈 몇개", "이슈가 몇개", "이슈 개수", "이슈 카운트" 등
_COUNT_PATTERNS = re.compile(
    r'(몇\s*개|몇\s*건|개수|수|총|전체|카운트|count|total|how\s*many)',
    re.IGNORECASE,
)

# 완료/종료 상태값 (Jira 표준 + 국문)
_DONE_STATUSES = '("Closed", "Done", "완료", "종료", "해결됨", "닫힘")'


def _detect_status_intent(question: str) -> "str | None":
    """자연어 질문에서 상태 기반 의도를 감지하여 JQL 조건을 반환.

    예:
      "현재 액티브 이슈가 몇개야?" → status NOT IN ("Closed", ...) ORDER BY updated DESC
      "완료된 이슈 알려줘"         → status IN ("Closed", ...) ORDER BY updated DESC

    Returns
    -------
    str | None : JQL 조건문 (project 조건 제외), 의도 미감지 시 None
    """
    # 활성(액티브) 이슈 의도
    if _ACTIVE_PATTERNS.search(question):
        return f"status NOT IN {_DONE_STATUSES} ORDER BY updated DESC"

    # 완료/종료 이슈 의도
    if _CLOSED_PATTERNS.search(question):
        return f"status IN {_DONE_STATUSES} ORDER BY updated DESC"

    return None


def question_to_jql_variants(question: str, project_key: str = "") -> list:
    """자연어 질문에서 점진적으로 넓어지는 JQL 변환 목록 반환.

    첫 번째가 가장 구체적이고, 뒤로 갈수록 넓은 범위 검색.
    첫 번째 JQL로 0건이면 다음 것을 시도하는 broadening 패턴에 사용.

    상태 의도가 감지되면 해당 JQL을 단일 항목으로 반환 (broadening 불필요).

    예: "접속 불가 현상 관련 이슈" →
      1. text ~ "접속 불가 현상" (전체 키워드)
      2. text ~ "접속 불가"      (앞 2개 키워드)
      3. text ~ "접속"           (첫 키워드만)

    예: "현재 액티브 이슈가 몇개야?" →
      1. status NOT IN ("Closed", ...) ORDER BY updated DESC
    """
    if is_jql(question):
        return [question]

    # ── 키워드 규칙 매칭 ─────────────────────────────────────────
    from keyword_rules import match_jira_keyword_rule
    kw_rule = match_jira_keyword_rule(question, project_key=project_key)

    # ── 날짜 의도 감지 ──────────────────────────────────────────
    date_clause, question_cleaned = _detect_date_filter(question)

    # 상태 의도 감지 시 단일 JQL 반환
    intent_jql = _detect_status_intent(question)
    if intent_jql:
        if project_key:
            jql = f"project = {project_key} AND {intent_jql}"
        else:
            jql = intent_jql
        if date_clause:
            jql = _inject_before_order(jql, f'AND {date_clause}')
        if kw_rule:
            jql = _inject_before_order(jql, kw_rule["jql_append"])
        return [jql]

    # 날짜 제거 후 남은 키워드 추출
    keywords = _extract_keywords(question_cleaned)

    def _with_project(j: str) -> str:
        if not project_key:
            return j
        if " ORDER BY " in j.upper():
            idx = j.upper().index(" ORDER BY ")
            return f"project = {project_key} AND {j[:idx]}{j[idx:]}"
        return f"project = {project_key} AND {j}"

    def _with_date(j: str) -> str:
        if date_clause:
            return _inject_before_order(j, f'AND {date_clause}')
        return j

    def _with_rule(j: str) -> str:
        if kw_rule:
            return _inject_before_order(j, kw_rule["jql_append"])
        return j

    # 키워드 없고 날짜만 있는 경우 → 날짜 필터만으로 검색
    if not keywords or not any(k.strip() for k in keywords):
        jql = _with_rule(_with_date(_with_project('ORDER BY updated DESC')))
        return [jql]

    variants = []
    # 전체 키워드
    full = " ".join(keywords).replace('"', '\\"')
    variants.append(_with_rule(_with_date(_with_project(f'text ~ "{full}" ORDER BY updated DESC'))))

    # 키워드가 2개 이상이면 앞 2개만
    if len(keywords) >= 3:
        partial = " ".join(keywords[:2]).replace('"', '\\"')
        variants.append(_with_rule(_with_date(_with_project(f'text ~ "{partial}" ORDER BY updated DESC'))))

    # 첫 키워드만 (2자 이상일 때)
    if len(keywords) >= 2 and len(keywords[0]) >= 2:
        single = keywords[0].replace('"', '\\"')
        variants.append(_with_rule(_with_date(_with_project(f'text ~ "{single}" ORDER BY updated DESC'))))

    return variants


def looks_like_issue_key(text: str) -> bool:
    """이슈 키 패턴(PROJ-123)인지 판별합니다."""
    return bool(_ISSUE_KEY_RE.match(text.strip().upper()))


# ── AI 기반 JQL 생성 ───────────────────────────────────────────────────

_JQL_GEN_SYSTEM = """\
당신은 Jira JQL(Jira Query Language) 전문가입니다.
사용자의 자연어 질문을 정확한 JQL 쿼리로 변환하세요.

규칙:
1. 반드시 유효한 JQL만 출력하세요. 설명이나 마크다운 없이 JQL 한 줄만.
2. 날짜는 JQL 형식 사용: "2026-04-27", "-7d", startOfDay(), startOfWeek(), startOfMonth() 등.
3. 텍스트 검색은 text ~ "키워드" 사용 (summary 대신 text로 넓은 범위 검색).
4. 항상 ORDER BY updated DESC 또는 ORDER BY created DESC 를 끝에 추가.
5. 프로젝트 키가 주어지면 project = KEY 조건을 포함.
6. 오늘 날짜: {today}. "4월 27일"처럼 연도 없으면 올해({year})로 간주.
7. "~부터 ~까지" → created >= "시작" AND created <= "끝"
8. "최근 N일" → created >= "-Nd"
9. 우선순위 언급 시: priority IN (Critical, Major, Minor, Trivial, Blocker)
10. 상태 언급 시: status IN/NOT IN ("Open", "In Progress", "Closed", "Done", "Resolved")
"""


def generate_jql_with_ai(question: str, project_key: str = "") -> "str | None":
    """Claude Haiku를 사용하여 자연어 질문을 JQL로 변환.

    # DEPRECATED — rule-based fallback용으로만 사용 (task-128 Intent JSON 분리 아키텍처로 대체)

    Returns
    -------
    str | None : 생성된 JQL, 실패 시 None (fallback으로 question_to_jql_variants 사용)
    """
    import anthropic as _anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None

    today = _dt.date.today()
    system_prompt = _JQL_GEN_SYSTEM.format(today=today.isoformat(), year=today.year)

    user_msg = f"질문: {question}"
    if project_key:
        user_msg += f"\n프로젝트 키: {project_key}"

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        jql = msg.content[0].text.strip()
        # 마크다운 코드블록 제거
        if jql.startswith("```"):
            jql = jql.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        # JQL 유효성 기본 검증 (ORDER BY 포함 여부)
        if not jql or len(jql) < 10:
            logger.warning(f"[jira] AI JQL 생성 실패 — 너무 짧음: {jql!r}")
            return None
        logger.info(f"[jira][ai-jql] 질문={question!r} → JQL={jql}")
        return jql
    except Exception as e:
        logger.warning(f"[jira] AI JQL 생성 예외: {e}")
        return None


# ── Intent JSON 분리 아키텍처 (task-128) ────────────────────────────────

@dataclass
class JiraIntent:
    """자연어 질문에서 추출한 Jira 검색 의도."""
    project_key: str = ""
    date_field: Optional[str] = None      # "created" | "updated" | None
    date_from: Optional[str] = None       # "YYYY-MM-DD"
    date_to: Optional[str] = None         # "YYYY-MM-DD"
    status: list = field(default_factory=list)
    priority: list = field(default_factory=list)
    assignee: Optional[str] = None        # "currentUser()" | "username"
    reporter: Optional[str] = None
    issue_type: list = field(default_factory=list)
    labels: list = field(default_factory=list)
    text_keywords: list = field(default_factory=list)
    order_by: str = "updated DESC"
    limit: int = 10
    ambiguity_notes: str = ""
    ai_failed: bool = False               # True → fallback 경로로 진입


@dataclass
class SearchResult:
    """search_with_ladder 반환값."""
    data: object                          # Jira search response
    jql: str                              # 실제 사용된 JQL
    relaxation_level: int                 # 0 = 원본 조건 사용
    jql_history: list                     # [{level, jql, count, label}]
    total_count: int                      # 최종 이슈 수
    error: Optional[str] = None


# Intent 캐시: 동일 질문 60초 TTL (비용 절약)
_INTENT_CACHE: "dict[str, tuple]" = {}   # {key: (intent, timestamp)}
_INTENT_CACHE_TTL = 60                   # seconds

_INTENT_SYSTEM = """\
당신은 Jira 쿼리 Intent 분석기입니다.
사용자의 한국어 질문을 분석하여 JSON 객체만 출력하세요.
설명, 마크다운, 추가 텍스트는 절대 금지.

### 오늘 날짜: {today} (YYYY-MM-DD)

### 출력 JSON 스키마 (모든 필드 필수 포함, 모르면 null/[])
{{
  "date_field": "created" | "updated" | null,
  "date_from": "YYYY-MM-DD" | null,
  "date_to": "YYYY-MM-DD" | null,
  "status": ["Open","In Progress",...] | [],
  "priority": ["High","Highest",...] | [],
  "assignee": "currentUser()" | "username" | null,
  "reporter": null,
  "issue_type": ["Bug","Task",...] | [],
  "labels": [],
  "text_keywords": ["명사1","명사2"] | [],
  "order_by": "created DESC" | "updated DESC",
  "limit": 10,
  "ambiguity_notes": "가정/모호성 설명 (없으면 빈 문자열)"
}}

### 핵심 규칙
1. 날짜/기간은 date_from/date_to 필드 사용. text_keywords에 절대 포함 금지.
2. "4월 27일" → 연도={year}, date_from="{year}-04-27", date_to="{year}-04-27"
3. "부터~까지" → date_from=시작, date_to=끝 (둘 다 필수)
4. "최근 N일" → date_from=오늘-N일, date_to=오늘
5. "어제" → date_from=어제, date_to=어제
6. "이번 주" → date_from=이번주_월요일, date_to=오늘
7. "이번 달" → date_from=이번달_1일, date_to=오늘
8. 수정/업데이트 언급 → date_field="updated"; 생성/등록/올라온 → date_field="created"
9. text_keywords = 검색할 명사 단어만. "이슈","리스트","찾아줘","알려줘","생성된","올라온" 등 메타 단어 제외.
10. "내가 담당" → assignee="currentUser()"
11. "미해결/열린/진행중" → status=["Open","In Progress","Reopened"]
12. "완료/닫힌/해결됨" → status=["Done","Closed","Resolved"]
13. "버그만/버그" → issue_type=["Bug"]
14. order_by: 날짜 조건 있으면 "created DESC", 없으면 "updated DESC"
15. limit: 기본 10, "전부/모두/전체" → 50\
"""


def extract_intent(question: str, project_key: str = "") -> JiraIntent:
    """자연어 질문 → JiraIntent (AI 또는 fallback).

    실패 시 ai_failed=True인 JiraIntent 반환 (None 반환하지 않음).
    캐시: 동일 (question, project_key) 60초 TTL.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[jira][intent] ANTHROPIC_API_KEY 미설정 → ai_failed=True")
        return JiraIntent(project_key=project_key, ai_failed=True)

    # 상대 날짜 포함 시 캐시 키에 오늘 날짜 포함 (B-2 시정)
    has_relative = any(w in question for w in ["어제", "오늘", "이번", "지난", "최근", "올해"])
    cache_key = (
        f"{project_key}::{question}::{_dt.date.today().isoformat()}"
        if has_relative
        else f"{project_key}::{question}"
    )

    # 캐시 확인
    if cache_key in _INTENT_CACHE:
        intent, ts = _INTENT_CACHE[cache_key]
        if time.time() - ts < _INTENT_CACHE_TTL:
            logger.info("[jira][intent] 캐시 HIT: %r", question[:50])
            return intent

    today = _dt.date.today()
    system = _INTENT_SYSTEM.format(today=today.isoformat(), year=today.year)

    user_msg = f"질문: {question}"
    if project_key:
        user_msg += f"\n프로젝트 키: {project_key}"

    raw = ""
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = msg.content[0].text.strip()

        # 마크다운 코드블록 제거
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

        data = json.loads(raw)
        intent = JiraIntent(
            project_key=project_key,
            date_field=data.get("date_field"),
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
            status=data.get("status") or [],
            priority=data.get("priority") or [],
            assignee=data.get("assignee"),
            reporter=data.get("reporter"),
            issue_type=data.get("issue_type") or [],
            labels=data.get("labels") or [],
            text_keywords=data.get("text_keywords") or [],
            order_by=data.get("order_by", "updated DESC"),
            limit=min(int(data.get("limit", 10)), 50),
            ambiguity_notes=data.get("ambiguity_notes", ""),
        )

        logger.info("[jira][intent] question=%r → intent=%s",
                    question[:80], json.dumps({
                        "date_field": intent.date_field,
                        "date_from": intent.date_from, "date_to": intent.date_to,
                        "status": intent.status, "priority": intent.priority,
                        "keywords": intent.text_keywords,
                    }, ensure_ascii=False))

        _INTENT_CACHE[cache_key] = (intent, time.time())
        return intent

    except (json.JSONDecodeError, KeyError, AttributeError) as e:
        logger.warning("[jira][intent] JSON 파싱 실패: %s | raw=%r", e, raw[:100])
        return JiraIntent(project_key=project_key, ai_failed=True)
    except Exception as e:
        logger.warning("[jira][intent] AI 호출 실패: %s", e)
        return JiraIntent(project_key=project_key, ai_failed=True)


def _next_day(date_str: str) -> str:
    """YYYY-MM-DD → 다음 날짜 문자열."""
    d = _dt.date.fromisoformat(date_str)
    return (d + _dt.timedelta(days=1)).isoformat()


def build_jql_from_intent(intent: JiraIntent) -> str:
    """JiraIntent → JQL 문자열 (결정론적, 동일 intent = 동일 JQL).

    JQL 인젝션 방어: project_key/assignee/reporter 화이트리스트 검증,
    text_keywords 특수문자 escape 및 메타 키워드 제거.
    """
    clauses = []

    # 1. 프로젝트 (인젝션 방어: 영대문자+숫자+_- 만 허용)
    if intent.project_key:
        safe_pkey = re.sub(r"[^A-Z0-9_-]", "", intent.project_key.upper())
        if safe_pkey:
            clauses.append(f"project = {safe_pkey}")

    # 2. 날짜 조건
    if intent.date_from and intent.date_field:
        df = intent.date_field   # "created" or "updated"
        date_from = intent.date_from
        date_to = intent.date_to or intent.date_from

        if date_from == date_to:
            clauses.append(f'{df} >= "{date_from}" AND {df} < "{_next_day(date_to)}"')
        else:
            clauses.append(f'{df} >= "{date_from}" AND {df} <= "{date_to}"')

    # 3. 상태 (MEDIUM-1 시정: 내부 따옴표 escape)
    if intent.status:
        status_list = ", ".join(f'"{s.replace(chr(92), chr(92)+chr(92)).replace(chr(34), chr(92)+chr(34))}"' for s in intent.status)
        clauses.append(f"status IN ({status_list})")

    # 4. 우선순위 (MEDIUM-1 시정)
    if intent.priority:
        pri_list = ", ".join(f'"{p.replace(chr(92), chr(92)+chr(92)).replace(chr(34), chr(92)+chr(34))}"' for p in intent.priority)
        clauses.append(f"priority IN ({pri_list})")

    # 5. 담당자/보고자 (인젝션 방어: 영숫자+@+._()- 만 허용)
    if intent.assignee:
        if intent.assignee == "currentUser()":
            clauses.append("assignee = currentUser()")
        else:
            safe_assignee = re.sub(r'[^A-Za-z0-9@._()-]', '', intent.assignee)
            if safe_assignee:
                clauses.append(f'assignee = "{safe_assignee}"')
    if intent.reporter:
        safe_reporter = re.sub(r'[^A-Za-z0-9@._()-]', '', intent.reporter)
        if safe_reporter:
            clauses.append(f'reporter = "{safe_reporter}"')

    # 6. 이슈 유형 (MEDIUM-1 시정)
    if intent.issue_type:
        type_list = ", ".join(f'"{t.replace(chr(92), chr(92)+chr(92)).replace(chr(34), chr(92)+chr(34))}"' for t in intent.issue_type)
        clauses.append(f"issuetype IN ({type_list})")

    # 7. 라벨 (LOW-1 시정: 역슬래시 선처리)
    if intent.labels:
        for label in intent.labels:
            safe_label = label.replace('\\', '\\\\').replace('"', '\\"')
            clauses.append(f'labels = "{safe_label}"')

    # 8. 텍스트 키워드 (JQL 인젝션 방어)
    if intent.text_keywords:
        safe_keywords = []
        for kw in intent.text_keywords:
            kw_clean = kw.replace('\\', '\\\\').replace('"', '\\"')
            # ORDER BY, DROP, INSERT, UPDATE, ; 포함 토큰 제거
            if re.search(r'(?i)(order\s*by|;|drop\s|insert\s|update\s)', kw_clean):
                logger.warning("[jira][build] JQL 인젝션 의심 키워드 제거: %r", kw)
                continue
            safe_keywords.append(kw_clean)
        if safe_keywords:
            keyword_str = " ".join(safe_keywords)
            clauses.append(f'text ~ "{keyword_str}"')

    # ORDER BY — 화이트리스트 (HIGH-1 시정)
    _ALLOWED_ORDER = {"updated DESC", "updated ASC", "created DESC", "created ASC"}
    order = intent.order_by if intent.order_by in _ALLOWED_ORDER else "updated DESC"

    if not clauses:
        # 아무 조건 없으면 프로젝트만
        if intent.project_key:
            safe_pkey = re.sub(r"[^A-Z0-9_-]", "", intent.project_key.upper())
            return f"project = {safe_pkey} ORDER BY {order}"
        return f"ORDER BY {order}"

    return " AND ".join(clauses) + f" ORDER BY {order}"


def search_with_ladder(jira_cli, intent: JiraIntent,
                       max_queries: int = 4) -> SearchResult:
    """Intent로 JQL 생성 → 0건이면 자동 완화 → 최대 max_queries회 시도.

    완화 순서:
      L0: 원본 (모든 조건)
      L1: status 제거
      L2: status+priority 제거
      L3: keywords 절반 + 날짜 ±7일 확장
      이후: 결과 있으면 정지, 없으면 다음 단계

    에러 분류:
      - auth/5xx/timeout 에러 → 즉시 반환 (완화 무의미)
      - JQL syntax error(400) → 다음 ladder 진행
    """
    history = []
    queries_used = 0
    seen_jqls: "set[str]" = set()   # 중복 JQL 방지 (M-2 시정)

    def _is_fatal_error(err: str) -> bool:
        """인증/5xx/타임아웃 에러 → True (ladder 중단). JQL syntax → False (계속)."""
        lower = err.lower()
        return any(k in lower for k in ("인증", "401", "403", "timeout", "500", "502", "503"))

    def _try(relaxed_intent: JiraIntent, level: int, label: str) -> "Optional[SearchResult]":
        nonlocal queries_used
        if queries_used >= max_queries:
            return None

        jql = build_jql_from_intent(relaxed_intent)
        # 중복 JQL 건너뜀 (M-2: L1/L2가 동일 JQL 생성 시 낭비 방지)
        if jql in seen_jqls:
            logger.info("[jira][ladder] 중복 JQL skip (L%d): %s", level, jql[:80])
            return None
        seen_jqls.add(jql)

        data, err = jira_cli.search_issues(jql, max_results=relaxed_intent.limit)
        queries_used += 1

        count = 0
        if data and isinstance(data, dict):
            issues = data.get("issues", [])
            count = len(issues)

        history.append({"level": level, "jql": jql, "count": count, "label": label})

        if err:
            if _is_fatal_error(str(err)):
                # auth/5xx → 즉시 반환 (A-3 시정)
                return SearchResult(data=None, jql=jql, relaxation_level=level,
                                    jql_history=history, total_count=0, error=str(err))
            # JQL syntax error → None 반환하여 다음 ladder 진행
            logger.info("[jira][ladder] JQL 오류(L%d) → 다음 단계: %s", level, err)
            return None
        if count > 0:
            return SearchResult(data=data, jql=jql, relaxation_level=level,
                                jql_history=history, total_count=count)
        return None  # 0건 → 계속 완화

    # L0: 원본
    result = _try(intent, 0, "원본 조건")
    if result:
        return result

    # L1: status 제거
    if intent.status and queries_used < max_queries:
        r1 = replace(intent, status=[])
        result = _try(r1, 1, "상태 조건 제거")
        if result:
            return result

    # L2: status + priority 제거 (L1과 동일 JQL이면 seen_jqls로 자동 skip)
    if (intent.status or intent.priority) and queries_used < max_queries:
        r2 = replace(intent, status=[], priority=[])
        result = _try(r2, 2, "상태·우선순위 조건 제거")
        if result:
            return result

    # L3: 날짜 범위 ±7일 확장 + keywords 절반
    if queries_used < max_queries:
        r3_kwargs: dict = {"status": [], "priority": []}
        if intent.date_from:
            d_from = _dt.date.fromisoformat(intent.date_from)
            d_to = _dt.date.fromisoformat(intent.date_to or intent.date_from)
            r3_kwargs["date_from"] = (d_from - _dt.timedelta(days=7)).isoformat()
            r3_kwargs["date_to"] = (d_to + _dt.timedelta(days=7)).isoformat()
        if intent.text_keywords and len(intent.text_keywords) > 1:
            r3_kwargs["text_keywords"] = intent.text_keywords[:max(1, len(intent.text_keywords) // 2)]
        r3 = replace(intent, **r3_kwargs)
        result = _try(r3, 3, "날짜 ±7일 확장·조건 완화")
        if result:
            return result

    # 모두 0건 → 빈 결과 반환 (history 포함)
    last_jql = history[-1]["jql"] if history else build_jql_from_intent(intent)
    return SearchResult(data=None, jql=last_jql, relaxation_level=-1,
                        jql_history=history, total_count=0)


def format_intent_summary(intent: JiraIntent, result: "Optional[SearchResult]") -> str:
    """Slack 메시지에 추가할 "이해한 내용 + 결과 요약" 블록.

    - intent.ai_failed=True  → 경고 메시지 반환 (빈 문자열 금지 — ISS-007)
    - result=None            → 완화 이력 없이 intent 해석만 반환 (ai_failed 경로)
    - result.total_count==0  → 완화 이력 + 0건 메시지
    """
    if intent.ai_failed:
        # C-2 + MAJOR-4 시정: silent fallback 금지
        return ":warning: 자연어 분석 실패 — 키워드 매칭으로 대체합니다"

    parts = [":mag: *이해한 내용*"]

    # 날짜
    if intent.date_from:
        date_label = "생성일" if intent.date_field == "created" else "수정일"
        if intent.date_from == intent.date_to or not intent.date_to:
            parts.append(f"  • 기간: {intent.date_from} ({date_label} 기준)")
        else:
            parts.append(f"  • 기간: {intent.date_from} ~ {intent.date_to} ({date_label} 기준)")

    if intent.status:
        parts.append(f"  • 상태: {', '.join(intent.status)}")
    if intent.priority:
        parts.append(f"  • 우선순위: {', '.join(intent.priority)}")
    if intent.assignee:
        label = "나" if intent.assignee == "currentUser()" else intent.assignee
        parts.append(f"  • 담당자: {label}")
    if intent.issue_type:
        parts.append(f"  • 유형: {', '.join(intent.issue_type)}")
    if intent.text_keywords:
        parts.append(f"  • 키워드: {', '.join(intent.text_keywords)}")
    if intent.ambiguity_notes:
        parts.append(f"  • 가정: _{intent.ambiguity_notes}_")

    # 완화 이력 (result=None이면 생략 — ai_failed 경로에서 호출 시 안전)
    if result is not None:
        if result.relaxation_level > 0:
            h = result.jql_history
            parts.append(
                f"\n:warning: 원래 조건으로 0건 → 자동 완화 ({h[-1]['label']}) → *{result.total_count}건*"
            )
        elif result.relaxation_level == -1:
            # 모두 0건
            parts.append("\n:x: 조건을 완화해도 결과 없음")
            parts.append(f"  :bulb: 더 넓은 조건으로: `/jira {intent.project_key} \\ {{단순 키워드}}`")

    return "\n".join(parts)


# ── JiraClient ───────────────────────────────────────────────────────────

class JiraClient:
    """Jira MCP 클라이언트 (3계층 캐시 통합)."""

    def __init__(self):
        self._mcp = _get_mcp()

    def _parse_raw(self, raw) -> object:
        """raw(str 또는 dict) -> Python 객체"""
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return raw
        return raw

    # ── 캐시 내부 헬퍼 ────────────────────────────────────────

    @staticmethod
    def _cache_key_issue(key: str) -> str:
        return f"jira:issue:{key.upper()}"

    @staticmethod
    def _cache_key_project(key: str) -> str:
        return f"jira:project:{key.upper()}"

    @staticmethod
    def _cache_key_projects() -> str:
        return "jira:projects"

    def _try_cache_get(self, cache_key: str) -> tuple:
        """L1→L2 캐시 조회. (data, cache_status) 반환. 미스 시 (None, status)."""
        if not _JIRA_CACHE_ENABLED:
            return None, "DISABLED"

        # L1: 메모리
        mem = _mem_get(cache_key)
        if mem is not None:
            if _ops_log:
                _ops_log.cache_hit(cache_key, source="memory")
            return mem, "HIT_MEM"

        # L2: SQLite
        t0 = _perf.now_ms() if _perf else 0
        node = _jira_cache.get_node("jira", cache_key)
        if node:
            if _jira_cache.is_stale(node["id"]):
                return None, "MISS_STALE"
            content = _jira_cache.get_content(node["id"])
            if content and content.get("body_text"):
                try:
                    data = json.loads(content["body_text"])
                    _mem_set(cache_key, data)  # L2 → L1 승격
                    if _ops_log:
                        elapsed = _perf.elapsed_ms(t0) if _perf else 0
                        _ops_log.cache_hit(cache_key, source="sqlite",
                                           node_id=node["id"], elapsed_ms=elapsed)
                    return data, "HIT_DB"
                except (json.JSONDecodeError, TypeError):
                    pass

        if _ops_log:
            elapsed = _perf.elapsed_ms(t0) if _perf else 0
            _ops_log.cache_miss(cache_key, reason="not_found", elapsed_ms=elapsed)
        return None, "MISS"

    def _cache_store(self, cache_key: str, title: str, data, *,
                     node_type: str = "issue", ttl_hours: float = 0.17):
        """L2 SQLite + L1 메모리에 캐시 저장."""
        if not _JIRA_CACHE_ENABLED or data is None:
            return
        t0 = _perf.now_ms() if _perf else 0
        try:
            body_text = json.dumps(data, ensure_ascii=False)
            node_id = _jira_cache.put_page(
                "jira", cache_key, title,
                node_type=node_type,
                body_text=body_text,
            )
            _jira_cache.upsert_meta(node_id, ttl_hours=ttl_hours)
            _mem_set(cache_key, data)
            if _ops_log:
                elapsed = _perf.elapsed_ms(t0) if _perf else 0
                _ops_log.cache_store(title, node_id=node_id,
                                     source_id=cache_key,
                                     char_count=len(body_text),
                                     has_body=True, elapsed_ms=elapsed)
        except Exception as e:
            logger.warning("[jira] 캐시 저장 실패 (%s): %s", cache_key, e)

    # ── MCP 호출 메서드 (캐시 통합) ───────────────────────────

    def search_issues(self, jql: str, max_results: int = 10) -> tuple:
        """
        JQL 검색 (캐시 미적용 — 검색 결과는 매번 달라질 수 있음).
        MCP 장애 시 jira_mirror 테이블 fallback (task-108).

        Returns: (parsed_data, error_str)
        """
        raw, err = self._mcp.call_tool("jql_search", {
            "jql_request": jql,
            "limit": max_results,
        })
        if err:
            # MCP 실패 → 미러 fallback 시도
            if _mirror_search:
                try:
                    pkey = _extract_project_from_jql(jql) or None
                    query = _jql_to_query_text(jql)
                    mirror_results = _mirror_search(query, project_key=pkey)
                    if mirror_results:
                        age = _mirror_age_fn(pkey) if _mirror_age_fn else "미러 없음"
                        logger.info("[jira] MCP 장애 → 미러 fallback (%d건, %s)", len(mirror_results), age)
                        return {"issues": mirror_results, "_mirror_age": age}, None
                except Exception as _fe:
                    logger.warning("[jira] 미러 fallback 실패: %s", _fe)
            return None, err
        return self._parse_raw(raw), None

    def get_issue(self, key: str) -> tuple:
        """
        이슈 상세 조회. 캐시 TTL ~10분.

        Returns: (parsed_data, error_str)
        """
        key = key.upper()
        cache_key = self._cache_key_issue(key)
        cache_status = ""

        # 캐시 조회
        cached, cache_status = self._try_cache_get(cache_key)
        if cached is not None:
            return cached, None

        # MCP 호출
        raw, err = self._mcp.call_tool("get_issue", {"key": key})
        if err:
            return None, err
        data = self._parse_raw(raw)

        # 캐시 저장
        if data:
            title = key
            if isinstance(data, dict):
                fields = data.get("fields", {})
                if fields and fields.get("summary"):
                    title = f"{key} {fields['summary']}"
            self._cache_store(cache_key, title, data,
                              node_type="issue", ttl_hours=_JIRA_ISSUE_TTL)
            cache_status = cache_status or "STORE"

        return data, None

    def get_all_projects(self) -> tuple:
        """
        프로젝트 목록 조회. 캐시 TTL 24시간.

        Returns: (parsed_data, error_str)
        """
        cache_key = self._cache_key_projects()
        cache_status = ""

        # 캐시 조회
        cached, cache_status = self._try_cache_get(cache_key)
        if cached is not None:
            return cached, None

        # MCP 호출
        raw, err = self._mcp.call_tool("get_all_projects", {})
        if err:
            return None, err
        data = self._parse_raw(raw)

        # 캐시 저장
        if data:
            self._cache_store(cache_key, "all_projects", data,
                              node_type="project_list", ttl_hours=_JIRA_PROJECTS_TTL)

        return data, None

    def get_project(self, key: str) -> tuple:
        """
        프로젝트 상세 조회. 캐시 TTL 1시간.

        Returns: (parsed_data, error_str)
        """
        key = key.upper()
        cache_key = self._cache_key_project(key)
        cache_status = ""

        # 캐시 조회
        cached, cache_status = self._try_cache_get(cache_key)
        if cached is not None:
            return cached, None

        # MCP 호출
        raw, err = self._mcp.call_tool("get_project", {"key": key})
        if err:
            return None, err
        data = self._parse_raw(raw)

        # 캐시 저장
        if data:
            name = key
            if isinstance(data, dict) and data.get("name"):
                name = f"{key} - {data['name']}"
            self._cache_store(cache_key, name, data,
                              node_type="project", ttl_hours=_JIRA_PROJECT_TTL)

        return data, None


# ── Slack 포맷 헬퍼 ──────────────────────────────────────────────────────

def _extract_field(fields: dict, key: str, sub: str = "name") -> str:
    """fields dict에서 중첩 필드(예: status.name)를 안전하게 추출."""
    val = fields.get(key)
    if val is None:
        return ""
    if isinstance(val, dict):
        return val.get(sub, val.get("displayName", str(val)))
    return str(val)


def format_search_results(data, query: str) -> str:
    """JQL 검색 결과 -> Slack 텍스트"""
    if not data:
        return f":information_source: `{query}` 검색 결과가 없습니다."

    issues = []
    total = 0

    if isinstance(data, dict):
        issues = data.get("issues", [])
        total = data.get("total", len(issues))
    elif isinstance(data, list):
        issues = data
        total = len(data)

    if not issues:
        return f":information_source: `{query}` 에 해당하는 이슈가 없습니다."

    lines = [f"*:mag: '{query}' 검색 결과 ({total}건)*\n"]
    for i, issue in enumerate(issues[:15], 1):
        key = issue.get("key", "?")
        fields = issue.get("fields", {})
        summary = fields.get("summary", "(제목 없음)")
        status = _extract_field(fields, "status")
        assignee = _extract_field(fields, "assignee", "displayName")
        priority = _extract_field(fields, "priority")
        issuetype = _extract_field(fields, "issuetype")

        line = f"{i}. *<{_issue_url(key)}|{key}>* {summary}"
        extras = []
        if status:
            extras.append(f":white_small_square: {status}")
        if assignee:
            extras.append(f":bust_in_silhouette: {assignee}")
        if priority:
            extras.append(f":arrow_up_small: {priority}")
        if issuetype:
            extras.append(f":label: {issuetype}")
        if extras:
            line += "\n    " + "  ".join(extras)
        lines.append(line)

    if total > 15:
        lines.append(f"\n_...외 {total - 15}건_")

    return "\n".join(lines)


def format_issue(data) -> str:
    """단일 이슈 상세 -> Slack 텍스트"""
    if not data or not isinstance(data, dict):
        return ":information_source: 이슈 정보를 가져올 수 없습니다."

    key = data.get("key", "?")
    fields = data.get("fields", {})
    summary = fields.get("summary", "(제목 없음)")
    status = _extract_field(fields, "status")
    assignee = _extract_field(fields, "assignee", "displayName")
    reporter = _extract_field(fields, "reporter", "displayName")
    priority = _extract_field(fields, "priority")
    issuetype = _extract_field(fields, "issuetype")
    created = (fields.get("created") or "")[:10]
    updated = (fields.get("updated") or "")[:10]
    description = fields.get("description") or ""

    lines = [f"*:ticket: <{_issue_url(key)}|{key}> — {summary}*\n"]

    info_parts = []
    if issuetype:
        info_parts.append(f":label: *유형*: {issuetype}")
    if status:
        info_parts.append(f":white_small_square: *상태*: {status}")
    if priority:
        info_parts.append(f":arrow_up_small: *우선순위*: {priority}")
    if assignee:
        info_parts.append(f":bust_in_silhouette: *담당자*: {assignee}")
    if reporter:
        info_parts.append(f":pencil2: *보고자*: {reporter}")
    if created:
        info_parts.append(f":calendar: *생성*: {created}")
    if updated:
        info_parts.append(f":arrows_counterclockwise: *수정*: {updated}")

    if info_parts:
        lines.append("\n".join(info_parts))

    if description:
        desc_text = description if isinstance(description, str) else str(description)
        if len(desc_text) > 500:
            desc_text = desc_text[:500] + "..."
        lines.append(f"\n*설명:*\n{desc_text}")

    return "\n".join(lines)


def format_project(data) -> str:
    """프로젝트 상세 -> Slack 텍스트"""
    if not data or not isinstance(data, dict):
        return ":information_source: 프로젝트 정보를 가져올 수 없습니다."

    key = data.get("key", "?")
    name = data.get("name", "(이름 없음)")
    description = data.get("description") or ""
    lead = data.get("lead", {})
    lead_name = lead.get("displayName", lead.get("name", "")) if isinstance(lead, dict) else ""
    ptype = data.get("projectTypeKey", "")

    lines = [f"*:file_folder: {key} — {name}*\n"]
    if ptype:
        lines.append(f":label: *유형*: {ptype}")
    if lead_name:
        lines.append(f":bust_in_silhouette: *리드*: {lead_name}")
    if description:
        if len(description) > 300:
            description = description[:300] + "..."
        lines.append(f"\n*설명:*\n{description}")

    return "\n".join(lines)


def format_projects_list(data) -> str:
    """프로젝트 목록 -> Slack 텍스트"""
    projects = []
    if isinstance(data, list):
        projects = data
    elif isinstance(data, dict):
        projects = data.get("values", data.get("projects", []))
        if not projects and data.get("key"):
            projects = [data]

    if not projects:
        return ":information_source: 프로젝트 목록이 비어 있습니다."

    lines = [f"*:file_folder: 프로젝트 목록 ({len(projects)}개)*\n"]
    for p in projects[:30]:
        key = p.get("key", "?")
        name = p.get("name", "")
        ptype = p.get("projectTypeKey", "")

        line = f"• *{key}* — {name}"
        if ptype:
            line += f"  _{ptype}_"
        lines.append(line)

    if len(projects) > 30:
        lines.append(f"\n_...외 {len(projects) - 30}개_")

    return "\n".join(lines)


def _issue_url(key: str) -> str:
    """이슈 키로 Jira 웹 URL을 생성합니다."""
    base = os.getenv("JIRA_BASE_URL", "https://jira.smilegate.net")
    return f"{base}/browse/{key}"


def _project_url(project_key: str) -> str:
    """프로젝트 키로 Jira 프로젝트 웹 URL을 생성합니다."""
    base = os.getenv("JIRA_BASE_URL", "https://jira.smilegate.net")
    return f"{base}/projects/{project_key}/"


# ── Claude AI 질의용 텍스트 추출 ──────────────────────────────────────────

def get_issue_context_text(data) -> str:
    """이슈 데이터에서 Claude AI 컨텍스트 텍스트를 추출합니다."""
    if not data or not isinstance(data, dict):
        return ""

    key = data.get("key", "?")
    fields = data.get("fields", {})
    summary = fields.get("summary", "")
    status = _extract_field(fields, "status")
    assignee = _extract_field(fields, "assignee", "displayName")
    reporter = _extract_field(fields, "reporter", "displayName")
    priority = _extract_field(fields, "priority")
    issuetype = _extract_field(fields, "issuetype")
    description = fields.get("description") or ""
    created = (fields.get("created") or "")[:10]
    updated = (fields.get("updated") or "")[:10]

    parts = [
        f"이슈: {key} - {summary}",
        f"유형: {issuetype}" if issuetype else "",
        f"상태: {status}" if status else "",
        f"우선순위: {priority}" if priority else "",
        f"담당자: {assignee}" if assignee else "",
        f"보고자: {reporter}" if reporter else "",
        f"생성일: {created}" if created else "",
        f"수정일: {updated}" if updated else "",
    ]
    if description:
        desc_text = description if isinstance(description, str) else str(description)
        if len(desc_text) > 2000:
            desc_text = desc_text[:2000] + "..."
        parts.append(f"\n설명:\n{desc_text}")

    return "\n".join(p for p in parts if p)


def get_search_context_text(data) -> str:
    """JQL 검색 결과에서 Claude AI 컨텍스트 텍스트를 추출합니다.

    각 이슈의 핵심 메타데이터(유형, 우선순위, 라벨, 컴포넌트 등)를
    포함하여 Claude가 이슈 맥락을 정확히 파악할 수 있도록 합니다.
    """
    if not data:
        return ""

    issues = []
    if isinstance(data, dict):
        issues = data.get("issues", [])
    elif isinstance(data, list):
        issues = data

    if not issues:
        return ""

    parts = []
    for issue in issues[:10]:
        key = issue.get("key", "?")
        fields = issue.get("fields", {})
        summary = fields.get("summary", "")
        status = _extract_field(fields, "status")
        issuetype = _extract_field(fields, "issuetype")
        priority = _extract_field(fields, "priority")
        assignee = _extract_field(fields, "assignee", "displayName")
        description = fields.get("description") or ""

        # 핵심 메타데이터
        meta_parts = []
        if issuetype:
            meta_parts.append(f"유형: {issuetype}")
        meta_parts.append(f"상태: {status}")
        if priority:
            meta_parts.append(f"우선순위: {priority}")
        meta_parts.append(f"담당자: {assignee}")

        # 라벨, 컴포넌트, 수정 버전 (enrichment 대응 — 키워드 역할)
        labels = fields.get("labels") or []
        components = fields.get("components") or []
        fix_versions = fields.get("fixVersions") or []

        tags = []
        if labels:
            tags.extend(labels)
        if components:
            tags.extend(c.get("name", str(c)) if isinstance(c, dict) else str(c)
                        for c in components)
        if fix_versions:
            tags.extend(v.get("name", str(v)) if isinstance(v, dict) else str(v)
                        for v in fix_versions)

        entry = f"[{key}] {summary}\n{', '.join(meta_parts)}"
        if tags:
            entry += f"\n태그: {', '.join(tags[:8])}"
        if description:
            desc_text = description if isinstance(description, str) else str(description)
            if len(desc_text) > 300:
                desc_text = desc_text[:300] + "..."
            entry += f"\n설명: {desc_text}"
        parts.append(entry)

    return "\n\n---\n\n".join(parts)
