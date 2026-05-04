#!/usr/bin/env python3
"""
slack_bot.py - Slack 알림 봇 메인 진입점

사용법:
  python slack_bot.py                           # 봇 실행 (스케줄 + 인터랙션)
  python slack_bot.py --test     CHANNEL        # 테스트 메시지 전송
  python slack_bot.py --channels                # 접근 가능한 채널 목록 출력
  python slack_bot.py --send     CHANNEL "MSG"  # 즉시 메시지 전송
  python slack_bot.py --find-user NAME          # 사용자 ID 검색 (mention 설정용)

환경변수 (.env):
  SLACK_BOT_TOKEN = xoxb-...   (Bot Token — 메시지 전송)
  SLACK_APP_TOKEN = xapp-...   (App-Level Token — Socket Mode 인터랙션)
"""

import os
import re
import sys
import time
import atexit
import logging
import argparse

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

import interaction_handler as ih
import message_expiry
from message_expiry import ExpiringResponder
from response_formatter import format_ai_response, ANSWER_FORMAT_INSTRUCTION
from slack_sender import SlackSender
from scheduler    import NotificationScheduler
import wiki_client as wc
import gdi_client as gc
import jira_client as jc
import claim_handler as ch
# task-129.5 wiring: 4단계 파이프라인 helper (intent_pipeline.py)
import intent_pipeline as ip
from safety_guard import detect_write_intent, format_block_message, READ_ONLY_INSTRUCTION
from ops_tracker import get_tracker as _get_ops_tracker

# ── 로그 설정 ──────────────────────────────────────────────────
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("slack_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── API 토큰 사용량 로거 ──────────────────────────────────────
import threading
_token_logger = None
_token_logger_lock = threading.Lock()

# ── task-115 helpers (R-2/R-3/R-4) ────────────────────────────────────
# routing_spec.yaml folder_routing 정책 inline 코드화.
# - tsv_policy: TSV 키워드 명시 시에만 game_data 폴더 검색 (R-3)
# - intent_keywords: 질문 키워드 → folder_role list (R-2)

_TSV_WHITELIST = ("tsv", "게임 데이터", "데이터 db", "데이터db")  # 소문자 정규화 후 매칭

_INTENT_KEYWORDS: dict = {
    "planning":   ["기획", "기획서", "기획안", "명세", "패치노트", "빌드", "핫픽스"],
    "qa_result":  ["qa", "테스트", "bat", "bug verification", "update checklist"],
    "live_issue": ["라이브", "운영 이슈", "커뮤니티 버그", "cs 이슈", "인앱 이슈"],
    "game_data":  ["tsv", "게임 데이터", "테이블", "데이터 db", "데이터db"],
}


def _is_tsv_explicit(search_query: str) -> bool:
    """task-115 R-3 — TSV 키워드 명시 여부 (Master 정책: 명시 시에만 조회)."""
    if not search_query:
        return False
    q = search_query.lower()
    return any(kw in q for kw in _TSV_WHITELIST)


def _detect_folder_role(question: str) -> "list | None":
    """task-115 R-2 — 사용자 질문에서 folder_role 의도 추출 (intent 라우팅)."""
    if not question:
        return None
    q = question.lower()
    matched = []
    for role, kws in _INTENT_KEYWORDS.items():
        if any(kw.lower() in q for kw in kws):
            matched.append(role)
    return matched if matched else None


# task-115 R-4: TemporalResolver 싱글톤 (has_recent_bare 전용)
try:
    from analytics.temporal_resolver import TemporalResolver as _TR
    _temporal_resolver_115 = _TR()
except Exception:
    _temporal_resolver_115 = None


def _has_recent_bare(text: str) -> bool:
    """task-115 R-4 — '최근/가장 최근' 단독 매칭 (충돌 방지 포함)."""
    if not _temporal_resolver_115:
        return False
    try:
        return _temporal_resolver_115.has_recent_bare(text or "")
    except Exception:
        return False


def _log_token_usage(source: str, input_tokens: int, output_tokens: int):
    """Claude API 호출 후 토큰 사용량을 별도 로그 파일에 기록."""
    global _token_logger
    try:
        with _token_logger_lock:
            if _token_logger is None:
                _token_logger = logging.getLogger("token_usage")
                _token_logger.setLevel(logging.INFO)
                _token_logger.propagate = False
                _logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
                os.makedirs(_logs_dir, exist_ok=True)
                fh = logging.FileHandler(
                    os.path.join(_logs_dir, "token_usage.log"), encoding="utf-8"
                )
                fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
                _token_logger.addHandler(fh)
        _token_logger.info(f"{source} | in={input_tokens} | out={output_tokens} | total={input_tokens + output_tokens}")
    except Exception:
        pass  # 로깅 실패가 비즈니스 로직을 방해하지 않도록


# ── /wiki 검색 전략 예외처리 규칙 시스템 ──────────────────────────────────────
# wiki_search_rules.json 에 페이지별 예외 규칙을 정의합니다.
# 규칙이 없는 페이지 / 조건은 모두 기본 동작(페이지 직접 조회)을 사용합니다.

_WIKI_RULES_PATH  = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "wiki_search_rules.json"
)
_wiki_rules_cache     = None   # None = 미로드, [] = 로드 완료(규칙 없음), [..] = 규칙 있음
_wiki_rules_mtime     = 0.0    # 마지막으로 로드한 파일 mtime (변경 감지 hot reload용)


def _load_wiki_search_rules() -> list:
    """
    wiki_search_rules.json 에서 활성화된 규칙 목록을 로드합니다.
    파일이 변경된 경우 자동으로 다시 로드합니다 (hot reload).

    Returns: 활성 규칙 list (파일 없거나 오류 시 [])
    """
    import json as _json
    global _wiki_rules_cache, _wiki_rules_mtime

    # ── hot reload: 파일 mtime 비교 ────────────────────────────────────────
    try:
        cur_mtime = os.path.getmtime(_WIKI_RULES_PATH)
    except FileNotFoundError:
        if _wiki_rules_cache is None:
            logger.info("[wiki][규칙] wiki_search_rules.json 없음 — 기본 전략 사용")
            _wiki_rules_cache = []
        return _wiki_rules_cache

    if _wiki_rules_cache is not None and cur_mtime == _wiki_rules_mtime:
        return _wiki_rules_cache   # 변경 없음 → 캐시 반환

    # ── 파일 로드 ─────────────────────────────────────────────────────────
    try:
        with open(_WIKI_RULES_PATH, "r", encoding="utf-8") as f:
            data = _json.load(f)
        _wiki_rules_cache = [r for r in data.get("rules", []) if r.get("enabled", True)]
        _wiki_rules_mtime = cur_mtime
        logger.info(
            f"[wiki][규칙] {len(_wiki_rules_cache)}개 규칙 로드"
            f"{' (변경 감지 — hot reload)' if _wiki_rules_mtime else ''}"
        )
    except Exception as e:
        logger.warning(f"[wiki][규칙] 로드 실패: {e}")
        _wiki_rules_cache = []

    return _wiki_rules_cache


def _find_matching_rule(page_part: str, question: str) -> "dict | None":
    """
    page_part + question 에 일치하는 예외처리 규칙을 반환합니다.
    경로(A > B > C 또는 A / B / C) 형식이면 leaf 제목만 추출해 매칭합니다.
    일치하는 규칙이 없으면 None 반환 → 기본 동작 사용.

    Parameters
    ----------
    page_part : str   /wiki 명령의 페이지 부분 (예: "2026_MGQA", "A > B > 2026_MGQA")
    question  : str   /wiki 명령의 질문 부분   (예: "에픽세븐 가장 최근 업무 내역 요약")
    """
    rules = _load_wiki_search_rules()
    if not rules:
        return None

    # 경로 구분자가 있으면 leaf 제목만 추출 (규칙은 페이지 제목 단위로 정의됨)
    if " / " in page_part:
        leaf = page_part.split(" / ")[-1].strip()
    elif ">" in page_part:
        leaf = page_part.split(">")[-1].strip()
    else:
        leaf = page_part.strip()

    q_lower = question.lower()

    for rule in rules:
        pattern    = rule.get("page_pattern", "")
        match_type = rule.get("match_type", "exact")
        keywords   = rule.get("trigger", {}).get("keywords", [])

        # ── 페이지 패턴 매칭 ────────────────────────────────────────────
        if match_type == "exact":
            page_matched = (leaf == pattern)
        elif match_type == "contains":
            page_matched = (pattern in leaf)
        elif match_type == "startswith":
            page_matched = leaf.startswith(pattern)
        elif match_type == "regex":
            page_matched = bool(re.search(pattern, leaf))
        else:
            page_matched = False

        if not page_matched:
            continue

        # ── 질문 키워드 매칭 ────────────────────────────────────────────
        if any(kw in q_lower for kw in keywords):
            logger.info(
                f"[wiki][규칙매칭] id={rule.get('id')} "
                f"page='{leaf}' strategy='{rule.get('strategy')}' "
                f"(질문: '{question[:30]}...')"
            )
            return rule

    return None


# ── /wiki (페이지 조회) 헬퍼 ──────────────────────────────────────────────────

def _wiki_help(respond):
    """도움말"""
    respond(text=(
        "*Wiki 도움말*\n\n"
        "```\n"
        "/wiki 페이지명 \\ 질문           페이지 조회 + AI 답변\n"
        "/wiki search \\ 질문            키워드로 페이지 검색\n"
        "/wiki help                      이 도움말\n"
        "```\n\n"
        "예시:\n"
        "• `/wiki Game Service 1 \\ QA 일정 알려줘`\n"
        "• `/wiki search \\ 에픽세븐 배포 일정`"
    ))


def _wiki_fetch_page(client, page_part: str, fetch_full: bool = True,
                     question: str = ""):
    """
    경로 구분자 유무에 따라 적합한 방식으로 Confluence 페이지를 조회합니다.

    지원하는 경로 구분자:
    - '>'       → /wiki A > B > C  (봇 전용 포맷)
    - ' / '     → /wiki A / B / C  (Confluence 브레드크럼 복사 포맷)
    - 구분자 없음 → 제목 직접 검색

    Parameters
    ----------
    fetch_full : bool
        True  → get_page_by_id 추가 MCP 호출로 전체 본문 조회 (AI 질의용)
        False → cql_search body.view 만 사용 (단순 표시용, MCP 호출 1회 절약)
    question : str
        파이프(\\) 뒤의 질문 텍스트. 비어 있지 않으면 질문 맥락(연도 등)을
        활용한 스마트 검색을 먼저 시도합니다.

    Returns: (page_dict | None, error_str | None)
    """
    # Confluence 브레드크럼( A / B / C ) 또는 봇 포맷( A > B > C ) 모두 지원
    if " / " in page_part:
        segments = [s.strip() for s in page_part.split(" / ")]
    elif ">" in page_part:
        segments = [s.strip() for s in page_part.split(">")]
    else:
        segments = []

    if segments:
        leaf_title = segments[-1]
        ancestors  = segments[:-1]
        return client.get_page_by_path(ancestors, leaf_title,
                                       fetch_full=fetch_full)

    # 질문 맥락이 있으면 스마트 검색 (연도·키워드 인식)
    if question:
        return client.search_with_context(
            page_part, question=question, fetch_full=fetch_full
        )
    return client.get_page_by_title(page_part, fetch_full=fetch_full)


def _wiki_get_page(client, page_part: str, respond):
    """경로/페이지 제목으로 내용 조회 후 Slack 에 표시 (fetch_full=False: 표시 전용)"""
    page, err = _wiki_fetch_page(client, page_part, fetch_full=False)
    if err:
        respond(text=f"❌ 페이지 조회 실패\n```\n{err}\n```")
        return

    text      = page["text"]
    MAX_LEN   = 2800
    truncated = len(text) > MAX_LEN
    if truncated:
        text = text[:MAX_LEN]

    msg = (
        f"*📄 {page['title']}*\n"
        f"🔗 <{page['url']}|전체 페이지 열기>\n\n"
        f"```\n{text}\n```"
    )
    if truncated:
        msg += "\n\n⚠️ 내용이 길어 일부만 표시됩니다. 전체 내용은 페이지 링크를 확인하세요."
    respond(text=msg)


def _wiki_call_claude(page_title: str, page_text: str, question: str,
                      summary: str = "", keywords: list | None = None):
    """
    Claude API 호출만 수행하고 답변 텍스트를 반환합니다.
    오류 시 None 반환.

    summary/keywords가 제공되면 프롬프트 앞에 배치하여
    Claude가 페이지 맥락을 빠르게 파악하도록 합니다.
    """
    import anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    MAX_PAGE_CHARS = 40000
    truncated = len(page_text) > MAX_PAGE_CHARS
    content   = page_text[:MAX_PAGE_CHARS] if truncated else page_text
    trunc_note = "\n*(내용이 길어 일부만 포함됨)*\n" if truncated else ""

    # enrichment 컨텍스트 (summary/keywords가 있으면 프롬프트 앞에 배치)
    enrich_ctx = ""
    if summary:
        enrich_ctx += f"[페이지 요약] {summary}\n"
    if keywords:
        enrich_ctx += f"[핵심 키워드] {', '.join(keywords[:8])}\n"
    if enrich_ctx:
        enrich_ctx += "\n"

    prompt = (
        f"다음은 Confluence 페이지 '{page_title}'의 내용입니다:\n\n"
        f"{enrich_ctx}"
        f"{content}{trunc_note}\n\n"
        f"위 내용을 바탕으로 아래 질문에 한국어로 간결하게 답해주세요.\n\n"
        f"[답변 지침]\n"
        f"1. 질문에 특정 연도·기간이 명시된 경우, 해당 범위의 데이터만 사용하세요. 다른 연도 데이터와 혼용하지 마세요.\n"
        f"2. 페이지에 합계가 명시되어 있으면 그 값을 인용하고, 없으면 해당 범위의 항목을 직접 세어 답하세요. '[검색 관련 섹션]'이 있으면 우선 참고하세요.\n"
        f"3. 페이지에 관련 내용이 없으면 '해당 내용을 페이지에서 찾을 수 없습니다'라고 답하세요.\n"
        f"{READ_ONLY_INSTRUCTION}"
        f"{ANSWER_FORMAT_INSTRUCTION}\n\n"
        f"질문: {question}"
    )

    try:
        client_ai = anthropic.Anthropic(api_key=api_key)
        message   = client_ai.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 1024,
            messages   = [{"role": "user", "content": prompt}],
        )
        # 토큰 사용량 로깅
        if hasattr(message, 'usage'):
            _log_token_usage("wiki", message.usage.input_tokens, message.usage.output_tokens)
        return message.content[0].text
    except Exception as e:
        logger.error(f"[wiki] Claude API 오류: {e}")
        return None


# 빈 콘텐츠 / 답변 불가 — fallback 트리거
_MIN_CONTENT_LENGTH = 20   # 이 미만이면 유의미한 콘텐츠가 아닌 것으로 판단

# Confluence 매크로 전용 페이지 감지 패턴
# body가 매크로 JSON만 포함된 경우 (예: childpages, toc 등) → 유의미한 텍스트 아님
_MACRO_ONLY_PATTERNS = [
    '{"type":"childpages"',       # 하위 페이지 목록 매크로
    '{"type":"toc"',              # 목차 매크로
    '{"type":"pagetree"',         # 페이지 트리 매크로
    '{"type":"children"',         # 자식 페이지 매크로
    '{"type":"livesearch"',       # 라이브 검색 매크로
    '{"type":"recently-updated"', # 최근 업데이트 매크로
]

_NOT_FOUND_PATTERNS = [
    "찾을 수 없습니다",
    "찾을 수 없었습니다",
    "표시되지 않",
    "내용이 없",
    "내용을 확인할 수 없",
    "텍스트가 없",
    "비어 있",
    "제공된 텍스트",
    "관련 내용이 포함되어 있지 않",
    "관련된 내용이 없",
    "no relevant content",
    "not found",
]


def _is_macro_only_content(text: str) -> bool:
    """Confluence 매크로 JSON만 포함된 콘텐츠인지 판별.

    childpages, toc 등 매크로 전용 페이지는 본문이 JSON 구조로만 되어 있어
    사람이 읽을 수 있는 텍스트 콘텐츠가 아닙니다.
    이런 페이지는 Claude에 전달해도 의미 있는 답변을 얻을 수 없으므로
    즉시 폴백(하위 페이지/MCP 실시간 조회)으로 넘깁니다.
    """
    if not text:
        return False
    stripped = text.strip()
    return any(stripped.startswith(p) for p in _MACRO_ONLY_PATTERNS)


def _is_not_found(answer: str) -> bool:
    """Claude 응답이 '콘텐츠 없음'을 의미하는지 다중 패턴으로 판별"""
    if not answer:
        return True
    lower = answer.lower()
    return any(p in lower for p in _NOT_FOUND_PATTERNS)


# ── 답변 실패(answer miss) 전용 로거 ─────────────────────────────────────
_answer_miss_logger: "logging.Logger | None" = None

def _get_answer_miss_logger() -> logging.Logger:
    global _answer_miss_logger
    if _answer_miss_logger is not None:
        return _answer_miss_logger
    _answer_miss_logger = logging.getLogger("answer_miss")
    _answer_miss_logger.setLevel(logging.WARNING)
    _answer_miss_logger.propagate = False
    bot_dir  = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(os.path.dirname(bot_dir), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(logs_dir, "answer_miss.log"), encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _answer_miss_logger.addHandler(fh)
    return _answer_miss_logger


def _log_answer_miss(*, user_id: str, user_name: str,
                     page_title: str, page_id: str,
                     question: str, fallback_stages: str,
                     level: str = "ALL_MISS"):
    """답변 실패 케이스를 기록.

    level:
        CACHE_MISS — Stage 1(캐시 적재 데이터) 실패 시점
        ALL_MISS   — 모든 fallback 단계 실패 (기본값)
    """
    ml = _get_answer_miss_logger()
    user = f"{user_name}({user_id})" if user_id else (user_name or "unknown")
    ml.warning(
        f"{level} | user={user} | page={page_title} (id={page_id}) | "
        f"question={question} | stages={fallback_stages}"
    )
    logger.warning(
        f"[wiki][answer_miss] {level} | page={page_title} | "
        f"question={question}"
    )


def _wiki_ask_claude(page_title: str, page_text: str, page_url: str,
                     question: str, respond, wiki_client=None,
                     display_question: str = "",
                     page_summary: str = "", page_keywords: list | None = None):
    """
    Claude API 를 사용해 페이지 내용 기반으로 질문에 답변.

    wiki_client 가 전달되면 '찾을 수 없습니다' 응답 시 하위 페이지를
    자동 검색하여 재질의합니다 (MCP fallback).

    page_summary/page_keywords: enrichment 데이터 (있으면 프롬프트에 포함)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        respond(text=(
            "❌ `ANTHROPIC_API_KEY` 환경변수가 설정되지 않았습니다.\n"
            "Railway 환경변수에 Anthropic API 키를 추가하세요."
        ))
        return

    answer = _wiki_call_claude(page_title, page_text, question,
                               summary=page_summary,
                               keywords=page_keywords)
    if answer is None:
        respond(text="❌ Claude API 호출에 실패했습니다.")
        return

    source_label = page_title
    source_url   = page_url

    # ── MCP Fallback: 답변 불가 감지 → 하위 페이지 자동 검색 ──
    if _is_not_found(answer) and wiki_client:
        # page_text에서 page_id를 직접 알 수 없으므로 외부에서 전달
        # → _wiki_ask_claude_with_fallback 에서 처리
        pass  # fallback은 handler에서 처리

    respond(text=format_ai_response(
        question=question,
        raw_answer=answer,
        source_type="wiki",
        source_label=source_label,
        source_url=source_url,
        display_question=display_question,
    ))


def _wiki_search_pages(client, query: str, respond):
    """키워드로 페이지 검색 결과 표시"""
    pages, err = client.search_pages(query)
    if err:
        respond(text=f"❌ 검색 실패\n```\n{err}\n```")
        return
    if not pages:
        respond(text=f"ℹ️ `{query}` 에 해당하는 페이지가 없습니다.")
        return

    lines = [f"*🔍 '{query}' 검색 결과 ({len(pages)}건)*\n"]
    for p in pages:
        lines.append(f"• *{p['title']}*  →  `/wiki {p['title']}`")
    respond(text="\n".join(lines))


# ── /gdi (GDI 문서 조회) 헬퍼 ─────────────────────────────────────────────────

def _breadcrumb_to_path(breadcrumb: str) -> str:
    """
    GDI 웹 UI 스타일의 경로를 슬래시 경로로 변환합니다.
    'Chaoszero > Test Result > 260204 > 3-9차'
    → 'Chaoszero/Test Result/260204/3-9차/'
    '루트 > Chaoszero > ...' 형태에서 '루트'는 자동 제거됩니다.
    """
    parts = [p.strip() for p in breadcrumb.split(">") if p.strip()]
    if parts and parts[0] == "루트":
        parts = parts[1:]
    return "/".join(parts) + "/" if parts else ""


def _has_breadcrumb(text: str) -> bool:
    """텍스트에 '>' 구분자가 있으면 폴더 경로로 판단합니다."""
    return ">" in text


def _fetch_file_content(client, file_name: str) -> str:
    """
    파일명으로 내용(청크 텍스트)을 가져옵니다.
    5단계 폴백: 캐시전체 → exact_match → text_match → #접두사 제거 → unified_search
    Returns: 내용 텍스트 (없으면 빈 문자열)
    """
    import re as _re

    # 0) SQLite 캐시 전체 텍스트 우선 (일괄 적재 데이터)
    full_text = gc.get_file_content_full(file_name)
    if full_text:
        return full_text

    # 1) exact match
    data, err = client.search_by_filename(file_name, exact_match=True)
    if not err:
        text = gc.get_file_content_text(data)
        if text:
            return text

    # 2) text match (exact_match=False)
    data, err = client.search_by_filename(file_name, exact_match=False)
    if not err:
        text = gc.get_file_content_text(data)
        if text:
            return text

    # 3) #숫자 접두사 제거 후 재시도
    cleaned = _re.sub(r"^#\d+\s*", "", file_name).strip()
    if cleaned and cleaned != file_name:
        data, err = client.search_by_filename(cleaned, exact_match=False)
        if not err:
            text = gc.get_file_content_text(data)
            if text:
                return text

    # 4) unified_search 폴백
    search_query = cleaned or file_name
    data, err = client.unified_search(search_query)
    if not err:
        text = gc.get_search_context_text(data)
        if text:
            return text

    return ""


def _gdi_folder_ai(client, folder_path: str, file_keyword: str,
                    question: str, respond, user_id: str, user_name: str,
                    raw_text: str):
    """
    폴더 경로 기반 AI 답변.

    1. list_files_in_folder 로 폴더 내 파일 목록 조회
    2. file_keyword 가 비어있으면 → 파일 1개일 때만 자동 선택
    3. file_keyword 가 있으면 → 파일명에 키워드 포함된 것 필터
    4. 매칭된 파일의 내용(청크)을 가져와 Claude AI 에 질문
    """
    t0 = time.time()

    # 1단계: 폴더 내 파일 목록 조회 (하위 폴더 포함 — 프리픽스 매칭)
    data, err = client.list_files_in_folder(folder_path, page_size=100)
    if err:
        gc.log_gdi_query(user_id=user_id, user_name=user_name,
                         action="folder_ai", query=raw_text, error=str(err),
                         elapsed_ms=int((time.time() - t0) * 1000))
        respond(text=f"❌ 폴더 조회 실패\n```\n{err}\n```")
        return

    files = data.get("files", [])

    # 게임명 누락 보정: 파일 없으면 알려진 게임명을 접두사로 붙여 재시도
    if not files:
        _GAME_PREFIXES = ["Chaoszero", "Epicseven"]
        first_seg = folder_path.split("/")[0] if "/" in folder_path else folder_path.rstrip("/")
        if first_seg not in _GAME_PREFIXES:
            for prefix in _GAME_PREFIXES:
                alt_path = f"{prefix}/{folder_path}"
                alt_data, alt_err = client.list_files_in_folder(alt_path, page_size=100)
                if not alt_err and alt_data and alt_data.get("files"):
                    folder_path = alt_path
                    data = alt_data
                    files = alt_data["files"]
                    logger.info(f"[gdi] 경로 보정: {folder_path}")
                    break

    if not files:
        gc.log_gdi_query(user_id=user_id, user_name=user_name,
                         action="folder_ai", query=raw_text,
                         error="폴더에 파일 없음",
                         elapsed_ms=int((time.time() - t0) * 1000))
        respond(text=f"ℹ️ `{folder_path}` 폴더에 파일이 없습니다.\n"
                f"💡 게임명을 포함한 전체 경로를 입력해보세요.\n"
                f"예: `Chaoszero > {folder_path.replace('/', ' > ').rstrip(' > ')}`")
        return

    # 날짜 기준 정렬 헬퍼 (version_date > indexed_date > 빈 문자열)
    def _file_sort_key(f):
        return f.get("version_date", "") or f.get("indexed_date", "") or ""

    # 관련도 점수 계산 헬퍼: 키워드+질문의 단어가 파일명에 몇 개 포함되는지
    _STOP = {"가장", "최근", "관련된", "관련", "내용", "요약", "요약해줘",
             "해줘", "알려줘", "보여줘", "대한", "기획서", "문서", "파일",
             "의", "을", "를", "이", "가", "에", "은", "는", "과", "와"}
    _PARTICLES = ["에서", "으로", "까지", "부터", "에게", "한테",
                  "과", "와", "의", "을", "를", "이", "가", "에",
                  "은", "는", "도", "만", "로"]

    def _strip_particle(word: str) -> str:
        """한국어 조사를 제거합니다. '훈장과' → '훈장'"""
        for p in _PARTICLES:
            if word.endswith(p) and len(word) > len(p) + 1:
                return word[:-len(p)]
        return word

    def _relevance_score(f, kw: str, q: str) -> tuple:
        """(관련도 DESC, 날짜 DESC) 정렬 키"""
        name_lower = f.get("file_name", "").lower()
        raw_terms = set((kw + " " + q).lower().split()) - _STOP
        # 조사 제거한 버전도 추가
        terms = set()
        for t in raw_terms:
            if len(t) >= 2:
                terms.add(t)
                stripped = _strip_particle(t)
                if stripped != t and len(stripped) >= 2:
                    terms.add(stripped)
        score = sum(1 for t in terms if t in name_lower)
        date = f.get("version_date", "") or f.get("indexed_date", "") or ""
        return (score, date)

    # 2단계: 대상 파일 필터링
    if file_keyword:
        kw_lower = file_keyword.lower()
        matched = [f for f in files
                   if kw_lower in f.get("file_name", "").lower()]
        if not matched:
            gc.log_gdi_query(user_id=user_id, user_name=user_name,
                             action="folder_ai", query=raw_text,
                             error=f"키워드 '{file_keyword}' 매칭 파일 없음",
                             elapsed_ms=int((time.time() - t0) * 1000))
            flist = "\n".join(f"• `{f.get('file_name', '?')}`" for f in files[:15])
            respond(text=(
                f"ℹ️ `{folder_path}` 폴더(하위 포함)에서 `{file_keyword}` 키워드에 매칭되는 파일이 없습니다.\n\n"
                f"*폴더 내 파일 목록 ({len(files)}건):*\n{flist}"
            ))
            return
    else:
        matched = files

    # 키워드 없이 여러 개 → 키워드 요청
    if not file_keyword and len(matched) > 1:
        matched.sort(key=_file_sort_key, reverse=True)
        flist = "\n".join(
            f"• `{f.get('file_name', '?')}`"
            + (f"  ({(f.get('version_date') or f.get('indexed_date', ''))[:10]})"
               if f.get('version_date') or f.get('indexed_date') else "")
            for f in matched[:15]
        )
        more = f"\n_(+{len(matched) - 15}개 더)_" if len(matched) > 15 else ""
        respond(text=(
            f"ℹ️ `{folder_path}` 폴더(하위 포함)에 파일이 {len(matched)}개 있습니다.\n"
            f"파일명 키워드를 추가해주세요.\n\n"
            f"*사용법:* `/gdi 경로 \\ 파일키워드 \\ 질문`\n\n"
            f"*폴더 내 파일 (최신순):*\n{flist}{more}"
        ))
        gc.log_gdi_query(user_id=user_id, user_name=user_name,
                         action="folder_ai", query=raw_text,
                         result=f"다수 파일 {len(matched)}건, 키워드 필요",
                         elapsed_ms=int((time.time() - t0) * 1000))
        return

    # ── 3단계: 질문 의도 감지 ──
    _LIST_KW = {"종류", "목록", "리스트", "뭐가", "어떤 파일", "몇개", "몇 개",
                "무엇이", "무엇무엇", "어떤 것", "있는지", "있나", "있어"}
    _CONTENT_KW = {"요약", "내용", "분석", "설명해", "정리해", "정리", "핵심",
                   "변경사항", "변경점", "어떻게", "뭐가 바뀌", "뭐가 달라"}
    q_lower = question.lower()
    is_list_q = any(k in q_lower for k in _LIST_KW)
    is_content_q = any(k in q_lower for k in _CONTENT_KW)
    # 둘 다 매칭되면 content 우선 (예: "어떤 내용이 있는지 요약")
    # 둘 다 없으면 content 기본
    want_content = is_content_q or not is_list_q

    # 관련도+날짜 정렬 (모든 경우에 공통)
    matched.sort(
        key=lambda f: _relevance_score(f, file_keyword, question),
        reverse=True,
    )

    # ── 4단계: 단일 파일 또는 내용 질문 → 파일 내용 가져와서 분석 ──
    if len(matched) == 1 or want_content:
        target_file = matched[0]
        target_name = target_file.get("file_name", "?")
        target_path = target_file.get("file_path", "")

        context = _fetch_file_content(client, target_name)

        if not context:
            gc.log_gdi_query(user_id=user_id, user_name=user_name,
                             action="folder_ai", query=raw_text,
                             error=f"파일 내용 없음: {target_name}",
                             elapsed_ms=int((time.time() - t0) * 1000))
            respond(text=f"ℹ️ `{target_name}` 파일의 내용을 가져올 수 없습니다.\n이 파일 형식은 아직 인덱싱되지 않았을 수 있습니다.")
            return

        source_label = f"{folder_path}{target_name}"
        _gdi_ask_claude_content(context, source_label, question, respond,
                                display_question=f"/gdi {raw_text}")
        gc.log_gdi_query(user_id=user_id, user_name=user_name,
                         action="folder_ai", query=raw_text,
                         result=f"파일: {target_name}",
                         elapsed_ms=int((time.time() - t0) * 1000))
    else:
        # ── 5단계: 목록/종류 질문 → 파일 리스트를 Claude에게 전달 ──

        file_list_lines = []
        for i, f in enumerate(matched[:30], 1):
            fname = f.get("file_name", "?")
            fpath = f.get("file_path", "")
            stype = f.get("source_type", "")
            vdate = (f.get("version_date") or f.get("indexed_date") or "")[:10]
            line = f"{i}. {fname}"
            if fpath:
                line += f"  (경로: {fpath})"
            extras = []
            if stype:
                extras.append(f"형식: {stype}")
            if vdate:
                extras.append(f"날짜: {vdate}")
            if extras:
                line += f"  [{', '.join(extras)}]"
            file_list_lines.append(line)

        file_list_context = (
            f"폴더 '{folder_path}' 에서 키워드 '{file_keyword}' 로 검색한 결과 "
            f"총 {len(matched)}개 파일이 매칭되었습니다:\n\n"
            + "\n".join(file_list_lines)
        )

        source_label = f"{folder_path} (키워드: {file_keyword})"
        _gdi_ask_claude_list(file_list_context, source_label, question, respond,
                             display_question=f"/gdi {raw_text}")
        gc.log_gdi_query(user_id=user_id, user_name=user_name,
                         action="folder_ai_list", query=raw_text,
                         result=f"매칭 {len(matched)}건, 목록 기반 답변",
                         elapsed_ms=int((time.time() - t0) * 1000))


def _gdi_help(respond):
    """도움말"""
    respond(text=(
        "*GDI 도움말*\n\n"
        "```\n"
        "/gdi 폴더명 \\ 질문                     폴더 내 파일 분석\n"
        "/gdi 폴더명 \\ '파일키워드' \\ 질문       특정 파일 찾아 분석\n"
        "/gdi help                              이 도움말\n"
        "```\n\n"
        "예시:\n"
        "• `/gdi Chaoszero Test Result \\ 테스트 결과 요약해줘`\n"
        "• `/gdi Chaoszero Update Review \\ '은하 훈장' \\ 내용 요약`"
    ))


def _gdi_search(client, query: str, respond):
    """통합 검색 결과 표시 (택소노미 우선 → MCP 폴백)"""
    # 1. 택소노미 해석 시도
    tax_data = gc.taxonomy_search(query)
    if tax_data and tax_data.get("folders"):
        respond(text=gc.format_taxonomy_results(tax_data, query))
        return

    # 2. MCP 폴백
    data, err = client.unified_search(query)
    if err:
        respond(text=f"❌ 검색 실패\n```\n{err}\n```")
        return
    respond(text=gc.format_search_results(data, query))


def _gdi_file_search(client, filename: str, respond):
    """파일명 검색 결과 표시"""
    data, err = client.search_by_filename(filename)
    if err:
        respond(text=f"❌ 파일 검색 실패\n```\n{err}\n```")
        return
    respond(text=gc.format_file_search(data, filename))


def _gdi_folder_list(client, path: str, respond):
    """폴더 목록 표시"""
    # page:N 파싱
    page = 1
    if " page:" in path:
        parts = path.rsplit(" page:", 1)
        path = parts[0].strip()
        try:
            page = int(parts[1].strip())
        except ValueError:
            pass

    data, err = client.list_files_in_folder(path, page=page)
    if err:
        respond(text=f"❌ 폴더 조회 실패\n```\n{err}\n```")
        return

    # 게임명 누락 보정
    files = data.get("files", []) if isinstance(data, dict) else []
    if not files and page == 1:
        _GAME_PREFIXES = ["Chaoszero", "Epicseven"]
        first_seg = path.split("/")[0] if "/" in path else path.rstrip("/")
        if first_seg not in _GAME_PREFIXES:
            for prefix in _GAME_PREFIXES:
                alt_path = f"{prefix}/{path}"
                alt_data, alt_err = client.list_files_in_folder(alt_path, page=page)
                if not alt_err and alt_data and isinstance(alt_data, dict) and alt_data.get("files"):
                    path = alt_path
                    data = alt_data
                    break

    respond(text=gc.format_folder_list(data, path))


def _gdi_claude_call(prompt: str, source_label: str, question: str,
                     respond, source_url: str = "",
                     display_question: str = ""):
    """Claude API 공통 호출 + 통합 포맷 응답 전송."""
    import anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        respond(text=(
            "❌ `ANTHROPIC_API_KEY` 환경변수가 설정되지 않았습니다.\n"
            "환경변수에 Anthropic API 키를 추가하세요."
        ))
        return

    _elapsed = 0
    try:
        client_ai = anthropic.Anthropic(api_key=api_key)
        t0 = time.time()
        message   = client_ai.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 1024,
            messages   = [{"role": "user", "content": prompt}],
        )
        _elapsed = int((time.time() - t0) * 1000)
        # 토큰 사용량 로깅
        if hasattr(message, 'usage'):
            _log_token_usage("gdi", message.usage.input_tokens, message.usage.output_tokens)
        answer = message.content[0].text
    except Exception as e:
        logger.error(f"[gdi] Claude API 오류: {e}")
        respond(text=f"❌ Claude API 오류\n```\n{e}\n```")
        return

    # ── OpsTracker: GDI 답변 결과 기록 ────────────────────
    try:
        _ot = _get_ops_tracker()
        if _is_not_found(answer):
            _ot.response_fail("gdi", query=question, fail_reason="not_found",
                              page_title=source_label, elapsed_ms=_elapsed)
        else:
            _ot.response_success("gdi", query=question,
                                 page_title=source_label, elapsed_ms=_elapsed)
    except Exception:
        pass

    respond(text=format_ai_response(
        question=question,
        raw_answer=answer,
        source_type="gdi",
        source_label=source_label,
        source_url=source_url,
        display_question=display_question,
    ))


def _gdi_ask_claude(context_text: str, source_label: str,
                    question: str, respond,
                    display_question: str = ""):
    """GDI 통합검색 결과를 컨텍스트로 Claude AI 에 질문 (기존 호환).

    context_text에 enrichment 데이터(요약/키워드)가 포함되어 있으면
    Claude가 문서 맥락을 더 정확하게 파악하여 답변 품질이 향상됩니다.
    """
    MAX_CHARS = 20000
    truncated = len(context_text) > MAX_CHARS
    content   = context_text[:MAX_CHARS] if truncated else context_text
    trunc_note = "\n*(내용이 길어 일부만 포함됨)*\n" if truncated else ""

    prompt = (
        f"다음은 GDI 문서 저장소에서 검색한 '{source_label}' 관련 내용입니다.\n"
        f"각 문서에는 [요약]과 [키워드]가 포함될 수 있으며, "
        f"이를 통해 문서의 핵심 맥락을 빠르게 파악할 수 있습니다.\n\n"
        f"{content}{trunc_note}\n\n"
        f"위 내용을 바탕으로 아래 질문에 한국어로 간결하게 답해주세요.\n"
        f"질문에만 집중하세요. 질문에서 요청하지 않은 정보(통계, 메타데이터 등)는 포함하지 마세요.\n"
        f"{READ_ONLY_INSTRUCTION}"
        f"{ANSWER_FORMAT_INSTRUCTION}\n\n"
        f"질문: {question}"
    )
    _gdi_claude_call(prompt, source_label, question, respond,
                     display_question=display_question)


def _gdi_ask_claude_content(context_text: str, source_label: str,
                            question: str, respond,
                            display_question: str = ""):
    """파일 내용 기반 Claude 답변 — 요약/분석/내용 질문용."""
    MAX_CHARS = 50000
    truncated = len(context_text) > MAX_CHARS
    content   = context_text[:MAX_CHARS] if truncated else context_text
    trunc_note = "\n*(내용이 길어 일부만 포함됨)*\n" if truncated else ""

    prompt = (
        f"다음은 '{source_label}' 파일의 실제 내용입니다:\n\n"
        f"{content}{trunc_note}\n\n"
        f"[답변 지침]\n"
        f"- 위 파일 내용을 바탕으로 사용자의 질문에 한국어로 답변하세요.\n"
        f"- 질문이 요약이면 핵심 내용을 간결하게 요약하세요.\n"
        f"- 질문이 분석이면 주요 포인트를 정리하세요.\n"
        f"- 파일 내용에만 집중하세요. 검색 통계, 파일 개수, 형식 정보 등 메타데이터는 포함하지 마세요.\n"
        f"- 문서에 해당 내용이 없으면 '해당 내용을 문서에서 찾을 수 없습니다'라고 답하세요.\n"
        f"{READ_ONLY_INSTRUCTION}"
        f"{ANSWER_FORMAT_INSTRUCTION}\n\n"
        f"질문: {question}"
    )
    _gdi_claude_call(prompt, source_label, question, respond,
                     display_question=display_question)


def _gdi_ask_claude_list(file_list_text: str, source_label: str,
                         question: str, respond,
                         display_question: str = ""):
    """파일 목록 기반 Claude 답변 — 종류/목록/어떤 파일 질문용."""
    prompt = (
        f"다음은 GDI 문서 저장소에서 검색한 파일 목록입니다:\n\n"
        f"{file_list_text}\n\n"
        f"[답변 지침]\n"
        f"- 사용자가 파일의 종류/목록에 대해 질문하고 있습니다.\n"
        f"- 각 파일의 이름과 경로를 중심으로 깔끔하게 정리해서 답변하세요.\n"
        f"- 비슷한 파일끼리 그룹핑하거나 카테고리로 분류하면 좋습니다.\n"
        f"- 불필요한 분석, 통계, 추측은 하지 마세요.\n"
        f"- 질문에서 요청한 것만 답변하세요.\n"
        f"{ANSWER_FORMAT_INSTRUCTION}\n\n"
        f"질문: {question}"
    )
    _gdi_claude_call(prompt, source_label, question, respond,
                     display_question=display_question)


# ── /jira 헬퍼 함수 ──────────────────────────────────────────────────────

def _jira_help(respond):
    """Jira 도움말"""
    respond(text=(
        "*JIRA 도움말*\n\n"
        "```\n"
        "/jira 프로젝트명 \\ 질문               프로젝트 이슈 검색 + AI 답변\n"
        "/jira help                           이 도움말\n"
        "```\n\n"
        "프로젝트 목록:\n"
        "• `에픽세븐` / `GEP`\n"
        "• `리젝` / `PRH`\n"
        "• `카제나` / `GCZ`\n"
        "• `QA팀` / `SMQA`\n"
        "• `로드나인` / `LDN`\n"
        "• `로드나인아시아` / `LNA`\n\n"
        "예시:\n"
        "• `/jira 에픽세븐 \\ 접속 불가 이슈 알려줘`\n"
        "• `/jira GEP \\ 최근 Compatibility 버그 요약`"
    ))


def _jira_search(client, jql: str, user_id: str, user_name: str,
                  respond):
    """JQL 검색 결과 표시"""
    t0 = time.time()
    data, err = client.search_issues(jql)
    elapsed = int((time.time() - t0) * 1000)
    if err:
        respond(text=f":x: Jira 검색 실패\n```\n{err}\n```")
        jc.log_jira_query(user_id=user_id, user_name=user_name,
                          action="search", query=jql, error=str(err),
                          elapsed_ms=elapsed)
        return
    respond(text=jc.format_search_results(data, jql))
    mirror_age = data.get("_mirror_age") if isinstance(data, dict) else None
    if mirror_age:
        respond(text=f"_(미러 기준 {mirror_age} — MCP 장애로 로컬 캐시 사용)_")
    jc.log_jira_query(user_id=user_id, user_name=user_name,
                      action="search", query=jql, result="검색 완료",
                      elapsed_ms=elapsed)


def _jira_issue(client, key: str, user_id: str, user_name: str,
                respond):
    """이슈 상세 표시"""
    t0 = time.time()
    data, err = client.get_issue(key)
    elapsed = int((time.time() - t0) * 1000)
    if err:
        respond(text=f":x: 이슈 조회 실패: `{key}`\n```\n{err}\n```")
        jc.log_jira_query(user_id=user_id, user_name=user_name,
                          action="issue", query=key, error=str(err),
                          elapsed_ms=elapsed)
        return
    respond(text=jc.format_issue(data))
    jc.log_jira_query(user_id=user_id, user_name=user_name,
                      action="issue", query=key, result="조회 완료",
                      elapsed_ms=elapsed)


def _jira_project(client, key: str, user_id: str, user_name: str,
                   respond):
    """프로젝트 상세 표시"""
    t0 = time.time()
    data, err = client.get_project(key)
    elapsed = int((time.time() - t0) * 1000)
    if err:
        respond(text=f":x: 프로젝트 조회 실패: `{key}`\n```\n{err}\n```")
        jc.log_jira_query(user_id=user_id, user_name=user_name,
                          action="project", query=key, error=str(err),
                          elapsed_ms=elapsed)
        return
    respond(text=jc.format_project(data))
    jc.log_jira_query(user_id=user_id, user_name=user_name,
                      action="project", query=key, result="조회 완료",
                      elapsed_ms=elapsed)


def _jira_projects(client, user_id: str, user_name: str, respond):
    """프로젝트 목록 표시"""
    t0 = time.time()
    data, err = client.get_all_projects()
    elapsed = int((time.time() - t0) * 1000)
    if err:
        respond(text=f":x: 프로젝트 목록 조회 실패\n```\n{err}\n```")
        jc.log_jira_query(user_id=user_id, user_name=user_name,
                          action="projects", query="all",
                          error=str(err), elapsed_ms=elapsed)
        return
    respond(text=jc.format_projects_list(data))
    jc.log_jira_query(user_id=user_id, user_name=user_name,
                      action="projects", query="all",
                      result="목록 조회 완료", elapsed_ms=elapsed)


def _jira_claude_call(prompt: str, source_label: str, question: str,
                      respond, source_url: str = "",
                      display_question: str = "", prefix: str = ""):
    """Jira 컨텍스트 기반 Claude API 호출 + 통합 포맷 응답 전송.

    prefix: intent_summary 등 Claude 응답 앞에 붙일 텍스트 (M-3 시정).
    """
    import anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        respond(text=(
            ":x: `ANTHROPIC_API_KEY` 환경변수가 설정되지 않았습니다.\n"
            "환경변수에 Anthropic API 키를 추가하세요."
        ))
        return

    _elapsed = 0
    try:
        client_ai = anthropic.Anthropic(api_key=api_key)
        t0 = time.time()
        message   = client_ai.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 1024,
            messages   = [{"role": "user", "content": prompt}],
        )
        _elapsed = int((time.time() - t0) * 1000)
        # 토큰 사용량 로깅
        if hasattr(message, 'usage'):
            _log_token_usage("jira", message.usage.input_tokens, message.usage.output_tokens)
        answer = message.content[0].text
    except Exception as e:
        logger.error(f"[jira] Claude API 오류: {e}")
        respond(text=f":x: Claude API 오류\n```\n{e}\n```")
        return

    # ── OpsTracker: Jira 답변 결과 기록 ───────────────────
    try:
        _ot = _get_ops_tracker()
        if _is_not_found(answer):
            _ot.response_fail("jira", query=question, fail_reason="not_found",
                              page_title=source_label, elapsed_ms=_elapsed)
        else:
            _ot.response_success("jira", query=question,
                                 page_title=source_label, elapsed_ms=_elapsed)
    except Exception:
        pass

    full_text = format_ai_response(
        question=question,
        raw_answer=answer,
        source_type="jira",
        source_label=source_label,
        source_url=source_url,
        display_question=display_question,
    )
    if prefix:
        full_text = prefix + "\n\n" + full_text
    respond(text=full_text)


def _jira_ask_claude(context_text: str, source_label: str,
                     question: str, respond, source_url: str = "",
                     display_question: str = "", prefix: str = ""):
    """Jira 이슈/검색 결과를 컨텍스트로 Claude AI에 질문.

    context_text에는 이슈 유형, 우선순위, 태그(라벨/컴포넌트/버전) 등
    enrichment 수준의 메타데이터가 포함되어 정확한 답변을 돕습니다.
    prefix: intent_summary 등 Claude 응답 앞에 붙일 텍스트 (M-3 시정).
    """
    MAX_CHARS = 20000
    truncated = len(context_text) > MAX_CHARS
    content   = context_text[:MAX_CHARS] if truncated else context_text
    trunc_note = "\n*(내용이 길어 일부만 포함됨)*\n" if truncated else ""

    prompt = (
        f"다음은 Jira에서 조회한 '{source_label}' 관련 이슈 정보입니다.\n"
        f"각 이슈에는 유형, 상태, 우선순위, 태그(라벨/컴포넌트/버전) 정보가 포함되어 있습니다.\n\n"
        f"{content}{trunc_note}\n\n"
        f"위 내용을 바탕으로 아래 질문에 한국어로 간결하게 답해주세요.\n"
        f"질문에만 집중하세요. 질문에서 요청하지 않은 정보(메타데이터, 통계 등)는 포함하지 마세요.\n"
        f"{READ_ONLY_INSTRUCTION}"
        f"{ANSWER_FORMAT_INSTRUCTION}\n\n"
        f"질문: {question}"
    )
    _jira_claude_call(prompt, source_label, question, respond,
                      source_url=source_url,
                      display_question=display_question, prefix=prefix)


# ── 단일 인스턴스 보장 ────────────────────────────────────────

def _ensure_single_instance(pid_file: str = "slack_bot.pid"):
    """
    PID 파일을 이용해 봇이 하나만 실행되도록 보장합니다.
    - 시작 시: 기존 PID 파일의 프로세스가 살아있으면 종료
    - 종료 시: atexit 으로 PID 파일 자동 삭제
    """
    if os.path.exists(pid_file):
        try:
            with open(pid_file) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)   # signal 0 → 프로세스 존재 여부 확인
            logger.error(
                f"이미 실행 중인 봇 프로세스가 있습니다 (PID: {old_pid}).\n"
                f"중복 실행을 원한다면 '{pid_file}' 파일을 삭제 후 재시작하세요."
            )
            sys.exit(1)
        except (ProcessLookupError, OSError):
            pass   # 프로세스 없음 → 파일만 남은 것, 덮어씀
        except ValueError:
            pass   # 파일 내용 오류 → 무시

    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            os.remove(pid_file)
        except Exception:
            pass

    atexit.register(_cleanup)
    logger.info(f"[PID] 단일 인스턴스 등록: PID {os.getpid()} → {pid_file}")


# ── 체크리스트 상태 재구성 헬퍼 ───────────────────────────────

def _normalize_title(t: str) -> str:
    """
    Slack :emoji: 코드와 실제 이모지 문자를 모두 제거한 비교용 제목 반환.

    Slack 은 chat.postMessage 에 보낸 실제 이모지 문자(📋)를
    action payload 의 message.text 에서 :clipboard: 콜론 코드로 변환해 돌려준다.
    config.json 의 title 은 실제 이모지 문자를 사용하므로 직접 비교하면 항상 불일치.
    양쪽을 정규화한 뒤 비교해야 한다.

    예)
      "📋 일일 QA 체크리스트"          → "일일 QA 체크리스트"
      ":clipboard: 일일 QA 체크리스트" → "일일 QA 체크리스트"
    """
    t = re.sub(r':[a-z_]+:', '', t)                # :colon: 이모지 코드 제거
    t = re.sub(r'[^\w\s가-힣\[\]\(\)]', '', t)      # 실제 이모지·특수문자 제거
    return t.strip()


def _reconstruct_checklist_state(body: dict, checked: list):
    """
    상태 파일이 없을 때 Slack 메시지 body + config.json 으로 상태를 재구성합니다.

    Railway(스케줄러) ↔ 로컬 PC(커맨드 핸들러)가 분리된 환경에서
    data/checklist_state.json 이 두 환경에 공유되지 않아 발생하는
    '체크 시 진행률 미반영' 문제를 해결합니다.

    Returns
    -------
    dict  : {"title": ..., "items": [...], "checked": [...], "sent_at": ...}
    None  : 재구성 실패
    """
    import json as _json

    msg        = body.get("message", {})
    msg_blocks = msg.get("blocks", [])

    # ── 1. 타이틀 ──────────────────────────────────────────────────────────
    # 우선순위 A: body["message"]["text"]
    #   → send_interactive_checklist() 에서 text=schedule["title"] 로 전송했으므로
    #     Slack action payload 에서 항상 신뢰할 수 있는 값
    # 우선순위 B: header 블록 (blocks 가 payload 에 포함됐을 때만 동작)
    title = (msg.get("text") or "").strip()
    if not title:
        for block in msg_blocks:
            if block.get("type") == "header":
                title = block["text"]["text"]
                break
    if not title:
        title = "📋 체크리스트"

    logger.info(
        f"[체크리스트 재구성] msg.text={msg.get('text')!r}  "
        f"→ title={title!r}  blocks={len(msg_blocks)}개"
    )

    # ── 2. sent_at ─────────────────────────────────────────────────────────
    # 마지막 context 블록에서 추출 ("발송: YYYY-MM-DD HH:MM  |  자동 알림")
    # 멘션 context 블록도 있으므로 reversed() 로 마지막(타임스탬프) 블록을 찾음
    sent_at = ""
    for block in reversed(msg_blocks):
        if block.get("type") == "context":
            ctx = block.get("elements", [{}])[0].get("text", "")
            m = re.search(r'발송:\s*(.+?)\s*\|', ctx)
            if m:
                sent_at = m.group(1).strip()
            break

    # ── 3. items: config.json 에서 title 로 스케줄 매칭 ────────────────────
    items: list      = []
    schedule_type: str = ""
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = _json.load(f)
        config_titles = [s.get("title") for s in cfg.get("schedules", [])]
        logger.info(f"[체크리스트 재구성] config 타이틀 목록: {config_titles}")
        title_norm = _normalize_title(title)
        logger.info(f"[체크리스트 재구성] 정규화 타이틀: {title_norm!r}")
        for sched in cfg.get("schedules", []):
            if _normalize_title(sched.get("title", "")) == title_norm:
                items         = sched.get("items", [])
                schedule_type = sched.get("type", "")
                logger.info(f"[체크리스트 재구성] config 매칭 성공: {len(items)}개 / type={schedule_type!r}")
                break
    except Exception as e:
        logger.warning(f"[체크리스트 재구성] config.json 로드 실패: {e}")

    # ── 4. config 매칭 실패 시 → message blocks 의 checkboxes.options 에서 재구성
    # 신규 구조(chk_grp_*, chk_solo_*) 및 구버전(checklist_block) 모두 처리합니다.
    # (body["actions"][0]["options"] 는 Slack payload 에 포함되지 않음 — blocks 에서 찾아야 함)
    if not items:
        for block in msg_blocks:
            if block.get("type") == "actions":
                for elem in block.get("elements", []):
                    if elem.get("type") == "checkboxes":
                        for opt in elem.get("options", []):
                            val      = opt.get("value", "")
                            opt_text = opt.get("text", {}).get("text", "")
                            mentions = re.findall(r'<@([A-Z0-9]+)>', opt_text)
                            clean    = re.sub(r'\s{2,}담당:.*$', '', opt_text).strip().strip('*')
                            items.append({"value": val, "text": clean, "mentions": mentions})
        if items:
            logger.info(f"[체크리스트 재구성] message blocks에서 재구성: {len(items)}개")

    if not items:
        logger.warning(
            f"[체크리스트 재구성] items 구성 실패 — "
            f"title={title!r}, msg.text={msg.get('text')!r}, blocks={len(msg_blocks)}개"
        )
        return None

    logger.info(
        f"[체크리스트 재구성] 완료  title={title!r}  "
        f"items={len(items)}개  checked={len(checked)}개"
    )
    return {
        "title":         title,
        "items":         items,
        "checked":       checked,
        "sent_at":       sent_at,
        "schedule_type": schedule_type,
    }


# ── Bolt App 생성 + 액션 핸들러 등록 ──────────────────────────

def create_bolt_app(bot_token: str, slack_sender: SlackSender) -> App:
    """
    Slack Bolt App 을 생성하고 인터랙션 핸들러를 등록합니다.

    Parameters
    ----------
    bot_token    : Bot Token (xoxb-...)
    slack_sender : SlackSender 인스턴스 (메시지 업데이트에 사용)
    """
    app = App(token=bot_token)

    @app.action(re.compile(r"^checklist_toggle"))
    def handle_checklist_toggle(ack, body):
        """
        사용자가 체크리스트를 체크/언체크할 때 호출됩니다.
        - ack() 로 Slack 에 즉시 응답 (3초 이내 필수)
        - 상태 파일 갱신 후 chat.update 로 메시지 동기화

        [다중 사용자 동기화 전략]
        체크박스 action_id 는 chat.update 마다 동적으로 변경됩니다 (checklist_toggle_{ts_ms}).
        따라서 핸들러는 re.compile(r"^checklist_toggle") 로 모든 버전을 수신합니다.

        merge 전략:
          1. ih.get_by_ts() — 파일 기반 권위있는 서버 상태 (conversations.history 불필요)
          2. body["message"]["blocks"] 의 options(전체 옵션 목록) 에서
             인터랙션된 block_id 의 가능한 모든 값 제거
             (initial_options 는 stale 할 수 있으나 options 목록은 항상 고정)
          3. body["state"]["values"] 의 새 선택값 추가
        """
        ack()   # Slack 에 즉시 응답

        channel = body["channel"]["id"]
        ts      = body["message"]["ts"]

        # ── Step 1: ih.get_by_ts() 에서 권위있는 서버 최신 체크 상태 로드 ──────
        # conversations.history (groups:history 권한 필요) 를 사용하지 않고
        # interaction_handler 의 파일 기반 상태를 source of truth 로 사용합니다.
        # → ih 는 toggle 마다 갱신되므로 항상 최신 서버 상태를 반영합니다.
        existing_state = ih.get_by_ts(channel, ts)
        if existing_state:
            current_checked: set = set(existing_state.get("checked", []))
            logger.debug(f"[toggle] ih 상태 로드: checked={len(current_checked)}개")
        else:
            # ih 에 없을 때: body["message"]["blocks"] 의 initial_options 에서 초기값 복원
            current_checked = set()
            for block in body.get("message", {}).get("blocks", []):
                if block.get("type") == "actions":
                    for elem in block.get("elements", []):
                        if elem.get("type") == "checkboxes":
                            for opt in elem.get("initial_options", []):
                                current_checked.add(opt["value"])
            logger.debug(f"[toggle] ih 상태 없음 → body fallback: checked={len(current_checked)}개")

        # ── Step 2: body["actions"] 의 실제 인터랙션 delta 만 적용 ──────────────
        #
        # [왜 body["state"]["values"] 를 쓰지 않는가]
        # state.values 는 메시지 내 모든 체크박스 블록의 상태를 담고 있습니다.
        # 하지만 이 값은 B의 "클라이언트 렌더 상태"에 기반하므로 stale 할 수 있습니다.
        #   → A가 5개 체크 후 B가 즉시 클릭하면, B 화면이 아직 갱신 안 된 경우
        #     state.values 의 다른 블록들은 모두 빈 selected_options 로 전달됨
        #   → Step 3+4 에서 ih 의 A의 5개가 모두 제거되고 B의 1개만 남는 버그 발생
        #
        # [body["actions"] 를 사용하는 이유]
        # actions 는 이번 인터랙션에서 실제로 변경된 블록만 포함하며 신뢰할 수 있습니다.
        # 각 action 의 selected_options 는 해당 블록의 최신 완전한 선택 상태입니다.
        #   → ih state (A의 5개) + actions delta (B가 클릭한 블록만 교체) = 정확한 merge
        for action in body.get("actions", []):
            if action.get("type") != "checkboxes":
                continue

            action_block_id = action.get("block_id", "")

            # 해당 블록의 모든 가능한 옵션값 제거 (이 블록 전체를 새 값으로 교체)
            for block in body.get("message", {}).get("blocks", []):
                if block.get("block_id", "") == action_block_id:
                    for elem in block.get("elements", []):
                        if elem.get("type") == "checkboxes":
                            for opt in elem.get("options", []):
                                current_checked.discard(opt["value"])

            # 해당 블록의 최신 선택 상태 추가
            for opt in action.get("selected_options", []):
                current_checked.add(opt["value"])

        checked: list = list(current_checked)

        logger.info(
            f"체크리스트 토글 | 채널: {channel} | ts: {ts} | "
            f"체크된 항목: {checked}"
        )

        # 상태 갱신 (상태 파일 우선 → 없으면 body + config.json 으로 재구성)
        state = ih.update_checked(channel, ts, checked)
        if state is None:
            logger.info("상태 파일 미등록 → Slack body 에서 상태 재구성 시도")
            state = _reconstruct_checklist_state(body, checked)
            if state is None:
                logger.warning("체크리스트 상태를 재구성할 수 없습니다.")
                return

        # ── 전일 누락 섹션 추출 (block_id="missed_divider" sentinel 기준) ──────
        # body["message"]["blocks"] 에서 추출합니다.
        # 누락 섹션의 options 목록은 변하지 않으므로 stale body 여도 안전합니다.
        # (initial_options 는 _rebuild_missed_blocks_checked 에서 재계산됩니다)
        body_blocks     = body.get("message", {}).get("blocks", [])
        missed_section: list = []
        in_missed       = False
        for _blk in body_blocks:
            bid = _blk.get("block_id", "")
            if bid == "missed_divider":
                in_missed = True
            if in_missed:
                if not bid.startswith("missed_"):
                    break          # footer 구분자 — 수집 종료
                missed_section.append(_blk)

        # 메시지 업데이트
        slack_sender.update_interactive_checklist(
            channel,
            ts,
            state,
            missed_section = missed_section if missed_section else None,
        )

    @app.command("/wiki")
    def handle_wiki_command(ack, respond, command, client):
        """
        /wiki help              → 도움말
        /wiki search [검색어]   → 페이지 목록 검색
        /wiki [페이지 제목]     → 페이지 내용 조회
        """
        ack()
        if message_expiry.MESSAGE_EXPIRY_ENABLED:
            respond = ExpiringResponder(
                respond, client, command.get("channel_id", "")
            )
            respond.send_initial()
        text      = (command.get("text") or "").strip()
        user_id   = command.get("user_id", "")
        user_name = command.get("user_name", "")
        wiki_client = wc.ConfluenceWikiClient()

        if not text or text.lower() == "help":
            _wiki_help(respond)
            return

        parts = text.split(None, 1)
        if parts[0].lower() == "search":
            query = parts[1].strip() if len(parts) == 2 else ""
            if query:
                t0 = time.time()
                _wiki_search_pages(wiki_client, query, respond)
                wc.log_wiki_query(
                    user_id=user_id, user_name=user_name,
                    action="search", query=query,
                    result="검색 완료",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            else:
                respond(text="❌ 검색어를 입력하세요. 예: `/wiki search QA 일정`")
            return

        # task-132 PR1-E: 자연어 직접 입력도 4단계 파이프라인 우선 진입 (wiring 결함 시정)
        # 이전: "\\" 있을 때만 4단계 진입 → C2 "/wiki 4월 27~29일 업데이트된 페이지" 같은
        # 자연어는 기존 CQL 제목 검색 fallback으로 처리되어 task-129 4단계 무용.
        # 시정: "\\" 유무 무관하게 쓰기 차단 후 4단계 시도. False(예외/cache=None) 시만 fallback.
        _np_page_part, _, _np_question = text.partition("\\") if "\\" in text else ("", "", text)
        _np_page_part = _np_page_part.strip()
        _np_question = _np_question.strip()
        if _np_question:
            _np_write_kw = detect_write_intent(_np_question)
            if _np_write_kw:
                respond(text=format_block_message(_np_write_kw))
                return
            if ip.run_wiki_intent_pipeline(
                text=text, page_part=_np_page_part, question=_np_question,
                respond=respond, cache_mgr=wc._wiki_cache,
                ask_claude_fn=_wiki_ask_claude,
            ):
                wc.log_wiki_query(
                    user_id=user_id, user_name=user_name,
                    action="ask_claude_intent", query=text,
                    result="intent_pipeline",
                )
                return

        # 4단계 False 반환 시 기존 fallback (page_part \\ question 분기)
        if "\\" in text:
            page_part, _, question = text.partition("\\")
            page_part = page_part.strip()
            question  = question.strip()
            if page_part and question:

                t0 = time.time()
                # ── 페이지별 예외처리 규칙 확인 ──────────────────────────────
                matched_rule = _find_matching_rule(page_part, question)
                strategy = matched_rule.get("strategy") if matched_rule else None

                if strategy == "get_latest_descendant":
                    # leaf 제목 추출 (경로 형식 대응)
                    if " / " in page_part:
                        leaf = page_part.split(" / ")[-1].strip()
                    elif ">" in page_part:
                        leaf = page_part.split(">")[-1].strip()
                    else:
                        leaf = page_part.strip()

                    page, err = wiki_client.get_latest_descendant(leaf)

                    # 하위 페이지가 없으면 원래 페이지 직접 조회로 폴백
                    if err:
                        logger.warning(
                            f"[wiki][규칙폴백] get_latest_descendant 실패 → "
                            f"페이지 직접 조회 폴백: {err}"
                        )
                        page, err = _wiki_fetch_page(wiki_client, page_part)
                else:
                    # 기본 동작: 질문 맥락 인식 페이지 조회
                    page, err = _wiki_fetch_page(wiki_client, page_part,
                                                 question=question)

                if err:
                    wc.log_wiki_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude", query=f"{page_part} \\ {question}",
                        error=str(err),
                        elapsed_ms=int((time.time() - t0) * 1000),
                    )
                    respond(text=f"❌ 페이지 조회 실패\n```\n{err}\n```")
                    return

                # ── Claude 1차 답변 (적재 데이터) ────────────────
                source_label = page["title"]
                source_url   = page["url"]
                fallback_stage = "cache"  # 어느 단계에서 답변 성공했는지

                page_text = (page.get("text") or "").strip()
                if (len(page_text) < _MIN_CONTENT_LENGTH
                        or _is_macro_only_content(page_text)):
                    # 빈 콘텐츠 또는 매크로 전용 → Claude 호출 없이 즉시 폴백
                    reason = (
                        f"매크로 전용 콘텐츠 ({len(page_text)}자)"
                        if _is_macro_only_content(page_text)
                        else f"빈 콘텐츠 ({len(page_text)}자 < {_MIN_CONTENT_LENGTH})"
                    )
                    logger.info(
                        f"[wiki][fallback] {reason} → "
                        f"폴백 진행: {page.get('title')}"
                    )
                    answer = "해당 페이지에서 관련 내용을 찾을 수 없습니다."
                else:
                    answer = _wiki_call_claude(
                        page["title"], page_text, question,
                        summary=page.get("summary", ""),
                        keywords=page.get("keywords"),
                    )
                    if answer is None:
                        respond(text="❌ Claude API 호출에 실패했습니다.")
                        return

                # ── Fallback 파이프라인 ──────────────────────────
                # Stage 1(적재) 실패 → Stage 2(하위) → Stage 3(MCP 실시간)
                if _is_not_found(answer):
                    # ── CACHE_MISS 로깅: 적재 데이터만으로 답변 불가 ──
                    _log_answer_miss(
                        user_id=user_id, user_name=user_name,
                        page_title=page["title"],
                        page_id=page.get("id", ""),
                        question=question,
                        fallback_stages="cache",
                        level="CACHE_MISS",
                    )
                    logger.info(
                        f"[wiki][fallback] Stage 1 미발견 → "
                        f"하위 페이지 검색 (parent={page['title']}, "
                        f"id={page.get('id', 'N/A')})"
                    )

                    # ── Stage 2: 하위 페이지 검색 ────────────────
                    if page.get("id"):
                        children, _ = wiki_client.get_descendant_pages(
                            page["id"], limit=10
                        )
                        if children:
                            combined_parts = []
                            char_budget = 35000
                            for child in children:
                                child_text = child.get("text", "")
                                if (not child_text
                                        or _is_macro_only_content(child_text)):
                                    continue
                                combined_parts.append(
                                    f"=== {child['title']} ===\n"
                                    f"{child_text}"
                                )
                                char_budget -= len(child_text)
                                if char_budget <= 0:
                                    break

                            if combined_parts:
                                fallback_title = (
                                    f"{page['title']} "
                                    f"(하위 페이지 {len(combined_parts)}건)"
                                )
                                combined_text = "\n\n".join(combined_parts)
                                logger.info(
                                    f"[wiki][fallback] Stage 2: 하위 "
                                    f"{len(combined_parts)}건 "
                                    f"{len(combined_text)}자 → Claude 재질의"
                                )
                                answer2 = _wiki_call_claude(
                                    fallback_title, combined_text, question
                                )
                                if answer2 and not _is_not_found(answer2):
                                    answer = answer2
                                    source_label = fallback_title
                                    fallback_stage = "descendant"
                                    logger.info(
                                        "[wiki][fallback] Stage 2 성공"
                                    )

                    # ── Stage 3: MCP 실시간 조회 ─────────────────
                    if _is_not_found(answer):
                        logger.info(
                            "[wiki][fallback] Stage 2 실패 → "
                            "Stage 3: MCP 실시간 조회"
                        )

                        # 3-A: 원본 페이지 MCP 실시간 재조회
                        if page.get("id"):
                            live_text, live_err = (
                                wiki_client.fetch_page_live(page["id"])
                            )
                            if live_text and live_text != page["text"]:
                                logger.info(
                                    f"[wiki][fallback] Stage 3-A: "
                                    f"실시간 본문 {len(live_text)}자 "
                                    f"(캐시와 차이 있음)"
                                )
                                answer3 = _wiki_call_claude(
                                    page["title"], live_text, question
                                )
                                if (answer3 and
                                        not _is_not_found(answer3)):
                                    answer = answer3
                                    source_label = page["title"]
                                    fallback_stage = "mcp_live"
                                    logger.info(
                                        "[wiki][fallback] Stage 3-A 성공"
                                    )

                        # 3-B: MCP 본문 검색 (질문 키워드로 전문 검색)
                        if _is_not_found(answer):
                            search_q = question[:50]
                            live_pages, _ = (
                                wiki_client.search_content_live(
                                    search_q, limit=3
                                )
                            )
                            if live_pages:
                                combined_parts = []
                                char_budget = 35000
                                for lp in live_pages:
                                    lp_text = lp.get("text", "")
                                    if not lp_text:
                                        continue
                                    combined_parts.append(
                                        f"=== {lp['title']} ===\n"
                                        f"{lp_text}"
                                    )
                                    char_budget -= len(lp_text)
                                    if char_budget <= 0:
                                        break

                                if combined_parts:
                                    live_title = (
                                        f"MCP 검색 결과 "
                                        f"({len(combined_parts)}건)"
                                    )
                                    live_combined = "\n\n".join(
                                        combined_parts
                                    )
                                    logger.info(
                                        f"[wiki][fallback] Stage 3-B: "
                                        f"MCP 본문 검색 {len(combined_parts)}"
                                        f"건 → Claude 재질의"
                                    )
                                    answer4 = _wiki_call_claude(
                                        live_title, live_combined, question
                                    )
                                    if (answer4 and
                                            not _is_not_found(answer4)):
                                        answer = answer4
                                        source_label = live_title
                                        source_url = (
                                            live_pages[0].get("url", "")
                                        )
                                        fallback_stage = "mcp_search"
                                        logger.info(
                                            "[wiki][fallback] Stage 3-B 성공"
                                        )

                    # ── 최종 실패 로깅 ────────────────────────────
                    if _is_not_found(answer):
                        fallback_stage = "all_failed"
                        _log_answer_miss(
                            user_id=user_id,
                            user_name=user_name,
                            page_title=page["title"],
                            page_id=page.get("id", ""),
                            question=question,
                            fallback_stages="cache→descendant→mcp_live→mcp_search",
                            level="ALL_MISS",
                        )

                # ── OpsTracker: 운영 지표 기록 ──────────────
                _elapsed = int((time.time() - t0) * 1000)
                try:
                    _ot = _get_ops_tracker()
                    # 캐시 이벤트
                    if fallback_stage == "cache":
                        _ot.cache_hit("wiki", query=question,
                                      detail="local_data", elapsed_ms=_elapsed)
                    elif fallback_stage in ("descendant", "mcp_live", "mcp_search"):
                        _ot.mcp_fallback("wiki", query=question,
                                         detail=fallback_stage, elapsed_ms=_elapsed)
                    elif fallback_stage == "all_failed":
                        _ot.cache_miss("wiki", query=question,
                                       detail="all_stages_failed", elapsed_ms=_elapsed)
                    # 답변 결과
                    if _is_not_found(answer):
                        _ot.response_fail(
                            "wiki", query=f"{page_part} \\ {question}",
                            fail_reason=fallback_stage,
                            page_title=source_label, elapsed_ms=_elapsed,
                            user_id=user_id,
                            channel=command.get("channel_id", ""),
                        )
                    else:
                        _ot.response_success(
                            "wiki", query=f"{page_part} \\ {question}",
                            page_title=source_label, elapsed_ms=_elapsed,
                            user_id=user_id,
                            channel=command.get("channel_id", ""),
                        )
                except Exception as _ot_err:
                    logger.debug(f"[OpsTracker] wiki 기록 실패: {_ot_err}")

                respond(text=format_ai_response(
                    question=question,
                    raw_answer=answer,
                    source_type="wiki",
                    source_label=source_label,
                    source_url=source_url,
                    display_question=f"/wiki {text}",
                ))
                wc.log_wiki_query(
                    user_id=user_id, user_name=user_name,
                    action="ask_claude", query=f"{page_part} \\ {question}",
                    result=(
                        f"페이지: {source_label} "
                        f"(fallback={fallback_stage})"
                    ),
                    elapsed_ms=_elapsed,
                )
                return

        # 나머지는 모두 경로/페이지 제목으로 처리 (내용 전체 표시)
        t0 = time.time()
        _wiki_get_page(wiki_client, text, respond)
        wc.log_wiki_query(
            user_id=user_id, user_name=user_name,
            action="get_page", query=text,
            result="조회 완료",
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    @app.command("/wiki-sync")
    def handle_wiki_sync_command(ack, respond, command):
        """
        /wiki-sync          → Delta Sync (변경분만)
        /wiki-sync full     → Full Ingest (전체 재수집)
        /wiki-sync status   → 캐시 통계
        """
        ack()
        text = (command.get("text") or "").strip().lower()

        # 캐시 모듈 확인
        if not wc._CACHE_ENABLED:
            respond(text="❌ MCP 캐시 모듈이 로드되지 않았습니다.")
            return

        if text == "status":
            stats = wc._wiki_cache.get_stats()
            lines = [
                "📊 *Wiki 캐시 현황*",
                f"• 총 노드: {stats['total_nodes']}건",
                f"• 소스별: {stats.get('by_source', {})}",
                f"• 본문 캐시: {stats['total_content']}건 ({stats['total_chars']:,}자)",
                f"• DB 크기: {stats['db_size_kb']}KB",
            ]
            if stats.get("last_sync"):
                ls = stats["last_sync"]
                lines.append(f"• 최근 동기화: {ls['sync_type']} {ls['scope']} ({ls['started_at']}) — {ls['status']}")
            else:
                lines.append("• 최근 동기화: 없음")
            respond(text="\n".join(lines))
            return

        # SyncEngine 생성
        try:
            import sys as _sys
            _sys.path.insert(0, "D:/Vibe Dev/QA Ops/mcp-cache-layer")
            from src.sync_engine import SyncEngine
            engine = SyncEngine(wc._wiki_cache, wc._get_mcp())
        except Exception as e:
            respond(text=f"❌ 동기화 엔진 로드 실패: {e}")
            return

        space_key = wc._DEFAULT_SPACE_KEY

        if text == "full":
            respond(text=f"🔄 *{space_key}* 전체 동기화 시작... (시간이 걸릴 수 있습니다)")
            result = engine.full_ingest("wiki", space_key)
        else:
            respond(text=f"🔄 *{space_key}* 변경분 동기화 시작...")
            result = engine.delta_sync("wiki", space_key)

        if "error" in result:
            respond(text=f"❌ 동기화 실패: {result['error']}")
        else:
            respond(text=(
                f"✅ *{space_key}* 동기화 완료\n"
                f"• 스캔: {result['scanned']}건\n"
                f"• 추가: {result['added']}건\n"
                f"• 갱신: {result['updated']}건\n"
                f"• 에러: {result['errors']}건\n"
                f"• 소요: {result['duration_sec']}초"
            ))

    @app.command("/gdi")
    def handle_gdi_command(ack, respond, command, client):
        r"""
        /gdi help                              → 도움말
        /gdi search [검색어]                   → 통합 검색
        /gdi file [파일명]                     → 파일명 검색
        /gdi folder [경로]                     → 폴더 내 파일 목록
        /gdi [검색어]                          → 통합 검색
        /gdi [검색어] \ [질문]                 → 검색 + AI 답변
        /gdi [폴더명] \ [파일명] \ [질문]      → 폴더+파일 지정 + AI 답변
        """
        ack()
        if message_expiry.MESSAGE_EXPIRY_ENABLED:
            respond = ExpiringResponder(
                respond, client, command.get("channel_id", "")
            )
            respond.send_initial()
        text      = (command.get("text") or "").strip()
        user_id   = command.get("user_id", "")
        user_name = command.get("user_name", "")
        gdi_client = gc.GdiClient()

        if not text or text.lower() == "help":
            _gdi_help(respond)
            return

        parts_cmd = text.split(None, 1)

        # /gdi search [검색어]
        if parts_cmd[0].lower() == "search":
            query = parts_cmd[1].strip() if len(parts_cmd) == 2 else ""
            if query:
                t0 = time.time()
                _gdi_search(gdi_client, query, respond)
                gc.log_gdi_query(
                    user_id=user_id, user_name=user_name,
                    action="search", query=query,
                    result="검색 완료",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            else:
                respond(text="❌ 검색어를 입력하세요. 예: `/gdi search 에픽세븐`")
            return

        # /gdi file [파일명]
        if parts_cmd[0].lower() == "file":
            filename = parts_cmd[1].strip() if len(parts_cmd) == 2 else ""
            if filename:
                t0 = time.time()
                _gdi_file_search(gdi_client, filename, respond)
                gc.log_gdi_query(
                    user_id=user_id, user_name=user_name,
                    action="file_search", query=filename,
                    result="검색 완료",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            else:
                respond(text="❌ 파일명을 입력하세요. 예: `/gdi file hero_balance.xlsx`")
            return

        # /gdi folder [경로]
        if parts_cmd[0].lower() == "folder":
            path = parts_cmd[1].strip() if len(parts_cmd) == 2 else ""
            if path:
                t0 = time.time()
                _gdi_folder_list(gdi_client, path, respond)
                gc.log_gdi_query(
                    user_id=user_id, user_name=user_name,
                    action="folder_list", query=path,
                    result="조회 완료",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
            else:
                respond(text="❌ 폴더 경로를 입력하세요. 예: `/gdi folder Epicseven/Update Review`")
            return

        # "\" 구분자 처리
        pipe_parts = [p.strip() for p in text.split("\\")]

        # ── 쓰기 의도 차단 (GDI 파이프 공통) ──────────────────────
        if len(pipe_parts) >= 2:
            _gdi_question = pipe_parts[-1]
            write_kw = detect_write_intent(_gdi_question)
            if write_kw:
                respond(text=format_block_message(write_kw))
                return

        # ── 폴더 경로 모드: 첫 파트에 ">" 가 있으면 폴더 경로로 판단 ──
        if len(pipe_parts) >= 2 and _has_breadcrumb(pipe_parts[0]):
            folder_path = _breadcrumb_to_path(pipe_parts[0])

            if len(pipe_parts) >= 3:
                # 3파트: 경로 \ 파일키워드 \ 질문
                # '은하 훈장' 같은 인용부호 키워드 지원
                file_keyword = pipe_parts[1].strip("''\u2018\u2019\"")
                question     = " \\ ".join(pipe_parts[2:])
            else:
                # 2파트: 경로 \ 질문 (파일 1개일 때 자동 선택)
                file_keyword = ""
                question     = pipe_parts[1]

            if folder_path and question:
                _gdi_folder_ai(gdi_client, folder_path, file_keyword,
                               question, respond, user_id, user_name, text)
                return

        # 3파트 (폴더경로 아닌 경우): 키워드 \ 파일명 \ 질문
        if len(pipe_parts) >= 3:
            search_kw   = pipe_parts[0]
            file_name   = pipe_parts[1]
            question    = " \\ ".join(pipe_parts[2:])

            if search_kw and file_name and question:
                t0 = time.time()
                data, err = gdi_client.search_by_filename(file_name)
                if err:
                    gc.log_gdi_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude", query=text, error=str(err),
                        elapsed_ms=int((time.time() - t0) * 1000),
                    )
                    respond(text=f"❌ 파일 검색 실패\n```\n{err}\n```")
                    return

                context = gc.get_file_content_text(data)
                search_data = None  # 통합검색 폴백 시 할당

                if not context:
                    logger.info(f"[gdi] 파일 내용 없음, 통합 검색 전환: {search_kw} {file_name}")
                    search_data, serr = gdi_client.unified_search(
                        f"{search_kw} {file_name}"
                    )
                    if serr:
                        gc.log_gdi_query(
                            user_id=user_id, user_name=user_name,
                            action="ask_claude", query=text, error=str(serr),
                            elapsed_ms=int((time.time() - t0) * 1000),
                        )
                        respond(text=f"❌ 검색 실패\n```\n{serr}\n```")
                        return
                    context = gc.get_search_context_text(search_data)

                if not context:
                    gc.log_gdi_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude", query=text,
                        error="검색 결과 없음",
                        elapsed_ms=int((time.time() - t0) * 1000),
                    )
                    respond(text=f"ℹ️ `{search_kw}/{file_name}` 관련 문서를 찾을 수 없습니다.")
                    return

                # 출처: 실제 파일 경로 사용
                _active_data = search_data if search_data else data
                _3p_items = (_active_data or {}).get("results", [])
                if _3p_items:
                    _3p_paths = [r.get("file_path", r.get("file_name", ""))
                                 for r in _3p_items[:3] if r]
                    _3p_folders = []
                    for p in _3p_paths:
                        parts = p.rsplit("/", 1)
                        folder = parts[0] if len(parts) > 1 else p
                        if folder and folder not in _3p_folders:
                            _3p_folders.append(folder)
                    source_label = " / ".join(_3p_folders[:3]) if _3p_folders else f"{search_kw}/{file_name}"
                else:
                    source_label = f"{search_kw}/{file_name}"
                _gdi_ask_claude(context, source_label, question, respond,
                               display_question=f"/gdi {text}")
                gc.log_gdi_query(
                    user_id=user_id, user_name=user_name,
                    action="ask_claude",
                    query=text,
                    result=f"키워드: {search_kw}\n파일: {file_name}",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
                return

        # 2파트: 검색어 \ 질문
        if len(pipe_parts) == 2:
            search_query = pipe_parts[0]
            question     = pipe_parts[1]

            if search_query and question:
                # ── task-129.5: 4단계 파이프라인 진입 시도 ──
                # M-2 시정: _has_breadcrumb 분기는 위 L2209에서 처리 (여기는 not breadcrumb 보장)
                if ip.run_gdi_intent_pipeline(
                    text=text, folder=search_query, question=question,
                    respond=respond, cache_mgr=gc._gdi_cache,
                    ask_claude_fn=_gdi_ask_claude,
                ):
                    gc.log_gdi_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude_intent", query=text,
                        result="intent_pipeline",
                    )
                    return

                t0 = time.time()

                # ── 택소노미 우선 해석 (키워드+질문 결합) ─────────
                tax_data = gc.taxonomy_search(search_query, question=question)
                if tax_data and tax_data.get("files"):
                    context = gc.get_taxonomy_context_text(tax_data)
                    if context:
                        # 출처: 실제 매칭된 폴더 경로 표시
                        _tax_folders = tax_data.get("folders", [])
                        _tax_label = " / ".join(
                            f["full_path"] for f in _tax_folders[:3]
                        ) if _tax_folders else search_query
                        _gdi_ask_claude(context, _tax_label, question, respond,
                                       display_question=f"/gdi {text}")
                        _tax_files = tax_data.get("files", [])
                        _tax_top1 = "-"
                        if _tax_files:
                            _candidate = _tax_files[0].get("title", "") or _tax_files[0].get("file_name", "")
                            if _candidate:
                                _tax_top1 = _candidate
                        gc.log_gdi_query(
                            user_id=user_id, user_name=user_name,
                            action="ask_claude", query=text,
                            result=f"택소노미: {search_query}\n파일: {_tax_top1}",
                            elapsed_ms=int((time.time() - t0) * 1000),
                            cache_status="TAXONOMY",
                        )
                        return

                # ── GDI 키워드 규칙 매칭 ─────────────────────
                from keyword_rules import match_gdi_keyword_rule
                gdi_rule = match_gdi_keyword_rule(search_query)
                if gdi_rule and gdi_rule.get("type") == "search_by_filename":
                    data, err, _cs = gdi_client.search_by_filename(
                        gdi_rule["filename_pattern"],
                        game_name=gdi_rule.get("game_name"),
                    )
                else:
                    # task-115 v1 hooks (R-2/R-3/R-4/R-5)
                    from game_aliases import detect_game_in_text
                    _game_115 = detect_game_in_text(question) or detect_game_in_text(search_query)
                    _game_name_115 = _game_115["canonical"] if _game_115 else None
                    _folder_role_115 = _detect_folder_role(question)
                    _tsv_explicit_115 = _is_tsv_explicit(search_query)
                    _order_recent_115 = _has_recent_bare(question)

                    data, err = gdi_client.unified_search(
                        search_query,
                        game_name=_game_name_115,
                        folder_role_filter=_folder_role_115,
                        tsv_explicit=_tsv_explicit_115,
                        order_by_recent=_order_recent_115,
                    )
                if err:
                    gc.log_gdi_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude", query=text, error=str(err),
                        elapsed_ms=int((time.time() - t0) * 1000),
                    )
                    respond(text=f"❌ 검색 실패\n```\n{err}\n```")
                    return

                context = gc.get_search_context_text(data)
                if not context:
                    gc.log_gdi_query(
                        user_id=user_id, user_name=user_name,
                        action="ask_claude", query=text,
                        error="검색 결과 없음",
                        elapsed_ms=int((time.time() - t0) * 1000),
                    )
                    respond(text=f"ℹ️ `{search_query}` 관련 문서를 찾을 수 없습니다.")
                    return

                # 출처: 검색 결과의 실제 파일 경로 표시
                _res_items = (data or {}).get("results", [])
                if _res_items:
                    # 파일 경로에서 공통 폴더 추출 (예: "Chaoszero/TSV/...")
                    _paths = [r.get("file_path", r.get("file_name", ""))
                              for r in _res_items[:5] if r]
                    # 경로에서 파일명 제거하고 폴더 부분만 추출
                    _folders_seen = []
                    for p in _paths:
                        parts = p.rsplit("/", 1)
                        folder = parts[0] if len(parts) > 1 else p
                        if folder and folder not in _folders_seen:
                            _folders_seen.append(folder)
                    _src_label = " / ".join(_folders_seen[:3]) if _folders_seen else search_query
                else:
                    _src_label = search_query
                _gdi_ask_claude(context, _src_label, question, respond,
                               display_question=f"/gdi {text}")
                _top1_name = (_res_items[0].get("file_name", "") if _res_items else "") or "-"
                gc.log_gdi_query(
                    user_id=user_id, user_name=user_name,
                    action="ask_claude", query=text,
                    result=f"검색어: {search_query}\n파일: {_top1_name}",
                    elapsed_ms=int((time.time() - t0) * 1000),
                )
                return

        # 나머지: 통합 검색
        t0 = time.time()
        _gdi_search(gdi_client, text, respond)
        gc.log_gdi_query(
            user_id=user_id, user_name=user_name,
            action="search", query=text,
            result="검색 완료",
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    # ── /jira 커맨드 ─────────────────────────────────────────────

    # game_aliases.py 에서 게임명→프로젝트 키 매핑 (qa팀은 게임 아닌 별도 추가)
    from game_aliases import get_jira_project_key as _ga_jira_key
    _JIRA_EXTRA_NAMES = {"qa팀": "SMQA"}  # 게임 아닌 프로젝트 (game_aliases에 없음)
    _JIRA_VALID_KEYS = {"PRH", "EP7", "GEP", "GCZ", "SMQA", "LDN", "LNA"}

    def _resolve_jira_project(text: str):
        """프로젝트명 또는 키 → 프로젝트 키. 매핑 없으면 None."""
        # 1. game_aliases 통합 매핑
        jira_key = _ga_jira_key(text)
        if jira_key:
            return jira_key
        # 2. 비게임 프로젝트 (qa팀 등)
        if text.lower() in _JIRA_EXTRA_NAMES:
            return _JIRA_EXTRA_NAMES[text.lower()]
        # 3. 직접 프로젝트 키 입력
        if text.upper() in _JIRA_VALID_KEYS:
            return text.upper()
        return None

    @app.command("/jira")
    def handle_jira_command(ack, respond, command, client):
        r"""
        /jira help              → 도움말
        /jira [검색어]          → 이슈 검색
        /jira [검색어] \ [질문] → 이슈 검색 + AI 답변
        /jira [이슈키]          → 이슈 상세
        /jira [이슈키] \ [질문] → 이슈 + AI 답변
        """
        ack()
        if message_expiry.MESSAGE_EXPIRY_ENABLED:
            respond = ExpiringResponder(
                respond, client, command.get("channel_id", "")
            )
            respond.send_initial()
        text      = (command.get("text") or "").strip()
        user_id   = command.get("user_id", "")
        user_name = command.get("user_name", "")
        jira_cli  = jc.JiraClient()

        # ── help ──
        if not text or text.lower() == "help":
            _jira_help(respond)
            return

        # ── search [검색어/JQL] ──
        if text.lower().startswith("search "):
            query = text[7:].strip()
            if not query:
                _jira_help(respond)
                return
            jql = jc.to_jql(query)
            _jira_search(jira_cli, jql, user_id, user_name, respond)
            return

        # ── issue [이슈키] ──
        if text.lower().startswith("issue "):
            key = text[6:].strip().upper()
            if not key:
                _jira_help(respond)
                return
            _jira_issue(jira_cli, key, user_id, user_name, respond)
            return

        # ── projects (목록) ──
        if text.lower() == "projects":
            _jira_projects(jira_cli, user_id, user_name, respond)
            return

        # ── project [KEY] ──
        if text.lower().startswith("project "):
            key = text[8:].strip().upper()
            if not key:
                _jira_help(respond)
                return
            _jira_project(jira_cli, key, user_id, user_name, respond)
            return

        # ── 파이프(\) 구분: [프로젝트명/이슈키/검색어] \ [질문] ──
        if "\\" in text:
            pipe_parts = [p.strip() for p in text.split("\\")]
            if len(pipe_parts) >= 2:
                target   = pipe_parts[0]
                question = pipe_parts[-1]

                # ── 쓰기 의도 차단 ────────────────────────────────
                write_kw = detect_write_intent(question)
                if write_kw:
                    respond(text=format_block_message(write_kw))
                    return

                if target and question:
                    t0 = time.time()

                    # 프로젝트명 or 프로젝트 키 → 스코프 검색
                    project_key = _resolve_jira_project(target)
                    if project_key:
                        # ── Stage 1: Intent 추출 ──────────────────────────────────────
                        intent = jc.extract_intent(question, project_key=project_key)
                        intent_summary = jc.format_intent_summary(intent, None)  # ai_failed 경우 경고 메시지

                        if intent.ai_failed:
                            # AI 실패 → 기존 rule-based fallback (회귀 없음 보장)
                            logger.info("[jira] AI Intent 실패 → rule-based fallback")
                            jql_variants = jc.question_to_jql_variants(question, project_key=project_key)
                            data, err = None, None
                            for jql in jql_variants:
                                data, err = jira_cli.search_issues(jql)
                                if err:
                                    break
                                if jc.get_search_context_text(data):
                                    break
                                logger.info(f"[jira][broadening] 0건 → 다음 JQL 시도: {jql}")
                            elapsed = int((time.time() - t0) * 1000)
                            if err:
                                respond(text=f":x: 검색 실패\n```\n{err}\n```")
                                jc.log_jira_query(user_id=user_id, user_name=user_name,
                                                  action="ask_claude_project", query=text,
                                                  error=str(err), elapsed_ms=elapsed)
                                return
                            context = jc.get_search_context_text(data)
                            if not context:
                                respond(text=f"{intent_summary}\n:information_source: *{target}* 프로젝트에서 관련 이슈를 찾을 수 없습니다.".strip())
                                return
                            mirror_age = data.get("_mirror_age") if isinstance(data, dict) else None
                            proj_url = jc._project_url(project_key)
                            _jira_ask_claude(context, target, question, respond,
                                             source_url=proj_url,
                                             display_question=f"/jira {text}",
                                             prefix=intent_summary)
                            if mirror_age:
                                respond(text=f"_(미러 기준 {mirror_age} — MCP 장애로 로컬 캐시 사용)_")
                            jc.log_jira_query(user_id=user_id, user_name=user_name,
                                              action="ask_claude_project", query=text,
                                              result=f"프로젝트: {project_key}", elapsed_ms=elapsed)

                        else:
                            # ── Stage 2+3: JQL 빌드 + 완화 사다리 ─────────────────────
                            search_result = jc.search_with_ladder(jira_cli, intent)
                            elapsed = int((time.time() - t0) * 1000)

                            if search_result.error:
                                respond(text=f":x: 검색 실패\n```\n{search_result.error}\n```")
                                jc.log_jira_query(user_id=user_id, user_name=user_name,
                                                  action="ask_claude_project", query=text,
                                                  error=search_result.error, elapsed_ms=elapsed)
                                return

                            # Stage 4: UX — intent 해석 (search_result 포함하여 완화 이력 생성)
                            intent_summary = jc.format_intent_summary(intent, search_result)

                            if search_result.total_count == 0:
                                # 0건 (완화 시도 포함) — intent 해석 + 완화 이력 노출
                                respond(text=intent_summary)
                                jc.log_jira_query(user_id=user_id, user_name=user_name,
                                                  action="ask_claude_project", query=text,
                                                  result="0건", elapsed_ms=elapsed)
                                return

                            context = jc.get_search_context_text(search_result.data)
                            mirror_age = search_result.data.get("_mirror_age") if isinstance(search_result.data, dict) else None
                            proj_url = jc._project_url(project_key)
                            _jira_ask_claude(context, target, question, respond,
                                             source_url=proj_url,
                                             display_question=f"/jira {text}",
                                             prefix=intent_summary)
                            if mirror_age:
                                respond(text=f"_(미러 기준 {mirror_age} — MCP 장애로 로컬 캐시 사용)_")
                            jc.log_jira_query(user_id=user_id, user_name=user_name,
                                              action="ask_claude_project", query=text,
                                              result=f"프로젝트: {project_key}", elapsed_ms=elapsed)

                    elif jc.looks_like_issue_key(target):
                        # 이슈키 → 이슈 조회 → AI 답변
                        data, err = jira_cli.get_issue(target.upper())
                        elapsed = int((time.time() - t0) * 1000)
                        if err:
                            respond(text=f":x: 이슈 조회 실패: `{target}`\n```\n{err}\n```")
                            jc.log_jira_query(user_id=user_id, user_name=user_name,
                                              action="ask_claude_issue", query=text,
                                              error=str(err), elapsed_ms=elapsed)
                            return
                        context = jc.get_issue_context_text(data)
                        if not context:
                            respond(text=f":information_source: `{target}` 이슈 내용을 가져올 수 없습니다.")
                            return
                        _jira_ask_claude(context, target.upper(), question, respond,
                                         source_url=jc._issue_url(target.upper()),
                                         display_question=f"/jira {text}")
                        jc.log_jira_query(user_id=user_id, user_name=user_name,
                                          action="ask_claude_issue", query=text,
                                          result=f"이슈: {target.upper()}",
                                          elapsed_ms=elapsed)
                    else:
                        # 텍스트 → JQL 검색 → AI 답변
                        jql = jc.to_jql(target)
                        data, err = jira_cli.search_issues(jql)
                        elapsed = int((time.time() - t0) * 1000)
                        if err:
                            respond(text=f":x: 검색 실패\n```\n{err}\n```")
                            jc.log_jira_query(user_id=user_id, user_name=user_name,
                                              action="ask_claude_search", query=text,
                                              error=str(err), elapsed_ms=elapsed)
                            return
                        context = jc.get_search_context_text(data)
                        if not context:
                            respond(text=f":information_source: `{target}` 관련 이슈를 찾을 수 없습니다.")
                            return
                        _jira_ask_claude(context, target, question, respond,
                                        display_question=f"/jira {text}")
                        jc.log_jira_query(user_id=user_id, user_name=user_name,
                                          action="ask_claude_search", query=text,
                                          result=f"검색어: {target}",
                                          elapsed_ms=elapsed)
                    return

        # ── 이슈 키 단독 입력 → 이슈 조회 ──
        if jc.looks_like_issue_key(text):
            _jira_issue(jira_cli, text.upper(), user_id, user_name, respond)
            return

        # ── 나머지: 텍스트 → JQL 검색 ──
        jql = jc.to_jql(text)
        _jira_search(jira_cli, jql, user_id, user_name, respond)

    # ── /claim 핸들러 ──────────────────────────────────────────
    def _claim_help(respond):
        respond(text=(
            ":clipboard: */claim 사용법*\n\n"
            "`/claim [카테고리] [내용]` — 클레임 접수\n"
            "`/claim list` — 오늘 접수 목록\n"
            "`/claim list [날짜]` — 해당 날짜 접수 목록 (예: 2026-03-10)\n"
            "`/claim stats` — 오늘 카테고리별 통계\n\n"
            "*카테고리:*\n"
            ":bulb: `개선` — 기능 개선 요청\n"
            ":speech_balloon: `건의` — 건의·제안·요청\n"
            ":warning: `이슈` — 버그·오류·결함 신고\n"
            ":label: `기타` — 분류 외 (카테고리 생략 시 기본값)\n\n"
            "*예시:*\n"
            "`/claim 이슈 로그인 페이지에서 500 에러 발생`\n"
            "`/claim 개선 대시보드에 필터 기능 추가 요청`\n"
            "`/claim 건의 주간 리포트 자동 발송 기능`"
        ))

    @app.command("/claim")
    def handle_claim_command(ack, respond, command):
        ack()
        text    = (command.get("text") or "").strip()
        user_id   = command.get("user_id", "")
        user_name = command.get("user_name", "")

        if not text or text.lower() == "help":
            return _claim_help(respond)

        # ── list [날짜] ──
        if text.lower() == "list":
            claims = ch.get_claims_by_date()
            respond(text=ch.format_claim_list(claims, "오늘"))
            return

        if text.lower().startswith("list "):
            date_str = text[5:].strip()
            claims = ch.get_claims_by_date(date_str)
            respond(text=ch.format_claim_list(claims, date_str))
            return

        # ── stats ──
        if text.lower() == "stats":
            claims = ch.get_claims_by_date()
            respond(text=ch.format_claim_stats(claims))
            return

        # ── 클레임 접수 ──
        category, content = ch.parse_claim_input(text)
        if not content:
            respond(text=":x: 내용을 입력하세요. 예: `/claim 이슈 로그인 에러 발생`")
            return
        result = ch.submit_claim(user_id, user_name, category, content)
        respond(text=(
            f":white_check_mark: *클레임 접수 완료*\n\n"
            f"ID: `{result['id']}`\n"
            f"카테고리: {category}\n"
            f"내용: {content}"
        ))

    return app


# ── CLI 명령 핸들러 ────────────────────────────────────────────

def cmd_test(sender: SlackSender, channel: str):
    """지정 채널로 테스트 메시지 전송"""
    schedule = {
        "name":         "연결 테스트",
        "message_type": "checklist",
        "title":        "🧪 슬랙 봇 연결 테스트",
        "items":        [
            "Slack Bot Token 연결 확인",
            "채널 메시지 전송 확인",
            "Block Kit 렌더링 확인",
        ],
        "bot_name":  "알림 봇 테스트",
        "bot_emoji": ":bell:",
    }
    logger.info(f"테스트 메시지 전송 → 채널: {channel}")
    ok = sender.send(channel=channel, schedule=schedule)
    if ok:
        print("✅ 테스트 성공! Slack 채널을 확인하세요.")
    else:
        print("❌ 테스트 실패. 토큰과 채널 ID를 다시 확인하세요.")


def cmd_channels(sender: SlackSender):
    """접근 가능한 채널 목록 출력"""
    print("\n채널 목록 조회 중...")
    channels = sender.list_channels()
    if not channels:
        print("채널을 찾을 수 없습니다. (channels:read / groups:read 권한 확인)")
        return
    sep = "─" * 58
    print(f"\n{sep}")
    print(f"  총 {len(channels)}개 채널")
    print(sep)
    for ch in channels:
        lock = "🔒" if ch["is_private"] else "🔓"
        print(f"  {lock}  #{ch['name']:<30}  ID: {ch['id']}")
    print(sep)
    print("  → config.json 의 'channel' 필드에 ID 값을 넣으세요\n")


def cmd_send(sender: SlackSender, channel: str, message: str):
    """즉시 단일 메시지 전송"""
    ok = sender.send(
        channel  = channel,
        schedule = {
            "name":         "즉시 전송",
            "message_type": "text",
            "message":      message,
            "bot_name":     "알림 봇",
            "bot_emoji":    ":loudspeaker:",
        },
    )
    print(f"{'✅ 메시지 전송 완료' if ok else '❌ 전송 실패'} → {channel}")


def cmd_find_user(sender: SlackSender, query: str):
    """
    사용자 ID 검색 — config.json 의 mentions 필드에 넣을 U... ID 확인용
    권한: users:read
    """
    print(f"\n'{query}' 사용자 검색 중 (users:read 권한 필요)...")
    users = sender.find_users(query)
    if not users:
        print(f"  '{query}' 에 해당하는 사용자를 찾을 수 없습니다.")
        print("  (표시명·사용자명·실명 모두 일치하지 않음)")
        return
    sep = "─" * 76
    print(f"\n{sep}")
    print(f"  총 {len(users)}명 검색됨")
    print(sep)
    for u in users:
        print(
            f"  ID: {u['id']:<12}  이름: {u['real_name']:<16}  "
            f"사용자명: {u['name']:<20}  표시명: {u['display_name']}"
        )
    print(sep)
    print("  → config.json 의 'mentions' 필드에 ID(U...) 값을 넣으세요\n")


def cmd_run(sender: SlackSender, bolt_app: App, app_token: str):
    """
    스케줄러(BackgroundScheduler) 시작 후 Socket Mode 핸들러를 실행합니다.
    - 스케줄러는 백그라운드 스레드에서 실행 (논블로킹)
    - Socket Mode 핸들러는 메인 스레드를 점유 (블로킹)
    """
    _ensure_single_instance()
    # 스케줄러 백그라운드 시작
    scheduler = NotificationScheduler(sender, config_path="config.json")
    scheduler.start()   # 논블로킹

    # Socket Mode 핸들러 (블로킹 — 메인 스레드)
    logger.info("🔌 Socket Mode 연결 중... (종료: Ctrl+C)")
    handler = SocketModeHandler(bolt_app, app_token)
    try:
        handler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("봇이 정상 종료되었습니다.")
        scheduler.shutdown()


def cmd_scheduler_only(sender: SlackSender):
    """
    스케줄러만 실행합니다 — Socket Mode 없음 (Railway 전용 모드).
    Slack 메시지 전송은 HTTP API(chat.postMessage)만 사용하므로
    공용 클라우드에서도 동작합니다.
    """
    import time
    _ensure_single_instance("slack_bot_scheduler.pid")

    scheduler = NotificationScheduler(sender, config_path="config.json")
    scheduler.start()   # 논블로킹
    logger.info("📅 스케줄러 전용 모드 실행 중 — Socket Mode 없음 (Railway)")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("스케줄러가 정상 종료되었습니다.")
        scheduler.shutdown()


def _start_missed_items_timer(sender: SlackSender):
    """
    로컬 봇 전용 — 전일 누락 항목 전송 타이머 설정.

    config.json 에서 daily-qa-checklist 의 time 을 읽어
    1분 후에 누락 항목을 별도 메시지로 전송하는 일일 스케줄을 등록합니다.

    예: 체크리스트 10:00 → 누락 메시지 10:01
    """
    import json as _json
    import missed_tracker as mt
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    import pytz

    _KST = pytz.timezone("Asia/Seoul")

    # config.json 에서 대상 스케줄 조회
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = _json.load(f)
    except Exception as e:
        logger.warning(f"[missed-timer] config.json 로드 실패: {e}")
        return None

    target_schedule = None
    for s in cfg.get("schedules", []):
        if s.get("check_missed") and s.get("enabled", True):
            target_schedule = s
            break

    if not target_schedule:
        logger.info("[missed-timer] check_missed 스케줄 없음 → 타이머 미등록")
        return None

    channel     = target_schedule["channel"]
    checklist_time = target_schedule.get("time", "10:00")

    # 체크리스트 전송 시각 + 1분
    h, m = map(int, checklist_time.split(":"))
    m += 1
    if m >= 60:
        m -= 60
        h += 1

    def _send_missed():
        """전일 누락 항목을 별도 메시지로 전송"""
        try:
            missed = mt.get_missed_items_from_local_state()
            if not missed:
                logger.info("[missed-timer] 전일 누락 없음 → 메시지 미전송")
                return
            total = sum(len(g["items"]) for g in missed)
            logger.info(f"[missed-timer] 전일 누락 {total}건 발견 → 전송 시작")
            sender.send_missed_items_standalone(channel, missed)
        except Exception as e:
            logger.error(f"[missed-timer] 누락 메시지 전송 실패: {e}")

    sched = BackgroundScheduler(timezone=_KST)
    sched.add_job(
        _send_missed,
        trigger  = CronTrigger(hour=h, minute=m, day_of_week="mon-fri", timezone=_KST),
        id       = "local_missed_items",
        name     = "전일 누락 항목 전송",
    )
    sched.start()
    logger.info(f"[missed-timer] 전일 누락 타이머 등록 완료: 평일 {h:02d}:{m:02d} (체크리스트 +1분)")
    return sched


def _start_daily_recover_timer(sender: SlackSender, config_path: str):
    """
    로컬 봇 전용 — 매일 아침 누락 스케줄 자동 복구 타이머.

    봇이 며칠간 재시작 없이 살아있어도, 매일 아침 09:35 / 10:05 에
    오늘 누락된 스케줄을 감지하고 자동 재발송합니다.

    09:35: 미션 (09:00~09:30 발송) + 09:00대 일반 스케줄 복구
    10:05: 10:00대 일반 스케줄 복구 (일일 QA 체크리스트 등)
    """
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    import pytz

    _KST = pytz.timezone("Asia/Seoul")

    def _recover_job():
        try:
            ns = NotificationScheduler(sender, config_path=config_path)
            recovered = ns.recover_missed()
            if recovered:
                logger.info(f"[daily-recover] 누락 {len(recovered)}건 자동 복구 완료")
            else:
                logger.debug("[daily-recover] 누락 없음")
        except Exception as e:
            logger.error(f"[daily-recover] 복구 실패: {e}")

    sched = BackgroundScheduler(timezone=_KST)
    # 09:35 — 미션 (09:00~09:30 발송) + 09:00대 일반 스케줄 복구
    sched.add_job(
        _recover_job,
        trigger=CronTrigger(hour=9, minute=35, day_of_week="mon-fri", timezone=_KST),
        id="daily_recover_0935",
        name="누락 스케줄 복구 (09:35)",
    )
    # 10:05 — 10:00대 스케줄 복구 (일일 QA 체크리스트 등)
    sched.add_job(
        _recover_job,
        trigger=CronTrigger(hour=10, minute=5, day_of_week="mon-fri", timezone=_KST),
        id="daily_recover_1005",
        name="누락 스케줄 복구 (10:05)",
    )
    # 14:00 — 오후 최종 복구 (오전에 PC 꺼져있었을 경우 대비)
    sched.add_job(
        _recover_job,
        trigger=CronTrigger(hour=14, minute=0, day_of_week="mon-fri", timezone=_KST),
        id="daily_recover_1400",
        name="누락 스케줄 복구 (14:00)",
    )
    sched.start()
    logger.info("[daily-recover] 매일 누락 복구 타이머 등록: 평일 09:35, 10:05, 14:00")
    return sched


def cmd_commands_only(sender: SlackSender, bolt_app: App, app_token: str):
    """
    Socket Mode 핸들러만 실행합니다 — 스케줄러 없음 (로컬 PC 전용 모드).
    /wiki, /gdi 등 슬래시 커맨드를 사내망 PC에서 처리합니다.
    Railway 스케줄러와 충돌하지 않습니다.

    추가: 전일 누락 항목 전송 타이머 (체크리스트 시각 +1분, 평일만)
    추가: 시작 시 + 매일 아침 누락 스케줄 자동 복구
    """
    _ensure_single_instance()

    _config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

    # 시작 시 오늘 누락 스케줄 즉시 복구
    try:
        ns = NotificationScheduler(sender, config_path=_config_path)
        recovered = ns.recover_missed()
        if recovered:
            logger.info(f"[startup] 누락 스케줄 {len(recovered)}건 자동 복구 완료")
    except Exception as e:
        logger.warning(f"[startup] 누락 복구 중 오류 (무시): {e}")

    # 전일 누락 항목 타이머 등록
    missed_sched = _start_missed_items_timer(sender)

    # 매일 아침 누락 스케줄 자동 복구 타이머 등록
    recover_sched = _start_daily_recover_timer(sender, _config_path)

    logger.info("💬 커맨드 전용 모드 실행 중 — 스케줄러 없음 (로컬 PC)")
    logger.info("🔌 Socket Mode 연결 중... (종료: Ctrl+C)")
    handler = SocketModeHandler(bolt_app, app_token)
    try:
        handler.start()
    except (KeyboardInterrupt, SystemExit):
        if missed_sched:
            missed_sched.shutdown(wait=False)
        if recover_sched:
            recover_sched.shutdown(wait=False)
        logger.info("봇이 정상 종료되었습니다.")


# ── 메인 ──────────────────────────────────────────────────────

def main():
    load_dotenv(override=True)

    # ── 메시지 만료 설정 ──
    message_expiry.MESSAGE_EXPIRY_SECONDS = int(
        os.getenv("MESSAGE_EXPIRY_SECONDS", "600")
    )
    message_expiry.MESSAGE_EXPIRY_ENABLED = (
        os.getenv("MESSAGE_EXPIRY_ENABLED", "true").lower() == "true"
    )

    # ── 토큰 확인 ──
    bot_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    app_token = os.getenv("SLACK_APP_TOKEN", "").strip()

    if not bot_token or bot_token.startswith("xoxb-your"):
        logger.error("SLACK_BOT_TOKEN 이 설정되지 않았습니다.")
        logger.error(".env 파일에서 SLACK_BOT_TOKEN=xoxb-... 를 설정하세요.")
        logger.error("Slack API > OAuth & Permissions > Bot User OAuth Token 에서 복사하세요.")
        sys.exit(1)

    # ── 클라이언트 초기화 ──
    sender   = SlackSender(bot_token)
    bolt_app = create_bolt_app(bot_token, sender)

    # ── 연결 확인 ──
    result = sender.test_connection()
    if not result["success"]:
        logger.error(f"Slack 연결 실패: {result.get('error')}")
        logger.error("Bot Token(xoxb-)이 올바른지, 앱이 워크스페이스에 설치되었는지 확인하세요.")
        sys.exit(1)

    logger.info(
        f"✅ Slack 연결 성공  |  봇: @{result['bot']}  |  "
        f"워크스페이스: {result['team']}"
    )

    # ── CLI 인자 파싱 ──
    parser = argparse.ArgumentParser(
        description     = "Slack 알림 봇",
        formatter_class = argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--test",      metavar="CHANNEL",
        help="지정 채널로 테스트 메시지 전송 (채널 ID)",
    )
    parser.add_argument(
        "--channels",  action="store_true",
        help="접근 가능한 채널 목록 출력 (채널 ID 확인용)",
    )
    parser.add_argument(
        "--send",      nargs=2, metavar=("CHANNEL", "MESSAGE"),
        help='즉시 메시지 전송:  --send C0XXX "메시지 내용"',
    )
    parser.add_argument(
        "--find-user", metavar="NAME",
        help="사용자 ID 검색 (config.json mentions 설정용)\n예: --find-user 이동현",
    )
    parser.add_argument(
        "--scheduler-only", action="store_true",
        help="[Railway 전용] 스케줄러만 실행 — Socket Mode 없음\n"
             "공용 클라우드에서 사내망 없이 Slack 알림 전송만 담당합니다.",
    )
    parser.add_argument(
        "--commands-only", action="store_true",
        help="[로컬 PC 전용] 슬래시 커맨드만 실행 — 스케줄러 없음\n"
             "/wiki, /gdi 등 사내망 접근이 필요한 커맨드를 처리합니다.",
    )
    args = parser.parse_args()

    # ── 명령 분기 ──
    if args.test:
        cmd_test(sender, args.test)

    elif args.channels:
        cmd_channels(sender)

    elif args.send:
        cmd_send(sender, args.send[0], args.send[1])

    elif args.find_user:
        cmd_find_user(sender, args.find_user)

    elif args.scheduler_only:
        # ── Railway 전용: 스케줄러만 (Socket Mode 없음) ──
        logger.info("▶ 모드: 스케줄러 전용 (Railway)")
        cmd_scheduler_only(sender)

    elif args.commands_only:
        # ── 로컬 PC 전용: 슬래시 커맨드만 (스케줄러 없음) ──
        if not app_token or not app_token.startswith("xapp-"):
            logger.error("--commands-only 모드에는 SLACK_APP_TOKEN(xapp-...) 이 필요합니다.")
            sys.exit(1)
        logger.info("▶ 모드: 커맨드 전용 (로컬 PC)")
        cmd_commands_only(sender, bolt_app, app_token)

    else:
        # ── 풀 모드: 스케줄러 + Socket Mode (개발/테스트용) ──
        if not app_token or not app_token.startswith("xapp-"):
            logger.error("Socket Mode 실행에는 SLACK_APP_TOKEN(xapp-...) 이 필요합니다.")
            logger.error(".env 파일에서 SLACK_APP_TOKEN 을 설정하세요.")
            logger.error("(채널 목록/테스트만 사용할 경우: --channels / --test 옵션 사용)")
            sys.exit(1)
        logger.info("▶ 모드: 풀 모드 (스케줄러 + 커맨드)")
        cmd_run(sender, bolt_app, app_token)


if __name__ == "__main__":
    main()
