"""
wiki_client.py - MCP 프록시(mcp.sginfra.net)를 통해 Confluence 접근

직접 wiki.smilegate.net 호출은 SAML SSO 리다이렉트로 차단되므로,
Claude Desktop이 사용하는 것과 동일한 MCP 프록시를 이용합니다.

사용 가능한 MCP 도구:
  page_exists, get_page_by_title, get_page_by_id, get_page_id,
  get_all_pages_from_space, create_page, update_page, get_all_spaces,
  cql_search, get_page_comments, add_comment, test_confluence_connection


환경변수:
  CONFLUENCE_URL        : https://wiki.smilegate.net (페이지 URL 생성용)
  CONFLUENCE_TOKEN      : MCP 프록시 토큰 (x-confluence-wiki-token)
  CONFLUENCE_USERNAME   : MCP 사용자명 (x-confluence-wiki-username, 기본: es-wjkim)
  CONFLUENCE_SPACE_KEY  : 공간 키 (기본: QASGP)
"""

import os
import re
import json
import html as _html
import logging
import time

from mcp_core import McpSession
from game_aliases import detect_game_in_text
from search.cql_parallel import run_parallel_cql

# ── MCP 캐시 레이어 (optional — import 실패 시 캐시 없이 동작) ──────────────
_WIKI_MEM_TTL = 300  # 기본값 (config 로드 실패 시)

try:
    import sys as _sys
    _cache_path = os.environ.get("MCP_CACHE_PATH", "D:/Vibe Dev/QA Ops/mcp-cache-layer")
    if _cache_path not in _sys.path:
        _sys.path.insert(0, _cache_path)
    from src.cache_manager import CacheManager as _CacheManager
    from src.cache_logger import ops_log as _ops_log, perf as _perf
    from src import config as _cache_config
    _wiki_cache = _CacheManager()
    _WIKI_MEM_TTL = getattr(_cache_config, "WIKI_MEM_TTL_SEC", 300)
    _CACHE_ENABLED = True
except Exception:
    _wiki_cache = None
    _ops_log = None
    _perf = None
    _CACHE_ENABLED = False

# ── L1 인메모리 캐시 (gdi_client._mem_get/_mem_set/_mem_get_with_ttl 동일 패턴) ─
_WIKI_MEM_CACHE: dict = {}  # {key: (data, timestamp)}


def _mem_get(key: str):
    """L1 메모리 캐시 조회. TTL 초과 시 None."""
    entry = _WIKI_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < _WIKI_MEM_TTL:
        return entry[0]
    return None


def _mem_get_with_ttl(key: str, ttl_sec: int = None):
    """L1 메모리 캐시 조회. ttl_sec 미지정 시 기본 _WIKI_MEM_TTL 사용."""
    effective_ttl = ttl_sec if ttl_sec is not None else _WIKI_MEM_TTL
    entry = _WIKI_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < effective_ttl:
        return entry[0]
    return None


def _mem_set(key: str, data):
    """L1 메모리 캐시 저장."""
    _WIKI_MEM_CACHE[key] = (data, time.time())

logger = logging.getLogger(__name__)

# ── wiki 조회 전용 로거 (logs/wiki_query.log) ──────────────────────────────
_wiki_query_logger: "logging.Logger | None" = None

def _get_wiki_query_logger() -> logging.Logger:
    """wiki 조회 전용 로거를 반환합니다. 최초 호출 시 파일 핸들러를 설정합니다."""
    global _wiki_query_logger
    if _wiki_query_logger is not None:
        return _wiki_query_logger

    _wiki_query_logger = logging.getLogger("wiki_query")
    _wiki_query_logger.setLevel(logging.INFO)
    _wiki_query_logger.propagate = False  # 루트 로거로 전파 방지

    # logs/ 디렉토리 (프로젝트 루트 기준)
    bot_dir  = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(os.path.dirname(bot_dir), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, "wiki_query.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _wiki_query_logger.addHandler(fh)
    return _wiki_query_logger


def log_wiki_query(*, user_id: str = "", user_name: str = "",
                   action: str, query: str, result: str = "",
                   error: str = "", elapsed_ms: int = 0,
                   cache_status: str = ""):
    """
    /wiki 조회 내역을 logs/wiki_query.log 에 기록합니다.

    Parameters
    ----------
    user_id      : Slack 사용자 ID (예: U07PHCE4RCM)
    user_name    : Slack 사용자명 (예: es-wjkim)
    action       : 동작 종류 (search / get_page / ask_claude / get_latest 등)
    query        : 검색어 또는 페이지 제목
    result       : 결과 요약 (예: "페이지 발견: Game Service 1")
    error        : 에러 메시지 (없으면 빈 문자열)
    elapsed_ms   : 소요 시간 (밀리초)
    cache_status : 캐시 상태 (hit / miss / stale / disabled, 빈 문자열=미사용)
    """
    wl = _get_wiki_query_logger()
    status = "ERROR" if error else "OK"
    user   = f"{user_name}({user_id})" if user_id else (user_name or "unknown")

    msg = f"{status} | {action} | user={user} | query={query}"
    if cache_status:
        msg += f" | cache={cache_status}"
    if result:
        msg += f" | result={result}"
    if error:
        msg += f" | error={error}"
    if elapsed_ms > 0:
        msg += f" | {elapsed_ms}ms"

    if error:
        wl.error(msg)
    else:
        wl.info(msg)

MCP_URL            = "http://mcp.sginfra.net/confluence-wiki-mcp"
_DEFAULT_SPACE_KEY = "QASGP"
_DEFAULT_USERNAME  = "es-wjkim"
_DEFAULT_WIKI_URL  = "https://wiki.smilegate.net"

# ── 페이지 콘텐츠 TTL 캐시 ────────────────────────────────────────────────
_PAGE_CACHE: dict = {}   # {page_id: (plain_text, timestamp)}
_PAGE_CACHE_TTL   = 300  # 5분 (초)


def _cql_escape(value: str) -> str:
    """CQL 문자열 리터럴 escape (CWE-943 인젝션 방어).

    사용자 입력이 f'title = "{value}"' 형태로 CQL에 삽입될 때
    따옴표로 리터럴을 탈출해 쿼리 구조를 변조하는 것을 차단.
    백슬래시를 먼저 escape (escape 우회 방지) 후 따옴표 escape.
    정상 입력(따옴표 없는 제목/검색어)은 결과 불변.
    """
    if not value:
        return value
    return value.replace('\\', '\\\\').replace('"', '\\"')

# ── HTML 처리 헬퍼 ─────────────────────────────────────────────────────────

from html.parser import HTMLParser as _HTMLParser


class _ConfluenceHTMLExtractor(_HTMLParser):
    """Confluence storage 형식 HTML → 일반 텍스트 변환.

    html.parser 기반으로 구조적 파싱하여 데이터 손실을 최소화.
    - ac:parameter, ac:emoticon, script, style → 건너뜀 (노이즈)
    - Confluence 매크로 UI (차트 컨트롤, 필터 메뉴, 폼 요소) → 건너뜀
    - display:none 숨겨진 요소 → 건너뜀
    - ac:structured-macro, ac:rich-text-body 등 → 태그만 무시, 내부 텍스트 보존
    - 테이블: 셀 경계 ' | ', 행 경계 '\\n'
    - 리스트: li 항목마다 줄바꿈 + '- ' 접두사
    - 블록 요소(p, div, h1-h6, br): 줄바꿈
    """

    # 내용을 통째로 건너뛸 태그 (자식 포함)
    _SKIP_TAGS = frozenset([
        "script", "style", "ac:parameter", "ac:emoticon",
        "select", "option", "input", "button", "form", "canvas",
    ])
    # 이 CSS 클래스가 포함되면 해당 요소 전체를 건너뜀 (Confluence 매크로 UI)
    _SKIP_CLASSES = frozenset([
        # 차트 매크로 UI
        "chart-controls", "chart-menu-buttons", "aui-dropdown2",
        "aui-toolbar2", "tf-chart-message", "chart-settings",
        "tfac-menu",
        # 테이블 필터 매크로 UI
        "table-filter-menu", "table-filter-controls",
        "tableFilterCbStyle", "lockEnabled", "lockDisabled",
        "no-table-message", "waiting-for-table",
        "empty-message", "show-n-rows-only-message",
        "tf-hider-wrapper", "tf-shower-wrapper",
        "tf-body-storage",
    ])
    # 블록 요소 (앞뒤로 줄바꿈 삽입)
    _BLOCK_TAGS = frozenset([
        "p", "div", "br", "hr",
        "h1", "h2", "h3", "h4", "h5", "h6",
        "blockquote", "pre",
        "table", "thead", "tbody",
    ])

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_stack: list[str] = []  # 스킵 중인 태그 스택

    @property
    def _skipping(self) -> bool:
        return len(self._skip_stack) > 0

    def _should_skip(self, tag_lower: str, attrs: list) -> bool:
        """태그+속성 기반으로 건너뛸지 판단."""
        # 태그 자체가 스킵 대상
        if tag_lower in self._SKIP_TAGS:
            return True
        # display:none 숨겨진 요소
        attr_dict = dict(attrs)
        style = attr_dict.get("style", "")
        if "display:" in style and "none" in style:
            return True
        # Confluence 매크로 UI 클래스
        classes = set(attr_dict.get("class", "").split())
        if classes & self._SKIP_CLASSES:
            return True
        return False

    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        # 이미 스킵 중이면 깊이만 추적
        if self._skipping:
            self._skip_stack.append(tag_lower)
            return
        # 새로 스킵 진입 판단
        if self._should_skip(tag_lower, attrs):
            self._skip_stack.append(tag_lower)
            return

        if tag_lower in self._BLOCK_TAGS:
            self._parts.append("\n")
        elif tag_lower == "tr":
            self._parts.append("\n")
        elif tag_lower in ("td", "th"):
            if self._parts and not self._parts[-1].endswith("\n"):
                self._parts.append(" | ")
        elif tag_lower == "li":
            self._parts.append("\n- ")

    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if self._skipping:
            if self._skip_stack and self._skip_stack[-1] == tag_lower:
                self._skip_stack.pop()
            elif self._skip_stack:
                # 불일치 태그 — 가장 가까운 매칭 태그를 찾아 제거
                for i in range(len(self._skip_stack) - 1, -1, -1):
                    if self._skip_stack[i] == tag_lower:
                        self._skip_stack.pop(i)
                        break
            return

        if tag_lower in self._BLOCK_TAGS or tag_lower == "tr":
            self._parts.append("\n")
        else:
            # 인라인 태그 종료 시 공백 삽입 — 단어 병합 방지
            # (기존 regex 파서의 re.sub(r'<[^>]+>', ' ', text) 동작과 동일)
            self._parts.append(" ")

    def handle_data(self, data):
        if self._skipping:
            return
        self._parts.append(data)

    def unknown_decl(self, data):
        """<![CDATA[...]]> 등 비표준 선언 처리.

        Confluence storage HTML은 ac:plain-text-body, ac:plain-text-link-body
        안에 CDATA 섹션으로 코드 블록·매크로 본문·링크 텍스트를 저장한다.
        HTMLParser는 이를 unknown_decl로 전달하므로 여기서 추출.
        """
        if self._skipping:
            return
        # CDATA[ ... ] 형태에서 콘텐츠 추출
        if data.startswith("CDATA[") and data.endswith("]"):
            content = data[6:-1]  # "CDATA[" 접두사와 "]" 접미사 제거
            if content.strip():
                self._parts.append(content)
        elif data.startswith("CDATA["):
            # 닫히지 않은 CDATA (드문 경우)
            content = data[6:]
            if content.strip():
                self._parts.append(content)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        # CDATA 잔여 추출 (unknown_decl 미처리 케이스 대비)
        raw = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', raw, flags=re.DOTALL)
        # &nbsp; 등 non-breaking space 정규화
        raw = raw.replace('\xa0', ' ')
        # 공백 정규화
        raw = re.sub(r'[ \t]+', ' ', raw)
        raw = re.sub(r'\n[ \t]+', '\n', raw)
        raw = re.sub(r'\n{3,}', '\n\n', raw)
        return raw.strip()


def _strip_html(html_text: str) -> str:
    """Confluence 저장 형식 HTML → 일반 텍스트.

    html.parser 기반 구조적 파싱으로 매크로 내부 본문·테이블·리스트 데이터를 보존.
    """
    if not html_text:
        return ''
    parser = _ConfluenceHTMLExtractor()
    try:
        parser.feed(html_text)
        return parser.get_text()
    except Exception as e:
        logger.warning(f"[wiki] HTML 파서 실패, 정규식 폴백: {e}")
        # 폴백: 최소한의 정규식 처리
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_text,
                      flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = _html.unescape(text)
        return re.sub(r'\s+', ' ', text).strip()


def _clean_title(title: str) -> str:
    """검색 결과 제목에서 하이라이트 마커 제거 (@@@hl@@@...@@@endhl@@@)"""
    return re.sub(r'@@@hl@@@|@@@endhl@@@', '', title or '').strip()


# ── 싱글톤 MCP 세션 (mcp_session.McpSession 사용) ────────────────────────

_mcp_session: "McpSession | None" = None

def _get_mcp() -> McpSession:
    global _mcp_session
    if _mcp_session is None:
        _mcp_session = McpSession(
            url=MCP_URL,
            headers={
                "x-confluence-wiki-username": os.getenv("CONFLUENCE_USERNAME", _DEFAULT_USERNAME),
                "x-confluence-wiki-token"   : os.getenv("CONFLUENCE_TOKEN", ""),
            },
            label="wiki",
        )
    return _mcp_session


# ── ConfluenceWikiClient ──────────────────────────────────────────────────

class ConfluenceWikiClient:
    """
    Slack Bot에서 사용하는 Confluence 클라이언트.

    내부적으로 MCP 프록시(mcp.sginfra.net)를 통해 Confluence에 접근합니다.
    """

    def __init__(self):
        self._space_key = os.getenv("CONFLUENCE_SPACE_KEY", _DEFAULT_SPACE_KEY)
        self._wiki_url  = os.getenv("CONFLUENCE_URL", _DEFAULT_WIKI_URL).rstrip("/")
        self._mcp       = _get_mcp()

    # ── 페이지 조회 ───────────────────────────────────────────────────────

    def search_with_context(self, title: str, question: str = "",
                            space_key: str = None, fetch_full: bool = True):
        """
        질문 맥락을 활용한 스마트 페이지 검색.

        질문에서 게임명·연도·핵심 키워드를 추출하여, 해당 맥락에 맞는 페이지를
        우선 검색합니다. 실패 시 일반 get_page_by_title 로 폴백.

        검색 우선순위:
          1. 게임명 + 연도 → ancestor(게임) AND title ~ 연도 AND title ~ 키워드
          2. 게임명만      → ancestor(게임) AND title ~ 키워드
          3. 연도만        → title ~ 연도 AND title ~ 키워드
          4. 일반 검색     → get_page_by_title (제목 부분 일치)

        예: title="HotFix 내역", question="에픽세븐 2026년 핫픽스 알려줘"
          → ancestor="에픽세븐" AND title ~ "2026" AND title ~ "Hot"
          → "에픽세븐/EP | Live Service/HotFix 내역" 하위의 연도별 페이지 발견

        Parameters
        ----------
        title      : 사용자가 입력한 페이지 제목/키워드
        question   : 파이프(\\) 뒤의 질문 텍스트
        space_key  : Confluence 공간 키
        fetch_full : True → 전체 본문 조회 (AI 질의용)
        """
        sk = space_key or self._space_key

        # ── Stage -1: 제목 정확 일치 우선 검색 ─────────────────────
        # 사용자가 페이지 제목을 정확히 입력한 경우, 스마트 검색(게임/연도 추론)보다
        # 정확 일치를 우선합니다. 이렇게 해야 "2026_MGQA" 같은 정확한 페이지명이
        # 질문 속 게임명("카오스 제로")에 의해 엉뚱한 ancestor 트리로 빠지지 않습니다.
        exact_page, exact_err = self.get_page_by_title(
            title, space_key=space_key, fetch_full=fetch_full
        )
        if not exact_err and exact_page:
            exact_title = (exact_page.get("title") or "").strip()
            # 정확 일치 또는 밑줄→공백 변환 후 일치 체크
            title_normalized = title.replace("_", " ").strip().lower()
            exact_normalized = exact_title.lower()
            if title_normalized == exact_normalized or title.strip().lower() == exact_normalized:
                logger.info(
                    f"[wiki][정확일치] 제목 정확 매칭: '{exact_title}' ← '{title}'"
                )
                return exact_page, None

        # ── 질문에서 게임명 감지 ──────────────────────────────────
        game = detect_game_in_text(question) if question else None
        game_ancestor_id = None
        if game:
            game_ancestor_id = game.get("wiki_ancestor_id")
            logger.info(
                f"[wiki][스마트검색] 게임명 감지: {game['canonical']} "
                f"(ancestor_id={game_ancestor_id})"
            )

        # ── 질문/제목에서 연도 추출 ──────────────────────────────────
        year = None
        if question:
            m = re.search(r'(20\d{2})', question)
            if m:
                year = m.group(1)
        # question에서 못 찾았으면 title(page_part)에서도 추출
        if not year:
            m = re.search(r'(20\d{2})', title)
            if m:
                year = m.group(1)

        # ── 제목 키워드 분리 ──────────────────────────────────────
        raw_words = re.split(r'[\s_\-]+', title)
        keywords = []
        for w in raw_words:
            parts = re.findall(r'[A-Z][a-z]+|[a-z]+|[가-힣]+|\d+', w)
            keywords.extend(parts if parts else [w])
        keywords = [w for w in keywords if len(w) >= 2]

        # ── Stage 0: 키워드 규칙 직접 매핑 ───────────────────────
        from keyword_rules import match_wiki_keyword_rule
        game_name = game["canonical"] if game else None
        kw_rule = match_wiki_keyword_rule(question, game_canonical=game_name)
        if kw_rule:
            rule_page = kw_rule["page_title"]
            rule_cql = (
                f'title = "{_cql_escape(rule_page)}" AND space = "{_cql_escape(sk)}" AND type=page'
            )
            page = self._try_smart_cql(
                rule_cql, year or "", title, fetch_full
            )
            if page:
                return page, None
            logger.info(
                f"[wiki][규칙매칭] 규칙 페이지 '{rule_page}' CQL 미발견 "
                f"→ 기존 검색 로직 폴스루"
            )

        # ── 1차: 게임명 + 연도 + 키워드 조합 CQL ─────────────────
        if game_ancestor_id and year:
            _cqls_1 = [
                f'ancestor = {game_ancestor_id} AND '
                f'title ~ "{year}" AND title ~ "{kw}" '
                f'AND space = "{sk}" AND type=page '
                f'ORDER BY lastmodified DESC'
                for kw in keywords[:3]
            ] + [
                f'ancestor = {game_ancestor_id} AND '
                f'title ~ "{year}" AND space = "{sk}" AND type=page '
                f'ORDER BY lastmodified DESC'
            ]
            page = run_parallel_cql(
                [lambda c=c: self._try_smart_cql(c, year, title, fetch_full) for c in _cqls_1]
            )
            if page:
                return page, None

            logger.info(
                f"[wiki][스마트검색] 게임+연도 조합 미발견 → 게임 ancestor 검색"
            )

        # ── 2차: 게임명 + 키워드 (연도 없이) ─────────────────────
        if game_ancestor_id:
            _game_canonical = game["canonical"] if game else ""

            def _search_kw_2nd(kw):
                _cql = (
                    f'ancestor = {game_ancestor_id} AND '
                    f'title ~ "{kw}" '
                    f'AND space = "{sk}" AND type=page '
                    f'ORDER BY lastmodified DESC'
                )
                logger.info(f"[wiki][스마트검색] 게임+키워드 CQL: {_cql}")
                raw, err = self._mcp.call_tool(
                    "cql_search",
                    {"cql": _cql, "limit": 5, "expand": "body.view"},
                    timeout=5,
                )
                if err:
                    return None
                results = self._parse_cql_results(raw)
                if results:
                    _page = self._cql_result_to_page_dict(
                        results[0], title, fetch_full=fetch_full
                    )
                    logger.info(
                        f"[wiki][스마트검색] 게임+키워드 발견: {_page['title']} "
                        f"(game={_game_canonical}, kw={kw})"
                    )
                    return _page
                return None

            page = run_parallel_cql(
                [lambda kw=kw: _search_kw_2nd(kw) for kw in keywords[:3]]
            )
            if page:
                return page, None

            logger.info(
                f"[wiki][스마트검색] 게임 ancestor 내 미발견 → 연도 검색 폴백"
            )

        # ── 3차: 연도 + 키워드 (게임명 없이, 기존 로직) ──────────
        if year:
            _cqls_3 = [
                f'title ~ "{year}" AND title ~ "{kw}" '
                f'AND space = "{sk}" AND type=page '
                f'ORDER BY lastmodified DESC'
                for kw in keywords[:3]
            ]
            page = run_parallel_cql(
                [lambda c=c: self._try_smart_cql(c, year, title, fetch_full) for c in _cqls_3]
            )
            if page:
                return page, None

            logger.info(
                f"[wiki][스마트검색] 연도 특정 페이지 미발견 → 일반 검색 폴백 "
                f"(연도={year}, 키워드={keywords})"
            )

        # ── 4차 폴백: 일반 제목 검색 ─────────────────────────────
        page, err = self.get_page_by_title(
            title, space_key=space_key, fetch_full=fetch_full
        )
        if not err and page:
            return page, None

        # ── 5차 폴백: 로컬 캐시 본문 FTS 검색 (task-088) ─────────
        # MCP cql_search + 제목 매칭 모두 실패 시 캐시 본문에서 직접 검색.
        # Wiki 검색 fallback=all_failed 16% 회복 목적.
        fts_page = self._fts_fallback_search(title, question, sk, fetch_full)
        if fts_page:
            return fts_page, None

        # 모든 폴백 실패
        return page, err

    def _fts_fallback_search(self, title: str, question: str,
                             space_key: str, fetch_full: bool) -> "dict | None":
        """로컬 캐시 FTS 본문 검색 폴백 (task-088).

        cache_manager.search_content() MATCH로 본문 키워드 매칭. 결과 있으면
        get_page_by_id로 상세 조회. 실패 시 None 반환 (완전 실패는 호출자 처리).
        """
        if not _CACHE_ENABLED or not _wiki_cache:
            return None
        try:
            # 검색 쿼리: title + question 결합
            query_parts = [title.strip()]
            if question:
                query_parts.append(question.strip())
            query_text = " ".join(p for p in query_parts if p)
            if not query_text:
                return None

            results = _wiki_cache.search_content(
                query_text=query_text,
                source_type="wiki",
                space_key=space_key,
                limit=3,
            )
            if not results:
                return None

            best = results[0]
            logger.info(
                f"[wiki][FTS폴백] 본문 검색 매칭: '{best['title']}' "
                f"(node_id={best['node_id']}, rank={best['rank']:.2f}, query={query_text[:40]!r})"
            )

            # 제목 기반 상세 조회로 본문 확보
            if best.get("source_id"):
                page, err = self.get_page_by_id(
                    best["source_id"], fetch_full=fetch_full
                ) if hasattr(self, "get_page_by_id") else (None, "get_page_by_id 미구현")
                if not err and page:
                    page["_fallback"] = "fts_content"
                    return page

            # ID 조회 실패 → 제목으로 재시도
            page, err = self.get_page_by_title(
                best["title"], space_key=space_key, fetch_full=fetch_full
            )
            if not err and page:
                page["_fallback"] = "fts_content"
                return page

            # 캐시만 있고 MCP 실패한 경우: snippet으로 최소 정보 구성
            return {
                "title": best["title"],
                "url": best.get("url"),
                "text": best.get("snippet", ""),
                "_fallback": "fts_snippet_only",
                "_node_id": best["node_id"],
            }
        except Exception as e:
            logger.warning(f"[wiki][FTS폴백] 실행 실패: {e}")
            return None

    def _try_smart_cql(self, cql: str, year: str, title: str,
                       fetch_full: bool, timeout: int = 5) -> "dict | None":
        """스마트 CQL을 실행하여 연도가 제목에 포함된 최적 결과를 반환.

        결과 중 연도가 제목에 포함된 것을 우선 선택합니다.
        결과가 있지만 연도 매치가 없으면 첫 번째 결과를 반환.
        결과가 없으면 None.
        """
        logger.info(f"[wiki][스마트검색] CQL: {cql}")
        raw, err = self._mcp.call_tool(
            "cql_search",
            {"cql": cql, "limit": 5, "expand": "body.view"},
            timeout=timeout,
        )
        if err:
            return None
        results = self._parse_cql_results(raw)
        if not results:
            return None

        # 연도가 제목에 명시적으로 포함된 결과 우선
        best = None
        for r in results:
            r_title = _clean_title(
                r.get("title") or
                (r.get("content") or {}).get("title") or ""
            )
            if year in r_title:
                best = r
                break

        chosen = best or results[0]
        page = self._cql_result_to_page_dict(
            chosen, title, fetch_full=fetch_full
        )
        logger.info(
            f"[wiki][스마트검색] 발견: {page['title']} "
            f"(연도={year}, year_in_title={'yes' if best else 'no'})"
        )
        return page

    def get_page_by_title(self, title: str, space_key: str = None,
                          fetch_full: bool = True):
        """
        페이지 제목으로 Confluence 페이지 조회.
        (get_page_by_title MCP 도구는 서버 버그로 cql_search로 대체)

        검색 우선순위:
          1단계: 정확 일치 (title = "...")
          2단계: 부분 일치 (title ~ "...") — 결과 중 제목 유사도가 가장 높은 것 선택

        Parameters
        ----------
        fetch_full : bool
            True  → get_page_by_id 로 전체 본문 조회 (AI 질의용)
            False → cql_search 의 body.view excerpt 만 사용 (표시 전용, 빠름)

        Returns
        -------
        (page_dict | None, error_str | None)
          page_dict = {"id", "title", "url", "text"}
        """
        sk  = space_key or self._space_key

        # ── 0단계: 로컬 캐시 우선 조회 ────────────────────────────────
        # L1(인메모리) → L2(SQLite) 순서로 조회. MCP 호출 전 캐시 우선.
        _l1_key = f"title:{sk}:{title.strip().lower()}"
        _l1_hit = _mem_get(_l1_key)
        if _l1_hit is not None:
            logger.info(
                f"[wiki][L1캐시적중] 제목 인메모리 캐시: '{_l1_hit.get('title')}'"
            )
            return _l1_hit, None

        if _CACHE_ENABLED:
            cache_variants = [title.strip(), title.replace("_", " ").strip()]
            seen = set()
            for cv in cache_variants:
                if cv.lower() in seen:
                    continue
                seen.add(cv.lower())
                try:
                    node = _wiki_cache.get_node_by_title(cv, "wiki", sk)
                    if node:
                        content = _wiki_cache.get_content(node["id"])
                        if content and content.get("body_text"):
                            page_id = node["source_id"]
                            page_title = node.get("title", title)
                            page_url = node.get("url") or \
                                f"{self._wiki_url}/pages/viewpage.action?pageId={page_id}"
                            # enrichment 데이터
                            summary = content.get("summary") or ""
                            kw_raw = content.get("keywords") or ""
                            kw_list = []
                            if kw_raw:
                                try:
                                    kw_list = json.loads(kw_raw)
                                except (ValueError, TypeError):
                                    kw_list = []
                            logger.info(
                                f"[wiki][캐시적중] 로컬 캐시에서 제목 발견: "
                                f"'{page_title}' (id={page_id}, chars={content['char_count']})"
                            )
                            if _ops_log:
                                _ops_log.cache_hit(
                                    page_title, source="sqlite_title",
                                    node_id=node["id"],
                                )
                            _page_dict = {
                                "id": page_id, "title": page_title,
                                "url": page_url,
                                "text": content["body_text"],
                                "summary": summary, "keywords": kw_list,
                            }
                            # L1 캐시에도 저장 (다음 동일 제목 요청은 SQLite 우회)
                            _mem_set(_l1_key, _page_dict)
                            return _page_dict, None
                except Exception as e:
                    logger.debug(f"[wiki] 로컬 캐시 제목 조회 실패 (무시): {e}")

        # ── 1단계: MCP CQL 정확 일치 ──────────────────────────────────
        # 밑줄→공백 변환 버전도 시도 (Confluence 제목은 공백 기반)
        title_variants = [title]
        title_spaced = title.replace("_", " ").strip()
        if title_spaced != title:
            title_variants.append(title_spaced)

        for t_variant in title_variants:
            cql_exact = (f'title = "{_cql_escape(t_variant)}" AND space = "{_cql_escape(sk)}"'
                         f' AND type=page')
            logger.info(f"[wiki] 제목 정확 검색 CQL (MCP): {cql_exact}")
            raw, err = self._mcp.call_tool(
                "cql_search",
                {"cql": cql_exact, "limit": 1, "expand": "body.view"},
            )
            if not err:
                results = self._parse_cql_results(raw)
                if results:
                    logger.info(f"[wiki] MCP 정확 일치 발견: {t_variant}")
                    if _ops_log:
                        _ops_log.cache_miss(
                            t_variant, reason="title_not_in_cache",
                        )
                    return self._cql_result_to_page_dict(
                        results[0], title, fetch_full=fetch_full
                    ), None

        # ── 2단계: 부분 일치 (관련도 정렬) ─────────────────────────────
        cql_fuzzy = (f'title ~ "{_cql_escape(title)}" AND space = "{_cql_escape(sk)}"'
                     f' AND type=page ORDER BY lastmodified DESC')
        logger.info(f"[wiki] 제목 부분 검색 CQL: {cql_fuzzy} (fetch_full={fetch_full})")
        raw, err = self._mcp.call_tool(
            "cql_search",
            {"cql": cql_fuzzy, "limit": 5, "expand": "body.view"},
        )
        if err:
            return None, err

        results = self._parse_cql_results(raw)
        if not results:
            return None, f"'{title}' 페이지를 찾을 수 없습니다. (공간: {sk})"

        # 제목 유사도로 최적 결과 선택 (쿼리 단어가 제목에 많이 포함된 것 우선)
        query_words = set(re.split(r'[\s_\-]+', title.lower()))
        query_words = {w for w in query_words if len(w) >= 2}

        best, best_score = results[0], 0
        for r in results:
            r_title = _clean_title(
                r.get("title") or
                (r.get("content") or {}).get("title") or ""
            ).lower()
            score = sum(1 for w in query_words if w in r_title)
            if score > best_score:
                best, best_score = r, score

        return self._cql_result_to_page_dict(best, title,
                                             fetch_full=fetch_full), None

    def get_page_by_path(self, ancestors: list, leaf_title: str,
                         space_key: str = None, fetch_full: bool = True):
        """
        조상 페이지 경로 포함 검색.
        ancestor 조건 CQL이 파싱 실패하면 제목만으로 재시도합니다.

        Parameters
        ----------
        fetch_full : bool
            True  → get_page_by_id 로 전체 본문 조회 (AI 질의용)
            False → cql_search 의 body.view excerpt 만 사용 (표시 전용, 빠름)

        Returns
        -------
        (page_dict | None, error_str | None)
        """
        import re as _re
        sk = space_key or self._space_key

        # fetch_full=False 면 body.view 를 cql_search 에서 함께 가져옴
        expand_param = {} if fetch_full else {"expand": "body.view"}

        # CQL 파싱 오류를 일으키는 특수문자([]{}) 포함 ancestor는 제외
        _CQL_SPECIAL = _re.compile(r'[\[\]{}()\^\\]')
        safe_ancestors = [a.strip() for a in ancestors
                          if a.strip() and not _CQL_SPECIAL.search(a.strip())]
        skipped = [a.strip() for a in ancestors
                   if a.strip() and _CQL_SPECIAL.search(a.strip())]
        if skipped:
            logger.info(f"[wiki][경로검색] CQL 특수문자 포함 ancestor 제외: {skipped}")

        ancestor_conditions = " AND ".join(
            f'ancestor = "{a}"' for a in safe_ancestors
        )

        # ── 1차: (안전한) ancestor 조건 포함 CQL ─────────────────────────────
        cql = f'title ~ "{_cql_escape(leaf_title)}" AND space = "{_cql_escape(sk)}" AND type=page'
        if ancestor_conditions:
            cql += f" AND {ancestor_conditions}"
        cql += " ORDER BY lastmodified DESC"

        logger.info(f"[wiki][1차] CQL: {cql}")
        raw, err = self._mcp.call_tool(
            "cql_search", {"cql": cql, "limit": 5, **expand_param}
        )

        # ── 2차: CQL 파싱 오류 시 ancestor 없이 재시도 ───────────────────────
        if err and "cannot be parsed" in err.lower():
            logger.warning(f"[wiki][1차] CQL 파싱 오류 → 2차 폴백 (ancestor 제거)")
            cql2 = (f'title ~ "{_cql_escape(leaf_title)}" AND space = "{_cql_escape(sk)}"'
                    f' AND type=page ORDER BY lastmodified DESC')
            logger.info(f"[wiki][2차] CQL: {cql2}")
            raw, err = self._mcp.call_tool(
                "cql_search", {"cql": cql2, "limit": 5, **expand_param}
            )

        if err:
            logger.error(f"[wiki][검색실패] MCP 오류: {err}")
            return None, err

        results = self._parse_cql_results(raw)
        logger.info(f"[wiki][결과] 1~2차 검색 결과: {len(results)}건")

        # ── 3차: 결과 없을 때 → ancestor 없이 재시도 (1차가 ancestor로 실패한 경우) ──
        if not results and ancestor_conditions:
            logger.warning(f"[wiki][2차] 결과 없음 → 3차 폴백 (ancestor 완전 제거)")
            cql3 = (f'title ~ "{_cql_escape(leaf_title)}" AND space = "{_cql_escape(sk)}"'
                    f' AND type=page ORDER BY lastmodified DESC')
            logger.info(f"[wiki][3차] CQL: {cql3}")
            raw2, err2 = self._mcp.call_tool(
                "cql_search", {"cql": cql3, "limit": 5, **expand_param}
            )
            if not err2:
                results = self._parse_cql_results(raw2)
                logger.info(f"[wiki][결과] 3차 검색 결과: {len(results)}건")
            else:
                logger.error(f"[wiki][3차] MCP 오류: {err2}")

        if not results:
            path_str = " > ".join(list(ancestors) + [leaf_title])
            logger.warning(f"[wiki][최종실패] 경로 '{path_str}' — 페이지 없음")
            return None, (
                f"'{path_str}' 경로의 페이지를 찾을 수 없습니다. (공간: {sk})\n"
                f"💡 힌트: `/wiki {leaf_title}` 로 제목만 검색하거나 `/wiki search {leaf_title}` 로 유사 페이지를 확인해보세요."
            )

        return self._cql_result_to_page_dict(results[0], leaf_title,
                                             fetch_full=fetch_full), None

    def get_latest_descendant(self, page_title: str, space_key: str = None,
                              fetch_full: bool = True):
        """
        지정 페이지의 하위 페이지 중 가장 최근 생성된 페이지를 조회합니다.

        '최근/최신' 키워드 쿼리 최적화 전략:
          - 상위 페이지 전체 본문(예: 2026_MGQA) 대신 하위 페이지 1개만 가져와
            토큰 소모를 최소화합니다.
          - 상위 페이지 본문에는 월별 하위 링크가 가장 오래된 것부터 노출되어
            '가장 최근' 질문에 1월 데이터를 답변하는 오류를 방지합니다.

        Parameters
        ----------
        page_title : str
            부모/조상 페이지 제목 (예: "2026_MGQA", "에픽세븐")
        space_key  : str, optional
            Confluence 공간 키 (미지정 시 _space_key 사용)
        fetch_full : bool
            True  → get_page_by_id 로 전체 본문 조회 (AI 질의용)
            False → cql_search body.view 만 사용 (표시 전용, 빠름)

        Returns
        -------
        (page_dict | None, error_str | None)
          page_dict = {"id", "title", "url", "text"}
        """
        sk  = space_key or self._space_key
        cql = (f'ancestor = "{_cql_escape(page_title)}" AND space = "{_cql_escape(sk)}"'
               f' AND type=page ORDER BY created DESC')
        logger.info(f"[wiki][최신하위] CQL: {cql}")

        raw, err = self._mcp.call_tool(
            "cql_search",
            {"cql": cql, "limit": 1, "expand": "body.view"},
        )
        if err:
            logger.error(f"[wiki][최신하위] MCP 오류: {err}")
            return None, err

        results = self._parse_cql_results(raw)
        logger.info(f"[wiki][최신하위] 결과: {len(results)}건")

        if not results:
            return None, (
                f"'{page_title}' 의 하위 페이지를 찾을 수 없습니다. (공간: {sk})\n"
                f"💡 힌트: 페이지 제목을 정확히 입력했는지 확인하세요."
            )

        return self._cql_result_to_page_dict(
            results[0], page_title, fetch_full=fetch_full
        ), None

    def get_descendant_pages(self, page_id: str, space_key: str = None,
                             limit: int = 5, fetch_full: bool = True):
        """
        페이지 ID의 하위(descendant) 페이지 목록 조회.

        '찾을 수 없습니다' fallback 시 부모 페이지의 자식 페이지를 탐색하여
        실제 데이터가 있는 하위 페이지 내용을 가져옵니다.

        Parameters
        ----------
        page_id    : 부모 페이지의 Confluence ID
        space_key  : Confluence 공간 키
        limit      : 최대 하위 페이지 수
        fetch_full : True → 전체 본문 조회 (AI 질의용)

        Returns
        -------
        (list[page_dict], error_str | None)
        """
        sk = space_key or self._space_key
        cql = (f'ancestor = {page_id} AND space = "{sk}"'
               f' AND type=page ORDER BY lastmodified DESC')
        logger.info(f"[wiki][하위검색] CQL: {cql}")

        raw, err = self._mcp.call_tool(
            "cql_search",
            {"cql": cql, "limit": limit, "expand": "body.view"},
        )
        if err:
            logger.error(f"[wiki][하위검색] MCP 오류: {err}")
            return [], err

        results = self._parse_cql_results(raw)
        if not results:
            logger.info(f"[wiki][하위검색] 하위 페이지 없음 (parent_id={page_id})")
            return [], None

        pages = []
        for r in results:
            page = self._cql_result_to_page_dict(
                r, "", fetch_full=fetch_full
            )
            pages.append(page)

        logger.info(
            f"[wiki][하위검색] {len(pages)}건 발견 "
            f"(parent_id={page_id}, titles={[p['title'] for p in pages]})"
        )
        return pages, None

    def fetch_page_live(self, page_id: str):
        """
        MCP를 통해 Confluence 페이지를 **캐시 우회**하여 실시간 조회.

        적재된 데이터에서 답변을 찾지 못했을 때 최종 fallback으로 사용합니다.
        인메모리/SQLite 캐시를 건너뛰고 MCP → Confluence API 직접 호출.

        Returns: (plain_text | None, error_str | None)
        """
        logger.info(f"[wiki][MCP실시간] 캐시 우회 조회: id={page_id}")
        raw, err = self._mcp.call_tool(
            "get_page_by_id",
            {"page_id": page_id, "expand": "body.view"},
        )
        if err:
            logger.warning(f"[wiki][MCP실시간] 실패 (id={page_id}): {err}")
            return None, err

        data = self._parse_raw(raw)
        if not isinstance(data, dict):
            return None, "페이지 데이터 형식 오류"

        body_val  = data.get("body", {})
        html_body = (body_val.get("view", {}).get("value", "")
                     if isinstance(body_val, dict) else "")

        if html_body:
            text = _strip_html(html_body)
            logger.info(
                f"[wiki][MCP실시간] 성공: id={page_id}, "
                f"text_len={len(text)}"
            )
            return text, None

        return None, "본문(body.view)을 찾을 수 없습니다"

    def search_content_live(self, query: str, space_key: str = None,
                            limit: int = 3):
        """
        MCP CQL로 본문 내용 검색 → 페이지 목록 + 내용 반환.

        적재 데이터와 하위 페이지 모두 실패 시 최종 fallback으로,
        질문 키워드로 Confluence 전문 검색을 수행합니다.

        Returns: (list[page_dict], error_str | None)
        """
        sk = space_key or self._space_key
        cql = (f'space="{_cql_escape(sk)}" AND text ~ "{_cql_escape(query)}"'
               f' AND type=page ORDER BY lastmodified DESC')
        logger.info(f"[wiki][MCP본문검색] CQL: {cql}")

        raw, err = self._mcp.call_tool(
            "cql_search",
            {"cql": cql, "limit": limit, "expand": "body.view"},
        )
        if err:
            return [], err

        results = self._parse_cql_results(raw)
        if not results:
            return [], None

        pages = []
        for r in results:
            page = self._cql_result_to_page_dict(
                r, "", fetch_full=True
            )
            pages.append(page)

        logger.info(
            f"[wiki][MCP본문검색] {len(pages)}건 발견 "
            f"(titles={[p['title'] for p in pages]})"
        )
        return pages, None

    def search_pages(self, query: str, space_key: str = None):
        """
        텍스트 검색으로 페이지 목록 반환.

        Returns
        -------
        (list[{"id", "title"}] | None, error_str | None)
        """
        sk  = space_key or self._space_key
        cql = (f'space="{_cql_escape(sk)}" AND text ~ "{_cql_escape(query)}"'
               f' AND type=page ORDER BY lastmodified DESC')
        raw, err = self._mcp.call_tool("cql_search", {"cql": cql, "limit": 10})
        if err:
            return None, err

        results = self._parse_cql_results(raw)
        pages = []
        for r in results:
            content = r.get("content", {}) if isinstance(r, dict) else {}
            pages.append({
                "id"   : content.get("id", ""),
                "title": _clean_title(r.get("title") or content.get("title") or ""),
            })
        return pages, None

    def get_page_content(self, page_id: str) -> tuple:
        """
        페이지 ID로 전체 본문 조회 (get_page_by_id MCP 도구).
        cql_search 의 excerpt 대신 실제 페이지 전체 내용을 가져옵니다.

        캐시 순서: L1 인메모리(_PAGE_CACHE) → L2 SQLite → L3 MCP 호출

        Returns: (plain_text | None, error_str | None)
        """
        t0 = _perf.now_ms() if _perf else 0

        # ── L1: 인메모리 캐시 적중 확인 ─────────────────────────────────
        cached = _PAGE_CACHE.get(page_id)
        if cached and (time.time() - cached[1]) < _PAGE_CACHE_TTL:
            elapsed = _perf.elapsed_ms(t0) if _perf else 0
            logger.info(f"[wiki] 인메모리 캐시 적중 (TTL {_PAGE_CACHE_TTL}s): id={page_id}")
            if _ops_log:
                _ops_log.cache_hit(f"page#{page_id}", source="memory",
                                   elapsed_ms=elapsed)
            return cached[0], None

        # ── L2: SQLite 캐시 적중 확인 ───────────────────────────────────
        if _CACHE_ENABLED:
            try:
                node = _wiki_cache.get_node("wiki", page_id)
                if node and not _wiki_cache.is_stale(node["id"]):
                    content = _wiki_cache.get_content(node["id"])
                    if content and content["body_text"]:
                        _PAGE_CACHE[page_id] = (content["body_text"], time.time())
                        elapsed = _perf.elapsed_ms(t0) if _perf else 0
                        logger.info(f"[wiki] SQLite 캐시 적중: id={page_id}, chars={content['char_count']}")
                        if _ops_log:
                            _ops_log.cache_hit(
                                node.get("title", f"page#{page_id}"),
                                source="sqlite", node_id=node["id"],
                                elapsed_ms=elapsed,
                            )
                        return content["body_text"], None
            except Exception as e:
                logger.warning(f"[wiki] SQLite 캐시 조회 실패 (무시): {e}")

        # ── L3: MCP 호출 ────────────────────────────────────────────────
        mcp_t0 = _perf.now_ms() if _perf else 0
        raw, err = self._mcp.call_tool(
            "get_page_by_id",
            {"page_id": page_id, "expand": "body.view"},
        )
        mcp_elapsed = _perf.elapsed_ms(mcp_t0) if _perf else 0

        if err:
            logger.warning(f"[wiki] get_page_by_id 실패 (id={page_id}): {err}")
            if _ops_log:
                _ops_log.cache_miss(f"page#{page_id}", reason="mcp_error",
                                    elapsed_ms=_perf.elapsed_ms(t0) if _perf else 0)
            return None, err

        data = self._parse_raw(raw)
        if not isinstance(data, dict):
            return None, "페이지 데이터 형식 오류"

        body_val  = data.get("body", {})
        html_body = (body_val.get("view", {}).get("value", "")
                     if isinstance(body_val, dict) else "")

        if html_body:
            text = _strip_html(html_body)
            title = data.get("title", f"page#{page_id}")
            total_elapsed = _perf.elapsed_ms(t0) if _perf else 0

            # ── 인메모리 캐시 저장 ─────────────────────────────────────
            _PAGE_CACHE[page_id] = (text, time.time())
            logger.info(
                f"[wiki] 전체 본문 캐시 저장: id={page_id}, "
                f"html_len={len(html_body)}, text_len={len(text)}"
            )

            if _ops_log:
                _ops_log.cache_miss(title, reason="not_cached",
                                    elapsed_ms=total_elapsed)

            # ── SQLite 캐시 저장 ───────────────────────────────────────
            if _CACHE_ENABLED:
                try:
                    version_data = data.get("version", {})
                    _wiki_cache.put_page(
                        "wiki", page_id, title or f"page_{page_id}",
                        space_key=self._space_key,
                        last_modified=version_data.get("when") if isinstance(version_data, dict) else None,
                        version=version_data.get("number") if isinstance(version_data, dict) else None,
                        author=(version_data.get("by", {}).get("displayName")
                                if isinstance(version_data, dict) else None),
                        body_raw=html_body,
                        body_text=text,
                    )
                    logger.info(f"[wiki] SQLite 캐시 저장: id={page_id}, title={title}")
                except Exception as e:
                    logger.warning(f"[wiki] SQLite 캐시 저장 실패 (무시): {e}")

            return text, None

        return None, "본문(body.view)을 찾을 수 없습니다"

    # ── 내부 파싱 헬퍼 ────────────────────────────────────────────────────

    def _parse_raw(self, raw) -> object:
        """raw(str 또는 dict) → Python 객체"""
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return raw     # 순수 텍스트
        return raw

    def _parse_cql_results(self, raw) -> list:
        """cql_search 응답에서 results 목록 추출"""
        data = self._parse_raw(raw)
        if isinstance(data, dict):
            return data.get("results", [])
        elif isinstance(data, list):
            return data
        return []

    def _cql_result_to_page_dict(self, result: dict, fallback_title: str,
                                 fetch_full: bool = True) -> dict:
        """
        cql_search 결과의 단일 항목 → 표준 page_dict 변환.

        Parameters
        ----------
        fetch_full : bool
            True  → get_page_by_id 로 전체 본문 추가 조회 (AI 질의에 필요)
            False → cql_search 응답의 body.view / excerpt 만 사용 (빠른 표시 전용)

        cql_search 응답 구조:
          {
            "content": {"id": "...", "title": "...", "_links": {"webui": "/pages/..."}, "body": {...}},
            "title"  : "...",
            "excerpt": "...",
            "url"    : "/pages/...",
            ...
          }
        """
        content    = result.get("content", {}) if isinstance(result, dict) else {}
        page_id    = content.get("id", "")
        page_title = _clean_title(
            result.get("title") or content.get("title") or fallback_title
        )

        # URL 구성 (상대 경로 → 절대 경로)
        rel_url = (result.get("url")
                   or content.get("_links", {}).get("webui")
                   or (f"/pages/viewpage.action?pageId={page_id}" if page_id else ""))
        page_url = (f"{self._wiki_url}{rel_url}"
                    if rel_url.startswith("/") else
                    (rel_url or f"{self._wiki_url}/search?q={fallback_title}"))

        # excerpt 추출 (하이라이트 마커 제거 후 HTML 엔티티 디코딩)
        # @@@hl@@@...@@@endhl@@@ 패턴 제거
        excerpt_raw  = result.get("excerpt", "")
        excerpt_text = _html.unescape(
            re.sub(r'@@@\w+@@@', '', excerpt_raw)
        ).strip()

        # 전체 본문 조회: fetch_full=True 이고 page_id 있을 때만 get_page_by_id 호출
        # fetch_full=False (표시 전용) → MCP 추가 호출 없이 cql 응답 body.view 사용
        text = ""
        if fetch_full and page_id:
            full_text, ferr = self.get_page_content(page_id)
            if full_text:
                # excerpt가 있으면 관련 섹션 힌트로 전체 내용 앞에 포함
                # → Claude 가 검색에서 가장 연관된 섹션을 우선 참고할 수 있도록
                # (예: 다년도 데이터 페이지에서 특정 연도 데이터에 집중)
                if excerpt_text:
                    text = (
                        f"[검색 관련 섹션]\n{excerpt_text}\n\n"
                        f"[전체 페이지 내용]\n{full_text}"
                    )
                else:
                    text = full_text
            else:
                logger.warning(f"[wiki] 전체 본문 조회 실패, excerpt 사용: {ferr}")

        # fetch_full=False 이거나 전체 본문 조회 실패 시 → body.view 또는 excerpt 사용
        if not text:
            body_val  = content.get("body", {})
            html_body = (body_val.get("view", {}).get("value", "")
                         if isinstance(body_val, dict) else "")
            text = (_strip_html(html_body)
                    if html_body
                    else (excerpt_text or _html.unescape(excerpt_raw)))

        # ── SQLite 캐시에 메타데이터 저장 (본문 조회 여부와 무관) ──────────
        if _CACHE_ENABLED and page_id:
            try:
                version_data = content.get("version", {})
                _wiki_cache.upsert_node(
                    "wiki", page_id, page_title,
                    space_key=self._space_key, url=page_url,
                )
                node = _wiki_cache.get_node("wiki", page_id)
                if node:
                    _wiki_cache.upsert_meta(
                        node["id"],
                        last_modified=version_data.get("when") if isinstance(version_data, dict) else None,
                        version=version_data.get("number") if isinstance(version_data, dict) else None,
                    )
            except Exception as e:
                logger.debug(f"[wiki] CQL 결과 캐시 저장 실패 (무시): {e}")

        # ── enrichment 데이터 추가 (summary/keywords 캐시 조회) ────────
        summary = ""
        keywords_list = []
        if _CACHE_ENABLED and page_id:
            try:
                _node = _wiki_cache.get_node("wiki", page_id)
                if _node:
                    _content = _wiki_cache.get_content(_node["id"])
                    if _content:
                        summary = _content.get("summary") or ""
                        _kw_raw = _content.get("keywords") or ""
                        if _kw_raw:
                            import json as _json
                            try:
                                keywords_list = _json.loads(_kw_raw)
                            except (ValueError, TypeError):
                                keywords_list = []
            except Exception:
                pass

        logger.debug(f"[wiki] 페이지 파싱: id={page_id}, title={page_title}, "
                     f"fetch_full={fetch_full}, text_len={len(str(text))}")
        return {
            "id": page_id, "title": page_title, "url": page_url, "text": text,
            "summary": summary, "keywords": keywords_list,
        }

