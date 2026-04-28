"""
gdi_client.py - GDI(Game Doc Insight) MCP 클라이언트

MCP 프록시(mcp-dev.sginfra.net)를 통해 GDI 문서 저장소에 접근합니다.
wiki_client.py 와 동일한 패턴으로, mcp_session.McpSession 을 공유합니다.

캐시 계층 (Phase 2):
  L1: 인메모리 dict (_GDI_MEM_CACHE) — 5분 TTL
  L2: SQLite (mcp-cache-layer) — 폴더 6시간, 파일 24시간 TTL
  L3: MCP HTTP 호출 (폴백)

사용 가능한 GDI MCP 도구 (읽기 전용):
  unified_search, search_by_filename, list_files_in_folder,
  fetch_row_documents, search_vector_candidates,
  get_document_version_diff, compare_two_files, compare_folders, help

⚠️ GDI MCP 안전 원칙:
  - MCP 경유: 읽기(검색/조회) 전용 — 쓰기/업로드/삭제 원천 차단
  - MCP는 공유 서버이므로 데이터 변경 경로 완전 차단
  - S3 원본 데이터 변경은 로컬 AWS CLI 직접 접근으로만 가능
    (업로드: 사용자 승인 1회 / 삭제: 사용자 승인 2회 필요)

환경변수:
  GDI_MCP_URL  : MCP 서버 URL (기본: http://mcp-dev.sginfra.net/game-doc-insight-mcp)
"""

import os
import json
import logging
import re
import sqlite3
import time

from mcp_session import McpSession

logger = logging.getLogger(__name__)

# ── MCP 캐시 레이어 (옵셔널 — 임포트 실패 시 캐시 없이 동작) ──────────
_GDI_CACHE_ENABLED = False
_gdi_cache = None
_ops_log = None
_perf = None
_GDI_FOLDER_TTL = 6     # 기본값 (config 로드 실패 시)
_GDI_FILE_TTL = 24
_GDI_MEM_TTL = 300

# try 블록 내 logger.info(GDI_MODE) 참조보다 먼저 정의해야 NameError 방지
GDI_MODE = os.getenv("GDI_MODE", "local")

try:
    import sys as _sys
    _cache_path = "D:/Vibe Dev/QA Ops/mcp-cache-layer"
    if _cache_path not in _sys.path:
        _sys.path.insert(0, _cache_path)
    # scripts/ 경로도 추가 (folder_taxonomy 임포트용)
    _scripts_path = _cache_path + "/scripts"
    if _scripts_path not in _sys.path:
        _sys.path.insert(0, _scripts_path)
    from src.cache_manager import CacheManager as _CacheManager
    from src.cache_logger import ops_log as _ops_log_mod, perf as _perf_mod
    from src import config as _cache_config
    _gdi_cache = _CacheManager()
    _ops_log = _ops_log_mod
    _perf = _perf_mod
    _GDI_FOLDER_TTL = getattr(_cache_config, "GDI_FOLDER_TTL_HOURS", 6)
    _GDI_FILE_TTL = getattr(_cache_config, "GDI_FILE_TTL_HOURS", 24)
    _GDI_MEM_TTL = getattr(_cache_config, "GDI_MEM_TTL_SEC", 300)
    _GDI_CACHE_ENABLED = True
    logger.info("[gdi] 캐시 레이어 로드 완료 (folder TTL=%dh, file TTL=%dh, mem TTL=%ds, mode=%s)",
                _GDI_FOLDER_TTL, _GDI_FILE_TTL, _GDI_MEM_TTL, GDI_MODE)
except Exception as _e:
    logger.info("[gdi] 캐시 레이어 미사용: %s", _e)

# ── 쿼리 전처리 (task-101) ──────────────────────────────────────────────
_QUERY_PREPROCESSOR_ENABLED = False
_preprocess_query_fn = None
_apply_aliases_fn = None
_preprocess_with_ranges_fn = None
try:
    from analytics.query_preprocessor import (
        preprocess_query as _preprocess_query_fn,
        apply_aliases as _apply_aliases_fn,
        preprocess_query_with_ranges as _preprocess_with_ranges_fn,
    )
    _QUERY_PREPROCESSOR_ENABLED = True
except ImportError:
    logger.warning("[gdi] query_preprocessor 로드 실패, 전처리 비활성화")

# ── TTL 정책 (task-107) ──────────────────────────────────────────────────
_ttl_classify_fn = None
_ttl_get_fn = None
_QueryType = None
try:
    from analytics.ttl_policy import classify_query as _ttl_classify_fn
    from analytics.ttl_policy import get_ttl as _ttl_get_fn
    from analytics.ttl_policy import QueryType as _QueryType
    logger.info("[gdi] TTL 정책 모듈 로드 완료")
except ImportError:
    logger.info("[gdi] TTL 정책 모듈 미사용 — 기존 고정 TTL 사용")

# ── 폴더 택소노미 인덱스 (옵셔널) ─────────────────────────────────────────
_TAXONOMY_ENABLED = False
_folder_index = None

try:
    from folder_taxonomy import FolderIndex as _FolderIndex, QueryParser as _QueryParser
    _folder_index = _FolderIndex()
    _TAXONOMY_ENABLED = True
    logger.info("[gdi] 폴더 택소노미 로드 완료")
except Exception as _te:
    logger.info("[gdi] 폴더 택소노미 미사용: %s", _te)

# ── GDI 청크 재조합 공통 모듈 (task-075) ─────────────────────────────────
# mcp-cache-layer/scripts/reconstructors.py 를 import하여 load_gdi.py와 로직 단일화
# scripts_path는 이미 위의 캐시 레이어 블록에서 sys.path에 추가되었음
try:
    from reconstructors import reconstruct_body as _reconstruct_body_shared
    _HAS_RECONSTRUCTORS = True
    logger.info("[gdi] reconstructors 공통 모듈 로드 완료")
except Exception as _re_err:
    _HAS_RECONSTRUCTORS = False
    logger.info("[gdi] reconstructors 미사용 (fallback): %s", _re_err)

    # Fallback: 메타 접두사만 제거 후 단순 결합 (MAJOR-4 반영)
    _FALLBACK_META_RE = re.compile(
        r"^(?:index_mode|file_type|content_type): .+\n?", re.MULTILINE
    )

    def _reconstruct_body_shared(chunks, source_type):  # noqa: F811
        cleaned = [_FALLBACK_META_RE.sub("", c).strip() for c in chunks]
        return "\n".join(c for c in cleaned if c)

# ── GDI MCP 읽기 전용 허용 도구 (쓰기/삭제 원천 차단) ──────────────────
# MCP는 공유 서버 → 데이터 변경 경로 완전 차단
# S3 데이터 변경은 로컬 AWS CLI 직접 접근으로만 가능
GDI_MCP_READONLY_TOOLS = frozenset({
    "unified_search", "search_by_filename", "list_files_in_folder",
    "fetch_row_documents", "search_vector_candidates",
    "get_document_version_diff", "compare_two_files", "compare_folders",
    "list_files_in_folder", "help", "test_game_doc_insight_connection",
})

# ── L1 인메모리 캐시 ─────────────────────────────────────────────────────
_GDI_MEM_CACHE: dict = {}  # {key: (data, timestamp)}


def _mem_get(key: str):
    """L1 메모리 캐시 조회. TTL 초과 시 None."""
    entry = _GDI_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < _GDI_MEM_TTL:
        return entry[0]
    return None


def _mem_get_with_ttl(key: str, ttl_sec: int = None):
    """L1 메모리 캐시 조회. ttl_sec 미지정 시 기본 _GDI_MEM_TTL 사용.

    Args:
        key: 캐시 키
        ttl_sec: TTL(초). None 이면 _GDI_MEM_TTL(300s) 기본값 사용.

    Returns:
        캐시 데이터 또는 None
    """
    effective_ttl = ttl_sec if ttl_sec is not None else _GDI_MEM_TTL
    entry = _GDI_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < effective_ttl:
        return entry[0]
    return None


def _mem_set(key: str, data):
    """L1 메모리 캐시 저장."""
    _GDI_MEM_CACHE[key] = (data, time.time())

GDI_MCP_URL = os.getenv(
    "GDI_MCP_URL", "http://mcp-dev.sginfra.net/game-doc-insight-mcp"
)

# ── GDI 모드 스위치 ──────────────────────────────────────────────────────
# "local"  : 캐시(SQLite) 전용, MCP 폴백 차단 (gdi-repo/ 로컬 파일 기반)
# "cloud"  : 기존 동작 유지 (캐시 → MCP 폴백)
# 정의 위치: L46 (캐시 레이어 try 블록 이전으로 이동 — NameError 방지)

# ── 청크 검색 활성화 플래그 (task-104) ───────────────────────────────────
# CHUNK_SEARCH_ENABLED=true 로 설정 시 _local_chunk_search() 경로 사용.
# 기본 false — 기존 FTS MATCH 경로 100% 유지. chunk 0건 또는 예외 시 자동 fallback.
CHUNK_SEARCH_ENABLED = os.getenv("CHUNK_SEARCH_ENABLED", "false").lower() == "true"

# ── GDI 청크 메타데이터 정제 ──────────────────────────────────────────────
# GDI MCP가 반환하는 각 청크에는 인덱싱용 메타데이터 접두사가 붙는다:
#   index_mode: generic_tsv
#   file_type: <파일명>
#   content_type: generic_tsv
# 이 메타데이터를 제거하여 Claude 토큰을 절약한다.
_CHUNK_META_RE = re.compile(
    r"^(?:index_mode|file_type|content_type): .+\n?",
    re.MULTILINE,
)

# PPTX/XLSX 메타데이터 접두사 패턴
_PPTX_PREFIX_RE = re.compile(
    r"^Mode: generic_pptx > FileType: .+? > ContentType: generic_pptx > Slide: (\d+) > "
)
_PPTX_EMPTY_NOTES_RE = re.compile(r"\n?### Notes:\s*$")
_XLSX_PREFIX = "Mode: generic_xlsx"

# 시트/테이블당 최대 행 수
MAX_TABLE_ROWS = 500


def _clean_chunk_text(text: str) -> str:
    """GDI 청크의 메타데이터 접두사(index_mode/file_type/content_type)를 제거한다."""
    if not text:
        return text
    return _CHUNK_META_RE.sub("", text).strip()


def _clean_any_chunk(text: str) -> str:
    """모든 GDI 청크 형식의 메타데이터를 제거한다 (단일 청크용)."""
    if not text:
        return text
    # TSV 메타데이터
    text = _CHUNK_META_RE.sub("", text)
    # PPTX 접두사 → 슬라이드 번호만 유지
    m = _PPTX_PREFIX_RE.match(text)
    if m:
        text = f"[Slide {m.group(1)}] {text[m.end():]}"
        text = _PPTX_EMPTY_NOTES_RE.sub("", text)
    # XLSX 접두사 → 시트+행 정보만 유지
    elif text.startswith(_XLSX_PREFIX):
        parts = text.split(" > ")
        sheet = ""
        data_start = 0
        for i, part in enumerate(parts):
            if part.startswith("Sheet: "):
                sheet = part[7:]
            elif part.startswith("Row: "):
                data_start = i + 1
                break
        if data_start > 0:
            data_fields = " > ".join(parts[data_start:])
            text = f"[{sheet}] {data_fields}"
    return text.strip()


# ── 파일 형식별 재구성 로직은 reconstructors.py로 이동 (task-075) ────────
# _parse_xlsx_chunk, _reconstruct_xlsx/pptx/tsv/body 제거 — load_gdi.py와 통합
# 대신 상단 _reconstruct_body_shared(chunks, source_type) 사용 ─ reconstructors.reconstruct_body
# (fallback: import 실패 시 _clean_chunk_text 로직 포함)


# ── GDI 조회 전용 로거 (logs/gdi_query.log) ──────────────────────────────
_gdi_query_logger: "logging.Logger | None" = None


def _get_gdi_query_logger() -> logging.Logger:
    """GDI 조회 전용 로거를 반환합니다."""
    global _gdi_query_logger
    if _gdi_query_logger is not None:
        return _gdi_query_logger

    _gdi_query_logger = logging.getLogger("gdi_query")
    _gdi_query_logger.setLevel(logging.INFO)
    _gdi_query_logger.propagate = False

    bot_dir  = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(os.path.dirname(bot_dir), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, "gdi_query.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _gdi_query_logger.addHandler(fh)
    return _gdi_query_logger


def _sanitize_log_field(value):
    """
    gdi_query.log 1줄 1엔트리 불변식 보호 (task-118 R-1, OWASP A09).

    분리자 ' | ' (space-padded pipe)는 외부에서 사용. 사용자 입력 안의 ASCII '|'가
    dashboard parser(s3_server.py: split('|'))에서 필드 desync를 일으키는 것을 차단.

    - ASCII '|' (U+007C) → 전각 '｜' (U+FF5C) 치환
      · failure_analyzer.py: split(' | ') ASCII 기준 → 전각 통과 (회귀 0)
      · s3_server.py: split('|') ASCII 기준 → 전각 통과 (안전화)
      · grep/audit 시 시각 동등하나 parser는 명확히 구분 가능
    - 빈 값/None은 그대로 반환
    """
    if not value:
        return value
    return value.replace('|', '｜')


def log_gdi_query(*, user_id: str = "", user_name: str = "",
                  action: str, query: str, result: str = "",
                  error: str = "", elapsed_ms: int = 0,
                  cache_status: str = ""):
    """
    /gdi 조회 내역을 logs/gdi_query.log 에 기록합니다.
    wiki_client.log_wiki_query() 와 동일 인터페이스.

    cache_status: HIT_MEM, HIT_DB, MISS, MISS_STALE, STORE, DISABLED
    """
    gl     = _get_gdi_query_logger()
    status = "ERROR" if error else "OK"

    # Slack display_name은 user-controlled — sanitize 필수 (sec-code task-118 closeout)
    s_user_name = _sanitize_log_field(user_name)
    s_user_id   = _sanitize_log_field(user_id)
    user = f"{s_user_name}({s_user_id})" if s_user_id else (s_user_name or "unknown")

    s_query  = _sanitize_log_field(query)
    s_result = _sanitize_log_field(result)
    s_error  = _sanitize_log_field(error)

    msg = f"{status} | {action} | user={user} | query={s_query}"
    if s_result:
        msg += f" | result={s_result}"
    if s_error:
        msg += f" | error={s_error}"
    if cache_status:
        msg += f" | cache={cache_status}"
    if elapsed_ms > 0:
        msg += f" | {elapsed_ms}ms"

    msg = msg.replace('\r\n', '\\n').replace('\n', '\\n')

    if error:
        gl.error(msg)
    else:
        gl.info(msg)


# ── 싱글톤 MCP 세션 ─────────────────────────────────────────────────────

_mcp_session: "McpSession | None" = None


def _get_mcp() -> McpSession:
    global _mcp_session
    if _mcp_session is None:
        _mcp_session = McpSession(
            url=GDI_MCP_URL,
            label="gdi",
        )
    return _mcp_session


# ── FTS5 LIKE 폴백 유틸 (task-102) ──────────────────────────────────────────

def _escape_like(kw: str) -> str:
    """LIKE 절에서 사용할 특수문자 이스케이프 (\\, %, _)."""
    return kw.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


def _build_like_clauses(kws: list) -> tuple:
    """키워드 리스트 → (LIKE AND 절 리스트, 파라미터 리스트).

    각 키워드에 대해 title 또는 body_text LIKE 조건을 생성한다.
    """
    clauses = []
    params = []
    for kw in kws:
        escaped = _escape_like(kw)
        clauses.append(
            "(n.title LIKE ? ESCAPE '\\' OR dc.body_text LIKE ? ESCAPE '\\')"
        )
        params.extend([f"%{escaped}%", f"%{escaped}%"])
    return clauses, params


def _run_case_b(conn: sqlite3.Connection, fts_kws: list, short_kws: list,
                game_filter_clause: str, game_filter_params: list,
                limit: int,
                date_filter_clause: str = "",
                date_filter_params: list = None,
                folder_filter_clause: str = "",
                folder_filter_params: list = None,
                order_by_recent: bool = False) -> list:
    """case B: nodes 드라이빙 LIKE 검색 (trigram 불가 키워드 포함 시 사용).

    fts_kws + short_kws 모두 LIKE AND 조건으로 처리한다.
    SELECT 컬럼은 case A/C와 동일한 8-tuple (rank=NULL).

    date_filter_clause: "AND (dm.last_modified BETWEEN ? AND ? ...)" 형태 절.
    date_filter_params: BETWEEN 바인딩 파라미터 (start+'T00:00:00', end+'T23:59:59').
    folder_filter_clause / folder_filter_params: task-115 R-3 (TSV 차단/folder_role).
    order_by_recent: True 시 ref_date DESC NULLS LAST 우선 (task-115 R-4).
    """
    if date_filter_params is None:
        date_filter_params = []
    if folder_filter_params is None:
        folder_filter_params = []

    all_kws = fts_kws + short_kws
    if not all_kws and not game_filter_clause:
        return []

    like_clauses, params_like = _build_like_clauses(all_kws)

    where_extra = ""
    if like_clauses:
        where_extra += " AND " + " AND ".join(like_clauses)
    if game_filter_clause:
        where_extra += f" {game_filter_clause}"  # "AND LOWER(n.path) LIKE ? ESCAPE '\\'" 포함
    if folder_filter_clause:
        where_extra += f" {folder_filter_clause}"  # task-115 R-3

    order_by_b = (
        "ORDER BY n.ref_date DESC NULLS LAST, n.created_at DESC"
        if order_by_recent
        else "ORDER BY n.created_at DESC"
    )
    sql = f"""
        SELECT n.id, n.title, n.path, n.node_type,
               SUBSTR(dc.body_text, 1, 500) AS body_text,
               dc.summary, dc.keywords,
               NULL AS rank
        FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        LEFT JOIN doc_meta dm ON dm.node_id = n.id
        WHERE n.source_type = 'gdi'{where_extra}
          {date_filter_clause}
        {order_by_b}
        LIMIT ?
    """
    return conn.execute(
        sql, params_like + game_filter_params + folder_filter_params + date_filter_params + [limit]
    ).fetchall()


# ── GdiClient ────────────────────────────────────────────────────────────

class GdiClient:
    """GDI MCP 클라이언트 (3계층 캐시 통합).

    ⚠️ MCP 안전: 읽기 전용 도구만 호출 가능.
    GDI_MCP_READONLY_TOOLS에 없는 도구 호출 시 차단 + 에러 로그.
    """

    def __init__(self):
        self._mcp = _get_mcp()

    def _safe_call_tool(self, tool_name: str, args: dict):
        """MCP 도구 호출 — 읽기 전용 허용 목록 검증 후 실행.

        허용되지 않은 도구 호출 시 차단하고 에러를 반환한다.
        (MCP는 공유 서버 → 쓰기/삭제 원천 차단)
        """
        if tool_name not in GDI_MCP_READONLY_TOOLS:
            logger.error(
                "[gdi] ⛔ MCP 쓰기 차단: tool=%s (허용 목록 외)", tool_name)
            return None, f"BLOCKED: '{tool_name}' is not a read-only tool"
        return self._mcp.call_tool(tool_name, args)

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
    def _cache_key_folder(folder_path: str) -> str:
        return f"folder:{folder_path}"

    @staticmethod
    def _cache_key_file(filename: str) -> str:
        return f"file:{filename}"

    def _try_cache_get(self, cache_key: str, l1_ttl_sec: int = None) -> tuple:
        """L1→L2 캐시 조회. (data, cache_status) 반환. 미스 시 (None, status).

        Args:
            cache_key: 캐시 키
            l1_ttl_sec: L1 TTL(초). None이면 기본 _GDI_MEM_TTL 사용.
                        TIME_BOUNDED 쿼리 시 60 전달 → L1 TTL 단축.
        """
        if not _GDI_CACHE_ENABLED:
            return None, "DISABLED"

        # L1: 메모리 (l1_ttl_sec 적용)
        mem = _mem_get_with_ttl(cache_key, l1_ttl_sec)
        if mem is not None:
            if _ops_log:
                _ops_log.cache_hit(cache_key, source="memory")
            return mem, "HIT_MEM"

        # L2: SQLite
        t0 = _perf.now_ms() if _perf else 0
        node = _gdi_cache.get_node("gdi", cache_key)
        if node:
            if _gdi_cache.is_stale(node["id"]):
                return None, "MISS_STALE"
            content = _gdi_cache.get_content(node["id"])
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
                     node_type: str = "file", ttl_hours: int = 24,
                     path: str | None = None):
        """L2 SQLite + L1 메모리에 캐시 저장."""
        if not _GDI_CACHE_ENABLED or data is None:
            return
        t0 = _perf.now_ms() if _perf else 0
        try:
            body_text = json.dumps(data, ensure_ascii=False)
            node_id = _gdi_cache.put_page(
                "gdi", cache_key, title,
                node_type=node_type, path=path,
                body_text=body_text,
            )
            _gdi_cache.upsert_meta(node_id, ttl_hours=ttl_hours)
            _mem_set(cache_key, data)
            if _ops_log:
                elapsed = _perf.elapsed_ms(t0) if _perf else 0
                _ops_log.cache_store(title, node_id=node_id,
                                     source_id=cache_key,
                                     char_count=len(body_text),
                                     has_body=True, elapsed_ms=elapsed)
        except Exception as e:
            logger.warning("[gdi] 캐시 저장 실패 (%s): %s", cache_key, e)

    # ── MCP 호출 메서드 (캐시 통합) ───────────────────────────

    def unified_search(self, query_text: str, game_name: str = None,
                       top_k: int = 10,
                       date_ranges=None,
                       folder_role_filter: list = None,
                       tsv_explicit: bool = False,
                       order_by_recent: bool = False) -> tuple:
        """
        크로스 컬렉션 통합 검색.
        - local 모드: SQLite 캐시 DB에서 LIKE 검색
        - cloud 모드: MCP unified_search 호출

        task-115 v1 신규 kwargs (local 모드 전용, default 후방호환):
        - date_ranges: DateRange list (기존 _local_unified_search 위임)
        - folder_role_filter: ['planning', 'qa_result', ...] 매칭 폴더만 검색
        - tsv_explicit: False(default) 시 TSV 폴더 완전 차단 (task-115 R-3)
        - order_by_recent: True 시 ORDER BY ref_date DESC (task-115 R-4)

        Returns: (parsed_data, error_str)
        """
        # ── local 모드: SQLite 캐시에서 직접 검색 ──
        if GDI_MODE == "local":
            return self._local_unified_search(
                query_text, game_name, top_k,
                date_ranges=date_ranges,
                folder_role_filter=folder_role_filter,
                tsv_explicit=tsv_explicit,
                order_by_recent=order_by_recent,
            )

        # ── cloud 모드: MCP 호출 (task-115 v1 kwargs 무시 — local 전용) ──
        args = {"query_text": query_text, "top_k": top_k}
        if game_name:
            args["game_name"] = game_name

        raw, err = self._safe_call_tool("unified_search", args)
        if err:
            return None, err
        return self._parse_raw(raw), None

    def _local_chunk_search(self, query_text: str, game_name: str = None,
                            top_k: int = 10, date_ranges=None,
                            folder_role_filter: list = None,
                            tsv_explicit: bool = False,
                            order_by_recent: bool = False) -> tuple:
        """doc_chunks + chunks_fts FTS5 trigram 청크 검색 (task-104).

        SQL: chunks_fts MATCH → JOIN doc_chunks + nodes, LIMIT top_k*3
        Python dedupe: node_id 기준 best BM25 chunk 1건만 보존 → top_k 반환.

        반환 포맷: content_preview + chunk_content (동일값, 양쪽 키 필수)
          - chunk_content: format_search_results() L726이 읽음
          - content_preview: format_context_for_claude() L1174가 읽음
        2자 이하 키워드만 있으면 빈 결과 즉시 반환 (unified fallback 유도).

        date_ranges: DateRange list. None 또는 빈 list 시 날짜 필터 없음.
        """
        if not _GDI_CACHE_ENABLED or not _gdi_cache:
            return {"success": True, "results": [], "total_count": 0}, None

        conn = _gdi_cache._conn()
        try:
            keywords = [kw.strip() for kw in query_text.split() if kw.strip()]
            if not keywords:
                return {"success": True, "results": [], "total_count": 0}, None

            # trigram: 3자 이상만 FTS MATCH 가능
            fts_kws = [kw for kw in keywords if len(kw) >= 3]
            if not fts_kws:
                # 2자 이하 키워드만 → chunks_fts MATCH 불가 → 즉시 빈 결과
                logger.debug("[gdi] chunk_search: 2자 이하 키워드만 → unified fallback")
                return {"success": True, "results": [], "total_count": 0}, None

            fts_query = " AND ".join(
                '"' + kw.replace('"', '""') + '"' for kw in fts_kws
            )

            # game_name 필터 절
            game_filter_clause = ""
            game_filter_params: list = []
            if game_name:
                escaped_game = game_name.lower().replace(
                    "\\", "\\\\"
                ).replace("%", "\\%").replace("_", "\\_")
                game_filter_clause = "AND LOWER(n.path) LIKE ? ESCAPE '\\'"
                game_filter_params = [f"%{escaped_game}%"]

            # folder_role 필터 절 (task-115 R-3 — qa-structural MAJOR fix)
            folder_filter_clause = ""
            folder_filter_params: list = []
            if folder_role_filter:
                placeholders = ",".join("?" * len(folder_role_filter))
                folder_filter_clause = f"AND COALESCE(n.folder_role,'unknown') IN ({placeholders})"
                folder_filter_params = list(folder_role_filter)
            elif not tsv_explicit:
                folder_filter_clause = "AND COALESCE(n.folder_role,'unknown') != 'game_data'"

            # date_ranges 필터 절
            # last_modified: YYYY-MM-DDTHH:MM:SS 형식 → end에 T23:59:59 suffix 필수
            date_filter_clause = ""
            date_filter_params: list = []
            effective_ranges = date_ranges or []
            if effective_ranges:
                clauses = " OR ".join(
                    "dm.last_modified BETWEEN ? AND ?"
                    for _ in effective_ranges
                )
                date_filter_clause = f"AND ({clauses})"
                date_filter_params = [
                    v
                    for dr in effective_ranges
                    for v in (dr.start + "T00:00:00", dr.end + "T23:59:59")
                ]

            order_clause_chunk = (
                "ORDER BY n.ref_date DESC NULLS LAST, rank"
                if order_by_recent
                else "ORDER BY rank"
            )
            sql = f"""
                SELECT dc.node_id, n.title, n.path, n.node_type,
                       dc.text AS chunk_text,
                       dc.section_path,
                       bm25(chunks_fts) AS rank
                FROM chunks_fts
                JOIN doc_chunks dc ON dc.id = chunks_fts.rowid
                JOIN nodes n ON n.id = dc.node_id
                LEFT JOIN doc_meta dm ON dm.node_id = n.id
                WHERE chunks_fts MATCH ?
                  AND n.source_type = 'gdi'
                  {game_filter_clause}
                  {folder_filter_clause}
                  {date_filter_clause}
                {order_clause_chunk}
                LIMIT ?
            """
            params = (
                [fts_query] + game_filter_params + folder_filter_params
                + date_filter_params + [top_k * 3]
            )
            rows = conn.execute(sql, params).fetchall()

            # node_id 기준 dedupe — best BM25(낮을수록 좋음) chunk 1건 보존
            seen_nodes: dict = {}
            for row in rows:
                nid = row[0]
                rank = row[6]
                if nid not in seen_nodes or rank < seen_nodes[nid]["rank"]:
                    seen_nodes[nid] = {
                        "node_id": nid,
                        "title": row[1] or "",
                        "path": row[2] or "",
                        "node_type": row[3] or "gdi",
                        "chunk_text": row[4] or "",
                        "section_path": row[5],
                        "rank": rank,
                    }

            deduped = list(seen_nodes.values())[:top_k]

            results = []
            for item in deduped:
                preview = item["chunk_text"][:200]
                results.append({
                    "file_name": item["title"],
                    "file_path": item["path"],
                    "source_type": item["node_type"],
                    "content_preview": preview,      # format_context_for_claude() L1174
                    "chunk_content": preview,         # format_search_results() L726 (필수)
                    "summary": "",
                    "keywords": "",
                    "_rank": item["rank"],
                    "_collection": "gdi_local_chunks",
                    "_node_id": item["node_id"],     # task-113: bench adapter 의존
                    "matched_section": item["section_path"],
                })

            return {
                "success": True,
                "results": results,
                "total_count": len(results),
                "breakdown": {"gdi_local_chunks": len(results)},
                "_mode": "local_chunk_fts",
            }, None

        except sqlite3.OperationalError as e:
            logger.warning("[gdi] chunk_search FTS MATCH 오류 (query=%r): %s",
                           query_text, e)
            return {"success": True, "results": [], "total_count": 0,
                    "_mode": "local_chunk_fts", "_error": str(e)}, None
        except Exception as e:
            logger.error("[gdi] _local_chunk_search 오류: %s", e)
            return None, f"chunk 검색 오류: {e}"
        finally:
            conn.close()

    def _local_unified_search(self, query_text: str, game_name: str = None,
                              top_k: int = 10, date_ranges=None,
                              folder_role_filter: list = None,
                              tsv_explicit: bool = False,
                              order_by_recent: bool = False) -> tuple:
        """SQLite FTS5 MATCH 기반 검색 (local 모드 전용, task-077/102).

        trigram FTS5 + 2자 이하 키워드 LIKE 폴백 (task-102).
        3경로 분기:
          case A: fts_kws만 있을 때 — FTS MATCH 드라이빙
          case B: short_kws만 있을 때 — 순수 LIKE
          case C: 둘 다 있을 때 — FTS + Python 후필터, 0건 시 case B 재실행

        date_ranges: DateRange list (task-106). None 또는 빈 list 시 날짜 필터 없음.
        반환 포맷은 기존과 100% 호환 (content_preview 키 유지).
        """
        if not _GDI_CACHE_ENABLED or not _gdi_cache:
            return None, "캐시 레이어 미사용 (local 모드에서는 캐시 필수)"

        # ── CHUNK_SEARCH_ENABLED 분기 (task-104) ─────────────────────────
        # CHUNK_SEARCH_ENABLED=true 시 _local_chunk_search() 우선 시도.
        # chunk 0건 또는 예외 발생 시 아래 기존 FTS MATCH 경로로 자동 fallback.
        # CHUNK_SEARCH_ENABLED=false(기본) 시 이 블록 완전히 통과 — 기존 경로 100% 유지.
        if CHUNK_SEARCH_ENABLED:
            try:
                chunk_data, chunk_err = self._local_chunk_search(
                    query_text, game_name, top_k, date_ranges=date_ranges,
                    folder_role_filter=folder_role_filter,
                    tsv_explicit=tsv_explicit,
                    order_by_recent=order_by_recent,
                )
                if chunk_data and chunk_data.get("total_count", 0) > 0:
                    return chunk_data, chunk_err
                # chunk 0건 → fallback (청킹 누락 탐지용 로그)
                logger.debug(
                    "[gdi] chunk 0건 → unified fallback: query=%r", query_text
                )
            except Exception as _chunk_ex:
                logger.warning(
                    "[gdi] _local_chunk_search 예외 → unified fallback: %s",
                    _chunk_ex,
                )
        # ── 기존 FTS MATCH 경로 (변경 없음) — unified fallback 영구 유지 ──

        # 쿼리 전처리 — task-101/106 (alias 교정 + 날짜 정규화 + 시간 범위 추출)
        date_ranges_from_query = None
        if _QUERY_PREPROCESSOR_ENABLED and _preprocess_with_ranges_fn:
            query_text, date_ranges_from_query = _preprocess_with_ranges_fn(query_text)
            if game_name and _apply_aliases_fn:
                # game_name alias 교정 (CRITICAL: Choaszero→Chaoszero 폴더 필터링)
                game_name = _apply_aliases_fn(game_name)
        elif _QUERY_PREPROCESSOR_ENABLED and _preprocess_query_fn:
            query_text = _preprocess_query_fn(query_text)
            if game_name and _apply_aliases_fn:
                game_name = _apply_aliases_fn(game_name)

        # 호출자가 date_ranges를 직접 전달한 경우 우선, 없으면 쿼리 추출값 사용
        effective_ranges = date_ranges or date_ranges_from_query or []

        # ── TTL 정책 분류 (task-107) ──────────────────────────────────────
        _query_type = None
        _l1_ttl, _l2_skip, _l2_ttl = 300, False, _GDI_FILE_TTL  # ImportError 폴백
        if _ttl_classify_fn and _ttl_get_fn:
            _query_type = _ttl_classify_fn(query_text, effective_ranges)
            _l1_ttl, _l2_skip, _l2_ttl = _ttl_get_fn(_query_type)

        # date_filter 절 생성
        # ⚠ last_modified는 YYYY-MM-DDTHH:MM:SS 형식 → end에 T23:59:59 suffix 필수
        date_filter_clause = ""
        date_filter_params: list = []
        if effective_ranges:
            clauses = " OR ".join(
                "dm.last_modified BETWEEN ? AND ?"
                for _ in effective_ranges
            )
            date_filter_clause = f"AND ({clauses})"
            date_filter_params = [
                v
                for dr in effective_ranges
                for v in (dr.start + "T00:00:00", dr.end + "T23:59:59")
            ]

        conn = _gdi_cache._conn()
        try:
            # ── 키워드 분리 ──────────────────────────────────────────────
            keywords = [kw.strip() for kw in query_text.split() if kw.strip()]
            if not keywords:
                return {"success": True, "results": [], "total_count": 0}, None

            # trigram: 3자 이상만 FTS MATCH 가능, 2자 이하는 LIKE 폴백
            fts_kws   = [kw for kw in keywords if len(kw) >= 3]
            short_kws = [kw for kw in keywords if len(kw) < 3]

            # ── game_name 필터 절 ─────────────────────────────────────────
            game_filter_clause = ""
            game_filter_params = []
            if game_name:
                escaped_game = _escape_like(game_name.lower())
                game_filter_clause = "AND LOWER(n.path) LIKE ? ESCAPE '\\'"
                game_filter_params = [f"%{escaped_game}%"]

            # ── folder_role 필터 절 (task-115 R-3) ────────────────────────
            # tsv_explicit=False (default) 시 game_data 폴더(TSV) 완전 차단.
            # folder_role_filter 명시 시 해당 role만 통과 (intent 기반 라우팅).
            folder_filter_clause = ""
            folder_filter_params: list = []
            if folder_role_filter:
                placeholders = ",".join("?" * len(folder_role_filter))
                folder_filter_clause = f"AND COALESCE(n.folder_role,'unknown') IN ({placeholders})"
                folder_filter_params = list(folder_role_filter)
            elif not tsv_explicit:
                folder_filter_clause = "AND COALESCE(n.folder_role,'unknown') != 'game_data'"

            # ── ORDER BY 분기 (task-115 R-4) ──────────────────────────────
            # order_by_recent=True 시 ref_date DESC NULLS LAST 우선, default 기존 rank.
            order_clause_a = (
                "ORDER BY n.ref_date DESC NULLS LAST, rank LIMIT ?"
                if order_by_recent
                else "ORDER BY rank LIMIT ?"
            )

            # ── 경로 분기 ─────────────────────────────────────────────────
            if fts_kws:
                # case A 또는 case C: FTS MATCH 드라이빙
                fts_query = " AND ".join(
                    '"' + kw.replace('"', '""') + '"' for kw in fts_kws
                )
                sql = f"""
                    SELECT n.id, n.title, n.path, n.node_type,
                           SUBSTR(dc.body_text, 1, 500) AS body_text,
                           dc.summary, dc.keywords,
                           bm25(search_fts) AS rank
                    FROM search_fts
                    JOIN nodes n ON n.id = search_fts.rowid
                    JOIN doc_content dc ON dc.node_id = n.id
                    LEFT JOIN doc_meta dm ON dm.node_id = n.id
                    WHERE search_fts MATCH ?
                      AND n.source_type = 'gdi'
                      {game_filter_clause}
                      {folder_filter_clause}
                      {date_filter_clause}
                    {order_clause_a}
                """
                params = (
                    [fts_query] + game_filter_params + folder_filter_params
                    + date_filter_params + [top_k]
                )
                rows = conn.execute(sql, params).fetchall()

                if short_kws:
                    # case C: Python 후필터 (short_kws를 title/body_text에서 확인)
                    filtered = [
                        row for row in rows
                        if all(
                            skw.lower() in (row[1] or "").lower() or
                            skw.lower() in (row[4] or "").lower()
                            for skw in short_kws
                        )
                    ]
                    if not filtered:
                        # 0건 → case B 재실행 (fts_kws + short_kws 모두 LIKE)
                        rows = _run_case_b(
                            conn,
                            fts_kws=fts_kws,
                            short_kws=short_kws,
                            game_filter_clause=game_filter_clause,
                            game_filter_params=game_filter_params,
                            limit=top_k,
                            date_filter_clause=date_filter_clause,
                            date_filter_params=date_filter_params,
                            folder_filter_clause=folder_filter_clause,
                            folder_filter_params=folder_filter_params,
                            order_by_recent=order_by_recent,
                        )
                    else:
                        rows = filtered
                # else: case A — FTS 결과 그대로 사용

            else:
                # case B: fts_kws 없음, 순수 LIKE
                rows = _run_case_b(
                    conn,
                    fts_kws=[],
                    short_kws=short_kws,
                    game_filter_clause=game_filter_clause,
                    game_filter_params=game_filter_params,
                    limit=top_k,
                    date_filter_clause=date_filter_clause,
                    date_filter_params=date_filter_params,
                    folder_filter_clause=folder_filter_clause,
                    folder_filter_params=folder_filter_params,
                    order_by_recent=order_by_recent,
                )

            # ── 결과 변환 ────────────────────────────────────────────────
            results = []
            for row in rows:
                row_id, title, path, node_type, body_text, summary, keywords_col, rank = row
                # body_text 첫 200자 preview
                preview = body_text[:200] if body_text else ""
                results.append({
                    "file_name": title or "",
                    "file_path": path or "",
                    "source_type": node_type or "gdi",
                    "content_preview": preview,  # 기존 키 유지 (M-1)
                    "summary": summary or "",
                    "keywords": keywords_col or "",
                    "_rank": rank,
                    "_collection": "gdi_local_cache",
                    "_node_id": row_id,           # task-113: bench adapter 의존
                })

            data = {
                "success": True,
                "results": results,
                "total_count": len(results),
                "breakdown": {"gdi_local_cache": len(results)},
                "_mode": "local_fts",
                "_query_type": (_query_type.value if _query_type else "normal"),
                "_l2_skip": _l2_skip,
                "_l2_ttl": _l2_ttl,
            }
            return data, None

        except sqlite3.OperationalError as e:
            # FTS 쿼리 파싱 실패 시 (특수문자 등) 빈 결과 반환
            logger.warning("[gdi] FTS MATCH 쿼리 오류 (query=%r): %s", query_text, e)
            return {"success": True, "results": [], "total_count": 0,
                    "_mode": "local_fts", "_error": str(e)}, None
        except Exception as e:
            logger.error("[gdi] local unified_search 오류: %s", e)
            return None, f"local 검색 오류: {e}"
        finally:
            conn.close()

    def search_by_filename(self, filename_query: str, page: int = 1,
                           game_name: str = None,
                           page_size: int = 10,
                           exact_match: bool = False,
                           ttl_hours: float = None,
                           skip_l2: bool = False) -> tuple:
        """
        파일명 기반 검색 (청크 내용 포함, 페이지네이션).
        page=1 + page_size ≤ 20 일 때만 캐시 적용.

        Args:
            ttl_hours: L2 캐시 TTL(시간). None이면 _GDI_FILE_TTL(24h) 사용.
                       unified_search()에서 TTL 정책(task-107)에 따라 전달.
            skip_l2: True이면 _cache_store 호출 skip (TIME_BOUNDED 쿼리용).
                     독립 호출 시 기본 False → 기존 동작 100% 유지.

        Returns: (parsed_data, error_str, cache_status)
        """
        cache_status = ""

        # 캐시 조회 (첫 페이지 + 소규모만)
        use_cache = (page == 1 and page_size <= 20)
        if use_cache:
            cache_key = self._cache_key_file(filename_query)
            # skip_l2=True(TIME_BOUNDED)이면 L1 TTL도 단축 적용 (task-107)
            l1_ttl = 60 if skip_l2 else None
            cached, cache_status = self._try_cache_get(cache_key, l1_ttl_sec=l1_ttl)
            if cached is not None:
                return cached, None

        # local 모드: 캐시 미스 시 MCP 폴백 없이 None 반환
        if GDI_MODE == "local":
            logger.debug("[gdi] search_by_filename: local 모드 — 캐시 미스 (%s)", filename_query)
            return None, None

        # cloud 모드: MCP 호출
        args = {"file_name_query": filename_query, "page": page,
                "page_size": page_size}
        if game_name:
            args["game_name"] = game_name
        if exact_match:
            args["exact_match"] = True

        raw, err = self._safe_call_tool("search_by_filename", args)
        if err:
            return None, err
        data = self._parse_raw(raw)

        # 캐시 저장 (성공 + 파일 정보 있을 때)
        # skip_l2=True(TIME_BOUNDED) 시 저장 skip → stale hit 방지 (task-107)
        if use_cache and not skip_l2 and data and isinstance(data, dict) and data.get("file"):
            file_info = data["file"]
            title = file_info.get("file_name", filename_query)
            fpath = file_info.get("file_path", "")
            effective_ttl = ttl_hours if ttl_hours is not None else _GDI_FILE_TTL
            self._cache_store(
                self._cache_key_file(filename_query), title, data,
                node_type="file", ttl_hours=effective_ttl, path=fpath,
            )
            if not cache_status:
                cache_status = "STORE"

        return data, None

    def list_files_in_folder(self, folder_path: str, page: int = 1,
                             page_size: int = 20,
                             ttl_hours: float = None,
                             skip_l2: bool = False) -> tuple:
        """
        폴더 내 파일 목록 조회.
        page=1 일 때만 캐시 적용.

        Args:
            ttl_hours: L2 캐시 TTL(시간). None이면 _GDI_FOLDER_TTL(6h) 사용.
            skip_l2: True이면 _cache_store 호출 skip (TIME_BOUNDED 쿼리용).
                     독립 호출 시 기본 False → 기존 동작 100% 유지.

        Returns: (parsed_data, error_str)
        """
        cache_status = ""

        # 캐시 조회 (첫 페이지만)
        use_cache = (page == 1)
        if use_cache:
            cache_key = self._cache_key_folder(folder_path)
            cached, cache_status = self._try_cache_get(cache_key)
            if cached is not None:
                return cached, None

        # local 모드: 캐시 미스 시 MCP 폴백 없이 None 반환
        if GDI_MODE == "local":
            logger.debug("[gdi] list_files_in_folder: local 모드 — 캐시 미스 (%s)", folder_path)
            return None, None

        # cloud 모드: MCP 호출
        raw, err = self._safe_call_tool("list_files_in_folder", {
            "folder_path": folder_path,
            "page": page,
            "page_size": page_size,
        })
        if err:
            return None, err
        data = self._parse_raw(raw)

        # 캐시 저장
        # skip_l2=True(TIME_BOUNDED) 시 저장 skip → stale hit 방지 (task-107)
        if use_cache and not skip_l2 and data and isinstance(data, dict) and data.get("success"):
            effective_ttl = ttl_hours if ttl_hours is not None else _GDI_FOLDER_TTL
            self._cache_store(
                self._cache_key_folder(folder_path), folder_path, data,
                node_type="folder", ttl_hours=effective_ttl, path=folder_path,
            )
            if not cache_status:
                cache_status = "STORE"

        return data, None


# ── Slack 포맷 헬퍼 ──────────────────────────────────────────────────────

def format_search_results(data: dict, query: str) -> str:
    """unified_search 결과 -> Slack 텍스트"""
    if not data or not data.get("success"):
        return f"ℹ️ `{query}` 검색 결과가 없습니다."

    results = data.get("results", [])
    total   = data.get("total_count", len(results))

    if not results:
        return f"ℹ️ `{query}` 에 해당하는 문서가 없습니다."

    lines = [f"*🔍 '{query}' 검색 결과 ({total}건)*\n"]
    for i, r in enumerate(results[:10], 1):
        fname = r.get("file_name", "?")
        fpath = r.get("file_path", "")
        gname = r.get("game_name", "")
        coll  = r.get("_collection", "")
        score = r.get("_score", 0)

        # 청크 내용 요약 (메타데이터 정제 + 첫 150자)
        chunk = _clean_any_chunk(r.get("chunk_content", ""))
        if len(chunk) > 150:
            chunk = chunk[:150] + "..."

        line = f"{i}. *{fname}*"
        if gname:
            line += f"  [{gname}]"
        if fpath:
            line += f"\n    📁 `{fpath}`"
        if chunk:
            line += f"\n    📝 {chunk}"
        lines.append(line)

    return "\n".join(lines)


def format_file_search(data: dict, query: str) -> str:
    """search_by_filename 결과 -> Slack 텍스트"""
    if not data or not data.get("success"):
        msg = data.get("message", "") if data else ""
        return f"ℹ️ `{query}` 파일을 찾을 수 없습니다.{(' (' + msg + ')') if msg else ''}"

    file_info = data.get("file", {})
    chunks    = data.get("chunks", [])
    pagination = data.get("pagination", {})
    others    = data.get("other_matching_files", [])

    if not file_info:
        return f"ℹ️ `{query}` 파일을 찾을 수 없습니다."

    fname = file_info.get("file_name", "?")
    fpath = file_info.get("file_path", "")
    gname = file_info.get("game_name", "")

    lines = [f"*📄 {fname}*"]
    if gname:
        lines[0] += f"  [{gname}]"
    if fpath:
        lines.append(f"📁 `{fpath}`")

    # 청크 내용 표시 (최대 5개, 메타데이터 정제)
    if chunks:
        lines.append(f"\n*내용 ({len(chunks)}개 청크 중 최대 5개):*")
        for c in chunks[:5]:
            content = _clean_any_chunk(c.get("chunk_content", c.get("content", "")))
            if len(content) > 200:
                content = content[:200] + "..."
            lines.append(f"```\n{content}\n```")

    # 페이지네이션 정보
    if pagination and pagination.get("has_next"):
        cur  = pagination.get("current_page", 1)
        total_pages = pagination.get("total_pages", "?")
        lines.append(f"\n📖 페이지 {cur}/{total_pages}")

    # 다른 매칭 파일
    if others:
        lines.append(f"\n*유사 파일 ({len(others)}건):*")
        for o in others[:5]:
            oname = o.get("file_name", "?")
            opath = o.get("file_path", "")
            lines.append(f"• {oname}  (`{opath}`)" if opath else f"• {oname}")

    return "\n".join(lines)


def format_folder_list(data: dict, path: str) -> str:
    """list_files_in_folder 결과 -> Slack 텍스트"""
    if not data or not data.get("success"):
        msg = data.get("message", "") if data else ""
        return f"ℹ️ `{path}` 폴더를 찾을 수 없습니다.{(' (' + msg + ')') if msg else ''}"

    files      = data.get("files", [])
    total      = data.get("total_files", len(files))
    pagination = data.get("pagination", {})

    if not files:
        return f"ℹ️ `{path}` 폴더에 파일이 없습니다."

    lines = [f"*📁 {path}* ({total}개 파일)\n"]
    for f in files[:15]:
        fname  = f.get("file_name", "?")
        stype  = f.get("source_type", "")
        gname  = f.get("game_name", "")
        chunks = f.get("chunk_count", 0)
        idate  = (f.get("indexed_date") or "")[:10]

        line = f"• *{fname}*"
        extras = []
        if gname:
            extras.append(gname)
        if stype:
            extras.append(stype)
        if idate:
            extras.append(idate)
        if extras:
            line += f"  ({', '.join(extras)})"
        lines.append(line)

    if pagination and pagination.get("has_next"):
        cur   = pagination.get("current_page", 1)
        total_pages = pagination.get("total_pages", "?")
        lines.append(f"\n📖 페이지 {cur}/{total_pages} — 더 보려면 `/gdi folder {path} page:2`")

    return "\n".join(lines)


def get_file_content_text(data: dict) -> str:
    """
    search_by_filename 결과에서 파일 내용 텍스트를 추출합니다.
    Claude AI 답변 생성에 사용됩니다.
    파일 형식(xlsx/pptx/tsv)에 따라 마크다운 테이블/슬라이드 문서로 재구성합니다.

    Returns: 파일 내용 텍스트 (없으면 빈 문자열)
    """
    if not data or not data.get("success"):
        return ""

    chunks = data.get("chunks", [])
    if not chunks:
        return ""

    # source_type 감지 (file 메타데이터 또는 첫 청크 내용에서)
    source_type = ""
    file_info = data.get("file", {})
    if file_info:
        source_type = file_info.get("source_type", "")
    if not source_type and chunks:
        first = chunks[0].get("chunk_content", chunks[0].get("content", ""))
        if first.startswith(_XLSX_PREFIX):
            source_type = "generic_xlsx"
        elif _PPTX_PREFIX_RE.match(first):
            source_type = "generic_pptx"

    # raw 청크 텍스트 수집
    raw_chunks = []
    for c in chunks:
        text = c.get("chunk_content", c.get("content", ""))
        if text and text.strip():
            raw_chunks.append(text)

    # 형식별 재구성
    if source_type and raw_chunks:
        return _reconstruct_body_shared(raw_chunks, source_type)

    # 폴백: 단순 정제
    return "\n".join(_clean_chunk_text(t) for t in raw_chunks if t.strip())


def _select_best_file(file_info: dict, others: list, target_hint: str | None) -> tuple:
    """[task-083] 동명 파일 후보 중 best match 선택.

    Returns: (selected_file_info, reason)
        reason: "no_alternatives" | "exact_path_match" |
                "best_prefix_match" | "fallback_first"
    """
    candidates = [file_info] + (others or [])
    if len(candidates) <= 1:
        return file_info, "no_alternatives"
    if not target_hint:
        return file_info, "fallback_first"
    norm_hint = target_hint.replace('\\', '/').lower()
    # 1차: exact match 또는 endswith
    for c in candidates:
        cpath = (c.get('file_path') or '').replace('\\', '/').lower()
        if cpath and (cpath == norm_hint or cpath.endswith(norm_hint)):
            return c, "exact_path_match"
    # 2차: longest common prefix
    def _score(c):
        cpath = (c.get('file_path') or '').replace('\\', '/').lower()
        i = 0
        while i < min(len(cpath), len(norm_hint)) and cpath[i] == norm_hint[i]:
            i += 1
        return i
    best = max(candidates, key=_score)
    return best, "best_prefix_match"


def get_file_content_full(file_name: str, game_name: str = "",
                          mcp: "McpSession | None" = None,
                          target_path_hint: str | None = None) -> str:
    """파일 전체 텍스트 반환 (캐시 우선 → MCP 폴백).

    1) SQLite doc_content.body_text 조회 (일괄 적재 데이터 — 이미 재구성됨)
    2) 없으면 MCP search_by_filename 전체 페이지 수집 → 형식별 재구성

    Args:
        target_path_hint: [task-083] 동명 파일 다수 시 의도 경로 힌트.
            제공 시 _select_best_file로 best match 후보 선정 + 경고 로그.

    Returns: 파일 전체 텍스트 (없으면 빈 문자열)
    """
    # ── 1단계: 캐시 조회 (일괄 적재 데이터 — load_gdi.py에서 이미 재구성됨) ──
    if _GDI_CACHE_ENABLED and _gdi_cache:
        try:
            node = _gdi_cache.get_node_by_title(file_name, source_type="gdi")
            if node:
                content = _gdi_cache.get_content(node["id"])
                if content and content.get("body_text"):
                    logger.debug("[gdi] get_file_content_full: 캐시 HIT (%s)", file_name)
                    return content["body_text"]  # 이미 재구성된 데이터
        except Exception as e:
            logger.debug("[gdi] 캐시 조회 오류: %s", e)

    # ── 2단계: local 모드에서는 MCP 폴백 없이 빈 문자열 반환 ──
    if GDI_MODE == "local":
        logger.debug("[gdi] get_file_content_full: local 모드 — 캐시 미스 (%s)", file_name)
        return ""

    # ── 3단계: cloud 모드 — MCP 전체 청크 수집 (폴백) → 형식별 재구성 ──
    if mcp is None:
        mcp = McpSession(url=GDI_MCP_URL, label="gdi")

    raw_chunks = []
    source_type = ""
    page = 1
    while True:
        args = {
            "file_name_query": file_name,
            "exact_match": True,
            "page": page,
            "page_size": 20,
        }
        if game_name:
            args["game_name"] = game_name

        # MCP 읽기 전용 검증
        if "search_by_filename" not in GDI_MCP_READONLY_TOOLS:
            logger.error("[gdi] ⛔ MCP 쓰기 차단: search_by_filename")
            break
        raw, err = mcp.call_tool("search_by_filename", args)
        if err:
            break

        data = raw
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                break
        if not isinstance(data, dict):
            break

        # source_type 감지 (첫 페이지에서만)
        if not source_type:
            file_info = data.get("file", {})
            if file_info:
                source_type = file_info.get("source_type", "")
                # [task-083] 동명 파일 N>1 검증 (silent failure 방지)
                others = data.get("other_matching_files", []) or []
                if others:
                    selected, reason = _select_best_file(
                        file_info, others, target_path_hint)
                    total = 1 + len(others)
                    cand_paths = [file_info.get("file_path", "")] + [
                        o.get("file_path", "") for o in others[:5]]
                    if reason == "exact_path_match":
                        logger.info(
                            "[gdi:task-083] 동명 파일 %d개 — hint=%s, selected_path=%s (reason=%s)",
                            total, target_path_hint, selected.get("file_path", ""), reason)
                    else:
                        logger.warning(
                            "[gdi:task-083] 동명 파일 %d개 감지 — hint=%s, used_path=%s (reason=%s, candidates=%s)",
                            total, target_path_hint or "(none)",
                            file_info.get("file_path", ""), reason, cand_paths)
                    # NOTE: MVP는 경고만. 자동 재선택 청크 수집은 향후 개선.

        chunks = data.get("chunks", [])
        for c in chunks:
            text = c.get("chunk_content", "")
            if text and text.strip():
                raw_chunks.append(text)

        pagination = data.get("pagination", {})
        if not pagination.get("has_next"):
            break
        page += 1
        time.sleep(0.2)

    if not raw_chunks:
        return ""

    # source_type 자동 감지 (메타데이터 없을 때 첫 청크로 추론)
    if not source_type and raw_chunks:
        first = raw_chunks[0]
        if first.startswith(_XLSX_PREFIX):
            source_type = "generic_xlsx"
        elif _PPTX_PREFIX_RE.match(first):
            source_type = "generic_pptx"

    # 형식별 재구성 (task-075: reconstructors.py 공통 모듈 사용)
    return _reconstruct_body_shared(raw_chunks, source_type)


# ── 폴더 택소노미 검색 ──────────────────────────────────────────────────

def taxonomy_search(
    query: str,
    question: str = "",
    max_files: int = 20,
) -> dict | None:
    """자연어 질의를 폴더 택소노미로 해석하여 캐시 DB에서 직접 결과를 반환한다.

    키워드(query)와 질문(question)을 결합하여 파싱한다.
    예) query="카제나 2/4 3차", question="테스트 결과에서 FAIL 이슈?"
      → 결합: "카제나 2/4 3차 테스트 결과에서 FAIL 이슈?"
      → game=Chaoszero, date=0204, build=3차, category=Test Result

    택소노미가 비활성이거나, 게임명이 파싱되지 않으면 None 반환 (MCP 폴백).

    Returns:
        {"folders": list[dict], "files": list[dict], "parsed": dict}
        또는 None (해석 실패)
    """
    if not _TAXONOMY_ENABLED or not _folder_index:
        return None

    try:
        # 키워드 + 질문 결합하여 파싱 (카테고리 등 질문에서도 추출)
        combined = f"{query} {question}".strip() if question else query
        parsed = _QueryParser.parse(combined)

        # 최소 게임명이 있어야 택소노미 적용
        if not parsed.get("game"):
            return None

        # 결합 텍스트로 폴더/파일 조회
        folders = _folder_index.resolve_query(combined)
        if not folders:
            return None

        files = _folder_index.get_files_with_content(combined, max_files=max_files)

        logger.info(
            "[gdi] 택소노미 해석 성공: game=%s, cat=%s, date=%s, "
            "build=%s → folders=%d, files=%d",
            parsed.get("game"), parsed.get("category"),
            parsed.get("date_mmdd"), parsed.get("build"),
            len(folders), len(files),
        )

        return {
            "folders": folders,
            "files": files,
            "parsed": parsed,
        }
    except Exception as e:
        logger.warning("[gdi] 택소노미 검색 오류: %s", e)
        return None


def format_taxonomy_results(tax_data: dict, query: str) -> str:
    """taxonomy_search() 결과 → Slack 포맷 텍스트."""
    if not tax_data:
        return ""

    folders = tax_data.get("folders", [])
    files = tax_data.get("files", [])
    parsed = tax_data.get("parsed", {})

    lines = [f"*🗂️ '{query}' 택소노미 검색 결과*\n"]

    # 파싱 정보
    info_parts = []
    if parsed.get("game"):
        info_parts.append(f"게임: {parsed['game']}")
    if parsed.get("category"):
        info_parts.append(f"카테고리: {parsed['category']}")
    if parsed.get("date_mmdd"):
        mmdd = parsed["date_mmdd"]
        info_parts.append(f"날짜: {mmdd[:2]}/{mmdd[2:]}")
    if parsed.get("build"):
        b = parsed["build"]
        info_parts.append(f"빌드: {b.get('type', '')} {b.get('numbers', [])}")
    if info_parts:
        lines.append(f"📌 {' | '.join(info_parts)}\n")

    # 폴더 목록 (최대 10개)
    lines.append(f"*📁 매칭 폴더 ({len(folders)}개):*")
    for f in folders[:10]:
        fc = f.get("file_count", 0)
        lines.append(f"• `{f['full_path']}` ({fc}파일)")
    if len(folders) > 10:
        lines.append(f"  _... 외 {len(folders) - 10}개_")

    # 파일 목록 (최대 15개)
    if files:
        lines.append(f"\n*📄 파일 ({len(files)}개):*")
        for i, f in enumerate(files[:15], 1):
            title = f.get("title", "?")
            cc = f.get("char_count", 0)
            lines.append(f"{i}. *{title}* ({cc:,}자)")
        if len(files) > 15:
            lines.append(f"  _... 외 {len(files) - 15}개_")

    return "\n".join(lines)


def get_taxonomy_context_text(tax_data: dict, max_chars: int = 50000) -> str:
    """taxonomy_search() 결과에서 Claude AI용 컨텍스트 텍스트를 추출한다."""
    if not tax_data:
        return ""

    files = tax_data.get("files", [])
    if not files:
        return ""

    parts = []
    total_chars = 0
    for f in files:
        title = f.get("title", "?")
        body = f.get("body_text", "")
        source_id = f.get("source_id", "")
        if not body:
            continue
        section = f"[파일: {title}]\n경로: {source_id}\n내용:\n{body}"
        if total_chars + len(section) > max_chars:
            remaining = max_chars - total_chars
            if remaining > 200:
                parts.append(section[:remaining] + "\n\n_(본문 잘림)_")
            break
        parts.append(section)
        total_chars += len(section)

    return "\n\n---\n\n".join(parts)


def get_search_context_text(data: dict) -> str:
    """
    unified_search 결과에서 컨텍스트 텍스트를 추출합니다.
    Claude AI 답변 생성에 사용됩니다.
    각 청크의 형식(xlsx/pptx/tsv)에 맞게 메타데이터를 정제합니다.
    enrichment 데이터(summary/keywords)가 있으면 컨텍스트에 포함합니다.

    Returns: 검색 결과 컨텍스트 텍스트 (없으면 빈 문자열)
    """
    if not data or not data.get("success"):
        return ""

    results = data.get("results", [])
    if not results:
        return ""

    parts = []
    for r in results[:5]:
        fname   = r.get("file_name", "?")
        fpath   = r.get("file_path", "")
        # local 캐시 결과는 content_preview, cloud 결과는 chunk_content
        chunk   = r.get("content_preview") or _clean_any_chunk(r.get("chunk_content", ""))
        summary = r.get("summary", "")
        keywords = r.get("keywords", "")

        entry = f"[파일: {fname}]\n경로: {fpath}"
        if summary:
            entry += f"\n요약: {summary}"
        if keywords:
            entry += f"\n키워드: {keywords}"
        entry += f"\n내용: {chunk}"
        parts.append(entry)

    return "\n\n---\n\n".join(parts)
