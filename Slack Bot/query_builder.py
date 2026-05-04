"""
query_builder.py — 3-layer Query builder (task-129 S4-C)

설계 v4 §5 정합:
- _escape_like: LIKE ESCAPE '\\' (MAJOR-NEW-1 raw string)
- _strip_time_expressions: path_segments 시간 표현 제거 (MAJOR-NEW-2)
- _next_day: date_to exclusive 상한 (MAJOR-NEW-5, jira_client.py:807-810 정합)
- _resolve_order_by_wiki_list: doc_meta join 여부별 ORDER BY (MAJOR-NEW-3)
- BuiltQuery v4: skip_weight, need_doc_meta_join, relaxation_strip_log
- MINOR-A: build_gdi_query 내 effective_game 로컬 변수 (intent 직접 변이 금지)
"""

import re
import logging
import dataclasses
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── 시간 표현 패턴 (MAJOR-NEW-2 — _strip_time_expressions용) ─────────────────
_TIME_EXPR_PATTERNS = re.compile(
    r"^("
    r"오늘|어제|그제|내일|모레|"
    r"이번\s*(주|달|분기|연도|년)|"
    r"지난\s*(주|달|분기|연도|년)|"
    r"다음\s*(주|달|분기|연도|년)|"
    r"최근\s*\d+\s*(일|주|달|개월|년|시간)|"
    r"\d+\s*(일|주|달|개월|년)\s*(전|이내|이후)?|"
    r"\d{1,2}\s*월(\s*\d{1,2}\s*일)?|"
    r"\d{4}\s*년(\s*\d{1,2}\s*월)?|"
    r"\d{4}-\d{2}-\d{2}"
    r")$"
)

# ── 가중치 매트릭스 v4 (MINOR-C: patch_note 행 없음) ─────────────────────────
# (folder_role, chunk_origin) → weight
_WEIGHT_MATRIX: dict = {
    ("planning",   "sheet"):    1.4,
    ("planning",   "table"):    1.4,
    ("planning",   "section"):  1.2,
    ("planning",   "sliding"):  0.9,
    ("planning",   "preamble"): 1.0,
    ("planning",   "legacy"):   0.8,
    ("qa_result",  "sheet"):    1.3,
    ("qa_result",  "table"):    1.4,
    ("qa_result",  "section"):  1.1,
    ("qa_result",  "sliding"):  0.9,
    ("qa_result",  "preamble"): 1.0,
    ("qa_result",  "legacy"):   0.8,
    ("live_issue", "sheet"):    0.9,
    ("live_issue", "table"):    1.0,
    ("live_issue", "section"):  1.4,
    ("live_issue", "sliding"):  1.2,
    ("live_issue", "preamble"): 1.1,
    ("live_issue", "legacy"):   0.8,
    ("dashboard",  "sheet"):    1.0,
    ("dashboard",  "table"):    1.1,
    ("dashboard",  "section"):  1.2,
    ("dashboard",  "sliding"):  1.0,
    ("dashboard",  "preamble"): 1.0,
    ("dashboard",  "legacy"):   0.8,
    ("game_data",  "sheet"):    1.5,
    ("game_data",  "table"):    1.4,
    ("game_data",  "section"):  0.9,
    ("game_data",  "sliding"):  0.7,
    ("game_data",  "preamble"): 1.0,
    ("game_data",  "legacy"):   0.7,
    ("unknown",    "sheet"):    1.0,
    ("unknown",    "table"):    1.0,
    ("unknown",    "section"):  1.0,
    ("unknown",    "sliding"):  1.0,
    ("unknown",    "preamble"): 1.0,
    ("unknown",    "legacy"):   0.8,
}


# ── BuiltQuery dataclass v4 ───────────────────────────────────────────────────

@dataclass
class BuiltQuery:
    """빌드된 쿼리 결과 (설계 v4 §5.1)."""
    sql: str
    params: list
    intent_signature: str
    where_clauses: list
    weight_matrix: dict
    fts_query: Optional[str]
    domain: str
    has_fts: bool
    request_type: str
    metadata_field: Optional[str] = None
    skip_layer3: bool = False
    skip_weight: bool = False           # v4: metadata/list 경로
    need_doc_meta_join: bool = False    # v4 MAJOR-NEW-3
    relaxation_strip_log: list = field(default_factory=list)  # v4 MAJOR-NEW-2


# ── helper 함수들 ─────────────────────────────────────────────────────────────

# task-129.8: FTS5 trigram의 ≥3자 multi-byte 한계 보강 (LIKE fallback)
# 한글 완성형 음절 (U+AC00 ~ U+D7A3, '가'~'힣') — 자모 단독(ㄱ, ㅏ 등) 미포함
_KOREAN_CHAR_RE = re.compile(r"[가-힣]")


def _is_short_korean(kw) -> bool:
    """task-129.8: FTS5 trigram이 못 잡는 2자 이하 한국어 단어 감지.

    Returns:
        True — 한글 완성형 음절 1+ 포함 + 길이 ≤2 → body_text LIKE fallback 의무
        False — 3+자 / 영어 / None / 빈 / non-str / 자모 단독 / 한자 / 가나
    """
    if not isinstance(kw, str) or not kw:
        return False
    if len(kw) > 2:
        return False
    return bool(_KOREAN_CHAR_RE.search(kw))


def _escape_like(s: str) -> str:
    """LIKE 패턴 escape (설계 v4 §5.6 + MAJOR-NEW-1).

    Note (MINOR-B): 호출 시 lower() 적용 의무 — `_escape_like(seg.lower())`.
    SQL은 LOWER(n.path) LIKE ? 패턴이므로 입력도 lower 정합.

    raw string 대조 (MAJOR-NEW-1 실측):
      입력 'C:\\\\Users' (1BS) → lower 'c:\\\\users' → 결과 'c\\\\\\\\users' (2BS)
      %감싸기: '%c\\\\\\\\users%' = r'%c:\\\\users%'
    """
    return (s.replace("\\", "\\\\")
              .replace("%", "\\%")
              .replace("_", "\\_"))


def _strip_time_expressions(segments: list) -> "tuple[list, list]":
    """v4 MAJOR-NEW-2: path_segments에서 시간 표현 segment 제거.

    Returns:
        (cleaned_segments, removed_time_segments)
        예: ['패치노트','이번 달','카제나'] → (['패치노트','카제나'], ['이번 달'])
    """
    cleaned = []
    removed = []
    for seg in segments:
        if _TIME_EXPR_PATTERNS.match(seg.strip()):
            removed.append(seg)
        else:
            cleaned.append(seg)
    return cleaned, removed


def _next_day(date_str: str) -> str:
    """YYYY-MM-DD → 다음 날짜 문자열 (jira_client.py:807-810 정합).

    MAJOR-NEW-5: date_to exclusive 상한 처리.
    예: '2026-04-29' → '2026-04-30'
    """
    d = date.fromisoformat(date_str)
    return (d + timedelta(days=1)).isoformat()


def _resolve_order_by_wiki_list(intent, need_doc_meta_join: bool) -> str:
    """MAJOR-NEW-3: wiki list 경로 ORDER BY 결정.

    need_doc_meta_join=True → dm.last_modified DESC NULLS LAST (date 정렬)
    need_doc_meta_join=False → n.title ASC (안전 fallback — dm 참조 불가)
    """
    if need_doc_meta_join:
        return "dm.last_modified DESC NULLS LAST, n.title ASC"
    return "n.title ASC"


def _apply_weight_and_sort(rows: list, built: BuiltQuery) -> list:
    """v4 OQ-v3-2: skip_weight=True 시 weight 계산 skip."""
    if built.skip_weight:
        # metadata/list 경로 — SQL ORDER BY 결과 그대로
        limit = 10
        if built.params:
            for p in reversed(built.params):
                if isinstance(p, int):
                    limit = p
                    break
        return rows[:limit]

    # content_search 경로만 weight 적용
    weighted = []
    for row in rows:
        fts_rank = row.get("fts_rank")
        if fts_rank is None:
            score = 0.0
        else:
            weight = _WEIGHT_MATRIX.get(
                (row.get("folder_role"), row.get("chunk_origin")),
                1.0
            )
            score = (-fts_rank) * weight   # CRITICAL-1 부호 반전
        weighted.append((score, row))
    weighted.sort(key=lambda x: -x[0])
    return [r for _, r in weighted]


def _split_by_backslash(question: str) -> list:
    """MAJOR-NEW-6: '\\' 구분자로 분할 → segments 리스트."""
    return [s.strip() for s in question.split("\\") if s.strip()]


# ── game canonical 탐지 (의존성 없는 간단 버전) ──────────────────────────────

def _detect_game_canonical(seg: str) -> Optional[str]:
    """첫 segment가 게임 canonical 매칭 시 canonical 문자열만 반환.

    Step 6 시정: game_aliases.detect_game_in_text는 dict를 반환하므로
    canonical key만 추출. 과거 dict 그대로 반환 시 SQL params에 dict가 들어가는 버그.
    """
    try:
        from game_aliases import detect_game_in_text
        result = detect_game_in_text(seg)
        if isinstance(result, dict):
            return result.get("canonical")
        if isinstance(result, str):
            return result
        return None
    except (ImportError, Exception):
        return None


# ── build_wiki_query ──────────────────────────────────────────────────────────

def build_wiki_query(intent) -> BuiltQuery:
    """WikiIntent → BuiltQuery (설계 v4 §5.5.1)."""
    where_clauses = ["n.source_type = 'wiki'"]
    params = []
    need_doc_meta_join = False
    fts_query = None
    has_fts = False
    skip_weight = intent.request_type in ("metadata", "list")
    strip_log = []

    # need_doc_meta_join 강제 트리거 (MAJOR-NEW-3)
    if (intent.date_field == "last_modified"
            or intent.request_type == "metadata"
            or getattr(intent, "author", None)
            or getattr(intent, "labels", [])):
        need_doc_meta_join = True

    if intent.request_type == "list" and not getattr(intent, "body_keywords", []):
        need_doc_meta_join = True

    # ── Layer 1 WHERE ──────────────────────────────────────────────────────
    # space_key 필터
    space_key = getattr(intent, "space_key", None)
    if space_key:
        where_clauses.append("n.space_key = ?")
        params.append(space_key)

    # game / ancestor_game
    ancestor_game = getattr(intent, "ancestor_game", None)
    if ancestor_game:
        where_clauses.append("n.game_alias_kr = ?")
        params.append(ancestor_game)

    # title_keywords (page_path_segments 기반)
    title_keywords = getattr(intent, "title_keywords", [])
    for kw in title_keywords:
        where_clauses.append("LOWER(n.title) LIKE ? ESCAPE '\\'")
        params.append("%" + _escape_like(kw.lower()) + "%")

    # page_path_segments path LIKE
    page_path_segments = getattr(intent, "page_path_segments", [])
    if page_path_segments:
        cleaned_segs, removed_time = _strip_time_expressions(page_path_segments)
        if removed_time:
            strip_log.append({
                "stage": "_strip_time_expressions",
                "removed": removed_time,
                "reason": "시간 표현 segment는 ref_date_from/to로 변환됨"
            })
        if cleaned_segs:
            escaped = [_escape_like(s.lower()) for s in cleaned_segs]
            path_pattern = "%" + "%".join(escaped) + "%"
            where_clauses.append("LOWER(n.path) LIKE ? ESCAPE '\\'")
            params.append(path_pattern)

    # date_field 필터 (MAJOR-NEW-5 _next_day)
    date_field = getattr(intent, "date_field", None)
    date_from = getattr(intent, "date_from", None)
    date_to = getattr(intent, "date_to", None)

    if date_field == "last_modified" and date_from:
        need_doc_meta_join = True
        where_clauses.append("dm.last_modified >= ?")
        params.append(date_from)
    if date_field == "last_modified" and date_to:
        need_doc_meta_join = True
        where_clauses.append("dm.last_modified < ?")
        params.append(_next_day(date_to))

    # task-129.8 v2: body_keywords 길이 기반 분리 (FTS5 trigram ≥3자 한계 보강)
    # - ≤2자 한국어 → LIKE fallback (FTS5 trigram 못 잡음)
    # - 3+자 또는 영어 → FTS (기존 동작 유지)
    # - None / non-str / 빈 → 가드 무시 (C-2)
    body_keywords = getattr(intent, "body_keywords", []) or []
    fts_keywords = []
    short_kr_keywords = []
    for _kw in body_keywords:
        if not isinstance(_kw, str) or not _kw:
            continue
        if _is_short_korean(_kw):
            short_kr_keywords.append(_kw)
        else:
            fts_keywords.append(_kw)

    if fts_keywords:
        fts_query = " ".join(fts_keywords)
        has_fts = True

    # task-129.8: 2자 한국어 keywords → doc_content.body_text LIKE 절
    # AND 결합 (모든 short_kr 단어 모두 매칭 — precision 우선)
    # 주의: LIKE params append는 SQL 경로 1(FTS) prepend 이전에 완료되어야 binding 정합
    for _skw in short_kr_keywords:
        where_clauses.append("LOWER(dc.body_text) LIKE ? ESCAPE '\\'")
        params.append("%" + _escape_like(_skw.lower()) + "%")

    # ── SQL 선택 (request_type별 경로) ────────────────────────────────────
    order_by = _resolve_order_by_wiki_list(intent, need_doc_meta_join)
    limit = getattr(intent, "limit", 10)

    dm_cols = ""
    doc_meta_join = ""
    if need_doc_meta_join:
        dm_cols = ", dm.last_modified, dm.author"
        doc_meta_join = "LEFT JOIN doc_meta dm ON dm.node_id = n.id"

    if intent.request_type == "metadata" and getattr(intent, "metadata_field", None) == "last_modified":
        # 경로 2: metadata SQL
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url, n.space_key, n.game_alias_kr,
       n.folder_role{dm_cols},
       SUBSTR(dc.body_text, 1, 500) AS snippet,
       NULL AS fts_rank, NULL AS chunk_origin
FROM nodes n
LEFT JOIN doc_content dc ON dc.node_id = n.id
{doc_meta_join}
WHERE {' AND '.join(where_clauses)}
ORDER BY {order_by}
LIMIT ?
""".strip()
    elif has_fts:
        # 경로 1: FTS SQL
        fts_where = " AND ".join(where_clauses)
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url, n.space_key, n.game_alias_kr,
       n.folder_role{dm_cols},
       SUBSTR(dc.body_text, 1, 500) AS snippet,
       -bm25(search_fts) AS fts_rank, NULL AS chunk_origin
FROM nodes n
JOIN search_fts ON search_fts.rowid = n.id
LEFT JOIN doc_content dc ON dc.node_id = n.id
{doc_meta_join}
WHERE search_fts MATCH ? AND {fts_where}
ORDER BY fts_rank DESC, {order_by}
LIMIT ?
""".strip()
        # FTS 매치 파라미터를 맨 앞에 삽입
        params = [fts_query] + params
    else:
        # 경로 3: list / no-FTS SQL
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url, n.space_key, n.game_alias_kr,
       n.folder_role{dm_cols},
       SUBSTR(dc.body_text, 1, 500) AS snippet,
       NULL AS fts_rank, NULL AS chunk_origin
FROM nodes n
LEFT JOIN doc_content dc ON dc.node_id = n.id
{doc_meta_join}
WHERE {' AND '.join(where_clauses)}
ORDER BY {order_by}
LIMIT ?
""".strip()

    params.append(limit)

    return BuiltQuery(
        sql=sql,
        params=params,
        intent_signature=f"wiki:{intent.request_type}:{','.join(title_keywords)}",
        where_clauses=where_clauses,
        weight_matrix=_WEIGHT_MATRIX,
        fts_query=fts_query,
        domain="wiki",
        has_fts=has_fts,
        request_type=intent.request_type,
        metadata_field=getattr(intent, "metadata_field", None),
        skip_weight=skip_weight,
        need_doc_meta_join=need_doc_meta_join,
        relaxation_strip_log=strip_log,
    )


# ── build_gdi_query ───────────────────────────────────────────────────────────

def build_gdi_query(intent) -> BuiltQuery:
    """GdiIntent → BuiltQuery (설계 v4 §5.5.2).

    MINOR-A: intent 직접 변이 금지 — effective_game 로컬 변수 사용.
    """
    where_clauses = ["n.source_type = 'gdi'"]
    params = []
    fts_query = None
    has_fts = False
    skip_weight = intent.request_type in ("metadata", "list")
    strip_log = []

    # ── MINOR-A: game_alias 로컬 변수 (intent 직접 변이 금지) ────────────
    effective_game = list(intent.game_alias_kr) if intent.game_alias_kr else []
    if not effective_game:
        # cleaned_segs[0] canonical 탐지는 아래 path 처리 후
        pass

    # ── path_segments (MAJOR-NEW-2: _strip_time_expressions) ─────────────
    cleaned_segs, removed_time = _strip_time_expressions(intent.path_segments or [])
    if removed_time:
        strip_log.append({
            "stage": "_strip_time_expressions",
            "removed": removed_time,
            "reason": "시간 표현 segment는 ref_date_from/to로 변환됨"
        })

    # game canonical 자동 탐지 (MINOR-A: effective_game 로컬에만 반영)
    if cleaned_segs and not effective_game:
        canon = _detect_game_canonical(cleaned_segs[0])
        if canon:
            effective_game = [canon]  # intent.game_alias_kr는 절대 변경 안 함

    if cleaned_segs:
        escaped_segs = [_escape_like(s.lower()) for s in cleaned_segs]
        path_pattern = "%" + "%".join(escaped_segs) + "%"
        where_clauses.append("LOWER(n.path) LIKE ? ESCAPE '\\'")
        params.append(path_pattern)

    # game_alias_kr 필터 (effective_game 사용)
    if effective_game:
        placeholders = ",".join("?" * len(effective_game))
        where_clauses.append(f"n.game_alias_kr IN ({placeholders})")
        params.extend(effective_game)

    # folder_role 필터
    if intent.folder_role:
        placeholders = ",".join("?" * len(intent.folder_role))
        where_clauses.append(f"n.folder_role IN ({placeholders})")
        params.extend(intent.folder_role)

    # file_kind 필터
    if intent.file_kind:
        placeholders = ",".join("?" * len(intent.file_kind))
        where_clauses.append(f"n.file_kind IN ({placeholders})")
        params.extend(intent.file_kind)

    # ref_date 필터 (MAJOR-NEW-5: _next_day)
    if intent.ref_date_from:
        where_clauses.append("n.ref_date >= ?")
        params.append(intent.ref_date_from)
    if intent.ref_date_to:
        where_clauses.append("n.ref_date < ?")
        params.append(_next_day(intent.ref_date_to))

    # task-129.8.5: body_keywords 길이 분리 (FTS5 trigram ≥3자 한계 보강 — gdi)
    # - ≤2자 한국어 → EXISTS subquery on doc_chunks.text (1:N relation, node 단위 매칭)
    # - 3+자 / 영어 → 기존 chunks_fts MATCH (chunk 단위)
    # - None / non-str / 빈 → 가드 무시
    body_keywords = getattr(intent, "body_keywords", []) or []
    fts_keywords = []
    short_kr_keywords = []
    for _kw in body_keywords:
        if not isinstance(_kw, str) or not _kw:
            continue
        if _is_short_korean(_kw):
            short_kr_keywords.append(_kw)
        else:
            fts_keywords.append(_kw)

    if fts_keywords:
        fts_query = " ".join(fts_keywords)
        has_fts = True

    # task-129.8.5: short_kr_keywords → EXISTS subquery on doc_chunks.text
    # 1:N relation 처리 — 메인 dc(JOIN doc_chunks)와 충돌 회피 위해 dc_lk 별칭 사용
    for _skw in short_kr_keywords:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM doc_chunks dc_lk "
            "WHERE dc_lk.node_id = n.id "
            "AND LOWER(dc_lk.text) LIKE ? ESCAPE '\\')"
        )
        params.append("%" + _escape_like(_skw.lower()) + "%")

    limit = getattr(intent, "limit", 10)

    # ── SQL 선택 ───────────────────────────────────────────────────────────
    if intent.request_type == "metadata":
        # 경로 2: metadata
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url,
       n.folder_role, n.game_alias_kr, n.file_kind, n.ref_date,
       NULL AS chunk_id, NULL AS section_path, NULL AS chunk_origin,
       NULL AS snippet, NULL AS fts_rank
FROM nodes n
WHERE {' AND '.join(where_clauses)}
ORDER BY n.ref_date DESC NULLS LAST, n.title ASC
LIMIT ?
""".strip()
    elif has_fts:
        # 경로 1: FTS
        fts_where = " AND ".join(where_clauses)
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url,
       n.folder_role, n.game_alias_kr, n.file_kind, n.ref_date,
       dc.id AS chunk_id, dc.section_path, dc.chunk_origin,
       SUBSTR(dc.text, 1, 500) AS snippet,
       -bm25(chunks_fts) AS fts_rank
FROM nodes n
JOIN doc_chunks dc ON dc.node_id = n.id
JOIN chunks_fts ON chunks_fts.rowid = dc.id
WHERE chunks_fts MATCH ? AND {fts_where}
ORDER BY fts_rank DESC, n.ref_date DESC NULLS LAST
LIMIT ?
""".strip()
        params = [fts_query] + params
    else:
        # 경로 3: list / no-FTS
        sql = f"""
SELECT n.id AS node_id, n.title, n.path, n.url,
       n.folder_role, n.game_alias_kr, n.file_kind, n.ref_date,
       NULL AS chunk_id, NULL AS section_path, NULL AS chunk_origin,
       NULL AS snippet, NULL AS fts_rank
FROM nodes n
WHERE {' AND '.join(where_clauses)}
ORDER BY n.ref_date DESC NULLS LAST, n.title ASC
LIMIT ?
""".strip()

    params.append(limit)

    return BuiltQuery(
        sql=sql,
        params=params,
        intent_signature=f"gdi:{intent.request_type}:{','.join(cleaned_segs)}",
        where_clauses=where_clauses,
        weight_matrix=_WEIGHT_MATRIX,
        fts_query=fts_query,
        domain="gdi",
        has_fts=has_fts,
        request_type=intent.request_type,
        metadata_field=getattr(intent, "metadata_field", None),
        skip_weight=skip_weight,
        need_doc_meta_join=False,   # gdi는 doc_meta JOIN 불필요
        relaxation_strip_log=strip_log,
    )
