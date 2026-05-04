"""
relaxation_engine.py — 검색 완화 엔진 (task-129 S4-D)

설계 v4 §5.7 정합:
- SearchHit / SearchResult dataclass
- search_with_ladder: L0~L3 완화 사다리
- search_with_request_type_fallback: metadata→content_search fallback
  MAJOR-NEW-4: dataclasses.replace 의무 (intent in-place 변이 절대 금지)
"""

import logging
import dataclasses
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── SearchHit dataclass (MINOR-E) ─────────────────────────────────────────────

@dataclass
class SearchHit:
    """검색 결과 단건 (설계 v4 §6.1 MINOR-E)."""
    node_id: int
    chunk_id: Optional[int]
    title: str
    snippet: str
    score: float
    metadata: dict = field(default_factory=dict)  # SQL row → dict 변환


# ── SearchResult dataclass ────────────────────────────────────────────────────

@dataclass
class SearchResult:
    """검색 결과 집합 (설계 v4 §6.1)."""
    hits: list = field(default_factory=list)
    total_count: int = 0
    relaxation_level: int = 0
    history: list = field(default_factory=list)  # [(level, desc, count)]


# ── SQL row → SearchHit 변환 헬퍼 (MINOR-E) ──────────────────────────────────

def _row_to_search_hit(row, request_type: str = "content_search") -> SearchHit:
    """SQL row (dict-like) → SearchHit 변환.

    MINOR-E: metadata 경로 hit는 score=0.0, chunk_id=None.
    """
    if isinstance(row, dict):
        r = row
    else:
        # sqlite3.Row 또는 tuple 대응
        try:
            r = dict(row)
        except (TypeError, ValueError):
            r = {}

    is_metadata = request_type in ("metadata", "list")

    return SearchHit(
        node_id=r.get("node_id", 0),
        chunk_id=None if is_metadata else r.get("chunk_id"),
        title=r.get("title", ""),
        snippet=r.get("snippet", "") or "",
        score=0.0 if is_metadata else float(r.get("fts_rank") or 0.0),
        metadata={
            k: r.get(k)
            for k in ("last_modified", "author", "ref_date", "file_kind",
                      "folder_role", "game_alias_kr", "space_key", "path", "url")
            if k in r
        },
    )


# ── search_with_ladder ────────────────────────────────────────────────────────

def search_with_ladder(cache_mgr, intent, domain: str) -> SearchResult:
    """L0~L3 완화 사다리 검색 (설계 v4 §5.7).

    L0: 그대로 검색
    L1: body_keywords 제거
    L2: path_segments / title_keywords 완화 (첫 1개만)
    L3: 전체 content_search fallback (request_type=content_search)
    """
    from query_builder import build_wiki_query, build_gdi_query, _apply_weight_and_sort

    build_fn = build_wiki_query if domain == "wiki" else build_gdi_query

    result = SearchResult()
    history = []

    # L0: 원본 intent 그대로
    built = build_fn(intent)
    rows = _execute_query(cache_mgr, built)
    history.append((0, "original", len(rows)))

    if rows:
        result.hits = [_row_to_search_hit(r, intent.request_type) for r in
                       _apply_weight_and_sort([dict(r) if not isinstance(r, dict) else r
                                               for r in rows], built)]
        result.total_count = len(result.hits)
        result.relaxation_level = 0
        result.history = history
        return result

    # L1: body_keywords 제거
    l1_intent = dataclasses.replace(intent, body_keywords=[])
    built1 = build_fn(l1_intent)
    rows1 = _execute_query(cache_mgr, built1)
    history.append((1, "remove_body_keywords", len(rows1)))

    if rows1:
        result.hits = [_row_to_search_hit(r, l1_intent.request_type) for r in rows1]
        result.total_count = len(result.hits)
        result.relaxation_level = 1
        result.history = history
        return result

    # L2: path 완화 (첫 segment만 유지)
    if domain == "wiki":
        kw = getattr(intent, "title_keywords", [])
        l2_kw = kw[:1] if kw else []
        l2_intent = dataclasses.replace(intent, body_keywords=[], title_keywords=l2_kw)
    else:
        segs = getattr(intent, "path_segments", [])
        l2_segs = segs[:1] if segs else []
        l2_intent = dataclasses.replace(intent, body_keywords=[], path_segments=l2_segs)

    built2 = build_fn(l2_intent)
    rows2 = _execute_query(cache_mgr, built2)
    history.append((2, "relax_path", len(rows2)))

    if rows2:
        result.hits = [_row_to_search_hit(r, l2_intent.request_type) for r in rows2]
        result.total_count = len(result.hits)
        result.relaxation_level = 2
        result.history = history
        return result

    # L3: content_search fallback (dataclasses.replace 의무 — MAJOR-NEW-4)
    l3_intent = dataclasses.replace(intent, request_type="content_search", body_keywords=[])
    built3 = build_fn(l3_intent)
    rows3 = _execute_query(cache_mgr, built3)
    history.append((3, "content_search_fallback", len(rows3)))

    result.hits = [_row_to_search_hit(r, "content_search") for r in rows3]
    result.total_count = len(result.hits)
    result.relaxation_level = 3
    result.history = history
    return result


def _execute_query(cache_mgr, built) -> list:
    """BuiltQuery 실행 → row list (빈 list on error).

    task-129.7 LOW-1 시정: try/finally connection close 보장 (예외 시 누수 방지).
    task-132 PR1-H: 동일 node_id 중복 결과 제거 (chunks_fts MATCH 시 한 파일이 여러 chunks 매칭).
    """
    import sqlite3
    db_path = cache_mgr.get_db_path()
    if db_path == ":memory:":
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(built.sql, built.params).fetchall()
        # PR1-H: node_id 기준 dedup (첫 매칭 보존 — fts_rank/ref_date 정렬 이후이므로 best chunk)
        seen_ids = set()
        deduped = []
        for r in rows:
            try:
                nid = r["node_id"]
            except (KeyError, IndexError, TypeError):
                deduped.append(r)
                continue
            if nid is not None and nid in seen_ids:
                continue
            if nid is not None:
                seen_ids.add(nid)
            deduped.append(r)
        return deduped
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        logger.warning("[relaxation_engine] 쿼리 실행 실패: %s", e)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ── search_with_request_type_fallback ────────────────────────────────────────

def search_with_request_type_fallback(cache_mgr, intent, domain: str):
    """metadata 0건 → content_search fallback (MAJOR-NEW-4).

    MAJOR-NEW-4 핵심:
      fallback_intent = dataclasses.replace(intent, request_type="content_search")
      intent.request_type = "content_search"  절대 금지 (cache 오염)

    Returns:
        (SearchResult, actual_intent_used)
    """
    from query_builder import build_wiki_query, build_gdi_query, _apply_weight_and_sort

    build_fn = build_wiki_query if domain == "wiki" else build_gdi_query

    # 1차: 원본 intent로 검색
    built = build_fn(intent)
    rows = _execute_query(cache_mgr, built)

    if rows:
        hits = [_row_to_search_hit(r, intent.request_type)
                for r in ([dict(r) if not isinstance(r, dict) else r for r in rows])]
        return SearchResult(hits=hits, total_count=len(hits), relaxation_level=0), intent

    # 2차: metadata 0건 → content_search fallback
    # MAJOR-NEW-4: dataclasses.replace 의무 (in-place 변이 절대 금지)
    fallback_intent = dataclasses.replace(intent, request_type="content_search")
    built_fb = build_fn(fallback_intent)
    rows_fb = _execute_query(cache_mgr, built_fb)

    hits_fb = [_row_to_search_hit(r, "content_search")
               for r in ([dict(r) if not isinstance(r, dict) else r for r in rows_fb])]
    return (
        SearchResult(hits=hits_fb, total_count=len(hits_fb), relaxation_level=1),
        fallback_intent,
    )
