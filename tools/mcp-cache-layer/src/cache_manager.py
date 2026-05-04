"""캐시 매니저 — MCP 캐시 CRUD + 유효성 검사.

사용법:
    from src.cache_manager import CacheManager
    cm = CacheManager()
    cm.put_page("wiki", "123456", "QA 체크리스트", space_key="QASGP", ...)
    result = cm.get_page("QA 체크리스트", "QASGP")
"""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta

from . import config
from .cache_logger import ops_log, perf
from .models import get_connection, init_db

log = logging.getLogger("mcp_cache")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _content_hash(text: str | None) -> str | None:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class CacheManager:
    """MCP 캐시 매니저. 싱글 SQLite 파일 기반."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or config.DB_PATH
        from .models import _get_current_version, get_connection
        version = init_db(self._db_path)
        # init_db 반환값은 migrate의 최종 결과 — 실제 현재 버전 조회
        conn = get_connection(self._db_path)
        try:
            schema_v = _get_current_version(conn)
        finally:
            conn.close()
        ops_log.db_init(self._db_path, schema_version=schema_v)

    def _conn(self) -> sqlite3.Connection:
        return get_connection(self._db_path)

    def get_db_path(self) -> str:
        """task-129.5 wiring: relaxation_engine.search_with_ladder 호환 db_path 노출."""
        return self._db_path

    def _get_node_by_id(self, node_id: int) -> dict | None:
        """내부용: node_id로 노드 조회 (로깅용 제목 참조)."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT id, title FROM nodes WHERE id = ?", (node_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    # ── 노드 (L1) ───────────────────────────────────────────

    def get_node(self, source_type: str, source_id: str) -> dict | None:
        """source_type + source_id로 노드 조회."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM nodes WHERE source_type = ? AND source_id = ?",
                (source_type, source_id),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_node_by_title(
        self, title: str, source_type: str = "wiki", space_key: str | None = None
    ) -> dict | None:
        """제목으로 노드 조회 (case-insensitive)."""
        conn = self._conn()
        try:
            if space_key:
                row = conn.execute(
                    "SELECT * FROM nodes "
                    "WHERE title = ? COLLATE NOCASE AND source_type = ? AND space_key = ?",
                    (title, source_type, space_key),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM nodes "
                    "WHERE title = ? COLLATE NOCASE AND source_type = ?",
                    (title, source_type),
                ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_node(
        self,
        source_type: str,
        source_id: str,
        title: str,
        *,
        parent_id: int | None = None,
        path: str | None = None,
        node_type: str = "page",
        space_key: str | None = None,
        url: str | None = None,
    ) -> int:
        """노드 삽입 또는 업데이트. node_id 반환."""
        conn = self._conn()
        now = _now()
        try:
            existing = conn.execute(
                "SELECT id FROM nodes WHERE source_type = ? AND source_id = ?",
                (source_type, source_id),
            ).fetchone()

            if existing:
                node_id = existing["id"]
                conn.execute(
                    "UPDATE nodes SET title=?, parent_id=?, path=?, node_type=?, "
                    "space_key=?, url=?, updated_at=? WHERE id=?",
                    (title, parent_id, path, node_type, space_key, url, now, node_id),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO nodes "
                    "(source_type, source_id, title, parent_id, path, node_type, "
                    "space_key, url, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (source_type, source_id, title, parent_id, path, node_type,
                     space_key, url, now, now),
                )
                node_id = cur.lastrowid

            conn.commit()
            return node_id
        finally:
            conn.close()

    # ── 메타 (L2) ───────────────────────────────────────────

    def get_meta(self, node_id: int) -> dict | None:
        """노드의 메타데이터 조회."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM doc_meta WHERE node_id = ?", (node_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_meta(
        self,
        node_id: int,
        *,
        last_modified: str | None = None,
        version: int | None = None,
        content_hash: str | None = None,
        author: str | None = None,
        status: str | None = None,
        labels: list[str] | None = None,
        extra_meta: dict | None = None,
        ttl_hours: int | None = None,
    ) -> None:
        """메타데이터 삽입 또는 업데이트."""
        conn = self._conn()
        now = _now()
        ttl = ttl_hours or config.DEFAULT_TTL_HOURS
        labels_json = json.dumps(labels, ensure_ascii=False) if labels else None
        extra_json = json.dumps(extra_meta, ensure_ascii=False) if extra_meta else None
        try:
            existing = conn.execute(
                "SELECT id FROM doc_meta WHERE node_id = ?", (node_id,)
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE doc_meta SET last_modified=?, version=?, content_hash=?, "
                    "author=?, status=?, labels=?, extra_meta=?, cached_at=?, ttl_hours=? "
                    "WHERE node_id=?",
                    (last_modified, version, content_hash, author, status,
                     labels_json, extra_json, now, ttl, node_id),
                )
            else:
                conn.execute(
                    "INSERT INTO doc_meta "
                    "(node_id, last_modified, version, content_hash, author, "
                    "status, labels, extra_meta, cached_at, ttl_hours) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (node_id, last_modified, version, content_hash, author,
                     status, labels_json, extra_json, now, ttl),
                )

            conn.commit()
        finally:
            conn.close()

    # ── 본문 (L3) ───────────────────────────────────────────

    def get_content(self, node_id: int) -> dict | None:
        """노드의 본문 캐시 조회."""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM doc_content WHERE node_id = ?", (node_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_content(
        self,
        node_id: int,
        body_raw: str | None = None,
        body_text: str | None = None,
    ) -> None:
        """본문 캐시 삽입 또는 업데이트.

        task-086: 본문 갱신 시 search_fts 테이블도 자동 동기화 (DELETE+INSERT).
        Wiki/Jira/GDI 모든 source_type에 공통 적용. FTS 미존재 시 silent skip.
        """
        conn = self._conn()
        now = _now()

        # 글자 수 제한
        truncated = 0
        if body_text and len(body_text) > config.MAX_BODY_CHARS:
            body_text = body_text[: config.MAX_BODY_CHARS]
            truncated = 1
        if body_raw and len(body_raw) > config.MAX_BODY_CHARS:
            body_raw = body_raw[: config.MAX_BODY_CHARS]

        char_count = len(body_text) if body_text else 0

        try:
            existing = conn.execute(
                "SELECT id FROM doc_content WHERE node_id = ?", (node_id,)
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE doc_content SET body_raw=?, body_text=?, "
                    "body_truncated=?, char_count=?, cached_at=? WHERE node_id=?",
                    (body_raw, body_text, truncated, char_count, now, node_id),
                )
            else:
                conn.execute(
                    "INSERT INTO doc_content "
                    "(node_id, body_raw, body_text, body_truncated, char_count, cached_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (node_id, body_raw, body_text, truncated, char_count, now),
                )

            # task-086: FTS5 자동 sync (Wiki/Jira/GDI 공통)
            # search_fts 테이블은 schema v4+에만 존재 — 없으면 silent skip
            self._sync_fts(conn, node_id, body_text)

            conn.commit()
        finally:
            conn.close()

    def _sync_fts(
        self,
        conn: sqlite3.Connection,
        node_id: int,
        body_text: str | None,
    ) -> None:
        """search_fts 테이블에 node 본문을 DELETE+INSERT로 동기화 (task-086).

        - FTS 테이블 부재 시 silent skip (schema v3 이하 호환)
        - body_text 비어있으면 DELETE만 수행 (FTS에서 해당 rowid 제거)
        - nodes.title 조회해서 함께 인덱싱 (검색 품질)
        - summary/keywords 존재 시 포함 (GDI enrichment 지원)

        같은 connection/트랜잭션 내에서 실행 — 본문 저장과 원자성 보장.
        """
        try:
            # FTS 테이블 존재 확인
            fts_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='search_fts'"
            ).fetchone()
            if not fts_exists:
                return  # schema v3 이하 — silent skip

            # 기존 FTS 엔트리 제거 (contentless_delete=1 필요 — schema v4 정의에 포함)
            conn.execute("DELETE FROM search_fts WHERE rowid = ?", (node_id,))

            # body_text 비어있으면 INSERT 생략 (삭제만)
            if not body_text:
                return

            # title + summary + keywords 조회
            meta_row = conn.execute(
                """
                SELECT n.title, dc.summary, dc.keywords
                FROM nodes n
                LEFT JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.id = ?
                """,
                (node_id,),
            ).fetchone()

            if not meta_row:
                return  # node 자체가 없으면 skip

            title = meta_row["title"] if "title" in meta_row.keys() else ""
            summary = (meta_row["summary"] if "summary" in meta_row.keys() else "") or ""
            keywords = (meta_row["keywords"] if "keywords" in meta_row.keys() else "") or ""

            conn.execute(
                "INSERT INTO search_fts(rowid, title, body_text, summary, keywords) "
                "VALUES (?, ?, ?, ?, ?)",
                (node_id, title or "", body_text, summary, keywords),
            )
        except sqlite3.Error as e:
            # FTS sync 실패해도 본문 저장은 성공 — best-effort
            log.warning(f"FTS sync 실패 (node_id={node_id}): {e}")

    # ── 통합 조회/저장 ──────────────────────────────────────

    def get_page(
        self, title: str, space_key: str | None = None, source_type: str = "wiki"
    ) -> dict | None:
        """제목으로 페이지 캐시 전체 조회 (node + meta + content).

        Returns:
            dict with keys: node, meta, content  (각각 dict or None)
            None if 노드 자체가 없음
        """
        t0 = perf.now_ms()
        node = self.get_node_by_title(title, source_type, space_key)
        if not node:
            ops_log.cache_miss(title, reason="not_found",
                               elapsed_ms=perf.elapsed_ms(t0))
            return None

        meta = self.get_meta(node["id"])
        content = self.get_content(node["id"])
        elapsed = perf.elapsed_ms(t0)
        ops_log.cache_hit(title, source="sqlite", node_id=node["id"],
                          elapsed_ms=elapsed)
        return {"node": node, "meta": meta, "content": content}

    def put_page(
        self,
        source_type: str,
        source_id: str,
        title: str,
        *,
        space_key: str | None = None,
        parent_id: int | None = None,
        path: str | None = None,
        node_type: str = "page",
        url: str | None = None,
        last_modified: str | None = None,
        version: int | None = None,
        author: str | None = None,
        status: str | None = None,
        labels: list[str] | None = None,
        body_raw: str | None = None,
        body_text: str | None = None,
    ) -> int:
        """페이지 전체 캐시 저장 (node + meta + content). node_id 반환."""
        t0 = perf.now_ms()
        node_id = self.upsert_node(
            source_type, source_id, title,
            parent_id=parent_id, path=path, node_type=node_type,
            space_key=space_key, url=url,
        )

        c_hash = _content_hash(body_text)
        self.upsert_meta(
            node_id,
            last_modified=last_modified, version=version,
            content_hash=c_hash, author=author, status=status, labels=labels,
        )

        has_body = bool(body_raw or body_text)
        if has_body:
            self.upsert_content(node_id, body_raw=body_raw, body_text=body_text)

        ops_log.cache_store(
            title, node_id=node_id, source_id=source_id,
            char_count=len(body_text) if body_text else 0,
            has_body=has_body, elapsed_ms=perf.elapsed_ms(t0),
        )
        return node_id

    # ── Repair 헬퍼 ─────────────────────────────────────────

    def get_nodes_missing_content(self, source_type: str) -> list[dict]:
        """doc_content 행이 없는 노드 목록 반환 (body 누락 복구용)."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT n.id, n.source_id, n.title
                FROM nodes n
                LEFT JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = ? AND dc.id IS NULL
                """,
                (source_type,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_orphan_nodes(self, source_type: str) -> list[dict]:
        """parent_id NULL이면서 자식이 없는 리프 고아 노드 반환."""
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT n.id, n.source_id, n.title
                FROM nodes n
                WHERE n.source_type = ?
                  AND n.parent_id IS NULL
                  AND n.id NOT IN (
                      SELECT DISTINCT parent_id
                      FROM nodes
                      WHERE parent_id IS NOT NULL
                  )
                """,
                (source_type,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_parent_id(self, node_id: int, parent_id: int) -> None:
        """노드의 parent_id 업데이트."""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE nodes SET parent_id = ? WHERE id = ?",
                (parent_id, node_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ── 본문 FTS 검색 (task-088) ──────────────────────────

    def search_content(
        self,
        query_text: str,
        source_type: str,
        space_key: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """로컬 캐시의 본문을 FTS5 MATCH로 검색 (task-088).

        Wiki/Jira MCP 호출이 실패/0건일 때 폴백으로 사용.
        GDI는 이미 gdi_client._local_unified_search가 유사 로직 보유.

        Args:
            query_text: 검색 키워드 (공백 구분, AND 결합)
            source_type: "wiki" | "jira" | "gdi"
            space_key: Wiki 공간 또는 GDI 게임명 제한 (선택)
            limit: 반환 개수 상한

        Returns:
            [{"node_id": int, "title": str, "source_id": str, "path": str,
              "url": str|None, "snippet": str, "rank": float}, ...]
            FTS 테이블 없거나 파싱 실패 시 빈 리스트.
        """
        # 키워드 분리 → phrase AND 쿼리 (한국어 phrase match 보장)
        keywords = [kw.strip() for kw in (query_text or "").split() if kw.strip()]
        if not keywords:
            return []

        def escape_phrase(kw: str) -> str:
            return '"' + kw.replace('"', '""') + '"'

        fts_query = " AND ".join(escape_phrase(kw) for kw in keywords)

        conn = self._conn()
        try:
            # FTS 테이블 존재 확인 (schema v3 호환)
            fts_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='search_fts'"
            ).fetchone()
            if not fts_exists:
                return []

            params = [fts_query, source_type]
            space_filter = ""
            if space_key:
                # wiki는 nodes.space_key, gdi는 path LIKE 혼재. 기본은 space_key 일치.
                space_filter = "AND LOWER(n.space_key) = ?"
                params.append(space_key.lower())
            params.append(limit)

            sql = f"""
                SELECT n.id AS node_id, n.title, n.source_id, n.path, n.url,
                       SUBSTR(dc.body_text, 1, 500) AS snippet,
                       bm25(search_fts) AS rank
                FROM search_fts
                JOIN nodes n ON n.id = search_fts.rowid
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE search_fts MATCH ?
                  AND n.source_type = ?
                  {space_filter}
                ORDER BY rank
                LIMIT ?
            """
            rows = conn.execute(sql, params).fetchall()
            results = []
            for r in rows:
                results.append({
                    "node_id": r["node_id"],
                    "title": r["title"] or "",
                    "source_id": r["source_id"] or "",
                    "path": r["path"] or "",
                    "url": r["url"],
                    "snippet": (r["snippet"] or "")[:300],
                    "rank": r["rank"],
                })
            return results
        except sqlite3.OperationalError as e:
            # FTS 쿼리 파싱 실패 (특수문자 등) — 빈 결과
            log.warning(f"[search_content] FTS MATCH 오류: query={query_text!r} err={e}")
            return []
        except Exception as e:
            log.warning(f"[search_content] 실행 실패: {e}")
            return []
        finally:
            conn.close()

    # ── 캐시 유효성 ─────────────────────────────────────────

    def is_stale(self, node_id: int) -> bool:
        """캐시 TTL 만료 여부. 메타가 없으면 True (stale 취급)."""
        meta = self.get_meta(node_id)
        if not meta:
            return True

        cached_at = datetime.fromisoformat(meta["cached_at"])
        ttl = timedelta(hours=meta["ttl_hours"])
        now = datetime.now()
        age = now - cached_at
        age_hours = round(age.total_seconds() / 3600, 1)

        if now > cached_at + ttl:
            # 노드 제목 조회 (디버그용)
            node = self._get_node_by_id(node_id)
            title = node["title"] if node else f"node#{node_id}"
            ops_log.cache_stale(node_id, title,
                                ttl_hours=meta["ttl_hours"],
                                age_hours=age_hours)
            return True

        remaining = round((ttl - age).total_seconds() / 3600, 1)
        node = self._get_node_by_id(node_id)
        title = node["title"] if node else f"node#{node_id}"
        ops_log.cache_fresh(node_id, title,
                            ttl_hours=meta["ttl_hours"],
                            remaining_hours=remaining)
        return False

    def is_stale_vs_remote(
        self, node_id: int, remote_modified: str | None, remote_version: int | None = None
    ) -> bool:
        """원격 메타와 비교하여 stale 여부 판단.

        Primary: last_modified + version 비교
        TTL 만료 시에도 True.
        """
        if self.is_stale(node_id):
            return True

        meta = self.get_meta(node_id)
        if not meta:
            return True

        # last_modified 비교
        if remote_modified and meta["last_modified"] != remote_modified:
            node = self._get_node_by_id(node_id)
            title = node["title"] if node else f"node#{node_id}"
            ops_log.cache_stale(node_id, title,
                                remote_modified=remote_modified,
                                cached_modified=meta["last_modified"])
            return True

        # version 비교
        if remote_version and meta["version"] != remote_version:
            return True

        return False

    def mark_fresh(self, node_id: int) -> None:
        """cached_at을 현재 시각으로 갱신 (TTL 리셋)."""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE doc_meta SET cached_at = ? WHERE node_id = ?",
                (_now(), node_id),
            )
            conn.commit()
            node = self._get_node_by_id(node_id)
            title = node["title"] if node else f"node#{node_id}"
            ops_log.cache_refresh(node_id, title)
        finally:
            conn.close()

    # ── 트리 조회 ───────────────────────────────────────────

    def get_tree(
        self, source_type: str, space_key: str | None = None
    ) -> list[dict]:
        """소스 타입(+공간키)의 전체 노드 목록 반환."""
        conn = self._conn()
        try:
            if space_key:
                rows = conn.execute(
                    "SELECT * FROM nodes "
                    "WHERE source_type = ? AND space_key = ? ORDER BY path, title",
                    (source_type, space_key),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM nodes WHERE source_type = ? ORDER BY path, title",
                    (source_type,),
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_children(self, parent_node_id: int) -> list[dict]:
        """직계 자식 노드 목록."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE parent_id = ? ORDER BY node_type, title",
                (parent_node_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ── 정리 ────────────────────────────────────────────────

    def cleanup(self, days: int | None = None) -> int:
        """오래된 캐시 삭제. 삭제 건수 반환."""
        d = days or config.CLEANUP_DAYS
        conn = self._conn()
        try:
            cur = conn.execute(
                "DELETE FROM nodes WHERE id IN ("
                "  SELECT n.id FROM nodes n "
                "  JOIN doc_meta m ON m.node_id = n.id "
                "  WHERE m.cached_at < datetime('now', 'localtime', ? || ' days')"
                ")",
                (f"-{d}",),
            )
            conn.commit()
            count = cur.rowcount
            if count:
                log.info("%d일 미조회 캐시 %d건 삭제", d, count)
            ops_log.cleanup(count, days=d)
            return count
        finally:
            conn.close()

    # ── 통계 ────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """캐시 통계 반환."""
        conn = self._conn()
        try:
            stats = {}
            stats["total_nodes"] = conn.execute(
                "SELECT COUNT(*) FROM nodes"
            ).fetchone()[0]

            stats["by_source"] = {}
            for row in conn.execute(
                "SELECT source_type, COUNT(*) as cnt FROM nodes GROUP BY source_type"
            ):
                stats["by_source"][row["source_type"]] = row["cnt"]

            stats["total_content"] = conn.execute(
                "SELECT COUNT(*) FROM doc_content"
            ).fetchone()[0]

            stats["total_chars"] = conn.execute(
                "SELECT COALESCE(SUM(char_count), 0) FROM doc_content"
            ).fetchone()[0]

            # 최근 동기화
            sync_row = conn.execute(
                "SELECT source_type, scope, sync_type, started_at, status "
                "FROM sync_log ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            stats["last_sync"] = dict(sync_row) if sync_row else None

            # DB 파일 크기
            from pathlib import Path
            db_file = Path(self._db_path)
            stats["db_size_kb"] = round(db_file.stat().st_size / 1024, 1) if db_file.exists() else 0

            return stats
        finally:
            conn.close()

    # ── Enrichment 헬퍼 ───────────────────────────────────────

    def get_nodes_missing_enrichment(
        self, source_type: str | None = None, limit: int = 500
    ) -> list[dict]:
        """summary 또는 keywords가 NULL인 노드 목록.

        Returns: [{"id", "source_id", "title", "source_type",
                   "body_text", "summary", "keywords"}, ...]
        """
        conn = self._conn()
        try:
            where = "WHERE (dc.summary IS NULL OR dc.keywords IS NULL)"
            params: list = []
            if source_type:
                where += " AND n.source_type = ?"
                params.append(source_type)
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT n.id, n.source_id, n.title, n.source_type,
                       dc.body_text, dc.summary, dc.keywords
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                {where}
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_enrichment(
        self, node_id: int,
        summary: str | None = None,
        keywords: str | None = None,
    ) -> None:
        """summary/keywords만 업데이트 (body_text 변경 없음)."""
        conn = self._conn()
        try:
            fields: list[str] = []
            params: list = []
            if summary is not None:
                fields.append("summary = ?")
                params.append(summary)
            if keywords is not None:
                fields.append("keywords = ?")
                params.append(keywords)
            if not fields:
                return
            params.append(node_id)
            conn.execute(
                f"UPDATE doc_content SET {', '.join(fields)} WHERE node_id = ?",
                params,
            )
            conn.commit()
        finally:
            conn.close()

    def get_enrichment_stats(self) -> dict:
        """enrichment 채움 비율 통계.

        Returns: {"total_content", "has_summary", "has_keywords",
                  "has_both", "summary_pct", "keywords_pct",
                  "by_source": {src: {total, summary, keywords, pct}}}
        """
        conn = self._conn()
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM doc_content"
            ).fetchone()[0]
            has_summary = conn.execute(
                "SELECT COUNT(*) FROM doc_content WHERE summary IS NOT NULL"
            ).fetchone()[0]
            has_keywords = conn.execute(
                "SELECT COUNT(*) FROM doc_content WHERE keywords IS NOT NULL"
            ).fetchone()[0]
            has_both = conn.execute(
                "SELECT COUNT(*) FROM doc_content "
                "WHERE summary IS NOT NULL AND keywords IS NOT NULL"
            ).fetchone()[0]

            by_source: dict = {}
            for row in conn.execute(
                """
                SELECT n.source_type,
                       COUNT(*) as total,
                       SUM(CASE WHEN dc.summary IS NOT NULL THEN 1 ELSE 0 END) as s,
                       SUM(CASE WHEN dc.keywords IS NOT NULL THEN 1 ELSE 0 END) as k
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                GROUP BY n.source_type
                """
            ):
                src = row["source_type"]
                t = row["total"]
                by_source[src] = {
                    "total": t,
                    "summary": row["s"],
                    "keywords": row["k"],
                    "pct": round(row["s"] / t * 100, 1) if t else 0,
                }

            return {
                "total_content": total,
                "has_summary": has_summary,
                "has_keywords": has_keywords,
                "has_both": has_both,
                "summary_pct": round(has_summary / total * 100, 1) if total else 0,
                "keywords_pct": round(has_keywords / total * 100, 1) if total else 0,
                "by_source": by_source,
            }
        finally:
            conn.close()

# ── CLI 진입점 ───────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cm = CacheManager()
    print("캐시 통계:", json.dumps(cm.get_stats(), indent=2, ensure_ascii=False))
