"""동기화 엔진 — Full Ingest + Delta Sync.

Wiki MCP를 통해 Confluence 공간의 페이지 트리를 캐시 DB에 적재합니다.

사용법:
    # Slack Bot에서:
    from src.sync_engine import SyncEngine
    engine = SyncEngine(cache_manager, mcp_session)
    result = engine.delta_sync("wiki", "QASGP")

    # CLI:
    python -m src.sync_engine --full wiki QASGP
    python -m src.sync_engine --delta wiki QASGP
"""

import json
import logging
import re
import html as _html
import time
from datetime import datetime

from . import config
from .cache_logger import ops_log, perf
from .cache_manager import CacheManager

log = logging.getLogger("mcp_cache")


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _strip_html(html_text: str) -> str:
    """Confluence 저장 형식 HTML → 일반 텍스트.

    ac:parameter, ac:emoticon 등 Confluence 매크로 메타데이터를 제거하고
    CDATA, 링크 텍스트 등 실제 콘텐츠만 추출합니다.
    """
    text = html_text or ''
    # 1. script / style 전체 제거
    text = re.sub(
        r'<(script|style)[^>]*>.*?</\1>', '',
        text, flags=re.DOTALL | re.IGNORECASE,
    )
    # 2. Confluence 매크로 파라미터 전체 제거 (매크로 설정값 노이즈)
    text = re.sub(
        r'<ac:parameter[^>]*>.*?</ac:parameter>', '',
        text, flags=re.DOTALL,
    )
    # 3. CDATA 내용 추출: <![CDATA[텍스트]]> → 텍스트
    text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    # 4. ac:emoticon 제거
    text = re.sub(r'<ac:emoticon[^/]*/>', '', text)
    # 5. 테이블 구조 보존: 셀 경계 → ' | ', 행 경계 → '\n'
    text = re.sub(r'</t[dh]>\s*<t[dh][^>]*>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'</?tr[^>]*>', '\n', text, flags=re.IGNORECASE)
    # 6. 나머지 HTML/XML 태그 제거
    text = re.sub(r'<[^>]+>', ' ', text)
    # 7. HTML 엔티티 디코딩
    text = _html.unescape(text)
    # 8. 공백 정규화
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class SyncEngine:
    """MCP 동기화 엔진."""

    def __init__(self, cache: CacheManager, mcp_session):
        """
        Parameters
        ----------
        cache       : CacheManager 인스턴스
        mcp_session : McpSession 인스턴스 (wiki_client의 _get_mcp() 반환값)
        """
        self._cache = cache
        self._mcp = mcp_session

    # ── 공개 API ────────────────────────────────────────────

    def full_ingest(self, source_type: str, space_key: str,
                    fetch_body: bool = False) -> dict:
        """전체 페이지 트리 + 메타데이터 수집.

        fetch_body=True이면 각 페이지 본문도 MCP로 가져와 저장 (느림).
        Returns: {"scanned": int, "added": int, "updated": int, "errors": int, "duration_sec": float}
        """
        if source_type != "wiki":
            return {"error": f"미지원 소스: {source_type}"}

        return self._sync_wiki_space(space_key, full=True, fetch_body=fetch_body)

    def delta_sync(self, source_type: str, space_key: str,
                   fetch_body: bool = True) -> dict:
        """마지막 동기화 이후 변경분만 갱신. 본문도 함께 가져옴.

        Returns: {"scanned": int, "added": int, "updated": int, "errors": int, "duration_sec": float}
        """
        if source_type != "wiki":
            return {"error": f"미지원 소스: {source_type}"}

        return self._sync_wiki_space(space_key, full=False, fetch_body=fetch_body)

    # ── Wiki 동기화 ─────────────────────────────────────────

    def _sync_wiki_space(self, space_key: str, full: bool = False,
                         fetch_body: bool = False) -> dict:
        """Wiki 공간 동기화 실행."""
        sync_type = "full" if full else "delta"
        started = _now_iso()
        t0 = time.time()
        stats = {"scanned": 0, "added": 0, "updated": 0, "errors": 0}

        # sync_log 시작 기록
        self._log_sync_start(space_key, sync_type, started)

        last_sync = None
        if not full:
            last_sync = self._get_last_sync_time("wiki", space_key)

        ops_log.sync_start(sync_type, space_key, since=last_sync)

        try:
            if full:
                pages = self._fetch_all_pages(space_key)
            else:
                pages = self._fetch_changed_pages(space_key, last_sync)

            stats["scanned"] = len(pages)
            log.info("[sync] %s %s: %d건 스캔", sync_type, space_key, len(pages))

            for page in pages:
                page_t0 = perf.now_ms()
                try:
                    was_new = self._upsert_page(page, space_key,
                                                fetch_body=fetch_body)
                    action = "added" if was_new else "updated"
                    if was_new:
                        stats["added"] += 1
                    else:
                        stats["updated"] += 1

                    title = (page.get("title") or
                             page.get("content", {}).get("title", "?"))
                    source_id = page.get("content", {}).get("id", "")
                    ops_log.sync_page(
                        title, action=action, source_id=source_id,
                        elapsed_ms=perf.elapsed_ms(page_t0),
                    )
                except Exception as e:
                    stats["errors"] += 1
                    title = page.get("title", "?")
                    log.warning("[sync] 페이지 저장 실패: %s — %s", title, e)
                    ops_log.sync_page(title, action="error",
                                      elapsed_ms=perf.elapsed_ms(page_t0))
                    ops_log.sync_error(str(e), space_key=space_key,
                                       source_id=page.get("content", {}).get("id"))

            status = "success"
        except Exception as e:
            status = "error"
            stats["error_message"] = str(e)
            log.error("[sync] %s %s 실패: %s", sync_type, space_key, e)
            ops_log.sync_error(str(e), space_key=space_key)

        duration = round(time.time() - t0, 2)
        stats["duration_sec"] = duration

        # sync_log 완료 기록
        self._log_sync_finish(space_key, sync_type, started, status, stats, duration)
        ops_log.sync_finish(
            sync_type, space_key,
            scanned=stats["scanned"], added=stats["added"],
            updated=stats["updated"], errors=stats["errors"],
            duration_sec=duration,
        )
        log.info("[sync] %s %s 완료: %s (%.1fs)", sync_type, space_key, stats, duration)

        return stats

    def _fetch_all_pages(self, space_key: str,
                         max_pages: int = 10000) -> list[dict]:
        """CQL로 공간 전체 페이지 목록 조회."""
        cql = f'space = "{space_key}" AND type = page ORDER BY lastmodified DESC'
        return self._cql_paginate(cql, max_pages=max_pages,
                                  expand="content.ancestors,content.version")

    def _fetch_changed_pages(self, space_key: str, since: str | None) -> list[dict]:
        """마지막 동기화 이후 변경된 페이지만 조회."""
        if not since:
            log.info("[sync] 이전 동기화 기록 없음 → Full Ingest로 전환")
            return self._fetch_all_pages(space_key)

        # ISO 포맷(2026-03-17T13:00:04) → CQL 호환 포맷(2026-03-17 13:00)
        # Confluence CQL은 'T' 구분자를 파싱하지 못함
        since_cql = since.replace("T", " ")[:16]
        cql = (f'space = "{space_key}" AND type = page '
               f'AND lastModified > "{since_cql}" ORDER BY lastmodified DESC')
        return self._cql_paginate(cql, max_pages=200,
                                  expand="content.ancestors,content.version")

    def _cql_paginate(self, cql: str, max_pages: int = 500,
                      expand: str = "") -> list[dict]:
        """CQL 결과를 페이지네이션하여 전체 수집."""
        all_results = []
        start = 0
        limit = 50  # MCP/Confluence API 한 번에 최대

        while len(all_results) < max_pages:
            params = {"cql": cql, "limit": limit, "start": start}
            if expand:
                params["expand"] = expand
            raw, err = self._mcp.call_tool(
                "cql_search", params,
            )
            if err:
                log.error("[sync] CQL 오류: %s", err)
                break

            results = self._parse_cql_results(raw)
            if not results:
                break

            all_results.extend(results)
            start += limit

            # 결과가 limit보다 적으면 마지막 페이지
            if len(results) < limit:
                break

        return all_results[:max_pages]

    def _upsert_page(self, cql_result: dict, space_key: str,
                     fetch_body: bool = False) -> bool:
        """CQL 결과 항목 → 캐시 DB에 저장. 신규이면 True.

        fetch_body=True이면 MCP로 본문도 가져와 저장.
        """
        content = cql_result.get("content", {}) if isinstance(cql_result, dict) else {}
        page_id = content.get("id", "")
        if not page_id:
            return False

        title = re.sub(r'@@@hl@@@|@@@endhl@@@', '',
                       cql_result.get("title") or content.get("title") or "").strip()

        # 버전/수정일 추출
        version_data = content.get("version", {})
        last_modified = version_data.get("when") if isinstance(version_data, dict) else None
        version_num = version_data.get("number") if isinstance(version_data, dict) else None
        author = (version_data.get("by", {}).get("displayName")
                  if isinstance(version_data, dict) else None)

        # URL
        rel_url = (cql_result.get("url")
                   or content.get("_links", {}).get("webui", ""))
        url = f"https://wiki.smilegate.net{rel_url}" if rel_url.startswith("/") else rel_url

        # ancestors → parent 매핑
        ancestors = content.get("ancestors", [])
        parent_source_id = str(ancestors[-1]["id"]) if ancestors else None
        parent_node_id = None
        if parent_source_id:
            parent_node = self._cache.get_node("wiki", parent_source_id)
            if parent_node:
                parent_node_id = parent_node["id"]

        # path 구성
        ancestor_titles = [a.get("title", "") for a in ancestors if isinstance(a, dict)]
        path = "/" + "/".join(ancestor_titles + [title]) if ancestor_titles else f"/{title}"

        # 기존 노드 확인
        existing = self._cache.get_node("wiki", page_id)
        is_new = existing is None

        # 본문 fetch (선택적)
        body_raw, body_text = None, None
        if fetch_body:
            body_raw, body_text = self._fetch_page_body(page_id)

        # upsert
        self._cache.put_page(
            "wiki", page_id, title,
            space_key=space_key,
            parent_id=parent_node_id,
            path=path,
            url=url,
            last_modified=last_modified,
            version=version_num,
            author=author,
            body_raw=body_raw,
            body_text=body_text,
        )

        return is_new

    def _fetch_page_body(self, page_id: str) -> tuple[str | None, str | None]:
        """MCP로 페이지 본문 HTML을 가져와 (raw, text) 반환."""
        raw, err = self._mcp.call_tool(
            "get_page_by_id", {"page_id": page_id, "expand": "body.storage"},
        )
        if err:
            log.warning("[sync] 본문 fetch 실패: %s — %s", page_id, err)
            return None, None

        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return None, None

        body_html = ""
        if isinstance(raw, dict):
            body_html = raw.get("body", {}).get("storage", {}).get("value", "")

        if not body_html:
            return None, None

        body_text = _strip_html(body_html)
        limit = config.MAX_BODY_CHARS
        return body_html[:limit], body_text[:limit]

    # ── Repair 기능 ─────────────────────────────────────────

    def repair_missing_content(self, source_type: str = "wiki") -> dict:
        """doc_content 행이 없는 노드의 body를 MCP에서 재가져오기.

        Returns:
            {"total": int, "repaired": int, "failed": int}
        """
        missing = self._cache.get_nodes_missing_content(source_type)
        if not missing:
            return {"total": 0, "repaired": 0, "failed": 0}

        log.info("[repair] body 누락 노드 %d건 복구 시작", len(missing))
        repaired, failed = 0, 0
        for node in missing:
            try:
                body_raw, body_text = self._fetch_page_body(node["source_id"])
                if body_raw or body_text:
                    self._cache.upsert_content(
                        node["id"], body_raw, body_text,
                    )
                    repaired += 1
                    log.debug("[repair] body 복구 성공: %s (%s)",
                              node["title"], node["source_id"])
                else:
                    failed += 1
                    log.debug("[repair] body 비어있음 (매크로 전용?): %s",
                              node["title"])
            except Exception as e:
                failed += 1
                log.warning("[repair] body 복구 실패: %s — %s",
                            node["title"], e)

        result = {"total": len(missing), "repaired": repaired, "failed": failed}
        log.info("[repair] body 복구 완료: %s", result)
        return result

    def repair_parent_ids(self, source_type: str = "wiki") -> dict:
        """parent_id가 NULL인 고아 노드의 부모 관계를 MCP에서 재조회하여 복구.

        Returns:
            {"total": int, "repaired": int, "failed": int}
        """
        orphans = self._cache.get_orphan_nodes(source_type)
        if not orphans:
            return {"total": 0, "repaired": 0, "failed": 0}

        log.info("[repair] 고아 노드 %d건 parent_id 복구 시작", len(orphans))
        repaired, failed = 0, 0
        for node in orphans:
            try:
                # MCP에서 ancestors 포함한 페이지 정보 재조회
                raw, err = self._mcp.call_tool(
                    "get_page_by_id",
                    {"page_id": node["source_id"], "expand": "ancestors"},
                )
                if err:
                    failed += 1
                    continue

                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        failed += 1
                        continue

                ancestors = raw.get("ancestors", []) if isinstance(raw, dict) else []
                if not ancestors:
                    failed += 1
                    continue

                parent_source_id = str(ancestors[-1]["id"])
                parent_node = self._cache.get_node(source_type, parent_source_id)
                if parent_node:
                    self._cache.update_parent_id(node["id"], parent_node["id"])
                    repaired += 1
                    log.debug("[repair] parent 복구: %s → parent=%s",
                              node["title"], parent_node.get("title", "?"))
                else:
                    failed += 1
                    log.debug("[repair] parent 노드 DB 미존재: %s (parent_src=%s)",
                              node["title"], parent_source_id)
            except Exception as e:
                failed += 1
                log.warning("[repair] parent 복구 실패: %s — %s",
                            node["title"], e)

        result = {"total": len(orphans), "repaired": repaired, "failed": failed}
        log.info("[repair] parent 복구 완료: %s", result)
        return result

    # ── 파싱 헬퍼 ───────────────────────────────────────────

    def _parse_cql_results(self, raw) -> list:
        """cql_search 응답에서 results 목록 추출."""
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                return []
        if isinstance(raw, dict):
            return raw.get("results", [])
        if isinstance(raw, list):
            return raw
        return []

    # ── sync_log 관리 ───────────────────────────────────────

    def _get_last_sync_time(self, source_type: str, scope: str) -> str | None:
        """마지막 성공 동기화 시각."""
        from .models import get_connection
        conn = get_connection(self._cache._db_path)
        try:
            row = conn.execute(
                "SELECT started_at FROM sync_log "
                "WHERE source_type = ? AND scope = ? AND status = 'success' "
                "ORDER BY started_at DESC LIMIT 1",
                (source_type, scope),
            ).fetchone()
            return row["started_at"] if row else None
        finally:
            conn.close()

    def _log_sync_start(self, scope: str, sync_type: str, started_at: str):
        from .models import get_connection
        conn = get_connection(self._cache._db_path)
        try:
            conn.execute(
                "INSERT INTO sync_log (source_type, scope, sync_type, started_at) "
                "VALUES (?, ?, ?, ?)",
                ("wiki", scope, sync_type, started_at),
            )
            conn.commit()
        finally:
            conn.close()

    def _log_sync_finish(self, scope: str, sync_type: str, started_at: str,
                         status: str, stats: dict, duration: float):
        from .models import get_connection
        conn = get_connection(self._cache._db_path)
        try:
            conn.execute(
                "UPDATE sync_log SET finished_at=?, status=?, "
                "pages_scanned=?, pages_updated=?, pages_added=?, "
                "error_message=?, duration_sec=? "
                "WHERE source_type='wiki' AND scope=? AND started_at=?",
                (_now_iso(), status,
                 stats.get("scanned", 0), stats.get("updated", 0),
                 stats.get("added", 0), stats.get("error_message"),
                 duration, scope, started_at),
            )
            conn.commit()
        finally:
            conn.close()


# ── CLI 진입점 ───────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    parser = argparse.ArgumentParser(description="MCP Cache Sync Engine")
    parser.add_argument("mode", choices=["--full", "--delta"])
    parser.add_argument("source", default="wiki")
    parser.add_argument("space", default="QASGP")
    args = parser.parse_args()

    # Slack Bot의 McpSession 사용
    sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")
    import os
    from mcp_session import McpSession

    mcp = McpSession(
        url="http://mcp.sginfra.net/confluence-wiki-mcp",
        headers={
            "x-confluence-wiki-username": os.getenv("CONFLUENCE_USERNAME", "es-wjkim"),
            "x-confluence-wiki-token": os.getenv("CONFLUENCE_TOKEN", ""),
        },
        label="wiki",
    )

    cache = CacheManager()
    engine = SyncEngine(cache, mcp)

    if args.mode == "--full":
        result = engine.full_ingest(args.source, args.space)
    else:
        result = engine.delta_sync(args.source, args.space)

    print(json.dumps(result, indent=2, ensure_ascii=False))
