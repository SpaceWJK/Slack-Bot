"""Enrichment Engine — summary/keywords 자동 생성.

캐시 DB에 적재된 노드의 body_text를 분석하여
summary(1~3문장 요약)와 keywords(핵심 키워드 배열)를 채웁니다.

외부 API 호출 없이 Python 표준 라이브러리만으로 동작합니다 (Tier 1 로컬 추출).

사용법:
    # Python
    from src.enrichment import EnrichmentEngine
    from src.cache_manager import CacheManager
    engine = EnrichmentEngine(CacheManager())
    result = engine.enrich_batch("wiki")

    # CLI
    python -m src.enrichment --all
    python -m src.enrichment --wiki --force
"""

import json
import logging
import re
import time
from collections import Counter

from . import config
from .cache_manager import CacheManager

log = logging.getLogger("mcp_cache")

# ── 불용어 ──────────────────────────────────────────────────

_STOPWORDS_KO = frozenset({
    "있는", "하는", "되는", "위한", "대한", "통해", "따라", "경우",
    "사항", "내용", "관련", "기반", "해당", "진행", "확인", "필요",
    "에서", "으로", "하여", "있다", "된다", "한다", "대로", "것이",
    "때문", "이후", "이전", "또는", "그리고", "하지만", "그러나",
    "합니다", "입니다", "습니다", "됩니다", "있습", "없습",
    "페이지", "문서", "항목", "목록", "참고", "작성", "수정",
})

_STOPWORDS_EN = frozenset({
    "the", "and", "for", "with", "from", "this", "that", "are",
    "was", "were", "been", "have", "has", "had", "will", "would",
    "can", "could", "should", "may", "might", "not", "but", "all",
    "any", "some", "each", "every", "other", "more", "less",
    "also", "just", "only", "than", "then", "when", "where",
    "which", "who", "whom", "how", "what", "why", "into",
    "about", "after", "before", "between", "under", "over",
    "page", "null", "none", "true", "false", "http", "https",
})

_STOPWORDS = _STOPWORDS_KO | _STOPWORDS_EN


# ── 유틸리티 함수 ──────────────────────────────────────────

def _first_n_sentences(text: str, n: int = 3) -> str:
    """텍스트에서 첫 N문장을 추출.

    마침표(. ), 줄바꿈, 물음표(? ), 느낌표(! ) 기준으로 분리.
    """
    if not text:
        return ""
    # 연속 공백/줄바꿈 정리
    cleaned = re.sub(r'\s+', ' ', text.strip())
    # 문장 분리: 마침표/물음표/느낌표 뒤에 공백이 오는 지점
    parts = re.split(r'(?<=[.?!])\s+', cleaned)
    sentences = [p.strip() for p in parts if p.strip()]
    result = ". ".join(sentences[:n])
    if result and not result.endswith((".", "?", "!")):
        result += "."
    return result[:config.SUMMARY_MAX_CHARS]


def _freq_keywords(text: str, top_n: int = 8) -> list[str]:
    """빈도 기반 키워드 추출. Python 표준 라이브러리만 사용.

    한글 2~6자 단어 + 영문 3자+ 단어에서 불용어를 제외하고
    빈도 상위 top_n개를 반환합니다.
    """
    if not text:
        return []
    # 첫 5000자만 사용 (성능)
    sample = text[:5000]
    words = re.findall(r'[가-힣]{2,6}|[A-Za-z]{3,}', sample)
    filtered = [w for w in words if w.lower() not in _STOPWORDS and len(w) > 1]
    return [w for w, _ in Counter(filtered).most_common(top_n)]


def _path_to_category(path: str) -> str:
    """GDI 경로에서 카테고리 추출.

    예: 'Chaoszero/TSV/260204/file.tsv' → 'Chaoszero / TSV'
    """
    if not path:
        return "GDI"
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    if len(parts) >= 2:
        return f"{parts[0]} / {parts[1]}"
    elif parts:
        return parts[0]
    return "GDI"


def _path_segments(path: str) -> list[str]:
    """GDI 경로에서 의미 있는 세그먼트 추출 (키워드 후보).

    예: 'Chaoszero/TSV/260204/relic@tooltip.tsv'
        → ['Chaoszero', 'TSV', 'relic', 'tooltip']
    """
    if not path:
        return []
    parts = path.replace("\\", "/").split("/")
    segments = []
    for p in parts:
        # 파일명에서 확장자 제거 + @ 분리
        name = re.sub(r'\.\w+$', '', p)
        tokens = re.split(r'[@_\-\s]', name)
        for t in tokens:
            t = t.strip()
            # 숫자만인 세그먼트(날짜 폴더 등) 제외
            if t and not t.isdigit() and len(t) >= 2:
                segments.append(t)
    return segments


def _safe_json_loads(text: str | None) -> list:
    """JSON 배열 안전 파싱. 실패 시 빈 리스트."""
    if not text:
        return []
    try:
        result = json.loads(text)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


# ── Enrichment Engine ──────────────────────────────────────

class EnrichmentEngine:
    """캐시 노드의 summary/keywords 보강 엔진.

    모든 처리는 로컬(DB-only)로 수행됩니다. 외부 API 호출 없음.
    """

    def __init__(self, cache: CacheManager):
        self._cache = cache

    # ── 공개 API ────────────────────────────────────────────

    def enrich_batch(
        self,
        source_type: str,
        batch_size: int = 0,
        force: bool = False,
    ) -> dict:
        """source_type별 일괄 enrichment.

        Args:
            source_type: 'wiki' | 'jira' | 'gdi'
            batch_size: 배치 크기 (0이면 config 기본값)
            force: True이면 이미 채워진 노드도 재생성

        Returns:
            {"total": int, "enriched": int, "skipped": int,
             "errors": int, "duration_sec": float}
        """
        if not batch_size:
            batch_size = config.ENRICH_BATCH_SIZE

        t0 = time.time()
        enriched, skipped, errors = 0, 0, 0
        total_processed = 0

        while True:
            if force:
                nodes = self._get_all_nodes(source_type, batch_size, total_processed)
            else:
                nodes = self._cache.get_nodes_missing_enrichment(
                    source_type, limit=batch_size,
                )

            if not nodes:
                break

            for node in nodes:
                total_processed += 1
                try:
                    ok = self._enrich_node(node, source_type)
                    if ok:
                        enriched += 1
                    else:
                        skipped += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        log.warning(
                            "[Enrich] node#%d (%s) 오류: %s",
                            node["id"], node.get("title", "?"), e,
                        )

                if total_processed % 500 == 0:
                    log.info(
                        "[Enrich %s] 진행: %d건 (enriched=%d, skip=%d, err=%d)",
                        source_type, total_processed, enriched, skipped, errors,
                    )

            # force 모드에서는 offset으로 다음 배치
            if force:
                if len(nodes) < batch_size:
                    break
            else:
                # 미처리 노드가 배치 크기보다 적으면 종료
                if len(nodes) < batch_size:
                    break

        duration = round(time.time() - t0, 1)
        result = {
            "total": total_processed,
            "enriched": enriched,
            "skipped": skipped,
            "errors": errors,
            "duration_sec": duration,
        }
        log.info("[Enrich %s] 완료: %s", source_type, result)
        return result

    def enrich_single(self, node_id: int) -> bool:
        """단일 노드 enrichment. 성공 시 True."""
        conn = self._cache._conn()
        try:
            row = conn.execute(
                """
                SELECT n.id, n.source_id, n.title, n.source_type,
                       dc.body_text, dc.summary, dc.keywords
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.id = ?
                """,
                (node_id,),
            ).fetchone()
            if not row:
                return False
            return self._enrich_node(dict(row), row["source_type"])
        finally:
            conn.close()

    def get_stats(self) -> dict:
        """enrichment 통계 (cache_manager 위임)."""
        return self._cache.get_enrichment_stats()

    # ── 내부 로직 ───────────────────────────────────────────

    def _get_all_nodes(
        self, source_type: str, limit: int, offset: int
    ) -> list[dict]:
        """force 모드용: 전체 노드 조회 (offset 기반)."""
        conn = self._cache._conn()
        try:
            rows = conn.execute(
                """
                SELECT n.id, n.source_id, n.title, n.source_type,
                       dc.body_text, dc.summary, dc.keywords
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = ?
                ORDER BY n.id
                LIMIT ? OFFSET ?
                """,
                (source_type, limit, offset),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def _enrich_node(self, node: dict, source_type: str) -> bool:
        """단일 노드에 summary/keywords 생성 및 저장.

        body_text가 너무 짧으면 스킵 (False 반환).
        """
        body = node.get("body_text") or ""
        if len(body.strip()) < config.ENRICH_MIN_BODY_LEN:
            return False

        # 소스별 메타 조회
        meta = self._get_source_meta(node["id"], source_type)

        # Summary 생성
        summary = self._extract_summary(body, source_type, node, meta)

        # Keywords 생성
        keywords = self._extract_keywords(body, source_type, node, meta)

        # DB 저장
        kw_json = json.dumps(keywords, ensure_ascii=False) if keywords else "[]"
        self._cache.update_enrichment(node["id"], summary=summary, keywords=kw_json)
        return True

    def _get_source_meta(self, node_id: int, source_type: str) -> dict:
        """소스별 메타 정보 조회.

        Wiki: doc_meta.labels
        Jira: jira_issues.*
        GDI: (없음, 빈 dict)
        """
        conn = self._cache._conn()
        try:
            if source_type == "jira":
                row = conn.execute(
                    """
                    SELECT issue_type, status, priority, resolution,
                           components, labels, fix_versions
                    FROM jira_issues WHERE node_id = ?
                    """,
                    (node_id,),
                ).fetchone()
                return dict(row) if row else {}

            elif source_type == "wiki":
                row = conn.execute(
                    "SELECT labels, extra_meta FROM doc_meta WHERE node_id = ?",
                    (node_id,),
                ).fetchone()
                return dict(row) if row else {}

            return {}
        finally:
            conn.close()

    # ── Summary 추출 ────────────────────────────────────────

    def _extract_summary(
        self,
        body_text: str,
        source_type: str,
        node: dict,
        meta: dict,
    ) -> str:
        """소스별 로컬 summary 추출."""
        if source_type == "jira":
            return self._summary_jira(body_text, node, meta)
        elif source_type == "gdi":
            return self._summary_gdi(body_text, node)
        else:
            return self._summary_wiki(body_text, node)

    def _summary_wiki(self, body_text: str, node: dict) -> str:
        """Wiki: body_text 첫 3문장."""
        return _first_n_sentences(body_text, 3)

    def _summary_jira(self, body_text: str, node: dict, meta: dict) -> str:
        """Jira: 이슈 타입/상태/우선도 + description 첫 2문장."""
        parts = []
        itype = meta.get("issue_type", "")
        status = meta.get("status", "")
        priority = meta.get("priority", "")
        if itype or status:
            prefix_parts = [p for p in (itype, status, priority) if p]
            parts.append(" / ".join(prefix_parts))

        sentences = _first_n_sentences(body_text, 2)
        if sentences:
            parts.append(sentences)

        result = ". ".join(parts) if parts else _first_n_sentences(body_text, 3)
        return result[:config.SUMMARY_MAX_CHARS]

    def _summary_gdi(self, body_text: str, node: dict) -> str:
        """GDI: 경로 카테고리 + body 첫 2문장."""
        category = _path_to_category(node.get("path") or node.get("title", ""))
        sentences = _first_n_sentences(body_text, 2)
        result = f"[{category}] {sentences}" if sentences else f"[{category}]"
        return result[:config.SUMMARY_MAX_CHARS]

    # ── Keywords 추출 ───────────────────────────────────────

    def _extract_keywords(
        self,
        body_text: str,
        source_type: str,
        node: dict,
        meta: dict,
    ) -> list[str]:
        """소스별 keywords 추출."""
        structured: list[str] = []

        if source_type == "wiki":
            structured.extend(_safe_json_loads(meta.get("labels")))

        elif source_type == "jira":
            for field in ("labels", "components", "fix_versions"):
                structured.extend(_safe_json_loads(meta.get(field)))
            # issue_type도 키워드에 포함
            itype = meta.get("issue_type", "")
            if itype:
                structured.append(itype)

        elif source_type == "gdi":
            structured.extend(_path_segments(node.get("path", "")))

        # 제목에서도 키워드 추출
        title_kw = _freq_keywords(node.get("title", ""), top_n=3)
        structured.extend(title_kw)

        # body에서 빈도 키워드 추출 (구조화 키워드 수만큼 뺌)
        remaining = max(0, config.KEYWORDS_MAX_COUNT - len(structured))
        body_kw = _freq_keywords(body_text, top_n=remaining) if remaining > 0 else []

        # 중복 제거 + 최대 개수 제한
        seen: set[str] = set()
        result: list[str] = []
        for kw in structured + body_kw:
            kw_clean = kw.strip()
            kw_lower = kw_clean.lower()
            if kw_lower and kw_lower not in seen and kw_lower not in _STOPWORDS:
                seen.add(kw_lower)
                result.append(kw_clean)
            if len(result) >= config.KEYWORDS_MAX_COUNT:
                break

        return result


# ── CLI 진입점 ──────────────────────────────────────────────

def main():
    """커맨드라인에서 enrichment 실행."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="MCP Cache Enrichment")
    parser.add_argument("--wiki", action="store_true", help="Wiki 노드 enrichment")
    parser.add_argument("--jira", action="store_true", help="Jira 노드 enrichment")
    parser.add_argument("--gdi", action="store_true", help="GDI 노드 enrichment")
    parser.add_argument("--all", action="store_true", help="전체 소스 enrichment")
    parser.add_argument("--force", action="store_true", help="이미 채워진 노드도 재생성")
    parser.add_argument("--stats", action="store_true", help="enrichment 통계만 출력")
    args = parser.parse_args()

    cache = CacheManager()
    engine = EnrichmentEngine(cache)

    if args.stats:
        stats = engine.get_stats()
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        return

    sources: list[str] = []
    if args.all:
        sources = ["wiki", "jira", "gdi"]
    else:
        if args.wiki:
            sources.append("wiki")
        if args.jira:
            sources.append("jira")
        if args.gdi:
            sources.append("gdi")

    if not sources:
        parser.print_help()
        return

    results = {}
    for src in sources:
        results[src] = engine.enrich_batch(src, force=args.force)

    print("\n=== Enrichment 결과 ===")
    for src, r in results.items():
        print(f"  {src}: {r}")

    print("\n=== Enrichment 통계 ===")
    stats = engine.get_stats()
    for src, s in stats.get("by_source", {}).items():
        print(f"  {src}: {s['summary']}/{s['total']} ({s['pct']}%)")


if __name__ == "__main__":
    main()
