"""
bench_fts.py — FTS5 before/after 벤치마크 (task-102) + nDCG 확장 (task-113)

측정:
1. recall@10: 예상 문서 Top-10 포함 비율
2. P95 레이턴시: n_repeat 쿼리 분포 (기본 10회)
3. 인덱스 크기: page_count × page_size
4. precision@10: Top-10 중 정답 비율 (None 케이스는 분모 제외)
5. nDCG@10 (task-113): production adapter 경로 + raw FTS 경로 (chunks/search)

사용법:
    python scripts/bench_fts.py --db /path/to/mcp_cache.db
    python scripts/bench_fts.py --db /path/to/after.db --compare /path/to/before.db
"""
import argparse
import math
import sqlite3
import statistics
import sys
import time
from pathlib import Path

# ── 테스트 쿼리 (10건) ────────────────────────────────────────────────────────
# (query_text, expected_path_contains)  # None = precision 분모 제외
TEST_QUERIES = [
    ("기획서",       "기획서"),    # T-1: 한국어 3자 FTS 직접 매칭
    ("Chaos",        "Chaoszero"), # T-2: 영어 소문자 포함 (trigram case_insensitive)
    ("#124",         "124968"),    # T-3: 특수문자 + 숫자 조합
    ("업데이트",      "업데이트"),  # T-5: 회귀 검증
    ("스테이지",      "스테이지"),  # T-6: 한국어 3자 이상
    ("review",       "review"),    # T-7: 영어 소문자
    ("은하",          None),        # T-4: 2자 키워드 → LIKE 폴백 (FTS 결과 없어도 OK)
    ("빌드 테스트",   "테스트"),    # T-8: 복합 키워드 (1개 이상 히트)
    ("장애 리포트",   "리포트"),    # T-9: 복합 키워드
    ("20260204",     "20260204"),  # T-10: 날짜 숫자 키워드
]


# ── 측정 함수 ─────────────────────────────────────────────────────────────────

def _to_fts_query(query_text: str) -> str:
    """query_text를 FTS5 phrase 쿼리로 변환 (gdi_client 패턴 동일).

    각 공백 분리 키워드를 '"keyword"' phrase로 감싸 AND 연결.
    특수문자(#, -, 등)가 포함된 키워드도 올바르게 처리됨.
    """
    kws = [kw.strip() for kw in query_text.split() if kw.strip()]
    if not kws:
        return query_text
    return " AND ".join('"' + kw.replace('"', '""') + '"' for kw in kws)


def measure(conn: sqlite3.Connection, query_text: str,
            expected: "str | None", n_repeat: int = 10) -> tuple:
    """recall@10 + precision@10 + P95 레이턴시(ms) 측정.

    Returns: (recall, precision, p95_ms)
        recall: 1 (hit) or 0 (miss) — expected가 None이면 None
        precision: hits / len(paths) — expected가 None이면 None
        p95_ms: float
    """
    fts_query = _to_fts_query(query_text)  # phrase 쿼리로 변환 (#, 특수문자 안전 처리
    times = []
    rows = []
    for _ in range(n_repeat):
        t0 = time.perf_counter()
        try:
            # gdi_client._local_unified_search() 패턴 동일:
            # source_type='gdi' 필터 + BM25 랭킹 — wiki 오염 원천 차단
            rows = conn.execute(
                """SELECT n.id, n.path
                   FROM search_fts
                   JOIN nodes n ON n.id = search_fts.rowid
                   JOIN doc_content dc ON dc.node_id = n.id
                   WHERE search_fts MATCH ? AND n.source_type = 'gdi'
                   ORDER BY bm25(search_fts) LIMIT 10""",
                (fts_query,)
            ).fetchall()
        except sqlite3.OperationalError:
            # trigram은 2자 이하 MATCH 쿼리를 거부할 수 있음 → 0건 처리
            rows = []
        times.append(time.perf_counter() - t0)

    # P95 계산 (n_repeat < 20이면 max로 대체)
    if len(times) >= 20:
        p95_ms = statistics.quantiles(times, n=20)[18] * 1000
    else:
        p95_ms = max(times) * 1000

    if expected is None:
        return None, None, p95_ms

    if not rows:
        return 0, 0.0, p95_ms

    # JOIN 결과에서 path 직접 추출 (별도 쿼리 불필요)
    paths = [row[1] for row in rows if row[1]]

    hits = sum(1 for p in paths if expected.lower() in p.lower())
    recall = 1 if hits > 0 else 0
    precision = hits / len(paths) if paths else 0.0
    return recall, precision, p95_ms


def measure_chunks(conn: sqlite3.Connection, query_text: str,
                   expected: "str | None", n_repeat: int = 10) -> tuple:
    """청크 FTS (chunks_fts) 기반 recall@10 + precision@10 + P95 측정.

    Slack Bot gdi_client._local_chunk_search() 패턴 동일:
      chunks_fts MATCH ? → doc_chunks JOIN → nodes JOIN, source_type='gdi'
      ORDER BY rank LIMIT (top_k * 3) → DISTINCT node_id 10건

    task-104 청크 기반 검색 활성 상태의 KPI를 측정하기 위한 함수 (task-112).

    Returns: (recall, precision, p95_ms) — measure()와 동일 포맷.
    """
    fts_query = _to_fts_query(query_text)
    times = []
    rows = []
    for _ in range(n_repeat):
        t0 = time.perf_counter()
        try:
            # top_k * 3 = 30건 fetch → DISTINCT node_id 10건으로 집계 (gdi_client 패턴 일치)
            raw = conn.execute(
                """SELECT dc.node_id, n.path
                   FROM chunks_fts
                   JOIN doc_chunks dc ON dc.id = chunks_fts.rowid
                   JOIN nodes n ON n.id = dc.node_id
                   WHERE chunks_fts MATCH ? AND n.source_type = 'gdi'
                   ORDER BY rank LIMIT 30""",
                (fts_query,)
            ).fetchall()
        except sqlite3.OperationalError:
            raw = []

        # DISTINCT node_id 상위 10건 (첫 등장 순 유지)
        seen = set()
        rows = []
        for node_id, path in raw:
            if node_id not in seen:
                seen.add(node_id)
                rows.append((node_id, path))
                if len(rows) >= 10:
                    break

        times.append(time.perf_counter() - t0)

    if len(times) >= 20:
        p95_ms = statistics.quantiles(times, n=20)[18] * 1000
    else:
        p95_ms = max(times) * 1000

    if expected is None:
        return None, None, p95_ms

    if not rows:
        return 0, 0.0, p95_ms

    paths = [row[1] for row in rows if row[1]]
    hits = sum(1 for p in paths if expected.lower() in p.lower())
    recall = 1 if hits > 0 else 0
    precision = hits / len(paths) if paths else 0.0
    return recall, precision, p95_ms


# ── task-113 v4.2 3-stage metric ──────────────────────────────────────────────

def compute_ingest_coverage(
    expected_tokens: list, node_id: int, conn: sqlite3.Connection
) -> "float | None":
    """Stage 1: 원본 token-set이 SQLite 저장 column 합집합에 존재하는 비율.

    v4.2 Round 1 qa-functional EC-1 보완:
    - expected_tokens 빈 리스트 → None (분모 0 회피)
    - body_text/summary/keywords NULL → COALESCE 빈 문자열
    - 분모 0 케이스는 평균 계산에서 제외

    Returns: float in [0.0, 1.0] or None (제외)
    """
    if not expected_tokens:
        return None
    row = conn.execute(
        """SELECT
            COALESCE(n.title, '') AS title,
            COALESCE(dc.body_text, '') AS body_text,
            COALESCE(dc.summary, '') AS summary,
            COALESCE(dc.keywords, '') AS keywords,
            COALESCE(n.path, '') AS path
        FROM nodes n
        LEFT JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.id = ?""",
        (node_id,),
    ).fetchone()
    if row is None:
        return 0.0
    full_text = " ".join(row).lower()
    hits = sum(1 for tok in expected_tokens if tok.lower() in full_text)
    return hits / len(expected_tokens)


def compute_retrieval_recall(
    ranked: list, expected_node_ids: list, k: int = 10
) -> "float | None":
    """Stage 2: FTS5 매칭으로 expected_node_ids 회수 비율.

    expected 빈 → None.
    expected 와 ranked top-k 의 교집합 / expected 크기.
    """
    if not expected_node_ids:
        return None
    expected_set = set(expected_node_ids)
    top = set(ranked[:k])
    return len(expected_set & top) / len(expected_set)


def compute_routing_hit(
    ranked: list, expected_node_ids: list
) -> "float | None":
    """Stage 3: routing seam 결과가 expected_node_ids 와 매칭하는 비율.

    expected 빈 → None.
    binary recall: expected 중 한 건이라도 ranked 에 있으면 1, else 0.
    """
    if not expected_node_ids:
        return None
    expected_set = set(expected_node_ids)
    return 1.0 if any(nid in expected_set for nid in ranked) else 0.0


# ── task-113 nDCG 확장 ────────────────────────────────────────────────────────

def _ndcg_at_k(ranked: list, expected: list, k: int = 10) -> float:
    """순수 함수 nDCG@k (binary relevance, recall 기준).

    Args:
        ranked: 검색 결과 node_id top-K 리스트 (순서 보존)
        expected: 정답 node_id 리스트 (순서 무관)
        k: cutoff (기본 10)

    Returns:
        float in [0.0, 1.0]. expected 가 비어 있으면 0.0 반환.

    Determinism: 동일 입력 → 동일 출력 (math.log2 결정적).
    """
    if not expected:
        return 0.0
    expected_set = set(expected)
    # DCG: relevance = 1 if node_id in expected, else 0
    dcg = 0.0
    for i, nid in enumerate(ranked[:k]):
        if nid in expected_set:
            # rank position = i + 1 → discount = 1 / log2(i + 2)
            dcg += 1.0 / math.log2(i + 2)
    # IDCG: 정답 m건이 모두 top 위치에 있을 때
    m = min(len(expected_set), k)
    if m == 0:
        return 0.0
    idcg = sum(1.0 / math.log2(i + 2) for i in range(m))
    return dcg / idcg if idcg > 0 else 0.0


def measure_ndcg_production(
    adapter,
    query: str,
    expected_node_ids: list,
    n_repeat: int = 10,
) -> tuple:
    """ProductionSearchAdapter 경로 nDCG@10 + P95 레이턴시(ms).

    expected_node_ids 비어 있으면 (None, None) 반환 (miss_only 처리).

    Returns: (ndcg, p95_ms) — ndcg float | None, p95_ms float
    """
    times: list[float] = []
    last_ranked: list[int] = []
    for _ in range(n_repeat):
        t0 = time.perf_counter()
        try:
            last_ranked = adapter.search(query, top_k=10)
        except Exception:
            last_ranked = []
        times.append(time.perf_counter() - t0)

    if len(times) >= 20:
        p95_ms = statistics.quantiles(times, n=20)[18] * 1000
    else:
        p95_ms = max(times) * 1000

    if not expected_node_ids:
        return None, p95_ms
    return _ndcg_at_k(last_ranked, expected_node_ids, k=10), p95_ms


def measure_ndcg_raw_fts(
    conn: sqlite3.Connection,
    query: str,
    expected_node_ids: list,
    use_chunks: bool = True,
    n_repeat: int = 10,
) -> tuple:
    """raw FTS 경로 nDCG@10 + P95 레이턴시(ms).

    use_chunks=True (기본): chunks_fts JOIN doc_chunks → DISTINCT node_id top-10
    use_chunks=False: search_fts JOIN nodes (v1 원문 쿼리 — Round 3 reconcile)

    expected_node_ids 비어 있으면 (None, None) 반환.

    tie-break:
      - chunks_fts: ORDER BY rank ASC, n.id ASC LIMIT 30 → DISTINCT 10
      - search_fts: ORDER BY bm25(search_fts) ASC, n.id ASC LIMIT 10
    """
    fts_query = _to_fts_query(query)
    times: list[float] = []
    last_ranked: list[int] = []

    for _ in range(n_repeat):
        t0 = time.perf_counter()
        try:
            if use_chunks:
                raw = conn.execute(
                    """SELECT dc.node_id
                       FROM chunks_fts
                       JOIN doc_chunks dc ON dc.id = chunks_fts.rowid
                       JOIN nodes n ON n.id = dc.node_id
                       WHERE chunks_fts MATCH ? AND n.source_type = 'gdi'
                       ORDER BY rank ASC, n.id ASC LIMIT 30""",
                    (fts_query,),
                ).fetchall()
                seen: set[int] = set()
                last_ranked = []
                for (nid,) in raw:
                    if nid in seen:
                        continue
                    seen.add(nid)
                    last_ranked.append(int(nid))
                    if len(last_ranked) >= 10:
                        break
            else:
                rows = conn.execute(
                    """SELECT n.id
                       FROM search_fts
                       JOIN nodes n ON n.id = search_fts.rowid
                       WHERE search_fts MATCH ? AND n.source_type = 'gdi'
                       ORDER BY bm25(search_fts) ASC, n.id ASC LIMIT 10""",
                    (fts_query,),
                ).fetchall()
                last_ranked = [int(r[0]) for r in rows]
        except sqlite3.OperationalError:
            last_ranked = []
        times.append(time.perf_counter() - t0)

    if len(times) >= 20:
        p95_ms = statistics.quantiles(times, n=20)[18] * 1000
    else:
        p95_ms = max(times) * 1000

    if not expected_node_ids:
        return None, p95_ms
    return _ndcg_at_k(last_ranked, expected_node_ids, k=10), p95_ms


def get_fts_index_size(conn: sqlite3.Connection) -> int:
    """전체 DB 크기(bytes) = page_count × page_size."""
    row = conn.execute(
        "SELECT page_count * page_size AS bytes "
        "FROM pragma_page_count(), pragma_page_size()"
    ).fetchone()
    return row[0] if row else 0


def _get_tokenizer(conn: sqlite3.Connection) -> str:
    """search_fts 테이블의 토크나이저 정보 조회."""
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='search_fts'"
        ).fetchone()
        if row and row[0]:
            sql = row[0].lower()
            if 'trigram' in sql:
                return 'trigram'
            if 'unicode61' in sql:
                return 'unicode61'
        return 'unknown'
    except sqlite3.OperationalError:
        return 'unknown'


def _run_benchmark(conn: sqlite3.Connection, label: str) -> list:
    """전체 TEST_QUERIES 측정 후 결과 리스트 반환."""
    results = []
    print(f"\n[{label}] 토크나이저: {_get_tokenizer(conn)}")
    print(f"{'쿼리':<16} {'Recall':>7} {'Precision':>10} {'P95(ms)':>9}")
    print("-" * 46)

    recall_sum = 0
    recall_count = 0
    precision_sum = 0.0
    precision_count = 0

    for query_text, expected in TEST_QUERIES:
        recall, precision, p95_ms = measure(conn, query_text, expected)
        tag = ""
        if expected is None:
            tag = "(skip)"
        else:
            recall_sum += recall
            recall_count += 1
            if precision is not None:
                precision_sum += precision
                precision_count += 1

        r_str = f"{recall}" if recall is not None else "-"
        p_str = f"{precision:.2f}" if precision is not None else "-"
        print(f"  {query_text:<14} {r_str:>7} {p_str:>10} {p95_ms:>8.1f}ms {tag}")
        results.append({
            "query": query_text,
            "expected": expected,
            "recall": recall,
            "precision": precision,
            "p95_ms": p95_ms,
        })

    avg_recall = (recall_sum / recall_count) if recall_count else 0.0
    avg_precision = (precision_sum / precision_count) if precision_count else 0.0
    db_size_kb = get_fts_index_size(conn) / 1024
    print("-" * 46)
    print(f"  평균 Recall@10: {avg_recall:.2f}  "
          f"평균 Precision@10: {avg_precision:.2f}  "
          f"DB 크기: {db_size_kb:.0f} KB")
    return results


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="FTS5 벤치마크 (task-102)"
    )
    parser.add_argument("--db", default=None,
                        help="측정 대상 DB 경로")
    parser.add_argument("--compare", default=None,
                        help="비교 기준 DB 경로 (before; --db가 after)")
    args = parser.parse_args()

    # db 경로 결정
    if args.db:
        db_path = args.db
    else:
        PROJECT_ROOT = Path(__file__).resolve().parent.parent
        sys.path.insert(0, str(PROJECT_ROOT))
        from src import config as cache_config  # noqa: E402
        db_path = str(cache_config.DB_PATH)

    if not Path(db_path).exists():
        print(f"오류: DB 파일 없음: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn_main = sqlite3.connect(db_path)

    if args.compare:
        if not Path(args.compare).exists():
            print(f"오류: 비교 DB 파일 없음: {args.compare}", file=sys.stderr)
            conn_main.close()
            sys.exit(1)
        conn_before = sqlite3.connect(args.compare)
        _run_benchmark(conn_before, f"BEFORE ({Path(args.compare).name})")
        conn_before.close()
        _run_benchmark(conn_main, f"AFTER  ({Path(db_path).name})")
    else:
        _run_benchmark(conn_main, Path(db_path).name)

    conn_main.close()


if __name__ == "__main__":
    main()
