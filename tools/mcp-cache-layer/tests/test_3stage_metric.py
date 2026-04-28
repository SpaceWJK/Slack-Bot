"""
test_3stage_metric.py — task-113 v4.2 3-stage metric T-20

T-20: ingest_coverage / retrieval_recall / routing_hit 순수 함수 unit test.
- ingest_coverage 분모 0 (expected_tokens 빈) → None
- retrieval_recall 분모 0 (expected 빈) → None
- routing_hit 분모 0 (expected 빈) → None
- 정상 케이스: 알려진 입력 → 정해진 출력
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from bench_fts import (  # noqa: E402
    compute_ingest_coverage,
    compute_retrieval_recall,
    compute_routing_hit,
)


class TestComputeIngestCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # in-memory DB 로 nodes + doc_content 스키마 재현
        cls.conn = sqlite3.connect(":memory:")
        cls.conn.executescript("""
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                title TEXT, path TEXT, source_type TEXT
            );
            CREATE TABLE doc_content (
                node_id INTEGER PRIMARY KEY,
                body_text TEXT, summary TEXT, keywords TEXT
            );
            INSERT INTO nodes VALUES (1, 'Chaos report', 'Chaoszero/Update Review', 'gdi');
            INSERT INTO doc_content VALUES (1, 'long body about chaos', 'sum', 'kw1');
            INSERT INTO nodes VALUES (2, 'empty', NULL, 'gdi');
            -- node 2: doc_content 없음 (LEFT JOIN COALESCE 검증)
        """)

    def test_empty_tokens_returns_none(self) -> None:
        self.assertIsNone(
            compute_ingest_coverage([], 1, self.conn)
        )

    def test_node_not_found_returns_zero(self) -> None:
        self.assertEqual(
            compute_ingest_coverage(["xxx"], 999, self.conn), 0.0
        )

    def test_full_coverage(self) -> None:
        # "Chaos" + "Update" → 둘 다 존재
        cov = compute_ingest_coverage(
            ["Chaos", "Update"], 1, self.conn
        )
        self.assertEqual(cov, 1.0)

    def test_partial_coverage(self) -> None:
        # "Chaos" 존재, "Missing" 부재 → 0.5
        cov = compute_ingest_coverage(
            ["Chaos", "Missing"], 1, self.conn
        )
        self.assertAlmostEqual(cov, 0.5)

    def test_null_columns_handled(self) -> None:
        # node 2: doc_content 없음 → COALESCE 빈 문자열, title='empty'
        # "empty" 존재
        cov = compute_ingest_coverage(["empty"], 2, self.conn)
        self.assertEqual(cov, 1.0)


class TestComputeRetrievalRecall(unittest.TestCase):
    def test_empty_expected_returns_none(self) -> None:
        self.assertIsNone(compute_retrieval_recall([1, 2, 3], []))

    def test_full_recall(self) -> None:
        # expected 모두 top-10에 포함
        self.assertEqual(
            compute_retrieval_recall([1, 2, 3, 4], [1, 2]), 1.0
        )

    def test_partial_recall(self) -> None:
        # expected 2건 중 1건만 top-10
        self.assertAlmostEqual(
            compute_retrieval_recall([1, 99], [1, 2]), 0.5
        )

    def test_zero_recall(self) -> None:
        self.assertEqual(
            compute_retrieval_recall([99, 100], [1, 2]), 0.0
        )

    def test_cutoff_k(self) -> None:
        # k=2 → top-2 만 고려
        self.assertEqual(
            compute_retrieval_recall([99, 100, 1], [1], k=2), 0.0
        )


class TestComputeRoutingHit(unittest.TestCase):
    def test_empty_expected_returns_none(self) -> None:
        self.assertIsNone(compute_routing_hit([1, 2], []))

    def test_hit(self) -> None:
        self.assertEqual(compute_routing_hit([1, 2, 3], [3]), 1.0)

    def test_miss(self) -> None:
        self.assertEqual(compute_routing_hit([1, 2, 3], [99]), 0.0)

    def test_empty_ranked_with_expected(self) -> None:
        self.assertEqual(compute_routing_hit([], [1, 2]), 0.0)


if __name__ == "__main__":
    unittest.main()
