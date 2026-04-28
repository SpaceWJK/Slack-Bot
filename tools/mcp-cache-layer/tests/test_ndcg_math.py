"""
test_ndcg_math.py — _ndcg_at_k 순수 함수 unit test (task-113 T-8)

검증:
- 동일 입력 → 동일 출력 (determinism)
- expected 비어있으면 0.0
- 정답이 모두 top 위치 → 1.0
- 정답 없음 → 0.0
- partial credit 케이스
"""
from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from bench_fts import _ndcg_at_k  # noqa: E402


class TestNdcgAtK(unittest.TestCase):
    def test_empty_expected_returns_zero(self) -> None:
        self.assertEqual(_ndcg_at_k([1, 2, 3], [], k=10), 0.0)

    def test_perfect_rank_returns_one(self) -> None:
        # ranked 가 expected 그대로 → DCG == IDCG
        self.assertAlmostEqual(
            _ndcg_at_k([10, 20, 30], [10, 20, 30], k=10), 1.0
        )

    def test_no_overlap_returns_zero(self) -> None:
        self.assertEqual(_ndcg_at_k([1, 2, 3], [99, 100], k=10), 0.0)

    def test_single_hit_at_position_1(self) -> None:
        # 정답 1건이 첫 위치 → DCG=1, IDCG=1, nDCG=1
        self.assertAlmostEqual(_ndcg_at_k([5, 6, 7], [5], k=10), 1.0)

    def test_single_hit_at_position_2(self) -> None:
        # 정답 1건이 두 번째 위치 → DCG=1/log2(3), IDCG=1
        self.assertAlmostEqual(
            _ndcg_at_k([99, 5, 100], [5], k=10),
            1.0 / math.log2(3),
        )

    def test_determinism_same_input_same_output(self) -> None:
        a = _ndcg_at_k([1, 2, 3, 4, 5], [3, 5, 7], k=10)
        b = _ndcg_at_k([1, 2, 3, 4, 5], [3, 5, 7], k=10)
        self.assertEqual(a, b)

    def test_cutoff_k(self) -> None:
        # k=2 → top-2 만 고려, 3번째 정답은 무시
        nd = _ndcg_at_k([1, 2, 3, 4, 5], [5], k=2)
        self.assertEqual(nd, 0.0)

    def test_multi_node_partial(self) -> None:
        # 정답 3건 중 1건만 top-10 → partial
        # ranked: [1, 2, 3, 4], expected: {3, 99, 100}
        # DCG = 1 / log2(4) = 0.5
        # IDCG (m=3): 1 + 1/log2(3) + 1/log2(4) = 1 + 0.6309 + 0.5 = 2.1309
        nd = _ndcg_at_k([1, 2, 3, 4], [3, 99, 100], k=10)
        idcg = 1.0 + 1.0 / math.log2(3) + 1.0 / math.log2(4)
        self.assertAlmostEqual(nd, (1.0 / math.log2(4)) / idcg)


if __name__ == "__main__":
    unittest.main()
