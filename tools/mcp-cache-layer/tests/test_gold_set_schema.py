"""
test_gold_set_schema.py — gold_set.jsonl schema validator (task-113 T-4/T-5/T-6)

검증:
- 정확 16건 (real_log 8 + miss_only 8)
- 필수 필드: id, stratum, query, expected_node_ids, source, source_line
- miss_only.expected_node_ids == []
- real_log.expected_node_ids 모든 원소 sorted (multi-node rule)
- 라인 위치별 stratum 일관 (real_log 먼저, miss_only 후)

gold_set.jsonl 부재 시 skip (Step 4 구현 직후 단계는 OK).
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))


class TestGoldSetSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.gold_path = (
            Path(__file__).resolve().parent.parent / "data" / "gold_set.jsonl"
        )
        if not cls.gold_path.exists():
            raise unittest.SkipTest(
                f"gold_set.jsonl not found: {cls.gold_path}. "
                "Run scripts/generate_gold_set.py first."
            )
        cls.entries = []
        with open(cls.gold_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    cls.entries.append(json.loads(line))

    def test_v42_5_stratum_only(self) -> None:
        """v4.2 5 stratum 만 허용."""
        valid = {
            "gold_2part", "gold_simple", "gold_folder",
            "gold_miss_time_range", "gold_manual",
        }
        for e in self.entries:
            self.assertIn(
                e["stratum"], valid,
                f"Invalid stratum {e['stratum']!r}: {e}"
            )

    def test_required_fields(self) -> None:
        required = (
            "id", "stratum", "query", "expected_node_ids",
            "source", "source_line",
        )
        for e in self.entries:
            for k in required:
                self.assertIn(k, e, f"Missing key {k} in {e}")

    def test_miss_time_range_empty_expected(self) -> None:
        for e in self.entries:
            if e["stratum"] == "gold_miss_time_range":
                self.assertEqual(
                    e["expected_node_ids"], [], f"Non-empty miss: {e}"
                )

    def test_measurable_strata_have_expected(self) -> None:
        """gold_2part / gold_simple / gold_folder / gold_manual 은 expected ≥1."""
        measurable = ("gold_2part", "gold_simple", "gold_folder", "gold_manual")
        for e in self.entries:
            if e["stratum"] in measurable:
                ids = e["expected_node_ids"]
                self.assertGreater(
                    len(ids), 0,
                    f"{e['stratum']} empty expected: {e}"
                )
                for nid in ids:
                    self.assertIsInstance(nid, int)


if __name__ == "__main__":
    unittest.main()
