"""
test_production_adapter.py — ProductionSearchAdapter (task-113 T-1/T-2/T-3)

검증:
- T-1: SLACK_BOT_PATH 미설정 시 RuntimeError
- T-2: gdi_client.py 부재 시 RuntimeError
- T-3: search() 동일 쿼리 3회 byte-identical (SLACK_BOT_PATH 정상 환경에서만)
- _extract_node_id 동작 검증 (Slack Bot 의존 없는 unit)
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from production_adapter import ProductionSearchAdapter  # noqa: E402


class TestExtractNodeId(unittest.TestCase):
    def test_present_int(self) -> None:
        self.assertEqual(
            ProductionSearchAdapter._extract_node_id({"_node_id": 42}), 42
        )

    def test_present_str_int(self) -> None:
        self.assertEqual(
            ProductionSearchAdapter._extract_node_id({"_node_id": "7"}), 7
        )

    def test_missing(self) -> None:
        self.assertIsNone(
            ProductionSearchAdapter._extract_node_id({"file_name": "x"})
        )

    def test_none_value(self) -> None:
        self.assertIsNone(
            ProductionSearchAdapter._extract_node_id({"_node_id": None})
        )

    def test_invalid_value(self) -> None:
        self.assertIsNone(
            ProductionSearchAdapter._extract_node_id({"_node_id": "foo"})
        )

    def test_non_dict(self) -> None:
        self.assertIsNone(
            ProductionSearchAdapter._extract_node_id("not a dict")
        )


class TestSlackBotPathRequired(unittest.TestCase):
    def test_missing_env_raises(self) -> None:
        env_clean = dict(os.environ)
        env_clean.pop("SLACK_BOT_PATH", None)
        with mock.patch.dict(os.environ, env_clean, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                ProductionSearchAdapter()
            self.assertIn("SLACK_BOT_PATH", str(ctx.exception))

    def test_invalid_path_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # 빈 디렉터리 (gdi_client.py 부재)
            with self.assertRaises(RuntimeError) as ctx:
                ProductionSearchAdapter(slack_bot_path=tmp)
            self.assertIn("gdi_client.py", str(ctx.exception))


@unittest.skipUnless(
    os.environ.get("SLACK_BOT_PATH")
    and (Path(os.environ.get("SLACK_BOT_PATH", "")) / "gdi_client.py").exists(),
    "SLACK_BOT_PATH 환경변수 + gdi_client.py 가 있어야 실행",
)
class TestSearchDeterminism(unittest.TestCase):
    def test_three_runs_identical(self) -> None:
        adapter = ProductionSearchAdapter()
        q = "Chaoszero"
        out1 = adapter.search(q, top_k=10)
        out2 = adapter.search(q, top_k=10)
        out3 = adapter.search(q, top_k=10)
        self.assertEqual(out1, out2)
        self.assertEqual(out2, out3)


if __name__ == "__main__":
    unittest.main()
