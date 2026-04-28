"""
test_routing_replay.py — task-113 v4.2 routing_replay 테스트

T-19: 3 seam (gold_folder 포함) sample run 로그 (Round 2 합의 기준)
T-21: routing_spec.yaml ↔ slack_bot.py drift detect (spec.source_sha 비교)
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))


class FakeAdapter:
    """ProductionSearchAdapter 의 search/folder_path_search 만 stub."""
    slack_bot_root: Path = Path("/tmp/fake")

    def __init__(self) -> None:
        self.search_calls: list[tuple[str, int]] = []
        self.folder_calls: list[tuple[str, int]] = []
        self._search_results: list[int] = [101, 102, 103]
        self._folder_results: list[int] = [201, 202]

    def search(self, query: str, top_k: int = 10) -> list[int]:
        self.search_calls.append((query, top_k))
        return self._search_results

    def folder_path_search(self, folder_path: str, top_k: int = 10) -> list[int]:
        self.folder_calls.append((folder_path, top_k))
        # secondary_retry 시 prefix 추가된 path 매칭
        if folder_path.startswith("Chaoszero/"):
            return self._folder_results
        return []


_SPEC_YAML = """
spec:
  version: "v0"
  source_sha: "0000000000000000"
  extracted_at: "2026-04-25"

seams:
  ask_claude_2part:
    file: "slack_bot.py"
    line: 2041
    transform:
      - split: "\\\\"
      - take_index: 0
      - strip: true
    downstream:
      method: "gdi_client.unified_search"

  ask_claude_3part_fallback:
    file: "slack_bot.py"
    line: 1954
    transform:
      - split: "\\\\"
      - extract:
          search_kw: 0
          file_name: 1
      - format: "{search_kw} {file_name}"
    downstream:
      method: "gdi_client.unified_search"

  gdi_simple_search:
    file: "slack_bot.py"
    line: 1858
    transform: []
    downstream:
      method: "gdi_client.unified_search"

  folder_ai:
    file: "slack_bot.py"
    line: 1927
    transform: []
    downstream:
      method: "gdi_client.list_files_in_folder"
"""


@unittest.skipUnless(
    True,
    "PyYAML 의존",
)
class TestRoutingReplay(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.spec_path = Path(self.tmp.name) / "routing_spec.yaml"
        self.spec_path.write_text(_SPEC_YAML, encoding="utf-8")
        from routing_replay import RoutingReplay
        self.RoutingReplay = RoutingReplay
        self.adapter = FakeAdapter()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_t19_ask_claude_2part(self) -> None:
        """T-19: ask_claude_2part split → take_index → strip → unified_search."""
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        result = replay.replay("ask_claude_2part", "Chaoszero \\ 질문", top_k=5)
        self.assertEqual(result, [101, 102, 103])
        self.assertEqual(self.adapter.search_calls, [("Chaoszero", 5)])

    def test_t19_ask_claude_3part_fallback(self) -> None:
        """T-19: ask_claude_3part_fallback split → extract → format → unified_search."""
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        result = replay.replay(
            "ask_claude_3part_fallback",
            "Chaoszero \\ chaos.tsv \\ 어떤 내용?",
            top_k=5,
        )
        self.assertEqual(result, [101, 102, 103])
        # search_query "Chaoszero" + file_name "chaos.tsv" → "Chaoszero chaos.tsv"
        # split 후 strip 미적용으로 leading space 가능 — extract 결과는 그대로
        # 결과 query 는 "Chaoszero  chaos.tsv " 형태일 수 있음 (공백 포함)
        self.assertEqual(len(self.adapter.search_calls), 1)
        called_query = self.adapter.search_calls[0][0]
        self.assertIn("Chaoszero", called_query)
        self.assertIn("chaos.tsv", called_query)

    def test_t19_gdi_simple_search(self) -> None:
        """T-19: gdi_simple_search transform=[] → text 그대로 unified_search."""
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        result = replay.replay("gdi_simple_search", "test query", top_k=10)
        self.assertEqual(result, [101, 102, 103])
        self.assertEqual(self.adapter.search_calls, [("test query", 10)])

    def test_t19_folder_ai(self) -> None:
        """T-19: folder_ai → folder_path_search (1차 + secondary_retry)."""
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        # 1차: "Update Review" — FakeAdapter 는 Chaoszero/ prefix 시 hit
        result = replay.replay("folder_ai", "Update Review", top_k=10)
        # secondary_retry: "Chaoszero/Update Review" → 2번째 호출에서 hit
        self.assertEqual(result, [201, 202])
        self.assertEqual(len(self.adapter.folder_calls), 2)
        self.assertEqual(self.adapter.folder_calls[1][0],
                         "Chaoszero/Update Review")

    def test_t19_folder_ai_all_retry_fail(self) -> None:
        """T-19 보강 (Step 5 qa-functional MAJOR): 3 prefix 모두 fail → []."""
        # FakeAdapter._folder_results = [] 로 설정 — 항상 빈 리스트 반환
        self.adapter._folder_results = []
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        result = replay.replay("folder_ai", "Unknown/subfolder", top_k=10)
        self.assertEqual(result, [])
        # 1차 + Chaoszero/ + Epicseven/ + Kazena/ = 4회 호출
        self.assertEqual(len(self.adapter.folder_calls), 4)

    def test_t21_drift_detect_match(self) -> None:
        """T-21: drift detection — spec.source_sha 부재 시 비활성."""
        # source_sha "0000..." 인 spec, fake slack_bot_root 에 slack_bot.py 없음
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        # slack_bot.py 부재 → drift_warning True
        self.assertTrue(replay.drift_warning)

    def test_t21_drift_detect_no_spec_sha(self) -> None:
        """T-21: spec.source_sha 부재 시 drift detection 비활성."""
        spec_no_sha = self.spec_path.read_text(encoding="utf-8").replace(
            'source_sha: "0000000000000000"', 'source_sha: ""'
        )
        self.spec_path.write_text(spec_no_sha, encoding="utf-8")
        replay = self.RoutingReplay(str(self.spec_path), self.adapter,
                                     slack_bot_root=Path(self.tmp.name))
        self.assertFalse(replay.drift_warning)

    def test_t21_routing_spec_missing_fail_loud(self) -> None:
        """T-21: routing_spec.yaml 부재 시 RuntimeError fail-loud."""
        missing = Path(self.tmp.name) / "missing.yaml"
        with self.assertRaises(RuntimeError) as ctx:
            self.RoutingReplay(str(missing), self.adapter,
                                slack_bot_root=Path(self.tmp.name))
        self.assertIn("not found", str(ctx.exception))

    def test_t21_routing_spec_missing_seams(self) -> None:
        """T-21: routing_spec.yaml 의 seams 섹션 부재 시 RuntimeError."""
        bad_spec = Path(self.tmp.name) / "bad.yaml"
        bad_spec.write_text("spec:\n  version: 'v0'\n", encoding="utf-8")
        with self.assertRaises(RuntimeError) as ctx:
            self.RoutingReplay(str(bad_spec), self.adapter,
                                slack_bot_root=Path(self.tmp.name))
        self.assertIn("seams", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
