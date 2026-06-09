"""
test_agentic_grid.py — agentic_engine 그리드 도구 등록/핸들러 단위테스트 (task-193 ②)

TDD-First: 구현 전 Red 확인 → 구현 후 Green.
"""

import importlib
import json
import sys
import unittest
from unittest.mock import MagicMock, patch


class TestLoadGridSummary(unittest.TestCase):
    """_load_grid_summary() 로더 테스트."""

    def _reload(self):
        """모듈 재로드 (각 테스트 독립)."""
        if "agentic_engine" in sys.modules:
            del sys.modules["agentic_engine"]
        return importlib.import_module("agentic_engine")

    def test_returns_string_on_valid_json(self):
        """유효한 knowledge_grid.json → 비어있지 않은 문자열 반환."""
        sample = {
            "gdi": {
                "games": {"카제나": 100},
                "file_kinds": {"issue_unit_planning": 50},
                "recent_builds": {"카제나": ["20260527"]},
            },
            "wiki": {"categories": {"qa": 10}},
            "jira": {"game_tags": {"카제나": 30}},
        }
        ae = self._reload()
        # 내부 캐시 초기화
        ae._GRID_SUMMARY = None
        with patch("builtins.open", unittest.mock.mock_open(read_data=json.dumps(sample))):
            with patch("os.path.exists", return_value=True):
                result = ae._load_grid_summary()
        self.assertIsInstance(result, str)
        self.assertIn("카제나", result)

    def test_returns_empty_string_on_missing_file(self):
        """파일 없음 → graceful '' 반환 (crash 없음)."""
        ae = self._reload()
        ae._GRID_SUMMARY = None
        with patch("builtins.open", side_effect=FileNotFoundError("no file")):
            result = ae._load_grid_summary()
        self.assertEqual(result, "")

    def test_returns_empty_string_on_invalid_json(self):
        """잘못된 JSON → graceful '' 반환."""
        ae = self._reload()
        ae._GRID_SUMMARY = None
        with patch("builtins.open", unittest.mock.mock_open(read_data="NOT_JSON")):
            result = ae._load_grid_summary()
        self.assertEqual(result, "")

    def test_cached_after_first_load(self):
        """두 번째 호출은 파일 재열기 없이 캐시 반환."""
        sample = {"gdi": {"games": {}}}
        ae = self._reload()
        ae._GRID_SUMMARY = None
        call_count = [0]
        orig_open = open

        def counting_open(*args, **kwargs):
            call_count[0] += 1
            return unittest.mock.mock_open(read_data=json.dumps(sample))()

        with patch("builtins.open", counting_open):
            ae._load_grid_summary()
            ae._load_grid_summary()
        # 두 번 호출해도 open은 1회
        self.assertEqual(call_count[0], 1)


class TestGridToolsRegistered(unittest.TestCase):
    """gdi_get_build_index, get_related_nodes 도구 등록 확인."""

    @classmethod
    def setUpClass(cls):
        # gdi_client, biskit_client 등 외부 의존성 mock
        cls._patches = []
        for mod in ("biskit_client", "wiki_client", "gdi_client", "jira_client", "cost_tracker"):
            p = patch.dict("sys.modules", {mod: MagicMock()})
            p.start()
            cls._patches.append(p)

        if "agentic_engine" in sys.modules:
            del sys.modules["agentic_engine"]
        import agentic_engine as ae
        cls.ae = ae

    @classmethod
    def tearDownClass(cls):
        for p in cls._patches:
            p.stop()

    def test_gdi_get_build_index_registered(self):
        """gdi_get_build_index 도구가 레지스트리에 존재."""
        reg = self.ae._get_registry()
        schemas = reg.get_schemas(None)
        names = [s["name"] for s in schemas]
        self.assertIn("gdi_get_build_index", names)

    def test_get_related_nodes_registered(self):
        """get_related_nodes 도구가 레지스트리에 존재."""
        reg = self.ae._get_registry()
        schemas = reg.get_schemas(None)
        names = [s["name"] for s in schemas]
        self.assertIn("get_related_nodes", names)

    def test_total_tool_count_includes_two_new(self):
        """신규 도구 2개(gdi_get_build_index, get_related_nodes)가 목록에 존재."""
        reg = self.ae._build_registry()
        schemas = reg.get_schemas(None)
        names = [s["name"] for s in schemas]
        self.assertIn("gdi_get_build_index", names)
        self.assertIn("get_related_nodes", names)
        # GDI 그룹에 신규 2개 포함하여 최소 5개 이상
        gdi_tools = [n for n in names if n.startswith("gdi_") or n == "get_related_nodes"]
        self.assertGreaterEqual(len(gdi_tools), 5)

    def test_gdi_get_build_index_schema_required_field(self):
        """gdi_get_build_index: game_tag required."""
        reg = self.ae._get_registry()
        schemas = reg.get_schemas(None)
        schema = next(s for s in schemas if s["name"] == "gdi_get_build_index")
        self.assertIn("game_tag", schema["input_schema"]["required"])

    def test_get_related_nodes_schema_required_fields(self):
        """get_related_nodes: source_id, rel_type required."""
        reg = self.ae._get_registry()
        schemas = reg.get_schemas(None)
        schema = next(s for s in schemas if s["name"] == "get_related_nodes")
        self.assertIn("source_id", schema["input_schema"]["required"])
        self.assertIn("rel_type", schema["input_schema"]["required"])


class TestGridToolHandlers(unittest.TestCase):
    """핸들러 → gdi_client 함수 호출 매핑 검증."""

    def setUp(self):
        self._patches = []
        for mod in ("biskit_client", "wiki_client", "jira_client", "cost_tracker"):
            p = patch.dict("sys.modules", {mod: MagicMock()})
            p.start()
            self._patches.append(p)

        # gdi_client mock — search_by_build_meta / get_related 추적 가능
        self.mock_gc = MagicMock()
        self.mock_gc.GdiClient.return_value = MagicMock()
        self.mock_gc.search_by_build_meta = MagicMock(return_value=[])
        self.mock_gc.get_related = MagicMock(return_value=[])
        p = patch.dict("sys.modules", {"gdi_client": self.mock_gc})
        p.start()
        self._patches.append(p)

        if "agentic_engine" in sys.modules:
            del sys.modules["agentic_engine"]

    def tearDown(self):
        for p in self._patches:
            p.stop()
        if "agentic_engine" in sys.modules:
            del sys.modules["agentic_engine"]

    def _get_reg(self):
        import agentic_engine as ae
        return ae._build_registry()

    def test_gdi_get_build_index_calls_search_by_build_meta_required_only(self):
        """game_tag만 전달 시 search_by_build_meta(game_tag=...) 호출."""
        reg = self._get_reg()
        reg.dispatch("gdi_get_build_index", {"game_tag": "카제나"})
        self.mock_gc.search_by_build_meta.assert_called_once()
        call_kwargs = self.mock_gc.search_by_build_meta.call_args
        # positional or keyword 모두 허용
        args, kwargs = call_kwargs
        game_tag_val = kwargs.get("game_tag") or (args[0] if args else None)
        self.assertEqual(game_tag_val, "카제나")

    def test_gdi_get_build_index_passes_optional_args(self):
        """선택 인자(build_date, file_kind, build_type, build_seq) 전달 확인."""
        reg = self._get_reg()
        reg.dispatch("gdi_get_build_index", {
            "game_tag": "에픽세븐",
            "build_date": "20260527",
            "file_kind": "issue_unit_planning",
            "build_type": "정규",
            "build_seq": "2",
        })
        self.mock_gc.search_by_build_meta.assert_called_once()
        _, kwargs = self.mock_gc.search_by_build_meta.call_args
        self.assertEqual(kwargs.get("build_date"), "20260527")
        self.assertEqual(kwargs.get("file_kind"), "issue_unit_planning")

    def test_get_related_nodes_calls_get_related(self):
        """source_id + rel_type → get_related 호출."""
        reg = self._get_reg()
        reg.dispatch("get_related_nodes", {
            "source_id": "gdi:abc123",
            "rel_type": "same_folder",
        })
        self.mock_gc.get_related.assert_called_once()
        _, kwargs = self.mock_gc.get_related.call_args
        self.assertEqual(kwargs.get("source_id"), "gdi:abc123")
        self.assertEqual(kwargs.get("rel_type"), "same_folder")

    def test_get_related_nodes_passes_limit(self):
        """limit 파라미터 전달 확인."""
        reg = self._get_reg()
        reg.dispatch("get_related_nodes", {
            "source_id": "gdi:xyz",
            "rel_type": "same_issue",
            "limit": 5,
        })
        _, kwargs = self.mock_gc.get_related.call_args
        self.assertEqual(kwargs.get("limit"), 5)


class TestSystemPromptContainsGridRules(unittest.TestCase):
    """SYSTEM_PROMPT에 그리드 규칙 포함 확인."""

    @classmethod
    def setUpClass(cls):
        cls._patches = []
        for mod in ("biskit_client", "wiki_client", "gdi_client", "jira_client", "cost_tracker"):
            p = patch.dict("sys.modules", {mod: MagicMock()})
            p.start()
            cls._patches.append(p)
        if "agentic_engine" in sys.modules:
            del sys.modules["agentic_engine"]
        import agentic_engine as ae
        cls.ae = ae

    @classmethod
    def tearDownClass(cls):
        for p in cls._patches:
            p.stop()

    def test_system_prompt_has_gdi_meta_rule(self):
        """GDI 메타 축 탐색 절대 규칙 포함."""
        self.assertIn("GDI 메타 축 탐색", self.ae.SYSTEM_PROMPT)

    def test_system_prompt_has_issue_fallback(self):
        """issue_number 0건 fallback 규칙 포함."""
        self.assertIn("gdi_get_build_index", self.ae.SYSTEM_PROMPT)

    def test_system_prompt_has_cross_chain_rule(self):
        """BISKIT ↔ GDI 교차 분석 체인 포함."""
        self.assertIn("교차 분석 체인", self.ae.SYSTEM_PROMPT)


if __name__ == "__main__":
    unittest.main()
