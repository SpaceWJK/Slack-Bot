"""
test_agentic_engine.py — task-192 Step 4 TDD

_normalize 단위 테스트: tuple/dict/list/3-tuple/20행초과 각 케이스
ToolRegistry 등록/dispatch 테스트
"""

import os
import sys
import json
import pytest
from unittest.mock import MagicMock, patch

SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)


# ── agentic_engine import (anthropic 없어도 _normalize/ToolRegistry 단위 테스트 가능) ──
# anthropic import를 mock 처리하여 환경 의존성 제거
import unittest.mock as _mock
_mock_anthropic = _mock.MagicMock()
sys.modules.setdefault("anthropic", _mock_anthropic)

# biskit_client import 차단 (환경변수 없어도 테스트 가능)
sys.modules.setdefault("biskit_client", _mock.MagicMock())
sys.modules.setdefault("mcp_session", _mock.MagicMock())
sys.modules.setdefault("wiki_client", _mock.MagicMock())
sys.modules.setdefault("gdi_client", _mock.MagicMock())
sys.modules.setdefault("jira_client", _mock.MagicMock())
sys.modules.setdefault("game_aliases", _mock.MagicMock())
sys.modules.setdefault("cost_tracker", _mock.MagicMock())

import agentic_engine as ae


# ─────────────────────────────────────────────────────────────────────────────
# T-1: _normalize — tuple (data, err) 케이스
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalizeTuple:
    def test_tuple_no_error_returns_dict_json(self):
        """(dict, None) → dict JSON 반환"""
        result = ae._normalize(({"key": "value"}, None))
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_tuple_with_error_returns_error_str(self):
        """(data, "오류") → [오류] 메시지 반환"""
        result = ae._normalize((None, "MCP 연결 실패"))
        assert result.startswith("[오류]")
        assert "MCP 연결 실패" in result

    def test_tuple_none_data_returns_empty_result(self):
        """(None, None) → [결과 없음]"""
        result = ae._normalize((None, None))
        assert result == "[결과 없음]"

    def test_3tuple_uses_first_element(self):
        """3-tuple (data, err, cache_status) — gdi search_by_filename 패턴"""
        data = {"files": [{"name": "test.xlsx"}]}
        result = ae._normalize((data, None, "HIT"))
        parsed = json.loads(result)
        assert "files" in parsed

    def test_3tuple_with_error_uses_second_element(self):
        """3-tuple 두 번째 요소가 에러이면 오류 반환"""
        result = ae._normalize((None, "파일 없음", "MISS"))
        assert "[오류]" in result


# ─────────────────────────────────────────────────────────────────────────────
# T-2: _normalize — dict 행 제한 (20행 초과 케이스)
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalizeDict:
    def test_dict_data_rows_truncated_to_20(self):
        """data 키 배열 25행 → 20행으로 제한 + 메타 보존"""
        rows = [{"i": i} for i in range(25)]
        result = ae._normalize({"data": rows, "citation": "출처X"})
        parsed = json.loads(result)
        assert len(parsed["data"]) == 20
        assert "_data_truncated" in parsed
        assert "citation" in parsed  # 메타 필드 보존

    def test_dict_rows_key_truncated(self):
        """rows 키 30행 → 20행"""
        rows = [{"n": i} for i in range(30)]
        result = ae._normalize({"rows": rows})
        parsed = json.loads(result)
        assert len(parsed["rows"]) == 20
        assert "_rows_truncated" in parsed

    def test_dict_datasets_key_truncated(self):
        """datasets 키 21행 → 20행"""
        datasets = [{"id": i} for i in range(21)]
        result = ae._normalize({"datasets": datasets})
        parsed = json.loads(result)
        assert len(parsed["datasets"]) == 20

    def test_dict_articles_key_truncated(self):
        """articles 키 22행 → 20행"""
        articles = [{"slug": f"s{i}"} for i in range(22)]
        result = ae._normalize({"articles": articles})
        parsed = json.loads(result)
        assert len(parsed["articles"]) == 20

    def test_dict_exactly_20_rows_no_truncation(self):
        """정확히 20행이면 truncation 없음"""
        rows = [{"i": i} for i in range(20)]
        result = ae._normalize({"data": rows})
        parsed = json.loads(result)
        assert len(parsed["data"]) == 20
        assert "_data_truncated" not in parsed

    def test_dict_schema_preserved(self):
        """schema/analysis_context 등 메타 필드 보존"""
        payload = {
            "data": [{"x": 1}],
            "schema": {"columns": ["x"]},
            "analysis_context": "테스트",
        }
        result = ae._normalize(payload)
        parsed = json.loads(result)
        assert "schema" in parsed
        assert "analysis_context" in parsed

    def test_dict_result_within_8000_chars(self):
        """8000자 초과 시 truncated 태그 추가"""
        large_dict = {"data": [{"x": "a" * 500} for _ in range(20)]}
        result = ae._normalize(large_dict)
        assert len(result) <= 8100  # 8000 + '...[truncated]' 여유


# ─────────────────────────────────────────────────────────────────────────────
# T-3: _normalize — list 케이스
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalizeList:
    def test_list_truncated_to_20(self):
        """list 25개 → 20개"""
        result = ae._normalize([{"i": i} for i in range(25)])
        parsed = json.loads(result)
        assert len(parsed) == 20

    def test_list_under_20_preserved(self):
        """list 5개 → 그대로"""
        result = ae._normalize([1, 2, 3, 4, 5])
        parsed = json.loads(result)
        assert len(parsed) == 5

    def test_empty_list(self):
        """빈 list → []"""
        result = ae._normalize([])
        assert json.loads(result) == []


# ─────────────────────────────────────────────────────────────────────────────
# T-4: _normalize — string/None/primitive 케이스
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalizePrimitive:
    def test_none_returns_empty_result(self):
        result = ae._normalize(None)
        assert result == "[결과 없음]"

    def test_string_returns_as_is(self):
        result = ae._normalize("hello world")
        assert result == "hello world"

    def test_string_truncated_at_4000(self):
        long_str = "a" * 5000
        result = ae._normalize(long_str)
        assert len(result) <= 4000

    def test_integer_converted_to_str(self):
        result = ae._normalize(42)
        assert "42" in result


# ─────────────────────────────────────────────────────────────────────────────
# T-5: ToolRegistry — 등록 및 dispatch
# ─────────────────────────────────────────────────────────────────────────────

class TestToolRegistry:
    def setup_method(self):
        self.registry = ae.ToolRegistry()
        self.registry.register(
            name="test_tool",
            description="테스트 도구",
            input_schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
            handler=lambda a: f"결과: {a['query']}",
        )

    def test_register_stores_tool(self):
        schemas = self.registry.get_schemas(whitelist=["test_tool"])
        assert len(schemas) == 1
        assert schemas[0]["name"] == "test_tool"

    def test_get_schemas_filters_by_whitelist(self):
        self.registry.register("other_tool", "other", {}, lambda a: "x")
        schemas = self.registry.get_schemas(whitelist=["test_tool"])
        names = [s["name"] for s in schemas]
        assert "test_tool" in names
        assert "other_tool" not in names

    def test_get_schemas_none_whitelist_returns_all(self):
        self.registry.register("other_tool2", "other2", {}, lambda a: "y")
        schemas = self.registry.get_schemas(whitelist=None)
        names = [s["name"] for s in schemas]
        assert "test_tool" in names
        assert "other_tool2" in names

    def test_dispatch_calls_handler(self):
        result = self.registry.dispatch("test_tool", {"query": "안녕"})
        assert "안녕" in result

    def test_dispatch_unknown_tool_returns_error(self):
        result = self.registry.dispatch("nonexistent_tool", {})
        assert "[도구" in result and "오류" in result

    def test_dispatch_handler_exception_returns_error_not_raise(self):
        """핸들러 예외 시 loop 크래시 방지 — 에러 문자열 반환"""
        self.registry.register(
            "bad_tool", "bad", {},
            handler=lambda a: (_ for _ in ()).throw(RuntimeError("mock error")),
        )
        result = self.registry.dispatch("bad_tool", {})
        assert "[도구" in result and "오류" in result

    def test_schema_format_valid_for_anthropic(self):
        """Anthropic tools= 형식 검증: name/description/input_schema 필드 포함"""
        schemas = self.registry.get_schemas(whitelist=["test_tool"])
        schema = schemas[0]
        assert "name" in schema
        assert "description" in schema
        assert "input_schema" in schema


# ─────────────────────────────────────────────────────────────────────────────
# T-6: 모듈 레벨 싱글톤 및 상수 검증
# ─────────────────────────────────────────────────────────────────────────────

class TestModuleConstants:
    def test_max_iter_is_10(self):
        """Step 5 M-2 토큰 초과 대응 — MAX_ITER 8→10 상향"""
        assert ae.MAX_ITER == 10

    def test_token_budget_is_80000(self):
        """Step 5 M-2 토큰 초과 대응 — TOKEN_BUDGET 50000→80000 상향"""
        assert ae.TOKEN_BUDGET == 80000

    def test_registry_singleton_exists(self):
        assert ae._REGISTRY is not None
        assert isinstance(ae._REGISTRY, ae.ToolRegistry)

    def test_registry_has_17_tools(self):
        """17개 도구 등록 확인 (biskit 7 + wiki 4 + gdi 3 + jira 3)"""
        schemas = ae._REGISTRY.get_schemas(whitelist=None)
        assert len(schemas) == 17, f"도구 수: {len(schemas)} (기대: 17)"

    def test_ask_semaphore_exists(self):
        import threading
        assert isinstance(ae._ask_semaphore, type(threading.Semaphore(1)))
