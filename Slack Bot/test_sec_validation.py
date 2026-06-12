"""
test_sec_validation.py — 보안 검증 커버리지 확대 테스트 (task: CWE-20/943 잔여 보안)

TDD-First: Red → 구현 → Green.
검증 대상:
  1. agentic_engine._validate_tool_args 추가 검사
  2. jira_client._check_jql_danger 및 search_issues 위험패턴 차단
"""

import sys
import os
import importlib
import unittest
from unittest.mock import MagicMock, patch

# ─── agentic_engine import 경량 픽스처 ──────────────────────────────────────

_AGENTIC_MOCKS = [
    "slack_bolt", "slack_sdk", "slack_sdk.web",
    "mcp_session", "mcp_core",
    "wiki_client", "gdi_client", "jira_client",
    "biskit_client", "cache_manager",
    "keyword_rules",
]

def _import_agentic():
    """agentic_engine의 외부 의존성을 sys.modules에 mock하고 임포트.
    patcher를 stop하지 않아 모듈이 정상 상태로 남는다."""
    for m in _AGENTIC_MOCKS:
        if m not in sys.modules:
            sys.modules[m] = MagicMock()

    if "agentic_engine" in sys.modules:
        del sys.modules["agentic_engine"]

    return importlib.import_module("agentic_engine")


# ─── 1. agentic_engine._validate_tool_args ───────────────────────────────────

class TestValidateToolArgs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ae = _import_agentic()
        # staticmethod처럼 저장 — 일반 함수로 wrapping하여 bound method 문제 회피
        _fn = cls.ae._validate_tool_args
        cls.v = staticmethod(_fn)

    # ── 공통: 길이 cap (2000자 초과 차단) ──────────────────────────────────

    def test_long_str_blocked_wiki_query(self):
        """2001자 query → 차단."""
        result = self.v("wiki_search_content_live", {"query": "a" * 2001})
        self.assertNotEqual(result, "", "2001자 query는 차단되어야 함")

    def test_normal_query_passes(self):
        """한글 질문 200자 → 통과."""
        result = self.v("wiki_search_content_live", {"query": "카제나 접속 불가 이슈" * 10})
        self.assertEqual(result, "", "정상 한글 질문은 통과해야 함")

    def test_long_game_name_blocked(self):
        """2001자 game_name → 차단."""
        result = self.v("gdi_unified_search", {"game_name": "x" * 2001})
        self.assertNotEqual(result, "")

    def test_normal_game_name_passes(self):
        """일반 game_name → 통과."""
        result = self.v("gdi_unified_search", {"game_name": "카제나"})
        self.assertEqual(result, "")

    # ── space_key 화이트리스트 ──────────────────────────────────────────────

    def test_space_key_valid(self):
        """유효한 space_key (영문+숫자+언더스코어) → 통과."""
        result = self.v("wiki_search_with_context", {"space_key": "GAME_DEV"})
        self.assertEqual(result, "")

    def test_space_key_empty_allowed(self):
        """빈 space_key → 통과 (옵셔널 필드)."""
        result = self.v("wiki_search_with_context", {"space_key": ""})
        self.assertEqual(result, "")

    def test_space_key_injection_blocked(self):
        """space_key에 SQL 주입 문자 → 차단."""
        result = self.v("wiki_search_with_context", {"space_key": "'; DROP TABLE--"})
        self.assertNotEqual(result, "")

    def test_space_key_too_long_blocked(self):
        """33자 space_key → 차단."""
        result = self.v("wiki_search_with_context", {"space_key": "A" * 33})
        self.assertNotEqual(result, "")

    def test_space_key_special_chars_blocked(self):
        """공백 포함 space_key → 차단."""
        result = self.v("wiki_search_with_context", {"space_key": "GAME DEV"})
        self.assertNotEqual(result, "")

    # ── source_id path traversal 차단 ──────────────────────────────────────

    def test_source_id_traversal_blocked(self):
        """source_id에 .. 포함 → 차단."""
        result = self.v("get_related_nodes", {"rel_type": "same_folder", "source_id": "../../etc/passwd"})
        self.assertNotEqual(result, "")

    def test_source_id_newline_blocked(self):
        """source_id에 개행 → 차단."""
        result = self.v("get_related_nodes", {"rel_type": "same_folder", "source_id": "foo\nbar"})
        self.assertNotEqual(result, "")

    def test_source_id_normal_passes(self):
        """정상 source_id → 통과."""
        result = self.v("get_related_nodes", {"rel_type": "same_folder", "source_id": "카제나/2026-05-01/build.xlsx"})
        self.assertEqual(result, "")

    # ── biskit_execute_query.parameters dict cap ───────────────────────────

    def test_biskit_params_too_many_keys_blocked(self):
        """parameters dict 키 50개 초과 → 차단."""
        big_dict = {f"key_{i}": "val" for i in range(51)}
        result = self.v("biskit_execute_query", {"parameters": big_dict})
        self.assertNotEqual(result, "")

    def test_biskit_params_nested_too_deep_blocked(self):
        """parameters dict 3단계 이상 중첩 → 차단."""
        deep = {"a": {"b": {"c": {"d": "too deep"}}}}
        result = self.v("biskit_execute_query", {"parameters": deep})
        self.assertNotEqual(result, "")

    def test_biskit_params_normal_passes(self):
        """정상 parameters dict → 통과."""
        result = self.v("biskit_execute_query", {"parameters": {"game": "카제나", "date": "2026-05"}})
        self.assertEqual(result, "")

    # ── 기존 4개 검사 유지 확인 ────────────────────────────────────────────

    def test_existing_biskit_get_knowledge_traversal_blocked(self):
        """기존: biskit_get_knowledge slug traversal → 차단."""
        result = self.v("biskit_get_knowledge", {"slug": "../../secret"})
        self.assertNotEqual(result, "")

    def test_existing_gdi_folder_traversal_blocked(self):
        """기존: gdi_list_files_in_folder traversal → 차단."""
        result = self.v("gdi_list_files_in_folder", {"folder_path": "../etc/passwd"})
        self.assertNotEqual(result, "")

    def test_existing_jira_jql_injection_blocked(self):
        """기존: jira_search_issues JQL OR 1=1 → 차단."""
        result = self.v("jira_search_issues", {"jql": "project = X OR 1=1"})
        self.assertNotEqual(result, "")

    def test_existing_get_related_nodes_rel_type(self):
        """기존: get_related_nodes 잘못된 rel_type → 차단."""
        result = self.v("get_related_nodes", {"rel_type": "evil", "source_id": "foo"})
        self.assertNotEqual(result, "")


# ─── 2. jira_client._check_jql_danger + search_issues ───────────────────────

class TestJiraCheckJqlDanger(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """jira_client 임포트 (외부 MCP 연결 없이)."""
        mocks = ["mcp_core", "mcp_session"]
        for m in mocks:
            if m not in sys.modules:
                sys.modules[m] = MagicMock()
        if "jira_client" in sys.modules:
            del sys.modules["jira_client"]
        import jira_client as jc
        cls.jc = jc

    def test_normal_jql_passes(self):
        """일반 JQL → None (차단 없음)."""
        result = self.jc._check_jql_danger('project = GAME AND status = "진행중"')
        self.assertIsNone(result)

    def test_korean_question_passes(self):
        """한글 질문 JQL → None."""
        result = self.jc._check_jql_danger('text ~ "카제나 접속 불가"')
        self.assertIsNone(result)

    def test_or_injection_blocked(self):
        """OR 1=1 패턴 → 에러 문자열 반환."""
        result = self.jc._check_jql_danger("project = X OR 1=1")
        self.assertIsNotNone(result)
        self.assertIn("허용되지 않은", result)

    def test_semicolon_blocked(self):
        """세미콜론 → 차단."""
        result = self.jc._check_jql_danger("project = X; DROP TABLE issues")
        self.assertIsNotNone(result)

    def test_double_dash_blocked(self):
        """-- 주석 → 차단."""
        result = self.jc._check_jql_danger("project = X -- comment")
        self.assertIsNotNone(result)

    def test_delete_blocked(self):
        """DELETE 키워드 → 차단."""
        result = self.jc._check_jql_danger("DELETE FROM issues WHERE 1=1")
        self.assertIsNotNone(result)

    def test_update_blocked(self):
        """UPDATE 키워드 → 차단."""
        result = self.jc._check_jql_danger("UPDATE issues SET status='done'")
        self.assertIsNotNone(result)

    def test_insert_blocked(self):
        """INSERT 키워드 → 차단."""
        result = self.jc._check_jql_danger("INSERT INTO issues VALUES (1)")
        self.assertIsNotNone(result)

    def test_space_bypass_blocked(self):
        """공백 우회 시도 (OR  1 = 1) → 차단."""
        result = self.jc._check_jql_danger("project = X OR  1 = 1")
        self.assertIsNotNone(result)

    def test_search_issues_returns_error_tuple_on_danger(self):
        """search_issues: 위험 JQL → (None, '허용되지 않은 쿼리 패턴') 반환."""
        # JiraClient 인스턴스 없이 직접 메서드 테스트 — mock mcp
        mock_mcp = MagicMock()
        client = self.jc.JiraClient.__new__(self.jc.JiraClient)
        client._mcp = mock_mcp

        result = client.search_issues("project = X OR 1=1")
        self.assertEqual(result[0], None)
        self.assertIn("허용되지 않은", result[1])
        # MCP 호출이 일어나지 않아야 함
        mock_mcp.call_tool.assert_not_called()

    def test_search_issues_normal_jql_calls_mcp(self):
        """search_issues: 정상 JQL → MCP 호출 진행."""
        mock_mcp = MagicMock()
        mock_mcp.call_tool.return_value = ({"issues": []}, None)
        client = self.jc.JiraClient.__new__(self.jc.JiraClient)
        client._mcp = mock_mcp

        result = client.search_issues('project = GAME AND status = "진행중"')
        mock_mcp.call_tool.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
