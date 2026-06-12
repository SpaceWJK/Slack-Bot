"""
test_agentic_failfast.py — E-3 조기종료 검증 (TDD)

케이스:
  1. 전실패 2연속 → 조기종료 메시지 반환 + LLM 호출 최소화
  2. 1회 전실패 후 1회 부분성공 → 조기종료 없이 계속 진행
  3. 정상(도구 성공) → 조기종료 없이 end_turn 도달
  4. 도구 없는 단순 질문(end_turn) → 기존 동작 불변
"""

import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼: Anthropic SDK 객체 모킹
# ─────────────────────────────────────────────────────────────────────────────

def _make_tool_use_block(tool_id: str, name: str):
    b = MagicMock()
    b.type = "tool_use"
    b.id = tool_id
    b.name = name
    b.input = {}
    return b


def _make_text_block(text: str):
    b = MagicMock()
    b.type = "text"
    b.text = text
    return b


def _make_resp(stop_reason: str, blocks: list, in_tok: int = 10, out_tok: int = 20):
    r = MagicMock()
    r.stop_reason = stop_reason
    r.content = blocks
    usage = MagicMock()
    usage.input_tokens = in_tok
    usage.output_tokens = out_tok
    r.usage = usage
    return r


# ─────────────────────────────────────────────────────────────────────────────
# 픽스처: run_agentic에서 레지스트리 / Anthropic 클라이언트 교체
# ─────────────────────────────────────────────────────────────────────────────

def _stub_registry(dispatch_results: list):
    """dispatch_results: 순서대로 반환할 문자열 목록"""
    reg = MagicMock()
    reg.get_schemas.return_value = []
    reg.dispatch.side_effect = dispatch_results
    return reg


class TestAgenticFailFast(unittest.TestCase):

    def _run(self, client_create_side_effect, dispatch_results, *, whitelist=None):
        """run_agentic 실행 헬퍼. 레지스트리와 Anthropic 클라이언트를 mock."""
        import agentic_engine as ae

        reg = _stub_registry(dispatch_results)
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = client_create_side_effect

        # TrackedAnthropic import가 실패해도 fallback anthropic.Anthropic가 쓰이므로
        # agentic_engine 내부 Anthropic 클라이언트 생성 경로를 모두 patch
        with patch.object(ae, "_get_registry", return_value=reg), \
             patch.object(ae, "_load_catalog_summary", return_value=""), \
             patch.object(ae, "_load_grid_summary", return_value=""), \
             patch("anthropic.Anthropic", return_value=mock_client), \
             patch.dict("sys.modules", {"cost_tracker": MagicMock(
                 TrackedAnthropic=lambda *a, **kw: mock_client
             )}):
            result = ae.run_agentic("테스트 질문", whitelist)
        return result, mock_client, reg

    # ── 케이스 1: 전실패 2연속 → 조기종료 ──────────────────────────────────

    def test_two_consec_all_fail_returns_early(self):
        """iter 0, 1 모두 전체 도구 실패 → 조기종료 메시지"""
        tool_block_0a = _make_tool_use_block("id0a", "wiki_search_with_context")
        tool_block_1a = _make_tool_use_block("id1a", "gdi_unified_search")

        responses = [
            _make_resp("tool_use", [tool_block_0a]),  # iter 0: 도구 호출
            _make_resp("tool_use", [tool_block_1a]),  # iter 1: 도구 호출
        ]
        dispatch_results = [
            "[도구 wiki_search_with_context 실행 오류] 조회에 실패했습니다.",  # iter 0 실패
            "[도구 gdi_unified_search 실행 오류] 조회에 실패했습니다.",        # iter 1 실패
        ]

        result, mock_client, reg = self._run(responses, dispatch_results)

        self.assertIn("외부 데이터 서비스 연결이 불안정합니다", result)
        # LLM은 iter 0, 1 두 번만 호출되어야 함 (최종 요약 호출 없음)
        self.assertEqual(mock_client.messages.create.call_count, 2)

    # ── 케이스 2: 1회 전실패 → 1회 부분성공 → 조기종료 없이 계속 ────────────

    def test_one_fail_then_partial_success_continues(self):
        """iter 0 전실패 → iter 1 하나 성공 → _consec_all_fail 리셋 → 계속"""
        tool_0 = _make_tool_use_block("id0", "wiki_search_with_context")
        tool_1a = _make_tool_use_block("id1a", "gdi_unified_search")
        tool_1b = _make_tool_use_block("id1b", "biskit_list_projects")
        text_block = _make_text_block("최종 답변입니다.")

        responses = [
            _make_resp("tool_use", [tool_0]),          # iter 0: 도구 1개 호출
            _make_resp("tool_use", [tool_1a, tool_1b]),  # iter 1: 도구 2개 호출
            _make_resp("end_turn", [text_block]),       # iter 2: 최종
        ]
        dispatch_results = [
            "[도구 wiki_search_with_context 실행 오류] 실패.",  # iter 0 실패
            "[도구 gdi_unified_search 실행 오류] 실패.",        # iter 1a 실패
            "프로젝트 목록 정상 반환",                           # iter 1b 성공 → 리셋
        ]

        result, mock_client, reg = self._run(responses, dispatch_results)

        self.assertEqual(result, "최종 답변입니다.")
        # 조기종료 메시지 미포함 확인
        self.assertNotIn("외부 데이터 서비스 연결이 불안정합니다", result)

    # ── 케이스 3: 모든 도구 성공 → end_turn 정상 도달 ─────────────────────

    def test_all_success_reaches_end_turn(self):
        """도구 호출 전부 성공 → end_turn으로 정상 종료"""
        tool_0 = _make_tool_use_block("id0", "biskit_list_projects")
        text_block = _make_text_block("정상 답변")

        responses = [
            _make_resp("tool_use", [tool_0]),
            _make_resp("end_turn", [text_block]),
        ]
        dispatch_results = ["프로젝트 목록 데이터"]

        result, mock_client, _ = self._run(responses, dispatch_results)

        self.assertEqual(result, "정상 답변")
        self.assertEqual(mock_client.messages.create.call_count, 2)

    # ── 케이스 4: 도구 없는 end_turn → 기존 동작 불변 ─────────────────────

    def test_no_tool_end_turn_unaffected(self):
        """도구 미호출 즉시 end_turn → 기존 동작 100% 동일"""
        text_block = _make_text_block("단순 답변")
        responses = [_make_resp("end_turn", [text_block])]

        result, mock_client, reg = self._run(responses, [])

        self.assertEqual(result, "단순 답변")
        reg.dispatch.assert_not_called()
        self.assertEqual(mock_client.messages.create.call_count, 1)

    # ── 케이스 5: 1회만 전실패 → 조기종료 없음 (임계 2회 미달) ───────────

    def test_single_all_fail_does_not_exit_early(self):
        """1회만 전실패 → 아직 조기종료 없이 다음 iter 진행"""
        tool_0 = _make_tool_use_block("id0", "gdi_unified_search")
        text_block = _make_text_block("이후 답변")

        responses = [
            _make_resp("tool_use", [tool_0]),
            _make_resp("end_turn", [text_block]),
        ]
        dispatch_results = [
            "[도구 gdi_unified_search 실행 오류] 실패.",
        ]

        result, _, _ = self._run(responses, dispatch_results)
        self.assertEqual(result, "이후 답변")
        self.assertNotIn("외부 데이터 서비스 연결이 불안정합니다", result)


if __name__ == "__main__":
    unittest.main()
