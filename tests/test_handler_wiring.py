"""
test_handler_wiring.py — task-129.5 wiring TDD (T-1 ~ T-14)

intent_pipeline.py의 run_wiki_intent_pipeline / run_gdi_intent_pipeline 검증.
slack_bot.py 핸들러 본체 직접 호출 대신 분리된 helper 함수로 testability 확보.
slack_bolt 의존성 우회를 위해 intent_pipeline.py를 별도 모듈로 분리.

설계: step2_design_v2.md (Step 3 검수 시정 적용)
"""

import os
import sys
import pytest
from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)

CACHE_LAYER_ROOT = "D:/Vibe Dev/QA Ops/mcp-cache-layer"
if CACHE_LAYER_ROOT not in sys.path:
    sys.path.insert(0, CACHE_LAYER_ROOT)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: mock 클래스 (task-129 dataclass mimics)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _MockHit:
    node_id: int = 1
    chunk_id: int = None
    title: str = ""
    snippet: str = ""
    score: float = 0.0
    metadata: dict = field(default_factory=dict)


class _MockSearchResult:
    def __init__(self, hits=None, total_count=None, relaxation_level=0, history=None):
        self.hits = hits or []
        self.total_count = total_count if total_count is not None else len(self.hits)
        self.relaxation_level = relaxation_level
        self.history = history or []


def _make_wiki_intent(**kwargs):
    """WikiIntent 생성 헬퍼."""
    from intent_extractor import WikiIntent
    return WikiIntent(**kwargs)


def _make_gdi_intent(**kwargs):
    """GdiIntent 생성 헬퍼."""
    from intent_extractor import GdiIntent
    return GdiIntent(**kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# T-7: CacheManager.get_db_path() 양쪽 거울본 검증
# ─────────────────────────────────────────────────────────────────────────────

class TestCacheManagerFacade:
    """T-7: cache_manager.get_db_path() 인터페이스 검증."""

    def test_T7_get_db_path_method_exists_qa_ops(self):
        """T-7a: 본진 QA Ops/mcp-cache-layer/src/cache_manager.py에 get_db_path 존재."""
        from src.cache_manager import CacheManager
        assert hasattr(CacheManager, "get_db_path"), \
            "T-7a FAIL: 본진 CacheManager에 get_db_path 미정의"
        assert callable(getattr(CacheManager, "get_db_path"))


# ─────────────────────────────────────────────────────────────────────────────
# T-1, T-3, T-4, T-5, T-5b, T-5c, T-6, T-14: wiki pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestWikiPipeline:
    """task-129.5 run_wiki_intent_pipeline 검증."""

    def _import_pipeline(self):
        from intent_pipeline import run_wiki_intent_pipeline
        return run_wiki_intent_pipeline

    def test_T1_extract_intent_full_text(self):
        """T-1: extract_intent에 partition 이전 full_text 전달 (MAJOR-NEW-6)."""
        pipeline = self._import_pipeline()

        captured = {}

        def mock_extract(text, domain):
            captured["text"] = text
            captured["domain"] = domain
            return _make_wiki_intent(
                request_type="metadata",
                metadata_field="last_modified",
                page_path_segments=["카제나", "TEST INFO"],
                ai_failed=False,
            )

        ie_mod = MagicMock()
        ie_mod.extract_intent = mock_extract
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="TEST INFO", metadata={"last_modified": "2026-04-25T10:00:00"})],
            total_count=1, relaxation_level=0,
        )
        af_mod = MagicMock()
        af_mod.format_metadata_answer.return_value = "metadata answer"

        respond = MagicMock()
        cache_mgr = MagicMock()

        full_text = "카제나 \\ TEST INFO 최근 업데이트 된 날짜?"
        page_part = "카제나"
        question = "TEST INFO 최근 업데이트 된 날짜?"

        result = pipeline(
            text=full_text, page_part=page_part, question=question,
            respond=respond, cache_mgr=cache_mgr,
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert captured.get("text") == full_text, (
            f"T-1 FAIL (MAJOR-NEW-6): full_text 미전달. 받음={captured.get('text')!r}"
        )
        assert captured.get("domain") == "wiki"

    def test_T3_ai_failed_returns_true_with_explicit_message(self):
        """T-3 (task-132 PR1-B 시정): intent.ai_failed=True → 명시 안내 + return True.

        이전 동작 (fallthrough=False)은 사용자 5건 운영 실패 RCA에서 task-129 4단계
        파이프라인을 grep fallback으로 우회하던 회귀 근본 원인. 시정으로 fallthrough 차단.
        """
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(ai_failed=True)
        relax_mod = MagicMock()
        af_mod = MagicMock()
        respond_mock = MagicMock()

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=respond_mock, cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True, "T-3 (task-132 시정): ai_failed 시 명시 안내 + True 반환"
        relax_mod.search_with_ladder.assert_not_called()
        # 명시 안내 메시지 발송 검증
        respond_mock.assert_called_once()
        call_kwargs = respond_mock.call_args.kwargs
        assert "Intent" in call_kwargs.get("text", "") or "의도" in call_kwargs.get("text", ""), \
            "T-3 FAIL: ai_failed 시 명시 안내 텍스트 누락"

    def test_T3b_cache_none_returns_false_for_fallthrough(self):
        """T-3b: cache_mgr=None → False 반환 (fallthrough)."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(ai_failed=False)
        relax_mod = MagicMock()
        af_mod = MagicMock()

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=None,
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is False, "T-3b FAIL: cache=None 시 fallthrough"
        # cache_mgr=None 시 ie_mod도 호출되지 않아야 함 (R-5 정합)
        ie_mod.extract_intent.assert_not_called()
        relax_mod.search_with_ladder.assert_not_called()

    def test_T4_metadata_formatter(self):
        """T-4: request_type=metadata → format_metadata_answer 호출."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="metadata", metadata_field="last_modified", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="t", metadata={"last_modified": "2026-04-25"})],
            total_count=1,
        )
        af_mod = MagicMock()
        af_mod.format_metadata_answer.return_value = "formatted_metadata"

        respond = MagicMock()
        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=respond, cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True, "T-4: 처리 완료 시 True 반환"
        af_mod.format_metadata_answer.assert_called_once()
        respond.assert_called()

    def test_T5_list_formatter(self):
        """T-5: request_type=list → format_list_answer 호출."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="list", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="page1"), _MockHit(title="page2")], total_count=2,
        )
        af_mod = MagicMock()
        af_mod.format_list_answer.return_value = "formatted_list"

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True
        af_mod.format_list_answer.assert_called_once()

    def test_T5b_summary_formatter(self):
        """T-5b (M-1 시정): request_type=summary → format_summary_answer 호출."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="summary", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="t", snippet="요약")], total_count=1,
        )
        af_mod = MagicMock()
        af_mod.format_summary_answer.return_value = "summary_text"

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True
        af_mod.format_summary_answer.assert_called_once()

    def test_T5c_content_search_uses_ask_claude(self):
        """T-5c: request_type=content_search → ask_claude_fn 호출 (format_*_answer 미호출)."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="content_search", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="t", snippet="content")], total_count=1,
        )
        af_mod = MagicMock()
        ask_claude_fn = MagicMock()

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=ask_claude_fn,
        )
        assert result is True
        ask_claude_fn.assert_called_once()
        af_mod.format_metadata_answer.assert_not_called()
        af_mod.format_list_answer.assert_not_called()

    def test_T6_search_with_ladder_signature(self):
        """T-6: relax.search_with_ladder(cache_mgr, intent, "wiki") 호출."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        intent = _make_wiki_intent(request_type="metadata", ai_failed=False)
        ie_mod.extract_intent.return_value = intent
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(total_count=0)
        af_mod = MagicMock()

        cache_mgr = MagicMock()
        pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=cache_mgr,
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )

        call_args = relax_mod.search_with_ladder.call_args
        args = list(call_args.args)
        assert args[0] is cache_mgr, f"T-6: cache_mgr 첫 인자 의무. args={args}"
        assert args[1] is intent
        assert args[2] == "wiki"

    def test_T14_zero_hits_no_fallthrough(self):
        """T-14 (M-4 시정): 4단계 진입 후 0건 → "찾을 수 없습니다" 응답, return True (fallthrough 금지)."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="metadata", ai_failed=False, ambiguity_notes="모호함",
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(total_count=0)
        af_mod = MagicMock()
        respond = MagicMock()

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=respond, cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True, "T-14 FAIL (M-4): 0건 시 True (fallthrough 금지)"
        respond.assert_called()
        call_args = respond.call_args
        msg = call_args.kwargs.get("text") or (call_args.args[0] if call_args.args else "")
        assert "찾을 수 없" in msg

    def test_T_pipeline_exception_returns_false_for_fallthrough(self):
        """예외 발생 시 → False 반환 (fallthrough)."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_wiki_intent(
            request_type="metadata", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.side_effect = RuntimeError("DB error")
        af_mod = MagicMock()

        result = pipeline(
            text="x \\ y", page_part="x", question="y",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is False, "예외 시 False 반환 (fallthrough)"


# ─────────────────────────────────────────────────────────────────────────────
# T-2, T-gdi: gdi pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestGdiPipeline:
    """task-129.5 run_gdi_intent_pipeline 검증."""

    def _import_pipeline(self):
        from intent_pipeline import run_gdi_intent_pipeline
        return run_gdi_intent_pipeline

    def test_T2_extract_intent_full_text(self):
        """T-2: gdi extract_intent에 partition 이전 full_text 전달."""
        pipeline = self._import_pipeline()
        captured = {}

        def mock_extract(text, domain):
            captured["text"] = text
            captured["domain"] = domain
            return _make_gdi_intent(
                request_type="metadata",
                folder_role=["test_result"],
                game_alias_kr=["카제나"],
                ai_failed=False,
            )

        ie_mod = MagicMock()
        ie_mod.extract_intent = mock_extract
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="t", metadata={"ref_date": "2026-04-25"})],
            total_count=1,
        )
        af_mod = MagicMock()
        af_mod.format_metadata_answer.return_value = "x"

        full_text = "카제나 Test Result \\ 최근 결과 ref_date 알려줘"
        result = pipeline(
            text=full_text, folder="카제나 Test Result", question="최근 결과 ref_date 알려줘",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert captured.get("text") == full_text
        assert captured.get("domain") == "gdi"

    def test_T_gdi_metadata_formatter(self):
        """gdi metadata path → format_metadata_answer 호출."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        ie_mod.extract_intent.return_value = _make_gdi_intent(
            request_type="metadata", metadata_field="ref_date", ai_failed=False,
        )
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(
            hits=[_MockHit(title="t", metadata={"ref_date": "2026-04-25"})], total_count=1,
        )
        af_mod = MagicMock()
        af_mod.format_metadata_answer.return_value = "metadata"

        result = pipeline(
            text="x \\ y", folder="x", question="y",
            respond=MagicMock(), cache_mgr=MagicMock(),
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        assert result is True
        af_mod.format_metadata_answer.assert_called_once()

    def test_T_gdi_search_signature(self):
        """gdi search_with_ladder(cache_mgr, intent, "gdi") 인자 정합."""
        pipeline = self._import_pipeline()
        ie_mod = MagicMock()
        intent = _make_gdi_intent(request_type="metadata", ai_failed=False)
        ie_mod.extract_intent.return_value = intent
        relax_mod = MagicMock()
        relax_mod.search_with_ladder.return_value = _MockSearchResult(total_count=0)
        af_mod = MagicMock()

        cache_mgr = MagicMock()
        pipeline(
            text="x \\ y", folder="x", question="y",
            respond=MagicMock(), cache_mgr=cache_mgr,
            ie_mod=ie_mod, relax_mod=relax_mod, af_mod=af_mod,
            ask_claude_fn=MagicMock(),
        )
        args = list(relax_mod.search_with_ladder.call_args.args)
        assert args[0] is cache_mgr
        assert args[1] is intent
        assert args[2] == "gdi"


# ─────────────────────────────────────────────────────────────────────────────
# T-8, T-9: 회귀 / READ-ONLY 검증
# ─────────────────────────────────────────────────────────────────────────────

class TestRegressionGuard:
    """T-8, T-9: task-128 jira + task-129 4 모듈 READ-ONLY 검증."""

    def test_T8_jira_handler_function_intact(self):
        """T-8: jira_client.py 핵심 함수 시그니처 유지."""
        import jira_client as jc
        assert hasattr(jc, "extract_intent"), "T-8: jc.extract_intent 누락 — task-128 회귀"
        assert hasattr(jc, "search_with_ladder"), "T-8: jc.search_with_ladder 누락"
        assert callable(jc.extract_intent)
        assert callable(jc.search_with_ladder)

    def test_T9_intent_extractor_signature_unchanged(self):
        """T-9: intent_extractor.extract_intent(text, domain) 시그니처 유지."""
        import intent_extractor as ie
        import inspect
        sig = inspect.signature(ie.extract_intent)
        params = list(sig.parameters.keys())
        assert "text" in params, f"T-9: extract_intent 'text' 누락. params={params}"
        assert "domain" in params, f"T-9: 'domain' 누락. params={params}"

    def test_T9_relaxation_engine_signature_unchanged(self):
        """T-9: relaxation_engine.search_with_ladder(cache_mgr, intent, domain) 시그니처 유지."""
        import relaxation_engine as re_mod
        import inspect
        sig = inspect.signature(re_mod.search_with_ladder)
        params = list(sig.parameters.keys())
        assert params[:3] == ["cache_mgr", "intent", "domain"], (
            f"T-9: search_with_ladder 시그니처 변경. params={params}"
        )

    def test_T9_answer_formatter_three_funcs(self):
        """T-9: answer_formatter 3 함수 존재."""
        import answer_formatter as af
        assert callable(getattr(af, "format_metadata_answer", None))
        assert callable(getattr(af, "format_list_answer", None))
        assert callable(getattr(af, "format_summary_answer", None))

    def test_intent_pipeline_module_exports(self):
        """intent_pipeline.py 신규 모듈 — 핵심 함수 export."""
        import intent_pipeline as ip
        assert callable(getattr(ip, "run_wiki_intent_pipeline", None))
        assert callable(getattr(ip, "run_gdi_intent_pipeline", None))
        assert callable(getattr(ip, "hits_to_wiki_context", None))
        assert callable(getattr(ip, "hits_to_gdi_context", None))


# ─────────────────────────────────────────────────────────────────────────────
# T-helper: context 함수 검증
# ─────────────────────────────────────────────────────────────────────────────

class TestContextHelpers:
    """hits_to_wiki_context / hits_to_gdi_context 검증."""

    def test_hits_to_wiki_context_basic(self):
        from intent_pipeline import hits_to_wiki_context
        hits = [_MockHit(title="t1", snippet="s1"), _MockHit(title="t2", snippet="s2")]
        result = hits_to_wiki_context(hits)
        assert "t1" in result and "t2" in result
        assert "s1" in result and "s2" in result

    def test_hits_to_wiki_context_max_chars(self):
        from intent_pipeline import hits_to_wiki_context
        hits = [_MockHit(title=f"t{i}", snippet="x" * 500) for i in range(20)]
        result = hits_to_wiki_context(hits, max_chars=1000)
        assert len(result) <= 2000  # buffer 허용

    def test_hits_to_wiki_context_empty(self):
        from intent_pipeline import hits_to_wiki_context
        assert hits_to_wiki_context([]) == ""

    def test_hits_to_gdi_context_metadata_inline(self):
        from intent_pipeline import hits_to_gdi_context
        hits = [_MockHit(
            title="t1", snippet="s1",
            metadata={"path": "/p", "game_alias_kr": "카제나", "ref_date": "2026-04-25"},
        )]
        result = hits_to_gdi_context(hits)
        assert "path=/p" in result
        assert "game=카제나" in result
        assert "ref_date=2026-04-25" in result
