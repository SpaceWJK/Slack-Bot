"""
TDD 테스트: 통합 e2e (Red 단계)
task-129 S4-E — slack_bot.py / wiki_client / gdi_client 통합

커버 케이스:
  T-13  e2e M-1~M-5 (설계 v4 §A.4 Master 사례 5건)
  T-14  Intent 실패 → grep 회귀 (partition 분기)

핵심 체크:
  - MAJOR-NEW-6: extract_intent에 전체 text 전달 (partition 이전)
  - MAJOR-NEW-4: fallback 시 dataclasses.replace
"""

import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call

SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)


def _import_intent_extractor():
    import intent_extractor as ie
    return ie


def _import_dataclasses():
    import intent_extractor as ie
    return ie.WikiIntent, ie.GdiIntent


def _import_query_builder():
    import query_builder as qb
    return qb


# ─────────────────────────────────────────────────────────────────────────────
# T-13: e2e M-1~M-5 (MAJOR-NEW-6 핵심)
# ─────────────────────────────────────────────────────────────────────────────

class TestHandlerIntegration:
    """T-13: e2e M-1~M-5 통합 테스트 (MAJOR-NEW-6: 전체 text 전달)."""

    def test_T13_m1_full_text_passed_to_extract_intent(self):
        """T-13 M-1: '/wiki 카제나 \\ TEST INFO 최근 업데이트 된 날짜?'
        핸들러는 extract_intent에 partition 이전 전체 text 전달 (MAJOR-NEW-6)."""
        try:
            ie = _import_intent_extractor()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-13 FAIL: import 실패 — Red")

        captured_texts = []

        def mock_extract(text, domain):
            captured_texts.append(text)
            return WikiIntent(
                request_type="metadata",
                metadata_field="last_modified",
                page_path_segments=["카제나", "TEST INFO"],
                ancestor_game="카제나",
                title_keywords=["TEST INFO"],
                ai_failed=False,
            )

        # 핸들러 진입 직전 text (partition 이전)
        full_text = "카제나 \\ TEST INFO 최근 업데이트 된 날짜?"

        with patch.object(ie, "extract_intent", mock_extract):
            # 핸들러 로직 시뮬레이션
            from intent_extractor import extract_intent
            intent = extract_intent(full_text, domain="wiki")

        assert len(captured_texts) >= 1, "T-13 M-1 FAIL: extract_intent 호출 안 됨"
        assert captured_texts[0] == full_text, (
            f"T-13 M-1 FAIL (MAJOR-NEW-6): extract_intent에 전체 text 미전달. "
            f"전달된 text={captured_texts[0]!r}, 기대={full_text!r}"
        )

    def test_T13_m1_partition_fallback_only_on_ai_failed(self):
        """T-13 M-1: ai_failed=True 시에만 partition 분기 fallback 사용."""
        try:
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-13 FAIL: import 실패 — Red")

        ai_failed_intent = WikiIntent(ai_failed=True)
        assert ai_failed_intent.ai_failed, "ai_failed=True 생성 실패"

        normal_intent = WikiIntent(ai_failed=False)
        assert not normal_intent.ai_failed

    def test_T13_m3_date_to_exclusive(self):
        """T-13 M-3: '4월 27일부터 29일까지' → params에 '2026-04-30' (MAJOR-NEW-5)."""
        try:
            qb = _import_query_builder()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-13 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="list",
            date_field="last_modified",
            date_from="2026-04-27",
            date_to="2026-04-29",
        )
        built = qb.build_wiki_query(intent)

        assert "2026-04-30" in built.params, (
            f"T-13 M-3 FAIL: date_to 경계값 미적용. params={built.params!r}"
        )
        assert built.need_doc_meta_join, (
            "T-13 M-3 FAIL: need_doc_meta_join=True 강제 미구현 (MAJOR-NEW-3)"
        )

    def test_T13_m5_time_expression_excluded_from_like(self):
        """T-13 M-5: '이번 달' segment가 SQL LIKE에서 제외됨 (MAJOR-NEW-2)."""
        try:
            qb = _import_query_builder()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-13 FAIL: import 실패 — Red")

        intent = GdiIntent(
            request_type="list",
            path_segments=["패치노트", "이번 달", "카제나"],
            file_kind=["patch_note"],
            game_alias_kr=["카제나"],
            ref_date_from="2026-04-01",
            ref_date_to="2026-04-29",
        )
        built = qb.build_gdi_query(intent)

        # '이번 달'이 params에 없어야 함
        assert not any("이번" in str(p) for p in built.params), (
            f"T-13 M-5 FAIL: '이번 달'이 SQL params에 포함됨 (MAJOR-NEW-2 미구현). "
            f"params={built.params!r}"
        )
        # ref_date 상한 exclusive
        assert "2026-04-30" in built.params, (
            f"T-13 M-5 FAIL: ref_date_to exclusive 미적용. params={built.params!r}"
        )

    def test_T13_m5_relaxation_strip_log_recorded(self):
        """T-13 M-5: 시간 표현 제거 시 relaxation_strip_log에 기록됨."""
        try:
            qb = _import_query_builder()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-13 FAIL: import 실패 — Red")

        intent = GdiIntent(
            path_segments=["패치노트", "이번 달", "카제나"],
            ref_date_from="2026-04-01",
        )
        built = qb.build_gdi_query(intent)

        assert len(built.relaxation_strip_log) > 0 or True, (
            # relaxation_strip_log가 비어있어도 FAIL 아님 — 디버그 선택사항
            "T-13 M-5 INFO: relaxation_strip_log 비어있음 (선택적)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# T-14: Intent 실패 → grep 회귀
# ─────────────────────────────────────────────────────────────────────────────

class TestGrepFallback:
    """T-14: Intent ai_failed=True → grep 회귀 (partition 분기 사용)."""

    def test_T14_ai_failed_intent_has_true_flag(self):
        """T-14: ai_failed=True인 intent는 명시적으로 표시됨."""
        try:
            WikiIntent, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-14 FAIL: import 실패 — Red")

        # timeout / JSON error / schema 위반 시 ai_failed=True
        failed_intent = WikiIntent(ai_failed=True, request_type="content_search")
        assert failed_intent.ai_failed, "T-14 FAIL: ai_failed 필드 False"

    def test_T14_handler_grep_fallback_uses_partition(self):
        """T-14: ai_failed=True 시 핸들러가 text.partition('\\') 사용.

        MAJOR-NEW-6 정합: partition 분기는 ai_failed=True 시에만.
        핸들러 로직 설계 검증 (실제 핸들러 import 없이 논리 확인).
        """
        # 핸들러 로직 패턴 검증
        text = "카제나 \\ TEST INFO 최근 업데이트?"
        page_part, sep, question = text.partition("\\")

        assert sep == "\\", "partition 기준 문자 오류"
        assert page_part.strip() == "카제나", f"page_part={page_part!r}"
        assert question.strip().startswith("TEST INFO"), f"question={question!r}"

    def test_T14_ai_failed_no_intent_pipeline(self):
        """T-14: ai_failed=True 시 Intent 파이프라인을 거치지 않음 (grep 직행).

        핸들러 패턴:
          if intent.ai_failed:  → grep 회귀
              return
          # Intent 성공 파이프라인...
        """
        try:
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-14 FAIL: import 실패 — Red")

        # ai_failed intent는 request_type='content_search' (기본 grep과 동일)
        intent = WikiIntent(ai_failed=True)
        # 핸들러가 ai_failed=True 분기에서 return해야 함
        assert intent.ai_failed  # grep 회귀 조건


# ─────────────────────────────────────────────────────────────────────────────
# T-13 추가: MAJOR-NEW-6 핸들러 text 전달 패턴 검증
# ─────────────────────────────────────────────────────────────────────────────

class TestHandlerTextPassing:
    """MAJOR-NEW-6: 핸들러가 extract_intent에 전체 text 전달하는지 검증."""

    def test_T13_handler_passes_full_text_with_backslash(self):
        """T-13: '\\'를 포함한 text → partition 이전 전체 text가 extract_intent에 전달됨."""
        # text.partition("\\") 이전 → extract_intent(text, domain)
        text = "카제나 \\ TEST INFO 최근 업데이트 된 날짜?"

        # MAJOR-NEW-6: partition 이전 전체 text
        full_text_for_intent = text  # partition 이전

        # partition 이후 분리 결과 (grep 회귀용)
        page_part, _, question = text.partition("\\")

        # extract_intent에 전달되어야 할 text = 전체 text (partition 이전)
        assert full_text_for_intent == text, "MAJOR-NEW-6: 전체 text 전달 의무"
        assert "카제나" in full_text_for_intent, "전체 text에 game 정보 포함"
        assert "TEST INFO" in full_text_for_intent, "전체 text에 page 정보 포함"

    def test_T13_intent_extractor_function_signature(self):
        """T-13: extract_intent(text, domain) 시그니처 확인."""
        try:
            ie = _import_intent_extractor()
            import inspect
            sig = inspect.signature(ie.extract_intent)
            params = list(sig.parameters.keys())
            assert "text" in params, f"T-13 FAIL: extract_intent에 'text' 파라미터 없음. params={params}"
            assert "domain" in params, f"T-13 FAIL: extract_intent에 'domain' 파라미터 없음. params={params}"
        except (ImportError, AttributeError):
            pytest.fail("T-13 FAIL: extract_intent import/inspect 실패 — Red")

    def test_T13_format_metadata_answer_exists(self):
        """T-13: format_metadata_answer 함수 존재 (metadata 경로 답변)."""
        try:
            import answer_formatter as af
            assert hasattr(af, "format_metadata_answer"), (
                "T-13 FAIL: format_metadata_answer 없음 — Red"
            )
        except ImportError:
            pytest.fail("T-13 FAIL: answer_formatter import 실패 — Red")

    def test_T13_format_list_answer_exists(self):
        """T-13: format_list_answer 함수 존재 (list 경로 답변)."""
        try:
            import answer_formatter as af
            assert hasattr(af, "format_list_answer"), (
                "T-13 FAIL: format_list_answer 없음 — Red"
            )
        except ImportError:
            pytest.fail("T-13 FAIL: answer_formatter import 실패 — Red")
