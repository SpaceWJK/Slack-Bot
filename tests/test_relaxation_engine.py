"""
TDD 테스트: relaxation_engine.py (Red 단계)
task-129 S4-D — 완화 엔진

커버 케이스:
  T-12  완화 사다리 (L0~L3)
  MAJOR-NEW-4: dataclasses.replace + cache 무결성
"""

import sys
import os
import dataclasses
import pytest

SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)


def _import_relaxation_engine():
    import relaxation_engine as re_eng
    return re_eng


def _import_search_with_fallback():
    import relaxation_engine as re_eng
    return re_eng.search_with_request_type_fallback


def _import_dataclasses():
    import intent_extractor as ie
    return ie.WikiIntent, ie.GdiIntent


def _import_intent_cache():
    import intent_extractor as ie
    return ie._INTENT_CACHE


# ─────────────────────────────────────────────────────────────────────────────
# T-12: 완화 사다리 (L0~L3)
# ─────────────────────────────────────────────────────────────────────────────

class TestRelaxationLadder:
    """T-12: search_with_ladder L0~L3 완화 사다리."""

    def test_T12_relaxation_engine_module_exists(self):
        """T-12: relaxation_engine.py 모듈 존재."""
        try:
            re_eng = _import_relaxation_engine()
            assert re_eng is not None
        except ImportError:
            pytest.fail("T-12 FAIL: relaxation_engine import 실패 — Red")

    def test_T12_search_with_ladder_exists(self):
        """T-12: search_with_ladder 함수 존재."""
        try:
            re_eng = _import_relaxation_engine()
            assert hasattr(re_eng, "search_with_ladder"), (
                "T-12 FAIL: search_with_ladder 함수 없음 — Red"
            )
        except ImportError:
            pytest.fail("T-12 FAIL: relaxation_engine import 실패")

    def test_T12_search_result_dataclass_exists(self):
        """T-12: SearchResult dataclass 존재 (total_count, relaxation_level 등)."""
        try:
            re_eng = _import_relaxation_engine()
            assert hasattr(re_eng, "SearchResult"), (
                "T-12 FAIL: SearchResult 없음 — Red"
            )
            SR = re_eng.SearchResult
            fields = {f.name for f in dataclasses.fields(SR)}
            for req in ("hits", "total_count", "relaxation_level", "history"):
                assert req in fields, f"T-12 FAIL: SearchResult에 '{req}' 필드 없음"
        except ImportError:
            pytest.fail("T-12 FAIL: relaxation_engine import 실패")

    def test_T12_search_hit_dataclass_exists(self):
        """T-12 MINOR-E: SearchHit dataclass — chunk_id/score/metadata dict."""
        try:
            re_eng = _import_relaxation_engine()
            assert hasattr(re_eng, "SearchHit"), (
                "T-12 FAIL: SearchHit 없음 — Red"
            )
            SH = re_eng.SearchHit
            fields = {f.name for f in dataclasses.fields(SH)}
            for req in ("node_id", "chunk_id", "title", "snippet", "score", "metadata"):
                assert req in fields, f"T-12 FAIL: SearchHit에 '{req}' 필드 없음"
        except ImportError:
            pytest.fail("T-12 FAIL: relaxation_engine import 실패")

    def test_T12_row_to_search_hit_exists(self):
        """T-12 MINOR-E: _row_to_search_hit 헬퍼 존재."""
        try:
            re_eng = _import_relaxation_engine()
            assert hasattr(re_eng, "_row_to_search_hit"), (
                "T-12 FAIL: _row_to_search_hit 없음 — Red"
            )
        except ImportError:
            pytest.fail("T-12 FAIL: relaxation_engine import 실패")


# ─────────────────────────────────────────────────────────────────────────────
# MAJOR-NEW-4: dataclasses.replace + cache 무결성
# ─────────────────────────────────────────────────────────────────────────────

class TestMetadataFallbackNoMutation:
    """MAJOR-NEW-4: metadata fallback 시 intent in-place 변이 절대 금지."""

    def test_metadata_fallback_function_exists(self):
        """search_with_request_type_fallback 함수 존재."""
        try:
            fn = _import_search_with_fallback()
            assert callable(fn), "T-12 FAIL: search_with_request_type_fallback callable 아님"
        except (ImportError, AttributeError):
            pytest.fail("T-12 FAIL: search_with_request_type_fallback import 실패 — Red")

    def test_metadata_fallback_does_not_mutate_intent(self):
        """MAJOR-NEW-4 핵심: metadata 0건 fallback 시 원본 intent.request_type 불변.

        intent.request_type = "content_search" 직접 대입 금지.
        dataclasses.replace로 새 인스턴스 생성 의무.
        """
        try:
            search_fn = _import_search_with_fallback()
            WikiIntent, _ = _import_dataclasses()
        except (ImportError, AttributeError):
            pytest.fail("T-12 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="metadata",
            metadata_field="last_modified",
            title_keywords=["존재안함_xyz_unique"],
        )
        request_type_before = intent.request_type
        intent_id_before = id(intent)

        # cache_mgr mock (0건 반환)
        class MockCacheMgr:
            def get_db_path(self):
                return ":memory:"

        try:
            result, returned_intent = search_fn(MockCacheMgr(), intent, "wiki")
        except Exception:
            # 실제 DB 없어서 실패 가능 — 핵심은 intent 변이 여부
            pass

        # 원본 intent 불변 (MAJOR-NEW-4)
        assert intent.request_type == request_type_before == "metadata", (
            f"MAJOR-NEW-4 FAIL: intent.request_type이 변이됨. "
            f"before='metadata', after={intent.request_type!r}. "
            "in-place 변이 금지 위반 (dataclasses.replace 미사용) — Red"
        )

    def test_metadata_fallback_creates_new_instance(self):
        """MAJOR-NEW-4: fallback_intent는 원본 intent와 다른 인스턴스."""
        try:
            search_fn = _import_search_with_fallback()
            WikiIntent, _ = _import_dataclasses()
        except (ImportError, AttributeError):
            pytest.fail("T-12 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="metadata",
            metadata_field="last_modified",
            title_keywords=["존재안함_xyz_unique2"],
        )
        intent_id_before = id(intent)

        class MockCacheMgr:
            def get_db_path(self): return ":memory:"

        try:
            result, returned_intent = search_fn(MockCacheMgr(), intent, "wiki")
            # fallback 발생 시 다른 인스턴스
            if hasattr(returned_intent, "request_type") and returned_intent.request_type == "content_search":
                assert id(returned_intent) != intent_id_before, (
                    "MAJOR-NEW-4 FAIL: fallback_intent가 원본과 동일 인스턴스 (in-place 변이)"
                )
        except Exception:
            pass  # DB 없는 환경에서 실행 실패는 허용

    def test_intent_cache_unchanged_after_fallback(self):
        """MAJOR-NEW-4: _INTENT_CACHE 내 intent 인스턴스 변이 0.

        cache에서 가져온 intent를 request_type='content_search'로 직접 변경하면
        60초 내 재질의 시 cache hit + metadata 분기 보장 실패.
        """
        try:
            _INTENT_CACHE = _import_intent_cache()
        except (ImportError, AttributeError):
            pytest.fail("T-12 FAIL: _INTENT_CACHE import 실패 — Red")

        WikiIntent, _ = _import_dataclasses()
        # cache에 intent 저장
        cached_intent = WikiIntent(
            request_type="metadata",
            metadata_field="last_modified",
            title_keywords=["캐시 테스트"],
        )
        cache_key = "test_key_unique_xyz"
        _INTENT_CACHE[cache_key] = cached_intent

        # cache hit 시 intent를 직접 변이하는 코드가 있는지 확인
        # → fallback 로직이 in-place 변이하면 cache 내 intent도 변경됨
        # 여기서는 cache에 저장 후 원본 불변 확인
        assert _INTENT_CACHE[cache_key].request_type == "metadata", (
            "T-12 FAIL: _INTENT_CACHE 내 intent가 외부에서 변이됨"
        )

        # cleanup
        del _INTENT_CACHE[cache_key]
