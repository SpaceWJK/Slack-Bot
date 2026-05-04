"""
TDD 테스트: intent_extractor.py (Red 단계)
task-129 S4-B — Intent extractor 엔진

커버 케이스:
  T-6  wiki schema 정합 (field 존재 + 타입 검증)
  T-7  gdi schema 정합 + rule 8/9 v4 표현 (시간 표현 carryover 명시)
  T-8  strict validator — unknown field reject
  T-9  hallucination — enum 위반 reject (ai_failed=True)
"""

import sys
import os
import json
import pytest

# Slack Bot 루트를 sys.path에 추가
SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)


# ─────────────────────────────────────────────────────────────────────────────
# import helpers
# ─────────────────────────────────────────────────────────────────────────────

def _import_intent_extractor():
    """intent_extractor 모듈 import. 미존재 시 ImportError (Red)."""
    import intent_extractor as ie  # noqa — Red: 모듈 미존재
    return ie


def _import_dataclasses():
    """WikiIntent / GdiIntent dataclass import."""
    import intent_extractor as ie
    return ie.WikiIntent, ie.GdiIntent


def _import_validator():
    """_validate_against_schema import."""
    import intent_extractor as ie
    return ie._validate_against_schema


def _import_extract_intent():
    """extract_intent import."""
    import intent_extractor as ie
    return ie.extract_intent


# ─────────────────────────────────────────────────────────────────────────────
# T-6: wiki schema 정합
# ─────────────────────────────────────────────────────────────────────────────

class TestWikiSchema:
    """T-6: wiki.json schema 필드 존재 + WikiIntent dataclass 검증."""

    def test_wiki_schema_file_exists(self):
        """T-6: intent_schemas/wiki.json 파일 존재."""
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "wiki.json")
        assert os.path.exists(schema_path), (
            f"T-6 FAIL: wiki.json 없음. 경로={schema_path}. S4-A 미완료 — Red"
        )

    def test_wiki_schema_required_fields(self):
        """T-6: wiki.json에 필수 필드 정의 (request_type, metadata_field, page_path_segments, title_keywords)."""
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "wiki.json")
        if not os.path.exists(schema_path):
            pytest.skip("wiki.json 없음 — S4-A 선행 필요")
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        fields = schema.get("fields", {})
        required = {"request_type", "metadata_field", "page_path_segments", "title_keywords",
                    "ancestor_game", "date_field", "date_from", "date_to", "body_keywords"}
        for field in required:
            assert field in fields, f"T-6 FAIL: wiki.json에 '{field}' 필드 없음"

    def test_wiki_intent_dataclass_exists(self):
        """T-6: WikiIntent dataclass import 가능."""
        try:
            WikiIntent, _ = _import_dataclasses()
            assert WikiIntent is not None
        except ImportError as e:
            pytest.fail(f"T-6 FAIL: WikiIntent import 실패 — {e}. intent_extractor.py 미존재 — Red")

    def test_wiki_intent_request_type_enum(self):
        """T-6: WikiIntent.request_type 기본값 'content_search'."""
        try:
            WikiIntent, _ = _import_dataclasses()
            intent = WikiIntent()
            assert intent.request_type == "content_search", (
                f"T-6 FAIL: 기본 request_type 기대 'content_search', 실제 {intent.request_type!r}"
            )
        except ImportError:
            pytest.fail("T-6 FAIL: WikiIntent import 실패 — Red")

    def test_wiki_intent_ai_failed_default_false(self):
        """T-6: WikiIntent.ai_failed 기본값 False."""
        try:
            WikiIntent, _ = _import_dataclasses()
            intent = WikiIntent()
            assert intent.ai_failed == False, (
                f"T-6 FAIL: ai_failed 기본값 기대 False, 실제 {intent.ai_failed!r}"
            )
        except ImportError:
            pytest.fail("T-6 FAIL: WikiIntent import 실패")


# ─────────────────────────────────────────────────────────────────────────────
# T-7: gdi schema 정합 + rule 8/9 v4
# ─────────────────────────────────────────────────────────────────────────────

class TestGdiSchema:
    """T-7: gdi.json schema 검증 + MAJOR-NEW-2 시간 표현 carryover 명시."""

    def test_gdi_schema_file_exists(self):
        """T-7: intent_schemas/gdi.json 파일 존재."""
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "gdi.json")
        assert os.path.exists(schema_path), (
            f"T-7 FAIL: gdi.json 없음. 경로={schema_path}. S4-A 미완료 — Red"
        )

    def test_gdi_schema_required_fields(self):
        """T-7: gdi.json에 필수 필드 정의."""
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "gdi.json")
        if not os.path.exists(schema_path):
            pytest.skip("gdi.json 없음")
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        fields = schema.get("fields", {})
        required = {"request_type", "path_segments", "game_alias_kr", "folder_role",
                    "file_kind", "ref_date_from", "ref_date_to", "body_keywords"}
        for field in required:
            assert field in fields, f"T-7 FAIL: gdi.json에 '{field}' 필드 없음"

    def test_gdi_schema_rule8_9_time_expression_carryover(self):
        """T-7: gdi.json rule 8/9 (v4 MAJOR-NEW-2) — 시간 표현 path_segments carryover 명시.

        Rule 8: path_segments에 시간 표현이 있어도 Claude는 그대로 채움
        Rule 9: Claude는 동시에 ref_date_from/to 변환
        Query builder에서 자동 제거 (_strip_time_expressions)
        """
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "gdi.json")
        if not os.path.exists(schema_path):
            pytest.skip("gdi.json 없음")
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        rules = schema.get("extraction_rules", [])
        # rule 8/9 관련 내용 확인
        rules_text = " ".join(str(r) for r in rules)
        # Query builder에서 시간 표현 제거 언급
        assert "Query builder" in rules_text or "자동 제거" in rules_text or "MAJOR-NEW-2" in rules_text, (
            f"T-7 FAIL: gdi.json에 MAJOR-NEW-2 시간 표현 처리 rule 미명시. rules={rules[:3]}"
        )

    def test_gdi_schema_schema_version_4(self):
        """T-7: gdi.json schema_version이 4 (v4)."""
        schema_path = os.path.join(SLACK_BOT_ROOT, "intent_schemas", "gdi.json")
        if not os.path.exists(schema_path):
            pytest.skip("gdi.json 없음")
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        assert schema.get("schema_version") == 4, (
            f"T-7 FAIL: schema_version 기대 4, 실제 {schema.get('schema_version')}"
        )

    def test_gdi_intent_dataclass_exists(self):
        """T-7: GdiIntent dataclass import 가능."""
        try:
            _, GdiIntent = _import_dataclasses()
            assert GdiIntent is not None
        except ImportError as e:
            pytest.fail(f"T-7 FAIL: GdiIntent import 실패 — {e}. intent_extractor.py 미존재 — Red")

    def test_gdi_intent_path_segments_default_empty(self):
        """T-7: GdiIntent.path_segments 기본값 []."""
        try:
            _, GdiIntent = _import_dataclasses()
            intent = GdiIntent()
            assert intent.path_segments == [], (
                f"T-7 FAIL: path_segments 기본값 기대 [], 실제 {intent.path_segments!r}"
            )
        except ImportError:
            pytest.fail("T-7 FAIL: GdiIntent import 실패")


# ─────────────────────────────────────────────────────────────────────────────
# T-8: strict validator — unknown field reject
# ─────────────────────────────────────────────────────────────────────────────

class TestStrictValidator:
    """T-8 (task-132 PR1-D 시정): _validate_against_schema — unknown field tolerant.

    이전 strict reject 동작은 Claude haiku의 `schema_version`/`domain`/`fields` 등
    메타 필드 자동 추가를 ai_failed로 처리해 사용자 5건 운영 0% 직접 원인이 됨.
    시정: unknown field를 drop 후 정상 진행 (강한 거부 → 관대한 무시).
    """

    def test_T8_unknown_field_drop_and_continue(self):
        """T-8 (시정): unknown field가 있어도 drop 후 정상 intent 반환."""
        try:
            _validate = _import_validator()
        except (ImportError, AttributeError):
            pytest.fail("T-8 FAIL: _validate_against_schema import 실패")

        raw_json = {
            "request_type": "content_search",
            "schema_version": "1.0",  # Claude가 자주 추가하는 메타 필드
            "domain": "wiki",
            "unknown_field_xyz": "should be dropped, not rejected",
        }
        result = _validate(raw_json, domain="wiki")
        assert result is not None, "T-8 (시정) FAIL: unknown field가 drop되지 않고 reject됨"
        assert not getattr(result, "ai_failed", False), \
            "T-8 (시정) FAIL: unknown field로 ai_failed 처리됨"

    def test_T8_valid_intent_passes_validation(self):
        """T-8 회귀: 정상 intent는 통과."""
        try:
            _validate = _import_validator()
        except (ImportError, AttributeError):
            pytest.skip("_validate_against_schema 없음")

        valid_json = {
            "request_type": "content_search",
            "title_keywords": ["TEST INFO"],
            "ai_failed": False,
        }
        result = _validate(valid_json, domain="wiki")
        assert result is not None, "T-8 회귀 FAIL: 정상 intent가 reject됨"

    def test_T8_unknown_field_drop_gdi(self):
        """T-8 gdi 도메인에서도 unknown field drop."""
        try:
            _validate = _import_validator()
        except (ImportError, AttributeError):
            pytest.fail("T-8 FAIL: _validate_against_schema import 실패")

        raw_json = {
            "request_type": "list",
            "hallucinated_field": 42,
            "schema_version": "1.0",
        }
        result = _validate(raw_json, domain="gdi")
        assert result is not None, "T-8 (시정) FAIL: gdi unknown field로 reject됨"
        assert not getattr(result, "ai_failed", False)


# ─────────────────────────────────────────────────────────────────────────────
# T-9: hallucination reject (enum 위반 → ai_failed=True)
# ─────────────────────────────────────────────────────────────────────────────

class TestHallucinationReject:
    """T-9: enum 위반 request_type → ai_failed=True."""

    def test_T9_invalid_request_type_rejected(self):
        """T-9: request_type에 존재하지 않는 enum 값 → 거부."""
        try:
            _validate = _import_validator()
        except (ImportError, AttributeError):
            pytest.fail("T-9 FAIL: _validate_against_schema import 실패 — Red")

        raw_json = {
            "request_type": "hallucinated_type",  # enum 위반
            "title_keywords": [],
        }
        result = _validate(raw_json, domain="wiki")
        assert result is None or (hasattr(result, "ai_failed") and result.ai_failed), (
            f"T-9 FAIL: enum 위반 request_type이 통과됨. result={result!r}"
        )

    def test_T9_valid_enum_passes(self):
        """T-9 회귀: 올바른 enum 값 통과."""
        try:
            _validate = _import_validator()
        except (ImportError, AttributeError):
            pytest.skip("_validate_against_schema 없음")

        for rt in ["content_search", "metadata", "list", "summary"]:
            raw_json = {"request_type": rt}
            result = _validate(raw_json, domain="wiki")
            assert result is not None and not (hasattr(result, "ai_failed") and result.ai_failed), (
                f"T-9 회귀 FAIL: 정상 request_type={rt!r}가 reject됨"
            )

    def test_T9_intent_cache_module_exists(self):
        """T-9: _INTENT_CACHE (60초 cache) 모듈 변수 존재."""
        try:
            ie = _import_intent_extractor()
            assert hasattr(ie, "_INTENT_CACHE"), (
                "T-9 FAIL: _INTENT_CACHE 없음. 60초 cache 미구현 — Red"
            )
        except ImportError:
            pytest.fail("T-9 FAIL: intent_extractor import 실패 — Red")

    def test_T9_extract_intent_function_exists(self):
        """T-9: extract_intent(text, domain) 함수 존재."""
        try:
            extract = _import_extract_intent()
            assert callable(extract), "T-9 FAIL: extract_intent callable 아님"
        except (ImportError, AttributeError):
            pytest.fail("T-9 FAIL: extract_intent import 실패 — Red")
