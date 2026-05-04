"""
TDD 테스트: query_builder.py (Red 단계)
task-129 S4-C — 3-layer Query builder + 8개 SQL 함수

커버 케이스:
  T-10  Intent → SQL 변환 정합 (v4 12건)
  T-11  weight 정렬 정합 (skip_weight 분기)
  T-15  LIKE ESCAPE + raw string (MAJOR-NEW-1 실측값)

MAJOR-NEW-1 T-15 raw string 실측 (Python REPL에서 실측 완료, 2026-04-29):
  _escape_like("C:\\\\Users") → lower → 'c:\\\\users' → pct → '%c:\\\\users%'
  raw string: r"%c:\\users%"  (2 backslashes in raw = 2 actual backslashes in param)

  실측값 대조표:
  입력 repr         | 실제 값          | param repr          | param 실제           | raw string
  "C:\\\\Users"     | C:\\Users (1BS)  | 'c\\\\\\\\users'    | c\\\\users (2BS)     | r"%c:\\users%"
  "file_v1.txt"     | file_v1.txt      | 'file\\\\_v1.txt'  | file\\_v1.txt        | r"%file\_v1.txt%"
  "50%done"         | 50%done          | '50\\\\%done'       | 50\\%done            | r"%50\\%done%"
  "path\\\\\\\\with"| path\\\\with(2BS)| 'path\\\\\\\\\\\\\\\\with' | path\\\\\\\\with(4BS)| r"%path\\\\with%"
  "normal"          | normal           | 'normal'            | normal               | r"%normal%"
"""

import sys
import os
import pytest
from unittest.mock import MagicMock, patch
import dataclasses

SLACK_BOT_ROOT = os.path.join(os.path.dirname(__file__), "..", "Slack Bot")
if SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, SLACK_BOT_ROOT)


def _import_query_builder():
    import query_builder as qb
    return qb


def _import_helpers():
    import query_builder as qb
    return (
        qb._escape_like,
        qb._strip_time_expressions,
        qb._next_day,
        qb._resolve_order_by_wiki_list,
        qb._apply_weight_and_sort,
    )


def _import_build_functions():
    import query_builder as qb
    return qb.build_wiki_query, qb.build_gdi_query


def _import_dataclasses():
    import intent_extractor as ie
    return ie.WikiIntent, ie.GdiIntent


# ─────────────────────────────────────────────────────────────────────────────
# T-15: LIKE ESCAPE + raw string (MAJOR-NEW-1)
# ─────────────────────────────────────────────────────────────────────────────

class TestEscapeLikeRawString:
    """T-15: _escape_like raw string 실측값 검증 (MAJOR-NEW-1)."""

    @pytest.mark.parametrize("path_in_segment, expected_param", [
        # (입력 segment, expected param raw string)
        # case1: C:\Users (1 backslash) → lower 'c:\users' → escape 'c:\\users'
        # raw string r"%c:\\users%" = '%c:\\users%' in memory (2 backslashes)
        ("C:\\Users",    r"%c:\\users%"),
        # case2: file_v1.txt → underscore escape
        ("file_v1.txt",  r"%file\_v1.txt%"),
        # case3: 50%done → percent escape
        ("50%done",      r"%50\%done%"),
        # case4: path\\with (2 backslashes) → 4 backslashes in output
        ("path\\\\with",   r"%path\\\\with%"),
        # case5: normal (no special chars)
        ("normal",       r"%normal%"),
    ])
    def test_T15_path_segment_escape_raw_string(self, path_in_segment, expected_param):
        """T-15 (MAJOR-NEW-1): _escape_like + lower → param이 raw string 기대값과 일치.

        실측 기반:
          _escape_like(r'C:\\Users'.lower()) = 'c:\\\\users'
          '%' + 'c:\\\\users' + '%' = '%c:\\\\users%' == r"%c:\\users%"
        """
        try:
            _escape_like, _, _, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail(f"T-15 FAIL: _escape_like import 실패 — Red")

        # MINOR-B 정합: caller가 lower() 적용 의무
        result_escaped = _escape_like(path_in_segment.lower())
        actual_param = "%" + result_escaped + "%"

        assert actual_param == expected_param, (
            f"T-15 FAIL: param {actual_param!r} != expected {expected_param!r}\n"
            f"  input={path_in_segment!r}, lower={path_in_segment.lower()!r}, "
            f"escaped={result_escaped!r}"
        )

    def test_T15_sql_has_escape_clause(self):
        """T-15: SQL에 ESCAPE '\\' 존재."""
        try:
            build_wiki, build_gdi = _import_build_functions()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-15 FAIL: query_builder / intent_extractor import 실패 — Red")

        intent = GdiIntent(path_segments=["카제나"])
        built = build_gdi(intent)

        assert "ESCAPE" in built.sql, (
            f"T-15 FAIL: SQL에 ESCAPE 없음. sql={built.sql[:200]!r}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# T-10: Intent → SQL 변환 정합 (v4 추가 12건)
# ─────────────────────────────────────────────────────────────────────────────

class TestIntentToSql:
    """T-10: Intent → SQL 변환 정합 (시간 표현 제거 + _next_day + need_doc_meta_join + skip_weight)."""

    # ── 시간 표현 제거 (MAJOR-NEW-2) ──────────────────────────────────────────

    def test_T10_strip_time_expressions_basic(self):
        """T-10: ['패치노트','이번 달','카제나'] → cleaned=['패치노트','카제나'], removed=['이번 달']."""
        try:
            _, _strip, _, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _strip_time_expressions import 실패 — Red")

        cleaned, removed = _strip(["패치노트", "이번 달", "카제나"])
        assert cleaned == ["패치노트", "카제나"], (
            f"T-10 FAIL: cleaned={cleaned!r} 기대 ['패치노트','카제나']"
        )
        assert "이번 달" in removed, (
            f"T-10 FAIL: removed={removed!r}에 '이번 달' 없음"
        )

    @pytest.mark.parametrize("seg,is_time", [
        ("오늘", True), ("어제", True), ("이번 주", True), ("이번 달", True),
        ("지난 달", True), ("최근 7일", True), ("최근 1개월", True),
        ("3일 전", True), ("4월 27일", True), ("4월", True),
        ("2026년 4월", True), ("2026-04-27", True),
        ("카제나", False), ("패치노트", False), ("TEST INFO", False),
    ])
    def test_T10_strip_time_expressions_patterns(self, seg, is_time):
        """T-10: 시간 표현 패턴 각 케이스 검증."""
        try:
            _, _strip, _, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _strip_time_expressions import 실패 — Red")

        cleaned, removed = _strip([seg])
        if is_time:
            assert seg in removed, (
                f"T-10 FAIL: '{seg}'이 시간 표현인데 removed에 없음. cleaned={cleaned!r}"
            )
        else:
            assert seg in cleaned, (
                f"T-10 FAIL: '{seg}'이 시간 표현 아닌데 cleaned에 없음. removed={removed!r}"
            )

    def test_T10_m5_path_segments_after_strip(self):
        """T-10 M-5: gdi 쿼리 빌드 시 '이번 달' segment가 SQL LIKE에서 제외됨."""
        try:
            build_wiki, build_gdi = _import_build_functions()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = GdiIntent(
            request_type="list",
            path_segments=["패치노트", "이번 달", "카제나"],
            file_kind=["patch_note"],
            game_alias_kr=["카제나"],
            ref_date_from="2026-04-01",
            ref_date_to="2026-04-29",
        )
        built = build_gdi(intent)

        # '이번 달'이 LIKE 패턴에 포함되면 안 됨
        like_params = [p for p in built.params if isinstance(p, str) and "LIKE" in str(built.sql)]
        assert not any("이번" in str(p) for p in built.params), (
            f"T-10 M-5 FAIL: '이번 달'이 SQL params에 포함됨. params={built.params!r}"
        )
        # '패치노트', '카제나' 포함
        joined = " ".join(str(p) for p in built.params)
        assert "패치노트" in joined, f"T-10 M-5 FAIL: '패치노트' params 미포함"
        assert "카제나" in joined, f"T-10 M-5 FAIL: '카제나' params 미포함"

    # ── _next_day (MAJOR-NEW-5) ───────────────────────────────────────────────

    def test_T10_next_day_basic(self):
        """T-10: _next_day('2026-04-29') = '2026-04-30'."""
        try:
            _, _, _next_day, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _next_day import 실패 — Red")

        assert _next_day("2026-04-29") == "2026-04-30"

    def test_T10_next_day_month_boundary(self):
        """T-10: _next_day('2026-04-30') = '2026-05-01'."""
        try:
            _, _, _next_day, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _next_day import 실패 — Red")

        assert _next_day("2026-04-30") == "2026-05-01"

    def test_T10_next_day_year_boundary(self):
        """T-10: _next_day('2026-12-31') = '2027-01-01'."""
        try:
            _, _, _next_day, _, _ = _import_helpers()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _next_day import 실패 — Red")

        assert _next_day("2026-12-31") == "2027-01-01"

    def test_T10_wiki_date_to_exclusive_upper(self):
        """T-10 M-3: wiki date_to='2026-04-29' → params에 '2026-04-30' (MAJOR-NEW-5)."""
        try:
            build_wiki, _ = _import_build_functions()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="list",
            date_field="last_modified",
            date_from="2026-04-27",
            date_to="2026-04-29",
        )
        built = build_wiki(intent)

        assert "2026-04-30" in built.params, (
            f"T-10 FAIL: _next_day 미적용. params={built.params!r}, 기대 '2026-04-30'"
        )
        assert "2026-04-29" not in built.params, (
            f"T-10 FAIL: date_to 원값 '2026-04-29'가 params에 있음 (exclusive 미처리)"
        )

    def test_T10_gdi_ref_date_to_exclusive_upper(self):
        """T-10 M-5: gdi ref_date_to='2026-04-29' → params에 '2026-04-30'."""
        try:
            _, build_gdi = _import_build_functions()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = GdiIntent(
            request_type="list",
            ref_date_from="2026-04-01",
            ref_date_to="2026-04-29",
        )
        built = build_gdi(intent)

        assert "2026-04-30" in built.params, (
            f"T-10 FAIL: gdi _next_day 미적용. params={built.params!r}"
        )

    # ── need_doc_meta_join (MAJOR-NEW-3) ──────────────────────────────────────

    def test_T10_wiki_list_date_filter_forces_dm_join(self):
        """T-10 M-3: date_field=last_modified → need_doc_meta_join=True 강제."""
        try:
            build_wiki, _ = _import_build_functions()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="list",
            date_field="last_modified",
            date_from="2026-04-27",
            date_to="2026-04-29",
        )
        built = build_wiki(intent)

        assert built.need_doc_meta_join, (
            "T-10 FAIL: date_field=last_modified 시 need_doc_meta_join=True 강제 미구현 — Red"
        )

    def test_T10_wiki_list_order_by_with_dm_join(self):
        """T-10 M-3: need_doc_meta_join=True → ORDER BY dm.last_modified DESC NULLS LAST."""
        try:
            _, _, _, _resolve_order, _ = _import_helpers()
            WikiIntent, _ = _import_dataclasses()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _resolve_order_by_wiki_list import 실패 — Red")

        intent = WikiIntent(request_type="list", date_field="last_modified")
        order = _resolve_order(intent, need_doc_meta_join=True)

        assert "dm.last_modified" in order and "DESC" in order, (
            f"T-10 FAIL: need_doc_meta_join=True → 기대 dm.last_modified DESC, 실제 {order!r}"
        )

    def test_T10_wiki_list_order_by_without_dm_join(self):
        """T-10 M-3: need_doc_meta_join=False → ORDER BY n.title ASC (안전 fallback)."""
        try:
            _, _, _, _resolve_order, _ = _import_helpers()
            WikiIntent, _ = _import_dataclasses()
        except (ImportError, AttributeError):
            pytest.fail("T-10 FAIL: _resolve_order_by_wiki_list import 실패 — Red")

        intent = WikiIntent(request_type="list")
        order = _resolve_order(intent, need_doc_meta_join=False)

        assert "n.title" in order, (
            f"T-10 FAIL: need_doc_meta_join=False → 기대 n.title ASC, 실제 {order!r}"
        )

    # ── skip_weight (OQ-v3-2) ─────────────────────────────────────────────────

    def test_T10_skip_weight_for_metadata(self):
        """T-10: request_type=metadata → BuiltQuery.skip_weight=True."""
        try:
            build_wiki, _ = _import_build_functions()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = WikiIntent(
            request_type="metadata",
            metadata_field="last_modified",
            title_keywords=["TEST INFO"],
        )
        built = build_wiki(intent)

        assert built.skip_weight, (
            "T-10 FAIL: metadata 경로 skip_weight=True 미구현 — Red"
        )

    def test_T10_skip_weight_for_list(self):
        """T-10: request_type=list → BuiltQuery.skip_weight=True."""
        try:
            build_wiki, _ = _import_build_functions()
            WikiIntent, _ = _import_dataclasses()
        except ImportError:
            pytest.fail("T-10 FAIL: import 실패 — Red")

        intent = WikiIntent(request_type="list")
        built = build_wiki(intent)

        assert built.skip_weight, (
            "T-10 FAIL: list 경로 skip_weight=True 미구현 — Red"
        )

    def test_T10_weight_matrix_no_patch_note_row(self):
        """T-10 MINOR-C: weight matrix에 'patch_note' key 없음 (실 운영 distinct에 없음)."""
        try:
            qb = _import_query_builder()
        except ImportError:
            pytest.fail("T-10 FAIL: query_builder import 실패 — Red")

        matrix = getattr(qb, "_WEIGHT_MATRIX", None)
        assert matrix is not None, "T-10 FAIL: _WEIGHT_MATRIX 없음 — Red"

        # matrix key는 (folder_role, chunk_origin) tuple
        matrix_keys = set()
        for k in matrix.keys():
            if isinstance(k, tuple):
                matrix_keys.add(k[0])
            elif isinstance(k, str):
                matrix_keys.add(k)

        assert "patch_note" not in matrix_keys, (
            f"T-10 MINOR-C FAIL: _WEIGHT_MATRIX에 'patch_note' key 있음 (제거 필요)"
        )


# ─────────────────────────────────────────────────────────────────────────────
# T-11: weight 정렬 정합 (skip_weight 분기)
# ─────────────────────────────────────────────────────────────────────────────

class TestWeightSort:
    """T-11: _apply_weight_and_sort skip_weight 분기 검증."""

    def test_T11_skip_weight_true_no_score_calculation(self):
        """T-11: skip_weight=True → score 계산 없이 SQL 결과 그대로."""
        try:
            _, _, _, _, _apply_weight = _import_helpers()
            qb = _import_query_builder()
        except (ImportError, AttributeError):
            pytest.fail("T-11 FAIL: _apply_weight_and_sort import 실패 — Red")

        BuiltQuery = qb.BuiltQuery
        # skip_weight=True인 BuiltQuery mock
        built = BuiltQuery(
            sql="SELECT ...",
            params=[10],  # LIMIT=10
            intent_signature="",
            where_clauses=[],
            weight_matrix={},
            fts_query=None,
            domain="wiki",
            has_fts=False,
            request_type="metadata",
            skip_weight=True,
        )

        rows = [
            {"node_id": 1, "fts_rank": None, "folder_role": "planning", "chunk_origin": "table"},
            {"node_id": 2, "fts_rank": None, "folder_role": "qa_result", "chunk_origin": "sheet"},
        ]
        result = _apply_weight(rows, built)
        # skip_weight=True → 원래 순서 유지
        assert [r["node_id"] for r in result] == [1, 2], (
            f"T-11 FAIL: skip_weight=True 시 순서 변경됨. result={[r['node_id'] for r in result]!r}"
        )

    def test_T11_built_query_has_skip_weight_field(self):
        """T-11: BuiltQuery dataclass에 skip_weight 필드 존재."""
        try:
            qb = _import_query_builder()
        except ImportError:
            pytest.fail("T-11 FAIL: query_builder import 실패 — Red")

        BuiltQuery = getattr(qb, "BuiltQuery", None)
        assert BuiltQuery is not None, "T-11 FAIL: BuiltQuery 없음 — Red"

        import dataclasses as dc
        fields = {f.name for f in dc.fields(BuiltQuery)}
        assert "skip_weight" in fields, (
            f"T-11 FAIL: BuiltQuery에 skip_weight 필드 없음. fields={fields}"
        )
        assert "need_doc_meta_join" in fields, (
            f"T-11 FAIL: BuiltQuery에 need_doc_meta_join 필드 없음"
        )
        assert "relaxation_strip_log" in fields, (
            f"T-11 FAIL: BuiltQuery에 relaxation_strip_log 필드 없음"
        )

    def test_T11_minor_a_game_alias_local_var(self):
        """T-11 MINOR-A: build_gdi_query 내 game_alias 처리 시 intent 직접 변이 금지.

        intent.game_alias_kr = [...] 직접 대입 금지 → effective_game 로컬 변수 사용.
        검증: build 후 intent 원본 불변.
        """
        try:
            _, build_gdi = _import_build_functions()
            _, GdiIntent = _import_dataclasses()
        except ImportError:
            pytest.fail("T-11 FAIL: import 실패 — Red")

        # game_alias_kr=[] 상태로 진입 (canonical 자동 탐지 케이스)
        intent = GdiIntent(
            request_type="content_search",
            path_segments=["에픽세븐", "캐릭터"],
            game_alias_kr=[],
        )
        game_alias_before = list(intent.game_alias_kr)
        build_gdi(intent)

        # 원본 intent 불변 (MINOR-A)
        assert intent.game_alias_kr == game_alias_before, (
            f"T-11 MINOR-A FAIL: intent.game_alias_kr가 build 후 변이됨. "
            f"before={game_alias_before!r}, after={intent.game_alias_kr!r}"
        )
