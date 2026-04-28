"""
test_reconstructors_l2.py — task-081 L2 Top-N + Overview 단위 테스트

매트릭스:
  T-81-1: XLSX XML 모드 → <overview> 섹션 존재
  T-81-2: <overview> 내 sheet-info가 시트 수만큼 포함
  T-81-3: summary 시트의 sheet-info type="summary" + rows 일치
  T-81-4: 데이터 시트의 sheet-info type="data" + rows/columns 일치
  T-81-5: 큰 시트(>= PREVIEW_THRESHOLD)에 <preview rows="N"> 섹션 선행
  T-81-6: 작은 시트(< PREVIEW_THRESHOLD)에는 <preview> 없음
  T-81-7: preview 내용 = data 상위 N행 동일
  T-81-8: has_preview="true" 속성 overview 반영
  T-81-9: truncated 시트에 overview sheet-info truncated="true"
  T-81-10: 마크다운 모드에서 <overview>/<preview> 없음 (회귀)
  T-81-11: XML 파싱 성공 (xml.etree.ElementTree)
  T-81-12: overview 위치가 첫 번째 <sheet> 앞
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from reconstructors import (
    reconstruct_xlsx, reconstruct_body,
    MAX_TABLE_ROWS, PREVIEW_ROWS, PREVIEW_THRESHOLD,
)


def _make_xlsx_chunks(sheet_name: str, n_rows: int, cols: list):
    """테스트용 XLSX chunk 리스트 생성."""
    chunks = []
    for i in range(1, n_rows + 1):
        parts = [
            f"Mode: generic_xlsx",
            f"FileType: test.xlsx",
            f"Sheet: {sheet_name}",
            f"Row: {i}",
        ]
        for c, base in cols:
            parts.append(f"{c}: {base}_{i}")
        chunks.append(" > ".join(parts))
    return chunks


def test_t81_1_overview_section_exists():
    """XLSX XML 모드 → <overview> 섹션 존재."""
    chunks = _make_xlsx_chunks("영웅", 10, [("이름", "카린"), ("HP", "4200")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "h.xlsx"})
    assert "<overview>" in result
    assert "</overview>" in result


def test_t81_2_sheet_info_count():
    """overview에 sheet-info가 시트 수만큼 포함."""
    chunks = (
        _make_xlsx_chunks("Sheet1", 5, [("a", "x"), ("b", "y")])
        + _make_xlsx_chunks("Sheet2", 3, [("c", "z")])
    )
    result = reconstruct_xlsx(chunks, file_info={"file_name": "multi.xlsx"})
    root = ET.fromstring(result)
    overview = root.find("overview")
    assert overview is not None
    sheet_infos = overview.findall("sheet-info")
    assert len(sheet_infos) == 2


def test_t81_3_summary_sheet_info():
    """summary 시트의 sheet-info type='summary'."""
    chunks = _make_xlsx_chunks("Summary", 3, [("프로젝트", "E7"), ("버전", "1.2")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "s.xlsx"})
    root = ET.fromstring(result)
    overview = root.find("overview")
    info = overview.find("sheet-info")
    assert info.attrib["name"] == "Summary"
    assert info.attrib["type"] == "summary"
    assert info.attrib["rows"] == "3"


def test_t81_4_data_sheet_info():
    """데이터 시트의 sheet-info type='data'."""
    chunks = _make_xlsx_chunks("데이터", 20, [("col1", "v1"), ("col2", "v2"), ("col3", "v3")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "d.xlsx"})
    root = ET.fromstring(result)
    info = root.find("overview/sheet-info")
    assert info.attrib["type"] == "data"
    assert info.attrib["rows"] == "20"
    assert info.attrib["columns"] == "3"


def test_t81_5_preview_on_large_sheet():
    """큰 시트(>= PREVIEW_THRESHOLD)에 <preview> 선행."""
    n = PREVIEW_THRESHOLD + 10  # 60 rows
    chunks = _make_xlsx_chunks("Big", n, [("x", "a"), ("y", "b")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "big.xlsx"})
    assert f'<preview rows="{PREVIEW_ROWS}">' in result
    assert "</preview>" in result


def test_t81_6_no_preview_on_small_sheet():
    """작은 시트(< PREVIEW_THRESHOLD)에는 <preview> 없음."""
    n = PREVIEW_THRESHOLD - 10  # 40 rows
    chunks = _make_xlsx_chunks("Small", n, [("x", "a"), ("y", "b")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "small.xlsx"})
    assert "<preview" not in result


def test_t81_7_preview_content_matches_data():
    """preview 내용 = data 상위 N행과 동일."""
    n = PREVIEW_THRESHOLD + 10  # 60 rows
    chunks = _make_xlsx_chunks("Big", n, [("x", "a"), ("y", "b")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "big.xlsx"})
    root = ET.fromstring(result)
    sheet = root.find("sheet")
    preview = sheet.find("preview")
    data = sheet.find("data")
    # preview 내 행 = 헤더(1) + 구분선(1) + PREVIEW_ROWS
    preview_lines = [l for l in preview.text.split("\n") if l.strip()]
    assert len(preview_lines) == 2 + PREVIEW_ROWS  # 헤더 + 구분선 + 10 데이터
    # 첫 데이터 행은 _1 suffix
    assert "a_1" in preview.text
    # preview 행이 data 행과 일치 (상위 N행)
    data_lines = [l for l in data.text.split("\n") if l.strip()]
    for i in range(PREVIEW_ROWS):
        # 헤더/구분 2줄 건너뛰고 비교
        assert preview_lines[2 + i] == data_lines[2 + i]


def test_t81_8_has_preview_attr():
    """큰 시트의 sheet-info에 has_preview='true'."""
    n = PREVIEW_THRESHOLD + 5
    chunks = _make_xlsx_chunks("L", n, [("x", "a"), ("y", "b")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "l.xlsx"})
    root = ET.fromstring(result)
    info = root.find("overview/sheet-info")
    assert info.attrib.get("has_preview") == "true"


def test_t81_9_truncated_attr():
    """MAX_TABLE_ROWS 초과 시 sheet-info truncated='true'."""
    n = MAX_TABLE_ROWS + 50
    chunks = _make_xlsx_chunks("Huge", n, [("x", "a")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "huge.xlsx"})
    root = ET.fromstring(result)
    info = root.find("overview/sheet-info")
    assert info.attrib.get("truncated") == "true"


def test_t81_10_no_overview_in_markdown_mode():
    """마크다운 모드에서 <overview>/<preview> 없음 (회귀)."""
    chunks = _make_xlsx_chunks("Big", PREVIEW_THRESHOLD + 5, [("x", "a")])
    result = reconstruct_xlsx(chunks)  # file_info=None
    assert "<overview>" not in result
    assert "<preview" not in result
    # 마크다운 헤더는 유지
    assert "## Sheet: Big" in result


def test_t81_11_xml_parseable_with_overview():
    """overview + preview 포함 XML 파싱 성공."""
    n = PREVIEW_THRESHOLD + 20
    chunks = (
        _make_xlsx_chunks("Summary", 2, [("p", "E7"), ("v", "1")])
        + _make_xlsx_chunks("Data", n, [("x", "a"), ("y", "b")])
    )
    result = reconstruct_xlsx(chunks, file_info={"file_name": "mix.xlsx"})
    root = ET.fromstring(result)
    assert root.tag == "file"
    assert root.attrib["sheets"] == "2"
    overview = root.find("overview")
    assert len(overview.findall("sheet-info")) == 2
    sheets = root.findall("sheet")
    assert len(sheets) == 2


def test_t81_12_overview_before_first_sheet():
    """<overview> 위치가 첫 번째 <sheet> 앞."""
    chunks = _make_xlsx_chunks("A", 5, [("x", "a")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "a.xlsx"})
    overview_pos = result.find("<overview>")
    first_sheet_pos = result.find("<sheet ")
    assert overview_pos < first_sheet_pos
    assert overview_pos > result.find("<file ")


def test_t81_13_preview_escapes_special_chars():
    """preview 내 셀 값도 XML 이스케이프 적용."""
    # PREVIEW_THRESHOLD 이상 + 첫 행에 특수문자
    chunks = []
    chunks.append(
        "Mode: generic_xlsx > FileType: t.xlsx > Sheet: s > Row: 1 > name: <Tom & Jerry> > val: v1"
    )
    for i in range(2, PREVIEW_THRESHOLD + 5):
        chunks.append(
            f"Mode: generic_xlsx > FileType: t.xlsx > Sheet: s > Row: {i} > name: n_{i} > val: v_{i}"
        )
    result = reconstruct_xlsx(chunks, file_info={"file_name": "t.xlsx"})
    # XML 파싱 성공이 핵심
    root = ET.fromstring(result)
    preview = root.find("sheet/preview")
    assert preview is not None
    # 이스케이프 확인
    assert "&lt;Tom" in result
    assert "&amp;" in result


def test_t81_14_reconstruct_body_propagates_overview():
    """reconstruct_body 디스패처가 overview 전파."""
    chunks = _make_xlsx_chunks("X", 5, [("a", "b")])
    result = reconstruct_body(chunks, "generic_xlsx", file_info={"file_name": "x.xlsx"})
    assert "<overview>" in result
    assert "<sheet-info" in result


def test_t81_15_empty_has_preview_when_false():
    """작은 시트의 sheet-info에는 has_preview 속성 없음 (None = 속성 제외)."""
    chunks = _make_xlsx_chunks("S", 10, [("x", "a")])
    result = reconstruct_xlsx(chunks, file_info={"file_name": "s.xlsx"})
    root = ET.fromstring(result)
    info = root.find("overview/sheet-info")
    assert "has_preview" not in info.attrib


if __name__ == "__main__":
    import traceback
    tests = [(n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  PASS: {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL: {name} — {e}")
            traceback.print_exc()

    print(f"\n{'=' * 50}")
    print(f"Total: {len(tests)}, Passed: {passed}, Failed: {failed}")
    sys.exit(0 if failed == 0 else 1)
