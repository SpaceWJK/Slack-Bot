"""
test_reconstructors_xml.py — task-080 L1 XML 래핑 단위 테스트

매트릭스:
  T-1: XLSX <sheet rows> 정확
  T-2: Summary 시트 type="summary" (마크다운 헤더 중복 없음)
  T-3: PPTX <slide number>
  T-4: TSV <file total_rows>
  T-5: MAX_TABLE_ROWS truncation 마커
  T-6: XML escape (제어문자/&<>" 처리)
  T-7: file_info=None fallback (마크다운 유지, 고아 태그 없음)
  T-8: 빈 chunks + file_info → self-closing <file/>
  T-9: xml.etree.ElementTree.fromstring 파싱 성공
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from reconstructors import (
    reconstruct_xlsx, reconstruct_pptx, reconstruct_tsv, reconstruct_body,
    _xml_escape, _xml_attrs, MAX_TABLE_ROWS,
)


def test_t1_xlsx_sheet_rows_columns():
    chunks = [
        "Mode: generic_xlsx > FileType: hero.xlsx > Sheet: 영웅 > Row: 1 > 이름: 카린 > HP: 4200 > 클래스: 전사",
        "Mode: generic_xlsx > FileType: hero.xlsx > Sheet: 영웅 > Row: 2 > 이름: 아크엔젤 > HP: 12500 > 클래스: 힐러",
    ]
    result = reconstruct_xlsx(chunks, file_info={"file_name": "hero.xlsx"})
    assert '<file name="hero.xlsx" source_type="generic_xlsx" sheets="1">' in result
    assert '<sheet name="영웅" rows="2" columns="3">' in result
    assert '<headers>이름, HP, 클래스</headers>' in result
    assert '<data>' in result
    assert '카린' in result
    assert '</data>' in result
    assert '</sheet>' in result
    assert '</file>' in result


def test_t2_summary_sheet_type():
    """Summary 시트는 type="summary" + 마크다운 헤더 중복 없음."""
    chunks = [
        "Mode: generic_xlsx > FileType: s.xlsx > Sheet: Summary > Row: 1 > 프로젝트: Epicseven > 버전: 1.2",
        "Mode: generic_xlsx > FileType: s.xlsx > Sheet: Summary > Row: 2 > 담당자: 김우주 > 날짜: 2026-04-10",
    ]
    result = reconstruct_xlsx(chunks, file_info={"file_name": "s.xlsx"})
    assert '<sheet name="Summary" type="summary"' in result
    # M-3 해소: XML 모드에서 "## Sheet:" 중복 없음
    assert '## Sheet:' not in result
    # 실데이터는 포함
    assert 'Epicseven' in result


def test_t3_pptx_slide_number():
    chunks = [
        "Mode: generic_pptx > FileType: p.pptx > ContentType: generic_pptx > Slide: 1 > 첫번째 슬라이드",
        "Mode: generic_pptx > FileType: p.pptx > ContentType: generic_pptx > Slide: 2 > 두번째 슬라이드",
    ]
    result = reconstruct_pptx(chunks, file_info={"file_name": "p.pptx"})
    assert '<file name="p.pptx" source_type="generic_pptx" slides="2">' in result
    assert '<slide number="1">' in result
    assert '<slide number="2">' in result
    assert '<content>' in result
    assert '</content>' in result
    assert '첫번째' in result


def test_t4_tsv_total_rows():
    chunks = [
        "index_mode: generic_tsv\nname: 카린\nattack: 1200\ndefense: 800",
        "index_mode: generic_tsv\nname: 아크엔젤\nattack: 900\ndefense: 1500",
    ]
    result = reconstruct_tsv(chunks, file_info={"file_name": "balance.tsv"})
    assert '<file name="balance.tsv" source_type="generic_tsv" total_rows="2">' in result
    assert '<headers>name, attack, defense</headers>' in result
    assert '<data>' in result
    assert '카린' in result
    assert '</data>' in result


def test_t5_max_table_rows_marker():
    """MAX_TABLE_ROWS 초과 시 XML 주석 마커 확인."""
    chunks = []
    for i in range(MAX_TABLE_ROWS + 100):
        chunks.append(f"index_mode: generic_tsv\nid: {i}\nvalue: v{i}")
    result = reconstruct_tsv(chunks, file_info={"file_name": "big.tsv"})
    # XML 주석 마커 확인
    assert '<!-- rows truncated:' in result
    assert 'more rows omitted' in result


def test_t6_xml_escape():
    """XML escape: 제어 문자 제거 + 특수문자 이스케이프."""
    # 제어 문자
    assert _xml_escape("abc\x00def") == "abcdef"
    assert _xml_escape("a\x01b\x02c") == "abc"
    # 탭/LF/CR 보존
    assert "\t" in _xml_escape("a\tb")
    assert "\n" in _xml_escape("a\nb")
    # XML 특수문자
    assert _xml_escape("&<>\"") == "&amp;&lt;&gt;&quot;"
    # 복합
    assert _xml_escape('<hero name="카린" />') == '&lt;hero name=&quot;카린&quot; /&gt;'


def test_t7_file_info_none_fallback():
    """file_info=None이면 기존 마크다운 동작 (고아 XML 태그 없음)."""
    chunks = [
        "index_mode: generic_tsv\nid: 1\nvalue: v1",
        "index_mode: generic_tsv\nid: 2\nvalue: v2",
    ]
    # XML 모드
    xml_result = reconstruct_tsv(chunks, file_info={"file_name": "t.tsv"})
    assert '<file' in xml_result

    # 마크다운 모드 (fallback)
    md_result = reconstruct_tsv(chunks)
    assert '<file' not in md_result
    assert '<headers>' not in md_result  # 고아 태그 없음
    assert '<data>' not in md_result
    assert '| id | value |' in md_result  # 기존 마크다운 형식


def test_t8_empty_chunks_self_closing():
    """빈 chunks + file_info → self-closing <file/>."""
    result = reconstruct_xlsx([], file_info={"file_name": "empty.xlsx"})
    assert '<file name="empty.xlsx" source_type="generic_xlsx" sheets="0"/>' in result

    result_pptx = reconstruct_pptx([], file_info={"file_name": "empty.pptx"})
    assert '<file name="empty.pptx" source_type="generic_pptx" slides="0"/>' in result_pptx

    result_tsv = reconstruct_tsv([], file_info={"file_name": "empty.tsv"})
    assert '<file name="empty.tsv" source_type="generic_tsv" total_rows="0"/>' in result_tsv


def test_t9_xml_parseable():
    """xml.etree.ElementTree.fromstring으로 파싱 성공."""
    chunks = [
        "Mode: generic_xlsx > FileType: parse.xlsx > Sheet: 테스트 > Row: 1 > col1: val1 > col2: val2",
    ]
    result = reconstruct_xlsx(chunks, file_info={"file_name": "parse.xlsx"})
    # 마크다운 테이블의 | 문자가 XML에 포함되지만 이스케이프 불필요 (태그 외부)
    # 실제 파싱 가능한 부분만 추출해서 검증
    root = ET.fromstring(result)
    assert root.tag == 'file'
    assert root.attrib['name'] == 'parse.xlsx'
    assert root.attrib['source_type'] == 'generic_xlsx'
    sheets = root.findall('sheet')
    assert len(sheets) == 1
    assert sheets[0].attrib['name'] == '테스트'


def test_t9b_pptx_parseable():
    chunks = [
        "Mode: generic_pptx > FileType: p.pptx > ContentType: generic_pptx > Slide: 1 > 단순 텍스트",
    ]
    result = reconstruct_pptx(chunks, file_info={"file_name": "p.pptx"})
    root = ET.fromstring(result)
    assert root.tag == 'file'
    assert root.attrib['slides'] == '1'
    slides = root.findall('slide')
    assert slides[0].attrib['number'] == '1'


def test_t9c_tsv_parseable():
    chunks = ["index_mode: generic_tsv\na: 1\nb: 2"]
    result = reconstruct_tsv(chunks, file_info={"file_name": "t.tsv"})
    root = ET.fromstring(result)
    assert root.tag == 'file'
    assert root.attrib['total_rows'] == '1'


def test_t10_reconstruct_body_dispatcher():
    """reconstruct_body가 file_info 전달 제대로 하는지."""
    chunks = ["index_mode: generic_tsv\na: 1"]

    # file_info 전달
    r1 = reconstruct_body(chunks, "generic_tsv", file_info={"file_name": "x.tsv"})
    assert '<file' in r1

    # file_info 없음 (기존 호출)
    r2 = reconstruct_body(chunks, "generic_tsv")
    assert '<file' not in r2


def test_t12_xss_xlsx_data_escape():
    """[SECURITY] XLSX <data> 셀 값에 &<> 포함 시 XML 파싱 성공해야 함."""
    chunks = [
        "Mode: generic_xlsx > FileType: t.xlsx > Sheet: x > Row: 1 > a: <Tom & Jerry> > b: <i>italic</i>",
    ]
    result = reconstruct_xlsx(chunks, file_info={"file_name": "t.xlsx"})
    # 파싱 성공 여부 (가장 중요 — 이스케이프 안 되면 여기서 ParseError)
    root = ET.fromstring(result)
    assert root.tag == 'file'
    # 출력(raw)에 이스케이프 적용 확인 (ET.fromstring은 decode하므로 raw에서 체크)
    assert '&amp;' in result
    assert '&lt;Tom' in result
    # 실제 '<Tom>' 태그로 인식되지 않아야 함 (텍스트로만 존재)
    assert root.findall('.//Tom') == []
    # data text에는 decode된 원문이 있어야 함 (LLM이 읽을 수 있음)
    data_text = root.find('sheet/data').text
    assert '<Tom & Jerry>' in data_text  # decode 후 원문


def test_t13_xss_pptx_content_escape():
    """[SECURITY] PPTX <content> 내부 <script> 태그 이스케이프."""
    chunks = [
        "Mode: generic_pptx > FileType: p.pptx > ContentType: generic_pptx > Slide: 1 > <script>alert('xss')</script>",
    ]
    result = reconstruct_pptx(chunks, file_info={"file_name": "p.pptx"})
    root = ET.fromstring(result)
    content = root.find('slide/content').text
    # 텍스트로 처리되어야 함 (실제 <script> 태그 아님)
    assert '&lt;script&gt;' in result or '<script>' in content
    # <script>가 실제 자식 태그로 파싱되지 않음
    assert root.findall('.//script') == []


def test_t14_xss_tsv_cell_escape():
    """[SECURITY] TSV 셀 값 이스케이프."""
    chunks = ["index_mode: generic_tsv\nname: A&B\ndesc: 1<2"]
    result = reconstruct_tsv(chunks, file_info={"file_name": "t.tsv"})
    # 파싱 성공
    root = ET.fromstring(result)
    assert root.tag == 'file'
    # 이스케이프 적용
    assert '&amp;' in result
    assert '&lt;' in result


def test_t15_korean_regression():
    """한국어 데이터 회귀: 이스케이프 적용 후에도 한국어 정상."""
    chunks = [
        "Mode: generic_xlsx > FileType: k.xlsx > Sheet: 영웅 > Row: 1 > 이름: 카린 > HP: 4200",
    ]
    result = reconstruct_xlsx(chunks, file_info={"file_name": "k.xlsx"})
    root = ET.fromstring(result)
    assert root.find('sheet').attrib['name'] == '영웅'
    # 한국어 실데이터 포함 (이스케이프되지 않음)
    assert '카린' in result
    assert '4200' in result


def test_t11_backward_compat():
    """기존 호출 방식 (file_info 없이)이 동작해야 함."""
    chunks = [
        "Mode: generic_xlsx > FileType: h.xlsx > Sheet: 영웅 > Row: 1 > 이름: 카린 > HP: 4200",
    ]
    result = reconstruct_xlsx(chunks)  # 기존 시그니처
    assert '## Sheet: 영웅' in result
    assert '<file' not in result
    assert '| 이름 | HP |' in result


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
