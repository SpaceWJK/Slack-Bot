"""
test_reconstructors.py — task-075 단위 테스트

검증 매트릭스:
  T-1: import 성공
  T-3: XLSX 회귀 (Summary/fill-down 포함)
  T-4: TSV 회귀 (콜론 값/빈 헤더)
  T-5: PPTX 회귀 (탭 테이블/이미지 참조)
  T-6: TSV 성능 (O(n²) → O(n))

실행: python -m pytest tests/test_reconstructors.py -v
      또는: python tests/test_reconstructors.py
"""

import sys
import os
import time
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from reconstructors import (
    reconstruct_body, reconstruct_xlsx, reconstruct_pptx, reconstruct_tsv,
    MAX_TABLE_ROWS, MAX_COL_WIDTH,
    _clean_chunk_text, _is_summary_sheet, _sanitize_cells,
    _deduplicate_headers,
)


# ── T-1: Import 성공 ─────────────────────────────────────────────────────

def test_t1_import_succeeds():
    assert callable(reconstruct_body)
    assert callable(reconstruct_xlsx)
    assert callable(reconstruct_pptx)
    assert callable(reconstruct_tsv)
    assert MAX_TABLE_ROWS == 20000
    assert MAX_COL_WIDTH == 80


# ── T-3: XLSX 회귀 ────────────────────────────────────────────────────────

def test_t3_xlsx_basic():
    """기본 XLSX 청크 파싱 + 마크다운 테이블 변환."""
    chunks = [
        "Mode: generic_xlsx > FileType: test.xlsx > Sheet: 영웅 > Row: 1 > 이름: 아크엔젤 > HP: 12500 > 클래스: 힐러",
        "Mode: generic_xlsx > FileType: test.xlsx > Sheet: 영웅 > Row: 2 > 이름: 전사 > HP: 8000 > 클래스: 전사",
    ]
    result = reconstruct_xlsx(chunks)
    assert "## Sheet: 영웅" in result
    assert "아크엔젤" in result
    assert "12500" in result
    assert "| 이름 | HP | 클래스 |" in result


def test_t3_xlsx_summary_sheet():
    """Summary 시트는 key:value 포맷으로 변환."""
    chunks = [
        "Mode: generic_xlsx > FileType: t.xlsx > Sheet: Summary > Row: 1 > 프로젝트: Epicseven > 버전: 1.2",
        "Mode: generic_xlsx > FileType: t.xlsx > Sheet: Summary > Row: 2 > 담당자: 김우주 > 날짜: 2026-04-10",
    ]
    result = reconstruct_xlsx(chunks)
    assert "## Sheet: Summary" in result
    # Summary는 테이블이 아닌 bullet 형태
    assert "- **" in result


def test_t3_xlsx_fill_down():
    """카테고리 열 fill-down 동작 확인."""
    chunks = [
        f"Mode: generic_xlsx > FileType: t.xlsx > Sheet: A > Row: {i+1} > 분류: {c} > 이름: item{i} > 값: {v}"
        for i, (c, v) in enumerate([
            ("전사", 100), ("", 80), ("", 90),  # fill-down 대상
            ("마법사", 120), ("", 110),
            ("힐러", 150), ("", 140),
        ])
    ]
    result = reconstruct_xlsx(chunks)
    # fill-down 후 "전사"가 여러 번 등장해야 함
    # (빈 셀 50% 이상일 때만 트리거 — 3/7≈42%라 트리거 안 됨 → 키워드 매칭으로 트리거)
    # _CATEGORY_KEYWORDS에 "분류" 포함
    assert result.count("전사") >= 2  # 첫 행 + fill-down


# ── T-4: TSV 회귀 ────────────────────────────────────────────────────────

def test_t4_tsv_basic():
    """TSV 청크를 마크다운 테이블로 변환."""
    chunks = [
        "index_mode: generic_tsv\nfile_type: a.tsv\ncontent_type: generic_tsv\n이름: 영웅A\nHP: 100\nMP: 50",
        "index_mode: generic_tsv\nfile_type: a.tsv\ncontent_type: generic_tsv\n이름: 영웅B\nHP: 200\nMP: 80",
    ]
    result = reconstruct_tsv(chunks)
    assert "| 이름 | HP | MP |" in result
    assert "영웅A" in result
    assert "영웅B" in result


def test_t4_tsv_colon_in_value():
    """값에 콜론이 포함된 경우 (url 등)."""
    chunks = [
        "index_mode: generic_tsv\nname: test\nurl: http://example.com:8080/api",
    ]
    result = reconstruct_tsv(chunks)
    assert "test" in result
    assert "http://example.com:8080/api" in result


def test_t4_tsv_header_order_preserved():
    """헤더 삽입 순서가 유지되는지 (O(n) 개선 회귀 방지)."""
    chunks = [
        "index_mode: generic_tsv\na: 1\nb: 2\nc: 3",
        "index_mode: generic_tsv\nd: 4\ne: 5",  # 새 헤더 d, e 추가
    ]
    result = reconstruct_tsv(chunks)
    # 헤더가 a, b, c, d, e 순서로 나와야 함
    first_line = result.split("\n")[0]
    # 마크다운 테이블 헤더 라인
    assert first_line.startswith("| a | b | c | d | e")


# ── T-5: PPTX 회귀 ───────────────────────────────────────────────────────

def test_t5_pptx_basic():
    """PPTX 슬라이드 번호 + 내용."""
    chunks = [
        "Mode: generic_pptx > FileType: a.pptx > ContentType: generic_pptx > Slide: 1 > 제목 슬라이드\n소개 내용",
        "Mode: generic_pptx > FileType: a.pptx > ContentType: generic_pptx > Slide: 2 > 두번째 슬라이드",
    ]
    result = reconstruct_pptx(chunks)
    assert "## Slide 1" in result
    assert "## Slide 2" in result
    assert "제목 슬라이드" in result


def test_t5_pptx_image_reference():
    """이미지 참조 통일."""
    chunks = [
        "Mode: generic_pptx > FileType: a.pptx > ContentType: generic_pptx > Slide: 1 > ![](hero.png) 영웅 이미지",
    ]
    result = reconstruct_pptx(chunks)
    assert "[이미지: hero.png]" in result
    assert "![](" not in result


def test_t5_pptx_tab_table_conversion():
    """탭 구분 데이터 → 마크다운 테이블."""
    tab_text = "이름\tHP\tMP\n영웅\t100\t50\n전사\t150\t30\n마법사\t80\t120"
    chunks = [
        f"Mode: generic_pptx > FileType: a.pptx > ContentType: generic_pptx > Slide: 1 > {tab_text}",
    ]
    result = reconstruct_pptx(chunks)
    assert "| 이름 | HP | MP |" in result
    assert "| 영웅 | 100 | 50 |" in result


def test_t5_pptx_empty_notes_removed():
    """빈 Notes 섹션 제거."""
    chunks = [
        "Mode: generic_pptx > FileType: a.pptx > ContentType: generic_pptx > Slide: 1 > 내용\n### Notes:\n",
    ]
    result = reconstruct_pptx(chunks)
    assert "### Notes:" not in result
    assert "내용" in result


# ── T-6: TSV 성능 ────────────────────────────────────────────────────────

def _generate_tsv_chunks(cols: int, rows: int):
    """벤치마크용 TSV 청크 생성. 헤더가 청크마다 달라져서 O(n²) 효과 유발."""
    chunks = []
    # 각 청크가 서로 다른 컬럼 서브셋을 가지도록
    # (실제 GDI 청크는 행마다 일부 필드만 포함되는 경우가 있음)
    for r in range(rows):
        lines = ["index_mode: generic_tsv", f"file_type: t{r}.tsv", "content_type: generic_tsv"]
        # 컬럼 순서를 매번 약간 달리해서 O(n²) 발생 가능하게 함
        for c in range(cols):
            # 매 행마다 다른 인덱스 offset을 줘서 새 헤더가 나올 수 있게
            col_idx = (c + r) % cols
            lines.append(f"col_{col_idx}: val_{r}_{c}")
        chunks.append("\n".join(lines))
    return chunks


def test_t6_tsv_performance_wide():
    """1000 columns × 100 rows — O(n²) 개선 확실한 케이스."""
    chunks = _generate_tsv_chunks(cols=1000, rows=100)
    t0 = time.time()
    result = reconstruct_tsv(chunks)
    elapsed = time.time() - t0
    print(f"\n  [PERF T-6 wide] 1000×100: {elapsed:.4f}s")
    # 실측 NEW ~0.05s. 여유 있게 0.5s 기준
    assert elapsed < 0.5, f"O(n) 개선 후에도 {elapsed:.3f}s — 성능 목표 미달"


def test_t6_tsv_performance_standard():
    """100 columns × 1000 rows — 완화된 기준."""
    chunks = _generate_tsv_chunks(cols=100, rows=1000)
    t0 = time.time()
    result = reconstruct_tsv(chunks)
    elapsed = time.time() - t0
    print(f"\n  [PERF T-6 standard] 100×1000: {elapsed:.4f}s")
    # 실측 NEW ~0.05s. 여유 있게 1.0s 기준
    assert elapsed < 1.0, f"O(n) 개선 후에도 {elapsed:.3f}s — 성능 목표 미달"


# ── 부가 테스트: 빈 입력, fallback ───────────────────────────────────────

def test_empty_chunks():
    assert reconstruct_body([], "generic_xlsx") == ""
    assert reconstruct_body([], "generic_tsv") == ""
    assert reconstruct_body([], "generic_pptx") == ""


def test_unknown_source_type():
    """알 수 없는 source_type은 fallback (메타 제거 후 결합)."""
    chunks = ["index_mode: foo\ncontent here", "another line"]
    result = reconstruct_body(chunks, "unknown_type")
    assert "content here" in result
    assert "another line" in result
    # 메타 라인은 제거
    assert "index_mode: foo" not in result


def test_sanitize_cells_pipe_escape():
    """파이프 문자 이스케이프."""
    cells = ["a|b", "normal", "c\nd"]
    result = _sanitize_cells(cells)
    assert result[0] == "a\\|b"
    assert result[2] == "c d"  # 개행이 공백으로


def test_sanitize_cells_max_width():
    """셀 너비 제한."""
    long_val = "a" * 100
    result = _sanitize_cells([long_val])
    assert len(result[0]) <= MAX_COL_WIDTH + 1  # '…' 1자 추가
    assert result[0].endswith("…")


def test_is_summary_sheet():
    assert _is_summary_sheet("Summary")
    assert _is_summary_sheet("summary")
    assert _is_summary_sheet("요약")
    assert _is_summary_sheet("Report")
    assert not _is_summary_sheet("영웅_스탯")


def test_deduplicate_headers():
    headers, keep = _deduplicate_headers(["a", "b", "a", "c"])
    assert headers == ["a", "b", "c"]
    assert keep == [0, 1, 3]

    headers, keep = _deduplicate_headers(["a", "b", "c"])
    assert headers == ["a", "b", "c"]
    assert keep == []  # 중복 없음


# ── 직접 실행 ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    test_funcs = [
        (name, obj) for name, obj in globals().items()
        if name.startswith("test_") and callable(obj)
    ]
    passed = 0
    failed = 0
    for name, fn in test_funcs:
        try:
            fn()
            passed += 1
            print(f"  PASS: {name}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL: {name} — {e}")
            traceback.print_exc()
        except Exception as e:
            failed += 1
            print(f"  ERROR: {name} — {e}")
            traceback.print_exc()

    print(f"\n{'=' * 50}")
    print(f"Total: {len(test_funcs)}, Passed: {passed}, Failed: {failed}")
    sys.exit(0 if failed == 0 else 1)
