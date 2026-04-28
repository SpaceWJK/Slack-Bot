"""
file_parsers.py — 원본 파일 직접 파싱 고도화 모듈

GDI MCP 청크 텍스트 대신 원본 파일(PPTX, XLSX, TSV, PNG)에서 직접 파싱하여
사람이 보는 형태와 최대한 동일한 마크다운 텍스트를 생성한다.

사용법:
    from file_parsers import parse_file
    result = parse_file("path/to/file.pptx")
    # result = {"body_text": "## Slide 1\n...", "metadata": {...}, "images": [...]}

지원 형식:
    - PPTX: 슬라이드별 텍스트 + 테이블 + 이미지 참조
    - XLSX: 시트별 마크다운 테이블 (병합 셀 fill-down, 빈 행 스킵)
    - TSV:  원본 탭 구분 직접 파싱 → 마크다운 테이블
    - PNG:  메타데이터 + (선택) Vision API 분석

의존성:
    pip install python-pptx openpyxl Pillow pytesseract
    Tesseract OCR 바이너리 별도 설치 필요 (https://github.com/UB-Mannheim/tesseract/wiki)
    미설치 시 PPTX 이미지 OCR 건너뜀 (기존 동작 유지)
"""

import csv
import io
import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────
MAX_TABLE_ROWS = 20000      # 마크다운 테이블 최대 행 수
MAX_BODY_CHARS = 500_000    # body_text 최대 글자 수
MAX_COL_WIDTH = 80          # 셀 값 최대 표시 길이 (넘으면 잘라냄)
IMAGE_DIR_NAME = "_images"  # 이미지 추출 저장 폴더명
_tesseract_available: "bool | None" = None  # None=미확인, True=가용, False=불가


# ═══════════════════════════════════════════════════════════════════════════════
# 통합 진입점
# ═══════════════════════════════════════════════════════════════════════════════

def parse_file(file_path: str, *, extract_images: bool = True,
               image_base_dir: Optional[str] = None) -> dict:
    """원본 파일을 파싱하여 구조화된 결과를 반환한다.

    Args:
        file_path: 원본 파일 경로
        extract_images: PPTX 이미지를 디스크에 추출할지 여부
        image_base_dir: 이미지 저장 기본 경로 (None이면 파일 옆 _images/)

    Returns:
        {
            "body_text": str,      # 마크다운 텍스트
            "metadata": dict,      # 파일 메타데이터
            "images": list[dict],  # 추출된 이미지 정보 (PPTX만)
            "source_type": str,    # generic_pptx / generic_xlsx / generic_tsv / image
        }
    """
    ext = Path(file_path).suffix.lower()
    if ext == ".pptx":
        return parse_pptx(file_path, extract_images=extract_images,
                          image_base_dir=image_base_dir)
    elif ext in (".xlsx", ".xlsm"):
        return parse_xlsx(file_path)
    elif ext == ".tsv":
        return parse_tsv(file_path)
    elif ext in (".docx",):
        return parse_docx(file_path)
    elif ext in (".html", ".htm"):
        return parse_html(file_path, extract_images=extract_images,
                         image_base_dir=image_base_dir)
    elif ext in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"):
        return parse_image(file_path)
    else:
        return {
            "body_text": f"[지원하지 않는 형식: {ext}]",
            "metadata": {"file_path": file_path, "ext": ext},
            "images": [],
            "source_type": "unknown",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# PPTX 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_pptx(file_path: str, *, extract_images: bool = True,
               image_base_dir: Optional[str] = None) -> dict:
    """PPTX 파일을 파싱하여 슬라이드별 마크다운을 생성한다.

    각 슬라이드를 ## Slide N 헤더로 구분하고:
    - 텍스트: 그대로 출력
    - 테이블: 마크다운 테이블로 변환
    - 이미지: [이미지: 파일명, 크기] 참조 + 디스크 저장
    """
    from pptx import Presentation
    from pptx.enum.shapes import MSO_SHAPE_TYPE

    prs = Presentation(file_path)
    file_name = Path(file_path).stem
    parts = []
    images = []
    total_images = 0

    # 이미지 저장 디렉토리
    if extract_images:
        if image_base_dir:
            img_dir = Path(image_base_dir) / IMAGE_DIR_NAME / _safe_filename(file_name)
        else:
            img_dir = Path(file_path).parent / IMAGE_DIR_NAME / _safe_filename(file_name)
    else:
        img_dir = None

    for slide_idx, slide in enumerate(prs.slides, 1):
        slide_parts = []
        slide_parts.append(f"## Slide {slide_idx}")

        # 슬라이드 내 shape을 위치 순서(top→left)로 정렬
        shapes = sorted(slide.shapes, key=lambda s: (s.top or 0, s.left or 0))

        for shape in shapes:
            # ── 텍스트 프레임 ──
            if shape.has_text_frame:
                text = _extract_text_frame(shape.text_frame)
                if text:
                    slide_parts.append(text)

            # ── 테이블 ──
            if shape.has_table:
                table_md = _extract_table(shape.table)
                if table_md:
                    slide_parts.append(table_md)

            # ── 이미지 ──
            if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                total_images += 1
                img_info = _extract_image(shape, slide_idx, total_images,
                                          img_dir, extract_images)
                if img_info:
                    images.append(img_info)
                    size_kb = img_info["size_bytes"] // 1024
                    ref = f'[이미지: {img_info["filename"]}, {size_kb}KB]'
                    slide_parts.append(ref)
                    if img_info.get("saved_path"):
                        ocr_text = _ocr_image(img_info["saved_path"])
                        if ocr_text:
                            slide_parts.append(ocr_text)

            # ── 그룹 셰이프 (내부 텍스트 추출) ──
            if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
                group_text = _extract_group_text(shape)
                if group_text:
                    slide_parts.append(group_text)

        # ── 발표자 노트 (speaker notes) ──
        # 슬라이드 본문에 나타나지 않지만 기획/검토/QA 맥락이 다수 포함됨
        if slide.has_notes_slide:
            try:
                notes_frame = slide.notes_slide.notes_text_frame
                notes_text = _extract_text_frame(notes_frame)
                if notes_text and notes_text.strip():
                    slide_parts.append(f"> **[발표자 노트]** {notes_text}")
            except Exception as e:
                logger.debug("슬라이드 %d 노트 추출 실패: %s", slide_idx, e)

        # 빈 슬라이드 건너뛰기
        content = "\n".join(slide_parts[1:])  # 헤더 제외 내용
        if not content.strip():
            continue

        parts.append("\n".join(slide_parts))

    body_text = "\n\n".join(parts)
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": Path(file_path).name,
            "total_slides": len(prs.slides),
            "total_images": total_images,
            "slide_width": prs.slide_width,
            "slide_height": prs.slide_height,
        },
        "images": images,
        "source_type": "generic_pptx",
    }


def _extract_text_frame(text_frame) -> str:
    """텍스트 프레임에서 문단별 텍스트를 추출한다."""
    lines = []
    for para in text_frame.paragraphs:
        text = para.text.strip()
        if not text:
            continue
        # 글머리 기호 처리
        if para.level and para.level > 0:
            indent = "  " * para.level
            text = f"{indent}- {text}"
        lines.append(text)
    return "\n".join(lines)


def _extract_table(table) -> str:
    """PPTX 테이블을 마크다운 테이블로 변환한다."""
    rows = []
    for r_idx in range(len(table.rows)):
        cells = []
        for c_idx in range(len(table.columns)):
            cell_text = table.cell(r_idx, c_idx).text.strip()
            cell_text = cell_text.replace("|", "\\|").replace("\n", " ")
            if len(cell_text) > MAX_COL_WIDTH:
                cell_text = cell_text[:MAX_COL_WIDTH] + "…"
            cells.append(cell_text)
        rows.append(cells)

    if not rows:
        return ""

    result = []
    # 첫 행 = 헤더
    result.append("| " + " | ".join(rows[0]) + " |")
    result.append("|" + "|".join("---" for _ in rows[0]) + "|")
    for row in rows[1:]:
        result.append("| " + " | ".join(row) + " |")

    return "\n".join(result)


_TESSERACT_FALLBACK_PATHS = (
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
)
_TESSDATA_FALLBACK_PATHS = (
    os.path.expandvars(r"%USERPROFILE%\tessdata"),
    r"C:\Program Files\Tesseract-OCR\tessdata",
    r"C:\Program Files (x86)\Tesseract-OCR\tessdata",
)


def _autodetect_tesseract() -> None:
    """Tesseract 바이너리 + tessdata 디렉터리를 자동 탐지하여 pytesseract/환경변수 설정.

    Windows winget 설치 직후에는 현 프로세스 PATH에 반영이 안 될 수 있으므로,
    표준 설치 경로를 순차 탐색한다. 한국어 언어팩이 `%USERPROFILE%\\tessdata`에
    있는 경우도 지원한다 (관리자 권한 없이 설치 가능한 경로).
    """
    import pytesseract as _pt

    # tesseract 바이너리
    if not getattr(_pt.pytesseract, "_autodetected", False):
        for p in _TESSERACT_FALLBACK_PATHS:
            if os.path.isfile(p):
                _pt.pytesseract.tesseract_cmd = p
                break
        _pt.pytesseract._autodetected = True

    # tessdata 디렉터리 (kor.traineddata 우선 확인)
    if not os.environ.get("TESSDATA_PREFIX"):
        for p in _TESSDATA_FALLBACK_PATHS:
            if os.path.isfile(os.path.join(p, "kor.traineddata")):
                os.environ["TESSDATA_PREFIX"] = p
                break


def _ocr_image(image_path: str, lang: str = "kor+eng") -> str:
    """Tesseract OCR로 이미지에서 텍스트를 추출한다.

    Tesseract 미설치 시 경고 1회 로그 후 빈 문자열 반환 (graceful degradation).
    Windows 표준 설치 경로를 자동 탐지하므로 시스템 PATH 갱신 없이도 동작.
    """
    global _tesseract_available
    if _tesseract_available is False:
        return ""
    try:
        import pytesseract  # 지연 import — 모듈 레벨 ImportError 회피
        from PIL import Image as _PILImage
        if _tesseract_available is None:
            _autodetect_tesseract()
            pytesseract.get_tesseract_version()  # 바이너리 존재 1회 확인
            _tesseract_available = True
        text = pytesseract.image_to_string(_PILImage.open(image_path), lang=lang)
        return text.strip()
    except Exception as e:
        if _tesseract_available is None:
            _tesseract_available = False
            logger.warning("[file_parsers] Tesseract 미설치 — PPTX OCR 건너뜀: %s", e)
        else:
            logger.debug("[file_parsers] OCR 실패 (%s): %s", Path(image_path).name, e)
        return ""


def _extract_image(shape, slide_idx: int, img_seq: int,
                   img_dir: Optional[Path], save: bool) -> Optional[dict]:
    """이미지 셰이프에서 정보를 추출하고 선택적으로 디스크에 저장한다."""
    try:
        img = shape.image
        ext = img.ext or "png"
        blob = img.blob
        content_type = img.content_type
        filename = f"slide{slide_idx:02d}_img{img_seq:03d}.{ext}"

        saved_path = None
        if save and img_dir:
            img_dir.mkdir(parents=True, exist_ok=True)
            save_path = img_dir / filename
            save_path.write_bytes(blob)
            saved_path = str(save_path)

        return {
            "filename": filename,
            "content_type": content_type,
            "size_bytes": len(blob),
            "slide": slide_idx,
            "saved_path": saved_path,
            "width": shape.width,
            "height": shape.height,
        }
    except Exception as e:
        logger.warning("이미지 추출 실패 (slide %d): %s", slide_idx, e)
        return None


def _extract_group_text(group_shape) -> str:
    """그룹 셰이프 내부의 텍스트를 재귀적으로 추출한다."""
    texts = []
    for shape in group_shape.shapes:
        if shape.has_text_frame:
            t = _extract_text_frame(shape.text_frame)
            if t:
                texts.append(t)
        if hasattr(shape, 'shapes'):  # 중첩 그룹
            t = _extract_group_text(shape)
            if t:
                texts.append(t)
    return "\n".join(texts)


# ═══════════════════════════════════════════════════════════════════════════════
# XLSX 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_xlsx(file_path: str) -> dict:
    """XLSX 파일을 시트별 마크다운 테이블로 변환한다.

    핵심 처리:
    - 수직 병합: fill-down (카테고리 열)
    - 수평 병합: 중복 열 제거
    - Summary/Report 시트: key-value 형태로 파싱
    - 헤더 감지: 'ID', '번호', '분류' 등 키워드 기반 + 고유값 비율 판별
    - 빈 스페이서 열/행 자동 제거
    """
    import openpyxl

    wb = openpyxl.load_workbook(file_path, data_only=True)
    file_name = Path(file_path).name
    parts = []
    total_rows = 0

    for sname in wb.sheetnames:
        ws = wb[sname]

        # Summary/Report 시트 → 워크시트에서 직접 key-value 파싱
        if _is_summary_sheet_ws(sname, ws):
            summary_md = _parse_summary_sheet_ws(sname, ws)
            if summary_md:
                parts.append(summary_md)
            continue

        # 수평 병합 범위 수집 (중복 열 제거용)
        h_merges = _collect_horizontal_merges(ws)

        # 원본 셀 값 읽기 (병합 해제 안 함 → 수평 중복 방지)
        raw_rows, use_cols = _read_sheet_raw(ws, h_merges)

        if not raw_rows:
            continue

        # 빈 열 제거 (전체가 빈 열)
        col_count = len(raw_rows[0])
        non_empty_cols = []
        for c in range(col_count):
            if any(r[c] for r in raw_rows):
                non_empty_cols.append(c)

        if not non_empty_cols:
            continue

        # 컬럼 영역 분리 (원본 워크시트 열 기준 3열 이상 갭이면 별도 테이블)
        regions = _split_column_regions(non_empty_cols, use_cols, raw_rows)
        sheet_header_added = False

        for region_idx, region_cols in enumerate(regions):
            filtered_rows = [[row[c] for c in region_cols] for row in raw_rows]

            # 모든 행이 비어있으면 건너뛰기
            if not any(any(v for v in r) for r in filtered_rows):
                continue

            # 영역 내 빈 열 제거 (대부분 비어있는 열)
            region_col_count = len(region_cols)
            keep_in_region = []
            for rc in range(region_col_count):
                if any(row[rc] for row in filtered_rows):
                    keep_in_region.append(rc)
            if keep_in_region and len(keep_in_region) < region_col_count:
                filtered_rows = [[row[rc] for rc in keep_in_region]
                                 for row in filtered_rows]

            # 테이블 헤더 행 감지
            header_idx = _detect_header_row(filtered_rows)
            if header_idx is None:
                header_idx = 0

            # 헤더 전 행은 메타데이터로 추출
            pre_header = filtered_rows[:header_idx]
            headers = filtered_rows[header_idx]
            data_rows = filtered_rows[header_idx + 1:]

            # 중복 헤더 제거 (수평 병합으로 같은 텍스트가 여러 열에)
            headers, keep_cols = _deduplicate_headers(headers)
            if keep_cols:
                data_rows = [[r[c] for c in keep_cols] for r in data_rows]

            # 빈 행 건너뛰기
            data_rows = [r for r in data_rows if any(v for v in r)]

            if not data_rows and not any(v for v in headers):
                continue

            # 카테고리 fill-down (빈 셀을 위 행 값으로 채움)
            if data_rows:
                data_rows = _fill_down_categories(headers, data_rows)

            # 시트 헤더 출력 (첫 영역에서만)
            total_rows += len(data_rows)
            if not sheet_header_added:
                parts.append(f"## Sheet: {sname}")
                sheet_header_added = True
            elif region_idx > 0:
                parts.append("")  # 영역 구분

            # 헤더 전 요약 정보 (있으면)
            meta_text = _format_pre_header(pre_header)
            if meta_text:
                parts.append(meta_text)
                parts.append("")

            # 마크다운 테이블
            if data_rows or any(v for v in headers):
                sanitized_headers = _sanitize_cells(headers)
                parts.append("| " + " | ".join(sanitized_headers) + " |")
                parts.append("|" + "|".join("---" for _ in sanitized_headers) + "|")

                for i, row in enumerate(data_rows):
                    if i >= MAX_TABLE_ROWS:
                        parts.append(
                            f"\n_(… 외 {len(data_rows) - MAX_TABLE_ROWS}행 생략)_"
                        )
                        break
                    # 열 수 맞추기
                    while len(row) < len(headers):
                        row.append("")
                    parts.append(
                        "| " + " | ".join(_sanitize_cells(row[:len(headers)])) + " |"
                    )

        parts.append("")

    body_text = "\n".join(parts)
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": file_name,
            "sheet_names": wb.sheetnames,
            "total_sheets": len(wb.sheetnames),
            "total_data_rows": total_rows,
        },
        "images": [],
        "source_type": "generic_xlsx",
    }


def _collect_horizontal_merges(ws) -> dict:
    """수평 병합 범위를 {(row, col): (min_row, min_col)} 매핑으로 수집.

    수평 병합: 같은 행 내 여러 열이 하나의 값을 공유.
    수직 병합은 별도로 fill-down 처리하므로 여기서는 수평만.
    """
    h_merge_cells = {}  # (row, col) → (min_row, min_col)  (첫 셀 제외)
    for merged_range in ws.merged_cells.ranges:
        if merged_range.min_row == merged_range.max_row:
            # 수평 병합 — 첫 열 외 나머지를 "병합됨"으로 표시
            for col in range(merged_range.min_col + 1, merged_range.max_col + 1):
                h_merge_cells[(merged_range.min_row, col)] = (
                    merged_range.min_row, merged_range.min_col
                )
    return h_merge_cells


def _build_vertical_merge_map(ws) -> dict:
    """수직 병합 범위를 (row, col) → (min_row, min_col) 매핑으로 변환.

    수직 병합: 여러 행이 같은 열에서 하나의 값을 공유 (카테고리 fill-down).
    """
    v_merge_map = {}
    for merged_range in ws.merged_cells.ranges:
        if merged_range.max_row > merged_range.min_row:
            min_row = merged_range.min_row
            min_col = merged_range.min_col
            for row in range(merged_range.min_row + 1, merged_range.max_row + 1):
                for col in range(merged_range.min_col, merged_range.max_col + 1):
                    v_merge_map[(row, col)] = (min_row, min_col)
    return v_merge_map


def _read_sheet_raw(ws, h_merges: dict) -> tuple[list[list[str]], list[int]]:
    """시트의 원본 셀 값을 읽되, 수평 병합 셀은 건너뛴다.

    수직 병합은 첫 셀 값을 유지 (fill-down은 나중에 처리).

    Returns:
        (raw_rows, use_cols) — use_cols는 원본 워크시트 열 번호 리스트
    """
    v_merge_map = _build_vertical_merge_map(ws)

    # 수평 병합으로 인한 중복 열 번호 수집
    h_merged_cols_per_row = {}
    for (r, c) in h_merges:
        h_merged_cols_per_row.setdefault(r, set()).add(c)

    # 전체 열 중 수평 병합으로 항상 숨겨야 할 열 판별
    # (헤더 행에서 수평 병합된 열은 전체 데이터에서도 제거)
    all_skip_cols = set()
    for (r, c) in h_merges:
        all_skip_cols.add(c)

    # 실제로 사용할 열 목록 결정
    all_cols = list(range(1, ws.max_column + 1))
    # 수평 병합 열이 전체의 대부분이면 제거, 아니면 유지
    # → 보수적: 2개 이상의 행에서 병합된 열만 제거
    col_merge_count = {}
    for (r, c) in h_merges:
        col_merge_count[c] = col_merge_count.get(c, 0) + 1
    skip_cols = {c for c, cnt in col_merge_count.items() if cnt >= 2}

    use_cols = [c for c in all_cols if c not in skip_cols]

    raw_rows = []
    for r_idx in range(1, ws.max_row + 1):
        row_vals = []
        for c_idx in use_cols:
            # 수직 병합 → 첫 셀 값 참조
            actual_r, actual_c = v_merge_map.get((r_idx, c_idx), (r_idx, c_idx))
            cell = ws.cell(row=actual_r, column=actual_c)
            val = cell.value
            if val is None:
                val = ""
            else:
                val = str(val).strip()
                if val == "\u3000":
                    val = ""
            row_vals.append(val)
        raw_rows.append(row_vals)

    return raw_rows, use_cols


def _is_summary_sheet_ws(name: str, ws) -> bool:
    """Summary/Report 형태의 시트인지 판별한다 (워크시트 직접 검사).

    판단 기준:
    - 시트 이름에 'summary', 'report', '요약' 포함
    - 또는: 행 수 < 35이고, 병합 셀이 많고 데이터가 sparse
    """
    name_lower = name.lower()
    if any(kw in name_lower for kw in ('summary', 'report', '요약')):
        return True
    if ws.max_row and ws.max_row <= 35:
        merge_count = len(list(ws.merged_cells.ranges))
        if merge_count > ws.max_row * 0.5:
            return True
    return False


def _parse_summary_sheet_ws(name: str, ws) -> str:
    """Summary 시트를 워크시트에서 직접 key-value 마크다운으로 변환한다.

    QA Report Summary 시트 구조:
    - 섹션 제목 (QA Report, Test Imformation, 테스트 기종, Final Result)
    - key: value 쌍 (제목, 담당자, Client 버전 등)
    - 테이블형 데이터 (OS/Device/OS ver)
    """
    parts = [f"## Sheet: {name}"]

    # 수직 병합 맵 생성 (카테고리 fill을 위해)
    v_merge_map = _build_vertical_merge_map(ws)

    # 테이블 영역 감지 (OS | Device | OS ver 같은 소테이블)
    table_start = None
    table_headers = []

    for r in range(1, ws.max_row + 1):
        cells = []
        for c in range(1, ws.max_column + 1):
            # 수직 병합 → 첫 셀 값 참조
            actual_r, actual_c = v_merge_map.get((r, c), (r, c))
            val = ws.cell(row=actual_r, column=actual_c).value
            if val is not None:
                val = str(val).strip().replace("\n", " ")
                if val and val != "\u3000":
                    cells.append((c, val))

        if not cells:
            continue

        # 테이블 모드 진행 중
        if table_start is not None:
            if cells:
                # 1개 값만 있을 때: 수직 병합 fill인지 실제 값인지 구분
                if len(cells) == 1:
                    single_col = cells[0][0]
                    single_val = cells[0][1]
                    # 수직 병합에서 채워진 값이면 → 아티팩트, 건너뛰기
                    actual_r, _ = v_merge_map.get((r, single_col), (r, single_col))
                    if actual_r != r:
                        continue
                    # 실제 셀 값이면 → 섹션 헤더, 테이블 종료
                    table_start = None
                    parts.append("")
                    parts.append(f"\n**{single_val}**")
                    continue

                row_vals = {}
                for col, val in cells:
                    best_h = None
                    best_dist = 999
                    for h_col, h_name in table_headers:
                        dist = abs(col - h_col)
                        if dist < best_dist:
                            best_dist = dist
                            best_h = h_name
                    if best_h and best_dist <= 2:
                        row_vals[best_h] = val

                if row_vals and any(v.strip() for v in row_vals.values()):
                    line = " | ".join(
                        f"{row_vals.get(h, '')}" for _, h in table_headers
                    )
                    parts.append(f"| {line} |")
                else:
                    table_start = None
                    parts.append("")
            continue

        # 1개 값 → 섹션 제목 또는 키워드
        if len(cells) == 1:
            parts.append(f"\n**{cells[0][1]}**")
            continue

        # 2개 값 → key: value
        if len(cells) == 2:
            parts.append(f"- **{cells[0][1]}**: {cells[1][1]}")
            continue

        # 3개 이상 → 소테이블 헤더 or 여러 key:value 쌍
        # 소테이블 감지: 모든 값이 짧은 텍스트(레이블)인 경우
        vals = [v for _, v in cells]
        all_labels = all(len(v) <= 15 and not re.match(r'^[\d,.]+$', v) for v in vals)

        if all_labels and len(cells) >= 3:
            # 소테이블 시작
            table_start = r
            table_headers = cells
            h_line = " | ".join(h for _, h in table_headers)
            sep = " | ".join("---" for _ in table_headers)
            parts.append(f"\n| {h_line} |")
            parts.append(f"| {sep} |")
            continue

        # key:value 쌍 묶기
        pairs = []
        i = 0
        while i < len(cells):
            col, val = cells[i]
            if i + 1 < len(cells):
                next_col, next_val = cells[i + 1]
                if next_col - col <= 2:
                    pairs.append(f"**{val}**: {next_val}")
                    i += 2
                    continue
            pairs.append(val)
            i += 1
        parts.append("- " + " / ".join(pairs))

    return "\n".join(parts)


def _split_column_regions(non_empty_cols: list[int],
                          use_cols: list[int] | None = None,
                          raw_rows: list[list[str]] | None = None,
                          gap_threshold: int = 3,
                          min_region_cols: int = 3) -> list[list[int]]:
    """비어있지 않은 열 인덱스(raw_rows 기준)를 원본 워크시트 열 갭 기준으로 분리한다.

    Args:
        non_empty_cols: raw_rows 내 데이터가 있는 열 인덱스 (0-based)
        use_cols: raw_rows 열 → 원본 워크시트 열 매핑 (1-based)
        raw_rows: 원본 행 데이터 (소규모 영역 병합 판단용)
        gap_threshold: 원본 워크시트 열 번호 기준 이 값 이상 갭이면 분리
        min_region_cols: 이보다 적은 열의 영역은 인접 영역에 병합

    Returns:
        [[raw_col_indices], ...] — 분리된 영역별 raw 열 인덱스 리스트
    """
    if not non_empty_cols:
        return []

    regions = [[non_empty_cols[0]]]
    for i in range(1, len(non_empty_cols)):
        cur_raw = non_empty_cols[i]
        prev_raw = non_empty_cols[i - 1]
        if use_cols:
            ws_gap = use_cols[cur_raw] - use_cols[prev_raw]
        else:
            ws_gap = cur_raw - prev_raw
        if ws_gap >= gap_threshold:
            regions.append([])
        regions[-1].append(cur_raw)

    # 영역이 1개면 분리 불필요
    if len(regions) <= 1:
        return regions

    # 소규모 영역 병합: 데이터가 거의 없는 영역만 다음 영역에 합침
    if raw_rows:
        merged = []
        pending_small = []
        for reg in regions:
            # 영역 내 비어있지 않은 행 수 계산
            nonempty_rows = sum(
                1 for row in raw_rows
                if any(row[c] for c in reg)
            )
            if nonempty_rows < 3 and len(reg) < min_region_cols:
                pending_small.append(reg)
            else:
                if pending_small:
                    for small in pending_small:
                        reg = small + reg
                    pending_small = []
                merged.append(reg)
        if pending_small:
            if merged:
                for small in pending_small:
                    merged[-1].extend(small)
            else:
                merged = [non_empty_cols]
        return merged

    return regions


def _deduplicate_headers(headers: list[str]) -> tuple[list[str], list[int]]:
    """중복 헤더 텍스트를 제거하고 유니크한 열만 남긴다.

    Returns:
        (deduplicated_headers, keep_col_indices)
    """
    seen = {}
    keep_cols = []
    deduped = []

    for i, h in enumerate(headers):
        if h and h in seen:
            continue  # 중복 → 건너뛰기
        seen[h] = i
        keep_cols.append(i)
        deduped.append(h)

    if len(deduped) == len(headers):
        return headers, []  # 중복 없음

    return deduped, keep_cols


def _format_pre_header(pre_header: list[list[str]]) -> str:
    """헤더 전 메타데이터 행을 요약 텍스트로 변환한다."""
    lines = []
    for row in pre_header:
        non_empty = [v for v in row if v]
        if not non_empty:
            continue
        if len(non_empty) == 1:
            lines.append(f"**{non_empty[0]}**")
        elif len(non_empty) == 2:
            lines.append(f"- {non_empty[0]}: {non_empty[1]}")
        elif len(non_empty) <= 4:
            lines.append("- " + " / ".join(non_empty))
    return "\n".join(lines)


def _detect_header_row(rows: list[list[str]]) -> Optional[int]:
    """테이블 헤더 행을 감지한다.

    우선순위:
    1. 'ID', '번호', 'No' 등 테이블 헤더 키워드가 포함된 행
    2. 3개 이상 고유 값이 있고, 모두 숫자가 아닌 첫 행
    """
    HEADER_KEYWORDS = {'id', '번호', 'no', 'no.', '항목', '분류', 'category',
                       'name', 'type', 'status', '결과', '내용', 'component',
                       '구분', 'description', 'title', 'device', 'os'}

    # 1차: 키워드 매칭
    for i, row in enumerate(rows):
        non_empty = [v for v in row if v]
        if len(non_empty) >= 3:
            lower_vals = {v.lower().strip() for v in non_empty}
            if lower_vals & HEADER_KEYWORDS:
                return i

    # 2차: 3개 이상 비숫자 고유값
    for i, row in enumerate(rows):
        non_empty = [v for v in row if v]
        if len(non_empty) >= 3:
            all_numeric = all(
                re.match(r'^-?[\d,.]+%?$', v) for v in non_empty
            )
            if not all_numeric:
                unique_ratio = len(set(non_empty)) / len(non_empty)
                if unique_ratio >= 0.5:  # 절반 이상 고유값
                    return i

    return None


def _fill_down_categories(headers: list[str], rows: list[list[str]]) -> list[list[str]]:
    """분류/카테고리 성격의 열에서 빈 셀을 위 행 값으로 채운다.

    판단 기준: 열 이름에 '분류', 'category', '구분', 'type', 'Component' 등이 포함되거나,
    해당 열의 빈 셀 비율이 50% 이상이고 첫 데이터가 비어있지 않은 경우.
    """
    CATEGORY_KEYWORDS = {'분류', 'category', '구분', 'type', 'component',
                         '대분류', '중분류', '소분류', 'group', '항목'}

    category_cols = set()
    for c_idx, header in enumerate(headers):
        h_lower = header.lower().strip()
        if any(kw in h_lower for kw in CATEGORY_KEYWORDS):
            category_cols.add(c_idx)

    # 빈 셀 비율 기반 감지 (50% 이상 빈 셀 + 첫 행에 값 있음)
    if rows:
        for c_idx in range(len(headers)):
            if c_idx in category_cols:
                continue
            empty_count = sum(1 for r in rows if not r[c_idx])
            total = len(rows)
            if total > 5 and empty_count / total > 0.5:
                # 첫 값이 있고, 값이 반복적 패턴인 경우
                first_val = rows[0][c_idx] if rows[0][c_idx] else None
                if first_val:
                    category_cols.add(c_idx)

    if not category_cols:
        return rows

    # fill-down 실행
    result = []
    prev_vals = [""] * len(headers)
    for row in rows:
        new_row = list(row)
        for c_idx in category_cols:
            if c_idx < len(new_row):
                if new_row[c_idx]:
                    prev_vals[c_idx] = new_row[c_idx]
                else:
                    new_row[c_idx] = prev_vals[c_idx]
        result.append(new_row)

    return result


def _sanitize_cells(cells: list[str]) -> list[str]:
    """셀 값을 마크다운 테이블에 안전하게 넣을 수 있도록 정리한다."""
    result = []
    for v in cells:
        v = v.replace("|", "\\|").replace("\n", " ").replace("\r", "")
        if len(v) > MAX_COL_WIDTH:
            v = v[:MAX_COL_WIDTH] + "…"
        if not v:
            v = " "  # 빈 셀은 공백으로 (테이블 깨짐 방지)
        result.append(v)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# TSV 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_tsv(file_path: str) -> dict:
    """TSV 파일을 마크다운 테이블로 변환한다.

    헤더(첫 행) + 데이터 행을 마크다운 테이블로 출력.
    큰 파일은 MAX_TABLE_ROWS로 잘라낸다.
    """
    file_name = Path(file_path).name
    rows = []
    headers = []
    encoding = _detect_encoding(file_path)

    try:
        # 대용량 TSV 필드 지원 (스토리/대사 등 긴 텍스트)
        csv.field_size_limit(10 * 1024 * 1024)  # 10MB
        with open(file_path, "r", encoding=encoding, errors="replace") as f:
            # BOM 제거
            reader = csv.reader(f, delimiter="\t")
            for i, row in enumerate(reader):
                # 빈 행 건너뛰기
                if not any(cell.strip() for cell in row):
                    continue
                if i == 0:
                    headers = [h.strip() for h in row]
                else:
                    rows.append([cell.strip() for cell in row])
    except Exception as e:
        logger.error("TSV 파일 읽기 실패: %s — %s", file_path, e)
        return {
            "body_text": f"[TSV 파일 읽기 실패: {e}]",
            "metadata": {"file_path": file_path, "error": str(e)},
            "images": [],
            "source_type": "generic_tsv",
        }

    if not headers:
        return {
            "body_text": "[TSV 파일 비어있음]",
            "metadata": {"file_path": file_path},
            "images": [],
            "source_type": "generic_tsv",
        }

    # 빈 열 제거
    non_empty_cols = []
    for c in range(len(headers)):
        if headers[c] or any(c < len(r) and r[c] for r in rows):
            non_empty_cols.append(c)

    if non_empty_cols:
        headers = [headers[c] for c in non_empty_cols]
        rows = [[r[c] if c < len(r) else "" for c in non_empty_cols] for r in rows]

    # 마크다운 테이블 생성
    parts = []
    sanitized_h = _sanitize_cells(headers)
    parts.append("| " + " | ".join(sanitized_h) + " |")
    parts.append("|" + "|".join("---" for _ in sanitized_h) + "|")

    truncated = False
    for i, row in enumerate(rows):
        if i >= MAX_TABLE_ROWS:
            truncated = True
            break
        # 열 수 맞추기
        while len(row) < len(headers):
            row.append("")
        parts.append("| " + " | ".join(_sanitize_cells(row[:len(headers)])) + " |")

    if truncated:
        parts.append(f"\n_(… 외 {len(rows) - MAX_TABLE_ROWS}행 생략, 총 {len(rows)}행)_")

    body_text = "\n".join(parts)
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": file_name,
            "total_rows": len(rows),
            "total_columns": len(headers),
            "headers": headers,
        },
        "images": [],
        "source_type": "generic_tsv",
    }


def _detect_encoding(file_path: str) -> str:
    """파일 인코딩을 감지한다 (BOM 기반 + fallback)."""
    with open(file_path, "rb") as f:
        raw = f.read(4)
    if raw[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    return "utf-8"


# ═══════════════════════════════════════════════════════════════════════════════
# PNG/이미지 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_image(file_path: str) -> dict:
    """이미지 파일의 메타데이터를 추출한다.

    Vision API 연동은 별도 호출 (여기서는 메타데이터만).
    """
    file_name = Path(file_path).name
    file_size = os.path.getsize(file_path)

    width, height = 0, 0
    img_format = Path(file_path).suffix.lstrip(".")

    try:
        from PIL import Image
        with Image.open(file_path) as img:
            width, height = img.size
            img_format = img.format or img_format
    except ImportError:
        logger.info("Pillow 미설치 — 이미지 크기 추출 불가")
    except Exception as e:
        logger.warning("이미지 메타 추출 실패: %s — %s", file_path, e)

    size_kb = file_size // 1024
    body_text = (
        f"## 이미지: {file_name}\n"
        f"- 형식: {img_format.upper()}\n"
        f"- 크기: {width}x{height} px\n"
        f"- 파일 크기: {size_kb} KB\n"
        f"- 경로: {file_path}\n"
    )

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": file_name,
            "width": width,
            "height": height,
            "format": img_format,
            "file_size_bytes": file_size,
        },
        "images": [{"filename": file_name, "saved_path": file_path,
                     "size_bytes": file_size, "content_type": f"image/{img_format.lower()}"}],
        "source_type": "image",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DOCX 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_docx(file_path: str) -> dict:
    """DOCX 파일을 마크다운 텍스트로 변환한다.

    헤딩 스타일은 마크다운 헤딩(##)으로, 테이블은 마크다운 테이블로 변환.
    이미지는 참조 텍스트만 남긴다.
    """
    try:
        from docx import Document
    except ImportError:
        return {
            "body_text": "[python-docx 미설치 — DOCX 파싱 불가]",
            "metadata": {"file_path": file_path},
            "images": [],
            "source_type": "generic_docx",
        }

    doc = Document(file_path)
    parts: list[str] = []
    file_name = Path(file_path).name

    # 헤딩 스타일 → 마크다운 레벨 매핑
    heading_map = {
        "Heading 1": "#", "Heading 2": "##", "Heading 3": "###",
        "Heading 4": "####", "Heading 5": "#####", "Heading 6": "######",
        "Title": "#", "Subtitle": "##",
    }

    for element in doc.element.body:
        tag = element.tag.split("}")[-1]  # namespace 제거

        if tag == "p":
            # 단락 처리
            from docx.text.paragraph import Paragraph
            para = Paragraph(element, doc)
            text = para.text.strip()
            if not text:
                continue

            style_name = para.style.name if para.style else ""
            prefix = heading_map.get(style_name, "")
            if prefix:
                parts.append(f"{prefix} {text}")
            else:
                # 불릿/번호 리스트 감지
                numPr = element.find(
                    ".//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}numPr"
                )
                if numPr is not None:
                    parts.append(f"- {text}")
                else:
                    parts.append(text)

        elif tag == "tbl":
            # 테이블 처리
            from docx.table import Table
            table = Table(element, doc)
            rows_data = []
            for row in table.rows:
                cells = [cell.text.replace("|", "\\|").replace("\n", " ").strip()
                         for cell in row.cells]
                rows_data.append(cells)

            if rows_data:
                # 첫 행을 헤더로
                header = rows_data[0]
                parts.append("| " + " | ".join(h or " " for h in header) + " |")
                parts.append("|" + "|".join("---" for _ in header) + "|")
                for row_cells in rows_data[1:]:
                    # 열 수 맞추기
                    while len(row_cells) < len(header):
                        row_cells.append(" ")
                    parts.append(
                        "| " + " | ".join(c or " " for c in row_cells[:len(header)]) + " |"
                    )

    body_text = "\n\n".join(parts)
    # NBSP 등 특수 공백 정리
    body_text = body_text.replace("\xa0", " ").replace("\u200b", "")
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": file_name,
            "total_paragraphs": len(doc.paragraphs),
            "total_tables": len(doc.tables),
        },
        "images": [],
        "source_type": "generic_docx",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# HTML 파서
# ═══════════════════════════════════════════════════════════════════════════════

def parse_html(file_path: str, *, extract_images: bool = True,
               image_base_dir: Optional[str] = None) -> dict:
    """HTML 파일을 마크다운 텍스트로 변환한다.

    맥락 보존 우선순위:
      1) 헤딩 구조 (h1~h6) → 마크다운 헤딩
      2) 문단/리스트/표 → 마크다운
      3) <img alt/title> 텍스트 (캡션 역할) 보존
      4) <figure>/<figcaption> 보존
      5) base64 인라인 이미지 OCR (Tesseract 가용 시)
      6) <script>/<style>/<nav>/<footer> 제거

    외부 src URL 이미지는 다운로드하지 않음 (보안/속도). alt/title만 유지.
    """
    try:
        from bs4 import BeautifulSoup, NavigableString
    except ImportError:
        return {
            "body_text": "[BeautifulSoup 미설치 — HTML 파싱 불가]",
            "metadata": {"file_path": file_path},
            "images": [],
            "source_type": "generic_html",
        }

    import base64
    import binascii

    # 인코딩 자동 감지 (UTF-8 우선, 실패 시 CP949)
    raw_bytes = Path(file_path).read_bytes()
    for enc in ("utf-8", "cp949", "euc-kr", "latin-1"):
        try:
            html = raw_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        html = raw_bytes.decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    # 1) 불필요 태그 제거
    for tag_name in ("script", "style", "nav", "footer", "noscript", "template"):
        for t in soup.find_all(tag_name):
            t.decompose()

    file_name = Path(file_path).name
    parts: list[str] = []
    images_info: list[dict] = []
    total_images = 0
    ocr_count = 0

    # 이미지 저장 디렉터리 준비 (base64 추출용)
    if extract_images:
        if image_base_dir:
            img_dir = Path(image_base_dir) / IMAGE_DIR_NAME / _safe_filename(Path(file_path).stem)
        else:
            img_dir = Path(file_path).parent / IMAGE_DIR_NAME / _safe_filename(Path(file_path).stem)
    else:
        img_dir = None

    # 제목 보존
    title_tag = soup.find("title")
    if title_tag and title_tag.get_text(strip=True):
        parts.append(f"# {title_tag.get_text(strip=True)}")

    body = soup.body or soup

    def _flatten_text(node) -> str:
        """블록 내 인라인 텍스트를 공백 연결하여 반환."""
        return " ".join(node.get_text(separator=" ", strip=True).split())

    def _render_table(table_tag) -> str:
        rows: list[list[str]] = []
        for tr in table_tag.find_all("tr"):
            cells = [
                c.get_text(separator=" ", strip=True).replace("|", "\\|")
                for c in tr.find_all(["th", "td"])
            ]
            if cells:
                rows.append(cells)
        if not rows:
            return ""
        # 첫 행 헤더 가정
        out = ["| " + " | ".join(rows[0]) + " |",
               "|" + "|".join("---" for _ in rows[0]) + "|"]
        for row in rows[1:]:
            # 열 수 맞춤 (rowspan/colspan 정확 처리는 생략 — 채우기)
            pad = len(rows[0]) - len(row)
            if pad > 0:
                row = row + [""] * pad
            elif pad < 0:
                row = row[:len(rows[0])]
            out.append("| " + " | ".join(row) + " |")
        return "\n".join(out)

    def _handle_image(img_tag) -> Optional[str]:
        """img 태그에서 맥락 정보 추출. alt/title 우선, base64는 OCR 시도."""
        nonlocal total_images, ocr_count
        total_images += 1
        alt = (img_tag.get("alt") or "").strip()
        title = (img_tag.get("title") or "").strip()
        src = (img_tag.get("src") or "").strip()

        ref_bits = []
        if alt:
            ref_bits.append(f"alt={alt}")
        if title and title != alt:
            ref_bits.append(f"title={title}")

        # base64 인라인 이미지 처리
        ocr_text = ""
        if src.startswith("data:image/") and "base64," in src and img_dir:
            try:
                header, b64 = src.split("base64,", 1)
                mime = header[5:].split(";")[0]  # e.g. "image/png"
                ext = mime.split("/")[-1] or "png"
                img_bytes = base64.b64decode(b64)
                img_dir.mkdir(parents=True, exist_ok=True)
                fname = f"html_img{total_images:03d}.{ext}"
                save_path = img_dir / fname
                save_path.write_bytes(img_bytes)
                images_info.append({
                    "filename": fname,
                    "content_type": mime,
                    "size_bytes": len(img_bytes),
                    "saved_path": str(save_path),
                })
                # OCR
                if extract_images:
                    ocr_text = _ocr_image(str(save_path))
                    if ocr_text:
                        ocr_count += 1
            except (binascii.Error, ValueError) as e:
                logger.debug("base64 이미지 디코드 실패: %s", e)

        if not ref_bits and not ocr_text:
            return None

        label = ", ".join(ref_bits) if ref_bits else src[:40] + "…"
        out = f"[이미지: {label}]"
        if ocr_text:
            out += f"\n> OCR: {ocr_text.strip()}"
        return out

    def _walk(node, depth: int = 0):
        """DOM을 순회하며 parts에 마크다운을 추가한다."""
        if isinstance(node, NavigableString):
            text = str(node).strip()
            if text:
                parts.append(text)
            return

        if node.name is None:
            return

        name = node.name.lower()

        if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(name[1])
            text = _flatten_text(node)
            if text:
                parts.append(f"{'#' * level} {text}")
            return

        if name == "p":
            text = _flatten_text(node)
            if text:
                parts.append(text)
            return

        if name in ("ul", "ol"):
            for li in node.find_all("li", recursive=False):
                t = _flatten_text(li)
                if t:
                    bullet = "-" if name == "ul" else "1."
                    parts.append(f"{bullet} {t}")
            return

        if name == "table":
            md = _render_table(node)
            if md:
                parts.append(md)
            return

        if name == "img":
            out = _handle_image(node)
            if out:
                parts.append(out)
            return

        if name == "figure":
            # figure 안의 img 먼저 처리 (figcaption은 별도로)
            for child in node.children:
                if hasattr(child, "name") and child.name == "figcaption":
                    continue
                _walk(child, depth + 1)
            caption = node.find("figcaption")
            if caption:
                cap_text = _flatten_text(caption)
                if cap_text:
                    parts.append(f"*Caption: {cap_text}*")
            return

        if name == "pre":
            # 코드 블록
            code_text = node.get_text()
            if code_text.strip():
                parts.append("```\n" + code_text.rstrip() + "\n```")
            return

        if name == "blockquote":
            text = _flatten_text(node)
            if text:
                parts.append(f"> {text}")
            return

        if name in ("br", "hr"):
            parts.append("")
            return

        # 기본: 자식 노드 재귀
        for child in node.children:
            _walk(child, depth + 1)

    _walk(body)

    body_text = "\n\n".join(p for p in parts if p.strip())
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

    return {
        "body_text": body_text,
        "metadata": {
            "file_path": file_path,
            "file_name": file_name,
            "total_images": total_images,
            "ocr_hits": ocr_count,
            "title": title_tag.get_text(strip=True) if title_tag else "",
        },
        "images": images_info,
        "source_type": "generic_html",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 유틸리티
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_filename(name: str) -> str:
    """파일명에서 위험한 문자를 제거한다."""
    return re.sub(r'[<>:"/\\|?*]', "_", name)


# ── CLI 테스트 ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python file_parsers.py <file_path>")
        sys.exit(1)

    path = sys.argv[1]
    result = parse_file(path, extract_images=False)

    print(f"\n{'='*60}")
    print(f"source_type: {result['source_type']}")
    print(f"metadata: {result['metadata']}")
    print(f"images: {len(result['images'])}개")
    print(f"body_text 길이: {len(result['body_text'])} chars")
    print(f"{'='*60}")
    print(result["body_text"][:3000])
    if len(result["body_text"]) > 3000:
        print(f"\n... (총 {len(result['body_text'])} chars)")
