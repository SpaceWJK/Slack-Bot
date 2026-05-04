"""
reconstructors.py — GDI MCP 청크 재조합 공통 모듈

load_gdi.py와 gdi_client.py에 중복 구현되어 있던 재조합 로직을 통합.
XLSX/PPTX/TSV 형식별로 청크를 사람(및 LLM)이 읽기 좋은 마크다운으로 재구성.

공개 API:
  - reconstruct_body(chunks, source_type) — 파일 형식 디스패처 (주 사용 API)
  - reconstruct_xlsx(chunks)
  - reconstruct_pptx(chunks)
  - reconstruct_tsv(chunks)

task-075 반영:
  - load_gdi.py 고도화 버전을 정본으로 채택 (PC-1)
  - TSV 헤더 중복 체크를 O(n²) → O(n)로 개선 (seen_headers set)
  - MAX_TABLE_ROWS = 20000 (task-109, PC-3 완화)

타입 힌트는 Python 3.9+ 기준. mcp-cache-layer와 Slack Bot 양쪽에서 import.
"""

import re
from collections import OrderedDict


# ── 상수 ────────────────────────────────────────────────────────────────────

# GDI 청크 메타데이터 접두사 제거 패턴
# 각 청크 시작에 반복되는 index_mode/file_type/content_type 3줄 제거
_CHUNK_META_RE = re.compile(
    r"^(?:index_mode|file_type|content_type): .+\n?",
    re.MULTILINE,
)

# PPTX 청크 메타데이터 접두사 패턴
_PPTX_PREFIX_RE = re.compile(
    r"^Mode: generic_pptx > FileType: .+? > ContentType: generic_pptx > Slide: (\d+) > "
)

# PPTX 빈 Notes 섹션 제거 패턴
_PPTX_EMPTY_NOTES_RE = re.compile(r"\n?### Notes:\s*$")

# XLSX 청크 시작 패턴
_XLSX_PREFIX = "Mode: generic_xlsx"

# 시트/테이블당 최대 행 수 (body_text 크기 제한)
MAX_TABLE_ROWS = 20000
MAX_COL_WIDTH = 80   # 셀 값 최대 표시 길이

# task-081: Preview + Overview 상수
PREVIEW_ROWS = 10             # 큰 시트의 미리보기 행 수
PREVIEW_THRESHOLD = 50        # 이 행 수 이상이면 <preview> 섹션 생성

# 카테고리 성격 열 판별 키워드
_CATEGORY_KEYWORDS = {
    '분류', 'category', '구분', 'type', 'component',
    '대분류', '중분류', '소분류', 'group', '항목',
}

# task-080: XML 안전 처리 상수
# XML 1.0에서 허용하지 않는 제어 문자 (탭 0x09, LF 0x0A, CR 0x0D는 보존)
_XML_UNSAFE_CHARS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

# body_text truncation 마커 (MAX_BODY_CHARS 초과 시 load_gdi에서 사용)
TRUNCATION_MARKER = "\n<!-- body truncated at MAX_BODY_CHARS -->"

# 행 수 초과 truncation 마커 (MAX_TABLE_ROWS 초과 시)
def _rows_truncated_marker(shown: int, total: int) -> str:
    """MAX_TABLE_ROWS 초과 시 표시할 주석 마커."""
    return f"<!-- rows truncated: {total - shown} more rows omitted (total {total}) -->"


def _xml_escape(text) -> str:
    """XML 속성값/텍스트 컨텐츠 안전 이스케이프 (task-080).

    - NUL byte + 제어문자 제거 (탭/LF/CR은 보존)
    - & < > " 이스케이프
    """
    if text is None:
        return ""
    s = str(text)
    s = _XML_UNSAFE_CHARS_RE.sub('', s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))


def _xml_attrs(**attrs) -> str:
    """속성 딕셔너리 → `key="value"` 문자열. None 값은 제외."""
    parts = []
    for k, v in attrs.items():
        if v is None:
            continue
        parts.append(f'{k}="{_xml_escape(v)}"')
    return " ".join(parts)


# ── 내부 유틸리티 ─────────────────────────────────────────────────────────

def _clean_chunk_text(text: str) -> str:
    """GDI 청크의 메타데이터 접두사를 제거한다.

    GDI MCP가 반환하는 각 청크에는 아래 3줄이 접두사로 붙는다:
        index_mode: generic_tsv
        file_type: <파일명>
        content_type: generic_tsv
    이 메타데이터는 검색 인덱싱용이며 실제 콘텐츠가 아니므로 제거한다.
    """
    if not text:
        return text
    return _CHUNK_META_RE.sub("", text).strip()


def _sanitize_cells(cells):
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


def _is_summary_sheet(sheet_name: str) -> bool:
    """시트명 기반 Summary/Report 시트 판별."""
    name_lower = sheet_name.lower()
    return any(kw in name_lower for kw in ('summary', 'report', '요약'))


def _deduplicate_headers(headers):
    """중복 헤더 텍스트를 처리하고 유니크한 열 이름을 반환한다.

    task-127 Step 6 시정: 중복 헤더 열을 단순 제거하는 대신 suffix(_2, _3, ...)를
    붙여서 데이터를 보존한다. 예: ['변화율', '변화율'] → ['변화율', '변화율_2'].
    이렇게 하면 기간별 변화율 컬럼처럼 의미있는 중복 컬럼의 데이터 손실을 방지한다.

    Returns:
        (renamed_headers, keep_col_indices)
        keep_col_indices: 변경이 있으면 전체 인덱스 목록, 없으면 [] (호환성 유지)
    """
    seen: dict = {}  # 헤더 값 → 마지막 suffix 번호
    renamed = []
    has_change = False

    for i, h in enumerate(headers):
        if h and h in seen:
            seen[h] += 1
            renamed.append(f"{h}_{seen[h]}")
            has_change = True
        else:
            seen[h] = 1
            renamed.append(h)

    if not has_change:
        return headers, []  # 중복 없음 — 원본 반환, keep_cols=[] (전체 열 유지)

    return renamed, list(range(len(renamed)))


def _fill_down_chunks(headers, rows):
    """카테고리 성격의 열에서 빈 셀을 위 행 값으로 채운다.

    판단: 열 이름 키워드 매칭 + 빈 셀 비율 50% 이상인 열.
    """
    if not rows or not headers:
        return rows

    # 키워드 기반 카테고리 열 감지
    cat_cols = set()
    for h in headers:
        h_lower = h.lower().strip()
        if any(kw in h_lower for kw in _CATEGORY_KEYWORDS):
            cat_cols.add(h)

    # 빈 셀 비율 기반 감지 (50% 이상 빈 + 첫 행 값 존재)
    if len(rows) > 5:
        for h in headers:
            if h in cat_cols:
                continue
            empty_count = sum(1 for r in rows if not r.get(h, ""))
            if empty_count / len(rows) > 0.5:
                first_val = rows[0].get(h, "")
                if first_val:
                    cat_cols.add(h)

    if not cat_cols:
        return rows

    result = []
    prev = {h: "" for h in cat_cols}
    for row in rows:
        new_row = dict(row)
        for h in cat_cols:
            val = new_row.get(h, "")
            if val:
                prev[h] = val
            else:
                new_row[h] = prev[h]
        result.append(new_row)
    return result


def _format_summary_rows(sheet_name, rows, headers, include_md_header: bool = True):
    """Summary 시트를 key:value 형태로 포맷.

    데이터가 적고(행 < 35) key:value 구조인 시트를 읽기 좋게 변환.

    Args:
        include_md_header: True면 `## Sheet: {name}` 마크다운 헤더 포함.
                           False면 제외 (task-080 XML 모드에서 중복 방지).
    """
    parts = []
    if include_md_header:
        parts.append(f"## Sheet: {sheet_name}")
    for row in rows:
        non_empty = [(h, row[h]) for h in headers if row.get(h, "")]
        if not non_empty:
            continue
        if len(non_empty) == 1:
            parts.append(f"\n**{non_empty[0][1]}**")
        elif len(non_empty) == 2:
            parts.append(f"- **{non_empty[0][1]}**: {non_empty[1][1]}")
        else:
            pairs = [f"**{k}**: {v}" for k, v in non_empty]
            parts.append("- " + " / ".join(pairs))
    return "\n".join(parts)


def _parse_xlsx_chunk(text: str):
    """XLSX 청크 1개를 파싱하여 (sheet, row_num, {col: val})를 반환."""
    lines = text.split("\n")
    first_line = lines[0]

    if not first_line.startswith(_XLSX_PREFIX):
        return None, -1, {}

    parts = first_line.split(" > ")

    sheet = ""
    row_num = -1
    data_start = 0

    for i, part in enumerate(parts):
        if part.startswith("Sheet: "):
            sheet = part[7:]
        elif part.startswith("Row: "):
            try:
                row_num = int(part[5:])
            except ValueError:
                pass
            data_start = i + 1
            break

    cols = {}
    data_parts = parts[data_start:]
    for j, part in enumerate(data_parts):
        idx = part.find(": ")
        if idx > 0:
            key = part[:idx].strip()
            val = part[idx + 2:].strip()
            # 마지막 필드에 멀티라인 값이 있으면 합침
            if j == len(data_parts) - 1 and len(lines) > 1:
                extra = " ".join(l.strip() for l in lines[1:] if l.strip())
                if extra:
                    val += " " + extra
            cols[key] = val

    return sheet, row_num, cols


def _buf_to_md_table(rows):
    """탭 분리된 행 리스트를 마크다운 테이블로 변환."""
    if not rows:
        return ""
    max_cols = max(len(r) for r in rows)
    # 열 수 정규화
    for r in rows:
        while len(r) < max_cols:
            r.append("")
    header = _sanitize_cells([c.strip() for c in rows[0]])
    md = ["| " + " | ".join(header) + " |"]
    md.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows[1:]:
        md.append("| " + " | ".join(_sanitize_cells([c.strip() for c in row])) + " |")
    return "\n".join(md)


def _detect_and_convert_table(text: str) -> str:
    """텍스트 내 탭 구분 데이터가 있으면 마크다운 테이블로 변환."""
    lines = text.split("\n")
    tab_lines = [l for l in lines if "\t" in l]
    # 3줄 이상 탭 구분 데이터가 있으면 테이블로 변환
    if len(tab_lines) < 3:
        return text

    result_lines = []
    table_buf = []
    in_table = False

    for line in lines:
        if "\t" in line:
            if not in_table:
                in_table = True
                table_buf = []
            table_buf.append(line.split("\t"))
        else:
            if in_table:
                result_lines.append(_buf_to_md_table(table_buf))
                table_buf = []
                in_table = False
            result_lines.append(line)

    if in_table and table_buf:
        result_lines.append(_buf_to_md_table(table_buf))

    return "\n".join(result_lines)


# ── 공개 API: 파일 형식별 재구성 ───────────────────────────────────────

def reconstruct_xlsx(chunks, file_info: dict = None) -> str:
    """XLSX 청크를 시트별 마크다운 테이블로 재구성.

    고도화 처리:
    - Summary/Report 시트 → key:value 형태 변환
    - 중복 헤더 제거
    - 카테고리 열 fill-down (빈 셀 상위값 채움)
    - pre-header 메타데이터 분리
    - 셀 값 sanitize (길이 제한, 파이프 이스케이프, 빈 셀 공백)

    task-080 (file_info 전달 시 XML 래핑):
    - <file name="..." source_type="generic_xlsx" sheets="N">
    - <sheet name="..." rows="N" columns="N"> / <sheet type="summary">

    task-081 (L2 고도화, XML 모드 전용):
    - <overview><sheet-info name="..." rows="N" columns="N" type="summary|data" truncated="true"/>...</overview>
    - 큰 시트(rows >= PREVIEW_THRESHOLD)에 <preview rows="N">...</preview> 섹션 선행
    """
    sheets = OrderedDict()        # {sheet: [row_dict, ...]}
    sheet_headers = OrderedDict()  # {sheet: [col_names]}
    sheet_header_sets = {}         # {sheet: set(col_names)}  — O(n) 중복 체크용

    for chunk in chunks:
        sheet, row_num, cols = _parse_xlsx_chunk(chunk)
        if sheet is None or not cols:
            continue

        if sheet not in sheets:
            sheets[sheet] = []
            sheet_headers[sheet] = list(cols.keys())
            sheet_header_sets[sheet] = set(cols.keys())
        else:
            # O(n) 중복 체크 (기존 O(n²) 개선)
            header_set = sheet_header_sets[sheet]
            for k in cols.keys():
                if k not in header_set:
                    header_set.add(k)
                    sheet_headers[sheet].append(k)

        sheets[sheet].append(cols)

    if not sheets:
        # 빈 청크: file_info 있으면 self-closing 태그, 아니면 기존 fallback
        if file_info:
            attrs = _xml_attrs(
                name=file_info.get("file_name", "unknown.xlsx"),
                source_type="generic_xlsx",
                sheets=0,
            )
            return f"<file {attrs}/>"
        return "\n".join(chunks)

    xml_mode = file_info is not None
    result = []

    # task-081: 시트 정보 수집 (overview + sheet 출력 일관성)
    # 2-pass 구조: 각 시트의 최종 메타데이터를 먼저 확정한 뒤 overview와 sheet 출력
    sheet_outputs = []  # [(sheet_name, sheet_xml_lines, sheet_info_dict)]

    if xml_mode:
        attrs = _xml_attrs(
            name=file_info.get("file_name", "unknown.xlsx"),
            source_type="generic_xlsx",
            sheets=len(sheets),
        )
        result.append(f"<file {attrs}>")

    for sheet_name, rows in sheets.items():
        headers = sheet_headers[sheet_name]

        # ── Summary/Report 시트 → key:value 포맷 ──
        if _is_summary_sheet(sheet_name) and len(rows) <= 35:
            # XML 모드에서는 include_md_header=False (M-3 반영)
            summary = _format_summary_rows(
                sheet_name, rows, headers,
                include_md_header=not xml_mode,
            )
            if summary:
                if xml_mode:
                    sheet_attrs = _xml_attrs(
                        name=sheet_name, type="summary", rows=len(rows),
                    )
                    # task-081: sheet_outputs에 수집 (overview와 함께 합산)
                    sheet_xml = [
                        f"<sheet {sheet_attrs}>",
                        summary,
                        "</sheet>",
                    ]
                    sheet_outputs.append({
                        "name": sheet_name,
                        "lines": sheet_xml,
                        "info": {"rows": len(rows), "columns": len(headers), "type": "summary"},
                    })
                else:
                    result.append(summary)
                    result.append("")
            continue

        # ── 헤더 중복 제거 ──
        headers, keep_cols = _deduplicate_headers(headers)
        if keep_cols:
            rows = [{h: r.get(h, "") for h in headers} for r in rows]

        # ── pre-header 메타데이터 분리 ──
        data_start = 0
        pre_header_rows = []
        if len(rows) > 3:
            median_cols = sorted(
                len([h for h in headers if r.get(h, "")]) for r in rows
            )[len(rows) // 2]
            for i, r in enumerate(rows):
                filled = len([h for h in headers if r.get(h, "")])
                if filled <= 2 and median_cols > 3:
                    pre_header_rows.append(r)
                    data_start = i + 1
                else:
                    break

        data_rows = rows[data_start:]

        # ── 카테고리 fill-down ──
        if data_rows:
            data_rows = _fill_down_chunks(headers, data_rows)

        # ── 빈 열 제거 ──
        non_empty_headers = [
            h for h in headers
            if any(r.get(h, "") for r in data_rows)
        ]
        if non_empty_headers and len(non_empty_headers) < len(headers):
            headers = non_empty_headers

        # ── 출력 (XML 모드) ──
        if xml_mode:
            sheet_attrs = _xml_attrs(
                name=sheet_name,
                rows=len(data_rows),
                columns=len(headers),
            )
            sheet_xml = [
                f"<sheet {sheet_attrs}>",
                f"<headers>{_xml_escape(', '.join(headers))}</headers>",
            ]

            if pre_header_rows:
                meta_lines = []
                for pr in pre_header_rows:
                    non_empty = [(h, pr[h]) for h in sheet_headers[sheet_name]
                                 if pr.get(h, "")]
                    # task-080 CRITICAL fix: XML body 이스케이프 필요
                    if len(non_empty) == 1:
                        meta_lines.append(f"**{_xml_escape(non_empty[0][1])}**")
                    elif len(non_empty) == 2:
                        meta_lines.append(
                            f"- {_xml_escape(non_empty[0][1])}: "
                            f"{_xml_escape(non_empty[1][1])}"
                        )
                    elif non_empty:
                        meta_lines.append(
                            "- " + " / ".join(_xml_escape(v) for _, v in non_empty)
                        )
                if meta_lines:
                    sheet_xml.append("<meta>")
                    sheet_xml.extend(meta_lines)
                    sheet_xml.append("</meta>")

            # task-080 CRITICAL fix: XML body 내 셀 값 이스케이프
            sanitized_h = [_xml_escape(c) for c in _sanitize_cells(headers)]
            header_row = "| " + " | ".join(sanitized_h) + " |"
            sep_row = "|" + "|".join("---" for _ in sanitized_h) + "|"

            # task-081: 큰 시트는 <preview> 섹션 선행 (LLM 빠른 구조 파악)
            if len(data_rows) >= PREVIEW_THRESHOLD:
                preview_limit = min(PREVIEW_ROWS, len(data_rows))
                sheet_xml.append(f'<preview rows="{preview_limit}">')
                sheet_xml.append(header_row)
                sheet_xml.append(sep_row)
                for i in range(preview_limit):
                    vals = [data_rows[i].get(h, "") for h in headers]
                    escaped_vals = [_xml_escape(c) for c in _sanitize_cells(vals)]
                    sheet_xml.append("| " + " | ".join(escaped_vals) + " |")
                sheet_xml.append("</preview>")

            sheet_xml.append("<data>")
            sheet_xml.append(header_row)
            sheet_xml.append(sep_row)

            truncated_flag = False
            for i, row in enumerate(data_rows):
                if i >= MAX_TABLE_ROWS:
                    sheet_xml.append(_rows_truncated_marker(MAX_TABLE_ROWS, len(data_rows)))
                    truncated_flag = True
                    break
                vals = [row.get(h, "") for h in headers]
                escaped_vals = [_xml_escape(c) for c in _sanitize_cells(vals)]
                sheet_xml.append("| " + " | ".join(escaped_vals) + " |")

            sheet_xml.append("</data>")
            sheet_xml.append("</sheet>")

            sheet_outputs.append({
                "name": sheet_name,
                "lines": sheet_xml,
                "info": {
                    "rows": len(data_rows),
                    "columns": len(headers),
                    "type": "data",
                    "truncated": truncated_flag,
                    "has_preview": len(data_rows) >= PREVIEW_THRESHOLD,
                },
            })
            continue

        # ── 출력 (마크다운 모드, 기존 동작) ──
        result.append(f"## Sheet: {sheet_name}\n")

        if pre_header_rows:
            for pr in pre_header_rows:
                non_empty = [(h, pr[h]) for h in sheet_headers[sheet_name]
                             if pr.get(h, "")]
                if len(non_empty) == 1:
                    result.append(f"**{non_empty[0][1]}**")
                elif len(non_empty) == 2:
                    result.append(f"- {non_empty[0][1]}: {non_empty[1][1]}")
                elif non_empty:
                    result.append("- " + " / ".join(v for _, v in non_empty))
            result.append("")

        sanitized_h = _sanitize_cells(headers)
        result.append("| " + " | ".join(sanitized_h) + " |")
        result.append("|" + "|".join("---" for _ in sanitized_h) + "|")

        for i, row in enumerate(data_rows):
            if i >= MAX_TABLE_ROWS:
                result.append(f"\n_(… 외 {len(data_rows) - MAX_TABLE_ROWS}행 생략)_")
                break
            vals = [row.get(h, "") for h in headers]
            result.append("| " + " | ".join(_sanitize_cells(vals)) + " |")

        result.append("")

    # task-081: XML 모드 최종 조립 (overview 선행 + sheet_outputs 합병)
    if xml_mode:
        # overview 섹션: 시트 간 관계 요약
        overview_lines = ["<overview>"]
        for so in sheet_outputs:
            info = so["info"]
            info_attrs = _xml_attrs(
                name=so["name"],
                rows=info.get("rows", 0),
                columns=info.get("columns", 0),
                type=info.get("type"),
                truncated="true" if info.get("truncated") else None,
                has_preview="true" if info.get("has_preview") else None,
            )
            overview_lines.append(f"<sheet-info {info_attrs}/>")
        overview_lines.append("</overview>")
        result.extend(overview_lines)

        # sheet_outputs 펼쳐서 append
        for so in sheet_outputs:
            result.extend(so["lines"])

        result.append("</file>")

    return "\n".join(result)


def reconstruct_pptx(chunks, file_info: dict = None) -> str:
    """PPTX 청크를 슬라이드별 문서로 정제.

    고도화 처리:
    - 메타데이터 접두사 제거 + 슬라이드 번호 헤더
    - 빈 Notes/슬라이드 제거
    - 이미지 참조 통일 (![](파일명) → [이미지: 파일명])
    - 탭 구분 데이터 → 마크다운 테이블 변환

    task-080 (file_info 전달 시 XML 래핑):
    - <file name="..." source_type="generic_pptx" slides="N">
    - <slide number="N"><content>...</content></slide>
    """
    slide_entries = []  # [(slide_num, content), ...]

    for chunk in chunks:
        m = _PPTX_PREFIX_RE.match(chunk)
        if m:
            slide_num = m.group(1)
            content = chunk[m.end():]
        else:
            content = chunk
            slide_num = None

        # 빈 Notes 섹션 제거
        content = _PPTX_EMPTY_NOTES_RE.sub("", content)
        content = content.strip()

        if not content:
            continue

        # 이미지 참조 통일
        content = re.sub(
            r'!\[\]\(([^)]+)\)',
            lambda m: f'[이미지: {m.group(1)}]',
            content,
        )

        # 탭 구분 데이터 → 마크다운 테이블 변환
        content = _detect_and_convert_table(content)

        slide_entries.append((slide_num, content))

    # 빈 청크 처리
    if not slide_entries:
        if file_info:
            attrs = _xml_attrs(
                name=file_info.get("file_name", "unknown.pptx"),
                source_type="generic_pptx",
                slides=0,
            )
            return f"<file {attrs}/>"
        return ""

    xml_mode = file_info is not None

    if xml_mode:
        parts = []
        attrs = _xml_attrs(
            name=file_info.get("file_name", "unknown.pptx"),
            source_type="generic_pptx",
            slides=len(slide_entries),
        )
        parts.append(f"<file {attrs}>")
        for slide_num, content in slide_entries:
            slide_attrs = _xml_attrs(number=slide_num) if slide_num else ""
            opening = f"<slide {slide_attrs}>" if slide_attrs else "<slide>"
            parts.append(opening)
            parts.append("<content>")
            # task-080 CRITICAL fix: XML body 이스케이프
            # content에 <, >, & 포함 시 XML 구조 파괴 방지
            parts.append(_xml_escape(content))
            parts.append("</content>")
            parts.append("</slide>")
        parts.append("</file>")
        return "\n".join(parts)

    # 마크다운 모드 (기존 동작)
    parts = []
    for slide_num, content in slide_entries:
        if slide_num:
            parts.append(f"## Slide {slide_num}\n{content}")
        else:
            parts.append(content)
    return "\n\n".join(parts)


def reconstruct_tsv(chunks, file_info: dict = None) -> str:
    """TSV 청크를 마크다운 테이블로 재구성.

    고도화 처리:
    - 빈 열 자동 제거
    - 열 수 불일치 패딩
    - 셀 값 sanitize (길이 제한, 이스케이프, 빈 셀 공백)

    성능 (task-075): 헤더 O(n²) → O(n) (seen_headers set).

    task-080 (file_info 전달 시 XML 래핑):
    - <file name="..." source_type="generic_tsv" total_rows="N">
    - <headers>...</headers><data>...</data></file>
    """
    all_rows = []
    all_headers = []
    seen_headers = set()

    for chunk in chunks:
        cleaned = _CHUNK_META_RE.sub("", chunk).strip()
        if not cleaned:
            continue

        cols = {}
        for line in cleaned.split("\n"):
            line = line.strip()
            if not line:
                continue
            idx = line.find(": ")
            if idx > 0:
                key = line[:idx]
                val = line[idx + 2:]
                cols[key] = val
                if key not in seen_headers:
                    seen_headers.add(key)
                    all_headers.append(key)

        if cols:
            all_rows.append(cols)

    if not all_rows or not all_headers:
        if file_info:
            attrs = _xml_attrs(
                name=file_info.get("file_name", "unknown.tsv"),
                source_type="generic_tsv",
                total_rows=0,
            )
            return f"<file {attrs}/>"
        return "\n".join(chunks)

    # 빈 열 제거
    non_empty_headers = [
        h for h in all_headers
        if any(r.get(h, "") for r in all_rows)
    ]
    if non_empty_headers:
        all_headers = non_empty_headers

    xml_mode = file_info is not None
    result = []

    if xml_mode:
        attrs = _xml_attrs(
            name=file_info.get("file_name", "unknown.tsv"),
            source_type="generic_tsv",
            total_rows=len(all_rows),
        )
        result.append(f"<file {attrs}>")
        result.append(f"<headers>{_xml_escape(', '.join(all_headers))}</headers>")
        result.append("<data>")

    # 마크다운 테이블 생성 (XML 모드에서는 셀 값 이스케이프)
    # task-080 CRITICAL fix: XML 모드에서 셀 값에 &<> 포함 시 파싱 실패 방지
    sanitized = _sanitize_cells(all_headers)
    if xml_mode:
        sanitized_h = [_xml_escape(c) for c in sanitized]
    else:
        sanitized_h = sanitized
    result.append("| " + " | ".join(sanitized_h) + " |")
    result.append("|" + "|".join("---" for _ in sanitized_h) + "|")

    for i, row in enumerate(all_rows):
        if i >= MAX_TABLE_ROWS:
            if xml_mode:
                result.append(_rows_truncated_marker(MAX_TABLE_ROWS, len(all_rows)))
            else:
                result.append(f"\n_(… 외 {len(all_rows) - MAX_TABLE_ROWS}행 생략, "
                              f"총 {len(all_rows)}행)_")
            break
        vals = [row.get(h, "") for h in all_headers]
        sanitized_v = _sanitize_cells(vals)
        if xml_mode:
            sanitized_v = [_xml_escape(c) for c in sanitized_v]
        result.append("| " + " | ".join(sanitized_v) + " |")

    if xml_mode:
        result.append("</data>")
        result.append("</file>")

    return "\n".join(result)


def reconstruct_body(chunks, source_type: str, file_info: dict = None) -> str:
    """파일 형식에 맞게 청크를 사람(및 LLM)이 보는 형태로 재구성.

    Args:
        chunks: 원본 청크 문자열 리스트
        source_type: "generic_xlsx", "generic_pptx", "generic_tsv" 중 하나
        file_info: 파일 메타데이터 dict (task-080).
                   전달 시 XML 태그로 래핑하여 LLM 파싱 최적화.
                   키: file_name, file_path, source_type, chunk_count 등.
                   None이면 기존 마크다운 포맷 (하위 호환성).

    Returns:
        재구성된 body_text (XML 또는 마크다운)
    """
    if not chunks:
        return ""
    if source_type == "generic_xlsx":
        return reconstruct_xlsx(chunks, file_info=file_info)
    elif source_type == "generic_pptx":
        return reconstruct_pptx(chunks, file_info=file_info)
    elif source_type == "generic_tsv":
        return reconstruct_tsv(chunks, file_info=file_info)
    # 알 수 없는 형식: 기존 방식 (단순 결합 + 메타 접두사 제거)
    return "\n".join(_clean_chunk_text(c) for c in chunks if c.strip())
