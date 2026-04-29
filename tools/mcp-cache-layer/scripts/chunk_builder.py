"""
chunk_builder.py — doc_chunks + chunks_fts 청킹 CLI (task-104)

nodes + doc_content 테이블에서 body_text를 읽어 800자 슬라이딩 윈도우 청크로 분할,
doc_chunks + chunks_fts FTS5 contentless 테이블에 저장한다.

사용법:
    python scripts/chunk_builder.py                    # dry-run (기본)
    python scripts/chunk_builder.py --apply            # 실제 DB 변경
    python scripts/chunk_builder.py --game chaoszero   # 게임 필터
    python scripts/chunk_builder.py --node 1234        # 단일 노드
    python scripts/chunk_builder.py --min-chars 100    # body_text 최소 길이 필터
    python scripts/chunk_builder.py --batch-size 100   # 배치 크기 조정
    python scripts/chunk_builder.py --resume-from 500  # node_id >= 500부터 재개

제약:
    - DELETE 순서: chunks_fts 먼저 → doc_chunks 나중 (contentless_delete=1 필수)
    - INSERT: doc_chunks INSERT → last_insert_rowid() → chunks_fts rowid 명시
    - WAL: busy_timeout=10000 + batch 200 + 1000마다 PASSIVE checkpoint
    - MIN_CHARS 50자 미만 청크 버림 (병합 아님)
    - OVERLAP: 이전 청크 마지막 100자를 현재 청크 앞에 포함 (슬라이딩 윈도우)
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional
from typing import Iterator

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.models import get_connection  # noqa: E402
from src import config as cache_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("chunk_builder")

# ── 청킹 파라미터 ─────────────────────────────────────────────────────────
CHUNK_SIZE = 800   # 청크당 목표 글자 수
OVERLAP    = 100   # 이전 청크 마지막 N자를 현재 청크 앞에 포함
MIN_CHARS  = 50    # 이 미만 청크는 버림 (병합 아님)

BATCH_SIZE       = 200   # nodes per commit
CHECKPOINT_EVERY = 1000  # PASSIVE checkpoint 주기 (nodes 단위)


# ── 마커 정규식 (task-125 정합 — coverage_comparator.py와 동일) ─────────────
# HIGH-1/HIGH-2 ReDoS 가드 (task-125 Step 7): {1,500} 길이 제한
_FORMULA_BLOCK_RE = re.compile(r"\[=([^\]\n]{1,500})\]")
_IMAGE_MARKER_RE  = re.compile(r"\[이미지[^\]\n]{1,500}\]")

# 시트/슬라이드 경계 패턴
_SHEET_HEADER_RE = re.compile(r"^##\s+Sheet:\s+(.+)$", re.MULTILINE)
_SLIDE_HEADER_RE = re.compile(r"^##\s+Slide\s+(\d+)\s*$", re.MULTILINE)


# ═══════════════════════════════════════════════════════════════════════════════
# 마커 atomic 가드 helper (task-124 §4)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_inside_marker(text: str, pos: int) -> tuple:
    """pos가 마커 내부인지 확인. inside=True면 (True, start, end) 반환.

    탐색 범위: pos를 중심으로 좌/우 ≤500자 (마커 길이 상한).
    MINOR-1 시정: 우측 탐색을 pos+501로 보강 — 마커 끝이 정확히 500자 떨어진 경계 케이스 포착.
    """
    left_window = max(0, pos - 500)
    # MINOR-1 시정: pos+502 — [= + 500자 내용 + ] = 503자 마커에서 ] 포함 보장
    # 설계 §4.2 "pos+501" 대비 +1 추가: [=...{499자}] 형태 마커는 502자이므로
    # pos=0 기준 snippet[0:502]가 필요. {1,500} 패턴 최대 길이(500자 내용)+2(괄호)=502자
    snippet = text[left_window:pos + 502]
    for pat in (_FORMULA_BLOCK_RE, _IMAGE_MARKER_RE):
        for m in pat.finditer(snippet):
            abs_start = left_window + m.start()
            abs_end = left_window + m.end()
            if abs_start <= pos < abs_end:
                return True, abs_start, abs_end
    return False, -1, -1


def _marker_safe_split_position(text: str, candidate_pos: int, chunk_start_offset: int) -> int:
    """candidate_pos가 마커 내부면 마커 직전(보장 안전) 또는 직후로 이동.

    C-1 시정: chunk_start_offset 매개변수 명시. _safe_sliding_window가 chunk 시작점을 추적하여 전달.

    선택 규칙:
      - m_start - chunk_start_offset >= MIN_CHARS면 직전 선택 (chunk가 50자 이상 확보됨)
      - 그렇지 않으면 직후 선택 (chunk 길이 일시 초과 허용 — 최대 CHUNK_SIZE+500 = 1,300자)
    """
    inside, m_start, m_end = _is_inside_marker(text, candidate_pos)
    if not inside:
        return candidate_pos
    return m_start if (m_start - chunk_start_offset) >= MIN_CHARS else m_end


def _safe_sliding_window(text: str, chunk_start_base: int = 0) -> list:
    """CHUNK_SIZE/OVERLAP 슬라이딩 윈도우 분할 + 마커 atomic 가드.

    M-2 시정: 시그니처 명시.
    chunk_start_base: 호출자가 추적하는 절대 offset (현 함수는 text 내부 상대 좌표만 추적).
    알고리즘:
      1. start=0부터 CHUNK_SIZE 단위로 진행
      2. 매 chunk boundary 후보 end = start + CHUNK_SIZE
      3. _marker_safe_split_position(text, end, chunk_start_offset=start)로 보정
      4. text[start:safe_end] 추출, OVERLAP 100자 적용 (다음 chunk 시작 = safe_end - OVERLAP)
    """
    chunks: list = []
    start = 0
    total = len(text)
    while start < total:
        end = min(start + CHUNK_SIZE, total)
        if end < total:
            # 마커 atomic 가드 — chunk_start_offset=start로 직전/직후 판단
            safe_end = _marker_safe_split_position(text, end, chunk_start_offset=start)
        else:
            safe_end = end
        chunks.append(text[start:safe_end])
        if safe_end >= total:
            break
        # 다음 시작점: OVERLAP 만큼 뒤로 (단 역행 방지 가드)
        start = max(safe_end - OVERLAP, start + 1)
    return chunks


# ═══════════════════════════════════════════════════════════════════════════════
# 표 블록 helper (task-124 §5)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_table_header_line(line: str) -> bool:
    """헤더 라인 인식.

    MINOR-3: count("|") >= 2 완화 (1컬럼 표 `| col |` 인식).
    separator 조건과 결합하여 `||` 연속 오탐 방지.
    """
    s = line.strip()
    return s.startswith("|") and s.endswith("|") and s.count("|") >= 2 and "---" not in s


def _is_table_separator_line(line: str) -> bool:
    """구분자 라인 인식."""
    s = line.strip()
    return s.startswith("|") and s.endswith("|") and "---" in s


def _is_table_data_line(line: str) -> bool:
    """데이터 행 인식.

    M-5 시정: separator 라인과 구분 위해 '---' 포함 라인 제외.
    """
    s = line.strip()
    return s.startswith("|") and s.endswith("|") and s.count("|") >= 2 and "---" not in s


def _split_into_blocks(segment: str) -> list:
    """segment를 (block_type, block_text) 블록으로 분리.

    표 시작 인식: _is_table_header_line(line[i]) AND _is_table_separator_line(line[i+1])
    데이터 행 수집: _is_table_data_line (M-5 시정으로 separator 라인 자동 제외 → 연속 표 분리 정확)
    """
    lines = segment.split("\n")
    blocks: list = []
    current_text_lines: list = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if _is_table_header_line(line) and i + 1 < n and _is_table_separator_line(lines[i + 1]):
            if current_text_lines:
                text_block = "\n".join(current_text_lines).strip()
                if text_block:
                    blocks.append(("text", text_block))
                current_text_lines = []
            table_lines = [line, lines[i + 1]]
            j = i + 2
            while j < n and _is_table_data_line(lines[j]):
                table_lines.append(lines[j])
                j += 1
            blocks.append(("table", "\n".join(table_lines)))
            i = j
        else:
            current_text_lines.append(line)
            i += 1
    if current_text_lines:
        text_block = "\n".join(current_text_lines).strip()
        if text_block:
            blocks.append(("text", text_block))
    return blocks


def _split_table_block(table_block: str) -> list:
    """표 블록을 헤더 보존 분할.

    C-4 시정: 반환 타입 list[(chunk_text, origin)] — 단일 행이 CHUNK_SIZE 초과 시 origin="sliding".
    MINOR-4 시정: 단일 행 CHUNK_SIZE 초과 조건은 row_len > CHUNK_SIZE (헤더 무관 단독 행 길이).
    """
    lines = table_block.split("\n")
    if len(lines) < 3:
        # 헤더만 있는 표 (데이터 없음)
        return [(table_block, "table")]

    header = "\n".join(lines[:2])    # 헤더 + 구분자
    header_len = len(header) + 1     # +1 for \n

    chunks: list = []
    current_rows: list = []
    current_len = header_len

    for row in lines[2:]:
        row_len = len(row) + 1   # +1 for \n

        # MINOR-4 시정: 단일 행이 CHUNK_SIZE 초과 — 희귀 케이스
        if row_len > CHUNK_SIZE:
            if current_rows:
                chunks.append((header + "\n" + "\n".join(current_rows), "table"))
                current_rows = []
                current_len = header_len
            # 단일 행 슬라이딩 분할 — origin="sliding"
            for sw in _safe_sliding_window(header + "\n" + row):
                chunks.append((sw, "sliding"))
            continue

        if current_len + row_len > CHUNK_SIZE and current_rows:
            chunks.append((header + "\n" + "\n".join(current_rows), "table"))
            current_rows = [row]
            current_len = header_len + row_len
        else:
            current_rows.append(row)
            current_len += row_len

    if current_rows:
        chunks.append((header + "\n" + "\n".join(current_rows), "table"))

    return chunks


# ═══════════════════════════════════════════════════════════════════════════════
# 시트 경계 인식 helper (task-124 §6)
# ═══════════════════════════════════════════════════════════════════════════════

def _split_by_sheet_boundary(body_text: str) -> list:
    """xlsx/pptx 시트 경계 분리.

    M-7 시정: 첫 시트 헤더 이전 선행 텍스트(boundaries[0][0] > 0)를 별도 segment로 보존.
              section_path=None, build_chunks의 sheet_name=None 경로로 "preamble" origin 부여.
    """
    boundaries: list = []
    for m in _SHEET_HEADER_RE.finditer(body_text):
        boundaries.append((m.start(), f"Sheet:{m.group(1).strip()}"))
    for m in _SLIDE_HEADER_RE.finditer(body_text):
        boundaries.append((m.start(), f"Slide {m.group(1).strip()}"))
    boundaries.sort()

    if not boundaries:
        return [(None, body_text)]

    segments: list = []

    # M-7 시정: 첫 시트 이전 선행 텍스트 segment로 보존 (origin="preamble")
    if boundaries[0][0] > 0:
        preamble = body_text[:boundaries[0][0]].strip()
        if preamble:
            segments.append((None, preamble))

    for i, (pos, name) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(body_text)
        segments.append((name, body_text[pos:end]))
    return segments


def _merge_small_segments(segments, threshold: int = 400) -> list:
    """작은 시트는 인접 시트와 묶음. section_path는 첫 시트명 유지.

    M-6 시정:
      - else 분기에서 buf_name, buf_text = name, text 라인 추가 (첫 세그먼트가 threshold 초과해도 보존)
      - 비교 조건에 \\n\\n 2자 포함: len(buf_text) + 2 + len(text) <= threshold
    """
    merged: list = []
    buf_name = None
    buf_text = ""
    for name, text in segments:
        # M-6 시정: \n\n 2자 포함 비교
        joined_len = len(buf_text) + (2 if buf_text else 0) + len(text)
        if joined_len <= threshold:
            buf_name = buf_name or name
            buf_text = buf_text + "\n\n" + text if buf_text else text
        else:
            if buf_text:
                merged.append((buf_name, buf_text))
            # M-6 시정: 현재 세그먼트를 새 buf에 적재 (else 분기 누락 시정)
            buf_name, buf_text = name, text
    if buf_text:
        merged.append((buf_name, buf_text))
    return merged


# ═══════════════════════════════════════════════════════════════════════════════
# 청킹 로직 (task-124 §3 A1 Semantic Chunking — 기존 슬라이딩 윈도우 대체)
# ═══════════════════════════════════════════════════════════════════════════════

def build_chunks(body_text: str) -> list:
    """body_text를 의미 단위 우선 분할 (A1 Semantic Chunking — task-124).

    우선순위:
      L1: 시트/슬라이드 경계 (M-7 선행 텍스트 보존 포함)
      L2: 표 블록 (헤더+구분자+데이터 rows)
      L3a: 표 헤더 반복 분할 (표 블록 > CHUNK_SIZE 시)
      L3b: 단락(\\n\\n) 그룹화
      L4: 슬라이딩 윈도우 fallback (단락 > CHUNK_SIZE 시) — 마커 atomic 가드 포함

    Returns:
        list of dict: {seq, text, section_path, char_count, chunk_origin}
    """
    if not body_text:
        return []

    # ── L1: 시트/슬라이드 경계 인식 (M-7 선행 텍스트 보존 포함) ─────────────
    sheet_segments = _split_by_sheet_boundary(body_text)

    # MAJOR-NEW-1 시정: preamble(name=None) segment는 병합 대상에서 분리.
    # _merge_small_segments는 name이 동일하지 않은 segment 병합 시 첫 name 채택 →
    # preamble이 첫 시트와 병합되면 chunk_origin="preamble" 소실 위험.
    preamble_segs = [(n, t) for n, t in sheet_segments if n is None]
    named_segs = [(n, t) for n, t in sheet_segments if n is not None]
    named_segs = _merge_small_segments(named_segs, threshold=CHUNK_SIZE // 2)
    sheet_segments = preamble_segs + named_segs

    result: list = []
    seq = 0

    for sheet_name, segment in sheet_segments:
        # ── L2: 표 블록 인식 (segment 내부) ─────────────────────────────────
        blocks = _split_into_blocks(segment)

        # C-4 시정: 슬라이딩 fallback chunk만 "sliding" 별도 부여
        # M-1 시정: blocks_count(blocks) → len(blocks)
        text_origin = (
            "section" if len(blocks) > 1
            else ("preamble" if sheet_name is None else "sheet")
        )

        # task-124 Step 6 root cause 시정 (STRUCTURE_LOSS 0% → 100% 폭증):
        # _split_into_blocks가 "## Sheet: 시트명" 라인을 16자 짜리 text block으로 분리하면
        # build_chunks의 MIN_CHARS=50 미달로 버려짐 → cache에 시트 헤더 0회 등장.
        # 시정: 각 sheet_segment의 첫 chunk text 앞에 sheet header prepend (sheet_name 있을 때만).
        first_chunk_in_segment = True

        for block_type, block_text in blocks:
            if block_type == "table":
                # ── L3a: 표 헤더 반복 분할 (R-3) ─────────────────────────
                # C-4 정합: _split_table_block이 list[(text, origin)] tuple 반환
                block_chunks = _split_table_block(block_text)
            else:
                # ── L3b: 단락(\n\n) 그룹화 ───────────────────────────────
                section_chunks = _group_sections(_split_sections(block_text))
                block_chunks = []
                for sec in section_chunks:
                    if len(sec) <= CHUNK_SIZE:
                        block_chunks.append((sec, text_origin))
                    else:
                        # ── L4: 슬라이딩 윈도우 fallback ─────────────────
                        # C-4 시정: 슬라이딩 결과만 origin="sliding"
                        for sw_chunk in _safe_sliding_window(sec):
                            block_chunks.append((sw_chunk, "sliding"))

            for chunk_text, origin in block_chunks:
                if len(chunk_text) >= MIN_CHARS:
                    # task-124 Step 6 시정: 첫 chunk에 sheet header prepend
                    if first_chunk_in_segment and sheet_name:
                        chunk_text_final = f"## {sheet_name}\n{chunk_text}"
                        first_chunk_in_segment = False
                    else:
                        chunk_text_final = chunk_text
                    result.append({
                        "seq": seq,
                        "text": chunk_text_final,
                        "section_path": sheet_name,
                        "char_count": len(chunk_text_final),
                        "chunk_origin": origin,
                    })
                    seq += 1

    return result


def _split_sections(body_text: str) -> list:
    """\\n\\n 또는 탭 행 기준으로 섹션 분리."""
    lines = body_text.split("\n")
    sections = []
    current_lines: list = []

    for line in lines:
        if line.startswith("\t") and current_lines:
            merged = "\n".join(current_lines).strip()
            if merged:
                sections.append(merged)
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_lines:
        merged = "\n".join(current_lines).strip()
        if merged:
            sections.append(merged)

    result = []
    for sec in sections:
        parts = [p.strip() for p in sec.split("\n\n") if p.strip()]
        result.extend(parts)

    return result if result else [body_text]


def _group_sections(sections: list) -> list:
    """섹션들을 CHUNK_SIZE 이하로 그룹핑."""
    groups = []
    current_parts: list = []
    current_len = 0

    for sec in sections:
        sec_len = len(sec)
        if sec_len > CHUNK_SIZE:
            if current_parts:
                groups.append("\n\n".join(current_parts))
                current_parts = []
                current_len = 0
            groups.append(sec)
        elif current_len + sec_len + 2 > CHUNK_SIZE and current_parts:
            groups.append("\n\n".join(current_parts))
            current_parts = [sec]
            current_len = sec_len
        else:
            current_parts.append(sec)
            current_len += sec_len + (2 if current_parts else 0)

    if current_parts:
        groups.append("\n\n".join(current_parts))

    return groups if groups else sections


def _sliding_window(text: str) -> list:
    """기존 슬라이딩 윈도우 (마커 가드 없음 — 레거시, 내부 참조용)."""
    chunks = []
    start = 0
    total = len(text)

    while start < total:
        end = start + CHUNK_SIZE
        chunk = text[start:end]
        chunks.append(chunk)
        if end >= total:
            break
        start = end - OVERLAP

    return chunks


# ── DB 조회 ───────────────────────────────────────────────────────────────

def iter_nodes(conn: sqlite3.Connection, game_name: str = None,
               node_id: int = None, min_chars: int = 0,
               resume_from: int = 0) -> Iterator[dict]:
    """청킹 대상 노드 조회 제너레이터."""
    where_clauses = ["n.source_type = 'gdi'"]
    params: list = []

    if node_id is not None:
        where_clauses.append("n.id = ?")
        params.append(node_id)

    if game_name:
        where_clauses.append("LOWER(n.path) LIKE ? ESCAPE '\\'")
        escaped = game_name.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append(f"%{escaped}%")

    if resume_from > 0:
        where_clauses.append("n.id >= ?")
        params.append(resume_from)

    if min_chars > 0:
        where_clauses.append("dc.char_count >= ?")
        params.append(min_chars)
    else:
        # body_text 비어있지 않은 노드만
        where_clauses.append("dc.body_text IS NOT NULL")
        where_clauses.append("dc.body_text != ''")

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT n.id, n.title, dc.body_text, dc.char_count
        FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        WHERE {where_sql}
        ORDER BY n.id
    """
    cur = conn.execute(sql, params)
    for row in cur:
        yield {
            "node_id": row[0],
            "title": row[1] or "",
            "body_text": row[2] or "",
            "char_count": row[3] or 0,
        }


# ── INSERT / DELETE (멱등) ────────────────────────────────────────────────

def insert_chunks(conn: sqlite3.Connection, node_id: int, title: str,
                  chunks: list[dict]) -> int:
    """멱등 청크 upsert.

    DELETE 순서 (contentless_delete=1 필수):
      1. 기존 doc_chunks.id 수집 (chunks_fts rowid = doc_chunks.id)
      2. chunks_fts DELETE (rowid 명시)
      3. doc_chunks DELETE
      4. doc_chunks INSERT + last_insert_rowid() 수집
      5. chunks_fts INSERT with explicit rowid

    Returns: 삽입된 청크 수
    """
    # ① 기존 id 수집
    old_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM doc_chunks WHERE node_id = ?", (node_id,)
        ).fetchall()
    ]

    # ② chunks_fts DELETE (contentless 모드 — rowid 명시 필수)
    if old_ids:
        ph = ",".join("?" * len(old_ids))
        conn.execute(f"DELETE FROM chunks_fts WHERE rowid IN ({ph})", old_ids)

    # ③ doc_chunks DELETE
    conn.execute("DELETE FROM doc_chunks WHERE node_id = ?", (node_id,))

    # ④⑤ INSERT doc_chunks + chunks_fts
    inserted = 0
    for chunk in chunks:
        conn.execute(
            "INSERT INTO doc_chunks(node_id, seq, text, section_path, char_count, chunk_origin) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (node_id, chunk["seq"], chunk["text"],
             chunk["section_path"], chunk["char_count"],
             chunk.get("chunk_origin", "legacy")),
        )
        chunk_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO chunks_fts(rowid, title, text) VALUES (?, ?, ?)",
            (chunk_id, title, chunk["text"]),
        )
        inserted += 1

    return inserted


# ── 통계 ─────────────────────────────────────────────────────────────────

def get_chunk_stats(conn: sqlite3.Connection) -> dict:
    """현재 청크 테이블 통계."""
    total_chunks = conn.execute("SELECT COUNT(*) FROM doc_chunks").fetchone()[0]
    total_nodes = conn.execute(
        "SELECT COUNT(DISTINCT node_id) FROM doc_chunks"
    ).fetchone()[0]
    fts_total = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
    return {
        "total_chunks": total_chunks,
        "nodes_with_chunks": total_nodes,
        "chunks_fts_total": fts_total,
    }


# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="doc_chunks + chunks_fts FTS5 청킹 빌더 (task-104)",
    )
    parser.add_argument("--apply", action="store_true",
                        help="실제 DB 변경 (기본: dry-run)")
    parser.add_argument("--game", type=str, default=None,
                        help="게임 필터 (예: chaoszero) — nodes.path LIKE 매칭")
    parser.add_argument("--node", type=int, default=None,
                        help="단일 노드 ID 처리")
    parser.add_argument("--min-chars", type=int, default=0,
                        help="body_text 최소 길이 (기본 0 — 모든 노드)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"노드 배치 크기 (기본 {BATCH_SIZE})")
    parser.add_argument("--resume-from", type=int, default=0,
                        help="특정 node_id부터 재개 (멱등이므로 재실행 안전)")
    args = parser.parse_args()

    db_path = Path(cache_config.DB_PATH)
    if not db_path.exists():
        log.error("DB 파일 없음: %s", db_path)
        sys.exit(1)

    # schema_version 확인 (v6 필요)
    check_conn = get_connection(str(db_path))
    try:
        ver = check_conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
        if ver < 11:
            log.error(
                "schema v%d 감지 — v11 필요. "
                "먼저 python -m src.models 실행하여 migrate() 적용 후 재시도.",
                ver,
            )
            check_conn.close()
            sys.exit(1)
        # chunks_fts 테이블 존재 확인
        check_conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()
    except sqlite3.OperationalError as schema_err:
        log.error("schema 확인 실패: %s", schema_err)
        check_conn.close()
        sys.exit(1)
    finally:
        check_conn.close()

    mode_label = "APPLY" if args.apply else "DRY-RUN"
    scope_parts = []
    if args.node:
        scope_parts.append(f"node={args.node}")
    if args.game:
        scope_parts.append(f"game={args.game}")
    if args.min_chars > 0:
        scope_parts.append(f"min_chars={args.min_chars}")
    if args.resume_from > 0:
        scope_parts.append(f"resume_from={args.resume_from}")
    scope_label = " / ".join(scope_parts) if scope_parts else "전체"

    log.info("=" * 60)
    log.info("chunk_builder [%s] — %s", mode_label, scope_label)
    log.info("CHUNK_SIZE=%d  OVERLAP=%d  MIN_CHARS=%d  BATCH_SIZE=%d",
             CHUNK_SIZE, OVERLAP, MIN_CHARS, args.batch_size)
    log.info("=" * 60)

    t0 = time.time()

    if args.apply:
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 10000")
    else:
        # dry-run: 읽기 전용 연결
        conn = get_connection(str(db_path))

    try:
        # 사전 통계
        try:
            before = get_chunk_stats(conn)
        except sqlite3.OperationalError:
            before = {"total_chunks": 0, "nodes_with_chunks": 0, "chunks_fts_total": 0}

        log.info("[사전 상태] chunks=%d  nodes_with_chunks=%d  fts=%d",
                 before["total_chunks"], before["nodes_with_chunks"],
                 before["chunks_fts_total"])

        # 노드 목록 조회 (dry-run도 동일하게 조회)
        read_conn = get_connection(str(db_path))
        node_list = list(iter_nodes(
            read_conn,
            game_name=args.game,
            node_id=args.node,
            min_chars=args.min_chars,
            resume_from=args.resume_from,
        ))
        read_conn.close()

        total_nodes = len(node_list)
        total_expected_chunks = 0

        # 예상 청크 수 계산 (항상 실행)
        for node in node_list:
            chunks = build_chunks(node["body_text"])
            total_expected_chunks += len(chunks)

        log.info("대상 노드: %d개  예상 청크: %d개 (CHUNK_SIZE=%d, MIN_CHARS=%d)",
                 total_nodes, total_expected_chunks, CHUNK_SIZE, MIN_CHARS)

        if not args.apply:
            log.info("[DRY-RUN] DB 변경 없음. --apply 플래그로 실제 적용.")
            conn.close()
            return

        # APPLY 모드: 실제 처리
        processed_nodes = 0
        inserted_chunks = 0
        skipped_nodes = 0

        for i, node in enumerate(node_list):
            node_id = node["node_id"]
            title   = node["title"]
            body    = node["body_text"]

            chunks = build_chunks(body)
            if not chunks:
                skipped_nodes += 1
                log.debug("노드 %d: 유효 청크 없음 (body_len=%d) — skip",
                          node_id, len(body))
                continue

            conn.execute("BEGIN IMMEDIATE")
            try:
                n = insert_chunks(conn, node_id, title, chunks)
                conn.execute("COMMIT")
                inserted_chunks += n
                processed_nodes += 1
            except sqlite3.Error as db_err:
                conn.execute("ROLLBACK")
                log.warning("노드 %d INSERT 실패 (rollback): %s", node_id, db_err)
                skipped_nodes += 1
                continue

            # 배치 커밋마다 PASSIVE checkpoint
            if (i + 1) % args.batch_size == 0:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                log.info("  [진행] %d/%d 노드 처리, 청크 %d개 삽입",
                         i + 1, total_nodes, inserted_chunks)

            if (i + 1) % CHECKPOINT_EVERY == 0:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                log.info("  [checkpoint] %d 노드 처리 완료", i + 1)

        # 최종 PASSIVE checkpoint
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

        elapsed = time.time() - t0

        # 사후 통계
        after = get_chunk_stats(conn)
        log.info("=" * 60)
        log.info("[완료] %.1f초 경과", elapsed)
        log.info("  처리 노드: %d개  삽입 청크: %d개  스킵: %d개",
                 processed_nodes, inserted_chunks, skipped_nodes)
        log.info("  [사후 상태] chunks=%d  nodes_with_chunks=%d  fts=%d",
                 after["total_chunks"], after["nodes_with_chunks"],
                 after["chunks_fts_total"])
        log.info("=" * 60)

    except Exception as fatal_err:
        log.error("치명적 오류: %s", fatal_err)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
