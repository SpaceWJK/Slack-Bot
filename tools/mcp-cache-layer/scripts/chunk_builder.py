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
import sqlite3
import sys
import time
from pathlib import Path
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


# ── 청킹 로직 ─────────────────────────────────────────────────────────────

def build_chunks(body_text: str) -> list[dict]:
    """body_text를 CHUNK_SIZE/OVERLAP 기반 슬라이딩 윈도우로 분할.

    알고리즘:
    1. \\n\\n으로 섹션 분리
    2. 섹션들을 CHUNK_SIZE 이하로 그룹핑 (섹션 경계 우선)
    3. 그룹이 CHUNK_SIZE 초과 시 슬라이딩 윈도우 적용
    4. 각 청크에 이전 청크 마지막 OVERLAP자 접두 (seq>=1)
    5. MIN_CHARS 미만 버림

    Returns:
        list of dict: {seq, text, section_path, char_count}
    """
    if not body_text:
        return []

    # 섹션 분리 (\\n\\n 또는 탭 행)
    raw_sections = _split_sections(body_text)

    # 섹션을 CHUNK_SIZE 이하로 그룹핑하여 청크 텍스트 목록 생성
    raw_chunks = _group_sections(raw_sections)

    # OVERLAP 적용 + 슬라이딩 윈도우
    result = []
    seq = 0
    prev_tail = ""  # 이전 청크의 마지막 OVERLAP자

    for raw_text in raw_chunks:
        # OVERLAP 접두 적용 (seq >= 1부터)
        if prev_tail:
            chunk_text = prev_tail + raw_text
        else:
            chunk_text = raw_text

        # 이 청크가 CHUNK_SIZE 이하 → 단일 청크로 처리
        if len(chunk_text) <= CHUNK_SIZE:
            if len(chunk_text) >= MIN_CHARS:
                result.append({
                    "seq": seq,
                    "text": chunk_text,
                    "section_path": None,
                    "char_count": len(chunk_text),
                })
                seq += 1
            prev_tail = chunk_text[-OVERLAP:] if len(chunk_text) >= OVERLAP else chunk_text
        else:
            # 슬라이딩 윈도우 분할
            sub_chunks = _sliding_window(chunk_text)
            for sub in sub_chunks:
                if len(sub) >= MIN_CHARS:
                    result.append({
                        "seq": seq,
                        "text": sub,
                        "section_path": None,
                        "char_count": len(sub),
                    })
                    seq += 1
            if sub_chunks:
                last = sub_chunks[-1]
                prev_tail = last[-OVERLAP:] if len(last) >= OVERLAP else last
            else:
                prev_tail = ""

    return result


def _split_sections(body_text: str) -> list[str]:
    """\\n\\n 또는 탭 행 기준으로 섹션 분리."""
    # 탭으로 시작하는 행을 섹션 구분자로 처리 (TSV 메타 구조 대응)
    lines = body_text.split("\n")
    sections = []
    current_lines: list[str] = []

    for line in lines:
        if line.startswith("\t") and current_lines:
            # 탭 행 → 이전 섹션 마감 후 새 섹션 시작
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

    # \\n\\n 기준으로 추가 분리
    result = []
    for sec in sections:
        parts = [p.strip() for p in sec.split("\n\n") if p.strip()]
        result.extend(parts)

    return result if result else [body_text]


def _group_sections(sections: list[str]) -> list[str]:
    """섹션들을 CHUNK_SIZE 이하로 그룹핑."""
    groups = []
    current_parts: list[str] = []
    current_len = 0

    for sec in sections:
        sec_len = len(sec)
        if sec_len > CHUNK_SIZE:
            # 섹션 자체가 CHUNK_SIZE 초과 → 현재 그룹 마감 후 단독 처리
            if current_parts:
                groups.append("\n\n".join(current_parts))
                current_parts = []
                current_len = 0
            groups.append(sec)
        elif current_len + sec_len + 2 > CHUNK_SIZE and current_parts:
            # 추가하면 CHUNK_SIZE 초과 → 현재 그룹 마감
            groups.append("\n\n".join(current_parts))
            current_parts = [sec]
            current_len = sec_len
        else:
            current_parts.append(sec)
            current_len += sec_len + (2 if current_parts else 0)

    if current_parts:
        groups.append("\n\n".join(current_parts))

    return groups if groups else sections


def _sliding_window(text: str) -> list[str]:
    """CHUNK_SIZE/OVERLAP 슬라이딩 윈도우 분할."""
    chunks = []
    start = 0
    total = len(text)

    while start < total:
        end = start + CHUNK_SIZE
        chunk = text[start:end]
        chunks.append(chunk)
        if end >= total:
            break
        # 다음 시작점: OVERLAP만큼 뒤로 (이전 청크 마지막 OVERLAP자가 다음 청크에 포함)
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
            "INSERT INTO doc_chunks(node_id, seq, text, section_path, char_count) "
            "VALUES (?, ?, ?, ?, ?)",
            (node_id, chunk["seq"], chunk["text"],
             chunk["section_path"], chunk["char_count"]),
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
        if ver < 6:
            log.error(
                "schema v%d 감지 — v6 필요. "
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
