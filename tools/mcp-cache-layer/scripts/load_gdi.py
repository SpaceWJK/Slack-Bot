"""
load_gdi.py — GDI 문서 저장소 일괄 적재 스크립트

GDI(Game Doc Insight) MCP 서버의 파일을 SQLite에 일괄 적재한다.
GDI 파일은 불변(수정/삭제 없음, 신규 추가만)이므로 일괄 적재가 최적.

사용법:
    python scripts/load_gdi.py Epicseven             # 특정 게임 전체 적재
    python scripts/load_gdi.py Chaoszero              # Chaoszero 전체 적재
    python scripts/load_gdi.py --all                  # 모든 게임 적재
    python scripts/load_gdi.py --delta Epicseven      # 신규 파일만 적재
    python scripts/load_gdi.py --folder "Update/" Chaoszero  # 특정 폴더만
    python scripts/load_gdi.py --stats Chaoszero      # 적재 통계 출력

저장 구조:
  nodes:       source_type="gdi", source_id=file_path, title=file_name, space_key=game_name
  doc_content: body_text = 전체 청크 텍스트 결합
  doc_meta:    ttl_hours=0 (불변), extra_meta={source_type, chunk_count, version_date}
"""

import sys
import os
import json
import re
import time
import sqlite3
from datetime import datetime
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

from src.models import init_db, get_connection, migrate
from mcp_session import McpSession
from reconstructors import (
    reconstruct_body, reconstruct_xlsx, reconstruct_pptx, reconstruct_tsv,
    MAX_TABLE_ROWS, MAX_COL_WIDTH,
)
from dotenv import load_dotenv

load_dotenv("D:/Vibe Dev/Slack Bot/.env")

# ── 설정 ────────────────────────────────────────────────────────────────────

GDI_MCP_URL = os.getenv("GDI_MCP_URL", "http://mcp-dev.sginfra.net/game-doc-insight-mcp")
DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")

# 게임명 정규화 (CLI 입력 → MCP game_name)
GAME_NAMES = {
    "chaoszero": "chaoszero",
    "epicseven": "epicseven",
    "lordnine_asia": "lordnine_asia",
    "cz": "chaoszero",
    "e7": "epicseven",
    "ln": "lordnine_asia",
    "lordnine": "lordnine_asia",
}

# 부하 관리
FILE_SLEEP = 0.3        # 파일 간 대기 (초)
PAGE_SLEEP = 0.2        # 페이지네이션 간 대기 (초)
COMMIT_INTERVAL = 50    # N건마다 커밋
LIST_PAGE_SIZE = 50     # list_files_in_folder 페이지 크기
CHUNK_PAGE_SIZE = 20    # search_by_filename 페이지 크기
MAX_BODY_CHARS = 500000 # 본문 최대 글자 수

# ── 파일 형식별 재구성 로직은 reconstructors.py로 이동 ─────────────────────
# _CHUNK_META_RE, _PPTX_PREFIX_RE, _PPTX_EMPTY_NOTES_RE, _XLSX_PREFIX,
# MAX_TABLE_ROWS, MAX_COL_WIDTH, _clean_chunk_text, _sanitize_cells,
# _is_summary_sheet, _deduplicate_headers, _fill_down_chunks,
# _format_summary_rows, _parse_xlsx_chunk, _detect_and_convert_table,
# _buf_to_md_table, _reconstruct_xlsx/pptx/tsv/body 는 모두 reconstructors.py로 이동.
# load_gdi.py는 상단 import로 사용 (task-075).


# ── 헬퍼 ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _normalize_game(name: str) -> str:
    """CLI 입력을 MCP game_name으로 정규화."""
    key = name.lower().strip()
    if key in GAME_NAMES:
        return GAME_NAMES[key]
    return key


# ── MCP 세션 ─────────────────────────────────────────────────────────────────

def get_mcp() -> McpSession:
    return McpSession(url=GDI_MCP_URL, label="gdi")


# ── 1단계: 파일 목록 수집 ───────────────────────────────────────────────────

def crawl_files(mcp: McpSession, root_path: str, game_name: str = "") -> list[dict]:
    """list_files_in_folder 페이지네이션으로 전체 파일 목록 수집.

    Returns: [{"file_name", "file_path", "source_type", "chunk_count",
               "version_date", "indexed_date"}, ...]
    """
    all_files = []
    page = 1
    while True:
        args = {
            "folder_path": root_path,
            "page": page,
            "page_size": LIST_PAGE_SIZE,
        }
        raw, err = mcp.call_tool("list_files_in_folder", args)
        if err:
            print(f"  [ERROR] list_files_in_folder: {err}")
            break

        data = _parse_mcp(raw)
        if not data or not isinstance(data, dict):
            break

        files = data.get("files", [])
        if not files:
            break

        all_files.extend(files)
        total = data.get("total_files", "?")
        print(f"  page {page}: {len(files)}건 (누적: {len(all_files)}/{total})")

        pagination = data.get("pagination", {})
        if not pagination.get("has_next"):
            break
        page += 1
        time.sleep(PAGE_SLEEP)

    return all_files


# ── 2단계: 파일 청크 수집 ───────────────────────────────────────────────────

def fetch_file_chunks(mcp: McpSession, file_name: str,
                      file_path: str = "",
                      game_name: str = "",
                      source_type: str = "") -> tuple[str, int]:
    """search_by_filename으로 전체 청크 텍스트 수집 + 형식별 재구성.

    NOTE: GDI MCP의 exact_match는 확장자 포함 시 버그가 있어 일반 검색 사용.
          game_name은 GDI MCP가 모든 파일을 'epicseven'으로 반환하므로 미전달.

    Args:
        source_type: 파일 형식 (generic_xlsx, generic_pptx, generic_tsv).
                     형식에 맞게 사람이 보는 형태로 재구성한다.

    Returns: (combined_text, chunk_count)
    """
    all_chunks = []
    chunk_count = 0
    page = 1

    # 확장자 제거한 쿼리명 (exact_match 버그 우회)
    query_name = file_name.rsplit(".", 1)[0] if "." in file_name else file_name

    while True:
        args = {
            "file_name_query": query_name,
            "page": page,
            "page_size": CHUNK_PAGE_SIZE,
        }
        # game_name은 전달하지 않음 (GDI MCP가 모든 파일을 epicseven으로 반환)

        raw, err = mcp.call_tool("search_by_filename", args)
        if err:
            if page == 1:
                print(f"    [ERROR] fetch_chunks({file_name}): {err}")
            break

        data = _parse_mcp(raw)
        if not data or not isinstance(data, dict):
            break

        # 결과 파일 정보 확인 (경고만, 중단하지 않음)
        # NOTE: search_by_filename은 파일명 기반 검색이므로
        #       다른 날짜 폴더의 동명 파일이 반환될 수 있음 (정상 동작)

        chunks = data.get("chunks", [])
        if not chunks:
            break

        for c in chunks:
            text = c.get("chunk_content", "").strip()
            if text:
                all_chunks.append(text)
                chunk_count += 1

        pagination = data.get("pagination", {})
        if not pagination.get("has_next"):
            break
        page += 1
        time.sleep(PAGE_SLEEP)

    # 형식별 재구성 (사람+LLM이 보는 형태로 변환) — reconstructors.py
    # task-080: file_info 전달 → XML 태그 래핑 (LLM 파싱 최적화)
    file_info = {
        "file_name": file_name,
        "file_path": file_path,
        "source_type": source_type,
        "chunk_count": chunk_count,
    }
    combined = reconstruct_body(all_chunks, source_type, file_info=file_info)

    # MAX_BODY_CHARS 제한 — task-080: 슬라이스 후 truncation 마커 포함 (총 길이 한도 유지)
    if len(combined) > MAX_BODY_CHARS:
        from reconstructors import TRUNCATION_MARKER
        keep = MAX_BODY_CHARS - len(TRUNCATION_MARKER)
        combined = combined[:keep] + TRUNCATION_MARKER

    return combined, chunk_count


def _parse_mcp(raw) -> dict | list | None:
    """MCP 응답을 dict/list로 파싱."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return None


# ── 3단계: DB 저장 ──────────────────────────────────────────────────────────

def upsert_gdi_file(conn: sqlite3.Connection, file_info: dict,
                     body_text: str, game_name: str):
    """nodes + doc_content + doc_meta 저장."""
    file_name = file_info.get("file_name", "")
    file_path = file_info.get("file_path", "")
    source_type_gdi = file_info.get("source_type", "")  # xlsx, pptx, tsv 등
    chunk_count = file_info.get("chunk_count", 0)
    version_date = file_info.get("version_date", "")
    indexed_date = file_info.get("indexed_date", "")

    if not file_path:
        return

    # ── 1. nodes 테이블 ──
    existing = conn.execute(
        "SELECT id FROM nodes WHERE source_type='gdi' AND source_id=?",
        (file_path,)
    ).fetchone()

    if existing:
        node_id = existing[0]
        conn.execute(
            "UPDATE nodes SET title=?, updated_at=? WHERE id=?",
            (file_name, _now(), node_id)
        )
    else:
        cur = conn.execute(
            "INSERT INTO nodes (source_type, source_id, title, path, node_type, "
            "space_key, url, created_at, updated_at) "
            "VALUES ('gdi', ?, ?, ?, 'file', ?, '', ?, ?)",
            (file_path, file_name, file_path, game_name, _now(), _now())
        )
        node_id = cur.lastrowid

    # ── 2. doc_content 테이블 ──
    char_count = len(body_text)
    body_truncated = 1 if len(body_text) >= MAX_BODY_CHARS else 0

    existing_content = conn.execute(
        "SELECT id FROM doc_content WHERE node_id=?", (node_id,)
    ).fetchone()

    if existing_content:
        conn.execute(
            "UPDATE doc_content SET body_text=?, char_count=?, "
            "body_truncated=?, cached_at=? WHERE node_id=?",
            (body_text, char_count, body_truncated, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO doc_content (node_id, body_raw, body_text, char_count, "
            "body_truncated, cached_at) VALUES (?, '', ?, ?, ?, ?)",
            (node_id, body_text, char_count, body_truncated, _now())
        )

    # ── 3. doc_meta 테이블 ──
    extra_meta = json.dumps({
        "file_source_type": source_type_gdi,
        "chunk_count": chunk_count,
        "version_date": version_date,
        "indexed_date": indexed_date,
    }, ensure_ascii=False)

    existing_meta = conn.execute(
        "SELECT id FROM doc_meta WHERE node_id=?", (node_id,)
    ).fetchone()

    if existing_meta:
        conn.execute(
            "UPDATE doc_meta SET extra_meta=?, last_modified=?, cached_at=? WHERE node_id=?",
            (extra_meta, version_date, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO doc_meta (node_id, status, labels, extra_meta, "
            "last_modified, cached_at, ttl_hours) "
            "VALUES (?, '', '', ?, ?, ?, 0)",
            (node_id, extra_meta, version_date, _now())
        )

    # ── 4. search_fts 동기화 (task-077) ──
    # node_id는 위에서 신규(cur.lastrowid) 또는 기존(existing[0]) 경로로 확보된 값.
    # contentless 모드 제약: UPDATE 불가 → DELETE + INSERT.
    # 동일 트랜잭션 내 수행 → load_gdi 배치 commit까지 원자성 보장.
    conn.execute("DELETE FROM search_fts WHERE rowid = ?", (node_id,))
    if body_text:  # 빈 문자열 제외 (M-3 반영)
        # doc_content.summary/keywords는 load_gdi에서 저장 안 함 → 빈 문자열로 FTS 등록
        conn.execute(
            "INSERT INTO search_fts(rowid, title, body_text, summary, keywords) "
            "VALUES (?, ?, ?, '', '')",
            (node_id, file_name, body_text)
        )


# ── 메인 적재 함수 ──────────────────────────────────────────────────────────

def load_game(game_name: str, folder_filter: str = "", delta: bool = False,
              max_files: int = 0):
    """게임의 GDI 파일 일괄 적재.

    Args:
        game_name: MCP game_name (chaoszero, epicseven)
        folder_filter: 특정 폴더만 적재 (예: "Update/")
        delta: True이면 DB에 없는 신규 파일만 적재
        max_files: 0이면 무제한, 양수이면 해당 건수만 적재 (테스트용)
    """
    print(f"\n{'='*60}")
    mode = "Delta" if delta else "Full"
    print(f"GDI {mode} 적재 시작: {game_name}")
    if folder_filter:
        print(f"  폴더 필터: {folder_filter}")
    print(f"{'='*60}")

    mcp = get_mcp()
    t0 = time.time()

    # 1단계: 파일 목록 수집
    # GDI MCP 폴더 경로: "Chaoszero/", "Epicseven/", "Lordnine_Asia/" 등
    FOLDER_MAP = {
        "chaoszero": "Chaoszero",
        "epicseven": "Epicseven",
        "lordnine_asia": "Lordnine_Asia",
    }
    game_folder = FOLDER_MAP.get(game_name, game_name.capitalize())
    root_path = f"{game_folder}/"
    if folder_filter:
        root_path = f"{game_folder}/{folder_filter}"

    print(f"\n[1/3] 파일 목록 수집 ({root_path})")
    files_raw = crawl_files(mcp, root_path, game_name)
    print(f"  → 원본 {len(files_raw)}건 조회 ({time.time()-t0:.1f}s)")

    # 동명 파일 중복 제거 (같은 file_name → indexed_date가 최신인 것 유지)
    # search_by_filename은 파일명 기반이므로 동명 파일은 하나만 저장
    seen = {}
    for f in files_raw:
        fname = f.get("file_name", "")
        idx_date = f.get("indexed_date", "")
        if fname not in seen or idx_date > seen[fname].get("indexed_date", ""):
            seen[fname] = f
    files = list(seen.values())
    if len(files) < len(files_raw):
        print(f"  → 중복 제거 후: {len(files)}건 (동명 파일 {len(files_raw)-len(files)}건 제외)")

    if not files:
        print("파일 없음, 종료")
        return {"scanned": 0, "added": 0, "skipped": 0, "errors": 0}

    # DB 연결 (busy_timeout: 다른 프로세스 락 대기 최대 10초)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")

    # Delta 모드: DB에 이미 있는 파일 집합 수집
    existing_paths = set()
    if delta:
        rows = conn.execute(
            "SELECT source_id FROM nodes WHERE source_type='gdi' AND space_key=?",
            (game_name,)
        ).fetchall()
        existing_paths = {r[0] for r in rows}
        print(f"  DB 기존: {len(existing_paths)}건")

    # 적재 대상 필터링
    targets = []
    skipped = 0
    for f in files:
        fp = f.get("file_path", "")
        if delta and fp in existing_paths:
            skipped += 1
            continue
        targets.append(f)

    if delta:
        print(f"  → 신규 대상: {len(targets)}건 (기존 skip: {skipped}건)")

    # max_files 제한 (테스트용)
    if max_files > 0 and len(targets) > max_files:
        # chunk_count가 작은 순으로 정렬 (테스트 빠르게)
        targets.sort(key=lambda f: f.get("chunk_count", 0))
        targets = targets[:max_files]
        print(f"  → --test: 최소 {max_files}건만 적재")

    if not targets:
        conn.close()
        print("적재 대상 없음, 종료")
        return {"scanned": len(files), "added": 0, "skipped": skipped, "errors": 0}

    # 2단계 + 3단계: 파일별 청크 수집 → DB 저장
    print(f"\n[2/3] 파일 내용 수집 + DB 저장 (대상: {len(targets)}건)")
    added = 0
    errors = 0
    started = _now()

    for i, file_info in enumerate(targets, 1):
        fname = file_info.get("file_name", "?")
        fpath = file_info.get("file_path", "")
        try:
            body_text, chunk_count = fetch_file_chunks(
                mcp, fname, file_path=fpath, game_name=game_name,
                source_type=file_info.get("source_type", "")
            )
            if body_text:
                file_info["chunk_count"] = chunk_count
                upsert_gdi_file(conn, file_info, body_text, game_name)
                added += 1
            else:
                # 빈 파일도 메타만 저장
                upsert_gdi_file(conn, file_info, "", game_name)
                added += 1
        except Exception as e:
            errors += 1
            print(f"    [WARN] {fname}: {e}")

        # 진행률 출력 + 커밋
        if i % COMMIT_INTERVAL == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(targets) - i) / rate if rate > 0 else 0
            print(f"  {i}/{len(targets)} 완료 ({elapsed:.0f}s, "
                  f"~{eta:.0f}s 남음, 오류: {errors})")

        time.sleep(FILE_SLEEP)

    conn.commit()

    # sync_log 기록
    duration = round(time.time() - t0, 2)
    sync_type = "delta" if delta else "full"
    scope = f"{game_name}/{folder_filter}" if folder_filter else game_name
    conn.execute(
        "INSERT INTO sync_log (source_type, scope, sync_type, started_at, "
        "finished_at, status, pages_scanned, pages_added, pages_updated, "
        "duration_sec, error_message) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("gdi", scope, sync_type, started, _now(), "success",
         len(files), added, 0, duration,
         f"errors={errors}" if errors else None)
    )
    conn.commit()
    conn.close()

    stats = {
        "scanned": len(files), "added": added,
        "skipped": skipped, "errors": errors,
        "duration_sec": duration,
    }
    print(f"\n[3/3] 적재 완료:")
    print(f"  스캔: {len(files)}건, 적재: {added}건, "
          f"스킵: {skipped}건, 오류: {errors}건")
    print(f"  소요: {duration:.1f}초")

    return stats


# ── Delta 전체 (auto_sync 연동) ──────────────────────────────────────────────

def delta_ingest_all() -> dict:
    """DB에 적재된 모든 게임 Delta 적재."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    games = [r[0] for r in conn.execute(
        "SELECT DISTINCT space_key FROM nodes WHERE source_type='gdi' ORDER BY space_key"
    ).fetchall()]
    conn.close()

    if not games:
        print("적재된 GDI 게임 없음")
        return {}

    print(f"GDI Delta 대상 게임: {', '.join(games)}")
    results = {}
    for g in games:
        results[g] = load_game(g, delta=True)

    # 전체 요약
    total = {"scanned": 0, "added": 0, "skipped": 0, "errors": 0}
    for stats in results.values():
        for k in total:
            total[k] += stats.get(k, 0)

    print(f"\n{'='*60}")
    print(f"GDI Delta 전체 완료:")
    print(f"  게임: {len(games)}개")
    print(f"  스캔: {total['scanned']}건, 적재: {total['added']}건, "
          f"스킵: {total['skipped']}건, 오류: {total['errors']}건")
    return results


# ── 통계 ────────────────────────────────────────────────────────────────────

def _print_stats(games: list[str]):
    """적재 통계 출력."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    for g in games:
        row = conn.execute(
            "SELECT COUNT(*), "
            "COUNT(CASE WHEN dc.body_text != '' AND dc.body_text IS NOT NULL THEN 1 END), "
            "SUM(CASE WHEN dc.char_count IS NOT NULL THEN dc.char_count ELSE 0 END) "
            "FROM nodes n LEFT JOIN doc_content dc ON dc.node_id=n.id "
            "WHERE n.source_type='gdi' AND n.space_key=?",
            (g,)
        ).fetchone()
        print(f"\n[{g}] 노드: {row[0]}건, 본문 있음: {row[1]}건, "
              f"총 글자 수: {row[2]:,}")

        # 최근 sync_log
        sync = conn.execute(
            "SELECT sync_type, started_at, pages_scanned, pages_added, duration_sec "
            "FROM sync_log WHERE source_type='gdi' AND scope LIKE ? "
            "ORDER BY started_at DESC LIMIT 3",
            (f"{g}%",)
        ).fetchall()
        if sync:
            print(f"  최근 동기화:")
            for s in sync:
                print(f"    {s[0]} | {s[1]} | 스캔: {s[2]}건 | "
                      f"추가: {s[3]}건 | {s[4]}초")
    conn.close()


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    migrate(DB_PATH)

    if "--stats" in args:
        args.remove("--stats")
        games = [_normalize_game(a) for a in args if not a.startswith("--")]
        if not games:
            games = ["chaoszero", "epicseven"]
        _print_stats(games)
        sys.exit(0)

    if "--all" in args:
        args.remove("--all")
        delta = "--delta" in args
        if delta:
            args.remove("--delta")
        for g in ["chaoszero", "epicseven"]:
            load_game(g, delta=delta)
        sys.exit(0)

    delta = "--delta" in args
    if delta:
        args.remove("--delta")

    max_files = 0
    if "--test" in args:
        args.remove("--test")
        max_files = 5  # 테스트: 5건만

    folder_filter = ""
    if "--folder" in args:
        idx = args.index("--folder")
        if idx + 1 < len(args):
            folder_filter = args[idx + 1]
            args = args[:idx] + args[idx+2:]
        else:
            print("--folder 옵션에 폴더 경로를 지정하세요")
            sys.exit(1)

    # 게임명
    games = [_normalize_game(a) for a in args if not a.startswith("--")]
    if not games:
        print("사용법: python scripts/load_gdi.py [--delta] [--folder PATH] GAME_NAME")
        print("  GAME_NAME: Chaoszero, Epicseven, cz, e7")
        sys.exit(1)

    for g in games:
        load_game(g, folder_filter=folder_filter, delta=delta, max_files=max_files)

    print("\n\n전체 적재 완료!")
    _print_stats(games)
