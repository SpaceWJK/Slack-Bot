"""
load_gdi_local.py — GDI 로컬 파일 기반 일괄 적재 스크립트

gdi-repo/ 디렉토리의 파일을 file_parsers.py로 직접 파싱하여 SQLite에 적재.
MCP 서버 호출 없이 로컬에서 완전히 동작한다.

사용법:
    python scripts/load_gdi_local.py Chaoszero          # 특정 게임 전체 적재
    python scripts/load_gdi_local.py --all               # 모든 게임 적재
    python scripts/load_gdi_local.py --delta Chaoszero   # 신규 파일만 적재
    python scripts/load_gdi_local.py --stats             # 적재 통계 출력
    python scripts/load_gdi_local.py --test Chaoszero    # 테스트 (10건만)
"""

import sys
import os
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from src.models import migrate
from file_parsers import parse_xlsx, parse_tsv, parse_pptx, parse_image, parse_docx
from folder_taxonomy import FolderIndex
# task-116 R-J fix: v10 컬럼 자동 채움 (신규 적재 시 task-115 라우팅 정책 즉시 적용)
from backfill_folder_role import (
    classify_folder_role,
    classify_game_alias,
    classify_file_kind,
    extract_ref_date,
)

# ── 설정 ────────────────────────────────────────────────────────────────────

DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")
GDI_REPO = PROJECT_ROOT / "gdi-repo"

# 지원 확장자 → 파서 매핑
# task-127 S4-7 M-2 (CRITICAL 4): lambda wrapper로 extract_images=False 명시
PARSERS = {
    ".xlsx": lambda p: parse_xlsx(p, extract_images=False),
    ".tsv": parse_tsv,
    ".pptx": parse_pptx,
    ".docx": parse_docx,
    ".png": parse_image,
    ".jpg": parse_image,
    ".jpeg": parse_image,
}

# source_type 매핑
SOURCE_TYPES = {
    ".xlsx": "generic_xlsx",
    ".tsv": "generic_tsv",
    ".pptx": "generic_pptx",
    ".docx": "generic_docx",
    ".png": "generic_png",
    ".jpg": "generic_png",
    ".jpeg": "generic_png",
}

# 게임명 정규화
GAME_NAMES = {
    "chaoszero": "chaoszero",
    "epicseven": "epicseven",
    "cz": "chaoszero",
    "e7": "epicseven",
    "lordnine_asia": "lordnine_asia",
    "ln": "lordnine_asia",
}

# 부하 관리
COMMIT_INTERVAL = 100
MAX_BODY_CHARS = 500_000


# ── 헬퍼 ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _normalize_game(name: str) -> str:
    key = name.lower().strip()
    return GAME_NAMES.get(key, key)


def _relative_path(file_path: Path) -> str:
    """gdi-repo/ 기준 상대 경로 반환."""
    try:
        return str(file_path.relative_to(GDI_REPO)).replace("\\", "/")
    except ValueError:
        return str(file_path)


def _detect_game(rel_path: str) -> str:
    """상대 경로에서 게임명 추출."""
    parts = rel_path.split("/")
    if parts:
        first = parts[0].lower()
        return GAME_NAMES.get(first, first)
    return ""


# ── 파일 스캔 ───────────────────────────────────────────────────────────────

def scan_files(game_name: str, folder_filter: str = "") -> list[Path]:
    """gdi-repo/{game}/ 아래 지원 확장자 파일 목록 수집."""
    game_dir = GDI_REPO / game_name.capitalize()
    if not game_dir.exists():
        # 대소문자 변형 시도
        for d in GDI_REPO.iterdir():
            if d.is_dir() and d.name.lower() == game_name.lower():
                game_dir = d
                break

    if not game_dir.exists():
        print(f"  [WARN] 게임 디렉토리 없음: {game_dir}")
        return []

    search_dir = game_dir
    if folder_filter:
        search_dir = game_dir / folder_filter

    files = []
    for ext in PARSERS:
        files.extend(search_dir.rglob(f"*{ext}"))

    # _images/ 폴더 내 파일 제외 (PPTX에서 추출한 이미지)
    files = [f for f in files if "_images" not in f.parts]

    return sorted(files)


# ── DB 저장 ─────────────────────────────────────────────────────────────────

def upsert_file(conn: sqlite3.Connection, rel_path: str, file_name: str,
                body_text: str, source_type: str, game_name: str,
                metadata: dict):
    """nodes + doc_content + doc_meta 저장 (upsert)."""
    if not rel_path:
        return

    # ── 1. nodes (task-116 R-J: v10 컬럼 자동 채움) ──
    _role = classify_folder_role(rel_path)
    _alias = classify_game_alias(rel_path)
    _kind = classify_file_kind(file_name)
    _ref = extract_ref_date(rel_path, file_name)

    existing = conn.execute(
        "SELECT id FROM nodes WHERE source_type='gdi' AND source_id=?",
        (rel_path,)
    ).fetchone()

    if existing:
        node_id = existing[0]
        conn.execute(
            "UPDATE nodes SET title=?, updated_at=?, "
            "folder_role=?, game_alias_kr=?, file_kind=?, ref_date=? "
            "WHERE id=?",
            (file_name, _now(), _role, _alias, _kind, _ref, node_id)
        )
    else:
        cur = conn.execute(
            "INSERT INTO nodes (source_type, source_id, title, path, node_type, "
            "space_key, url, created_at, updated_at, "
            "folder_role, game_alias_kr, file_kind, ref_date) "
            "VALUES ('gdi', ?, ?, ?, 'file', ?, '', ?, ?, ?, ?, ?, ?)",
            (rel_path, file_name, rel_path, game_name, _now(), _now(),
             _role, _alias, _kind, _ref)
        )
        node_id = cur.lastrowid

    # ── 2. doc_content ──
    char_count = len(body_text)
    body_truncated = 1 if char_count >= MAX_BODY_CHARS else 0

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

    # ── 3. doc_meta ──
    extra_meta = json.dumps({
        "file_source_type": source_type,
        "total_rows": metadata.get("total_rows", 0),
        "total_columns": metadata.get("total_columns", 0),
        "total_slides": metadata.get("total_slides", 0),
        "headers": metadata.get("headers", []),
        "parse_method": "local_file_parsers",
    }, ensure_ascii=False)

    existing_meta = conn.execute(
        "SELECT id FROM doc_meta WHERE node_id=?", (node_id,)
    ).fetchone()

    if existing_meta:
        conn.execute(
            "UPDATE doc_meta SET extra_meta=?, cached_at=? WHERE node_id=?",
            (extra_meta, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO doc_meta (node_id, status, labels, extra_meta, "
            "last_modified, cached_at, ttl_hours) "
            "VALUES (?, '', '', ?, ?, ?, 0)",
            (node_id, extra_meta, _now(), _now())
        )


# ── 메인 적재 함수 ──────────────────────────────────────────────────────────

def load_game(game_name: str, folder_filter: str = "", delta: bool = False,
              max_files: int = 0):
    """게임의 로컬 GDI 파일 일괄 적재."""
    print(f"\n{'='*60}")
    mode = "Delta" if delta else "Full"
    print(f"GDI Local {mode} 적재: {game_name}")
    if folder_filter:
        print(f"  폴더 필터: {folder_filter}")
    print(f"{'='*60}")

    t0 = time.time()

    # 1단계: 파일 스캔
    print(f"\n[1/3] 파일 스캔")
    files = scan_files(game_name, folder_filter)
    print(f"  → {len(files)}건 발견 ({time.time()-t0:.1f}s)")

    if not files:
        print("파일 없음, 종료")
        return {"scanned": 0, "added": 0, "skipped": 0, "errors": 0}

    # 확장자별 통계
    ext_stats = {}
    for f in files:
        ext = f.suffix.lower()
        ext_stats[ext] = ext_stats.get(ext, 0) + 1
    for ext, cnt in sorted(ext_stats.items()):
        print(f"    {ext}: {cnt}건")

    # DB 연결 (busy_timeout: 다른 프로세스 락 대기 최대 10초)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")

    # Delta 모드: DB 기존 파일 수집
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
        rel = _relative_path(f)
        if delta and rel in existing_paths:
            skipped += 1
            continue
        targets.append(f)

    if delta:
        print(f"  → 신규 대상: {len(targets)}건 (기존 skip: {skipped}건)")

    # max_files 제한
    if max_files > 0 and len(targets) > max_files:
        targets = targets[:max_files]
        print(f"  → --test: {max_files}건만 적재")

    if not targets:
        conn.close()
        print("적재 대상 없음, 종료")
        return {"scanned": len(files), "added": 0, "skipped": skipped, "errors": 0}

    # 2단계: 파싱 + DB 저장
    print(f"\n[2/3] 파싱 + DB 저장 (대상: {len(targets)}건)")
    added = 0
    errors = 0
    error_list = []
    started = _now()

    for i, file_path in enumerate(targets, 1):
        ext = file_path.suffix.lower()
        parser = PARSERS.get(ext)
        if not parser:
            continue

        rel_path = _relative_path(file_path)
        file_name = file_path.name
        source_type = SOURCE_TYPES.get(ext, "unknown")

        try:
            result = parser(str(file_path))
            body_text = result.get("body_text", "")
            metadata = result.get("metadata", {})

            # MAX_BODY_CHARS 제한
            if len(body_text) > MAX_BODY_CHARS:
                body_text = body_text[:MAX_BODY_CHARS] + "\n\n_(본문 잘림)_"

            upsert_file(conn, rel_path, file_name, body_text,
                       source_type, game_name, metadata)
            added += 1

        except Exception as e:
            errors += 1
            error_list.append((file_name, str(e)))
            if errors <= 5:
                print(f"    [WARN] {file_name}: {e}")

        # 진행률
        if i % COMMIT_INTERVAL == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(targets) - i) / rate if rate > 0 else 0
            print(f"  {i}/{len(targets)} 완료 ({elapsed:.0f}s, "
                  f"~{eta:.0f}s 남음, 오류: {errors})")

    conn.commit()

    # sync_log 기록
    duration = round(time.time() - t0, 2)
    sync_type = "delta_local" if delta else "full_local"
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

    # ── 폴더 택소노미 인덱스 자동 빌드 ──
    if added > 0:
        try:
            fi = FolderIndex(db_path=DB_PATH, repo_path=GDI_REPO)
            fi_result = fi.build(game_filter=game_name)
            print(f"  📂 폴더 인덱스: {fi_result['indexed']}개 항목 빌드")
        except Exception as e:
            print(f"  [WARN] 폴더 인덱스 빌드 실패: {e}")

    # 결과
    stats = {
        "scanned": len(files), "added": added,
        "skipped": skipped, "errors": errors,
        "duration_sec": duration,
    }
    print(f"\n[3/3] 적재 완료:")
    print(f"  스캔: {len(files)}건, 적재: {added}건, "
          f"스킵: {skipped}건, 오류: {errors}건")
    print(f"  소요: {duration:.1f}초")

    if error_list and len(error_list) > 5:
        print(f"  (추가 오류 {len(error_list) - 5}건 생략)")

    return stats


# ── Delta 전체 (auto_sync 연동) ──────────────────────────────────────────────

def delta_ingest_all() -> dict:
    """gdi-repo/ 아래 모든 게임 Delta 적재 (로컬 파일 기반).

    auto_sync.py에서 호출. MCP 서버 없이 로컬 파일만 사용한다.
    """
    game_dirs = [
        d.name.lower() for d in GDI_REPO.iterdir()
        if d.is_dir() and not d.name.startswith((".", "_"))
    ]

    if not game_dirs:
        print("gdi-repo/ 아래 게임 폴더 없음")
        return {}

    print(f"GDI Local Delta 대상 게임: {', '.join(sorted(game_dirs))}")
    results = {}
    for g in sorted(game_dirs):
        results[g] = load_game(g, delta=True)

    # 전체 요약
    total = {"scanned": 0, "added": 0, "skipped": 0, "errors": 0}
    for stats in results.values():
        for k in total:
            total[k] += stats.get(k, 0)

    print(f"\n{'='*60}")
    print(f"GDI Local Delta 전체 완료:")
    print(f"  게임: {len(game_dirs)}개")
    print(f"  스캔: {total['scanned']}건, 적재: {total['added']}건, "
          f"스킵: {total['skipped']}건, 오류: {total['errors']}건")
    return results


# ── 통계 ────────────────────────────────────────────────────────────────────

def print_stats(games: list[str] = None):
    """적재 통계 출력."""
    conn = sqlite3.connect(DB_PATH, timeout=10)

    if not games:
        rows = conn.execute(
            "SELECT DISTINCT space_key FROM nodes WHERE source_type='gdi' "
            "ORDER BY space_key"
        ).fetchall()
        games = [r[0] for r in rows]

    if not games:
        print("적재된 GDI 데이터 없음")
        conn.close()
        return

    for g in games:
        row = conn.execute(
            "SELECT COUNT(*), "
            "COUNT(CASE WHEN dc.body_text != '' AND dc.body_text IS NOT NULL THEN 1 END), "
            "SUM(CASE WHEN dc.char_count IS NOT NULL THEN dc.char_count ELSE 0 END) "
            "FROM nodes n LEFT JOIN doc_content dc ON dc.node_id=n.id "
            "WHERE n.source_type='gdi' AND n.space_key=?",
            (g,)
        ).fetchone()
        total_chars = row[2] or 0
        print(f"\n[{g}]")
        print(f"  노드: {row[0]}건, 본문 있음: {row[1]}건, "
              f"총 글자: {total_chars:,} ({total_chars/1024/1024:.1f}MB)")

        # 확장자별 분포
        ext_rows = conn.execute(
            "SELECT json_extract(dm.extra_meta, '$.file_source_type'), COUNT(*) "
            "FROM nodes n JOIN doc_meta dm ON dm.node_id=n.id "
            "WHERE n.source_type='gdi' AND n.space_key=? "
            "GROUP BY json_extract(dm.extra_meta, '$.file_source_type')",
            (g,)
        ).fetchall()
        if ext_rows:
            for ext, cnt in ext_rows:
                print(f"    {ext or 'unknown'}: {cnt}건")

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
                      f"추가: {s[3]}건 | {s[4]:.1f}초")

    conn.close()


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    migrate(DB_PATH)

    if "--stats" in args:
        args.remove("--stats")
        games = [_normalize_game(a) for a in args if not a.startswith("--")]
        print_stats(games or None)
        sys.exit(0)

    if "--all" in args:
        args.remove("--all")
        delta = "--delta" in args
        if delta:
            args.remove("--delta")
        # gdi-repo/ 아래 모든 게임 폴더 자동 검색
        game_dirs = [d.name.lower() for d in GDI_REPO.iterdir()
                     if d.is_dir() and not d.name.startswith((".", "_"))]
        for g in sorted(game_dirs):
            load_game(g, delta=delta)
        print_stats()
        sys.exit(0)

    delta = "--delta" in args
    if delta:
        args.remove("--delta")

    max_files = 0
    if "--test" in args:
        args.remove("--test")
        max_files = 10

    folder_filter = ""
    if "--folder" in args:
        idx = args.index("--folder")
        if idx + 1 < len(args):
            folder_filter = args[idx + 1]
            args = args[:idx] + args[idx+2:]

    games = [_normalize_game(a) for a in args if not a.startswith("--")]
    if not games:
        print("사용법: python scripts/load_gdi_local.py [--delta] [--test] GAME_NAME")
        print("  GAME_NAME: Chaoszero, Epicseven, cz, e7, --all")
        sys.exit(1)

    for g in games:
        load_game(g, folder_filter=folder_filter, delta=delta, max_files=max_files)

    print("\n")
    print_stats(games)
