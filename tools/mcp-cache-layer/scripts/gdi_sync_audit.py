"""
gdi_sync_audit.py — gdi-repo ↔ DB 동기화 정합성 감사 스크립트

파일시스템(gdi-repo/)과 SQLite DB(cache/mcp_cache.db)의 GDI 적재 상태를 비교하여
orphan(DB only) / missing(FS only) 레코드를 리포트. 필요 시 orphan 정리.

사용법:
    python scripts/gdi_sync_audit.py                        # 전 게임 감사
    python scripts/gdi_sync_audit.py --game Chaoszero       # 특정 게임만
    python scripts/gdi_sync_audit.py --output audit.json    # JSON 리포트 저장
    python scripts/gdi_sync_audit.py --dry-run --apply      # 영향 예상만 (삭제 안 함)
    python scripts/gdi_sync_audit.py --apply --game Chaoszero  # 실제 정리 (2회 승인 필요)

주요 기능:
  - FS/DB 수 비교 + diff 분류
  - orphan 카테고리 분류 (double_slash / non_gdi_ext / root_level / other)
  - WAL-safe 백업 (sqlite3.Connection.backup API)
  - FTS5 사전 삭제 (contentless_delete=1 활용)
  - FK CASCADE로 nodes → doc_content/doc_meta 연쇄 삭제
  - 삭제 후 4가지 검증 쿼리 실행

task-076 구현.
"""

import argparse
import json
import re
import sqlite3
import sys
from collections import OrderedDict
from datetime import datetime
from pathlib import Path


# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.models import get_connection  # noqa: E402
from src import config as cache_config  # noqa: E402


# ── 상수 ────────────────────────────────────────────────────────────────────

GDI_EXTENSIONS = ('.xlsx', '.tsv', '.pptx')
DEFAULT_REPO_PATH = PROJECT_ROOT / "gdi-repo"
MAX_BACKUPS = 3  # 최근 N개 백업만 유지

# 게임 이름 정규화 (CLI 입력 → DB space_key)
# load_gdi.py GAME_NAMES와 동일 규칙: 소문자
GAME_KEYS = {
    "epicseven": "epicseven",
    "chaoszero": "chaoszero",
    "lordnine_asia": "lordnine_asia",
}

# gdi-repo 하위 게임 폴더명 (대소문자 보존)
GAME_FS_NAMES = {
    "epicseven": "Epicseven",
    "chaoszero": "Chaoszero",
    "lordnine_asia": "Lordnine_Asia",
}


# ── 유틸리티 ──────────────────────────────────────────────────────────────

def normalize_path(p: str) -> str:
    """경로 정규화: 백슬래시 → 슬래시, 연속 슬래시 단일화, trailing 슬래시 제거,
    소문자화 (Windows 파일시스템 케이스 비민감 매칭).

    task-116 정정 (2026-04-25): cache nodes.path를 S3 표준 'Lordnine_Asia/' 대문자 A로 통일.
    Windows 기본 파일시스템이 case-insensitive이므로 DB/FS 매칭도 소문자 기준.
    """
    p = p.replace("\\", "/")
    p = re.sub(r'/+', '/', p)
    return p.rstrip('/').lower()


def normalize_game_key(cli_game: str) -> str:
    """CLI 입력 → DB space_key (소문자 정규화).

    'Chaoszero', 'chaoszero', 'CHAOSZERO' 모두 'chaoszero'로.
    """
    key = cli_game.lower().strip()
    if key not in GAME_KEYS:
        raise ValueError(
            f"알 수 없는 게임: {cli_game!r}. "
            f"지원: {', '.join(GAME_KEYS.keys())}"
        )
    return GAME_KEYS[key]


# ── 수집 ────────────────────────────────────────────────────────────────

def collect_fs_files(repo_root: Path, game_key: str) -> set[str]:
    """파일시스템에서 GDI 파일을 수집하여 정규화된 상대경로 set 반환.

    반환 예: {"Chaoszero/TSV/xxx.tsv", ...}
    """
    fs_name = GAME_FS_NAMES[game_key]
    game_dir = repo_root / fs_name
    if not game_dir.exists():
        return set()

    result = set()
    for ext in GDI_EXTENSIONS:
        for f in game_dir.rglob(f"*{ext}"):
            try:
                rel = f.relative_to(repo_root).as_posix()
                result.add(normalize_path(rel))
            except ValueError:
                # 심볼릭 링크 등으로 상대경로 계산 실패 시 스킵
                continue
    return result


def collect_db_files(conn: sqlite3.Connection, game_key: str) -> tuple[dict[str, int], list[int]]:
    """DB에서 nodes 조회하여 (정규화_path → node.id, 중복_ids) 반환.

    Returns:
        (unique_map, duplicate_ids)
        unique_map: 정규화 후 첫번째 id만 유지한 dict
        duplicate_ids: 정규화 후 중복된 나머지 레코드의 id 리스트 (정리 대상)
    """
    rows = conn.execute("""
        SELECT source_id, id FROM nodes
        WHERE source_type = 'gdi' AND space_key = ?
    """, (game_key,)).fetchall()
    unique_map = {}
    duplicate_ids = []
    for r in rows:
        norm = normalize_path(r['source_id'])
        if norm in unique_map:
            # 중복: 가장 낮은 id만 남기고 나머지는 정리 대상
            existing_id = unique_map[norm]
            if r['id'] < existing_id:
                duplicate_ids.append(existing_id)
                unique_map[norm] = r['id']
            else:
                duplicate_ids.append(r['id'])
        else:
            unique_map[norm] = r['id']
    return unique_map, duplicate_ids


# ── 분류 ────────────────────────────────────────────────────────────────

# 순서 의존: 첫 매치 반환. "other"는 반드시 마지막.
CATEGORY_RULES = OrderedDict([
    ("double_slash",  lambda p: "//" in p),
    ("non_gdi_ext",   lambda p: not p.lower().endswith(GDI_EXTENSIONS)),
    ("root_level",    lambda p: p.count('/') <= 1),
    ("other",         lambda p: True),
])


def classify_orphan(path: str) -> str:
    """orphan 경로를 카테고리로 분류. 첫 매칭 반환."""
    for category, check in CATEGORY_RULES.items():
        if check(path):
            return category
    return "other"


# ── 백업 ────────────────────────────────────────────────────────────────

def backup_db(src_conn: sqlite3.Connection, db_path: Path) -> Path:
    """WAL-safe DB 백업. sqlite3.Connection.backup() 사용."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak_path = db_path.with_suffix(f".db.bak_{ts}")
    with sqlite3.connect(str(bak_path)) as bak_conn:
        src_conn.backup(bak_conn)
    return bak_path


def rotate_backups(db_path: Path, keep: int = MAX_BACKUPS):
    """db.bak_* 파일을 최신 N개만 유지하고 나머지 삭제."""
    pattern = f"{db_path.name}.bak_*"
    backups = sorted(
        db_path.parent.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in backups[keep:]:
        try:
            old.unlink()
            print(f"  [rotate] 삭제: {old.name}")
        except OSError as e:
            print(f"  [rotate] 삭제 실패: {old.name} ({e})")


# ── 정리 ────────────────────────────────────────────────────────────────

def delete_orphans(conn: sqlite3.Connection, orphan_ids: list[int]) -> int:
    """orphan 레코드 삭제. FTS 사전 정리 + FK CASCADE.

    1. search_fts에서 rowid 삭제 (contentless_delete=1 지원)
    2. nodes 삭제 → doc_content/doc_meta CASCADE
    """
    if not orphan_ids:
        return 0

    # FK CASCADE 보장 (이중 안전망)
    conn.execute("PRAGMA foreign_keys = ON")

    placeholders = ",".join("?" * len(orphan_ids))

    # 1. FTS 사전 삭제
    conn.execute(
        f"DELETE FROM search_fts WHERE rowid IN ({placeholders})",
        orphan_ids,
    )

    # 2. nodes 삭제 (CASCADE로 doc_content/doc_meta 연쇄)
    cur = conn.execute(
        f"DELETE FROM nodes WHERE id IN ({placeholders})",
        orphan_ids,
    )
    conn.commit()
    return cur.rowcount


def verify_cleanup(conn: sqlite3.Connection, game_key: str) -> dict:
    """정리 후 검증 쿼리 4종 실행. 모두 기대값과 일치해야 PASS."""
    r = {}
    r['nodes_count'] = conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE source_type='gdi' AND space_key = ?",
        (game_key,)
    ).fetchone()[0]
    r['orphan_doc_content'] = conn.execute(
        "SELECT COUNT(*) FROM doc_content WHERE node_id NOT IN (SELECT id FROM nodes)"
    ).fetchone()[0]
    r['orphan_doc_meta'] = conn.execute(
        "SELECT COUNT(*) FROM doc_meta WHERE node_id NOT IN (SELECT id FROM nodes)"
    ).fetchone()[0]
    r['ghost_fts'] = conn.execute(
        "SELECT COUNT(*) FROM search_fts WHERE rowid NOT IN (SELECT id FROM nodes)"
    ).fetchone()[0]
    return r


# ── 감사 ────────────────────────────────────────────────────────────────

def audit_game(conn: sqlite3.Connection, repo_root: Path, game_key: str) -> dict:
    """단일 게임 감사. orphan/duplicate/missing 리포트 반환."""
    fs_paths = collect_fs_files(repo_root, game_key)
    db_map, duplicate_ids = collect_db_files(conn, game_key)

    fs_count = len(fs_paths)
    # 실제 DB 레코드 수 = unique + duplicate
    db_raw_count = conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE source_type='gdi' AND space_key=?",
        (game_key,)
    ).fetchone()[0]

    db_paths = set(db_map.keys())
    orphan_paths = db_paths - fs_paths  # DB only (삭제 대상)
    missing_paths = fs_paths - db_paths  # FS only (적재 필요)

    # orphan 상세 (id + 카테고리)
    orphans = []
    cat_counter = {k: 0 for k in CATEGORY_RULES}
    for p in sorted(orphan_paths):
        cat = classify_orphan(p)
        cat_counter[cat] += 1
        orphans.append({
            "node_id": db_map[p],
            "source_id": p,
            "category": cat,
        })

    return {
        "game_key": game_key,
        "fs_name": GAME_FS_NAMES[game_key],
        "fs_count": fs_count,
        "db_count": db_raw_count,
        "db_unique_count": len(db_map),
        "diff": db_raw_count - fs_count,
        "orphan_count": len(orphan_paths),
        "duplicate_count": len(duplicate_ids),
        "duplicate_ids": duplicate_ids,
        "missing_count": len(missing_paths),
        "orphans": orphans,
        "missing": sorted(missing_paths)[:100],
        "category_counts": cat_counter,
        "clean": (
            len(orphan_paths) == 0
            and len(missing_paths) == 0
            and len(duplicate_ids) == 0
        ),
    }


# ── 리포트 ──────────────────────────────────────────────────────────────

def print_report(results: list[dict]):
    """stdout에 사람 친화 리포트 출력."""
    print()
    print("=" * 60)
    print(f"GDI Sync Audit Report — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)
    print()

    total_orphans = 0
    dirty_games = 0

    for r in results:
        status = "CLEAN" if r['clean'] else "DIRTY"
        print(f"[{r['fs_name']:15}] FS: {r['fs_count']:>6} | DB: {r['db_count']:>6} | "
              f"diff: {r['diff']:+d}  {status}")

        if r.get('duplicate_count', 0):
            dirty_games += 1 if r['orphan_count'] == 0 else 0
            print(f"  ├─ 중복 레코드       : {r['duplicate_count']} (정규화 후 동일 경로)")

        if r['orphan_count']:
            dirty_games += 1
            total_orphans += r['orphan_count']
            # 분류 합산 검증
            cat_sum = sum(r['category_counts'].values())
            for cat, cnt in r['category_counts'].items():
                if cnt > 0:
                    print(f"  ├─ {cat:20}: {cnt}")
            # 분류 합산 = orphan 총합
            if cat_sum != r['orphan_count']:
                print(f"  !! 분류 합산 {cat_sum} != orphan 총합 {r['orphan_count']} (분류 로직 오류)")
            else:
                print(f"  └─ 분류 합산       : {cat_sum} ✓")

        if r['missing_count']:
            print(f"  ⚠ FS only (DB 적재 필요): {r['missing_count']}건")
            for p in r['missing'][:5]:
                print(f"     - {p}")
            if r['missing_count'] > 5:
                print(f"     ... 외 {r['missing_count'] - 5}건")

        print()

    print(f"Summary: {dirty_games} game(s) need cleanup, total {total_orphans} orphan(s)")
    print()


# ── CLI ─────────────────────────────────────────────────────────────────

def confirm_apply(game_label: str, orphan_count: int, backup_path: Path) -> bool:
    """2회 승인 프롬프트. CRITICAL 데이터 보호."""
    print()
    print(f"경고: [{game_label}]에서 {orphan_count}건의 orphan 레코드를 삭제합니다.")
    print(f"  백업: {backup_path}")
    print(f"  영향: nodes + doc_content + doc_meta (CASCADE) + search_fts (사전 삭제)")

    a1 = input(f"\n1차 확인 — 계속하시겠습니까? [y/N]: ").strip().lower()
    if a1 != 'y':
        return False

    a2 = input(
        f"2차 확인 — 정말로 {orphan_count}건을 삭제합니까?\n"
        f"   'DELETE' 입력 시 진행, 그 외는 취소: "
    ).strip()
    return a2 == 'DELETE'


def main():
    parser = argparse.ArgumentParser(
        description="GDI 캐시 DB 동기화 정합성 감사",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--game", type=str, default=None,
                        help="특정 게임만 (Epicseven/Chaoszero/Lordnine_Asia). "
                             "미지정 시 전 게임")
    parser.add_argument("--repo-path", type=Path, default=DEFAULT_REPO_PATH,
                        help=f"gdi-repo 경로 (기본: {DEFAULT_REPO_PATH})")
    parser.add_argument("--output", type=Path, default=None,
                        help="JSON 리포트 저장 경로")
    parser.add_argument("--dry-run", action="store_true",
                        help="영향 예상만 출력 (실제 삭제 안 함)")
    parser.add_argument("--apply", action="store_true",
                        help="orphan 실제 삭제 (2회 승인 필요)")
    parser.add_argument("--no-backup", action="store_true",
                        help="⚠️  백업 스킵 (데이터 복구 불가, CI 전용)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="상세 로그")

    args = parser.parse_args()

    # 대상 게임 결정
    if args.game:
        try:
            target_keys = [normalize_game_key(args.game)]
        except ValueError as e:
            print(f"오류: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        target_keys = list(GAME_KEYS.values())

    # 경로 검증
    if not args.repo_path.exists():
        print(f"오류: gdi-repo 경로 없음: {args.repo_path}", file=sys.stderr)
        sys.exit(1)

    # DB 연결 (get_connection으로 일관성)
    db_path = Path(cache_config.DB_PATH)
    if not db_path.exists():
        print(f"오류: DB 파일 없음: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = get_connection(str(db_path))

    # 감사 실행
    results = [audit_game(conn, args.repo_path, k) for k in target_keys]
    print_report(results)

    # JSON 리포트
    if args.output:
        args.output.write_text(
            json.dumps({
                "audit_time": datetime.now().isoformat(),
                "games": {r['fs_name']: r for r in results},
            }, indent=2, ensure_ascii=False),
            encoding='utf-8',
        )
        print(f"JSON 리포트 저장: {args.output}\n")

    # 정리 실행
    if args.apply:
        dirty = [r for r in results if r['orphan_count'] > 0 or r.get('duplicate_count', 0) > 0]
        if not dirty:
            print("정리할 orphan/duplicate 없음. 완료.")
            conn.close()
            return

        for r in dirty:
            total_to_delete = r['orphan_count'] + r.get('duplicate_count', 0)

            if args.dry_run:
                print(f"[DRY-RUN] {r['fs_name']}: orphan {r['orphan_count']} + "
                      f"duplicate {r.get('duplicate_count', 0)} = {total_to_delete}건 삭제 예정")
                continue

            # 백업
            backup_path = None
            if not args.no_backup:
                backup_path = backup_db(conn, db_path)
                print(f"\n[{r['fs_name']}] 백업 생성: {backup_path}")
                rotate_backups(db_path)

            # 2회 승인
            if not confirm_apply(r['fs_name'], total_to_delete, backup_path or Path("(no backup)")):
                print(f"[{r['fs_name']}] 취소됨")
                continue

            # 삭제: orphan + duplicate
            all_ids = [o['node_id'] for o in r['orphans']] + r.get('duplicate_ids', [])
            deleted = delete_orphans(conn, all_ids)
            print(f"\n[{r['fs_name']}] 삭제 완료: {deleted}건 (orphan {r['orphan_count']} + "
                  f"duplicate {r.get('duplicate_count', 0)})")

            # 검증
            v = verify_cleanup(conn, r['game_key'])
            print(f"  검증:")
            print(f"    nodes count:       {v['nodes_count']} (기대: {r['fs_count']})")
            print(f"    orphan doc_content: {v['orphan_doc_content']} (기대: 0)")
            print(f"    orphan doc_meta:   {v['orphan_doc_meta']} (기대: 0)")
            print(f"    ghost FTS:         {v['ghost_fts']} (기대: 0)")
            ok = (v['nodes_count'] == r['fs_count']
                  and v['orphan_doc_content'] == 0
                  and v['orphan_doc_meta'] == 0
                  and v['ghost_fts'] == 0)
            print(f"  결과: {'✓ PASS' if ok else '✗ FAIL'}")

    conn.close()


if __name__ == "__main__":
    main()
