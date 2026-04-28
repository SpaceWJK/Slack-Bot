"""migrate_v4_to_v5.py — FTS5 unicode61 → trigram 마이그레이션 (task-102)

--apply  : WAL checkpoint → 백업 → 스키마 v5 적용 → FTS 전체 재인덱싱
--rollback: trigram 드롭 → unicode61 복원 → 재인덱싱 → 수동 조치 안내

사용법:
    python scripts/migrate_v4_to_v5.py --apply [--db /path/to/mcp_cache.db]
    python scripts/migrate_v4_to_v5.py --rollback [--db /path/to/mcp_cache.db]
"""
import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.models import get_connection, migrate  # noqa: E402
from src import config as cache_config  # noqa: E402

# ── 롤백 SQL: trigram → unicode61 복원 ──────────────────────────────────────

ROLLBACK_SQL = """
DROP TABLE IF EXISTS search_fts;
CREATE VIRTUAL TABLE search_fts USING fts5(
    title, body_text, summary, keywords,
    content='', contentless_delete=1,
    tokenize='unicode61'
);
DELETE FROM schema_version WHERE version = 5;
"""


# ── 내부 헬퍼 ────────────────────────────────────────────────────────────────

def _run_rebuild(db_path: str) -> None:
    """FTS 전체 재구축 (gdi/wiki/jira 모두)."""
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    from fts_rebuild import rebuild_fts, get_coverage_stats  # noqa: E402
    conn = get_connection(db_path)
    try:
        for source_type in ['gdi', 'wiki', 'jira']:
            result = rebuild_fts(conn, source_type=source_type)
            stats = get_coverage_stats(conn, source_type)
            print(f"  [{source_type}] {result['inserted']}건 "
                  f"({stats['coverage_pct']:.1f}%)")
    finally:
        conn.close()


# ── apply ────────────────────────────────────────────────────────────────────

def apply_v5(db_path: str) -> None:
    """v4 → v5 마이그레이션 적용."""
    print(f"[v4→v5] {db_path}")

    # 0. 멱등성 체크: 이미 v5이면 rebuild skip (T-12)
    conn_check = get_connection(db_path)
    try:
        row = conn_check.execute("SELECT MAX(version) FROM schema_version").fetchone()
        pre_version = row[0] if row and row[0] else 0
    finally:
        conn_check.close()

    if pre_version >= 5:
        print(f"  이미 schema v{pre_version} — 재인덱싱 skip")
        return

    # 1. WAL checkpoint (WAL 파일을 메인 DB에 병합 후 close)
    conn = get_connection(db_path)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    print("  WAL checkpoint 완료")

    # 2. 백업
    backup = Path(db_path).with_suffix('.v4_backup.db')
    shutil.copy2(db_path, backup)
    print(f"  백업: {backup}")

    # 3. 마이그레이션 (MIGRATIONS[5] 적용)
    version = migrate(db_path)
    print(f"  스키마 v{version} 적용 완료")

    # 4. FTS 전체 재인덱싱
    print("  FTS 재인덱싱 시작...")
    _run_rebuild(db_path)
    print("  완료")


# ── rollback ─────────────────────────────────────────────────────────────────

def rollback_v5(db_path: str) -> None:
    """v5 → v4 롤백 (trigram → unicode61 복원)."""
    print(f"[v5→v4 롤백] {db_path}")

    conn = get_connection(db_path)
    conn.executescript(ROLLBACK_SQL)
    conn.commit()
    conn.close()

    print("  스키마 롤백 완료 (unicode61 복원)")
    print("  FTS 재인덱싱 시작...")
    _run_rebuild(db_path)
    print("  재구축 완료")
    print("  !! 주의: src/models.py MIGRATIONS[5] 주석 처리 후 Slack Bot 재시작 필요.")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="FTS5 unicode61→trigram 마이그레이션 (task-102)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--apply", action="store_true",
                       help="v4→v5 마이그레이션 적용")
    group.add_argument("--rollback", action="store_true",
                       help="v5→v4 롤백 (unicode61 복원)")
    parser.add_argument("--db", default=None,
                        help="DB 경로 (기본: cache_config.DB_PATH)")
    args = parser.parse_args()

    db_path = args.db or str(cache_config.DB_PATH)

    if args.apply:
        apply_v5(db_path)
    elif args.rollback:
        rollback_v5(db_path)


if __name__ == "__main__":
    main()
