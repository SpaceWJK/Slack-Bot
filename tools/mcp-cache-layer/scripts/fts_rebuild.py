"""
fts_rebuild.py — SQLite FTS5 검색 인덱스 전체 재구축

search_fts 테이블을 전량 DELETE 후 nodes + doc_content JOIN으로 재INSERT.
contentless 모드 + contentless_delete=1 활용.

사용법:
    python scripts/fts_rebuild.py                           # 기본(gdi) 전체 재구축
    python scripts/fts_rebuild.py --source-type wiki        # Wiki만
    python scripts/fts_rebuild.py --source-type jira        # Jira만
    python scripts/fts_rebuild.py --all                     # 모든 source_type
    python scripts/fts_rebuild.py --game chaoszero          # gdi + 특정 게임만
    python scripts/fts_rebuild.py --verify-only             # 현재 커버리지만 확인
    python scripts/fts_rebuild.py --batch-size 1000         # 배치 크기 조정

task-077 구현, task-086에서 source_type 일반화 (Wiki/Jira/GDI 공통).
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.models import get_connection  # noqa: E402
from src import config as cache_config  # noqa: E402


def get_coverage_stats(conn: sqlite3.Connection, source_type: str = 'gdi',
                       space_key: str = None) -> dict:
    """현재 FTS 커버리지 측정."""
    where_extra = ""
    params = [source_type]
    if space_key:
        where_extra = "AND n.space_key = ?"
        params.append(space_key.lower())

    # 인덱싱 대상 = body_text 비어있지 않은 nodes
    eligible = conn.execute(f"""
        SELECT COUNT(*) FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.source_type = ?
          AND dc.body_text IS NOT NULL
          AND dc.body_text != ''
          {where_extra}
    """, params).fetchone()[0]

    # FTS 커버 = eligible 중 FTS에 rowid 존재
    covered = conn.execute(f"""
        SELECT COUNT(*) FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.source_type = ?
          AND dc.body_text IS NOT NULL
          AND dc.body_text != ''
          AND EXISTS (SELECT 1 FROM search_fts f WHERE f.rowid = n.id)
          {where_extra}
    """, params).fetchone()[0]

    # FTS 전체 크기
    fts_total = conn.execute("SELECT COUNT(*) FROM search_fts").fetchone()[0]

    # Ghost (FTS에는 있지만 nodes에 없는 rowid)
    ghost = conn.execute("""
        SELECT COUNT(*) FROM search_fts
        WHERE rowid NOT IN (SELECT id FROM nodes)
    """).fetchone()[0]

    return {
        "eligible_nodes": eligible,
        "covered_in_fts": covered,
        "fts_total": fts_total,
        "ghost_entries": ghost,
        "coverage_pct": (covered * 100 / eligible) if eligible > 0 else 0,
    }


def rebuild_fts(conn: sqlite3.Connection, batch_size: int = 500,
                source_type: str = 'gdi', space_key: str = None) -> dict:
    """FTS 전체 재구축.

    1. DELETE 대상 범위 (게임 필터 있으면 해당 게임만)
    2. 배치 INSERT (500건 단위 중간 커밋)
    3. 검증 쿼리 실행

    Returns: {"deleted": N, "inserted": N, "elapsed_sec": X.X, "verify": {...}}
    """
    t0 = time.time()

    # 1. DELETE — source_type 범위로 한정 (task-086 수정: Wiki 재구축 시 GDI 파괴 방지)
    if space_key:
        # 해당 게임 노드만 삭제
        del_cur = conn.execute("""
            DELETE FROM search_fts
            WHERE rowid IN (
                SELECT n.id FROM nodes n
                WHERE n.source_type = ? AND n.space_key = ?
            )
        """, (source_type, space_key.lower()))
    else:
        # source_type 범위로만 삭제 (다른 source_type 영향 없음)
        del_cur = conn.execute("""
            DELETE FROM search_fts
            WHERE rowid IN (
                SELECT n.id FROM nodes n
                WHERE n.source_type = ?
            )
        """, (source_type,))
    deleted = del_cur.rowcount
    conn.commit()
    print(f"  [DELETE] {deleted}건 삭제 완료")

    # 2. INSERT (eligible 대상만)
    where_extra = ""
    params = [source_type]
    if space_key:
        where_extra = "AND n.space_key = ?"
        params.append(space_key.lower())

    cur = conn.execute(f"""
        SELECT n.id, n.title, dc.body_text, dc.summary, dc.keywords
        FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.source_type = ?
          AND dc.body_text IS NOT NULL
          AND dc.body_text != ''
          {where_extra}
        ORDER BY n.id
    """, params)

    batch = []
    inserted = 0
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        batch_data = [
            (r['id'], r['title'] or '',
             r['body_text'], r['summary'] or '', r['keywords'] or '')
            for r in rows
        ]
        conn.executemany(
            "INSERT INTO search_fts(rowid, title, body_text, summary, keywords) "
            "VALUES (?, ?, ?, ?, ?)",
            batch_data
        )
        conn.commit()  # 배치 단위 커밋 (재시작 안전성)
        inserted += len(batch_data)
        print(f"  [INSERT] {inserted}건 재구축...")

    elapsed = time.time() - t0

    # 3. 검증
    verify = get_coverage_stats(conn, source_type, space_key)

    return {
        "deleted": deleted,
        "inserted": inserted,
        "elapsed_sec": elapsed,
        "verify": verify,
    }


def main():
    parser = argparse.ArgumentParser(
        description="SQLite FTS5 검색 인덱스 재구축 (task-077/086)",
    )
    parser.add_argument("--source-type", type=str, default="gdi",
                        help="재구축할 source_type (gdi/wiki/jira). 기본: gdi")
    parser.add_argument("--all", action="store_true",
                        help="모든 source_type 재구축 (gdi + wiki + jira). --source-type 무시")
    parser.add_argument("--game", type=str, default=None,
                        help="특정 게임(space_key)만 재구축 — gdi에서 주로 사용")
    parser.add_argument("--verify-only", action="store_true",
                        help="재구축 없이 현재 커버리지만 확인")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="배치 INSERT 크기 (기본: 500)")
    args = parser.parse_args()

    db_path = Path(cache_config.DB_PATH)
    if not db_path.exists():
        print(f"오류: DB 파일 없음: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = get_connection(str(db_path))

    # task-086: source_type 리스트 결정
    if args.all:
        source_types = ["gdi", "wiki", "jira"]
    else:
        source_types = [args.source_type]

    total_deleted = 0
    total_inserted = 0
    total_elapsed = 0.0

    for st in source_types:
        space_key = args.game if st == "gdi" else None
        scope = f"{st}" + (f" / 게임 '{space_key}'" if space_key else "")
        print(f"\n{'=' * 60}")
        print(f"FTS5 {'커버리지 확인' if args.verify_only else '재구축'} ({scope})")
        print(f"{'=' * 60}")

        # 사전 검증
        print("[사전 상태]")
        before = get_coverage_stats(conn, st, space_key)
        print(f"  인덱싱 대상 (body_text != ''): {before['eligible_nodes']}")
        print(f"  FTS 커버: {before['covered_in_fts']} ({before['coverage_pct']:.1f}%)")
        print(f"  FTS 총 레코드: {before['fts_total']}")
        print(f"  Ghost 레코드: {before['ghost_entries']}")

        if args.verify_only:
            continue

        print(f"\n[재구축 시작 - {st}]")
        result = rebuild_fts(conn, batch_size=args.batch_size,
                             source_type=st, space_key=space_key)

        print(f"\n[{st} 완료] {result['elapsed_sec']:.1f}초")
        print(f"  삭제: {result['deleted']}건")
        print(f"  재INSERT: {result['inserted']}건")

        total_deleted += result['deleted']
        total_inserted += result['inserted']
        total_elapsed += result['elapsed_sec']

    if not args.verify_only and len(source_types) > 1:
        print(f"\n{'=' * 60}")
        print(f"[전체 합계] {total_elapsed:.1f}초")
        print(f"  총 삭제: {total_deleted}건")
        print(f"  총 재INSERT: {total_inserted}건")
        print(f"{'=' * 60}")

    # 사후 검증 + 최종 판정 (all 모드에서 모든 source_type 검증)
    all_ok = True
    if not args.verify_only:
        print("\n[사후 검증]")
        for st in source_types:
            space_key = args.game if st == "gdi" else None
            v = get_coverage_stats(conn, st, space_key)
            print(f"  [{st}] 대상: {v['eligible_nodes']}, 커버: {v['covered_in_fts']} "
                  f"({v['coverage_pct']:.1f}%), Ghost: {v['ghost_entries']}")
            if v['eligible_nodes'] > 0:
                if v['coverage_pct'] < 99.99 or v['ghost_entries'] > 0:
                    all_ok = False
        print(f"\n결과: {'✓ PASS' if all_ok else '✗ FAIL'}")

    conn.close()


if __name__ == "__main__":
    main()
