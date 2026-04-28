"""task-115 R-1 backfill — nodes 4 컬럼 자동 채움.

대전제 (Slack Bot 고도화 종착지 축 1): AI 맥락 이해 + 원본 보존 적재 → 데이터 계층 라벨링.

채움 컬럼:
- folder_role: 폴더 의미 (planning/qa_result/live_issue/game_data/dashboard/unknown)
- game_alias_kr: 게임 한글 alias (카제나/에픽세븐/로드나인 아시아)
- file_kind: 파일 종류 9종 (patch_note/qa_check_list/bat_result/...)
- ref_date: ISO 8601 (YYYY-MM-DD), 폴더/파일명 정규식 추출

실행:
    cd "D:/Vibe Dev/QA Ops/mcp-cache-layer"
    python scripts/backfill_folder_role.py
    python scripts/backfill_folder_role.py --sample 100  # 검수 샘플 출력
    python scripts/backfill_folder_role.py --dry-run     # UPDATE 미적용
"""

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

# ── 매핑 정의 (Step 1 SSoT 정합) ────────────────────────────────

# §2.1 Chaoszero 의미 폴더 5종 → folder_role
FOLDER_ROLE_MAP = {
    "Update Review": "planning",
    "Test Result": "qa_result",
    "Live Issue": "live_issue",
    "TSV": "game_data",
    "Dashboard": "dashboard",
}

# task-116 추가 fix: case-insensitive lookup (S3 prefix case 변형 대비 — 예: Epicseven/Live issue 소문자 i)
_FOLDER_ROLE_MAP_LC = {k.lower(): v for k, v in FOLDER_ROLE_MAP.items()}

# §6.3 게임 영문 폴더 (cache nodes.path 첫 segment) → 한글 alias
GAME_FOLDER_TO_KR = {
    "Chaoszero": "카제나",
    "Epicseven": "에픽세븐",
    "Lordnine_Asia": "로드나인 아시아",
}
# task-116 추가: case-insensitive (S3 case 변형 대비 — 예: Lordnine_Asia 대문자 A)
_GAME_FOLDER_TO_KR_LC = {k.lower(): v for k, v in GAME_FOLDER_TO_KR.items()}

# §2.3 파일명 명명 규칙 9종 → file_kind
# 우선순위: 더 specific한 패턴 먼저 (위에서 아래로 매칭)
FILE_KIND_PATTERNS = [
    (re.compile(r"^패치노트_\d{8}_"), "patch_note"),
    (re.compile(r"^QA_CL_"), "qa_check_list"),
    (re.compile(r"_BAT_\d{6,8}\.xlsx$"), "bat_result"),
    (re.compile(r"_Bug Verification_\d{6,8}\.xlsx$"), "bug_verification"),
    (re.compile(r"_Update Checklist_\d{6,8}\.xlsx$"), "update_checklist"),
    (re.compile(r"_커뮤니티버그_\d{6,8}\.xlsx$"), "live_issue_community"),
    (re.compile(r"_CS인앱이슈_\d{6,8}\.xlsx$"), "live_issue_cs"),
    (re.compile(r"^\d{6}_공통 참고 문서_"), "common_ref_doc"),
    (re.compile(r"^#\d+\s+[\w가-힣]+\s+-"), "issue_unit_planning"),
]

# §2.4 ref_date 추출 정규식
_DATE_8 = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")  # YYYYMMDD (2026~)
_DATE_6 = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")    # YYMMDD (25~ → 2025~)


def _valid_md(month: str, day: str) -> bool:
    """MINOR-2 fix: 월(01~12) + 일(01~31) 범위 검증."""
    try:
        m, d = int(month), int(day)
        return 1 <= m <= 12 and 1 <= d <= 31
    except (TypeError, ValueError):
        return False


def extract_ref_date(path: str, title: str) -> str | None:
    """폴더 segment 우선 (3-depth 날짜 폴더), 파일명 fallback."""
    if not path:
        return None
    segments = path.split("/")
    # 폴더 segment 우선 (역순 — 가장 깊은 segment부터)
    candidates = list(reversed(segments[:-1])) + [title or ""]
    for seg in candidates:
        if not seg:
            continue
        m8 = _DATE_8.search(seg)
        if m8 and _valid_md(m8.group(2), m8.group(3)):
            return f"{m8.group(1)}-{m8.group(2)}-{m8.group(3)}"
        m6 = _DATE_6.search(seg)
        if m6 and _valid_md(m6.group(2), m6.group(3)):
            yy = int(m6.group(1))
            year = 2000 + yy if yy < 70 else 1900 + yy
            return f"{year:04d}-{m6.group(2)}-{m6.group(3)}"
    return None


def classify_folder_role(path: str) -> str:
    """nodes.path에서 의미 폴더 segment 추출 → folder_role.

    case-insensitive lookup (task-116 fix): S3 prefix case 변형 (예: Live issue 소문자 i) 대비.
    """
    if not path:
        return "unknown"
    segments = path.split("/")
    for seg in segments[1:3]:
        v = _FOLDER_ROLE_MAP_LC.get(seg.lower())
        if v:
            return v
    return "unknown"


def classify_game_alias(path: str) -> str | None:
    """nodes.path 첫 segment → 한글 alias (case-insensitive lookup)."""
    if not path:
        return None
    first = path.split("/", 1)[0]
    return _GAME_FOLDER_TO_KR_LC.get(first.lower())


def classify_file_kind(title: str) -> str | None:
    """파일명 → file_kind (9종 패턴)."""
    if not title:
        return None
    for pat, kind in FILE_KIND_PATTERNS:
        if pat.search(title):
            return kind
    return None


def backfill(db_path: Path, *, dry_run: bool = False, sample: int = 0) -> dict:
    """nodes 4 컬럼 backfill 실행."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    # 대상: source_type='gdi' 전수
    rows = cur.execute(
        "SELECT id, path, title FROM nodes WHERE source_type='gdi'"
    ).fetchall()

    stats = {
        "total": len(rows),
        "folder_role": {"planning": 0, "qa_result": 0, "live_issue": 0,
                        "game_data": 0, "dashboard": 0, "unknown": 0},
        "game_alias_kr": {},
        "file_kind": {},
        "ref_date": {"filled": 0, "null": 0},
    }

    updates = []
    for nid, path, title in rows:
        role = classify_folder_role(path)
        alias = classify_game_alias(path)
        kind = classify_file_kind(title)
        ref = extract_ref_date(path, title)

        stats["folder_role"][role] = stats["folder_role"].get(role, 0) + 1
        if alias:
            stats["game_alias_kr"][alias] = stats["game_alias_kr"].get(alias, 0) + 1
        if kind:
            stats["file_kind"][kind] = stats["file_kind"].get(kind, 0) + 1
        if ref:
            stats["ref_date"]["filled"] += 1
        else:
            stats["ref_date"]["null"] += 1

        updates.append((role, alias, kind, ref, nid))

    # 샘플 출력 (검수용)
    if sample > 0:
        print(f"\n=== Sample ({min(sample, len(updates))}건) ===")
        step = max(1, len(updates) // sample)
        for i in range(0, len(updates), step):
            if i >= sample * step:
                break
            r, a, k, rf, nid = updates[i]
            p, t = rows[i][1], rows[i][2]
            print(f"  id={nid} role={r} alias={a} kind={k} ref={rf}")
            print(f"    path={p[:70]}")
            print(f"    title={(t or '')[:60]}")

    # UPDATE 실행
    if not dry_run:
        cur.executemany(
            "UPDATE nodes SET folder_role=?, game_alias_kr=?, file_kind=?, ref_date=? WHERE id=?",
            updates,
        )
        conn.commit()
        print(f"\n[backfill] UPDATE 적용: {len(updates)}건")
    else:
        print(f"\n[backfill] dry-run — UPDATE 미적용 ({len(updates)}건 대상)")

    conn.close()
    return stats


def main():
    ap = argparse.ArgumentParser(description="task-115 R-1 backfill")
    ap.add_argument("--db", default="cache/mcp_cache.db", help="mcp_cache.db 경로")
    ap.add_argument("--dry-run", action="store_true", help="UPDATE 미적용")
    ap.add_argument("--sample", type=int, default=0, help="검수 샘플 출력 건수")
    args = ap.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"오류: DB 부재: {db_path}", file=sys.stderr)
        sys.exit(1)

    stats = backfill(db_path, dry_run=args.dry_run, sample=args.sample)

    print("\n=== 통계 ===")
    print(f"  total: {stats['total']}")
    print(f"  folder_role: {json.dumps(stats['folder_role'], ensure_ascii=False)}")
    print(f"  game_alias_kr: {json.dumps(stats['game_alias_kr'], ensure_ascii=False)}")
    print(f"  file_kind: {json.dumps(stats['file_kind'], ensure_ascii=False)}")
    print(f"  ref_date: {json.dumps(stats['ref_date'], ensure_ascii=False)}")


if __name__ == "__main__":
    main()
