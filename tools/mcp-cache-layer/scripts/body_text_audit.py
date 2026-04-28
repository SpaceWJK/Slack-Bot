"""
body_text_audit.py — GDI body_text 감사 및 backfill 스크립트

body_text가 NULL / empty 또는 char_count < threshold인 GDI 노드를
gdi-repo 로컬 파일 재파싱으로 backfill하고, FTS 부분 재인덱싱 후
감사 리포트 JSON을 저장한다.

아키텍처:
  Phase 1: audit_coverage()       — NULL/short/orphan 노드 집계 (read-only)
  Phase 2: backfill_node()        — 로컬 파일 재파싱 → doc_content UPDATE
  Phase 3: fts_partial_update()   — 영향 노드만 FTS 부분 재인덱싱 (chunk 999)
  Phase 4: generate_report()      — JSON 리포트 저장

사용법:
    python scripts/body_text_audit.py                   # dry-run (감사만)
    python scripts/body_text_audit.py --apply           # 실제 backfill 실행
    python scripts/body_text_audit.py --apply --game chaoszero
    python scripts/body_text_audit.py --apply --no-fts  # FTS 재인덱싱 생략
    python scripts/body_text_audit.py --output path/to/report.json

task-103 구현.
"""

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

# ── 경로 설정 ─────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
scripts_dir = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(scripts_dir))

from src.models import get_connection  # noqa: E402
from src import config as cache_config  # noqa: E402
from file_parsers import parse_xlsx, parse_tsv, parse_pptx  # noqa: E402
from fts_rebuild import get_coverage_stats  # noqa: E402

# ── 로거 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

GDI_REPO = PROJECT_ROOT / "gdi-repo"
MAX_BODY_CHARS = 500_000
SHORT_THRESHOLD = 100           # char_count < 이 값이면 backfill 대상
COMMIT_INTERVAL = 50
FTS_CHUNK_SIZE = 999            # SQLite placeholder 한계

SUPPORTED_EXTS = {'.xlsx', '.tsv', '.pptx'}

PARSERS = {
    '.xlsx': parse_xlsx,
    '.tsv': parse_tsv,
    '.pptx': parse_pptx,
}

# 파서가 예외 없이 오류 문자열을 body_text에 반환하는 패턴 감지용
_PARSER_ERROR_MARKERS = (
    "[TSV 파일 읽기 실패",
    "[XLSX 파싱 실패",
    "[PPTX 파싱 실패",
    "읽기 실패",
    "파싱 오류",
    "설치 필요",
)


# ── 데이터 클래스 ──────────────────────────────────────────────────────────────

@dataclass
class AuditResult:
    total_gdi: int = 0
    null_body: int = 0              # body_text IS NULL
    empty_body: int = 0             # body_text = ''
    short_body: int = 0             # 0 < char_count < SHORT_THRESHOLD
    orphan_no_doc_content: int = 0  # nodes에는 있으나 doc_content 행 없음
    fill_rate_pct: float = 0.0      # (total - null - empty - short) / total * 100
    targets: list = field(default_factory=list)  # [{node_id, source_id, space_key, ...}]


class BackfillStatus(str, Enum):
    UPDATED     = "updated"       # char_count >= SHORT_THRESHOLD 달성
    NO_FILE     = "no_file"       # gdi-repo 파일 없음
    UNSUPPORTED = "unsupported"   # 비지원 확장자 (.docx/.png 등)
    PARSE_ERROR = "parse_error"   # 파서 예외 또는 오류 문자열 반환
    STILL_SHORT = "still_short"   # 파싱됐으나 char_count < SHORT_THRESHOLD
    ORPHAN_SKIP = "orphan_skip"   # doc_content 행 없음 — INSERT 금지


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────

def _now() -> str:
    """현재 시각을 ISO 8601 문자열로 반환."""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _resolve_file(source_id: str) -> Optional[Path]:
    """source_id → gdi-repo 절대경로. 없으면 None.

    source_id 예: "Chaoszero/TSV/20260204/xxx.tsv"
    Windows 역슬래시 정규화 후 대소문자 변형도 시도한다.

    path traversal 방어: resolve() 후 GDI_REPO 경계 이탈 시 None 반환.
    DB source_id에 절대경로나 '..' 포함 시 차단.
    """
    _gdi_root = GDI_REPO.resolve()
    norm = source_id.replace("\\", "/")

    p = (GDI_REPO / norm).resolve()
    if not p.is_relative_to(_gdi_root):
        log.warning("PATH_TRAVERSAL 차단: source_id=%s → %s", source_id, p)
        return None
    if p.exists():
        return p

    # 대소문자 변형 대응 (Windows FS case-insensitive, DB source_id는 혼재)
    p_lower = (GDI_REPO / norm.lower()).resolve()
    if not p_lower.is_relative_to(_gdi_root):
        return None
    if p_lower.exists():
        return p_lower

    return None


# ── Phase 1: 감사 쿼리 ────────────────────────────────────────────────────────

def audit_coverage(
    conn: sqlite3.Connection,
    threshold: int = SHORT_THRESHOLD,
    game_filter: Optional[str] = None,
) -> AuditResult:
    """NULL/short/orphan 노드를 LEFT JOIN + UNION ALL로 집계한다.

    orphan: doc_content 행이 없는 nodes (INSERT 금지 — ORPHAN_SKIP 대상).
    타겟: body_text IS NULL OR body_text = '' OR char_count < threshold.

    Args:
        conn:        SQLite 연결
        threshold:   char_count 기준 (기본 SHORT_THRESHOLD)
        game_filter: 특정 게임(space_key)만 감사. None이면 전체.

    Returns:
        AuditResult (targets 포함)
    """
    game_clause = "AND n.space_key = :game_filter" if game_filter else ""

    # 전체 GDI 노드 수
    total_params: dict = {"game_filter": game_filter} if game_filter else {}
    total_sql = f"""
        SELECT COUNT(*)
        FROM nodes n
        WHERE n.source_type = 'gdi'
          {game_clause}
    """
    total_gdi = conn.execute(total_sql, total_params).fetchone()[0]

    # 타겟 쿼리: orphan UNION ALL body_text 이슈 노드
    if game_filter:
        orphan_clause = "AND n.space_key = ?"
        body_clause = "AND n.space_key = ?"
    else:
        orphan_clause = ""
        body_clause = ""

    target_sql = f"""
        SELECT n.id          AS node_id,
               n.source_id,
               n.space_key,
               n.title,
               NULL          AS body_text,
               0             AS char_count,
               0             AS has_doc_content
        FROM nodes n
        LEFT JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.source_type = 'gdi'
          AND dc.node_id IS NULL
          {orphan_clause}

        UNION ALL

        SELECT n.id          AS node_id,
               n.source_id,
               n.space_key,
               n.title,
               dc.body_text,
               COALESCE(dc.char_count, 0) AS char_count,
               1             AS has_doc_content
        FROM nodes n
        JOIN doc_content dc ON dc.node_id = n.id
        WHERE n.source_type = 'gdi'
          AND (
              dc.body_text IS NULL
              OR dc.body_text = ''
              OR dc.char_count < ?
          )
          {body_clause}
        ORDER BY space_key, source_id
    """

    # SQL 파라미터 바인딩 순서: orphan space_key → threshold → body space_key
    if game_filter:
        final_params = [game_filter, threshold, game_filter]
    else:
        final_params = [threshold]

    rows = conn.execute(target_sql, final_params).fetchall()

    result = AuditResult(total_gdi=total_gdi)
    for row in rows:
        r = dict(row)
        if not r["has_doc_content"]:
            result.orphan_no_doc_content += 1
        elif r["body_text"] is None:
            result.null_body += 1
        elif r["body_text"] == "":
            result.empty_body += 1
        else:
            # char_count > 0 이지만 < threshold
            result.short_body += 1

        result.targets.append({
            "node_id":         r["node_id"],
            "source_id":       r["source_id"],
            "space_key":       r["space_key"],
            "title":           r["title"],
            "char_count":      r["char_count"],
            "has_doc_content": bool(r["has_doc_content"]),
        })

    # fill_rate_pct: 이슈 없는 노드 비율 (orphan 포함 — doc_content 없는 노드도 bad로 집계)
    bad = result.null_body + result.empty_body + result.short_body + result.orphan_no_doc_content
    if total_gdi > 0:
        result.fill_rate_pct = (total_gdi - bad) / total_gdi * 100
    else:
        result.fill_rate_pct = 100.0

    log.info(
        "감사 완료: 전체=%d, NULL=%d, empty=%d, short=%d, orphan=%d, fill_rate=%.1f%%",
        result.total_gdi, result.null_body, result.empty_body,
        result.short_body, result.orphan_no_doc_content, result.fill_rate_pct,
    )
    return result


# ── Phase 2: backfill ─────────────────────────────────────────────────────────

def backfill_node(
    conn: sqlite3.Connection,
    node: dict,
    dry_run: bool = True,
) -> BackfillStatus:
    """단일 노드의 body_text를 로컬 파일 재파싱으로 갱신한다.

    STILL_SHORT여도 UPDATE는 항상 실행한다 (멱등성 보장).

    Args:
        conn:    SQLite 연결 (commit은 호출자 책임)
        node:    audit_coverage() targets 항목
        dry_run: True이면 DB 변경 없이 상태만 반환

    Returns:
        BackfillStatus
    """
    # 1. orphan 노드 — doc_content 행이 없으므로 INSERT 금지
    if not node.get("has_doc_content", True):
        log.warning("ORPHAN_SKIP: node_id=%s source_id=%s", node["node_id"], node["source_id"])
        return BackfillStatus.ORPHAN_SKIP

    # 2. source_id 정규화
    norm_source_id = node["source_id"].replace("\\", "/")

    # 3. 파일 경로 해석
    file_path = _resolve_file(norm_source_id)
    if file_path is None:
        log.debug("NO_FILE: %s", norm_source_id)
        return BackfillStatus.NO_FILE

    # 4. 확장자 확인
    ext = file_path.suffix.lower()

    # 5. 비지원 확장자 → UNSUPPORTED
    if ext not in SUPPORTED_EXTS:
        log.debug("UNSUPPORTED ext=%s: %s", ext, norm_source_id)
        return BackfillStatus.UNSUPPORTED

    # 6. 파싱 시도
    try:
        result = PARSERS[ext](str(file_path))
        new_body = result.get("body_text", "")[:MAX_BODY_CHARS]
    except Exception as exc:
        log.warning("PARSE_ERROR (exception) %s: %s", norm_source_id, exc)
        return BackfillStatus.PARSE_ERROR

    # 7. 파서 오류 문자열 감지 (예외 없이 오류를 body_text로 반환하는 케이스)
    if any(marker in new_body for marker in _PARSER_ERROR_MARKERS):
        log.warning("PARSE_ERROR (marker) %s: %.80s", norm_source_id, new_body)
        return BackfillStatus.PARSE_ERROR

    # 8. dry-run: 시뮬레이션만 (UPDATED 반환)
    if dry_run:
        return BackfillStatus.UPDATED

    # 9. DB UPDATE (STILL_SHORT여도 항상 실행 — 멱등성 보장)
    new_char_count = len(new_body)
    body_truncated = 1 if new_char_count >= MAX_BODY_CHARS else 0
    conn.execute(
        """
        UPDATE doc_content
        SET body_text = ?, char_count = ?, body_truncated = ?, cached_at = ?
        WHERE node_id = ?
        """,
        (new_body, new_char_count, body_truncated, _now(), node["node_id"]),
    )

    # 10. STILL_SHORT 판별
    if new_char_count < SHORT_THRESHOLD:
        log.debug(
            "STILL_SHORT node_id=%s char_count=%d source=%s",
            node["node_id"], new_char_count, norm_source_id,
        )
        return BackfillStatus.STILL_SHORT

    return BackfillStatus.UPDATED


# ── Phase 3: FTS 부분 재인덱싱 ────────────────────────────────────────────────

def fts_partial_update(conn: sqlite3.Connection, node_ids: list[int]) -> int:
    """UPDATED/STILL_SHORT 노드에 대해 FTS를 부분 재인덱싱한다.

    SQLite placeholder 한계(999)를 초과하지 않도록 CHUNK=999로 분할 처리.
    DELETE commit 선행 후 INSERT commit 후행 — fts_rebuild.py 패턴 일치.

    Args:
        conn:     SQLite 연결
        node_ids: FTS 갱신 대상 node_id 리스트

    Returns:
        총 INSERT 건수
    """
    if not node_ids:
        return 0

    CHUNK = FTS_CHUNK_SIZE
    total_inserted = 0

    for i in range(0, len(node_ids), CHUNK):
        chunk = node_ids[i:i + CHUNK]
        ph = ",".join("?" * len(chunk))

        # 1. DELETE (commit 선행 — fts_rebuild.py 패턴 일치)
        conn.execute(f"DELETE FROM search_fts WHERE rowid IN ({ph})", chunk)
        conn.commit()

        # 2. SELECT 갱신된 body_text
        rows = conn.execute(
            f"""
            SELECT n.id, n.title, dc.body_text, dc.summary, dc.keywords
            FROM nodes n
            JOIN doc_content dc ON dc.node_id = n.id
            WHERE n.id IN ({ph})
              AND dc.body_text IS NOT NULL
              AND dc.body_text != ''
            """,
            chunk,
        ).fetchall()

        # 3. INSERT
        conn.executemany(
            "INSERT INTO search_fts(rowid, title, body_text, summary, keywords) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r["id"],
                    r["title"] or "",
                    r["body_text"],
                    r["summary"] or "",
                    r["keywords"] or "",
                )
                for r in rows
            ],
        )
        conn.commit()

        total_inserted += len(rows)
        log.info(
            "FTS 부분 갱신: chunk %d/%d — INSERT %d건",
            i // CHUNK + 1,
            (len(node_ids) - 1) // CHUNK + 1,
            len(rows),
        )

    return total_inserted


# ── Phase 4: 리포트 생성 ───────────────────────────────────────────────────────

def generate_report(
    before_audit: AuditResult,
    after_audit: AuditResult,
    before_fts: dict,
    after_fts: dict,
    results: list[dict],
) -> dict:
    """감사 결과를 JSON 리포트 딕셔너리로 변환한다.

    before_fts / after_fts는 get_coverage_stats() 반환값.
    "coverage_pct" 키는 리포트에서 "fts_coverage_pct"로 리네임한다.

    Args:
        before_audit: backfill 전 AuditResult
        after_audit:  backfill 후 AuditResult
        before_fts:   backfill 전 get_coverage_stats() 반환값
        after_fts:    backfill 후 get_coverage_stats() 반환값
        results:      backfill_node() 결과 리스트 (node_id/space_key/source_id/status)

    Returns:
        JSON 직렬화 가능한 리포트 딕셔너리
    """
    # backfill 결과 집계
    status_counts: dict[str, int] = defaultdict(int)
    game_breakdown: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for r in results:
        status_counts[r["status"]] += 1
        game_breakdown[r["space_key"] or "unknown"][r["status"]] += 1

    # game_breakdown defaultdict → 일반 dict
    game_breakdown_plain = {
        game: dict(counts)
        for game, counts in sorted(game_breakdown.items())
    }

    report = {
        "audit_time": _now(),
        "before": {
            "total_gdi_nodes":         before_audit.total_gdi,
            "null_body":               before_audit.null_body,
            "empty_body":              before_audit.empty_body,
            "short_body":              before_audit.short_body,
            "orphan_no_doc_content":   before_audit.orphan_no_doc_content,
            "fill_rate_pct":           round(before_audit.fill_rate_pct, 2),
            "fts_coverage_pct":        round(before_fts["coverage_pct"], 2),
            "fts_eligible":            before_fts["eligible_nodes"],
        },
        "after": {
            "total_gdi_nodes":         after_audit.total_gdi,
            "null_body":               after_audit.null_body,
            "empty_body":              after_audit.empty_body,
            "short_body":              after_audit.short_body,
            "orphan_no_doc_content":   after_audit.orphan_no_doc_content,
            "fill_rate_pct":           round(after_audit.fill_rate_pct, 2),
            "fts_coverage_pct":        round(after_fts["coverage_pct"], 2),
            "fts_eligible":            after_fts["eligible_nodes"],
        },
        "backfill": {
            "updated":       status_counts.get(BackfillStatus.UPDATED.value, 0),
            "still_short":   status_counts.get(BackfillStatus.STILL_SHORT.value, 0),
            "no_file":       status_counts.get(BackfillStatus.NO_FILE.value, 0),
            "unsupported":   status_counts.get(BackfillStatus.UNSUPPORTED.value, 0),
            "parse_error":   status_counts.get(BackfillStatus.PARSE_ERROR.value, 0),
            "orphan_skip":   status_counts.get(BackfillStatus.ORPHAN_SKIP.value, 0),
        },
        "game_breakdown": game_breakdown_plain,
    }
    return report


def save_report(report: dict, output_path: str) -> None:
    """리포트를 JSON 파일로 저장한다."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(str(out), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("리포트 저장: %s", out)


# ── 출력 헬퍼 ─────────────────────────────────────────────────────────────────

def _print_dry_run_summary(audit: AuditResult, fts_stats: dict) -> None:
    """dry-run 결과 요약 출력 (log.info 사용)."""
    sep = "=" * 60
    log.info(sep)
    log.info("[DRY-RUN] 감사 결과 요약 (DB 변경 없음)")
    log.info(sep)
    log.info("  전체 GDI 노드:   %d", audit.total_gdi)
    log.info("  NULL body_text:  %d", audit.null_body)
    log.info("  빈  body_text:   %d", audit.empty_body)
    log.info("  short (<100):    %d", audit.short_body)
    log.info("  orphan:          %d", audit.orphan_no_doc_content)
    log.info("  fill_rate:       %.1f%%", audit.fill_rate_pct)
    log.info(
        "  FTS 커버리지:    %.1f%% (%d/%d)",
        fts_stats["coverage_pct"],
        fts_stats["covered_in_fts"],
        fts_stats["eligible_nodes"],
    )
    log.info("  backfill 대상:  %d건", len(audit.targets))
    log.info("  --apply 플래그를 추가하면 실제 backfill을 실행합니다.")
    log.info(sep)


def _print_summary(report: dict) -> None:
    """실행 결과 요약 출력 (log.info 사용)."""
    b = report["before"]
    a = report["after"]
    bf = report["backfill"]

    sep = "=" * 60
    log.info(sep)
    log.info("[BACKFILL 완료] 결과 요약")
    log.info(sep)
    log.info("  %-20s %10s %10s", "항목", "이전", "이후")
    log.info("  %s", "-" * 40)
    log.info("  %-20s %10d %10d", "NULL body_text",  b["null_body"],  a["null_body"])
    log.info("  %-20s %10d %10d", "empty body_text", b["empty_body"], a["empty_body"])
    log.info("  %-20s %10d %10d", "short (<100)",    b["short_body"], a["short_body"])
    log.info("  %-20s %9.1f%% %9.1f%%", "fill_rate_pct",
             b["fill_rate_pct"], a["fill_rate_pct"])
    log.info("  %-20s %9.1f%% %9.1f%%", "FTS coverage",
             b["fts_coverage_pct"], a["fts_coverage_pct"])
    log.info("  backfill 결과:")
    log.info("    UPDATED:     %d", bf["updated"])
    log.info("    STILL_SHORT: %d", bf["still_short"])
    log.info("    NO_FILE:     %d", bf["no_file"])
    log.info("    UNSUPPORTED: %d", bf["unsupported"])
    log.info("    PARSE_ERROR: %d", bf["parse_error"])
    log.info("    ORPHAN_SKIP: %d", bf["orphan_skip"])
    log.info(sep)


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    """CLI 인수 파서 빌드.

    주의: --dry-run 플래그는 등록하지 않는다. (AttributeError 방지)
    dry_run = not args.apply 단일 체크로 결정.
    """
    p = argparse.ArgumentParser(
        description=(
            "GDI body_text 감사 및 backfill 스크립트 (task-103)\n"
            "  --apply 없으면 dry-run (DB 변경 없음)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--db", type=str, default=None,
                   help="DB 경로 (기본: cache_config.DB_PATH)")
    p.add_argument("--game", type=str, default=None,
                   help="특정 게임만 감사/backfill (chaoszero/epicseven/...)")
    p.add_argument("--threshold", type=int, default=SHORT_THRESHOLD,
                   help=f"char_count 기준 (기본: {SHORT_THRESHOLD})")
    p.add_argument("--apply", action="store_true",
                   help="실제 backfill 실행. 미지정 시 dry-run (감사만)")
    p.add_argument("--output", type=str, default=None,
                   help="JSON 리포트 저장 경로 (기본: audit_results/audit_YYYYMMDD_HHMMSS.json)")
    p.add_argument("--no-fts", action="store_true",
                   help="FTS 재인덱싱 생략")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # dry_run 결정: --apply 없으면 dry-run
    dry_run = not args.apply

    # DB 경로 결정
    db_path = args.db or cache_config.DB_PATH

    # gdi-repo 존재 확인
    if not GDI_REPO.exists():
        log.error("gdi-repo 디렉토리 없음: %s", GDI_REPO)
        sys.exit(1)

    # 기본 output 경로 설정
    if args.output:
        output_path = args.output
    else:
        audit_dir = scripts_dir / "audit_results"
        audit_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(audit_dir / f"audit_{ts}.json")

    # DB 연결
    conn = get_connection(db_path)
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")  # WAL 부하 완화

    try:
        # ── Phase 1: Audit ──────────────────────────────────────────────────────
        before_audit = audit_coverage(conn, args.threshold, args.game)
        before_fts = get_coverage_stats(conn, "gdi", args.game)

        log.info(
            "대상: %d건 (NULL: %d, empty: %d, short: %d, orphan: %d)",
            len(before_audit.targets),
            before_audit.null_body,
            before_audit.empty_body,
            before_audit.short_body,
            before_audit.orphan_no_doc_content,
        )

        if dry_run:
            _print_dry_run_summary(before_audit, before_fts)
            return

        # ── Phase 2: Backfill ───────────────────────────────────────────────────
        results: list[dict] = []
        for i, node in enumerate(before_audit.targets, 1):
            status = backfill_node(conn, node, dry_run=False)
            results.append({
                "node_id":   node["node_id"],
                "space_key": node["space_key"],
                "source_id": node["source_id"],
                "status":    status.value,
            })
            if i % COMMIT_INTERVAL == 0:
                conn.commit()
                log.info("진행: %d/%d 처리됨", i, len(before_audit.targets))

        conn.commit()

        # ── Phase 3: FTS 부분 재인덱싱 ─────────────────────────────────────────
        updated_ids = [
            r["node_id"] for r in results
            if r["status"] in (BackfillStatus.UPDATED.value, BackfillStatus.STILL_SHORT.value)
        ]

        if updated_ids and not args.no_fts:
            fts_count = fts_partial_update(conn, updated_ids)
            log.info("FTS 재인덱싱 완료: %d건 INSERT", fts_count)
        elif args.no_fts:
            log.info("FTS 재인덱싱 생략 (--no-fts)")
        else:
            log.info("FTS 재인덱싱 대상 없음")

        # ── Phase 4: 리포트 생성 ─────────────────────────────────────────────────
        after_audit = audit_coverage(conn, args.threshold, args.game)
        after_fts = get_coverage_stats(conn, "gdi", args.game)

        report = generate_report(
            before_audit, after_audit,
            before_fts, after_fts,
            results,
        )

        save_report(report, output_path)
        _print_summary(report)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
