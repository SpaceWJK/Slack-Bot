"""
parser_pipeline.py — Wiki 파서 고도화 파이프라인 (반복 사용)

파서 수정 후 매번 실행하여 개선 상태를 추적하고,
문제 패턴을 자동 분류하며, 스크린샷 샘플 검증 세트를 생성한다.

단계:
  1. COMPARE  — 전체 페이지 old/new 파서 비교 + 순수 텍스트 diff
  2. CLASSIFY — old_better 페이지의 HTML 패턴 자동 분류
  3. SAMPLE   — 패턴별 대표 페이지 스크린샷 검증 세트 생성
  4. APPLY    — 최종 파싱 결과를 DB body_text에 반영

사용법:
    python scripts/parser_pipeline.py compare              # 1단계: 전수 비교
    python scripts/parser_pipeline.py classify             # 2단계: 패턴 분류
    python scripts/parser_pipeline.py sample [--count 5]   # 3단계: 스크린샷 샘플
    python scripts/parser_pipeline.py apply [--dry-run]    # 4단계: DB 반영
    python scripts/parser_pipeline.py status               # 현재 상태 요약
    python scripts/parser_pipeline.py full                 # 1→2→3 전체 실행

반복 사이클:
    파서 수정 → compare → classify → (패턴 확인 → 파서 재수정) → compare ...
    최종 확인 → sample → (스크린샷 검토) → apply
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import html as _html
from pathlib import Path
from collections import Counter, defaultdict

# ── 경로 설정 ─────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")
SCREENSHOT_DIR = PROJECT_ROOT / "cache" / "screenshots"
PIPELINE_DIR = PROJECT_ROOT / "cache" / "parser_pipeline"

# ── 로깅 ─────────────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "parser_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("parser_pipeline")


# ── 파서 임포트 ───────────────────────────────────────────────

from wiki_client import _strip_html as new_strip_html
from src.sync_engine import _strip_html as old_strip_html


# ── 유틸 ─────────────────────────────────────────────────────

def _pure_text(text: str) -> str:
    """포맷팅 요소를 모두 제거하고 순수 텍스트만 추출."""
    t = text
    t = re.sub(r'[|\-]{2,}', '', t)     # 테이블 구분자
    t = re.sub(r'^- ', '', t, flags=re.MULTILINE)  # 리스트 마커
    t = re.sub(r'\s+', '', t)           # 모든 공백
    return t


def _get_pages(db_path: str) -> list[dict]:
    """body_raw 있는 wiki 페이지 전체 조회."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT n.id as node_id, n.source_id, n.title, n.url,
                   dc.body_raw, dc.body_text as cached_text,
                   LENGTH(dc.body_raw) as raw_len
            FROM nodes n
            JOIN doc_content dc ON dc.node_id = n.id
            WHERE n.source_type = 'wiki'
              AND dc.body_raw IS NOT NULL AND dc.body_raw != ''
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════
# STEP 1: COMPARE — 전수 비교
# ══════════════════════════════════════════════════════════════

def step_compare():
    """전체 페이지에 대해 old/new 파서 비교 + 순수 텍스트 diff."""
    PIPELINE_DIR.mkdir(parents=True, exist_ok=True)

    pages = _get_pages(DB_PATH)
    total = len(pages)
    log.info("=== COMPARE 시작: %d개 페이지 ===", total)

    results = []
    t0 = time.time()

    for i, page in enumerate(pages, 1):
        pid = page["source_id"]
        body_raw = page["body_raw"]

        try:
            new_text = new_strip_html(body_raw)
            old_text = old_strip_html(body_raw)

            new_pure = _pure_text(new_text)
            old_pure = _pure_text(old_text)

            # 순수 텍스트 diff
            pure_diff = len(new_pure) - len(old_pure)
            fmt_diff = len(new_text) - len(old_text)

            # 분류
            if pure_diff < -10:
                category = "CONTENT_LOSS"
            elif fmt_diff < -20:
                category = "FORMAT_ONLY"   # 포맷만 다름, 콘텐츠 동일
            elif fmt_diff == 0 and pure_diff == 0:
                category = "IDENTICAL"
            else:
                category = "OK"  # new가 같거나 더 좋음

            has_screenshot = (SCREENSHOT_DIR / f"{pid}.png").exists()

            results.append({
                "page_id": pid,
                "node_id": page["node_id"],
                "title": page["title"],
                "url": page.get("url", ""),
                "raw_len": page["raw_len"],
                "old_len": len(old_text),
                "new_len": len(new_text),
                "fmt_diff": fmt_diff,
                "old_pure_len": len(old_pure),
                "new_pure_len": len(new_pure),
                "pure_diff": pure_diff,
                "category": category,
                "has_screenshot": has_screenshot,
            })

        except Exception as e:
            log.warning("[%d/%d] 실패 %s: %s", i, total, pid, e)

        if i % 500 == 0:
            log.info("[%d/%d] 처리 중...", i, total)

    duration = round(time.time() - t0, 1)

    # ── 통계 ──
    cats = Counter(r["category"] for r in results)
    total_processed = len(results)

    summary = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_pages": total_processed,
        "duration_sec": duration,
        "categories": dict(cats),
        "content_loss_count": cats.get("CONTENT_LOSS", 0),
        "format_only_count": cats.get("FORMAT_ONLY", 0),
        "ok_count": cats.get("OK", 0) + cats.get("IDENTICAL", 0),
        "old_better_count": cats.get("CONTENT_LOSS", 0) + cats.get("FORMAT_ONLY", 0),
        "with_screenshot": sum(1 for r in results if r["has_screenshot"]),
    }

    # 저장
    compare_file = PIPELINE_DIR / "compare_results.json"
    compare_file.write_text(
        json.dumps({"summary": summary, "pages": results},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log.info("=== COMPARE 완료 (%.1fs) ===", duration)
    log.info("  전체: %d | OK: %d | FORMAT_ONLY: %d | CONTENT_LOSS: %d",
             total_processed, summary["ok_count"],
             summary["format_only_count"], summary["content_loss_count"])

    return summary


# ══════════════════════════════════════════════════════════════
# STEP 2: CLASSIFY — old_better 페이지 HTML 패턴 분류
# ══════════════════════════════════════════════════════════════

# 파서가 놓칠 수 있는 HTML 패턴 정의
_PATTERNS = [
    ("ac:structured-macro", r'<ac:structured-macro[^>]*ac:name="([^"]*)"'),
    ("ac:parameter",        r'<ac:parameter'),
    ("display:none",        r'display:\s*none'),
    ("select/option",       r'<select[\s>]'),
    ("table-filter",        r'class="[^"]*table-filter'),
    ("chart-macro",         r'class="[^"]*chart-'),
    ("form-element",        r'<(input|button|form)[\s>]'),
    ("iframe",              r'<iframe[\s>]'),
    ("confluence-info-macro", r'ac:name="(info|note|warning|tip|expand)"'),
    ("code-block",          r'ac:name="code"'),
    ("toc-macro",           r'ac:name="toc"'),
    ("jira-macro",          r'ac:name="jira"'),
    ("excerpt-macro",       r'ac:name="excerpt"'),
    ("truncated-html",      r'<[a-z]+[^>]*$'),  # 잘린 태그 (200K 경계)
]


def step_classify():
    """old_better 페이지들의 HTML 패턴을 분류."""
    compare_file = PIPELINE_DIR / "compare_results.json"
    if not compare_file.exists():
        log.error("compare_results.json 없음 — 먼저 'compare' 실행 필요")
        return None

    with open(compare_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # CONTENT_LOSS + FORMAT_ONLY 페이지만 대상
    problem_pages = [
        p for p in data["pages"]
        if p["category"] in ("CONTENT_LOSS", "FORMAT_ONLY")
    ]

    if not problem_pages:
        log.info("old_better 페이지 없음 — 분류 불필요")
        return {"total": 0, "patterns": {}}

    log.info("=== CLASSIFY 시작: %d개 문제 페이지 ===", len(problem_pages))

    # DB에서 body_raw 가져오기
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    pattern_groups = defaultdict(list)  # pattern_name → [page_info]
    pattern_counts = Counter()

    for page in problem_pages:
        pid = page["page_id"]
        row = conn.execute("""
            SELECT dc.body_raw FROM doc_content dc
            JOIN nodes n ON dc.node_id = n.id
            WHERE n.source_id = ? AND n.source_type = 'wiki'
        """, (pid,)).fetchone()

        if not row or not row[0]:
            continue

        html = row[0]
        matched_patterns = []

        for pname, regex in _PATTERNS:
            matches = re.findall(regex, html, re.IGNORECASE)
            if matches:
                matched_patterns.append(pname)
                pattern_counts[pname] += 1

        page_info = {
            "page_id": pid,
            "title": page["title"],
            "category": page["category"],
            "pure_diff": page["pure_diff"],
            "fmt_diff": page["fmt_diff"],
            "raw_len": page["raw_len"],
            "patterns": matched_patterns,
            "has_screenshot": page["has_screenshot"],
        }

        if matched_patterns:
            for p in matched_patterns:
                pattern_groups[p].append(page_info)
        else:
            pattern_groups["_unknown"].append(page_info)

    conn.close()

    # 결과 정리
    classify_result = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_problem_pages": len(problem_pages),
        "pattern_frequency": dict(pattern_counts.most_common()),
        "pattern_groups": {
            k: {
                "count": len(v),
                "content_loss": sum(1 for p in v if p["category"] == "CONTENT_LOSS"),
                "format_only": sum(1 for p in v if p["category"] == "FORMAT_ONLY"),
                "pages": sorted(v, key=lambda x: x["pure_diff"])[:10],
            }
            for k, v in sorted(pattern_groups.items(),
                                key=lambda x: len(x[1]), reverse=True)
        },
    }

    classify_file = PIPELINE_DIR / "classify_results.json"
    classify_file.write_text(
        json.dumps(classify_result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log.info("=== CLASSIFY 완료 ===")
    log.info("  패턴 빈도:")
    for pname, cnt in pattern_counts.most_common():
        cl = sum(1 for p in pattern_groups[pname]
                 if p["category"] == "CONTENT_LOSS")
        log.info("    %-30s %3d건 (CONTENT_LOSS: %d)", pname, cnt, cl)

    if "_unknown" in pattern_groups:
        log.info("  미분류: %d건", len(pattern_groups["_unknown"]))

    return classify_result


# ══════════════════════════════════════════════════════════════
# STEP 3: SAMPLE — 스크린샷 검증 세트 생성
# ══════════════════════════════════════════════════════════════

def step_sample(count_per_pattern: int = 3):
    """패턴별 대표 페이지를 선정하여 스크린샷+파싱결과 비교 세트 생성."""
    classify_file = PIPELINE_DIR / "classify_results.json"
    if not classify_file.exists():
        log.error("classify_results.json 없음 — 먼저 'classify' 실행 필요")
        return None

    with open(classify_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    sample_dir = PIPELINE_DIR / "samples"
    if sample_dir.exists():
        shutil.rmtree(sample_dir)
    sample_dir.mkdir(parents=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    selected = []

    for pattern_name, group in data["pattern_groups"].items():
        pages = group["pages"]
        # 스크린샷 있는 페이지 우선, 그 중 CONTENT_LOSS 우선
        with_ss = [p for p in pages if p["has_screenshot"]]
        without_ss = [p for p in pages if not p["has_screenshot"]]

        candidates = with_ss[:count_per_pattern]
        if len(candidates) < count_per_pattern:
            candidates += without_ss[:count_per_pattern - len(candidates)]

        for page in candidates:
            pid = page["page_id"]

            # 파싱 결과 저장
            row = conn.execute("""
                SELECT dc.body_raw FROM doc_content dc
                JOIN nodes n ON dc.node_id = n.id
                WHERE n.source_id = ? AND n.source_type = 'wiki'
            """, (pid,)).fetchone()

            if not row or not row[0]:
                continue

            new_text = new_strip_html(row[0])
            old_text = old_strip_html(row[0])

            safe_name = pattern_name.replace(":", "_").replace("/", "_")
            page_dir = sample_dir / safe_name / pid
            page_dir.mkdir(parents=True, exist_ok=True)

            # 파싱 결과 저장
            (page_dir / "new_parser.txt").write_text(new_text, encoding="utf-8")
            (page_dir / "old_parser.txt").write_text(old_text, encoding="utf-8")

            # 스크린샷 복사 (링크 대신 복사)
            ss_path = SCREENSHOT_DIR / f"{pid}.png"
            if ss_path.exists():
                shutil.copy2(ss_path, page_dir / "screenshot.png")

            # 메타 정보
            meta = {
                "page_id": pid,
                "title": page["title"],
                "pattern": pattern_name,
                "category": page["category"],
                "pure_diff": page["pure_diff"],
                "fmt_diff": page["fmt_diff"],
                "new_len": len(new_text),
                "old_len": len(old_text),
                "has_screenshot": ss_path.exists(),
            }
            (page_dir / "meta.json").write_text(
                json.dumps(meta, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            selected.append(meta)

    conn.close()

    # 샘플 인덱스 저장
    index = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count_per_pattern": count_per_pattern,
        "total_samples": len(selected),
        "samples": selected,
    }
    (PIPELINE_DIR / "sample_index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log.info("=== SAMPLE 완료: %d개 샘플 생성 → %s ===",
             len(selected), sample_dir)
    return index


# ══════════════════════════════════════════════════════════════
# STEP 4: APPLY — 최종 파싱 결과 DB 반영
# ══════════════════════════════════════════════════════════════

def step_apply(dry_run: bool = False):
    """새 파서 결과를 DB body_text에 반영."""
    pages = _get_pages(DB_PATH)
    total = len(pages)

    log.info("=== APPLY %s: %d개 페이지 ===",
             "(DRY-RUN)" if dry_run else "", total)

    if dry_run:
        # dry-run: 변경될 건 수만 계산
        changed = 0
        for page in pages:
            new_text = new_strip_html(page["body_raw"])
            cached = page.get("cached_text") or ""
            if new_text != cached:
                changed += 1
        log.info("  변경 대상: %d / %d 페이지", changed, total)
        return {"dry_run": True, "total": total, "would_change": changed}

    conn = sqlite3.connect(DB_PATH)
    t0 = time.time()
    updated = 0

    for i, page in enumerate(pages, 1):
        try:
            new_text = new_strip_html(page["body_raw"])
            conn.execute(
                "UPDATE doc_content SET body_text = ? WHERE node_id = ?",
                (new_text, page["node_id"]),
            )
            updated += 1
        except Exception as e:
            log.warning("[%d] 실패 %s: %s", i, page["source_id"], e)

        if i % 500 == 0:
            conn.commit()
            log.info("[%d/%d] 처리 중...", i, total)

    conn.commit()
    conn.close()
    duration = round(time.time() - t0, 1)

    log.info("=== APPLY 완료: %d건 업데이트 (%.1fs) ===", updated, duration)
    return {"dry_run": False, "total": total, "updated": updated,
            "duration_sec": duration}


# ══════════════════════════════════════════════════════════════
# STATUS — 현재 상태 요약
# ══════════════════════════════════════════════════════════════

def show_status():
    """마지막 실행 결과 요약 출력."""
    print("=" * 60)
    print("Wiki 파서 파이프라인 상태")
    print("=" * 60)

    # compare 결과
    compare_file = PIPELINE_DIR / "compare_results.json"
    if compare_file.exists():
        with open(compare_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        s = data["summary"]
        print(f"\n[COMPARE] {s['timestamp']}")
        print(f"  전체: {s['total_pages']}  |  소요: {s['duration_sec']}s")
        print(f"  OK: {s['ok_count']}  |  FORMAT_ONLY: {s['format_only_count']}"
              f"  |  CONTENT_LOSS: {s['content_loss_count']}")
        print(f"  스크린샷 보유: {s['with_screenshot']}")
    else:
        print("\n[COMPARE] 미실행")

    # classify 결과
    classify_file = PIPELINE_DIR / "classify_results.json"
    if classify_file.exists():
        with open(classify_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"\n[CLASSIFY] {data['timestamp']}")
        print(f"  문제 페이지: {data['total_problem_pages']}")
        if data.get("pattern_frequency"):
            print("  상위 패턴:")
            for p, c in list(data["pattern_frequency"].items())[:5]:
                print(f"    {p}: {c}건")
    else:
        print("\n[CLASSIFY] 미실행")

    # sample 결과
    sample_file = PIPELINE_DIR / "sample_index.json"
    if sample_file.exists():
        with open(sample_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"\n[SAMPLE] {data['timestamp']}")
        print(f"  총 샘플: {data['total_samples']}개")
    else:
        print("\n[SAMPLE] 미실행")

    print()


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Wiki 파서 고도화 파이프라인",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
단계별 실행:
  compare   — 전체 페이지 old/new 비교 + 순수 텍스트 diff
  classify  — old_better 페이지 HTML 패턴 분류
  sample    — 패턴별 스크린샷 검증 세트 생성
  apply     — 최종 파싱 결과 DB 반영
  status    — 현재 상태 요약
  full      — compare → classify → sample 연속 실행
        """,
    )
    parser.add_argument("step", choices=[
        "compare", "classify", "sample", "apply", "status", "full",
    ])
    parser.add_argument("--count", type=int, default=3,
                        help="sample: 패턴당 샘플 수 (기본 3)")
    parser.add_argument("--dry-run", action="store_true",
                        help="apply: 실제 변경 없이 건수만 확인")
    args = parser.parse_args()

    if args.step == "compare":
        result = step_compare()
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.step == "classify":
        result = step_classify()
        if result:
            print(json.dumps(
                {k: v for k, v in result.items() if k != "pattern_groups"},
                ensure_ascii=False, indent=2,
            ))

    elif args.step == "sample":
        result = step_sample(count_per_pattern=args.count)
        if result:
            print(f"샘플 {result['total_samples']}개 생성 완료")

    elif args.step == "apply":
        result = step_apply(dry_run=args.dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.step == "status":
        show_status()

    elif args.step == "full":
        log.info("=== FULL 파이프라인 시작 ===")
        step_compare()
        step_classify()
        step_sample(count_per_pattern=args.count)
        show_status()
        log.info("=== FULL 파이프라인 완료 ===")


if __name__ == "__main__":
    main()
