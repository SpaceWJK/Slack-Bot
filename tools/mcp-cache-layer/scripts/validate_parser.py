"""
validate_parser.py — Wiki 파서 검증 + 시뮬레이션

캐시 DB의 body_raw(Confluence storage HTML)에 wiki_client.py의
새 파서(_ConfluenceHTMLExtractor)를 적용하고, 기존 파서(regex) 결과와 비교.
스크린샷이 있는 페이지는 이미지 경로도 함께 기록.

사용법:
    python scripts/validate_parser.py                     # 전체 시뮬레이션
    python scripts/validate_parser.py --page 552936235    # 단일 페이지
    python scripts/validate_parser.py --limit 20          # 처음 20개만
    python scripts/validate_parser.py --report             # 통계 리포트만 출력
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import html as _html
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

# ── 설정 ─────────────────────────────────────────────────────

DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")
SCREENSHOT_DIR = PROJECT_ROOT / "cache" / "screenshots"
RESULTS_DIR = PROJECT_ROOT / "cache" / "parser_results"

# ── 로깅 ─────────────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "validate_parser.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("validate_parser")


# ── 파서 임포트 ──────────────────────────────────────────────

# 새 파서: wiki_client.py의 HTMLParser 기반
from wiki_client import _strip_html as new_strip_html

# 기존 파서: sync_engine.py의 regex 기반
from src.sync_engine import _strip_html as old_strip_html


# ── DB 조회 ──────────────────────────────────────────────────

def get_pages_with_body(db_path: str, page_id: str = None) -> list[dict]:
    """body_raw 있는 wiki 페이지 목록 + body_raw/body_text 포함."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if page_id:
            sql = """
                SELECT n.source_id, n.title, n.url,
                       dc.body_raw, dc.body_text as cached_text,
                       LENGTH(dc.body_raw) as raw_len
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = 'wiki' AND n.source_id = ?
                  AND dc.body_raw IS NOT NULL AND dc.body_raw != ''
            """
            rows = conn.execute(sql, (page_id,)).fetchall()
        else:
            sql = """
                SELECT n.source_id, n.title, n.url,
                       dc.body_raw, dc.body_text as cached_text,
                       LENGTH(dc.body_raw) as raw_len
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = 'wiki'
                  AND dc.body_raw IS NOT NULL AND dc.body_raw != ''
                ORDER BY LENGTH(dc.body_raw) DESC
            """
            rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── 파서 비교 ────────────────────────────────────────────────

def compare_parsers(page: dict) -> dict:
    """한 페이지에 두 파서를 적용하고 결과 비교."""
    pid = page["source_id"]
    body_raw = page["body_raw"]
    cached_text = page.get("cached_text", "") or ""

    # 두 파서 적용
    t0 = time.time()
    new_text = new_strip_html(body_raw)
    new_ms = round((time.time() - t0) * 1000, 1)

    t0 = time.time()
    old_text = old_strip_html(body_raw)
    old_ms = round((time.time() - t0) * 1000, 1)

    # 스크린샷 존재 여부
    screenshot = SCREENSHOT_DIR / f"{pid}.png"
    has_screenshot = screenshot.exists()

    result = {
        "page_id": pid,
        "title": page["title"],
        "url": page.get("url", ""),
        "raw_len": page["raw_len"],
        "old_parser": {
            "text_len": len(old_text),
            "time_ms": old_ms,
        },
        "new_parser": {
            "text_len": len(new_text),
            "time_ms": new_ms,
            "text": new_text,
        },
        "cached_text_len": len(cached_text),
        "has_screenshot": has_screenshot,
        "screenshot_path": str(screenshot) if has_screenshot else None,
        # 비교 지표
        "improvement": {
            "len_diff": len(new_text) - len(old_text),
            "len_ratio": round(len(new_text) / max(len(old_text), 1), 2),
        },
    }

    return result


# ── 통계 리포트 ──────────────────────────────────────────────

def generate_report(results: list[dict]) -> dict:
    """전체 결과에서 통계 리포트 생성."""
    total = len(results)
    if total == 0:
        return {"total": 0}

    with_screenshot = sum(1 for r in results if r["has_screenshot"])

    old_lens = [r["old_parser"]["text_len"] for r in results]
    new_lens = [r["new_parser"]["text_len"] for r in results]

    # 새 파서가 더 많은 텍스트를 추출한 페이지
    improved = sum(1 for r in results if r["improvement"]["len_diff"] > 0)
    same = sum(1 for r in results if r["improvement"]["len_diff"] == 0)
    worse = sum(1 for r in results if r["improvement"]["len_diff"] < 0)

    # 빈 결과 (파서 출력이 0자)
    old_empty = sum(1 for l in old_lens if l == 0)
    new_empty = sum(1 for l in new_lens if l == 0)

    report = {
        "total_pages": total,
        "with_screenshot": with_screenshot,
        "old_parser": {
            "avg_len": round(sum(old_lens) / total),
            "min_len": min(old_lens),
            "max_len": max(old_lens),
            "empty_count": old_empty,
            "total_chars": sum(old_lens),
        },
        "new_parser": {
            "avg_len": round(sum(new_lens) / total),
            "min_len": min(new_lens),
            "max_len": max(new_lens),
            "empty_count": new_empty,
            "total_chars": sum(new_lens),
        },
        "comparison": {
            "new_better": improved,
            "same": same,
            "old_better": worse,
            "improvement_rate": round(improved / total * 100, 1),
        },
        # 가장 큰 차이 TOP 10 (새 파서가 더 많이 추출)
        "top_improvements": sorted(
            [{"page_id": r["page_id"], "title": r["title"][:50],
              "old": r["old_parser"]["text_len"], "new": r["new_parser"]["text_len"],
              "diff": r["improvement"]["len_diff"]}
             for r in results if r["improvement"]["len_diff"] > 50],
            key=lambda x: x["diff"], reverse=True
        )[:10],
        # 새 파서가 덜 추출한 페이지 (주의 필요)
        "regressions": sorted(
            [{"page_id": r["page_id"], "title": r["title"][:50],
              "old": r["old_parser"]["text_len"], "new": r["new_parser"]["text_len"],
              "diff": r["improvement"]["len_diff"]}
             for r in results if r["improvement"]["len_diff"] < -20],
            key=lambda x: x["diff"]
        )[:10],
    }

    return report


# ── 메인 ─────────────────────────────────────────────────────

def run(page_id: str = None, limit: int = 0, report_only: bool = False):
    """파서 검증 실행."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 페이지 목록
    pages = get_pages_with_body(DB_PATH, page_id=page_id)
    if limit > 0:
        pages = pages[:limit]

    total = len(pages)
    log.info("=== 파서 검증 시작: %d개 페이지 ===", total)

    # 2. 각 페이지 파서 비교
    results = []
    t0 = time.time()

    for i, page in enumerate(pages, 1):
        try:
            result = compare_parsers(page)
            results.append(result)

            # 개별 결과 저장 (text는 별도 파일로)
            pid = page["source_id"]
            result_file = RESULTS_DIR / f"{pid}.json"
            save_data = {k: v for k, v in result.items()
                         if k != "new_parser" or k == "new_parser"}
            # text는 너무 길 수 있으므로 별도 txt로 저장
            text_file = RESULTS_DIR / f"{pid}.txt"
            text_file.write_text(result["new_parser"]["text"], encoding="utf-8")

            # JSON에는 text 제외
            save_result = dict(result)
            save_result["new_parser"] = {
                k: v for k, v in result["new_parser"].items() if k != "text"
            }
            save_result["text_file"] = str(text_file)
            result_file.write_text(
                json.dumps(save_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            if i % 100 == 0 or i <= 3:
                log.info("[%d/%d] %s: old=%d new=%d diff=%+d",
                         i, total, pid,
                         result["old_parser"]["text_len"],
                         result["new_parser"]["text_len"],
                         result["improvement"]["len_diff"])
        except Exception as e:
            log.warning("[%d/%d] 실패 %s: %s", i, total, page["source_id"], e)

    duration = round(time.time() - t0, 1)
    log.info("파서 검증 완료: %d개 처리 (%.1f초)", len(results), duration)

    # 3. 통계 리포트
    report = generate_report(results)
    report["duration_sec"] = duration

    report_file = RESULTS_DIR / "_report.json"
    report_file.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("리포트 저장: %s", report_file)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wiki 파서 검증")
    parser.add_argument("--page", help="단일 페이지 ID")
    parser.add_argument("--limit", type=int, default=0, help="최대 처리 수 (0=전체)")
    parser.add_argument("--report", action="store_true", help="기존 결과에서 리포트만 출력")
    args = parser.parse_args()

    result = run(page_id=args.page, limit=args.limit, report_only=args.report)
    print(json.dumps(result, indent=2, ensure_ascii=False))
