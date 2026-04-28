"""
capture_screenshots.py — Wiki 페이지 스크린샷 캡처

Confluence REST API에서 body.view(렌더된 HTML)를 가져와
로컬 HTML 파일로 래핑한 뒤 Playwright로 스크린샷을 캡처합니다.

사용법:
    python scripts/capture_screenshots.py                 # 전체 (미캡처 페이지만)
    python scripts/capture_screenshots.py --force          # 전체 강제 재캡처
    python scripts/capture_screenshots.py --page 552936235 # 단일 페이지
    python scripts/capture_screenshots.py --limit 10       # 처음 10개만
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import ssl
import sys
import tempfile
import time
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

from dotenv import load_dotenv

load_dotenv("D:/Vibe Dev/Slack Bot/.env")

# ── 설정 ─────────────────────────────────────────────────────

CONFLUENCE_URL = os.getenv("CONFLUENCE_URL", "https://wiki.smilegate.net")
CONFLUENCE_TOKEN = os.getenv("CONFLUENCE_TOKEN", "")

DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")
SCREENSHOT_DIR = PROJECT_ROOT / "cache" / "screenshots"
TEMPLATE_PATH = PROJECT_ROOT / "templates" / "confluence_viewer.html"

VIEWPORT_WIDTH = 1280
SCREENSHOT_TIMEOUT_MS = 15000

# ── 로깅 ─────────────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "capture_screenshots.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("screenshots")


# ── SSL 어댑터 (레거시 서버 호환) ─────────────────────────────

def _make_session():
    """SSL 레거시 호환 requests 세션."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.ssl_ import create_urllib3_context

    class _LegacySSL(HTTPAdapter):
        def init_poolmanager(self, *a, **kw):
            ctx = create_urllib3_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
            kw["ssl_context"] = ctx
            super().init_poolmanager(*a, **kw)

    s = requests.Session()
    s.mount("https://", _LegacySSL())
    s.headers["Authorization"] = f"Bearer {CONFLUENCE_TOKEN}"
    return s


# ── DB 조회 ──────────────────────────────────────────────────

def get_wiki_pages(db_path: str, page_id: str = None) -> list[dict]:
    """캐시 DB에서 body_raw 있는 wiki 페이지 목록 조회."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if page_id:
            rows = conn.execute("""
                SELECT n.source_id, n.title, n.url
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = 'wiki'
                  AND n.source_id = ?
                  AND dc.body_raw IS NOT NULL AND dc.body_raw != ''
            """, (page_id,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT n.source_id, n.title, n.url
                FROM nodes n
                JOIN doc_content dc ON dc.node_id = n.id
                WHERE n.source_type = 'wiki'
                  AND dc.body_raw IS NOT NULL AND dc.body_raw != ''
                ORDER BY n.source_id
            """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── body.view 가져오기 ───────────────────────────────────────

def fetch_body_view(session, page_id: str) -> str | None:
    """REST API에서 body.view (렌더된 HTML) 가져오기."""
    url = f"{CONFLUENCE_URL}/rest/api/content/{page_id}"
    try:
        r = session.get(url, params={"expand": "body.view"}, timeout=20)
        if r.status_code == 200:
            data = r.json()
            return data.get("body", {}).get("view", {}).get("value", "")
        else:
            log.warning("REST API %s: HTTP %d", page_id, r.status_code)
            return None
    except Exception as e:
        log.warning("REST API %s 실패: %s", page_id, e)
        return None


# ── HTML 래핑 + 스크린샷 ─────────────────────────────────────

def wrap_html(title: str, body_html: str) -> str:
    """body.view HTML을 Confluence 스타일 템플릿에 래핑."""
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    html = template.replace("{{TITLE}}", title)
    html = html.replace("{{BODY}}", body_html)
    return html


def capture_page_screenshot(page, html_content: str, output_path: Path,
                            browser) -> bool:
    """Playwright로 로컬 HTML 렌더 후 스크린샷 캡처."""
    # 임시 HTML 파일 저장
    tmp_file = Path(tempfile.gettempdir()) / f"wiki_{page['source_id']}.html"
    tmp_file.write_text(html_content, encoding="utf-8")

    try:
        context = browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": 800},
            ignore_https_errors=True,
        )
        pg = context.new_page()
        pg.goto(f"file:///{tmp_file.as_posix()}", timeout=SCREENSHOT_TIMEOUT_MS)
        pg.wait_for_load_state("networkidle", timeout=5000)

        # 전체 페이지 스크린샷
        pg.screenshot(path=str(output_path), full_page=True)

        context.close()
        return True
    except Exception as e:
        log.warning("스크린샷 실패 %s: %s", page["source_id"], e)
        try:
            context.close()
        except Exception:
            pass
        return False
    finally:
        try:
            tmp_file.unlink(missing_ok=True)
        except Exception:
            pass


# ── 메인 ─────────────────────────────────────────────────────

def run(page_id: str = None, force: bool = False, limit: int = 0):
    """스크린샷 캡처 실행."""
    from playwright.sync_api import sync_playwright

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 대상 페이지 목록
    pages = get_wiki_pages(DB_PATH, page_id=page_id)
    if limit > 0:
        pages = pages[:limit]

    total = len(pages)
    log.info("=== 스크린샷 캡처 시작: %d개 페이지 ===", total)

    # 2. 이미 캡처된 페이지 필터링
    if not force:
        before = len(pages)
        pages = [p for p in pages
                 if not (SCREENSHOT_DIR / f"{p['source_id']}.png").exists()]
        skipped = before - len(pages)
        if skipped:
            log.info("이미 캡처됨 %d개 스킵, 남은 %d개", skipped, len(pages))

    if not pages:
        log.info("캡처할 페이지 없음")
        return {"total": total, "captured": 0, "failed": 0, "skipped": total}

    # 3. HTTP 세션 + Playwright 시작
    session = _make_session()

    captured = 0
    failed = 0
    t0 = time.time()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)

        for i, page in enumerate(pages, 1):
            pid = page["source_id"]
            title = page["title"]
            output = SCREENSHOT_DIR / f"{pid}.png"

            # body.view 가져오기
            body_html = fetch_body_view(session, pid)
            if not body_html:
                log.warning("[%d/%d] %s: body.view 없음, 스킵", i, len(pages), pid)
                failed += 1
                continue

            # HTML 래핑 + 스크린샷
            full_html = wrap_html(title, body_html)
            ok = capture_page_screenshot(page, full_html, output, browser)

            if ok:
                captured += 1
                size_kb = output.stat().st_size / 1024
                if i % 50 == 0 or i <= 3:
                    log.info("[%d/%d] OK %s (%.0fKB) - %s",
                             i, len(pages), pid, size_kb, title[:40])
            else:
                failed += 1
                # 1회 재시도
                time.sleep(1)
                ok2 = capture_page_screenshot(page, full_html, output, browser)
                if ok2:
                    captured += 1
                    failed -= 1
                    log.info("[%d/%d] 재시도 OK %s", i, len(pages), pid)

            # 서버 부하 방지 (REST API 호출 간격)
            if i % 10 == 0:
                time.sleep(0.5)

        browser.close()

    duration = round(time.time() - t0, 1)
    result = {
        "total": total,
        "captured": captured,
        "failed": failed,
        "skipped": total - len(pages),
        "duration_sec": duration,
    }
    log.info("=== 캡처 완료: %s (%.1f초) ===", result, duration)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wiki 페이지 스크린샷 캡처")
    parser.add_argument("--page", help="단일 페이지 ID")
    parser.add_argument("--force", action="store_true", help="기존 스크린샷 덮어쓰기")
    parser.add_argument("--limit", type=int, default=0, help="최대 캡처 수 (0=전체)")
    args = parser.parse_args()

    result = run(page_id=args.page, force=args.force, limit=args.limit)
    print(json.dumps(result, indent=2, ensure_ascii=False))
