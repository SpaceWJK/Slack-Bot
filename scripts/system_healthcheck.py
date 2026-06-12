"""
system_healthcheck.py — Slack Bot 시스템 검수/시뮬레이션 도구

시스템의 모든 모듈, 기능, 연동을 검증하는 헬스체크 스크립트.
실제 Slack 메시지를 보내지 않고 코드 레벨에서 구동 가능 여부를 확인한다.

사용법:
  python scripts/system_healthcheck.py              # 전체 검수
  python scripts/system_healthcheck.py --module wiki # 특정 모듈만
  python scripts/system_healthcheck.py --quick       # 빠른 검수 (MCP 제외)
  python scripts/system_healthcheck.py --fix         # 문제 발견 시 자동 제안

검수 항목:
  1. 모듈 임포트 검증 — 모든 .py 파일 import 가능 여부
  2. 환경변수 검증   — 필수 환경변수 존재 여부
  3. MCP 연결 검증   — Wiki/Jira/GDI MCP 서버 응답 확인
  4. 캐시 DB 검증    — SQLite 스키마 + 데이터 무결성
  5. 설정 파일 검증  — config.json 구조 + 스케줄 유효성
  6. 레거시 탐지     — 사용하지 않는 모듈/코드 감지
  7. 로그 분석       — 최근 에러 패턴 요약
"""

import sys
import os
import io
import json
import importlib
import sqlite3
import time
import re
from datetime import datetime, timedelta
from pathlib import Path

# Windows CP949 콘솔 대응
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BOT_DIR = PROJECT_ROOT / "Slack Bot"
CACHE_DIR = Path("D:/Vibe Dev/QA Ops/mcp-cache-layer")
VENV_SITE = PROJECT_ROOT / "venv" / "Lib" / "site-packages"

# venv site-packages 우선 등록 (slack_bolt, slack_sdk 등)
if VENV_SITE.exists():
    sys.path.insert(0, str(VENV_SITE))
sys.path.insert(0, str(BOT_DIR))
sys.path.insert(0, str(CACHE_DIR))

# ── 결과 수집 ────────────────────────────────────────────────

class CheckResult:
    def __init__(self, category: str, name: str, status: str,
                 detail: str = "", fix_hint: str = ""):
        self.category = category
        self.name = name
        self.status = status       # PASS, FAIL, WARN, SKIP
        self.detail = detail
        self.fix_hint = fix_hint

results: list[CheckResult] = []

def _add(cat, name, status, detail="", fix=""):
    results.append(CheckResult(cat, name, status, detail, fix))

# ── 1. 모듈 임포트 검증 ────────────────────────────────────────

CORE_MODULES = [
    "slack_bot", "slack_sender", "mcp_session",
    "wiki_client", "gdi_client", "jira_client",
    "scheduler", "interaction_handler", "missed_tracker",
    "schedule_monitor", "keyword_rules", "game_aliases",
    "safety_guard", "response_formatter", "message_expiry",
]

OPTIONAL_MODULES = [
    "claim_handler", "test_mission", "repair_checklist",
    "update_mission_progress",
]

def check_module_imports():
    """모든 핵심 모듈의 import 가능 여부 확인."""
    for mod_name in CORE_MODULES:
        try:
            importlib.import_module(mod_name)
            _add("모듈", mod_name, "PASS")
        except Exception as e:
            _add("모듈", mod_name, "FAIL", str(e),
                 f"모듈 의존성 확인: {mod_name}.py")

    for mod_name in OPTIONAL_MODULES:
        try:
            importlib.import_module(mod_name)
            _add("모듈", f"{mod_name} (선택)", "PASS")
        except Exception as e:
            _add("모듈", f"{mod_name} (선택)", "WARN", str(e))


# ── 2. 환경변수 검증 ──────────────────────────────────────────

REQUIRED_ENV = [
    ("SLACK_BOT_TOKEN", "Slack 봇 토큰"),
    ("SLACK_APP_TOKEN", "Slack 앱 토큰"),
    ("ANTHROPIC_API_KEY", "Claude API 키"),
]

OPTIONAL_ENV = [
    ("CONFLUENCE_USERNAME", "Wiki 사용자명"),
    ("CONFLUENCE_TOKEN", "Wiki 토큰"),
    ("JIRA_USERNAME", "Jira 사용자명"),
    ("JIRA_TOKEN", "Jira 토큰"),
    ("GDI_MCP_URL", "GDI MCP URL"),
    ("WIKI_MCP_URL", "Wiki MCP URL"),
    ("JIRA_MCP_URL", "Jira MCP URL"),
]

def check_env_vars():
    """필수 환경변수 존재 여부 확인."""
    from dotenv import load_dotenv
    load_dotenv(str(PROJECT_ROOT / ".env"), override=True)

    for key, desc in REQUIRED_ENV:
        val = os.getenv(key, "")
        if val:
            masked = val[:4] + "***" + val[-4:] if len(val) > 8 else "***"
            _add("환경변수", f"{key} ({desc})", "PASS", masked)
        else:
            _add("환경변수", f"{key} ({desc})", "FAIL", "미설정",
                 f".env 파일에 {key} 추가")

    for key, desc in OPTIONAL_ENV:
        val = os.getenv(key, "")
        if val:
            _add("환경변수", f"{key} ({desc})", "PASS")
        else:
            _add("환경변수", f"{key} ({desc})", "WARN", "미설정 (옵션)")


# ── 3. MCP 연결 검증 ──────────────────────────────────────────

def check_mcp_connections(skip: bool = False):
    """Wiki/Jira/GDI MCP 서버 응답 확인."""
    if skip:
        _add("MCP", "Wiki MCP", "SKIP", "빠른 검수 모드")
        _add("MCP", "Jira MCP", "SKIP", "빠른 검수 모드")
        _add("MCP", "GDI MCP", "SKIP", "빠른 검수 모드")
        return

    try:
        from mcp_core import McpSession
    except ImportError:
        _add("MCP", "mcp_session 임포트", "FAIL", "mcp-core 패키지 미설치")
        return

    from dotenv import load_dotenv
    load_dotenv(str(PROJECT_ROOT / ".env"))

    # Wiki MCP
    wiki_url = os.getenv("WIKI_MCP_URL", "http://mcp.sginfra.net/confluence-wiki-mcp")
    try:
        mcp = McpSession(url=wiki_url, headers={
            "x-confluence-wiki-username": os.getenv("CONFLUENCE_USERNAME", ""),
            "x-confluence-wiki-token": os.getenv("CONFLUENCE_TOKEN", ""),
        }, label="wiki")
        data, err = mcp.call_tool("get_all_spaces", {})
        if err:
            _add("MCP", "Wiki MCP", "FAIL", f"오류: {err}")
        else:
            _add("MCP", "Wiki MCP", "PASS", f"응답 정상")
    except Exception as e:
        _add("MCP", "Wiki MCP", "FAIL", str(e))

    # Jira MCP
    jira_url = os.getenv("JIRA_MCP_URL", "http://mcp.sginfra.net/confluence-jira-mcp")
    try:
        mcp = McpSession(url=jira_url, headers={
            "x-confluence-jira-username": os.getenv("JIRA_USERNAME", ""),
            "x-confluence-jira-token": os.getenv("JIRA_TOKEN", ""),
        }, label="jira")
        data, err = mcp.call_tool("get_all_projects", {})
        if err:
            _add("MCP", "Jira MCP", "FAIL", f"오류: {err}")
        else:
            _add("MCP", "Jira MCP", "PASS", f"응답 정상")
    except Exception as e:
        _add("MCP", "Jira MCP", "FAIL", str(e))

    # GDI MCP
    gdi_url = os.getenv("GDI_MCP_URL", "http://mcp-dev.sginfra.net/game-doc-insight-mcp")
    try:
        mcp = McpSession(url=gdi_url, label="gdi")
        data, err = mcp.call_tool("test_game_doc_insight_connection", {})
        if err:
            _add("MCP", "GDI MCP", "FAIL", f"오류: {err}")
        else:
            _add("MCP", "GDI MCP", "PASS", f"응답 정상")
    except Exception as e:
        _add("MCP", "GDI MCP", "FAIL", str(e))


# ── 4. 캐시 DB 검증 ──────────────────────────────────────────

def check_cache_db():
    """SQLite 캐시 DB 무결성 + 적재 상태 확인."""
    db_path = str(CACHE_DIR / "cache" / "mcp_cache.db")
    if not os.path.exists(db_path):
        _add("캐시DB", "mcp_cache.db", "WARN", "파일 없음 (최초 실행 전)")
        return

    try:
        conn = sqlite3.connect(db_path)

        # 무결성 체크
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result[0] == "ok":
            _add("캐시DB", "무결성 체크", "PASS")
        else:
            _add("캐시DB", "무결성 체크", "FAIL", result[0])

        # 테이블 존재 확인
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        expected = {"nodes", "doc_content", "doc_meta", "sync_log"}
        missing = expected - tables
        if missing:
            _add("캐시DB", "테이블 구조", "FAIL", f"누락: {missing}",
                 "python -c \"from src.models import migrate; migrate()\"")
        else:
            _add("캐시DB", "테이블 구조", "PASS")

        # 소스별 적재 현황
        for src in ["wiki", "jira", "gdi"]:
            row = conn.execute(
                "SELECT COUNT(*) FROM nodes WHERE source_type=?", (src,)
            ).fetchone()
            count = row[0]
            status = "PASS" if count > 0 else "WARN"
            _add("캐시DB", f"{src} 적재량", status, f"{count}건")

        # 최근 동기화 기록
        syncs = conn.execute(
            "SELECT source_type, scope, sync_type, finished_at, status "
            "FROM sync_log ORDER BY finished_at DESC LIMIT 5"
        ).fetchall()
        if syncs:
            for s in syncs:
                status = "PASS" if s[4] == "success" else "WARN"
                _add("캐시DB", f"sync: {s[0]}/{s[1]}", status,
                     f"{s[2]} @ {s[3]}")
        else:
            _add("캐시DB", "동기화 기록", "WARN", "기록 없음")

        # orphan 노드 (doc_content 없는 nodes)
        orphan = conn.execute(
            "SELECT COUNT(*) FROM nodes n "
            "LEFT JOIN doc_content dc ON dc.node_id=n.id "
            "WHERE dc.id IS NULL"
        ).fetchone()[0]
        if orphan > 0:
            _add("캐시DB", "orphan 노드", "WARN", f"{orphan}건 (본문 없음)",
                 "SELECT source_type, source_id FROM nodes WHERE id NOT IN "
                 "(SELECT node_id FROM doc_content)")
        else:
            _add("캐시DB", "orphan 노드", "PASS", "없음")

        conn.close()
    except Exception as e:
        _add("캐시DB", "DB 접근", "FAIL", str(e))


# ── 5. 설정 파일 검증 ──────────────────────────────────────────

def check_config():
    """config.json 구조 + 스케줄 유효성."""
    config_path = BOT_DIR / "config.json"
    if not config_path.exists():
        _add("설정", "config.json", "FAIL", "파일 없음")
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        _add("설정", "config.json 파싱", "PASS")
    except Exception as e:
        _add("설정", "config.json 파싱", "FAIL", str(e))
        return

    # 필수 키 확인
    for key in ["timezone", "schedules", "user_map"]:
        if key in config:
            _add("설정", f"config.{key}", "PASS")
        else:
            _add("설정", f"config.{key}", "FAIL", "키 누락")

    # 스케줄 유효성
    schedules = config.get("schedules", [])
    enabled = [s for s in schedules if s.get("enabled")]
    disabled = [s for s in schedules if not s.get("enabled")]
    _add("설정", "스케줄 현황", "PASS",
         f"활성 {len(enabled)}개, 비활성 {len(disabled)}개")

    for s in enabled:
        sid = s.get("id", "?")
        stype = s.get("type", "?")
        # channel 존재 확인
        if not s.get("channel"):
            _add("설정", f"스케줄 {sid}", "WARN", "channel 미설정")

    # wiki_search_rules.json
    rules_path = BOT_DIR / "wiki_search_rules.json"
    if rules_path.exists():
        try:
            with open(rules_path, "r", encoding="utf-8") as f:
                rules = json.load(f)
            _add("설정", "wiki_search_rules.json", "PASS", f"{len(rules)}개 규칙")
        except Exception as e:
            _add("설정", "wiki_search_rules.json", "FAIL", str(e))
    else:
        _add("설정", "wiki_search_rules.json", "WARN", "파일 없음")


# ── 6. 레거시 탐지 ────────────────────────────────────────────

def check_legacy():
    """사용하지 않는 모듈/코드 감지."""
    # _legacy 폴더
    legacy_dir = PROJECT_ROOT / "_legacy"
    if legacy_dir.exists():
        files = list(legacy_dir.rglob("*"))
        _add("레거시", "_legacy/ 폴더", "WARN",
             f"{len(files)}개 파일 (삭제 예정)",
             "rm -rf _legacy/")
    else:
        _add("레거시", "_legacy/ 폴더", "PASS", "없음")

    # BOT_DIR 내 미사용 .py 파일 감지 (어떤 .py에서도 import 되지 않는 파일)
    bot_py_files = {f.stem for f in BOT_DIR.glob("*.py")}
    all_imported = set()
    for py_file in BOT_DIR.glob("*.py"):
        try:
            code = py_file.read_text(encoding="utf-8")
            for m in re.findall(r'(?:from|import)\s+(\w+)', code):
                all_imported.add(m)
        except Exception:
            pass
    # 메인 파일 자체 + __init__ 제외
    bot_py_files.discard("slack_bot")
    bot_py_files.discard("__init__")
    bot_py_files.discard("__pycache__")

    unused = bot_py_files - all_imported
    if unused:
        _add("레거시", "미참조 모듈", "WARN",
             f"{', '.join(sorted(unused))}",
             "프로젝트 내 어떤 .py에서도 import 되지 않는 파일")
    else:
        _add("레거시", "미참조 모듈", "PASS", "모두 참조됨")


# ── 7. 로그 분석 ──────────────────────────────────────────────

def check_logs():
    """최근 로그에서 에러 패턴 요약."""
    logs_dir = PROJECT_ROOT / "logs"
    if not logs_dir.exists():
        _add("로그", "logs/ 폴더", "WARN", "없음")
        return

    log_files = {
        "wiki_query.log": "Wiki 조회",
        "gdi_query.log": "GDI 조회",
        "jira_query.log": "Jira 조회",
        "answer_miss.log": "답변 실패",
    }

    for fname, desc in log_files.items():
        fpath = logs_dir / fname
        if not fpath.exists():
            _add("로그", f"{desc} ({fname})", "WARN", "파일 없음")
            continue

        # 파일 크기 + 마지막 수정일
        stat = fpath.stat()
        size_kb = stat.st_size / 1024
        mtime = datetime.fromtimestamp(stat.st_mtime)
        age = datetime.now() - mtime

        # 최근 에러 카운트 (최근 7일)
        error_count = 0
        total_lines = 0
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    total_lines += 1
                    if "ERROR" in line or "error" in line.lower():
                        error_count += 1
        except Exception:
            pass

        detail = f"{size_kb:.0f}KB, {total_lines}줄, 에러 {error_count}건"
        if age > timedelta(days=7):
            detail += f", 마지막 갱신: {age.days}일 전"
        status = "WARN" if error_count > 10 else "PASS"
        _add("로그", f"{desc} ({fname})", status, detail)


# ── 리포트 출력 ────────────────────────────────────────────────

def print_report(show_fix: bool = False):
    """검수 결과 리포트 출력."""
    print(f"\n{'='*70}")
    print(f"  Slack Bot 시스템 헬스체크 리포트")
    print(f"  실행: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    # 카테고리별 그룹핑
    categories = {}
    for r in results:
        categories.setdefault(r.category, []).append(r)

    stats = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}
    for cat, items in categories.items():
        print(f"\n■ {cat}")
        print("-" * 50)
        for r in items:
            icon = {"PASS": "[OK]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}[r.status]
            stats[r.status] += 1
            line = f"  {icon} {r.name}"
            if r.detail:
                line += f"  — {r.detail}"
            print(line)
            if show_fix and r.fix_hint and r.status in ("FAIL", "WARN"):
                print(f"     -> {r.fix_hint}")

    # 요약
    total = sum(stats.values())
    print(f"\n{'='*70}")
    print(f"  요약: {total}건 검수")
    print(f"  PASS: {stats['PASS']}  FAIL: {stats['FAIL']}  "
          f"WARN: {stats['WARN']}  SKIP: {stats['SKIP']}")

    if stats["FAIL"] > 0:
        print(f"\n  ** FAIL 항목이 있습니다. --fix 옵션으로 수정 제안을 확인하세요.")
    elif stats["WARN"] > 0:
        print(f"\n  * 경고 항목이 있습니다. 확인이 필요합니다.")
    else:
        print(f"\n  ALL CLEAR - 모든 검수 통과!")
    print(f"{'='*70}")

    return stats


# ── CLI ────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Slack Bot 시스템 헬스체크")
    parser.add_argument("--module", type=str, default="",
                        help="특정 모듈만 검수 (wiki, jira, gdi, cache, config)")
    parser.add_argument("--quick", action="store_true",
                        help="빠른 검수 (MCP 연결 제외)")
    parser.add_argument("--fix", action="store_true",
                        help="문제 발견 시 수정 제안 표시")
    args = parser.parse_args()

    module_filter = args.module.lower()

    if not module_filter or module_filter == "module":
        check_module_imports()
    if not module_filter or module_filter == "env":
        check_env_vars()
    if not module_filter or module_filter in ("wiki", "jira", "gdi", "mcp"):
        check_mcp_connections(skip=args.quick)
    if not module_filter or module_filter == "cache":
        check_cache_db()
    if not module_filter or module_filter == "config":
        check_config()
    if not module_filter or module_filter == "legacy":
        check_legacy()
    if not module_filter or module_filter == "log":
        check_logs()

    stats = print_report(show_fix=args.fix)

    # 종료 코드: FAIL 있으면 1
    sys.exit(1 if stats["FAIL"] > 0 else 0)


if __name__ == "__main__":
    main()
