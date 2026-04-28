"""
auto_sync.py — Wiki + Jira + GDI 자동적재 스크립트

Windows Task Scheduler에서 주기적으로 호출:
  1. Wiki: delta sync (변경 페이지만)
  2. Jira: delta sync (변경 이슈만)
  3. GDI:  S3 → gdi-repo/ 동기화 → 로컬 파싱 → SQLite 적재

⚠️ S3 안전 원칙:
  - 자동 동기화: S3 → gdi-repo/ 다운로드만 (업로드/삭제 절대 불가)
  - 로컬 CLI 수동: 업로드(사용자 승인 1회) / 삭제(사용자 승인 2회)
  - GDI MCP 경유: 읽기 전용 (쓰기/삭제 원천 차단)

사용법:
    python scripts/auto_sync.py              # delta sync (일반 주기)
    python scripts/auto_sync.py --full-wiki  # Wiki 전체 적재 강제 실행
"""

import sys
import os
import json
import time
import subprocess
import shutil
import logging
from datetime import datetime
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

from src.models import init_db, get_connection, migrate
from src.cache_manager import CacheManager
from src.sync_engine import SyncEngine
from mcp_session import McpSession
from dotenv import load_dotenv

# ── 로깅 설정 ────────────────────────────────────────────────
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "auto_sync.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("auto_sync")

# ── 환경변수 ──────────────────────────────────────────────────
load_dotenv(str(PROJECT_ROOT / ".env"))              # mcp-cache-layer .env (S3 설정 등)
load_dotenv("D:/Vibe Dev/Slack Bot/.env")            # Slack Bot .env (MCP 인증 등)

WIKI_MCP_URL = os.getenv("WIKI_MCP_URL", "http://mcp.sginfra.net/confluence-wiki-mcp")
WIKI_USERNAME = os.getenv("CONFLUENCE_USERNAME", "es-wjkim")
WIKI_TOKEN = os.getenv("CONFLUENCE_TOKEN", "")

JIRA_MCP_URL = os.getenv("JIRA_MCP_URL", "http://mcp.sginfra.net/confluence-jira-mcp")
JIRA_USERNAME = os.getenv("JIRA_USERNAME", "es-wjkim")
JIRA_TOKEN = os.getenv("JIRA_TOKEN", "")

WIKI_SPACE_KEY = "QASGP"

# GDI 모드 스위치 ("local" | "cloud")
GDI_MODE = os.getenv("GDI_MODE", "local")

DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")

# ── S3 → gdi-repo/ 단방향 동기화 설정 ─────────────────────────────
#
# ⚠️ S3 안전 원칙:
#   1. 자동 동기화: S3 → gdi-repo/ 다운로드만 수행 (업로드/삭제 절대 불가)
#   2. 로컬 CLI 수동: 업로드(사용자 승인 1회), 삭제(사용자 승인 2회) 필요
#   3. GDI MCP 경유: 읽기 전용 — 쓰기/업로드/삭제 원천 차단
#      (MCP는 공유 서버이므로 데이터 변경 경로 완전 차단)
#
GDI_S3_BUCKET = os.getenv("GDI_S3_BUCKET", "game-doc-insight-resource")
GDI_S3_SYNC = os.getenv("GDI_S3_SYNC", "true").lower() == "true"
GDI_S3_TIMEOUT = int(os.getenv("GDI_S3_TIMEOUT", "300"))
GDI_S3_EXCLUDE = [p.strip() for p in
                  os.getenv("GDI_S3_EXCLUDE", "*.png,*.jpg,*.jpeg").split(",")]
GDI_S3_GAMES = [g.strip() for g in
                os.getenv("GDI_S3_GAMES", "Chaoszero").split(",")]
GDI_REPO = str(PROJECT_ROOT / "gdi-repo")

# AWS CLI 탐색
AWS_CLI = shutil.which("aws") or r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"


# ── MCP 세션 생성 ─────────────────────────────────────────────

def _wiki_mcp() -> McpSession:
    return McpSession(
        url=WIKI_MCP_URL,
        headers={
            "x-confluence-wiki-username": WIKI_USERNAME,
            "x-confluence-wiki-token": WIKI_TOKEN,
        },
        label="wiki",
    )


def _jira_mcp() -> McpSession:
    return McpSession(
        url=JIRA_MCP_URL,
        headers={
            "x-confluence-jira-username": JIRA_USERNAME,
            "x-confluence-jira-token": JIRA_TOKEN,
        },
        label="jira",
    )


# ── Wiki 동기화 ───────────────────────────────────────────────

def _wiki_needs_full_ingest() -> bool:
    """Wiki 전체 적재가 필요한지 판별.

    sync_log에 'full' type의 성공 기록이 없거나,
    전체 적재 시 scanned 수가 max_pages 제한(round number)에 딱 걸려 있으면 True.
    (예: 2000건 정확히 → 더 있을 가능성 → 재실행 필요)
    """
    conn = get_connection(DB_PATH)
    try:
        row = conn.execute(
            "SELECT pages_scanned FROM sync_log "
            "WHERE source_type='wiki' AND scope=? AND sync_type='full' AND status='success' "
            "ORDER BY started_at DESC LIMIT 1",
            (WIKI_SPACE_KEY,)
        ).fetchone()
        if not row:
            return True
        scanned = row[0]
        # max_pages 제한에 정확히 걸린 경우 (500, 1000, 2000 등 round number)
        # → 더 많은 페이지가 있을 가능성
        if scanned in (500, 1000, 2000, 5000):
            return True
        return False
    finally:
        conn.close()


def sync_wiki(force_full: bool = False) -> dict:
    """Wiki 동기화.

    force_full=True이면 전체 적재.
    아니면 자동 판별: 전체 적재가 필요하면 full, 아니면 delta.
    """
    cache = CacheManager(db_path=DB_PATH)
    mcp = _wiki_mcp()
    engine = SyncEngine(cache, mcp)

    if force_full or _wiki_needs_full_ingest():
        log.info("=== Wiki Full Ingest 시작 (space=%s) ===", WIKI_SPACE_KEY)
        result = engine.full_ingest("wiki", WIKI_SPACE_KEY, fetch_body=True)
        log.info("Wiki Full Ingest 완료: %s", result)
    else:
        log.info("=== Wiki Delta Sync 시작 (space=%s) ===", WIKI_SPACE_KEY)
        result = engine.delta_sync("wiki", WIKI_SPACE_KEY, fetch_body=True)
        log.info("Wiki Delta Sync 완료: %s", result)

    # ── Repair: body 누락 + parent_id 고아 복구 ──
    try:
        repair_body = engine.repair_missing_content("wiki")
        repair_parent = engine.repair_parent_ids("wiki")
        if repair_body["repaired"] or repair_parent["repaired"]:
            log.info("[Repair] body=%s, parent=%s", repair_body, repair_parent)
        result["repair_body"] = repair_body
        result["repair_parent"] = repair_parent
    except Exception as e:
        log.warning("[Repair] 복구 중 오류 (동기화 결과는 정상): %s", e)

    return result


# ── Jira 동기화 ──────────────────────────────────────────────

def sync_jira() -> dict:
    """적재된 모든 Jira 프로젝트 Delta Sync."""
    # load_jira.py의 delta_sync_all() 재사용
    from scripts.load_jira import delta_sync_all
    log.info("=== Jira Delta Sync 시작 ===")
    results = delta_sync_all()
    log.info("Jira Delta Sync 완료: %s", {k: v.get("scanned", 0) for k, v in results.items()})
    return results


# ── S3 → gdi-repo/ 단방향 동기화 ─────────────────────────────

def _build_s3_sync_cmd(game: str) -> list[str]:
    """S3 → 로컬 단방향 sync 명령 생성.

    ⚠️ 구조적 안전장치:
    - 인자 순서: s3://source → local/dest (역방향 생성 불가)
    - --delete 미사용 (로컬 파일 삭제 방지)
    - 읽기 전용 명령(sync)만 사용
    """
    s3_source = f"s3://{GDI_S3_BUCKET}/{game}/"     # ← 항상 S3가 source
    local_dest = str(Path(GDI_REPO) / game)          # ← 항상 로컬이 dest

    cmd = [AWS_CLI, "s3", "sync", s3_source, local_dest, "--no-progress"]
    #                              ^^^^^^^^  ^^^^^^^^^^
    #                              SOURCE    DEST  (이 순서 고정, 역전 불가)

    for pattern in GDI_S3_EXCLUDE:
        if pattern:
            cmd.extend(["--exclude", pattern])

    # ⚠️ --delete 절대 사용 금지 (S3에서 사라진 파일을 로컬에서 삭제하지 않음)
    return cmd


def _parse_sync_output(stdout: str) -> int:
    """aws s3 sync stdout에서 다운로드 파일 수 파싱."""
    if not stdout:
        return 0
    return sum(1 for line in stdout.strip().splitlines()
               if line.strip().startswith("download:"))


def _sync_s3(games: list[str] | None = None) -> dict:
    """S3 → gdi-repo/ 단방향 동기화 (다운로드 전용).

    ⚠️ 안전 원칙:
    - S3 → 로컬 다운로드만 수행 (업로드/삭제 절대 불가)
    - 실패 시 기존 gdi-repo/ 파일 그대로 유지 (fallback)
    - 모든 에러는 auto_sync.log에 상세 기록 (CMD + STDERR)
    """
    if not GDI_S3_SYNC:
        log.info("S3 sync 비활성화 (GDI_S3_SYNC=false)")
        return {"skipped": True}

    target_games = games or GDI_S3_GAMES
    results = {}

    for game in target_games:
        game = game.strip()
        if not game:
            continue

        t0 = time.time()
        local_dest = Path(GDI_REPO) / game
        local_dest.mkdir(parents=True, exist_ok=True)

        try:
            cmd = _build_s3_sync_cmd(game)
            log.info("S3 sync 시작 [%s] s3://%s/%s/ → %s",
                     game, GDI_S3_BUCKET, game, local_dest)

            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=GDI_S3_TIMEOUT,
                encoding="utf-8",
                errors="replace",       # 한글 파일명 깨짐 방지
            )

            elapsed = round(time.time() - t0, 1)

            if proc.returncode != 0:
                stderr_msg = (proc.stderr.strip()[:500]
                              if proc.stderr else "(empty)")
                log.error(
                    "S3 sync 실패 [%s] returncode=%d (%.1fs)\n"
                    "  CMD: %s\n"
                    "  STDERR: %s",
                    game, proc.returncode, elapsed,
                    " ".join(cmd), stderr_msg,
                )
                results[game] = {
                    "status": "error",
                    "returncode": proc.returncode,
                    "stderr": stderr_msg,
                    "duration_sec": elapsed,
                }
                continue        # 실패해도 다음 게임 계속 진행

            downloaded = _parse_sync_output(proc.stdout)
            log.info("S3 sync 완료 [%s] %d건 다운로드 (%.1fs)",
                     game, downloaded, elapsed)
            results[game] = {
                "status": "ok",
                "downloaded": downloaded,
                "duration_sec": elapsed,
            }

        except subprocess.TimeoutExpired:
            elapsed = round(time.time() - t0, 1)
            log.error("S3 sync 타임아웃 [%s] %d초 초과 (%.1fs)",
                      game, GDI_S3_TIMEOUT, elapsed)
            results[game] = {
                "status": "timeout",
                "timeout_sec": GDI_S3_TIMEOUT,
                "duration_sec": elapsed,
            }

        except FileNotFoundError:
            log.error("AWS CLI 미설치 또는 경로 오류 (path=%s) "
                      "— S3 sync 전체 스킵", AWS_CLI)
            results[game] = {
                "status": "error",
                "stderr": f"aws CLI not found: {AWS_CLI}",
            }
            break                # CLI 없으면 전체 중단

        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            log.error("S3 sync 예외 [%s]: %s", game, e, exc_info=True)
            results[game] = {
                "status": "error",
                "stderr": str(e)[:500],
                "duration_sec": elapsed,
            }

    return results


# ── GDI 동기화 (로컬 레포 기반) ───────────────────────────────

def sync_gdi() -> dict:
    """GDI Delta 적재 — GDI_MODE에 따라 loader 자동 선택.

    - local  : gdi-repo/ 로컬 파일 직접 파싱 (load_gdi_local)
    - cloud  : MCP 서버 경유 (load_gdi)
    """
    if GDI_MODE == "local":
        # ① S3 → gdi-repo/ 동기화 (새/변경 파일 다운로드만)
        s3_result = _sync_s3()

        # ② 로컬 파일 파싱 → SQLite 적재 (기존 파이프라인 그대로)
        from scripts.load_gdi_local import delta_ingest_all
        log.info("=== GDI Local Delta Sync 시작 (gdi-repo/, mode=%s) ===", GDI_MODE)
        results = delta_ingest_all()
        results["_s3_sync"] = s3_result
        log.info("GDI Local Delta Sync 완료: %s",
                 {k: v.get("added", 0) for k, v in results.items()
                  if k != "_s3_sync"})
    else:
        from scripts.load_gdi import delta_ingest_all
        log.info("=== GDI Cloud Delta Sync 시작 (MCP, mode=%s) ===", GDI_MODE)
        results = delta_ingest_all()
        log.info("GDI Cloud Delta Sync 완료: %s",
                 {k: v.get("scanned", 0) for k, v in results.items()})
    return results


# ── 메인 ─────────────────────────────────────────────────────

def run(force_full_wiki: bool = False):
    """Wiki + Jira + GDI 자동적재 실행."""
    t0 = time.time()
    log.info("=" * 60)
    log.info("자동적재 시작 (%s)", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # DB 마이그레이션
    migrate(DB_PATH)

    # 1. Wiki 동기화
    try:
        wiki_result = sync_wiki(force_full=force_full_wiki)
    except Exception as e:
        log.error("Wiki 동기화 실패: %s", e, exc_info=True)
        wiki_result = {"error": str(e)}

    # 2. Jira 동기화
    try:
        jira_result = sync_jira()
    except Exception as e:
        log.error("Jira 동기화 실패: %s", e, exc_info=True)
        jira_result = {"error": str(e)}

    # 3. GDI 동기화 (신규 파일만)
    try:
        gdi_result = sync_gdi()
    except Exception as e:
        log.error("GDI 동기화 실패: %s", e, exc_info=True)
        gdi_result = {"error": str(e)}

    # 4. Enrichment (summary/keywords 보강) + sync_log 기록
    enrich_result = {}
    enrich_started = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    enrich_t0 = time.time()
    try:
        from src.enrichment import EnrichmentEngine
        enrich_engine = EnrichmentEngine(CacheManager(db_path=DB_PATH))
        total_enriched, total_count = 0, 0
        for src in ("wiki", "jira", "gdi"):
            enrich_result[src] = enrich_engine.enrich_batch(src)
            total_enriched += enrich_result[src].get("enriched", 0)
            total_count += enrich_result[src].get("total", 0)
        log.info("[Enrich] 완료: %s",
                 {k: f"{v['enriched']}/{v['total']}" for k, v in enrich_result.items()})
        # sync_log에 enrichment 이력 기록
        enrich_duration = round(time.time() - enrich_t0, 1)
        try:
            import sqlite3 as _sq
            with _sq.connect(DB_PATH, timeout=30) as _conn:
                _conn.execute(
                    "INSERT INTO sync_log "
                    "(source_type, scope, sync_type, started_at, finished_at, "
                    "status, pages_scanned, pages_updated, duration_sec) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("enrichment", "all", "batch", enrich_started,
                     datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                     "success", total_count, total_enriched, enrich_duration),
                )
        except Exception as db_err:
            log.warning("[Enrich] sync_log 기록 실패: %s", db_err)
    except Exception as e:
        log.warning("[Enrich] 실패 (동기화 결과는 정상): %s", e)
        enrich_result = {"error": str(e)}
        # 실패도 sync_log에 기록
        try:
            import sqlite3 as _sq
            with _sq.connect(DB_PATH, timeout=30) as _conn:
                _conn.execute(
                    "INSERT INTO sync_log "
                    "(source_type, scope, sync_type, started_at, finished_at, "
                    "status, error_message, duration_sec) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("enrichment", "all", "batch", enrich_started,
                     datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                     "failed", str(e), round(time.time() - enrich_t0, 1)),
                )
        except Exception:
            pass

    duration = round(time.time() - t0, 1)
    log.info("자동적재 완료 (%.1f초)", duration)
    log.info("=" * 60)

    return {"wiki": wiki_result, "jira": jira_result, "gdi": gdi_result,
            "enrichment": enrich_result, "duration_sec": duration}


if __name__ == "__main__":
    force_full = "--full-wiki" in sys.argv
    result = run(force_full_wiki=force_full)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
