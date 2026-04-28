"""캐시 시스템 전용 로거.

운영 로그(INFO)와 디버그 추적(DEBUG)을 `logs/cache_ops.log`에 기록.
기존 mcp_cache 로거와 독립적으로 동작하며, 캐시 성능 분석에 필요한
구조화된 데이터를 남긴다.

로그 포맷:
    2026-03-10T14:30:00 [INFO ] cache.hit   | title=QA체크리스트 | source=sqlite | 12ms
    2026-03-10T14:30:01 [DEBUG] cache.stale | node=42 | ttl_hours=24 | age_hours=25.3
    2026-03-10T14:30:02 [INFO ] sync.page   | title=업데이트 노트 | action=updated | 340ms

사용법:
    from src.cache_logger import ops_log, perf

    ops_log.cache_hit("QA 체크리스트", source="sqlite", elapsed_ms=12)
    ops_log.cache_miss("새 페이지", reason="not_found")

    with perf.timer("mcp_call"):
        result = mcp.call_tool(...)
    # → [DEBUG] perf.timer | label=mcp_call | 1523ms
"""

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from . import config

# ── 로그 파일 설정 ─────────────────────────────────────────────

LOG_FILE = config.LOGS_DIR / "cache_ops.log"
MAX_BYTES = 5 * 1024 * 1024   # 5MB
BACKUP_COUNT = 3               # 최대 3개 백업 (총 20MB)


def _setup_logger() -> logging.Logger:
    """cache_ops 전용 로거 초기화."""
    logger = logging.getLogger("cache_ops")

    # 이미 핸들러가 설정되어 있으면 중복 방지
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # 상위 로거로 전파 방지

    # 파일 핸들러 (DEBUG 이상 전부 기록)
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-5s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    logger.addHandler(fh)

    return logger


_logger = _setup_logger()


# ── 구조화 로그 헬퍼 ──────────────────────────────────────────

def _kv(**kwargs) -> str:
    """key=value 쌍을 파이프 구분 문자열로 변환.

    None 값은 제외. 밀리초 값은 정수로 표시.
    """
    parts = []
    for k, v in kwargs.items():
        if v is None:
            continue
        if k.endswith("_ms") and isinstance(v, (int, float)):
            parts.append(f"{int(v)}ms")
        else:
            parts.append(f"{k}={v}")
    return " | ".join(parts)


# ── OpsLog: 캐시 운영 로그 ───────────────────────────────────

class OpsLog:
    """캐시 운영 이벤트 기록."""

    # ── 캐시 조회 ──────────────────────────────────

    def cache_hit(self, title: str, *, source: str = "sqlite",
                  node_id: int | None = None, elapsed_ms: float | None = None):
        """캐시 적중 (L1 인메모리 또는 L2 SQLite)."""
        _logger.info("cache.hit   | %s",
                      _kv(title=title, source=source, node_id=node_id,
                          elapsed_ms=elapsed_ms))

    def cache_miss(self, title: str, *, reason: str = "not_found",
                   elapsed_ms: float | None = None):
        """캐시 미스 (MCP 호출 필요).

        reason: not_found | stale | no_content | disabled
        """
        _logger.info("cache.miss  | %s",
                      _kv(title=title, reason=reason, elapsed_ms=elapsed_ms))

    def cache_stale(self, node_id: int, title: str, *,
                    ttl_hours: int | None = None, age_hours: float | None = None,
                    remote_modified: str | None = None,
                    cached_modified: str | None = None):
        """캐시 만료 판정 상세 (디버그)."""
        _logger.debug("cache.stale | %s",
                       _kv(node_id=node_id, title=title,
                           ttl_hours=ttl_hours, age_hours=age_hours,
                           remote_mod=remote_modified, cached_mod=cached_modified))

    def cache_fresh(self, node_id: int, title: str, *,
                    ttl_hours: int | None = None, remaining_hours: float | None = None):
        """캐시 유효 판정 (디버그)."""
        _logger.debug("cache.fresh | %s",
                       _kv(node_id=node_id, title=title,
                           ttl_hours=ttl_hours, remaining_h=remaining_hours))

    def cache_store(self, title: str, *, node_id: int | None = None,
                    source_id: str | None = None, char_count: int | None = None,
                    has_body: bool = False, elapsed_ms: float | None = None):
        """캐시 저장 (MCP 결과 → DB)."""
        _logger.info("cache.store | %s",
                      _kv(title=title, node_id=node_id, source_id=source_id,
                          chars=char_count, has_body=has_body,
                          elapsed_ms=elapsed_ms))

    def cache_refresh(self, node_id: int, title: str):
        """TTL 리셋 (mark_fresh)."""
        _logger.debug("cache.refresh | %s", _kv(node_id=node_id, title=title))

    # ── 동기화 ─────────────────────────────────────

    def sync_start(self, sync_type: str, space_key: str, *,
                   since: str | None = None):
        """동기화 시작."""
        _logger.info("sync.start  | %s",
                      _kv(type=sync_type, space=space_key, since=since))

    def sync_page(self, title: str, *, action: str = "updated",
                  source_id: str | None = None, elapsed_ms: float | None = None):
        """개별 페이지 동기화 결과.

        action: added | updated | skipped | error
        """
        level = logging.WARNING if action == "error" else logging.DEBUG
        _logger.log(level, "sync.page   | %s",
                    _kv(title=title, action=action, source_id=source_id,
                        elapsed_ms=elapsed_ms))

    def sync_finish(self, sync_type: str, space_key: str, *,
                    scanned: int = 0, added: int = 0, updated: int = 0,
                    errors: int = 0, duration_sec: float = 0):
        """동기화 완료 요약."""
        _logger.info("sync.done   | %s",
                      _kv(type=sync_type, space=space_key,
                          scanned=scanned, added=added, updated=updated,
                          errors=errors, duration=f"{duration_sec:.1f}s"))

    def sync_error(self, msg: str, *, space_key: str | None = None,
                   source_id: str | None = None):
        """동기화 에러."""
        _logger.error("sync.error  | %s",
                       _kv(msg=msg, space=space_key, source_id=source_id))

    # ── 정리/관리 ──────────────────────────────────

    def cleanup(self, deleted: int, *, days: int = 30):
        """캐시 정리 결과."""
        if deleted > 0:
            _logger.info("cache.cleanup | %s",
                          _kv(deleted=deleted, older_than=f"{days}d"))

    def export(self, space_key: str, *, pages: int = 0,
               path: str | None = None):
        """JSON export 결과."""
        _logger.info("cache.export | %s",
                      _kv(space=space_key, pages=pages, path=path))

    def db_init(self, db_path: str, *, schema_version: int = 0):
        """DB 초기화 완료."""
        _logger.info("db.init     | %s",
                      _kv(path=db_path, schema_v=schema_version))

    def db_migrate(self, version: int):
        """마이그레이션 적용."""
        _logger.info("db.migrate  | %s", _kv(version=version))


# ── PerfLog: 성능 타이밍 로그 ─────────────────────────────────

class PerfLog:
    """성능 측정 유틸리티."""

    @staticmethod
    def now_ms() -> float:
        """현재 시각 (밀리초 정밀도)."""
        return time.perf_counter() * 1000

    @staticmethod
    def elapsed_ms(start_ms: float) -> float:
        """시작 시각 이후 경과 밀리초."""
        return time.perf_counter() * 1000 - start_ms

    @contextmanager
    def timer(self, label: str, *, level: int = logging.DEBUG):
        """컨텍스트 매니저로 블록 실행 시간 측정.

        Usage:
            with perf.timer("mcp_call"):
                result = mcp.call_tool(...)
        """
        t0 = time.perf_counter()
        yield
        elapsed = (time.perf_counter() - t0) * 1000
        _logger.log(level, "perf.timer  | %s",
                    _kv(label=label, elapsed_ms=elapsed))

    def compare(self, label: str, cache_ms: float, mcp_ms: float):
        """캐시 vs MCP 성능 비교 기록."""
        saved = mcp_ms - cache_ms
        ratio = (mcp_ms / cache_ms) if cache_ms > 0 else 0
        _logger.info("perf.compare | %s",
                      _kv(label=label,
                          cache_ms=cache_ms, mcp_ms=mcp_ms,
                          saved=f"{saved:.0f}ms", ratio=f"{ratio:.1f}x"))


# ── 모듈 수준 싱글톤 ─────────────────────────────────────────

ops_log = OpsLog()
perf = PerfLog()
