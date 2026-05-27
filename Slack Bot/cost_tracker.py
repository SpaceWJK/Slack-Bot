"""
cost_tracker.py — pm2 서비스 Anthropic API 비용 추적 래퍼

사용법:
    from cost_tracker import TrackedAnthropic
    client = TrackedAnthropic("voice-worker")   # anthropic.Anthropic() 대체
    # 이후 client.messages.create(...) 동일 사용

특징:
    - 비동기 배치 INSERT (10초마다) → 서비스 블로킹 없음
    - fail-open: 추적 실패가 서비스 중단 야기 금지
    - supabase 패키지 불필요 (urllib.request 직접 사용)
    - SSL verify=False (회사 CA 제약)
"""
import json
import logging
import os
import queue
import ssl
import sys
import threading
import time
import urllib.request
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logger = logging.getLogger(__name__)

# ── 모델별 단가 (USD / 1M tokens) ──────────────────────────────────────────
_PRICES: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5":   (0.80,  4.00),
    "claude-haiku-3":     (0.25,  1.25),
    "claude-sonnet-4-6":  (3.00, 15.00),
    "claude-opus-4-6":    (5.00, 25.00),
}

def _model_key(model: str) -> str:
    m = (model or "").lower()
    if "haiku-4-5" in m or "haiku-4-20" in m:
        return "claude-haiku-4-5"
    if "haiku" in m:
        return "claude-haiku-3"
    if "sonnet" in m:
        return "claude-sonnet-4-6"
    if "opus" in m:
        return "claude-opus-4-6"
    return model


def _calc_cost(model: str, tokens_in: int, tokens_out: int) -> float:
    pin, pout = _PRICES.get(_model_key(model), (3.0, 15.0))
    return tokens_in / 1e6 * pin + tokens_out / 1e6 * pout


# ── Supabase REST 클라이언트 ─────────────────────────────────────────────────
def _make_ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


_SSL_CTX = _make_ssl_ctx()


def _supabase_insert(rows: list[dict]) -> None:
    """cost_events 테이블에 배치 INSERT. 실패 시 로그만 남김 (fail-open)."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key or not rows:
        return

    endpoint = url.rstrip("/") + "/rest/v1/cost_events"
    payload = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )
    try:
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=15) as resp:
            if resp.status not in (200, 201):
                logger.warning("[cost_tracker] supabase %d", resp.status)
    except Exception as e:
        logger.warning("[cost_tracker] insert fail: %s", e)


# ── 백그라운드 flush 스레드 ────────────────────────────────────────────────
class _FlushThread(threading.Thread):
    def __init__(self, q: queue.Queue, interval: int = 10) -> None:
        super().__init__(daemon=True, name="cost-tracker-flush")
        self._q = q
        self._interval = interval

    def run(self) -> None:
        while True:
            time.sleep(self._interval)
            batch: list[dict] = []
            while len(batch) < 200:
                try:
                    batch.append(self._q.get_nowait())
                except queue.Empty:
                    break
            if batch:
                _supabase_insert(batch)


# 프로세스당 단일 큐 + 스레드
_GLOBAL_QUEUE: queue.Queue = queue.Queue(maxsize=5000)
_FLUSH_THREAD: _FlushThread | None = None
_INIT_LOCK = threading.Lock()


def _ensure_flush_thread() -> None:
    global _FLUSH_THREAD
    if _FLUSH_THREAD is not None:
        return
    with _INIT_LOCK:
        if _FLUSH_THREAD is None:
            _FLUSH_THREAD = _FlushThread(_GLOBAL_QUEUE)
            _FLUSH_THREAD.start()


# ── 메시지 프록시 ──────────────────────────────────────────────────────────
class _MessagesProxy:
    def __init__(self, inner_messages: Any, agent_name: str) -> None:
        self._inner = inner_messages
        self._agent = agent_name

    def create(self, **kwargs) -> Any:
        response = self._inner.create(**kwargs)
        try:
            model = kwargs.get("model", "")
            usage = getattr(response, "usage", None)
            if usage:
                tin = getattr(usage, "input_tokens", 0)
                tout = getattr(usage, "output_tokens", 0)
                cost = _calc_cost(model, tin, tout)
                _GLOBAL_QUEUE.put_nowait({
                    "agent_name": self._agent,
                    "model": _model_key(model),
                    "tokens_in": tin,
                    "tokens_out": tout,
                    "cost_usd": round(cost, 8),
                    "cost_type": "inference",
                    "session_id": f"pm2-{self._agent}",
                    "metadata": {},
                })
        except Exception as e:
            logger.debug("[cost_tracker] record skip: %s", e)
        return response


# ── 공개 API ───────────────────────────────────────────────────────────────
class TrackedAnthropic:
    """anthropic.Anthropic()의 drop-in 대체. 비용을 Supabase에 자동 기록."""

    def __init__(self, agent_name: str, api_key: str | None = None, **kwargs) -> None:
        try:
            from anthropic import Anthropic as _Anthropic
            init_kwargs = {k: v for k, v in {"api_key": api_key, **kwargs}.items() if v is not None}
            self._inner = _Anthropic(**init_kwargs)
        except ImportError as e:
            raise ImportError("anthropic 패키지 필요: pip install anthropic") from e

        self._agent_name = agent_name
        self.messages = _MessagesProxy(self._inner.messages, agent_name)
        _ensure_flush_thread()
        logger.info("[cost_tracker] TrackedAnthropic 초기화: agent=%s", agent_name)

    # anthropic.Anthropic의 다른 속성(beta 등) 투명 전달
    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
