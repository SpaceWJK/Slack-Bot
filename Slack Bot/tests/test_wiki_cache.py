"""
test_wiki_cache.py — wiki_client L1 인메모리 캐시 단위 테스트

검증 항목:
  1. _mem_set / _mem_get 기본 동작 (저장 후 즉시 조회 성공)
  2. TTL 만료 후 _mem_get → None 반환
  3. _mem_get_with_ttl — 커스텀 TTL 적용
  4. 존재하지 않는 키 → None 반환
"""

import sys
import os
import time

# Slack Bot 루트 경로 추가 (import 해결)
_BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BOT_DIR not in sys.path:
    sys.path.insert(0, _BOT_DIR)


def _import_mem_functions():
    """wiki_client에서 _mem_get/_mem_set/_mem_get_with_ttl/_WIKI_MEM_CACHE 임포트.

    reload 금지 — 타 테스트의 sys.modules mock과 충돌(TypeError) 발생.
    이미 로드된 실모듈이 있으면 그대로 사용, mock 잔재면 제거 후 재임포트.
    """
    import types
    import unittest.mock as mock

    existing = sys.modules.get("wiki_client")
    if isinstance(existing, types.ModuleType) and hasattr(existing, "_mem_get"):
        return existing
    # mock 잔재 또는 미로드 → 깨끗하게 재임포트
    sys.modules.pop("wiki_client", None)
    mocks = {
        "mcp_core": mock.MagicMock(),
        "game_aliases": mock.MagicMock(),
        "search.cql_parallel": mock.MagicMock(),
        "keyword_rules": mock.MagicMock(),
    }
    with mock.patch.dict("sys.modules", mocks):
        import wiki_client as wc
        return wc


# ── 테스트 케이스 ────────────────────────────────────────────────────────────

def test_mem_set_and_get_returns_stored_value():
    """저장한 값을 TTL 이내에 조회하면 동일 데이터 반환."""
    wc = _import_mem_functions()
    wc._WIKI_MEM_CACHE.clear()

    wc._mem_set("key1", {"title": "테스트 페이지"})
    result = wc._mem_get("key1")
    assert result == {"title": "테스트 페이지"}, f"expected dict, got {result!r}"


def test_mem_get_missing_key_returns_none():
    """존재하지 않는 키 조회 시 None."""
    wc = _import_mem_functions()
    wc._WIKI_MEM_CACHE.clear()

    result = wc._mem_get("nonexistent_key")
    assert result is None, f"expected None, got {result!r}"


def test_mem_get_expired_returns_none():
    """TTL 만료 후 _mem_get → None."""
    wc = _import_mem_functions()
    wc._WIKI_MEM_CACHE.clear()

    # 만료된 항목을 직접 삽입 (timestamp = 현재 - TTL - 1초)
    wc._WIKI_MEM_CACHE["expired_key"] = ("old_data", time.time() - wc._WIKI_MEM_TTL - 1)
    result = wc._mem_get("expired_key")
    assert result is None, f"expected None for expired entry, got {result!r}"


def test_mem_get_with_ttl_custom_ttl():
    """_mem_get_with_ttl — 짧은 커스텀 TTL로 만료 동작 확인."""
    wc = _import_mem_functions()
    wc._WIKI_MEM_CACHE.clear()

    # 2초 전 항목 + 커스텀 TTL 1초 → 만료
    wc._WIKI_MEM_CACHE["short_ttl"] = ("data", time.time() - 2)
    result = wc._mem_get_with_ttl("short_ttl", ttl_sec=1)
    assert result is None, f"expected None for custom-TTL expired, got {result!r}"

    # 같은 항목을 기본 TTL(300s)로 조회 → 아직 유효
    result2 = wc._mem_get_with_ttl("short_ttl", ttl_sec=300)
    assert result2 == "data", f"expected 'data' with 300s TTL, got {result2!r}"


def test_mem_get_with_ttl_default_uses_wiki_mem_ttl():
    """ttl_sec=None 이면 _WIKI_MEM_TTL 기본값 사용."""
    wc = _import_mem_functions()
    wc._WIKI_MEM_CACHE.clear()

    wc._mem_set("default_ttl_key", "value123")
    result = wc._mem_get_with_ttl("default_ttl_key")  # ttl_sec=None → 기본값
    assert result == "value123", f"expected 'value123', got {result!r}"


if __name__ == "__main__":
    test_mem_set_and_get_returns_stored_value()
    test_mem_get_missing_key_returns_none()
    test_mem_get_expired_returns_none()
    test_mem_get_with_ttl_custom_ttl()
    test_mem_get_with_ttl_default_uses_wiki_mem_ttl()
    print("All tests PASS")
