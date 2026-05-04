"""
intent_extractor.py — wiki/gdi Intent 추출 엔진 (task-129 S4-B)

jira_client.py 패턴 정합:
- _INTENT_CACHE: {key: (intent, timestamp)}  (60초 TTL)
- Claude haiku-4-5 + timeout 1.8s
- JSON parse + _validate_against_schema (strict + reject_unknown_fields)
- ai_failed=True 반환 (None 반환 금지)
"""

import json
import logging
import os
import time
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── 캐시 (jira_client.py:675 정합) ────────────────────────────────────────────
_INTENT_CACHE: "dict[str, tuple]" = {}   # {key: (intent, timestamp)}
_INTENT_CACHE_TTL = 60                   # seconds
_INTENT_CACHE_MAX = 500                  # task-129.7 LOW-2: 무한 누적 방지 (LRU eviction)


def _evict_intent_cache_if_full() -> None:
    """task-129.7 LOW-2: _INTENT_CACHE 크기가 max 초과 시 가장 오래된 entry 제거."""
    if len(_INTENT_CACHE) <= _INTENT_CACHE_MAX:
        return
    # timestamp 기준 가장 오래된 25% 제거 (배치 evict로 amortize)
    target_remove = len(_INTENT_CACHE) - int(_INTENT_CACHE_MAX * 0.75)
    sorted_keys = sorted(_INTENT_CACHE.items(), key=lambda kv: kv[1][1])
    for k, _ in sorted_keys[:target_remove]:
        _INTENT_CACHE.pop(k, None)

# ── schema 디렉터리 ──────────────────────────────────────────────────────────
_SCHEMA_DIR = Path(__file__).parent / "intent_schemas"

# ── 허용 필드 정의 (schema.json 기반) ─────────────────────────────────────────
_WIKI_ALLOWED_FIELDS = {
    "request_type", "metadata_field", "page_path_segments", "title_keywords",
    "ancestor_game", "date_field", "date_from", "date_to", "body_keywords",
    "space_key", "author", "labels", "limit", "ambiguity_notes", "ai_failed",
}

_GDI_ALLOWED_FIELDS = {
    "request_type", "metadata_field", "path_segments", "game_alias_kr",
    "folder_role", "file_kind", "ref_date_from", "ref_date_to",
    "file_path_hint", "body_keywords", "limit", "ambiguity_notes", "ai_failed",
}

_WIKI_REQUEST_TYPE_ENUM = {"metadata", "content_search", "list", "summary"}
_GDI_REQUEST_TYPE_ENUM  = {"metadata", "content_search", "list", "summary"}
_WIKI_METADATA_FIELD_ENUM = {"last_modified", "author", "space_key", "labels", None}
_GDI_METADATA_FIELD_ENUM  = {"last_modified", "ref_date", "file_kind", "folder_role", None}


# ── Intent dataclass ──────────────────────────────────────────────────────────

@dataclass
class WikiIntent:
    """wiki 도메인 Intent (설계 v4 §4.1)."""
    request_type: str = "content_search"
    metadata_field: Optional[str] = None
    page_path_segments: list = field(default_factory=list)
    title_keywords: list = field(default_factory=list)
    ancestor_game: Optional[str] = None
    date_field: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    body_keywords: list = field(default_factory=list)
    space_key: Optional[str] = None
    author: Optional[str] = None
    labels: list = field(default_factory=list)
    limit: int = 10
    ambiguity_notes: str = ""
    ai_failed: bool = False


@dataclass
class GdiIntent:
    """gdi 도메인 Intent (설계 v4 §4.1)."""
    request_type: str = "content_search"
    metadata_field: Optional[str] = None
    path_segments: list = field(default_factory=list)
    game_alias_kr: list = field(default_factory=list)
    folder_role: list = field(default_factory=list)
    file_kind: list = field(default_factory=list)
    ref_date_from: Optional[str] = None
    ref_date_to: Optional[str] = None
    file_path_hint: Optional[str] = None
    body_keywords: list = field(default_factory=list)
    limit: int = 10
    ambiguity_notes: str = ""
    ai_failed: bool = False


# ── schema 로드 ───────────────────────────────────────────────────────────────

def _load_schema(domain: str) -> Optional[dict]:
    """intent_schemas/{domain}.json 로드."""
    path = _SCHEMA_DIR / f"{domain}.json"
    if not path.exists():
        logger.warning("[intent_extractor] schema 없음: %s", path)
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("[intent_extractor] schema 로드 실패: %s", e)
        return None


# ── strict validator ──────────────────────────────────────────────────────────

def _validate_against_schema(raw_json: dict, domain: str):
    """JSON 검증 (strict=True, reject_unknown_fields=True).

    성공 시 WikiIntent / GdiIntent 반환.
    실패 시 None 반환 (호출자가 ai_failed=True intent 생성).
    """
    if domain == "wiki":
        allowed = _WIKI_ALLOWED_FIELDS
        rt_enum = _WIKI_REQUEST_TYPE_ENUM
        mf_enum = _WIKI_METADATA_FIELD_ENUM
    else:
        allowed = _GDI_ALLOWED_FIELDS
        rt_enum = _GDI_REQUEST_TYPE_ENUM
        mf_enum = _GDI_METADATA_FIELD_ENUM

    # unknown field reject
    unknown = set(raw_json.keys()) - allowed
    if unknown:
        logger.warning("[intent_extractor] unknown fields reject: %s", unknown)
        return None

    # request_type enum 검증
    rt = raw_json.get("request_type", "content_search")
    if rt not in rt_enum:
        logger.warning("[intent_extractor] invalid request_type: %r", rt)
        return None

    # metadata_field enum 검증 (None 허용)
    mf = raw_json.get("metadata_field", None)
    if mf not in mf_enum:
        logger.warning("[intent_extractor] invalid metadata_field: %r", mf)
        return None

    # limit 클램프 (DoS 방어 — sec-code MEDIUM-1)
    try:
        _limit = int(raw_json.get("limit", 10))
    except (ValueError, TypeError):
        _limit = 10
    _limit = max(1, min(_limit, 30))

    # dataclass 생성
    try:
        if domain == "wiki":
            return WikiIntent(
                request_type=raw_json.get("request_type", "content_search"),
                metadata_field=raw_json.get("metadata_field"),
                page_path_segments=raw_json.get("page_path_segments", []),
                title_keywords=raw_json.get("title_keywords", []),
                ancestor_game=raw_json.get("ancestor_game"),
                date_field=raw_json.get("date_field"),
                date_from=raw_json.get("date_from"),
                date_to=raw_json.get("date_to"),
                body_keywords=raw_json.get("body_keywords", []),
                space_key=raw_json.get("space_key"),
                author=raw_json.get("author"),
                labels=raw_json.get("labels", []),
                limit=_limit,
                ambiguity_notes=raw_json.get("ambiguity_notes", ""),
                ai_failed=False,
            )
        else:
            return GdiIntent(
                request_type=raw_json.get("request_type", "content_search"),
                metadata_field=raw_json.get("metadata_field"),
                path_segments=raw_json.get("path_segments", []),
                game_alias_kr=raw_json.get("game_alias_kr", []),
                folder_role=raw_json.get("folder_role", []),
                file_kind=raw_json.get("file_kind", []),
                ref_date_from=raw_json.get("ref_date_from"),
                ref_date_to=raw_json.get("ref_date_to"),
                file_path_hint=raw_json.get("file_path_hint"),
                body_keywords=raw_json.get("body_keywords", []),
                limit=_limit,
                ambiguity_notes=raw_json.get("ambiguity_notes", ""),
                ai_failed=False,
            )
    except (ValueError, TypeError) as e:
        logger.warning("[intent_extractor] dataclass 생성 실패: %s", e)
        return None


# ── 시스템 프롬프트 빌더 ──────────────────────────────────────────────────────

def _build_system_prompt(domain: str, today: str) -> str:
    """도메인별 system prompt 생성 (schema.json 인라인)."""
    schema = _load_schema(domain)
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2) if schema else "{}"
    return (
        f"당신은 {domain} 검색 Intent 분석기입니다.\n"
        "사용자의 한국어 질문을 분석하여 JSON 객체만 출력하세요.\n"
        "설명, 마크다운, 추가 텍스트는 절대 금지.\n\n"
        f"오늘 날짜: {today}\n\n"
        "출력 JSON 스키마:\n"
        f"{schema_str}\n\n"
        "핵심 규칙:\n"
        "1. 모든 필드를 포함하세요 (모르면 null 또는 [])\n"
        "2. 스키마에 없는 필드는 절대 포함 금지\n"
        "3. request_type은 enum 중 하나만 사용\n"
        "4. 시간 표현은 오늘 기준으로 절대 날짜로 변환"
    )


# ── extract_intent ────────────────────────────────────────────────────────────

def extract_intent(text: str, domain: str = "wiki"):
    """자연어 텍스트 → WikiIntent / GdiIntent (MAJOR-NEW-6: 전체 text 전달).

    Args:
        text: partition 이전 전체 텍스트 (MAJOR-NEW-6 의무)
        domain: "wiki" | "gdi"

    Returns:
        WikiIntent 또는 GdiIntent. 실패 시 ai_failed=True.
    """
    import datetime as _dt
    today = _dt.date.today().isoformat()

    # 캐시 체크 (jira_client.py:741 정합)
    cache_key = f"{domain}:{text}"
    if cache_key in _INTENT_CACHE:
        intent, ts = _INTENT_CACHE[cache_key]
        if time.time() - ts < _INTENT_CACHE_TTL:
            logger.debug("[intent_extractor] cache hit: %s", cache_key[:60])
            return intent

    # Claude API 호출 (jira_client.py:619 정합 — anthropic 직접 사용)
    try:
        import anthropic as _anthropic
    except ImportError as e:
        logger.warning("[intent_extractor] anthropic SDK 미설치: %s", e)
        return _make_failed_intent(domain)

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[intent_extractor] ANTHROPIC_API_KEY 미설정")
        return _make_failed_intent(domain)

    system = _build_system_prompt(domain, today)

    try:
        client = _anthropic.Anthropic(api_key=api_key, timeout=1.8)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": text}],
        )
        raw = response.content[0].text.strip()
    except Exception as e:
        logger.warning("[intent_extractor] Claude 호출 실패 (timeout/error): %s", e)
        return _make_failed_intent(domain)

    # JSON 파싱
    try:
        raw_json = json.loads(raw)
    except json.JSONDecodeError as e:
        # task-129.7 INFO-2: raw 내용 직접 노출 대신 길이만 + 첫 5자 prefix (PII 노출 최소화)
        prefix = raw[:5] if raw else ""
        logger.warning(
            "[intent_extractor] JSON 파싱 실패: %s | raw_len=%d prefix=%r",
            e, len(raw), prefix,
        )
        return _make_failed_intent(domain)

    # strict 검증
    intent = _validate_against_schema(raw_json, domain)
    if intent is None:
        logger.warning("[intent_extractor] schema 검증 실패: domain=%s", domain)
        return _make_failed_intent(domain)

    # 캐시 저장 (task-129.7 LOW-2: max-size 초과 시 LRU eviction)
    _evict_intent_cache_if_full()
    _INTENT_CACHE[cache_key] = (intent, time.time())
    logger.debug("[intent_extractor] intent 추출 성공: domain=%s rt=%s", domain, intent.request_type)
    return intent


def _make_failed_intent(domain: str):
    """ai_failed=True intent 반환 (None 반환 금지)."""
    if domain == "gdi":
        return GdiIntent(ai_failed=True)
    return WikiIntent(ai_failed=True)
