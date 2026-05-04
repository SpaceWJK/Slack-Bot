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
import re as _re
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

    # task-132 PR1-D: unknown field tolerant (reject → warn + drop)
    # Claude haiku가 응답에 schema_version/domain/fields 같은 메타 필드를 종종 추가.
    # 이전 reject 동작 = 사용자 5건 운영 실패 (모두 ai_failed) 직접 원인.
    # strict 의도는 enum/타입 검증이지 추가 메타 필드 거부가 아님 → 무시 후 진행.
    # 주의: fields wrapper flat 처리는 회귀 유발 (Step 6에서 시도 후 revert)로 PR1 범위 외.
    # → fields wrapper 응답 시 C3/R3 등 일부 케이스 부정확 (PR2 영역 escalate).
    unknown = set(raw_json.keys()) - allowed
    if unknown:
        logger.info("[intent_extractor] unknown fields ignored: %s", unknown)
        raw_json = {k: v for k, v in raw_json.items() if k in allowed}

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


# ── 결정론적 후처리 (PR1-G) ───────────────────────────────────────────────────

# 게임 alias 매핑 — cache nodes.game_alias_kr 실제 값 기준 (한국어 그대로)
# CLAUDE.md "Cache는 S3 표준에 맞춤" 원칙: 매핑은 cache 값에 맞춰야 함.
# cache distinct: '에픽세븐'(13527), '카제나'(8864), '로드나인 아시아'(496)
_GAME_ALIASES_KR = {
    "에픽세븐": "에픽세븐", "에픽 세븐": "에픽세븐", "에세": "에픽세븐",
    "Epicseven": "에픽세븐", "epicseven": "에픽세븐",
    "카제나": "카제나", "카오스제로": "카제나", "카오스 제로": "카제나",
    "Chaoszero": "카제나", "chaoszero": "카제나",
    "로드나인": "로드나인 아시아", "로드 나인": "로드나인 아시아", "로드나인 아시아": "로드나인 아시아",
    "Lordnine_Asia": "로드나인 아시아", "Lordnine": "로드나인 아시아",
}

# file_kind 키워드 매핑 — cache 실제 값 (영어, 그대로 일치)
# cache distinct: patch_note(110), bat_result(100), qa_check_list(96), update_checklist(39),
# bug_verification(94), live_issue_community(19), common_ref_doc(14), issue_unit_planning(1474)
_FILE_KIND_KEYWORDS = {
    "patch_note": ["패치노트", "패치 노트", "patch_note", "patch note"],
    "issue_unit_planning": ["기획서", "기획 문서", "기획", "design", "spec", "사양서"],
    "qa_check_list": ["QA 체크리스트", "qa_check_list", "QA 보고서"],
    "bat_result": ["BAT", "bat_result", "BAT 결과"],
    "bug_verification": ["버그 검증", "bug_verification", "버그 확인"],
    "update_checklist": ["Update Checklist", "업데이트 체크리스트", "update_checklist"],
    "live_issue_community": ["라이브 이슈", "live_issue", "운영 이슈"],
}

# folder_role 키워드 매핑 — cache 실제 값 (영어 enum)
# cache distinct: game_data(19949), planning(2167), unknown(497), qa_result(249), live_issue(23)
# 주의: 패치노트는 cache 폴더 구조상 Update Review (planning) 하위에 위치 → planning 매핑.
# 도메인 지식 매핑 추가 (운영 사용자 의미 매핑):
#   - 신캐릭터/신규 영웅/신규 캐릭터 = Update Review 하위 영웅 기획서 → planning
#   - 사양서/스펙/기획안 = 기획서 → planning
_FOLDER_ROLE_KEYWORDS = {
    "planning": [
        "Update Review", "update review",
        "기획서", "기획 문서", "기획", "기획안",
        "패치노트", "patch note",
        "신캐릭터", "신규 캐릭터", "신규 영웅", "신캐", "신영웅",
        "캐릭터", "영웅",  # 영웅/캐릭터 단독도 기획서 영역 매핑
        "사양서", "스펙",
        "체험", "체험하기",  # cache path "영웅 체험하기" 패턴
    ],
    "qa_result": ["QA", "qa", "검수 결과", "Test Result", "test result", "테스트 결과", "QA 보고서"],
    "game_data": ["TSV", "tsv", "데이터", "game data"],
    "live_issue": ["라이브 이슈", "live issue", "운영 이슈"],
}

# request_type 결정론 키워드 (LLM 결과 보정용) — PR1-G v2 강화
_REQUEST_TYPE_OVERRIDES = [
    # (정규식, request_type, metadata_field)
    (r"(마지막|최근).{0,5}(업데이트|수정|변경).{0,3}(날짜|일자|일|언제)", "metadata", "last_modified"),
    (r"(언제|날짜|일자).{0,3}(수정|변경|업데이트)", "metadata", "last_modified"),
    # list 분기 패턴 강화 (LLM 비결정성 시정)
    (r"(파일명|파일 명|파일목록|파일 목록)", "list", None),
    (r"(목록|리스트)", "list", None),
    (r"\d+월\s*\d+\s*(?:일)?\s*(?:부터|~|에서|-).*?\d+\s*일", "list", None),  # "4월 27일부터 29일", "4월 27~29일", "4월 27 ~ 29일"
    (r"(이번 주|저번 주|이번 달|저번 달|최근 \d+)", "list", None),
    (r"(요약|정리)", "summary", None),
]

# 날짜 범위 추출 (list 경로용)
_DATE_RANGE_RE = _re.compile(r"(\d+)\s*월\s*(\d+)\s*일.*?(?:부터|~|에서)\s*(\d+)\s*일")

def _post_process_intent(intent, text: str, domain: str):
    """task-132 PR1-G: LLM 추출 결과를 결정론적 키워드 매칭으로 보정.

    Claude haiku-4-5가 모든 케이스를 content_search로 분류하는 한계 시정. 사용자 질문에
    명시된 게임명/file_kind/request_type 키워드를 정확히 추출하여 LLM 결과 보강.
    """
    if intent is None or getattr(intent, "ai_failed", False):
        return intent

    text_lower = text.lower()

    # 1. game_alias_kr 보정 (gdi 도메인) — Master 명시 정확 추출
    if domain == "gdi" and not getattr(intent, "game_alias_kr", []):
        detected_games = []
        for kr, canonical in _GAME_ALIASES_KR.items():
            if kr in text or kr.lower() in text_lower:
                if canonical not in detected_games:
                    detected_games.append(canonical)
        if detected_games:
            intent.game_alias_kr = detected_games

    # 1.5 path_segments 정규화 — Step 6 시도 후 revert (PR1 범위 외)
    # path_segments LIKE 매칭 정합화는 cache 라벨링 영역 의존 (영어/한국어 + 공백 차이) →
    # PR2 또는 cache 라벨링 보강 영역으로 escalate.

    # 2. file_kind 보정 (gdi 도메인)
    if domain == "gdi" and not getattr(intent, "file_kind", []):
        detected_kinds = []
        for kind, keywords in _FILE_KIND_KEYWORDS.items():
            if any(kw in text or kw.lower() in text_lower for kw in keywords):
                detected_kinds.append(kind)
        if detected_kinds:
            intent.file_kind = detected_kinds

    # 3. folder_role 보정 (gdi 도메인)
    if domain == "gdi" and not getattr(intent, "folder_role", []):
        detected_roles = []
        for role, keywords in _FOLDER_ROLE_KEYWORDS.items():
            if any(kw in text or kw.lower() in text_lower for kw in keywords):
                detected_roles.append(role)
        if detected_roles:
            intent.folder_role = detected_roles

    # 4. request_type 보정 (LLM이 content_search로 수렴한 경우만 재분류)
    if intent.request_type == "content_search":
        for pattern, new_rt, mf in _REQUEST_TYPE_OVERRIDES:
            if _re.search(pattern, text):
                intent.request_type = new_rt
                if mf and hasattr(intent, "metadata_field"):
                    intent.metadata_field = mf
                logger.info(
                    "[intent_extractor] PR1-G request_type 보정: content_search → %s "
                    "(pattern matched: %s)", new_rt, pattern[:30]
                )
                break

    # 4.4 wiki domain page_path_segments + title_keywords 자동 추출 (LLM fields wrapper drop 보정)
    # 운영 발견: LLM이 `{"fields":{"page_path_segments":["카제나","TEST INFO"]}}` 응답 시
    # PR1-D unknown fields drop으로 page_path_segments 소실 → SQL이 모든 페이지 후보 → 무관 답변.
    # 시정 (일반화): wiki domain에서 page_path_segments=[] AND title_keywords=[] 시 text의 "\\" 분할로 자동 추출.
    # 추가: wiki cache title은 영어 약어 사용 (카제나 → "CZ", 에픽세븐 → "EP7" 등) → game_alias 한국어는
    # page_path_segments에서 제거 후 ancestor_game으로 분리 (SQL strict 매칭 회피).
    if domain == "wiki" and not getattr(intent, "page_path_segments", None) \
            and not getattr(intent, "title_keywords", None):
        parts = text.split("\\")
        if len(parts) >= 2:
            cleaned_parts = [p.strip() for p in parts if p.strip()]
            if cleaned_parts:
                # 첫 segment가 게임 alias면 분리만 (cache wiki game_alias_kr=None 3126건 전체 →
                # ancestor_game SQL strict 매칭 0건 유발 → 자동 매핑 안 함, page_path_segments에서만 제거)
                game_aliases = set(_GAME_ALIASES_KR.keys())
                page_segs_filtered = []
                for p in cleaned_parts:
                    if p in game_aliases:
                        continue  # game_alias 자동 매핑 제거 (cache 라벨링 부재)
                    page_segs_filtered.append(p)
                # 모든 segment에서 filler 제거 (title 매칭 정합)
                filler_words = ["알려줘", "찾아줘", "보여줘", "뭐야", "뭐지", "있어", "있나",
                                "관련된", "관련", "최근", "이번", "저번", "지난",
                                "업데이트", "수정", "변경", "날짜", "일자",
                                "된", "한", "할", "되", "되었", "?", "!"]
                normalized_segs = []
                for seg in page_segs_filtered:
                    cleaned = seg
                    for kw in sorted(filler_words, key=len, reverse=True):
                        cleaned = cleaned.replace(kw, " ")
                    cleaned = " ".join(cleaned.split())
                    if cleaned:
                        normalized_segs.append(cleaned)
                if normalized_segs:
                    intent.page_path_segments = normalized_segs
                # title_keywords: 마지막 정규화 segment 토큰화
                last_part = normalized_segs[-1] if normalized_segs else ""
                title_tokens = [t.strip() for t in last_part.split() if len(t.strip()) >= 2]
                if title_tokens:
                    intent.title_keywords = title_tokens

    # 4.5 body_keywords 자동 추출 (LLM이 빈 채로 수렴 시 보정) — 일반화 패턴
    # 운영 발견: "카제나 \\ 은하계 재해 기획서" / "에픽세븐 \\ 신캐릭터 알려줘" 등
    # LLM body_keywords=[] → SQL이 file_kind/folder_role 매칭만으로 1000+건 후보 →
    # ref_date NULL 정렬 무작위 → 의도 무관 결과 (특정 케이스가 아닌 광범위 결함).
    # 시정: text의 "\\" 이후 부분에서 매핑된 키워드 + filler 동사 제거 후 남는 명사 토큰을 body_keywords로.
    if domain == "gdi" and not getattr(intent, "body_keywords", []):
        parts = text.split("\\")
        if len(parts) >= 2:
            last_part = parts[-1].strip()
            cleaned = last_part
            # 1. 매핑된 키워드 제거 (folder_role/file_kind/게임명)
            removable = []
            for kw_list in _FOLDER_ROLE_KEYWORDS.values():
                removable.extend(kw_list)
            for kw_list in _FILE_KIND_KEYWORDS.values():
                removable.extend(kw_list)
            removable.extend(_GAME_ALIASES_KR.keys())
            # 2. filler 동사/조사/시간표현 제거 (일반 한국어 명령형 문장 정규화)
            #    "알려줘/찾아줘/보여줘/있어/뭐야/뭔지/어떤지/관련 된/관련된/관련/관한"
            #    "최근/이번/저번/지난/이전" (시간 한정사 — ref_date 자동 추출되거나 무시)
            filler_words = [
                "알려줘", "찾아줘", "보여줘", "확인해줘", "정리해줘", "출력해줘",
                "뭐야", "뭐지", "뭔지", "있어", "있나", "있나요", "있어요",
                "어떤지", "어떻게", "어떤", "무엇", "어디",
                "관련 된", "관련된", "관련", "관한", "대한",
                "최근", "이번", "저번", "지난", "이전",
            ]
            removable.extend(filler_words)
            # 긴 키워드 먼저 제거 (substring 매칭 우선)
            for kw in sorted(removable, key=len, reverse=True):
                cleaned = cleaned.replace(kw, " ")
            # 3. 잔여 토큰: 2자 이상 + 한국어/영어/숫자만
            tokens = [t.strip() for t in cleaned.split() if len(t.strip()) >= 2]
            if tokens:
                intent.body_keywords = tokens
                logger.info("[intent_extractor] PR1-G body_keywords 자동 추출: %s", tokens)

    # 5. 날짜 범위 자동 추출 (list 경로) — wiki domain
    if domain == "wiki" and intent.request_type == "list":
        if not getattr(intent, "date_from", None) and not getattr(intent, "date_to", None):
            m = _DATE_RANGE_RE.search(text)
            if m:
                import datetime as _dt
                year = _dt.date.today().year
                month = int(m.group(1))
                day_from = int(m.group(2))
                day_to = int(m.group(3))
                try:
                    intent.date_from = f"{year}-{month:02d}-{day_from:02d}"
                    intent.date_to = f"{year}-{month:02d}-{day_to:02d}"
                    if hasattr(intent, "date_field") and not intent.date_field:
                        intent.date_field = "last_modified"
                    logger.info(
                        "[intent_extractor] PR1-G 날짜 범위 자동 추출: %s ~ %s",
                        intent.date_from, intent.date_to
                    )
                except (ValueError, TypeError):
                    pass

    # 5.4 list/metadata 경로는 body_keywords 무시 (일반화)
    # list = 파일 목록, metadata = 단일 페이지 메타 → file_kind/folder_role/ref_date 컬럼 필터로 충분.
    # body_keywords가 SQL chunks_fts MATCH로 추가되면 불필요 strict → 0건 회귀.
    # 운영 케이스 "패치노트 파일명이 뭐야?" body=['파일명이'] filler가 매칭 0건 유발.
    if intent.request_type in ("list", "metadata"):
        if hasattr(intent, "body_keywords"):
            intent.body_keywords = []

    # 5.5 body_keywords 중복 제거 — file_kind/folder_role 매핑된 키워드는 body에서 빼기
    # 운영 회귀: body=['패치노트'] + file_kind=['patch_note'] → SQL chunks_fts MATCH '패치노트'
    # AND file_kind='patch_note' = 이중 strict → 0건. 이미 file_kind 컬럼 필터로 충분하므로
    # body_keywords에서 동일 의미 키워드 제거.
    if domain == "gdi" and getattr(intent, "body_keywords", None):
        mapped_kws = set()
        for kind in (getattr(intent, "file_kind", []) or []):
            mapped_kws.update(_FILE_KIND_KEYWORDS.get(kind, []))
        for role in (getattr(intent, "folder_role", []) or []):
            mapped_kws.update(_FOLDER_ROLE_KEYWORDS.get(role, []))
        if mapped_kws:
            mapped_lower = {k.lower() for k in mapped_kws}
            intent.body_keywords = [
                b for b in intent.body_keywords
                if b.lower() not in mapped_lower
                and not any(b.lower() in m for m in mapped_lower)
                and not any(m in b.lower() for m in mapped_lower)
            ]

    # 6. ambiguity_notes 정리 — LLM "추정" 메시지가 사용자에게 노출되지 않도록
    # 운영 캡처: "_(intent: 카제나 게임 은하계 재해 관련 캐릭터/콘텐츠 기획서로 추정)_"
    # 사용자에게 의미 없는 LLM 추측 노출 → 빈 문자열로 초기화 (디버그 로그만 유지)
    if hasattr(intent, "ambiguity_notes") and intent.ambiguity_notes:
        if "추정" in intent.ambiguity_notes or "해석" in intent.ambiguity_notes:
            logger.debug(
                "[intent_extractor] PR1-G ambiguity_notes drop: %s",
                intent.ambiguity_notes
            )
            intent.ambiguity_notes = ""

    return intent


# ── 시스템 프롬프트 빌더 ──────────────────────────────────────────────────────

def _build_system_prompt(domain: str, today: str) -> str:
    """도메인별 system prompt 생성 (schema.json 인라인).

    task-132 PR1-F: file_kind/game_alias/request_type 추출 정확도 강화.
    이전 prompt는 "request_type은 enum 중 하나만 사용" 정도만 명시 → Claude haiku가
    모든 케이스를 content_search로 수렴 (운영 5건 모두 content_search). 도메인 별 분기
    예시를 추가하여 실제 분기 호출 정확도 향상.
    """
    schema = _load_schema(domain)
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2) if schema else "{}"

    if domain == "gdi":
        examples = (
            "예시 (반드시 학습):\n"
            "- '에픽세븐 \\\\ 신캐릭터 알려줘' → request_type='content_search', game_alias_kr=['에픽세븐'], path_segments=['에픽세븐','신캐릭터'], file_kind=[], body_keywords=['신캐릭터']\n"
            "- '카제나 \\\\ 최근 패치노트 파일명이 뭐야?' → request_type='list', game_alias_kr=['카제나'], file_kind=['patch_note'], ref_date_to=null (최근=ORDER BY ref_date DESC, 명시 절대일자 없으면 null)\n"
            "- '카제나 \\\\ 은하계 재해 기획서' → request_type='content_search', game_alias_kr=['카제나'], folder_role=['기획서'], body_keywords=['은하계','재해'], path_segments=['카제나','은하계 재해 기획서']\n"
            "- 'Update Review \\\\ 은하계 재해 기획서' → request_type='content_search', folder_role=['기획서'], body_keywords=['은하계','재해']\n"
            "- 'TSV', 'tsv' 명시 없으면 file_kind=[] (TSV 자동 제외 정책)\n\n"
            "file_kind enum: ['patch_note','issue_unit_planning','qa_check_list','bat_result','bug_verification','update_checklist','live_issue_community','common_ref_doc']\n"
            "folder_role enum: ['planning','qa_result','game_data','live_issue','dashboard','unknown']\n"
            "request_type 분기 규칙:\n"
            "- '파일명/목록/뭐가 있어' 등 → list\n"
            "- '마지막/최근 수정일/업데이트 날짜' 등 → metadata + metadata_field='last_modified'\n"
            "- '요약해줘/정리해줘' 등 → summary\n"
            "- 그 외 본문 검색 → content_search\n\n"
        )
    else:  # wiki
        examples = (
            "예시 (반드시 학습):\n"
            "- '카제나 \\\\ TEST INFO 최근 업데이트 된 날짜?' → request_type='metadata', metadata_field='last_modified', page_path_segments=['카제나','TEST INFO']\n"
            "- '4월 27일부터 29일까지 업데이트된 페이지' → request_type='list', date_field='last_modified', date_from='2026-04-27', date_to='2026-04-29'\n"
            "- '리타 관련 자료' → request_type='content_search', title_keywords=['리타']\n\n"
            "request_type 분기 규칙:\n"
            "- '마지막/최근/언제/날짜' 메타 정보 → metadata\n"
            "- '기간/날짜 범위/최근 N일' 페이지 목록 → list\n"
            "- '요약/정리' → summary\n"
            "- 그 외 → content_search\n\n"
        )

    return (
        f"당신은 {domain} 검색 Intent 분석기입니다.\n"
        "사용자의 한국어 질문을 분석하여 JSON 객체만 출력하세요.\n"
        "설명, 마크다운, 추가 텍스트는 절대 금지.\n\n"
        f"오늘 날짜: {today}\n\n"
        "출력 JSON 스키마:\n"
        f"{schema_str}\n\n"
        f"{examples}"
        "핵심 규칙:\n"
        "1. 모든 필드를 포함하세요 (모르면 null 또는 [])\n"
        "2. 스키마에 없는 필드는 절대 포함 금지 (단 schema_version/domain/fields 메타 추가는 무시됨)\n"
        "3. request_type은 enum 중 하나만 사용 — 위 분기 규칙 엄수\n"
        "4. 시간 표현은 오늘 기준으로 절대 날짜로 변환\n"
        "5. game_alias_kr / file_kind / folder_role / path_segments는 사용자 질문에 명시된 키워드 기반으로 정확히 추출 (추정 금지, 명시 안 됐으면 [])"
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
        client = _anthropic.Anthropic(api_key=api_key, timeout=5.0)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            temperature=0.0,  # task-132 PR1-I (advisor freeze): LLM 비결정성 차단
            system=system,
            messages=[{"role": "user", "content": text}],
        )
        raw = response.content[0].text.strip()
    except Exception as e:
        logger.warning("[intent_extractor] Claude 호출 실패 (timeout/error): %s", e)
        return _make_failed_intent(domain)

    # 세션 41 시정: Claude haiku가 ```json ... ``` 또는 ```js ... ``` markdown 코드 블록으로
    # 응답 wrap 시 json.loads 실패하던 문제 해결 (운영 로그 prefix='```js' 다수 확인).
    if raw.startswith("```"):
        lines = raw.split("\n")
        # 첫 줄(```json/```js/```) 제거
        if len(lines) > 1:
            lines = lines[1:]
        # 마지막 줄이 ``` 이면 제거
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()

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

    # task-132 PR1-G: 결정론적 후처리 (game_alias/file_kind/folder_role/request_type 보정)
    intent = _post_process_intent(intent, text, domain)

    # 캐시 저장 (task-129.7 LOW-2: max-size 초과 시 LRU eviction)
    # task-132 PR1-A: ai_failed 결과는 cache 금지 — retry storm 방지.
    # ai_failed=True가 60s cache hit으로 반환되면 같은 query 무한 grep fallback 누적.
    if not getattr(intent, "ai_failed", False):
        _evict_intent_cache_if_full()
        _INTENT_CACHE[cache_key] = (intent, time.time())
    logger.debug("[intent_extractor] intent 추출: domain=%s rt=%s ai_failed=%s",
                 domain, intent.request_type, getattr(intent, "ai_failed", False))

    # task-129.6 모니터링 인프라: 운영 audit용 Intent 추출 결과 dedicated 로그
    # 1주일 monthly audit 100건 sample 정확도 측정 baseline (intent_audit.log)
    _audit_log_intent(domain, text, intent)

    return intent


def _audit_log_intent(domain: str, text: str, intent) -> None:
    """task-129.6: Intent 추출 결과 audit 전용 로그 적재.

    형식 (JSONL): {ts, domain, text_len, text_prefix, request_type, metadata_field,
                   ambiguity_notes, ai_failed}
    PII 보호: text 직접 미저장 (length + prefix 10자)
    """
    try:
        import json as _json
        import datetime as _dt
        log_dir = Path(__file__).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / "intent_audit.jsonl"

        record = {
            "ts": _dt.datetime.now().isoformat(),
            "domain": domain,
            "text_len": len(text),
            "text_prefix": text[:10] if text else "",
            "request_type": getattr(intent, "request_type", None),
            "metadata_field": getattr(intent, "metadata_field", None),
            "ambiguity_notes": getattr(intent, "ambiguity_notes", "") or "",
            "ai_failed": getattr(intent, "ai_failed", False),
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(_json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        # audit 로그 실패는 운영 무영향
        logger.debug("[intent_audit] log 적재 실패 (무시): %s", e)


def _make_failed_intent(domain: str):
    """ai_failed=True intent 반환 (None 반환 금지)."""
    if domain == "gdi":
        return GdiIntent(ai_failed=True)
    return WikiIntent(ai_failed=True)
