"""
agentic_engine.py — tool_use agentic loop 엔진 (task-192)

Claude Sonnet이 도구를 agentic하게 반복 호출하여 통합 검색 수행.
19개 클라이언트 함수를 tool로 등록. /ask 파일럿 커맨드 전용.

설계: step2_design.md v4 (Step 3 검수 4채널 + GPT-4o adversarial 반영)
"""

import json
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

MAX_ITER = 10            # BISKIT AI는 8~13단계 호출 — 여유 확보
TOKEN_BUDGET = 80000     # Step 5 실측: 토픽 분석 시 50K 빠듯 → 상향

# ── 모듈 레벨 세마포어 (동시 3건 제한 — 스레드 폭증 방지) ──────────────
_ask_semaphore = threading.Semaphore(3)

# ── SYSTEM_PROMPT (BISKIT 전략 인코딩) ────────────────────────────────────
SYSTEM_PROMPT = """당신은 게임 데이터/문서/이슈를 통합 분석하는 AI 어시스턴트입니다.

[도구 사용 전략]
- 넓은 질문은 구조 파악 먼저: biskit_get_project_menu_tree → search_datasets 순서
- 첫 검색 빈약하면 다른 키워드로 재검색 (동의어·상위·하위 개념 변환)
- 파라미터 있는 도구: get_dataset_parameters 확인 후 execute_query
- 도메인 어휘 불명확: search_knowledge → get_knowledge로 정의 확인
- 결과 해석 시 출처(citation) 반드시 포함
- 표준지표 우선. 분석 답변 시 metricCategory 명시

[🔴 execute_query 0건 시 — 답변 금지, 무조건 재시도 (절대 규칙)]
- 결과가 전부 0/null이면 절대 최종 답변을 생성하지 마라. 반드시 execute_query를 다시 호출하라.
- "데이터 없음", "재조회 추천" 같은 답변을 쓰는 것은 금지 — 네가 직접 재시도해야 한다.
- 재시도 순서 (0건이면 다음 단계로):
  1. lang="KO"로 재호출 (한국 게임 커뮤니티는 KO에 데이터 있음)
  2. 여전히 0건이면 lang="EN" → lang="JA"
  3. 그래도 0건이면 perdCd 전환(D↔W) + communityCd/channelCd 개별 값
- lang="ALL"은 다국어 분리 저장 데이터셋에서 빈 값이 나오는 함정이다. KO를 먼저 시도하라.
- 최소 3회 다른 파라미터로 재시도한 뒤에만 "데이터 없음" 결론 허용.
- exampleValue는 참고일 뿐, 0건이면 무시하고 다른 값 시도.

[프로젝트 매핑]
- 게임 별칭은 biskit_list_projects의 aiDescription 참조
- "전체 게임" 요구 시 categories로 게임 프로젝트만 추림
- 🔴 Jira: 게임은 단일 프로젝트 키 없음. project=EP7/CZN 등 추측 절대 금지.
  게임 단위 검색은 text ~ "에픽세븐"/"카제나" 로 전체 프로젝트 횡단 검색.
- 🔴 게임별 용어 차이: 카제나="핫픽스", 에픽세븐="N차 업데이트". 0건이면 동의어 재검색.

[교차 소스 분석]
- BISKIT 지표 + Wiki 문서 + GDI 파일 + Jira 이슈를 조합하여 풍부한 답변 제공
- 복수 소스 사용 시 각 출처를 명시

[🔴 질문 의도 → 소스 선택 가이드 (어느 도구를 쓸지 판단)]
- 핫픽스/패치노트/릴리즈 내역/업데이트 공지/QA 체크리스트/일정 → wiki_* (Confluence Release/Hotfix INFO 페이지)
- 이슈 트래킹/티켓/버그 등록 현황/작업 요청 건수 → jira_*
- 기획서/스펙 문서/원본 파일/빌드 산출물 → gdi_*
- 게임 지표/통계/유저 동향/감성 비율/매출/리텐션/DAU → biskit_*
- "이슈가 몇개"라도 핫픽스/패치 맥락이면 wiki 우선 (jira 직행 금지).
- 단일 소스로 부족하면 교차 사용 가능. 단 질문 핵심에 맞는 소스를 1순위로.

[🔴 카탈로그 활용 — 답변 가능 범위 판단]
- 아래 BISKIT 데이터 카탈로그로 "이 질문이 답변 가능한 범위인지" 먼저 판단하라.
- 단, 카탈로그에 없어 보여도 "데이터/권한 없음" 단정 금지.
  → 반드시 search_datasets로 1회 실검색 확인 후 판단 (카탈로그는 참고용, 최신 아닐 수 있음).
- 명백히 권한 밖(매출/ARPU/결제금액 등)이고 실검색도 0건이면 → "해당 지표는 권한 범위 밖" 명확히 답.
- 사고 순서: 질문 의도 → 카탈로그로 대상 프로젝트/메뉴 식별 → 데이터셋 선정
  → 파라미터 결정 → 조회 → 재가공(합산/비율/추세) → 분석 → 통일 포맷 답변
"""


# ─────────────────────────────────────────────────────────────────────────────
# 카탈로그 로더 (Tier1 요약 — SYSTEM_PROMPT 주입용, ≤500토큰 목표)
# ─────────────────────────────────────────────────────────────────────────────

import json as _json

_CATALOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "..", "data", "biskit_catalog.json")
_CATALOG_SUMMARY = None  # 메모리 캐싱 (1회 로드)


def _load_catalog_summary() -> str:
    """biskit_catalog.json → Tier1 요약 문자열. 실패 시 '' (graceful degradation)."""
    global _CATALOG_SUMMARY
    if _CATALOG_SUMMARY is not None:
        return _CATALOG_SUMMARY
    try:
        with open(_CATALOG_PATH, encoding="utf-8") as f:
            cat = _json.load(f)
        # built_at 7일 초과 경고
        import datetime as _dt
        try:
            built = _dt.datetime.fromisoformat(cat.get("built_at", ""))
            if (_dt.datetime.now() - built).days > 7:
                logger.warning(f"[agentic] biskit_catalog stale ({(_dt.datetime.now()-built).days}일) — 폴백 적극 권장")
        except Exception:
            pass
        # Tier1: 프로젝트명+별칭 + 메뉴 카테고리 (description 제외 — 토큰 절약)
        lines = ["\n[BISKIT 데이터 카탈로그 (참고용 — 최신 아닐 수 있음, 없어 보여도 search_datasets 1회 확인)]"]
        # 보안: BISKIT API 응답값 → SYSTEM_PROMPT 간접 인젝션 방어 (개행/대괄호 strip)
        def _safe(s: str) -> str:
            return str(s).replace("\n", " ").replace("\r", " ").replace("[", "(").replace("]", ")")[:80]
        for p in cat.get("projects", []):
            aliases = "/".join(_safe(a) for a in p.get("aliases", [])[:3])
            cats = set()
            mt = p.get("menu_tree") or []
            for mc in (mt if isinstance(mt, list) else []):
                c = _safe(mc.get("category", ""))
                menus = ",".join(_safe(m.get("name", "")) for m in mc.get("menus", [])[:6])
                if c:
                    cats.add(f"{c}({menus})")
            lines.append(f"- {_safe(p.get('name'))}({aliases}) [{len(p.get('datasets',[]))}개]: {' / '.join(cats)}")
        lines.append("- 권한 밖(거부 대상): 매출/ARPU/결제금액/순수익")
        _CATALOG_SUMMARY = "\n".join(lines)
    except Exception as e:
        logger.warning(f"[agentic] 카탈로그 로드 실패 ({e}) — Tier1 없이 동작 (degradation)")
        _CATALOG_SUMMARY = ""
    return _CATALOG_SUMMARY


# ─────────────────────────────────────────────────────────────────────────────
# _validate_tool_args: Claude 생성 인자 보안 검증 (Step 7 보안 C-2/M-1/M-2)
# ─────────────────────────────────────────────────────────────────────────────

import re as _re

# 허용 게임 prefix (gdi folder_path / jira project traversal 방어)
_ALLOWED_GAME_PREFIX = ("Chaoszero", "Epicseven", "Lordnine", "카제나", "에픽세븐", "로드나인")
_SLUG_RE = _re.compile(r"^[a-zA-Z0-9가-힣/_\-]+$")


def _validate_tool_args(name: str, args: dict) -> str:
    """Claude 생성 도구 인자 검증. 위험 시 차단 메시지 반환, 정상이면 ''."""
    if not isinstance(args, dict):
        return ""
    # M-2: biskit_get_knowledge slug — path traversal 차단
    if name == "biskit_get_knowledge":
        slug = str(args.get("slug", ""))
        if ".." in slug or slug.startswith(("/", "\\")) or not _SLUG_RE.match(slug):
            return "[보안] 잘못된 slug 형식입니다."
    # M-1: gdi_list_files_in_folder folder_path — traversal 차단
    if name == "gdi_list_files_in_folder":
        fp = str(args.get("folder_path", ""))
        if ".." in fp or fp.startswith(("/", "\\")) or ":" in fp:
            return "[보안] 잘못된 폴더 경로입니다."
    # C-2: jira_search_issues JQL — 위험 패턴 차단 (공백 정규화 후 검사)
    if name == "jira_search_issues":
        raw = str(args.get("jql", "")).lower()
        jql = _re.sub(r"\s+", "", raw)  # 모든 공백 제거 → 공백 우회 차단
        if any(p in jql for p in ("or1=1", ";", "--", "delete", "update", "insert", "/*", "*/")):
            return "[보안] 허용되지 않은 쿼리 패턴입니다."
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# _normalize: 클라이언트 반환타입 → 문자열 정규화
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(result) -> str:
    """클라이언트 함수 반환타입(tuple/dict/list/str/None) → str 정규화.

    - tuple: (data, err) 또는 (data, err, cache_status) 처리
    - dict: data/rows/datasets/articles 배열 20행 제한 + 메타 보존
    - list: 20개 제한
    - str: 4000자 cap
    - None: "[결과 없음]"
    """
    # tuple 처리 (2-tuple 또는 3-tuple)
    if isinstance(result, tuple):
        data = result[0]
        err = result[1] if len(result) > 1 else None
        if err:
            return f"[오류] {err}"
        result = data

    if result is None:
        return "[결과 없음]"

    if isinstance(result, str):
        return result[:4000]

    if isinstance(result, dict):
        out = dict(result)
        for key in ("data", "rows", "datasets", "articles"):
            if isinstance(out.get(key), list) and len(out[key]) > 20:
                total = len(out[key])
                out[key] = out[key][:20]
                out[f"_{key}_truncated"] = f"{total}행 중 20행 표시"
        s = json.dumps(out, ensure_ascii=False)
        return s if len(s) <= 8000 else s[:8000] + "...[truncated]"

    if isinstance(result, list):
        return json.dumps(result[:20], ensure_ascii=False)[:8000]

    return str(result)[:4000]


# ─────────────────────────────────────────────────────────────────────────────
# ToolRegistry
# ─────────────────────────────────────────────────────────────────────────────

class ToolRegistry:
    """클라이언트 함수 → Anthropic tool schema 매핑."""

    def __init__(self):
        self._tools = {}  # name → {schema, handler}

    def register(self, name: str, description: str, input_schema: dict, handler):
        self._tools[name] = {
            "schema": {
                "name": name,
                "description": description,
                "input_schema": input_schema or {"type": "object", "properties": {}},
            },
            "handler": handler,
        }

    def get_schemas(self, whitelist) -> list:
        """Anthropic tools= 형식 반환.
        whitelist=None → 전체. list/set → 정확 이름 매칭.
        str(prefix) → 해당 prefix로 시작하는 도구만 (예: "wiki_")."""
        if whitelist is None:
            return [v["schema"] for v in self._tools.values()]
        if isinstance(whitelist, str):
            return [v["schema"] for k, v in self._tools.items() if k.startswith(whitelist)]
        return [v["schema"] for k, v in self._tools.items() if k in whitelist]

    def dispatch(self, name: str, args: dict) -> str:
        """handler 실행 → _normalize → str. 예외 시 에러 문자열 (loop 크래시 방지)."""
        if name not in self._tools:
            return f"[도구 {name} 오류] 등록되지 않은 도구"
        # M-1/M-2: Claude 생성 인자 검증 (path traversal / 무단 조회 차단)
        guard = _validate_tool_args(name, args)
        if guard:
            return guard
        try:
            handler = self._tools[name]["handler"]
            return _normalize(handler(args))
        except Exception as e:
            # M-3: 내부 예외 상세(서버 URL/hostname) 노출 금지 — 로그만
            logger.warning(f"[agentic] 도구 {name} 실행 오류: {e}")
            return f"[도구 {name} 실행 오류] 조회에 실패했습니다."


# ─────────────────────────────────────────────────────────────────────────────
# Registry 빌드 (19개 도구)
# ─────────────────────────────────────────────────────────────────────────────

def _build_registry() -> ToolRegistry:
    """19개 클라이언트 함수를 tool로 등록한 싱글톤 레지스트리 빌드."""
    import biskit_client as bc
    import wiki_client as wc
    import gdi_client as gc
    import jira_client as jc

    _wiki = wc.ConfluenceWikiClient()
    _gdi = gc.GdiClient()
    _jira = jc.JiraClient()

    reg = ToolRegistry()

    # ── BISKIT 7개 ────────────────────────────────────────────────────────
    reg.register(
        "biskit_list_projects",
        "BISKIT 프로젝트 목록+별칭 조회. 게임명→project_id 매핑 시작점. "
        "aliases/aiDescription으로 '카제나'→id=16 같은 별칭 매핑에 사용.",
        {"type": "object", "properties": {}},
        lambda a: bc.list_projects(),
    )
    reg.register(
        "biskit_get_project_menu_tree",
        "프로젝트 지표 메뉴 트리 조회. 넓은 질문은 search_datasets 전에 먼저 호출하여 "
        "해당 게임에 어떤 지표 메뉴가 있는지 파악하라.",
        {"type": "object", "properties": {"project_id": {"type": "integer"}}, "required": ["project_id"]},
        lambda a: bc.call_tool("get_project_menu_tree", {"project_id": int(a["project_id"])}),
    )
    reg.register(
        "biskit_search_datasets",
        "키워드로 데이터셋 검색. keywords에 동의어를 모아 전달. "
        "첫 검색 빈약하면 다른 관점(도메인 축·동의어·상위/하위 개념)으로 재호출. "
        "표준지표 우선. 여러 프로젝트는 프로젝트별로 호출.",
        {
            "type": "object",
            "properties": {
                "project_id": {"type": "integer"},
                "keywords": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["project_id", "keywords"],
        },
        lambda a: bc.search_datasets(a["project_id"], " ".join(a.get("keywords", []))),
    )
    reg.register(
        "biskit_get_dataset_parameters",
        "데이터셋 컬럼·파라미터·dataGuide 조회. execute_query 호출 전 반드시 먼저 확인. "
        "dataset_ids 목록을 한 번에 전달.",
        {
            "type": "object",
            "properties": {"dataset_ids": {"type": "array", "items": {"type": "integer"}}},
            "required": ["dataset_ids"],
        },
        lambda a: bc.call_tool("get_dataset_parameters", {"dataset_ids": [int(i) for i in a["dataset_ids"]]}),
    )
    reg.register(
        "biskit_execute_query",
        "데이터셋 쿼리 실행. parameters는 get_dataset_parameters 결과 format대로 전달. "
        "startDt/endDt는 데이터셋 format 그대로. 응답의 citation을 답변에 반드시 포함. "
        "metricCategory 명시.",
        {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "integer"},
                "parameters": {"type": "object"},
            },
            "required": ["dataset_id"],
        },
        lambda a: bc.execute_query(int(a["dataset_id"]), a.get("parameters", {})),
    )
    reg.register(
        "biskit_search_knowledge",
        "BISKIT 도메인 지식 저장소 검색. 도메인 어휘 불명확·컬럼 의미 불분명 시 사용. "
        "메타 질문(용어 정의·게임 시스템)은 이것만으로 답변 가능. "
        "검색 후 get_knowledge로 본문 조회.",
        {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "sort_by": {"type": "string"},
                "limit": {"type": "integer"},
            },
        },
        lambda a: bc.call_tool("search_knowledge", {k: v for k, v in a.items() if v is not None}),
    )
    reg.register(
        "biskit_get_knowledge",
        "BISKIT 지식 페이지 본문 전체 조회. search_knowledge 응답의 slug를 그대로 사용.",
        {
            "type": "object",
            "properties": {"slug": {"type": "string"}},
            "required": ["slug"],
        },
        lambda a: bc.call_tool("get_knowledge", {"slug": a["slug"]}),
    )

    # ── Wiki (Confluence) 4개 ─────────────────────────────────────────────
    reg.register(
        "wiki_search_with_context",
        "질문 맥락(게임명·연도·키워드)을 활용한 Confluence 페이지 검색. "
        "게임명+연도 → 게임 ancestor + 연도 CQL 우선. 실패 시 단순 제목 검색 폴백.",
        {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "question": {"type": "string"},
                "space_key": {"type": "string"},
            },
            "required": ["title"],
        },
        lambda a: _wiki.search_with_context(
            a["title"], question=a.get("question", ""), space_key=a.get("space_key")
        ),
    )
    reg.register(
        "wiki_get_page_by_title",
        "Confluence 페이지 제목으로 직접 조회. 정확 제목 알 때 사용. "
        "1단계 정확 일치 → 2단계 부분 일치 자동 시도.",
        {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "space_key": {"type": "string"},
            },
            "required": ["title"],
        },
        lambda a: _wiki.get_page_by_title(a["title"], space_key=a.get("space_key")),
    )
    reg.register(
        "wiki_search_content_live",
        "Confluence 본문 내용 전문 검색 (CQL text~). "
        "제목 검색 실패 시 최후 폴백. 키워드로 전체 공간 내 본문 검색.",
        {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "space_key": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["query"],
        },
        lambda a: _wiki.search_content_live(
            a["query"], space_key=a.get("space_key"), limit=a.get("limit", 3)
        ),
    )
    reg.register(
        "wiki_get_descendant_pages",
        "페이지 ID의 하위 페이지 목록 조회. 부모 페이지 ID 알 때 하위 내용 탐색.",
        {
            "type": "object",
            "properties": {
                "page_id": {"type": "string"},
                "space_key": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["page_id"],
        },
        lambda a: _wiki.get_descendant_pages(
            a["page_id"], space_key=a.get("space_key"), limit=a.get("limit", 5)
        ),
    )

    # ── GDI 3개 ──────────────────────────────────────────────────────────
    reg.register(
        "gdi_unified_search",
        "GDI 게임 문서 크로스 컬렉션 통합 검색 (기획서/QA결과/라이브이슈). "
        "game_name 지정 시 해당 게임 문서만 검색.",
        {
            "type": "object",
            "properties": {
                "query_text": {"type": "string"},
                "game_name": {"type": "string"},
                "top_k": {"type": "integer"},
            },
            "required": ["query_text"],
        },
        lambda a: _gdi.unified_search(
            a["query_text"], game_name=a.get("game_name"), top_k=a.get("top_k", 10)
        ),
    )
    reg.register(
        "gdi_search_by_filename",
        "GDI 파일명 기반 검색. 특정 파일명·확장자로 문서 탐색.",
        {
            "type": "object",
            "properties": {
                "filename_query": {"type": "string"},
                "game_name": {"type": "string"},
            },
            "required": ["filename_query"],
        },
        lambda a: _gdi.search_by_filename(
            a["filename_query"], game_name=a.get("game_name")
        ),
    )
    reg.register(
        "gdi_list_files_in_folder",
        "GDI 폴더 내 파일 목록 조회. 특정 폴더 경로의 파일 구조 파악.",
        {
            "type": "object",
            "properties": {"folder_path": {"type": "string"}},
            "required": ["folder_path"],
        },
        lambda a: _gdi.list_files_in_folder(a["folder_path"]),
    )

    # ── Jira 3개 ─────────────────────────────────────────────────────────
    reg.register(
        "jira_search_issues",
        "Jira JQL로 이슈 검색. "
        "🔴 게임 이슈는 단일 프로젝트 키 없음 — 여러 프로젝트에 '[게임명]' 태그로 분산됨. "
        "project=XXX 추측 금지. 게임 단위 검색은 반드시 text ~ \"게임명\" 사용. "
        "예: 'text ~ \"에픽세븐\" AND created >= \"2026-05-01\" AND created <= \"2026-05-31\"'. "
        "용어 주의: 카제나는 '핫픽스', 에픽세븐은 'N차 업데이트' 명명 — "
        "0건이면 핫픽스/업데이트/패치/점검/배포 동의어로 재검색. "
        "개수 질문은 limit 크게(50+) + total 확인.",
        {
            "type": "object",
            "properties": {
                "jql": {"type": "string"},
                "max_results": {"type": "integer"},
            },
            "required": ["jql"],
        },
        lambda a: _jira.search_issues(a["jql"], max_results=a.get("max_results", 10)),
    )
    reg.register(
        "jira_get_issue",
        "Jira 이슈 상세 조회. 이슈 키(예: EP7-1234) 알 때 사용.",
        {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
        lambda a: _jira.get_issue(a["key"]),
    )
    reg.register(
        "jira_get_all_projects",
        "Jira 전체 프로젝트 목록 조회. 프로젝트 키 모를 때 먼저 확인.",
        {"type": "object", "properties": {}},
        lambda a: _jira.get_all_projects(),
    )

    return reg


# 모듈 레벨 싱글톤 (1회 초기화)
_REGISTRY: ToolRegistry = None  # 실제 초기화는 첫 import 시 — 클라이언트 의존성 lazy 처리


def _get_registry() -> ToolRegistry:
    """싱글톤 레지스트리 반환 (최초 호출 시 빌드)."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _build_registry()
    return _REGISTRY


# ─────────────────────────────────────────────────────────────────────────────
# 텍스트 추출 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

def _extract_text(resp) -> str:
    """Anthropic 응답에서 텍스트 블록 추출."""
    for block in resp.content:
        if getattr(block, "type", None) == "text":
            return block.text
    return ""


def _extract_last_text(messages: list) -> str:
    """마지막 assistant 메시지에서 텍스트 블록 추출 (MAX_ITER 소진 폴백용)."""
    for msg in reversed(messages):
        if not isinstance(msg, dict) or msg.get("role") != "assistant":
            continue
        content = msg.get("content", [])
        if not isinstance(content, list):
            continue
        for block in content:
            if getattr(block, "type", None) == "text":
                return block.text
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", "")
    return "분석 한도에 도달했습니다. 질문을 더 구체적으로 다시 시도해주세요."


# ─────────────────────────────────────────────────────────────────────────────
# run_agentic — tool_use loop 엔진
# ─────────────────────────────────────────────────────────────────────────────

def run_agentic(
    question: str,
    whitelist,
    system_hint: str = "",
    respond=None,
    heartbeat=None,
) -> str:
    """tool_use agentic loop로 질문에 답변.

    Args:
        question:    사용자 질문
        whitelist:   사용할 도구 이름 목록 (None이면 전체)
        system_hint: 추가 시스템 프롬프트
        respond:     Slack respond 함수 (미사용, 서명 호환용)
        heartbeat:   진행 상태 콜백 (msg: str) — 10초 throttle 적용

    Returns:
        최종 답변 문자열
    """
    import anthropic

    # Anthropic 클라이언트 초기화
    auth_token = os.getenv("ANTHROPIC_AUTH_TOKEN", "").strip()
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    try:
        from cost_tracker import TrackedAnthropic
        client = TrackedAnthropic("slack-bot-agentic", api_key=api_key)
    except Exception:
        client = anthropic.Anthropic(api_key=api_key)

    registry = _get_registry()
    tools = registry.get_schemas(whitelist)
    # 현재 날짜 컨텍스트 주입 — 연도 없는 질문을 과거(2025)로 오해석 방지
    import datetime as _dt
    _today = _dt.datetime.now().strftime("%Y-%m-%d")
    _date_ctx = (
        f"\n\n[현재 날짜] 오늘은 {_today} 입니다. "
        f"사용자가 연도 없이 '5월', '지난달' 등으로 물으면 현재 연도({_today[:4]}) 기준으로 해석하세요. "
        f"명시적 과거 연도가 없으면 과거 연도(2025 등)로 가정하지 마세요."
    )
    system = SYSTEM_PROMPT + _load_catalog_summary() + _date_ctx + (system_hint or "")
    messages = [{"role": "user", "content": question}]

    # 스레드 안전 지역변수
    token_spent = 0
    _last_hb = 0.0

    def _hb(msg: str):
        """heartbeat 10초 throttle."""
        nonlocal _last_hb
        if heartbeat is None:
            return
        now = time.time()
        if now - _last_hb >= 10:
            try:
                heartbeat(msg)
            except Exception:
                pass
            _last_hb = now

    for i in range(MAX_ITER):
        if token_spent > TOKEN_BUDGET:
            logger.warning(f"[agentic] 토큰 예산 초과 ({token_spent}). 조기 종료.")
            break

        # LLM 호출 — 네트워크/timeout 예외 처리
        try:
            resp = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=2048,
                system=system,
                tools=tools,
                messages=messages,
            )
        except Exception as e:
            logger.error(f"[agentic] LLM 호출 실패 iter={i}: {e}")
            if i == 0:
                return f"AI 분석 호출 실패\n```\n{str(e)[:200]}\n```"
            # 중간 실패 → 지금까지 수집분으로 요약 시도
            break

        # 토큰 추적 (지역변수 — 스레드 안전)
        if hasattr(resp, "usage") and resp.usage:
            in_tok = getattr(resp.usage, "input_tokens", 0)
            out_tok = getattr(resp.usage, "output_tokens", 0)
            token_spent += in_tok + out_tok
            logger.info(
                f"[agentic] iter={i} in={in_tok} out={out_tok} total={token_spent}"
            )

        if i > 0:
            _hb(f"분석 중... ({i}단계)")

        if resp.stop_reason == "tool_use":
            # assistant content 원본 보존 (SDK 객체 그대로 append)
            messages.append({"role": "assistant", "content": resp.content})

            # 한 turn의 모든 tool_use 블록에 tool_result 수집 후 한 번에 append
            tool_results = []
            for block in resp.content:
                if getattr(block, "type", None) == "tool_use":
                    result_str = registry.dispatch(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    })

            if tool_results:
                messages.append({"role": "user", "content": tool_results})
        else:
            # end_turn 또는 max_tokens — 최종 답변
            return _extract_text(resp)

    # MAX_ITER 소진 또는 중간 LLM 실패 → tool 없이 1회 더 호출 (강제 텍스트)
    messages.append({
        "role": "user",
        "content": "분석 한도 도달. 지금까지 수집한 정보로 최선의 답변을 작성하라.",
    })
    try:
        final = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=2048,
            system=system,
            messages=messages,  # tools 없음 → 강제 텍스트
        )
        return _extract_text(final) or _extract_last_text(messages)
    except Exception as e:
        logger.error(f"[agentic] 최종 요약 호출 실패: {e}")
        return _extract_last_text(messages)


# ─────────────────────────────────────────────────────────────────────────────
# 모듈 초기화 — 싱글톤 lazy init (import 시 클라이언트 초기화 시도)
# ─────────────────────────────────────────────────────────────────────────────
try:
    _REGISTRY = _build_registry()
except Exception as _init_err:
    logger.warning(f"[agentic] 레지스트리 초기화 실패 (실행 시 재시도): {_init_err}")
    _REGISTRY = None
