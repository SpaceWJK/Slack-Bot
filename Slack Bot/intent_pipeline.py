"""
intent_pipeline.py — task-129.5 wiring helper

slack_bot.py에서 분리하여 testability 확보 (slack_bolt 의존성 우회).
4단계 파이프라인 흐름:
  1. extract_intent (intent_extractor)
  2. build_*_query (query_builder, relaxation_engine 내부)
  3. search_with_ladder (relaxation_engine)
  4. answer_formatter / ask_claude_fn

설계: step2_design_v2.md (Step 3 검수 시정 적용)
- C-2: detect_write_intent는 호출자(slack_bot.py 핸들러)에서 선행 보장
- M-1: request_type별 분기 (metadata/list/summary/content_search)
- M-4: 0건 시 fallthrough 금지 ("찾을 수 없음" 응답 + return True)
- MAJOR-NEW-6: full_text 전달 의무
- MAJOR-NEW-4: dataclasses.replace (relaxation_engine 내부 보장)
"""

import logging
import re
import sqlite3
import time
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)

# ── 카테고리 감지 패턴 ────────────────────────────────────────────────────────
_CATEGORY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("hotfix", re.compile(r"핫픽스|hotfix|hot\s*fix|긴급패치", re.IGNORECASE)),
    ("release", re.compile(r"릴리즈|release|배포일정", re.IGNORECASE)),
    ("qa",      re.compile(r"체크리스트|점검항목", re.IGNORECASE)),
]


def _detect_wiki_category(text: str) -> "str | None":
    """질문 텍스트에서 wiki 그리드 카테고리를 감지합니다.

    Returns: 'hotfix' | 'release' | 'qa' | None
    """
    for category, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            return category
    return None


def grid_first_search_wiki(question: str, cache_mgr) -> "list | None":
    """wiki 그리드 1순위 검색 (task-193 Phase 2 §A).

    카테고리(hotfix/release/qa) 감지 후 nodes WHERE source_type='wiki' AND category=?
    [AND (game_tag=? OR game_tag IS NULL)] ORDER BY last_modified DESC LIMIT 5.

    Returns:
        None       — 카테고리 미감지 (기존 경로 fallback)
        []         — 카테고리 감지, 결과 0건 (기존 경로 fallback)
        [SearchHit, ...] — hits 있으면 이걸 컨텍스트로 사용
    """
    category = _detect_wiki_category(question)
    if category is None:
        return None  # 감지 안 됨 → 기존 경로

    # 게임 감지 (game_aliases는 순환 임포트 없이 lazy import)
    game_tag: "str | None" = None
    try:
        import game_aliases
        game_info = game_aliases.detect_game_in_text(question)
        if game_info:
            game_tag = game_info.get("canonical")
    except ImportError:
        pass

    db_path = cache_mgr.get_db_path()
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            if game_tag:
                sql = (
                    "SELECT n.id, n.title, n.url, dc.body_text, dc.summary, dm.last_modified "
                    "FROM nodes n "
                    "LEFT JOIN doc_content dc ON dc.node_id = n.id "
                    "LEFT JOIN doc_meta dm ON dm.node_id = n.id "
                    "WHERE n.source_type = 'wiki' AND n.category = ? "
                    "  AND (n.game_tag = ? OR n.game_tag IS NULL) "
                    "ORDER BY dm.last_modified DESC LIMIT 5"
                )
                rows = conn.execute(sql, (category, game_tag)).fetchall()
            else:
                sql = (
                    "SELECT n.id, n.title, n.url, dc.body_text, dc.summary, dm.last_modified "
                    "FROM nodes n "
                    "LEFT JOIN doc_content dc ON dc.node_id = n.id "
                    "LEFT JOIN doc_meta dm ON dm.node_id = n.id "
                    "WHERE n.source_type = 'wiki' AND n.category = ? "
                    "ORDER BY dm.last_modified DESC LIMIT 5"
                )
                rows = conn.execute(sql, (category,)).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.warning(f"[wiki/grid] DB 오류, fallback: {e}")
        return None

    # sqlite3.Row → SearchHit 호환 객체 변환
    # relaxation_engine 의존 없이 동일 구조의 간단한 dataclass 사용
    from dataclasses import dataclass, field as dc_field

    @dataclass
    class _GridHit:
        node_id: int
        chunk_id: "int | None"
        title: str
        snippet: str
        score: float
        metadata: dict = dc_field(default_factory=dict)

    hits = []
    for row in rows:
        r = dict(row)
        hits.append(_GridHit(
            node_id=r.get("id", 0),
            chunk_id=None,
            title=r.get("title", ""),
            snippet=(r.get("summary") or r.get("body_text") or "")[:500],
            score=0.0,
            metadata={"last_modified": r.get("last_modified") or ""},
        ))

    logger.info(
        f"[wiki/grid] category={category!r}, game_tag={game_tag!r}, hits={len(hits)}"
    )
    return hits


# ── gdi 그리드 1순위 검색 (task-193 Phase 2 §B) ──────────────────────────────

_GDI_KIND_PATTERNS = [
    ("issue_unit_planning", re.compile(r"기획서|사양서|스펙", re.IGNORECASE)),
    ("bat_result",          re.compile(r"\bBAT\b|뱃 ?결과", re.IGNORECASE)),
    ("patch_note",          re.compile(r"패치 ?노트|patch ?note", re.IGNORECASE)),
    ("qa_check_list",       re.compile(r"체크 ?리스트|점검 ?항목|checklist", re.IGNORECASE)),
]

_GDI_DATE_RE = re.compile(r"\b(20\d{6}|\d{4})\b")  # 20260527 또는 0527


def grid_first_search_gdi(question: str, cache_mgr) -> "list | None":
    """gdi 그리드 1순위 검색 — search_by_build_meta 재사용 (task-193 Phase 2 §B).

    게임 + file_kind 의도 감지 시 빌드 메타 축으로 캐시 직접 조회.
    Returns: None(미감지→기존 경로) | [](0건→기존 경로) | [_GridHit,...]
    """
    # file_kind 의도 감지
    file_kind = None
    for kind, pattern in _GDI_KIND_PATTERNS:
        if pattern.search(question):
            file_kind = kind
            break
    if file_kind is None:
        return None  # 의도 미감지 → 기존 경로

    # 게임 감지 (search_by_build_meta는 game_tag 필수)
    game_tag = None
    try:
        import game_aliases
        game_info = game_aliases.detect_game_in_text(question)
        if game_info:
            game_tag = game_info.get("canonical")
    except ImportError:
        pass
    if not game_tag:
        return None  # 게임 미특정 → 기존 경로

    # 날짜 감지: 8자리 그대로, 4자리(MMDD)는 올해 연도 보정
    build_date = None
    m = _GDI_DATE_RE.search(question)
    if m:
        raw = m.group(1)
        if len(raw) == 8:
            build_date = raw
        elif len(raw) == 4 and raw[:2] in {"01","02","03","04","05","06","07","08","09","10","11","12"}:
            import datetime as _dt
            build_date = f"{_dt.date.today().year}{raw}"

    try:
        import gdi_client as gc
        rows = gc.search_by_build_meta(
            game_tag=game_tag, build_date=build_date,
            file_kind=file_kind, limit=10,
        )
    except Exception as e:
        logger.warning(f"[gdi/grid] search_by_build_meta 오류, fallback: {e}")
        return None

    from dataclasses import dataclass, field as dc_field

    @dataclass
    class _GridHit:
        node_id: int
        chunk_id: "int | None"
        title: str
        snippet: str
        score: float
        metadata: dict = dc_field(default_factory=dict)

    hits = []
    for r in rows or []:
        hits.append(_GridHit(
            node_id=r.get("id", 0),
            chunk_id=None,
            title=r.get("title", ""),
            snippet="",
            score=0.0,
            metadata={
                "path": r.get("path") or "",
                "ref_date": r.get("ref_date") or r.get("build_date") or "",
                "file_kind": r.get("file_kind") or "",
            },
        ))

    logger.info(
        f"[gdi/grid] game={game_tag!r}, kind={file_kind!r}, "
        f"date={build_date!r}, hits={len(hits)}"
    )
    return hits


# ── helper context 함수 ───────────────────────────────────────────────────────

def hits_to_wiki_context(hits: list, max_chars: int = 10000) -> str:
    """task-129.5: relaxation_engine SearchHit list → Claude prompt context.

    _wiki_ask_claude(page_text=...) 호환. max_chars 제한.

    task-132 PR2: 메타 정보(last_modified/ref_date/path/author) 헤더 노출.
    list/시간범위 쿼리에서 LLM이 메타로 답변 가능하도록 시정 (gdi 일관성).
    """
    if not hits:
        return ""
    parts = []
    total = 0
    for hit in hits[:10]:
        title = getattr(hit, "title", "제목 없음")
        snippet = getattr(hit, "snippet", "") or ""
        meta = getattr(hit, "metadata", {}) or {}
        last_modified = meta.get("last_modified") or ""
        ref_date = meta.get("ref_date") or ""
        path = meta.get("path") or ""
        author = meta.get("author") or ""

        header = f"## {title}"
        meta_parts = []
        if last_modified:
            meta_parts.append(f"last_modified={last_modified}")
        if ref_date:
            meta_parts.append(f"ref_date={ref_date}")
        if author:
            meta_parts.append(f"author={author}")
        if path:
            meta_parts.append(f"path={path}")
        if meta_parts:
            header += f"  ({', '.join(meta_parts)})"

        block = f"{header}\n{snippet[:1000]}\n"
        if total + len(block) > max_chars:
            break
        parts.append(block)
        total += len(block)
    return "\n".join(parts)


def hits_to_gdi_context(hits: list, max_chars: int = 10000) -> str:
    """task-129.5 gdi: SearchHit list → Claude prompt context.

    gdi metadata: file_path / game_alias_kr / file_kind / ref_date 표기.
    """
    if not hits:
        return ""
    parts = []
    total = 0
    for hit in hits[:10]:
        title = getattr(hit, "title", "제목 없음")
        snippet = getattr(hit, "snippet", "") or ""
        meta = getattr(hit, "metadata", {}) or {}
        path = meta.get("path") or meta.get("file_path") or ""
        game = meta.get("game_alias_kr") or ""
        ref_date = meta.get("ref_date") or ""

        header = f"## {title}"
        meta_parts = []
        if path:
            meta_parts.append(f"path={path}")
        if game:
            meta_parts.append(f"game={game}")
        if ref_date:
            meta_parts.append(f"ref_date={ref_date}")
        if meta_parts:
            header += f"  ({', '.join(meta_parts)})"

        block = f"{header}\n{snippet[:1000]}\n"
        if total + len(block) > max_chars:
            break
        parts.append(block)
        total += len(block)
    return "\n".join(parts)


# ── wiki / gdi 4단계 파이프라인 helper ───────────────────────────────────────

def run_wiki_intent_pipeline(
    text: str,
    page_part: str,
    question: str,
    respond: Callable,
    cache_mgr: Any,
    *,
    ie_mod=None,
    relax_mod=None,
    af_mod=None,
    ask_claude_fn: Optional[Callable] = None,
) -> bool:
    """task-129.5: wiki 4단계 파이프라인 실행.

    Args:
        text: full_text (partition 이전, MAJOR-NEW-6 의무)
        page_part: '\\' 분리 후 page 부분 (해당 시)
        question: '\\' 분리 후 question 부분
        respond: Slack respond 콜백
        cache_mgr: CacheManager 인스턴스 또는 None
        ie_mod / relax_mod / af_mod: 의존성 주입 (테스트 시 mock 주입)
        ask_claude_fn: content_search 분기 시 호출할 함수

    Returns:
        True  — 파이프라인 처리 완료 (응답 전송됨, 호출자는 return)
        False — 파이프라인 미진입/실패 (호출자는 기존 fallback 흐름 진행)

    R-5 (회귀 보장): ai_failed=True OR cache_mgr=None OR 예외 → False (fallthrough)
    M-4 (시정): 정상 0건 → True (fallthrough 금지, "찾을 수 없음" 응답)
    """
    # ── 의존성 lazy import (테스트 시 mock 주입 우선) ──
    if ie_mod is None:
        try:
            import intent_extractor as ie_mod
        except ImportError:
            return False
    if relax_mod is None:
        try:
            import relaxation_engine as relax_mod
        except ImportError:
            return False
    if af_mod is None:
        try:
            import answer_formatter as af_mod
        except ImportError:
            return False

    # ── R-5 fallback 조건: cache=None ──
    if cache_mgr is None:
        return False

    # ── [신규 task-193] 그리드 1순위 검색 ──
    # 카테고리 감지(hotfix/release/qa) + 게임 감지 → category/game_tag 필터 즉답.
    # None=미감지, []=0건 → 기존 4단계 파이프라인 fallthrough.
    grid_hits = grid_first_search_wiki(text, cache_mgr)
    if grid_hits:
        # 그리드 hits 있음 → ask_claude_fn 컨텍스트로 전달 후 즉답
        context = hits_to_wiki_context(grid_hits)
        if ask_claude_fn is not None:
            try:
                ask_claude_fn(
                    page_title=page_part or "(그리드 검색)",
                    page_text=context,
                    page_url="",
                    question=question,
                    respond=respond,
                    display_question=f"/wiki {text}",
                )
            except Exception as e:
                logger.warning(f"[wiki/grid] ask_claude_fn 예외, fallthrough: {e}")
                return False
        else:
            respond(text=context or "그리드 검색 결과를 찾았으나 내용을 표시할 수 없습니다.")
        logger.info(f"[wiki/grid] 그리드 즉답 hits={len(grid_hits)}")
        return True
    # grid_hits is None(미감지) 또는 [](0건) → 기존 경로로 fallthrough

    # ── extract_intent (MAJOR-NEW-6: full_text) ──
    intent = None
    try:
        intent = ie_mod.extract_intent(text, domain="wiki")
    except Exception as e:
        logger.warning(f"[wiki/intent] extract_intent 예외: {e}")
        intent = None

    # task-132 PR1-B: ai_failed → grep fallthrough 제거
    # 사용자 5건 운영 실패 RCA: timeout/markdown 파싱 실패 시 grep fallback이 task-129
    # 4단계 파이프라인을 우회 → 운영 0% 동작. 명시 안내 + return True (fallthrough 차단).
    if intent is None or intent.ai_failed:
        respond(text=(
            "⚠️ 질문 의도 분석에 실패했습니다.\n"
            "다음 형식으로 다시 시도해주세요:\n"
            "  • metadata: `/wiki <페이지> 마지막 수정일?`\n"
            "  • list: `/wiki <기간> 업데이트된 페이지`\n"
            "  • content: `/wiki <키워드> 관련 자료`\n"
            "_(Stage 1 Intent 추출 실패 — Claude API timeout 또는 응답 파싱 오류)_"
        ))
        logger.warning(f"[wiki/intent] ai_failed 명시 안내 (text_len={len(text)})")
        return True

    # ── search_with_ladder ──
    try:
        t0 = time.time()
        result = relax_mod.search_with_ladder(cache_mgr, intent, "wiki")
        elapsed_ms = int((time.time() - t0) * 1000)
    except Exception as e:
        logger.warning(f"[wiki/intent] search_with_ladder 예외, fallthrough: {e}")
        return False

    # ── 0건 처리 (M-4 시정: fallthrough 금지) ──
    if result.total_count == 0:
        msg = "요청하신 정보를 찾을 수 없습니다."
        ambiguity = getattr(intent, "ambiguity_notes", "") or ""
        if ambiguity:
            msg += f"\n_(intent: {ambiguity})_"
        respond(text=msg)
        logger.info(
            f"[wiki/intent] 0 hits — req_type={intent.request_type}, "
            f"ambiguity={ambiguity!r}, elapsed_ms={elapsed_ms}"
        )
        return True

    # ── Stage 4: 모든 분기 통일 — ask_claude 자연어 합성 (PR1-K)
    # 사용자 명시 결함: list/metadata가 단순 list만 노출 → 여러 번 입력 시 답변 퀄리티 저하 체감.
    # 시정: 모든 분기에서 ask_claude_fn 우선 호출 (자연어 합성 + 통일 포맷).
    # ask_claude_fn 없거나 실패 시 fallback으로 format_*_answer 사용.
    try:
        if ask_claude_fn is not None:
            context = hits_to_wiki_context(result.hits)
            ask_claude_fn(
                page_title=page_part or "(자연어 검색)",
                page_text=context,
                page_url="",
                question=question,
                respond=respond,
                display_question=f"/wiki {text}",
            )
        elif intent.request_type == "metadata":
            respond(text=af_mod.format_metadata_answer(result.hits, intent, domain="wiki", raw_text=text))
        elif intent.request_type == "list":
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="wiki", raw_text=text))
        elif intent.request_type == "summary":
            respond(text=af_mod.format_summary_answer(result.hits, intent, domain="wiki", raw_text=text))
        else:
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="wiki", raw_text=text))
    except Exception as e:
        logger.warning(f"[wiki/intent] answer 분기 예외, fallthrough: {e}")
        return False

    logger.info(
        f"[wiki/intent] PASS — req_type={intent.request_type}, "
        f"hits={result.total_count}, relax_level={result.relaxation_level}, "
        f"elapsed_ms={elapsed_ms}"
    )
    return True


def run_gdi_intent_pipeline(
    text: str,
    folder: str,
    question: str,
    respond: Callable,
    cache_mgr: Any,
    *,
    ie_mod=None,
    relax_mod=None,
    af_mod=None,
    ask_claude_fn: Optional[Callable] = None,
) -> bool:
    """task-129.5: gdi 4단계 파이프라인 실행.

    호출 조건: 2 parts (folder \\ question) + breadcrumb 없음 (M-2 시정).
    Returns: True/False (wiki와 동일 의미).
    """
    if ie_mod is None:
        try:
            import intent_extractor as ie_mod
        except ImportError:
            return False
    if relax_mod is None:
        try:
            import relaxation_engine as relax_mod
        except ImportError:
            return False
    if af_mod is None:
        try:
            import answer_formatter as af_mod
        except ImportError:
            return False

    if cache_mgr is None:
        return False

    # ── 그리드 1순위 검색 (task-193 Phase 2 §B) — 0건/미감지 시 기존 경로 불변 ──
    grid_hits = grid_first_search_gdi(text, cache_mgr)
    if grid_hits:
        context = hits_to_gdi_context(grid_hits)
        if ask_claude_fn is not None:
            try:
                ask_claude_fn(
                    context_text=context,
                    source_label=folder or "(그리드 검색)",
                    question=question or text,
                    respond=respond,
                    display_question=f"/gdi {text}",
                )
            except Exception as e:
                logger.warning(f"[gdi/grid] ask_claude_fn 예외, fallthrough: {e}")
                return False
        else:
            respond(text=context or "그리드 검색 결과를 표시할 수 없습니다.")
        logger.info(f"[gdi/grid] 그리드 즉답 hits={len(grid_hits)}")
        return True
    # grid_hits None(미감지)/[](0건) → 기존 경로

    intent = None
    try:
        intent = ie_mod.extract_intent(text, domain="gdi")
    except Exception as e:
        logger.warning(f"[gdi/intent] extract_intent 예외: {e}")
        intent = None

    # task-132 PR1-B: ai_failed → grep fallthrough 제거 (gdi domain)
    if intent is None or intent.ai_failed:
        respond(text=(
            "⚠️ 질문 의도 분석에 실패했습니다.\n"
            "다음 형식으로 다시 시도해주세요:\n"
            "  • content: `/gdi <게임> \\ <키워드>`\n"
            "  • list: `/gdi <게임> \\ 패치노트 \\ <기간>`\n"
            "  • metadata: `/gdi <파일명> 마지막 수정일?`\n"
            "_(Stage 1 Intent 추출 실패 — Claude API timeout 또는 응답 파싱 오류)_"
        ))
        logger.warning(f"[gdi/intent] ai_failed 명시 안내 (text_len={len(text)})")
        return True

    try:
        t0 = time.time()
        result = relax_mod.search_with_ladder(cache_mgr, intent, "gdi")
        elapsed_ms = int((time.time() - t0) * 1000)
    except Exception as e:
        logger.warning(f"[gdi/intent] search_with_ladder 예외, fallthrough: {e}")
        return False

    if result.total_count == 0:
        msg = "요청하신 정보를 찾을 수 없습니다."
        ambiguity = getattr(intent, "ambiguity_notes", "") or ""
        if ambiguity:
            msg += f"\n_(intent: {ambiguity})_"
        respond(text=msg)
        logger.info(
            f"[gdi/intent] 0 hits — req_type={intent.request_type}, "
            f"ambiguity={ambiguity!r}, elapsed_ms={elapsed_ms}"
        )
        return True

    # ── Stage 4: 모든 분기 통일 — ask_claude 자연어 합성 (PR1-K)
    # 사용자 명시 결함: list/metadata가 단순 list만 노출 → 여러 번 입력 시 답변 퀄리티 저하 체감.
    # 시정: 모든 분기에서 ask_claude_fn 우선 호출 (자연어 합성 + 통일 포맷).
    try:
        if ask_claude_fn is not None:
            context = hits_to_gdi_context(result.hits)
            ask_claude_fn(
                context_text=context,
                source_label=folder or "(자연어 검색)",
                question=question or text,
                respond=respond,
                display_question=f"/gdi {text}",
            )
        elif intent.request_type == "metadata":
            respond(text=af_mod.format_metadata_answer(result.hits, intent, domain="gdi", raw_text=text))
        elif intent.request_type == "list":
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="gdi", raw_text=text))
        elif intent.request_type == "summary":
            respond(text=af_mod.format_summary_answer(result.hits, intent, domain="gdi", raw_text=text))
        else:
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="gdi", raw_text=text))
    except Exception as e:
        logger.warning(f"[gdi/intent] answer 분기 예외, fallthrough: {e}")
        return False

    logger.info(
        f"[gdi/intent] PASS — req_type={intent.request_type}, "
        f"hits={result.total_count}, relax={result.relaxation_level}, "
        f"elapsed_ms={elapsed_ms}"
    )
    return True
