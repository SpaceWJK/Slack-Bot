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
import time
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)


# ── helper context 함수 ───────────────────────────────────────────────────────

def hits_to_wiki_context(hits: list, max_chars: int = 10000) -> str:
    """task-129.5: relaxation_engine SearchHit list → Claude prompt context.

    _wiki_ask_claude(page_text=...) 호환. max_chars 제한.
    """
    if not hits:
        return ""
    parts = []
    total = 0
    for hit in hits[:10]:
        title = getattr(hit, "title", "제목 없음")
        snippet = getattr(hit, "snippet", "") or ""
        block = f"## {title}\n{snippet[:1000]}\n"
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

    # ── Stage 4: answer_formatter / ask_claude 분기 (M-1 시정) ──
    try:
        if intent.request_type == "metadata":
            respond(text=af_mod.format_metadata_answer(result.hits, intent, domain="wiki"))
        elif intent.request_type == "list":
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="wiki"))
        elif intent.request_type == "summary":
            respond(text=af_mod.format_summary_answer(result.hits, intent, domain="wiki"))
        else:  # content_search 또는 unknown
            if ask_claude_fn is None:
                logger.warning("[wiki/intent] content_search ask_claude_fn 미주입 → fallthrough")
                return False
            context = hits_to_wiki_context(result.hits)
            ask_claude_fn(
                page_title=page_part,
                page_text=context,
                page_url="",
                question=question,
                respond=respond,
                display_question=f"/wiki {text}",
            )
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

    try:
        if intent.request_type == "metadata":
            respond(text=af_mod.format_metadata_answer(result.hits, intent, domain="gdi"))
        elif intent.request_type == "list":
            respond(text=af_mod.format_list_answer(result.hits, intent, domain="gdi"))
        elif intent.request_type == "summary":
            respond(text=af_mod.format_summary_answer(result.hits, intent, domain="gdi"))
        else:  # content_search
            if ask_claude_fn is None:
                logger.warning("[gdi/intent] content_search ask_claude_fn 미주입 → fallthrough")
                return False
            context = hits_to_gdi_context(result.hits)
            ask_claude_fn(
                context_text=context,
                source_label=folder,
                question=question,
                respond=respond,
                display_question=f"/gdi {text}",
            )
    except Exception as e:
        logger.warning(f"[gdi/intent] answer 분기 예외, fallthrough: {e}")
        return False

    logger.info(
        f"[gdi/intent] PASS — req_type={intent.request_type}, "
        f"hits={result.total_count}, relax={result.relaxation_level}, "
        f"elapsed_ms={elapsed_ms}"
    )
    return True
