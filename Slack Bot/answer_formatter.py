"""
answer_formatter.py — 검색 결과 답변 포맷터 (task-129 S4-E + task-132 PR2 포맷 통일)

설계 v4 §6.2 + task-132 사용자 명시 요구:
- 모든 답변은 통일 포맷: 📋 질문 / 💬 답변 / 🔗 근거 / 📌 출처
- format_metadata_answer / format_list_answer / format_summary_answer 모두 동일 구조
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _to_kst(dt_str: Optional[str]) -> str:
    """ISO datetime → KST 표시 (UTC+9). silent NULL: None/'None'/'null' → '정보 없음'."""
    if not dt_str or str(dt_str).strip().lower() in ("none", "null", ""):
        return "정보 없음"
    try:
        # 간단한 포맷 처리 (datetime 없어도 동작)
        if "T" in dt_str:
            date_part, time_part = dt_str.split("T", 1)
            time_part = time_part[:8]  # HH:MM:SS
            return f"{date_part} {time_part} (KST)"
        return dt_str
    except Exception:
        return dt_str or "정보 없음"


def _question_text(intent, raw_text: Optional[str] = None) -> str:
    """사용자 원본 질문 복원 — raw_text 우선, fallback intent 추출.

    task-132 PR1-J: 운영에서 "(요청 내용)" 표시 결함 시정. format_*_answer 호출자가
    raw_text 명시 전달 시 사용자 원본 그대로 노출. intent path/keywords는 fallback.
    """
    if raw_text and raw_text.strip():
        return raw_text.strip()
    parts = []
    segs = (
        getattr(intent, "path_segments", None)
        or getattr(intent, "page_path_segments", None)
        or []
    )
    if segs:
        parts.extend([str(s).strip() for s in segs if s])
    keywords = (
        getattr(intent, "body_keywords", None)
        or getattr(intent, "title_keywords", None)
        or []
    )
    if keywords:
        parts.extend([str(k).strip() for k in keywords if k])
    return " ".join(parts) if parts else "(요청 내용)"


def _source_text(intent, domain: str) -> str:
    """출처 표시 (GDI · 게임명 / Wiki · space)."""
    if domain == "wiki":
        space = getattr(intent, "space_key", None) or "QASGP"
        return f"Wiki · {space}"
    # gdi
    games = getattr(intent, "game_alias_kr", []) or []
    if games:
        return f"GDI · {games[0]}"
    return "GDI"


def _build_unified(question: str, answer: str, grounds: list, source: str) -> str:
    """통일 답변 포맷: 📋 질문 / 💬 답변 / 🔗 근거 / 📌 출처."""
    lines = [
        "📋 *질문*",
        question,
        "",
        "💬 *답변*",
        answer,
    ]
    if grounds:
        lines.extend(["", "🔗 *근거*"])
        lines.extend(grounds)
    lines.extend(["", "📌 *출처*", source])
    return "\n".join(lines)


def format_metadata_answer(hits: list, intent, domain: str = "wiki", raw_text: Optional[str] = None) -> str:
    """metadata 경로 답변 포맷 — 통일 포맷 (📋💬🔗📌)."""
    question = _question_text(intent, raw_text)
    source = _source_text(intent, domain)

    if not hits:
        return _build_unified(question, "요청하신 정보를 찾을 수 없습니다.", [], source)

    grounds = []
    answer_parts = []
    for hit in hits[:5]:
        meta = hit.metadata if hasattr(hit, "metadata") else {}
        title = hit.title if hasattr(hit, "title") else "제목 없음"
        last_modified = meta.get("last_modified")
        ref_date = meta.get("ref_date")
        path = meta.get("path", "")

        # answer 첫 hit 메타 발췌
        if not answer_parts:
            mf = getattr(intent, "metadata_field", None) or "last_modified"
            if mf == "last_modified" and last_modified:
                answer_parts.append(f"*{title}* — 최근 수정: {_to_kst(last_modified)}")
            elif mf == "ref_date" and ref_date:
                answer_parts.append(f"*{title}* — ref_date: {ref_date}")
            elif last_modified:
                answer_parts.append(f"*{title}* — 최근 수정: {_to_kst(last_modified)}")
            else:
                answer_parts.append(f"*{title}* — 메타 정보 없음 (cache backfill 필요)")

        # 근거: 모든 hit
        line = f"- {title}"
        if last_modified:
            line += f" (최근 수정: {_to_kst(last_modified)})"
        if path:
            line += f"\n  경로: {path}"
        grounds.append(line)

    answer = "\n".join(answer_parts) or "메타 정보를 찾을 수 없습니다."
    return _build_unified(question, answer, grounds, source)


def format_list_answer(hits: list, intent, domain: str = "wiki", raw_text: Optional[str] = None) -> str:
    """list 경로 답변 포맷 — 통일 포맷 (📋💬🔗📌)."""
    question = _question_text(intent, raw_text)
    source = _source_text(intent, domain)

    if not hits:
        return _build_unified(question, "조건에 맞는 항목을 찾을 수 없습니다.", [], source)

    answer = f"검색 결과 {len(hits)}건의 파일이 있습니다."
    grounds = []
    for i, hit in enumerate(hits[:10], 1):
        title = hit.title if hasattr(hit, "title") else "제목 없음"
        meta = hit.metadata if hasattr(hit, "metadata") else {}
        ref_date = meta.get("ref_date", "")
        last_modified = meta.get("last_modified", "")
        date_info = ""
        if ref_date:
            date_info = f" ({ref_date})"
        elif last_modified:
            date_info = f" ({_to_kst(last_modified)})"
        grounds.append(f"{i}. {title}{date_info}")

    return _build_unified(question, answer, grounds, source)


def format_summary_answer(hits: list, intent, domain: str = "wiki", raw_text: Optional[str] = None) -> str:
    """summary 경로 답변 포맷 — 통일 포맷 (📋💬🔗📌)."""
    question = _question_text(intent, raw_text)
    source = _source_text(intent, domain)

    if not hits:
        return _build_unified(question, "요약할 내용을 찾을 수 없습니다.", [], source)

    grounds = []
    answer_parts = []
    for hit in hits[:3]:
        title = hit.title if hasattr(hit, "title") else "제목 없음"
        snippet = hit.snippet if hasattr(hit, "snippet") else ""
        if snippet:
            answer_parts.append(f"*{title}*\n{snippet[:300]}")
        grounds.append(f"- {title}")

    answer = "\n\n".join(answer_parts) or "요약할 내용을 찾을 수 없습니다."
    return _build_unified(question, answer, grounds, source)
