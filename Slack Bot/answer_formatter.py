"""
answer_formatter.py — 검색 결과 답변 포맷터 (task-129 S4-E)

설계 v4 §6.2 정합:
- format_metadata_answer: KST 변환 + silent NULL 차단
- format_list_answer: 제목 목록
- format_summary_answer: 요약 텍스트
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


def format_metadata_answer(hits: list, intent, domain: str = "wiki") -> str:
    """metadata 경로 답변 포맷 (KST + silent NULL 차단).

    빈 결과 시 "찾을 수 없습니다" 반환 (None/빈 문자열 금지).
    """
    if not hits:
        return "요청하신 정보를 찾을 수 없습니다."

    lines = []
    for hit in hits[:5]:
        meta = hit.metadata if hasattr(hit, "metadata") else {}
        title = hit.title if hasattr(hit, "title") else "제목 없음"
        lines.append(f"• *{title}*")

        last_modified = meta.get("last_modified")
        ref_date = meta.get("ref_date")
        if last_modified:
            lines.append(f"  최근 수정: {_to_kst(last_modified)}")
        if ref_date:
            lines.append(f"  ref_date: {ref_date}")

        # MEDIUM-3: cache backfill 안내 (last_modified/ref_date 둘 다 없을 때)
        if not last_modified and not ref_date:
            lines.append("  ⚠️ 원본 메타 미적재 (cache backfill 필요)")

        path = meta.get("path", "")
        if path:
            lines.append(f"  경로: {path}")

    return "\n".join(lines) if lines else "요청하신 정보를 찾을 수 없습니다."


def format_list_answer(hits: list, intent, domain: str = "wiki") -> str:
    """list 경로 답변 포맷 (제목 목록)."""
    if not hits:
        return "조건에 맞는 항목을 찾을 수 없습니다."

    lines = [f"검색 결과 {len(hits)}건:"]
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

        lines.append(f"{i}. {title}{date_info}")

    return "\n".join(lines)


def format_summary_answer(hits: list, intent, domain: str = "wiki") -> str:
    """summary 경로 답변 포맷 (snippet 기반 요약)."""
    if not hits:
        return "요약할 내용을 찾을 수 없습니다."

    lines = []
    for hit in hits[:3]:
        title = hit.title if hasattr(hit, "title") else "제목 없음"
        snippet = hit.snippet if hasattr(hit, "snippet") else ""
        lines.append(f"*{title}*")
        if snippet:
            lines.append(snippet[:300])
        lines.append("")

    return "\n".join(lines).strip() or "요약할 내용을 찾을 수 없습니다."
