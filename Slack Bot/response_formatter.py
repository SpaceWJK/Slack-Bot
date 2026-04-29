"""
response_formatter.py — 통합 응답 포맷터 (v1.5.4)

/wiki, /gdi, /jira AI 답변을 일관된 3단 구조로 출력.

구조:
  📋 질문
  💬 답변  (핵심 결론)
  📎 근거  (판단 이유 — 없으면 생략)
  🔗 출처  (원본 링크)
"""

import os
import re


# ── Claude 프롬프트에 추가할 응답 형식 지시문 ────────────────────

ANSWER_FORMAT_INSTRUCTION = (
    "\n\n[응답 형식]\n"
    "반드시 아래 형식으로 답변하세요:\n\n"
    "[답변]\n"
    "(질문에 대한 핵심 결론을 간결하게)\n\n"
    "[근거]\n"
    "(답변의 근거가 되는 원문 내용이나 판단 이유를 설명)"
)

# Jira 이슈 키 패턴 (예: GCZ-3394, EP7-1234)
_JIRA_KEY_RE = re.compile(r'\b([A-Z][A-Z0-9]{0,9}-\d+)\b')


# ── 파서 ─────────────────────────────────────────────────────────

def parse_answer_sections(raw: str) -> tuple:
    """Claude 응답에서 [답변]과 [근거] 섹션을 분리.

    반환: (answer, evidence)
    파싱 실패 시 (raw, "") — 안전 폴백.
    """
    m = re.search(
        r"\[답변\]\s*\n(.*?)\n\s*\[근거\]\s*\n(.*)",
        raw, re.DOTALL,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw.strip(), ""


def _md_to_slack(text: str) -> str:
    """Claude가 생성한 Markdown 강조 구문을 Slack mrkdwn으로 변환.

    - **굵게** → *굵게*  (Slack bold)
    - __굵게__ → *굵게*
    이중 asterisk/underscore가 Slack에서 리터럴로 보이는 문제 해결.
    """
    # **text** → *text* (순서 중요: *** 보호 후 처리)
    text = re.sub(r'\*\*\*(.+?)\*\*\*', r'*\1*', text, flags=re.DOTALL)
    text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', text, flags=re.DOTALL)
    # __text__ → *text*
    text = re.sub(r'__(.+?)__', r'*\1*', text, flags=re.DOTALL)
    return text


def _linkify_jira_keys(text: str, base_url: str) -> str:
    """텍스트 내 Jira 이슈 키를 Slack 하이퍼링크로 변환.

    이미 링크 안에 있는 키(<...|KEY>)는 건너뜀.
    """
    def _replace(m: re.Match) -> str:
        key = m.group(1)
        # 이미 Slack 링크 안에 있으면 건너뜀 (<URL|...) 패턴)
        start = m.start()
        preceding = text[max(0, start - 5):start]
        if '|' in preceding or '<' in preceding:
            return key
        return f"<{base_url}/browse/{key}|{key}>"

    return _JIRA_KEY_RE.sub(_replace, text)


# ── 포맷터 ───────────────────────────────────────────────────────

def format_ai_response(
    question: str,
    raw_answer: str,
    source_type: str,       # "wiki" | "jira" | "gdi"
    source_label: str,      # 페이지 제목 / 이슈키 / 파일명
    source_url: str = "",   # 원본 링크 (없으면 빈 문자열)
    display_question: str = "",  # 표시용 전체 커맨드 (없으면 question 사용)
) -> str:
    """3단 구조 통합 포맷 mrkdwn 문자열을 반환."""
    answer, evidence = parse_answer_sections(raw_answer)

    # ── Slack 포맷 정규화 ─────────────────────────────────────────
    # 1. **bold** → *bold* (Slack mrkdwn 변환)
    answer = _md_to_slack(answer)
    evidence = _md_to_slack(evidence)

    # 2. Jira 이슈 키 자동 하이퍼링크 (jira 타입만)
    if source_type == "jira":
        jira_base = os.getenv("JIRA_BASE_URL", "https://jira.smilegate.net")
        answer = _linkify_jira_keys(answer, jira_base)
        evidence = _linkify_jira_keys(evidence, jira_base)

    shown_question = display_question or question
    parts = [
        f"📋 *질문*\n{shown_question}",
        f"💬 *답변*\n{answer}",
    ]

    if evidence:
        parts.append(f"📎 *근거*\n{evidence}")

    # ── 출처 라인 ──
    type_label = {"wiki": "Wiki", "jira": "Jira", "gdi": "GDI"}.get(
        source_type, source_type
    )
    if source_url:
        # 이슈 키 단독 조회 시: 이슈 URL 직접 링크
        parts.append(
            f"🔗 *출처*: {type_label} · <{source_url}|{source_label} 바로가기>"
        )
    elif source_type == "jira":
        # 프로젝트 검색 시: source_label(프로젝트명)로 프로젝트 링크 추가 시도
        jira_base = os.getenv("JIRA_BASE_URL", "https://jira.smilegate.net")
        # source_label이 프로젝트 키 패턴이면 바로 사용, 아니면 텍스트만
        proj_key_m = re.match(r'^([A-Z][A-Z0-9]{1,9})$', source_label.strip())
        if proj_key_m:
            proj_key = proj_key_m.group(1)
            proj_url = f"{jira_base}/projects/{proj_key}/"
            parts.append(
                f"🔗 *출처*: {type_label} · <{proj_url}|{source_label}>"
            )
        else:
            parts.append(f"🔗 *출처*: {type_label} · {source_label}")
    else:
        parts.append(f"🔗 *출처*: {type_label} · {source_label}")

    return "\n\n".join(parts)
