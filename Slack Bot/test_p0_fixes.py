# -*- coding: utf-8 -*-
"""P0 보안/견고성 패치 검증 (predeploy 감사 후속).

T-1: jira log sanitize — '|' 파이프 인젝션 차단 (gdi와 동일 패턴)
T-2: wiki CQL escape — 따옴표 인젝션 차단
실행: python -m pytest test_p0_fixes.py -q
"""
import logging
import sys

logging.disable(logging.CRITICAL)


# ── T-1: jira _sanitize_log_field ─────────────────────────────────────
def test_jira_sanitize_pipe():
    import jira_client as jc
    assert hasattr(jc, "_sanitize_log_field"), "jira_client에 _sanitize_log_field 없음"
    assert jc._sanitize_log_field("a|b") == "a｜b"
    assert jc._sanitize_log_field("") == ""
    assert jc._sanitize_log_field(None) is None
    assert jc._sanitize_log_field("정상텍스트") == "정상텍스트"


def test_jira_log_query_sanitized(tmp_path, monkeypatch):
    """log_jira_query가 pipe 포함 입력을 sanitize해서 기록하는지."""
    import jira_client as jc
    records = []

    class FakeLogger:
        def info(self, msg): records.append(msg)
        def error(self, msg): records.append(msg)

    monkeypatch.setattr(jc, "_get_jira_query_logger", lambda: FakeLogger())
    jc.log_jira_query(user_id="U1", user_name="evil|name",
                      action="search", query="OK | injected=1")
    assert len(records) == 1
    # 사용자 입력 유래 ASCII 파이프가 로그 분리자로 남으면 안 됨
    assert "evil|name" not in records[0]
    assert "evil｜name" in records[0]
    assert "OK ｜ injected=1" in records[0]


# ── T-2: wiki CQL escape ──────────────────────────────────────────────
def test_wiki_cql_escape_exists():
    import wiki_client as wc
    assert hasattr(wc, "_cql_escape"), "wiki_client에 _cql_escape 없음"
    # 따옴표 무력화
    assert '"' not in wc._cql_escape('a"b').replace('\\"', "")
    assert wc._cql_escape('normal') == 'normal'
    assert wc._cql_escape('say "hi"') == 'say \\"hi\\"'
    # 백슬래시 선처리 (escape 우회 방지)
    assert wc._cql_escape('a\\"b') == 'a\\\\\\"b'
    assert wc._cql_escape("") == ""


def test_wiki_cql_injection_neutralized():
    """인젝션 페이로드가 escape 후 CQL 구조를 못 바꾸는지 (문자열 리터럴 안에 갇힘)."""
    import wiki_client as wc
    payload = '" OR type=blogpost OR title="'
    escaped = wc._cql_escape(payload)
    cql = f'title = "{escaped}" AND type=page'
    # escape 안 된 닫는 따옴표가 없어야 함 → 리터럴 탈출 불가
    import re
    unescaped_quotes = re.findall(r'(?<!\\)"', cql)
    # 정상 따옴표는 title = "..." 의 여는/닫는 2개뿐
    assert len(unescaped_quotes) == 2, f"리터럴 탈출 가능: {cql}"
