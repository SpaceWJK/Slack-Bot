"""task-118 R-1 — gdi_client._sanitize_log_field 단위 테스트.

OWASP A09 Logging Failures 차단 — 사용자 입력 안의 ASCII '|'가
dashboard parser(s3_server.py: split('|'))에서 필드 desync를 일으키지 않도록
전각 '｜' (U+FF5C)로 치환.
"""
import sys
from pathlib import Path

# Slack Bot/ 모듈 import
sys.path.insert(0, str(Path(__file__).parent.parent / "Slack Bot"))

import pytest
from gdi_client import _sanitize_log_field


def test_ascii_pipe_replaced():
    assert _sanitize_log_field("a|b") == "a｜b"


def test_no_pipe_unchanged():
    s = "Chaoszero/Update Review \\ 기획서 목록"
    assert _sanitize_log_field(s) == s


def test_empty_string():
    assert _sanitize_log_field("") == ""


def test_none_value():
    assert _sanitize_log_field(None) is None


def test_multiple_pipes():
    assert _sanitize_log_field("|a|b|") == "｜a｜b｜"


def test_korean_with_pipe():
    assert _sanitize_log_field("최근 | 데이터") == "최근 ｜ 데이터"


def test_pipe_in_kv():
    assert _sanitize_log_field("a=1|b=2") == "a=1｜b=2"


def test_real_query_with_slash():
    # 실측 로그 샘플 — slash는 보존, pipe만 치환
    s = "Chaoszero/Test Result \\ 최근 BAT 결과"
    assert _sanitize_log_field(s) == s


def test_taxonomy_response_with_newline_preserved():
    # \n은 log_gdi_query 단계에서 별도 escape됨 — sanitize는 \n 미건드림
    s = "택소노미: Chaoszero/Update Review\n파일: QA_CL_*.xlsx"
    assert _sanitize_log_field(s) == s


def test_injection_payload():
    # 가장 위험한 payload — 가짜 ERROR/duration 주입 시도
    s = "real query | ERROR | injected | 99999ms"
    expected = "real query ｜ ERROR ｜ injected ｜ 99999ms"
    assert _sanitize_log_field(s) == expected


def test_action_field_sanitize():
    # task-119 — action 인자도 sanitize 대상 (defense-in-depth)
    # 현재 호출처 모두 하드코딩이지만 향후 user input 흐름 추가 시 즉시 vector화 차단
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent / "Slack Bot"))
    from gdi_client import log_gdi_query

    log_gdi_query(
        user_id="U_TASK119",
        user_name="task119",
        action="ask|FAKE_STATUS|9999ms",  # injection 시도 (정상 호출처 없으나 방어)
        query="action sanitize test",
    )
    log_path = Path(__file__).parent.parent / "logs" / "gdi_query.log"
    last = log_path.read_text(encoding="utf-8").splitlines()[-1]
    # action 필드의 ASCII | 가 전각 ｜로 치환된 entry가 마지막 라인이어야 함
    assert "ask｜FAKE_STATUS｜9999ms" in last
    assert "ask|FAKE_STATUS|9999ms" not in last
    # split(' | ') 결과 정합성 (필드 desync 0)
    fields = last.split(" | ")
    # ts | OK | <action_sanitized> | user=... | query=...  → 5 fields
    assert len(fields) == 5

    # cleanup — task119 entries 제거 (qa-blackbox 운영 데이터 mutation 금지)
    lines = log_path.read_text(encoding="utf-8").splitlines(keepends=True)
    clean = [l for l in lines if "U_TASK119" not in l]
    log_path.write_text("".join(clean), encoding="utf-8")
