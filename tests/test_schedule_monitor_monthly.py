"""
회귀 테스트: monthly_last_weekday should_fire_today 버그 (commit 95f2121)

버그: should_fire_today()가 monthly_last_weekday를 weekly와 동일하게 처리하여
     금요일이면 무조건 True 반환 → 누락 복구 로직이 모든 금요일에 오발송.

수정: 이번 달의 마지막 해당 요일인지 calendar로 추가 검증.
"""

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytz
import pytest

# Slack Bot 소스 루트를 sys.path에 추가 (다른 테스트 파일과 동일한 패턴)
_SLACK_BOT_ROOT = str(Path(__file__).parent.parent / "Slack Bot")
if _SLACK_BOT_ROOT not in sys.path:
    sys.path.insert(0, _SLACK_BOT_ROOT)

KST = pytz.timezone("Asia/Seoul")

SCHEDULE = {
    "id": "monthly-qa-checklist",
    "type": "monthly_last_weekday",
    "day_of_week": "friday",
    "time": "09:45",
}

# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _kst(year: int, month: int, day: int) -> datetime:
    return KST.localize(datetime(year, month, day, 10, 0, 0))


def _call(fake_now: datetime) -> bool:
    """schedule_monitor.datetime.now 를 fake_now 로 패치하고 should_fire_today 호출."""
    import schedule_monitor as sm  # noqa: import inside to get fresh module state

    with patch.object(sm, "datetime", wraps=sm.datetime) as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.strptime.side_effect = datetime.strptime
        return sm.should_fire_today(SCHEDULE)


# ── 핵심 회귀 케이스 ──────────────────────────────────────────────────────────

class TestMonthlyLastWeekdayFireToday:
    """should_fire_today — monthly_last_weekday 타입 정확도"""

    # 2026년 5월: 마지막 금요일 = 29일
    def test_non_last_friday_returns_false(self):
        """5월 15일(금, 3번째): 오발송 버그 재현 — False 여야 함."""
        assert _call(_kst(2026, 5, 15)) is False  # ← 버그 전 True 반환

    def test_non_last_friday_week1_returns_false(self):
        """5월 1일(금, 1번째): False 여야 함."""
        assert _call(_kst(2026, 5, 1)) is False

    def test_non_last_friday_week2_returns_false(self):
        """5월 8일(금, 2번째): False 여야 함."""
        assert _call(_kst(2026, 5, 8)) is False

    def test_non_last_friday_week3_returns_false(self):
        """5월 22일(금, 4번째): False 여야 함."""
        assert _call(_kst(2026, 5, 22)) is False

    def test_last_friday_returns_true(self):
        """5월 29일(금, 마지막): True 여야 함."""
        assert _call(_kst(2026, 5, 29)) is True

    def test_non_friday_weekday_returns_false(self):
        """5월 29일이지만 목요일이 아님 — 요일 불일치: False."""
        # 5월 14일은 목요일
        assert _call(_kst(2026, 5, 14)) is False

    # 경계: 2월 (28/29일)
    def test_last_friday_february_non_leap(self):
        """2026년 2월 마지막 금요일 = 27일."""
        assert _call(_kst(2026, 2, 27)) is True

    def test_non_last_friday_february_non_leap(self):
        """2026년 2월 20일(금, 3번째): False."""
        assert _call(_kst(2026, 2, 20)) is False

    # monthly_last_weekday vs weekly 혼용 방지
    def test_weekly_schedule_unaffected(self):
        """weekly 타입은 마지막 요일 검사 없이 요일만 맞으면 True."""
        weekly_sched = {**SCHEDULE, "type": "weekly"}
        import schedule_monitor as sm
        with patch.object(sm, "datetime", wraps=sm.datetime) as mock_dt:
            mock_dt.now.return_value = _kst(2026, 5, 15)  # 금요일
            mock_dt.strptime.side_effect = datetime.strptime
            assert sm.should_fire_today(weekly_sched) is True


NTH_SCHEDULE = {
    "id": "monthly-qa-checklist",
    "type": "monthly_nth_weekday",
    "day_of_week": "friday",
    "weeks": [2, 4],
    "time": "09:45",
}


class TestMonthlyNthWeekdayFireToday:
    """should_fire_today — monthly_nth_weekday 타입 (2주차+4주차)"""

    # 2026년 5월: 금요일 = 1일, 8일, 15일, 22일, 29일
    def test_2nd_friday_returns_true(self):
        """5월 8일(2주차 금): True."""
        assert _call_nth(_kst(2026, 5, 8)) is True

    def test_3rd_friday_returns_false(self):
        """5월 15일(3주차 금): False."""
        assert _call_nth(_kst(2026, 5, 15)) is False

    def test_4th_friday_returns_true(self):
        """5월 22일(4주차 금): True."""
        assert _call_nth(_kst(2026, 5, 22)) is True

    def test_5th_friday_returns_false(self):
        """5월 29일(5주차 금 = 마지막이지만 5주차): False."""
        assert _call_nth(_kst(2026, 5, 29)) is False

    # 6월: 금요일 = 5일, 12일, 19일, 26일
    def test_june_2nd_friday(self):
        """6월 12일(2주차 금): True."""
        assert _call_nth(_kst(2026, 6, 12)) is True

    def test_june_4th_friday(self):
        """6월 26일(4주차 금): True."""
        assert _call_nth(_kst(2026, 6, 26)) is True

    def test_june_1st_friday_returns_false(self):
        """6월 5일(1주차 금): False."""
        assert _call_nth(_kst(2026, 6, 5)) is False

    def test_non_friday_returns_false(self):
        """5월 21일(목): False."""
        assert _call_nth(_kst(2026, 5, 21)) is False


def _call_nth(fake_now: datetime) -> bool:
    """monthly_nth_weekday 스케줄로 should_fire_today 호출."""
    import schedule_monitor as sm
    with patch.object(sm, "datetime", wraps=sm.datetime) as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.strptime.side_effect = datetime.strptime
        return sm.should_fire_today(NTH_SCHEDULE)
