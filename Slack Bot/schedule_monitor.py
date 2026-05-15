"""
schedule_monitor.py - 스케줄 실행 모니터링

각 스케줄 job이 실행될 때 log_fired()를 호출하여 당일 실행 기록을 남깁니다.
check_and_alert()는 매일 18:00(KST) 평일에 실행되며,
예정 시각 + 유예시간(grace) 이 지났음에도 실행 기록이 없는 스케줄을
config['monitor_alert_channel'] 채널에 알림 메시지로 발송합니다.

저장 파일: data/job_fire_log.json
{
  "2026-03-09": {
    "daily-qa-checklist": "10:00:23",
    "daily-work-header": "09:00:07",
    ...
  }
}

아키텍처 메모:
  Railway 파일시스템은 재배포 시 초기화됩니다.
  로그 파일이 없더라도 check_and_alert 는 당일 조회 기준으로만 동작하므로
  재배포 직후 false positive 가 발생할 수 있습니다 (메시지 내 안내 문구 포함).
"""

import json
import calendar
import logging
import os
from datetime import datetime, timedelta

import pytz
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)

_BASE     = os.path.dirname(os.path.abspath(__file__))
_LOG_FILE = os.path.join(_BASE, "data", "job_fire_log.json")
_KST      = pytz.timezone("Asia/Seoul")

# 특수 타입은 당일 실행 여부 판단이 어려워 모니터링 제외
_SKIP_TYPES = {"specific"}

# 미션 job: 09:00 + jitter 최대 30분 + 여유 60분 = 11:00 이후 체크
_MISSION_GRACE_HOUR   = 11
_DEFAULT_GRACE_MINUTES = 90   # 일반 스케줄 유예 시간 (분)


# ── 내부 유틸 ───────────────────────────────────────────────────────────────

def _load() -> dict:
    if not os.path.exists(_LOG_FILE):
        return {}
    try:
        with open(_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"[monitor] 로그 파일 로드 실패: {e}")
        return {}


def _save(data: dict):
    os.makedirs(os.path.dirname(_LOG_FILE), exist_ok=True)
    with open(_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── 공개 인터페이스 ──────────────────────────────────────────────────────────

def log_fired(schedule_id: str, date_str: str = None):
    """
    스케줄 job이 실행될 때 호출합니다. 당일 실행 시각을 기록합니다.

    Parameters
    ----------
    schedule_id : config.json schedule id
    date_str    : 로그 날짜 키 'YYYY-MM-DD' (생략 시 오늘 KST)
    """
    data     = _load()
    date_key = date_str or datetime.now(_KST).strftime("%Y-%m-%d")

    if date_key not in data:
        data[date_key] = {}

    fired_at = datetime.now(_KST).strftime("%H:%M:%S")
    data[date_key][schedule_id] = fired_at
    _save(data)
    logger.debug(f"[monitor] 실행 기록: {schedule_id} @ {fired_at}")


def get_fired_today(date_str: str = None) -> dict:
    """
    당일 실행된 schedule_id → fired_time 딕셔너리를 반환합니다.

    Returns
    -------
    dict : {"schedule-id": "HH:MM:SS", ...}  실행 기록 없으면 빈 dict
    """
    date_key = date_str or datetime.now(_KST).strftime("%Y-%m-%d")
    return _load().get(date_key, {})


# ── 당일 실행 여부 판단 ──────────────────────────────────────────────────────

_DAY_ABBR_MAP = {
    "monday": "mon", "tuesday": "tue", "wednesday": "wed",
    "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
    "mon": "mon", "tue": "tue", "wed": "wed",
    "thu": "thu", "fri": "fri", "sat": "sat", "sun": "sun",
    "월요일": "mon", "화요일": "tue", "수요일": "wed",
    "목요일": "thu", "금요일": "fri", "토요일": "sat", "일요일": "sun",
    "월": "mon", "화": "tue", "수": "wed",
    "목": "thu", "금": "fri", "토": "sat", "일": "sun",
}
_DAY_WD_IDX = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}


def should_fire_today(schedule: dict) -> bool:
    """
    오늘 이 스케줄이 실행되어야 하는지 판단합니다.

    specific 타입만 제외하고, nweekly/biweekly도 start_date 기준으로 판단합니다.
    quarterly_first_monday 는 당일 날짜 조건을 직접 확인합니다.
    """
    stype   = schedule.get("type", "")
    today   = datetime.now(_KST)
    weekday = today.weekday()   # 0=월 ... 6=일

    if stype in _SKIP_TYPES:
        return False

    if stype in ("daily", "mission"):
        return weekday < 5   # 평일만

    if stype == "weekly":
        raw   = schedule.get("day_of_week", "")
        abbr  = _DAY_ABBR_MAP.get(raw.lower().strip(), "")
        target_wd = _DAY_WD_IDX.get(abbr, -1)
        return weekday == target_wd

    if stype == "monthly_last_weekday":
        raw   = schedule.get("day_of_week", "")
        abbr  = _DAY_ABBR_MAP.get(raw.lower().strip(), "")
        target_wd = _DAY_WD_IDX.get(abbr, -1)
        if weekday != target_wd:
            return False
        # 이번 달의 마지막 해당 요일인지 추가 확인
        cal_month = calendar.monthcalendar(today.year, today.month)
        last_day = 0
        for week in cal_month:
            if week[target_wd] != 0:
                last_day = week[target_wd]
        return today.day == last_day

    if stype == "monthly_nth_weekday":
        raw   = schedule.get("day_of_week", "")
        abbr  = _DAY_ABBR_MAP.get(raw.lower().strip(), "")
        target_wd = _DAY_WD_IDX.get(abbr, -1)
        if weekday != target_wd:
            return False
        weeks = schedule.get("weeks", [])
        cal_month = calendar.monthcalendar(today.year, today.month)
        for n in weeks:
            count = 0
            for week in cal_month:
                if week[target_wd] != 0:
                    count += 1
                    if count == n and today.day == week[target_wd]:
                        return True
        return False

    if stype in ("biweekly", "nweekly"):
        raw   = schedule.get("day_of_week", "")
        abbr  = _DAY_ABBR_MAP.get(raw.lower().strip(), "")
        target_wd = _DAY_WD_IDX.get(abbr, -1)
        if weekday != target_wd:
            return False
        start_str = schedule.get("start_date", "")
        if not start_str:
            return False
        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            week_interval = int(schedule.get("week_interval", 2))
            days_diff = (today.date() - start_dt.date()).days
            weeks_diff = days_diff // 7
            return weeks_diff % week_interval == 0
        except (ValueError, ZeroDivisionError):
            return False

    if stype == "monthly":
        day_of_month = schedule.get("day_of_month", 1)
        return today.day == day_of_month

    if stype == "quarterly_first_monday":
        # 1·4·7·10월의 1~7일 중 월요일
        return (
            today.month in (1, 4, 7, 10)
            and 1 <= today.day <= 7
            and weekday == 0
        )

    return False


def scheduled_time_passed(schedule: dict, grace_minutes: int = _DEFAULT_GRACE_MINUTES) -> bool:
    """
    '스케줄 예정 시각 + grace_minutes' 가 현재 시각보다 이른지 확인합니다.
    이 조건을 만족해야 '충분한 시간이 지났음에도 미실행'으로 판단합니다.

    - 미션(mission): 09:00 + jitter 30분 + grace 60분 → 11:00 이후
    - 그 외: schedule['time'] + grace_minutes 이후
    """
    now   = datetime.now(_KST)
    stype = schedule.get("type", "")

    if stype == "mission":
        check_time = now.replace(
            hour=_MISSION_GRACE_HOUR, minute=0, second=0, microsecond=0
        )
        return now >= check_time

    time_str = schedule.get("time", "")
    if not time_str:
        return False

    try:
        h, m = map(int, time_str.strip().split(":"))
    except ValueError:
        return False

    scheduled_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
    check_dt     = scheduled_dt + timedelta(minutes=grace_minutes)
    return now >= check_dt


# ── 모니터링 체크 + 알림 ────────────────────────────────────────────────────

def check_and_alert(config: dict, slack_client):
    """
    당일 실행되어야 했던 스케줄 중 미실행 건을 Slack으로 알립니다.

    조건:
      1) enabled=True 인 스케줄
      2) should_fire_today() → True
      3) scheduled_time_passed() → True  (유예 시간 경과)
      4) get_fired_today() 에 실행 기록 없음

    Parameters
    ----------
    config       : config.json 전체 dict
    slack_client : Slack WebClient (chat:write 권한 필요)
    """
    alert_channel = config.get("monitor_alert_channel")
    if not alert_channel:
        logger.warning("[monitor] monitor_alert_channel 미설정 → 알림 건너뜀")
        return

    today = datetime.now(_KST)
    if today.weekday() >= 5:
        logger.info("[monitor] 주말 → 모니터링 건너뜀")
        return

    fired_today      = get_fired_today()
    missed_schedules = []

    for s in config.get("schedules", []):
        if not s.get("enabled", True):
            continue
        if not should_fire_today(s):
            continue
        if not scheduled_time_passed(s):
            continue
        if s["id"] in fired_today:
            continue
        missed_schedules.append(s)

    if not missed_schedules:
        logger.info("[monitor] ✅ 모든 스케줄 정상 실행 확인")
        return

    # ── 알림 메시지 구성 ────────────────────────────────────────────────────
    date_str = today.strftime("%Y-%m-%d")
    lines    = [
        f"🚨 *스케줄 미실행 감지* — {date_str} {today.strftime('%H:%M')} KST 기준\n"
    ]

    for s in missed_schedules:
        stype      = s.get("type", "?")
        sched_time = "09:00~09:30 (랜덤)" if stype == "mission" else s.get("time", "?")
        fired_time = fired_today.get(s["id"], "미기록")
        lines.append(
            f"• *{s.get('name', s['id'])}* (`{s['id']}`)\n"
            f"  └ 타입: `{stype}` | 예정: {sched_time} | 실행: {fired_time}"
        )

    lines.append(
        "\n_⚠ Railway 재배포 직후 발생한 경우 false positive일 수 있습니다._"
    )

    text = "\n".join(lines)
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
    ]

    try:
        slack_client.chat_postMessage(
            channel = alert_channel,
            text    = f"스케줄 미실행 감지 ({len(missed_schedules)}건) — {date_str}",
            blocks  = blocks,
        )
        logger.info(
            f"[monitor] 미실행 알림 전송: {len(missed_schedules)}건 → {alert_channel}"
        )
    except SlackApiError as e:
        logger.error(f"[monitor] 알림 전송 실패: {e.response['error']}")


# ── 봇 시작 시 누락 스케줄 자동 복구 ─────────────────────────────────────────

def recover_missed_schedules(config: dict, sender) -> list:
    """
    봇 시작 시 오늘 발송해야 했지만 누락된 스케줄을 감지하고 즉시 재발송합니다.

    Parameters
    ----------
    config : config.json 전체 dict
    sender : SlackSender 인스턴스

    Returns
    -------
    list : 재발송된 스케줄 이름 목록
    """
    today = datetime.now(_KST)
    if today.weekday() >= 5:
        logger.info("[recover] 주말 → 복구 건너뜀")
        return []

    fired_today = get_fired_today()
    recovered   = []

    for s in config.get("schedules", []):
        if not s.get("enabled", True):
            continue
        if not should_fire_today(s):
            continue
        # 예정 시각이 이미 지났는지 확인
        stype_check = s.get("type", "")
        if stype_check == "mission":
            # 미션은 09:00~09:30 발송, 09:30 이후면 복구 대상
            if today.hour < 9 or (today.hour == 9 and today.minute < 30):
                continue
        else:
            if not scheduled_time_passed(s, grace_minutes=0):
                continue
        if s["id"] in fired_today:
            continue

        # 누락 감지 → 재발송
        name = s.get("name", s["id"])
        try:
            msg_type = s.get("message_type", "")
            stype    = s.get("type", "")

            if msg_type == "interactive_checklist":
                import interaction_handler as ih
                import missed_tracker as mt

                # 전일 누락 항목 (check_missed 스케줄만)
                missed_items = None
                if s.get("check_missed"):
                    try:
                        missed_items = mt.get_missed_items(sender.client)
                        if not missed_items:
                            missed_items = mt.get_missed_items_from_channel(
                                slack_client=sender.client,
                                channel=s["channel"],
                                config_items=s.get("items", []),
                            )
                    except Exception:
                        pass

                ts = sender.send_interactive_checklist(
                    channel=s["channel"],
                    schedule=s,
                    missed_items=missed_items,
                )
                if ts:
                    ih.register(
                        channel=s["channel"],
                        ts=ts,
                        schedule_id=s["id"],
                        title=s.get("title", "📋 체크리스트"),
                        items=s.get("items", []),
                        schedule_type=stype,
                    )
                    # 전송 로그 기록
                    if s.get("check_missed"):
                        try:
                            flat_items = mt.extract_flat_items(s.get("items", []))
                            label      = mt.make_label(s)
                            mt.log_sent(
                                channel=s["channel"], ts=ts,
                                schedule_id=s["id"], label=label,
                                items=flat_items,
                            )
                        except Exception:
                            pass

            elif stype == "mission":
                sender.send_mission_reminder(s)

            else:
                sender.send(channel=s["channel"], schedule=s)

            log_fired(s["id"])
            recovered.append(name)
            logger.info(f"[recover] ✅ 재발송 완료: {name}")

        except Exception as e:
            logger.error(f"[recover] ❌ 재발송 실패: {name} → {e}")

    if recovered:
        # 복구 결과를 모니터링 채널에 알림
        alert_channel = config.get("monitor_alert_channel")
        if alert_channel:
            try:
                sender.client.chat_postMessage(
                    channel=alert_channel,
                    text=(
                        f"🔄 *봇 시작 시 누락 스케줄 자동 복구* — "
                        f"{today.strftime('%Y-%m-%d %H:%M')} KST\n"
                        f"복구 {len(recovered)}건: {', '.join(recovered)}"
                    ),
                )
            except Exception:
                pass

        logger.info(f"[recover] 누락 복구 완료: {len(recovered)}건 → {recovered}")
    else:
        logger.info("[recover] 누락 스케줄 없음 — 모든 스케줄 정상")

    return recovered
