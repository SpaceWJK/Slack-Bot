"""
scheduler.py - APScheduler 기반 알림 스케줄 관리

지원 타입:
  daily                - 매일 HH:MM
  weekly               - 매주 특정 요일 HH:MM
  monthly              - 매월 특정 일 HH:MM
  monthly_last_weekday - 매월 마지막 특정 요일 HH:MM  ← 인터랙티브 체크리스트에 사용
  biweekly             - 격주 특정 요일 HH:MM (start_date 기준)
  nweekly              - N주 간격 특정 요일 HH:MM (week_interval + start_date 기준)
  specific             - 특정 날짜+시간 1회
  mission              - 미션 진행 현황 리마인더, 평일 09:00~09:30 랜덤 발송
"""

import calendar
import json
import logging
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler   # ← 논블로킹
from apscheduler.triggers.cron         import CronTrigger
from apscheduler.triggers.date         import DateTrigger
from apscheduler.triggers.interval     import IntervalTrigger

import schedule_monitor as sm

logger = logging.getLogger(__name__)

# 요일 이름 → APScheduler 약어 매핑 (한/영 모두 지원)
DAY_MAP = {
    # 영문 전체
    "monday":    "mon", "tuesday": "tue", "wednesday": "wed",
    "thursday":  "thu", "friday":  "fri", "saturday":  "sat", "sunday": "sun",
    # 영문 약어
    "mon": "mon", "tue": "tue", "wed": "wed",
    "thu": "thu", "fri": "fri", "sat": "sat", "sun": "sun",
    # 한국어
    "월요일": "mon", "화요일": "tue", "수요일": "wed",
    "목요일": "thu", "금요일": "fri", "토요일": "sat", "일요일": "sun",
    "월": "mon", "화": "tue", "수": "wed",
    "목": "thu", "금": "fri", "토": "sat", "일": "sun",
}

DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

# calendar.weekday 인덱스 (월=0 ~ 일=6) — _is_last_weekday_of_month 에서 사용
DAY_WEEKDAY_IDX = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}


class NotificationScheduler:

    def __init__(self, slack_sender, config_path: str = "config.json"):
        self.sender      = slack_sender
        self.config_path = config_path
        self.config      = self._load_config()
        self.tz          = pytz.timezone(self.config.get("timezone", "Asia/Seoul"))
        # BackgroundScheduler: start() 가 논블로킹 → Slack Bolt 와 공존 가능
        self.scheduler   = BackgroundScheduler(timezone=self.tz)

    # ── 내부 유틸 ─────────────────────────────────────────────

    def _load_config(self) -> dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _parse_hm(self, time_str: str):
        """'HH:MM' → (hour, minute)"""
        h, m = map(int, time_str.strip().split(":"))
        return h, m

    def _resolve_day(self, day_str: str) -> str:
        """한/영 요일 문자열 → APScheduler 약어 (예: 'fri')"""
        key    = day_str.strip().lower()
        result = DAY_MAP.get(key)
        if not result:
            raise ValueError(f"알 수 없는 요일: '{day_str}'")
        return result

    def _is_last_weekday_of_month(self, date_obj, day_abbr: str) -> bool:
        """date_obj 가 해당 월의 마지막 day_abbr 요일인지 확인"""
        target_wd = DAY_WEEKDAY_IDX.get(day_abbr, 4)   # 기본: 금(4)
        cal       = calendar.monthcalendar(date_obj.year, date_obj.month)
        last_day  = 0
        for week in cal:
            if week[target_wd] != 0:
                last_day = week[target_wd]
        return date_obj.day == last_day

    def _is_nth_weekday_of_month(self, date_obj, day_abbr: str, n: int) -> bool:
        """date_obj 가 해당 월의 n번째 day_abbr 요일인지 확인 (1-indexed)"""
        target_wd = DAY_WEEKDAY_IDX.get(day_abbr, 4)
        cal       = calendar.monthcalendar(date_obj.year, date_obj.month)
        count     = 0
        for week in cal:
            if week[target_wd] != 0:
                count += 1
                if count == n:
                    return date_obj.day == week[target_wd]
        return False

    # ── Job 함수 생성기 ────────────────────────────────────────

    def _make_job(self, s: dict):
        """일반 텍스트/정적 체크리스트 job 생성"""
        def job():
            sm.log_fired(s["id"])
            self.sender.send(channel=s["channel"], schedule=s)
        job.__name__ = s.get("name", s["id"])
        return job

    def _make_mission_job(self, s: dict):
        """미션 진행 현황 리마인더 job 생성"""
        def job():
            sm.log_fired(s["id"])
            self.sender.send_mission_reminder(s)
        job.__name__ = s.get("name", s["id"])
        return job

    def _make_interactive_job(self, s: dict):
        """인터랙티브 체크리스트 job 생성 (전송 후 상태 등록)"""
        def job():
            sm.log_fired(s["id"])
            import interaction_handler as ih
            import missed_tracker as mt

            # ── 전일 누락 항목 조회 (check_missed: true 인 스케줄만) ──────────
            missed_items = None
            if s.get("check_missed"):
                try:
                    # 1차: 로컬 로그 파일 (Railway 재시작 없으면 빠름)
                    missed_items = mt.get_missed_items(self.sender.client)
                    # 2차 폴백: 로그 파일 없을 때 Slack 채널 히스토리 직접 스캔
                    if not missed_items:
                        missed_items = mt.get_missed_items_from_channel(
                            slack_client = self.sender.client,
                            channel      = s["channel"],
                            config_items = s.get("items", []),
                        )
                    if missed_items:
                        total_missed = sum(len(g["items"]) for g in missed_items)
                        logger.info(
                            f"[missed] 전일 누락 {total_missed}개 포함 예정: "
                            f"{[g['label'] for g in missed_items]}"
                        )
                except Exception as e:
                    logger.warning(f"[missed] 전일 누락 조회 실패 (무시): {e}")

            # ── 체크리스트 전송 ────────────────────────────────────────────────
            ts = self.sender.send_interactive_checklist(
                channel      = s["channel"],
                schedule     = s,
                missed_items = missed_items,
            )

            if ts:
                # 로컬 봇의 interaction_handler 에 상태 등록
                ih.register(
                    channel       = s["channel"],
                    ts            = ts,
                    schedule_id   = s["id"],
                    title         = s.get("title", "📋 체크리스트"),
                    items         = s.get("items", []),
                    schedule_type = s.get("type", ""),
                )
                # 다음날 누락 체크를 위해 전송 로그 기록
                if s.get("check_missed"):
                    try:
                        flat_items = mt.extract_flat_items(s.get("items", []))
                        label      = mt.make_label(s)
                        mt.log_sent(
                            channel     = s["channel"],
                            ts          = ts,
                            schedule_id = s["id"],
                            label       = label,
                            items       = flat_items,
                        )
                    except Exception as e:
                        logger.warning(f"[missed] 전송 로그 기록 실패 (무시): {e}")

        job.__name__ = s.get("name", s["id"])
        return job

    def _select_job_fn(self, s: dict):
        """message_type 에 따라 알맞은 job 함수 반환"""
        if s.get("message_type") == "interactive_checklist":
            return self._make_interactive_job(s)
        return self._make_job(s)

    def _register_job(self, s: dict, trigger, desc: str, job_fn=None, misfire_grace_time=None):
        """스케줄러에 job 등록"""
        fn = job_fn if job_fn is not None else self._select_job_fn(s)
        kwargs = dict(
            trigger = trigger,
            id      = s["id"],
            name    = s.get("name", s["id"]),
        )
        if misfire_grace_time is not None:
            kwargs["misfire_grace_time"] = misfire_grace_time
        self.scheduler.add_job(fn, **kwargs)
        logger.info(f"  ✅ 등록: [{s.get('name')}]  ({desc})")

    # ── 스케줄 타입별 등록 ─────────────────────────────────────

    def _add_daily(self, s: dict):
        h, m = self._parse_hm(s["time"])
        self._register_job(
            s,
            CronTrigger(day_of_week="mon-fri", hour=h, minute=m, timezone=self.tz),
            f"평일(월-금) {s['time']}",
            misfire_grace_time=60,   # 60초: Railway 재시작 1회 허용, 중복 발송 방지
        )

    def _add_weekly(self, s: dict):
        h, m = self._parse_hm(s["time"])
        day  = self._resolve_day(s["day_of_week"])
        self._register_job(
            s,
            CronTrigger(day_of_week=day, hour=h, minute=m, timezone=self.tz),
            f"매주 {s['day_of_week']} {s['time']}",
            misfire_grace_time=60,   # 60초: Railway 재시작 1회 허용, 중복 발송 방지
        )

    def _add_monthly(self, s: dict):
        h, m = self._parse_hm(s["time"])
        day  = s.get("day_of_month", 1)
        self._register_job(
            s,
            CronTrigger(day=day, hour=h, minute=m, timezone=self.tz),
            f"매월 {day}일 {s['time']}",
        )

    def _add_monthly_last_weekday(self, s: dict):
        """
        매월 마지막 특정 요일
        - CronTrigger: 매주 해당 요일에 실행
        - job 내부: 오늘이 '마지막 주' 인지 확인 → 아니면 스킵
        """
        h, m     = self._parse_hm(s["time"])
        day_abbr = self._resolve_day(s.get("day_of_week", "friday"))
        day_name = s.get("day_of_week", "금요일")

        def job():
            today = datetime.now(self.tz).date()
            if not self._is_last_weekday_of_month(today, day_abbr):
                logger.info(
                    f"  ⏭  스킵 (마지막 주 아님): [{s.get('name')}] {today}"
                )
                return
            sm.log_fired(s["id"])
            logger.info(f"  🚀  실행 (마지막 {day_name}): [{s.get('name')}] {today}")
            # message_type 에 맞는 실제 전송 수행
            self._select_job_fn(s)()

        job.__name__ = s.get("name", s["id"])
        self._register_job(
            s,
            CronTrigger(day_of_week=day_abbr, hour=h, minute=m, timezone=self.tz),
            f"매월 마지막 {day_name} {s['time']}",
            job_fn=job,
        )

    def _add_monthly_nth_weekday(self, s: dict):
        """
        매월 n번째 특정 요일 (복수 지정 가능)
        config 필드: day_of_week, weeks (list[int], 예: [2, 4])
        - CronTrigger: 매주 해당 요일에 실행
        - job 내부: 오늘이 지정된 주차인지 확인 → 아니면 스킵
        """
        h, m      = self._parse_hm(s["time"])
        day_abbr  = self._resolve_day(s.get("day_of_week", "friday"))
        day_name  = s.get("day_of_week", "금요일")
        weeks     = s.get("weeks", [])  # e.g. [2, 4]

        def job():
            today = datetime.now(self.tz).date()
            matched = any(self._is_nth_weekday_of_month(today, day_abbr, n) for n in weeks)
            if not matched:
                logger.info(
                    f"  ⏭  스킵 ({weeks}주차 아님): [{s.get('name')}] {today}"
                )
                return
            sm.log_fired(s["id"])
            logger.info(f"  🚀  실행 ({weeks}주차 {day_name}): [{s.get('name')}] {today}")
            self._select_job_fn(s)()

        job.__name__ = s.get("name", s["id"])
        weeks_str = "+".join(f"{n}주차" for n in weeks)
        self._register_job(
            s,
            CronTrigger(day_of_week=day_abbr, hour=h, minute=m, timezone=self.tz),
            f"매월 {weeks_str} {day_name} {s['time']}",
            job_fn=job,
        )

    def _add_biweekly(self, s: dict):
        """
        격주: IntervalTrigger(weeks=2)
        start_date 지정 없으면 → 오늘 기준 가장 가까운 해당 요일을 시작점으로 자동 설정
        """
        h, m      = self._parse_hm(s["time"])
        day       = self._resolve_day(s["day_of_week"])
        start_str = s.get("start_date")

        if start_str:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            start_dt = start_dt.replace(hour=h, minute=m, second=0, microsecond=0)
            start_dt = self.tz.localize(start_dt)
        else:
            now        = datetime.now(self.tz)
            target_idx = DAY_ORDER.index(day)
            today_idx  = now.weekday()
            delta      = (target_idx - today_idx) % 7
            start_dt   = (now + timedelta(days=delta)).replace(
                hour=h, minute=m, second=0, microsecond=0
            )

        trigger = IntervalTrigger(weeks=2, start_date=start_dt, timezone=self.tz)
        self._register_job(
            s,
            trigger,
            f"격주 {s['day_of_week']} {s['time']} (시작: {start_dt.strftime('%Y-%m-%d')})",
        )

    def _add_nweekly(self, s: dict):
        """
        N주 간격: IntervalTrigger(weeks=N)
        - week_interval : 간격 주 수 (기본 2)
        - start_date    : 첫 실행 날짜 'YYYY-MM-DD' (생략 시 가장 가까운 해당 요일)
        """
        h, m          = self._parse_hm(s["time"])
        day           = self._resolve_day(s["day_of_week"])
        week_interval = int(s.get("week_interval", 2))
        start_str     = s.get("start_date")

        if start_str:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            start_dt = start_dt.replace(hour=h, minute=m, second=0, microsecond=0)
            start_dt = self.tz.localize(start_dt)
        else:
            now        = datetime.now(self.tz)
            target_idx = DAY_ORDER.index(day)
            today_idx  = now.weekday()
            delta      = (target_idx - today_idx) % 7
            start_dt   = (now + timedelta(days=delta)).replace(
                hour=h, minute=m, second=0, microsecond=0
            )

        trigger = IntervalTrigger(weeks=week_interval, start_date=start_dt, timezone=self.tz)
        self._register_job(
            s,
            trigger,
            f"{week_interval}주 간격 {s['day_of_week']} {s['time']} (시작: {start_dt.strftime('%Y-%m-%d')})",
        )

    def _add_quarterly_first_monday(self, s: dict):
        """
        분기 첫째 월요일: 1월·4월·7월·10월 의 1~7일 사이 월요일
        - CronTrigger: 매주 월요일 지정 시각에 실행
        - job 내부: 현재 월이 분기 시작월(1/4/7/10) AND 날짜가 1~7일인지 확인 → 아니면 스킵
        """
        h, m = self._parse_hm(s["time"])

        def job():
            today = datetime.now(self.tz).date()
            if today.month not in (1, 4, 7, 10) or not (1 <= today.day <= 7):
                logger.info(
                    f"  ⏭  스킵 (분기 첫째 월요일 아님): [{s.get('name')}] {today}"
                )
                return
            sm.log_fired(s["id"])
            quarter = {1: 1, 4: 2, 7: 3, 10: 4}[today.month]
            logger.info(f"  🚀  실행 ({quarter}분기 첫째 월요일): [{s.get('name')}] {today}")
            self._select_job_fn(s)()

        job.__name__ = s.get("name", s["id"])
        self._register_job(
            s,
            CronTrigger(day_of_week="mon", hour=h, minute=m, timezone=self.tz),
            f"분기 첫째 월요일 {s['time']} (1·4·7·10월 1~7일)",
            job_fn=job,
        )

    def _add_mission(self, s: dict):
        """
        미션 진행 현황 리마인더: 평일 09:00~09:30 사이 랜덤 발송

        APScheduler CronTrigger 의 jitter 파라미터를 사용합니다.
        jitter=1800 → 09:00 기준 0~1800초(30분) 내 무작위 지연.
        채널마다 별도 job 이므로 동시 발송 충돌 없이 자연스럽게 분산됩니다.

        misfire_grace_time=7200: Railway 재시작 등으로 jitter 발송 시각이 지났더라도
        2시간 이내라면 재기동 후 즉시 발송. 기본값(1초)이면 jitter 구간 중 재시작 시 스킵됨.
        """
        self._register_job(
            s,
            CronTrigger(
                day_of_week="mon-fri",
                hour=9, minute=0,
                timezone=self.tz,
                jitter=1800,           # 0~30분 랜덤 지연
            ),
            "평일 09:00~09:30 (랜덤)",
            job_fn=self._make_mission_job(s),
            misfire_grace_time=7200,   # 2시간 — jitter 구간 중 재시작 후에도 당일 발송 보장
        )

    def _add_specific(self, s: dict):
        """특정 날짜+시간 1회성"""
        run_dt = datetime.strptime(s["datetime"], "%Y-%m-%d %H:%M")
        run_dt = self.tz.localize(run_dt)

        if run_dt <= datetime.now(self.tz):
            logger.warning(
                f"  ⚠  스킵 (이미 지난 시각): [{s.get('name')}] {s['datetime']}"
            )
            return

        self._register_job(
            s,
            DateTrigger(run_date=run_dt, timezone=self.tz),
            f"특정 날짜 {s['datetime']}",
        )

    # ── 공개 인터페이스 ───────────────────────────────────────

    def setup(self):
        """config.json 의 schedules 를 읽어 스케줄러에 등록"""
        logger.info("━" * 52)
        logger.info("📋 스케줄 등록 시작")
        logger.info("━" * 52)

        dispatch = {
            "daily":                   self._add_daily,
            "weekly":                  self._add_weekly,
            "monthly":                 self._add_monthly,
            "monthly_last_weekday":    self._add_monthly_last_weekday,
            "monthly_nth_weekday":     self._add_monthly_nth_weekday,
            "quarterly_first_monday":  self._add_quarterly_first_monday,
            "biweekly":                self._add_biweekly,
            "nweekly":                 self._add_nweekly,
            "specific":                self._add_specific,
            "mission":                 self._add_mission,
        }

        for s in self.config.get("schedules", []):
            name  = s.get("name", s.get("id", "?"))
            if not s.get("enabled", True):
                logger.info(f"  ⏸  비활성화 스킵: [{name}]")
                continue
            stype   = s.get("type", "")
            handler = dispatch.get(stype)
            try:
                if handler:
                    handler(s)
                else:
                    logger.error(f"  ❌ 알 수 없는 타입: '{stype}' [{name}]")
            except Exception as e:
                logger.error(f"  ❌ 등록 실패: [{name}] → {e}")

        # ── 스케줄 모니터링 job 등록 ──────────────────────────────────────────
        self._add_monitor()
        logger.info("━" * 52)

    def _add_monitor(self):
        """
        스케줄 미실행 감지 모니터링 job을 등록합니다.
        평일 18:00 KST 에 실행되어 당일 미실행 스케줄을 Slack으로 알립니다.
        config.json 에 monitor_alert_channel 이 없으면 등록은 하되 실행 시 건너뜁니다.
        """
        config = self.config

        def monitor_job():
            sm.check_and_alert(config, self.sender.client)

        monitor_job.__name__ = "schedule-monitor"
        try:
            self.scheduler.add_job(
                monitor_job,
                trigger = CronTrigger(
                    day_of_week = "mon-fri",
                    hour        = 18,
                    minute      = 0,
                    timezone    = self.tz,
                ),
                id                 = "schedule-monitor",
                name               = "스케줄 모니터링",
                misfire_grace_time = 3600,  # Railway 재시작 시 18:00 이후 1시간 이내라면 즉시 실행
            )
            logger.info("  ✅ 등록: [스케줄 모니터링]  (평일 18:00)")
        except Exception as e:
            logger.error(f"  ❌ 모니터링 등록 실패: {e}")

    def print_schedule(self):
        """등록된 스케줄 및 다음 실행 시각 출력"""
        jobs = self.scheduler.get_jobs()
        print("\n" + "=" * 60)
        print("  📅  등록된 알림 스케줄")
        print("=" * 60)
        if not jobs:
            print("  등록된 스케줄이 없습니다.")
        else:
            for job in jobs:
                nxt     = getattr(job, 'next_run_time', None)
                nxt_str = nxt.strftime("%Y-%m-%d %H:%M %Z") if nxt else "없음"
                print(f"  • {job.name}")
                print(f"    └ 다음 실행: {nxt_str}")
        print("=" * 60 + "\n")

    def _notify_startup(self):
        """
        Railway 재시작 시 monitor_alert_channel 에 알림을 발송합니다.
        재시작 시각을 Slack으로 즉시 알림 → 조기 감지 + 수동 대응 가능.
        """
        alert_channel = self.config.get("monitor_alert_channel")
        if not alert_channel:
            return
        try:
            now_kst = datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")
            self.sender.client.chat_postMessage(
                channel = alert_channel,
                text    = f"[Railway] 스케줄러 재시작 — {now_kst} KST\n"
                          f"misfire_grace_time=3600 적용 중 (재시작 후 1시간 이내 스케줄 자동 발송)",
            )
            logger.info(f"[startup] Railway 재시작 알림 발송 → {alert_channel}")
        except Exception as e:
            logger.warning(f"[startup] 재시작 알림 발송 실패: {e}")

    def recover_missed(self):
        """시작 시 오늘 누락된 스케줄을 감지하고 즉시 재발송 (스케줄러 없이 독립 실행 가능)"""
        return sm.recover_missed_schedules(self.config, self.sender)

    def start(self):
        """스케줄 등록 후 백그라운드 실행 (논블로킹 — Bolt 와 공존 가능)"""
        self.setup()
        self.print_schedule()
        logger.info("슬랙 알림 봇 스케줄러 시작 (백그라운드)")
        self.scheduler.start()
        self._notify_startup()   # Railway 재시작 알림 (monitor_alert_channel)
        self.recover_missed()    # 시작 시 누락 스케줄 자동 복구

    def shutdown(self):
        """스케줄러 중지"""
        self.scheduler.shutdown(wait=False)
        logger.info("스케줄러 종료")
