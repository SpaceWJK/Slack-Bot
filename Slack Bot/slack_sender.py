"""
slack_sender.py - Slack Web API 래퍼

필요 권한:
  chat:write           - 메시지 전송 (필수)
  chat:write.customize - 봇 이름/이모지 커스터마이즈 (선택)
  channels:read        - 공개 채널 목록
  channels:history     - 공개 채널 히스토리 읽기 (체크리스트 누락 폴백)
  groups:read          - 비공개 채널 목록
  groups:history       - 비공개 채널 히스토리·스레드 읽기 (미션 진행율 폴백, 필수)
  users:read           - 사용자 검색 (--find-user 용)
"""

import json
import logging
import os
import re
import time
from datetime import datetime, date as _date

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackSender:

    def __init__(self, token: str):
        self.client = WebClient(token=token)

        # config.json 의 user_map 로드 (UID → 표시 이름)
        self.user_map: dict = {}
        try:
            _base     = os.path.dirname(os.path.abspath(__file__))
            _cfg_path = os.path.join(_base, "config.json")
            with open(_cfg_path, "r", encoding="utf-8") as _f:
                self.user_map = json.load(_f).get("user_map", {})
        except Exception as e:
            logger.warning(f"user_map 로드 실패 (무시): {e}")

    # ──────────────────────────────────────────────────────────
    # Block Kit 빌더 — 일반 (text / checklist)
    # ──────────────────────────────────────────────────────────

    def _timestamp_block(self) -> dict:
        return {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  자동 알림",
            }],
        }

    def _build_text_blocks(self, message: str) -> list:
        return [
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            self._timestamp_block(),
        ]

    def _build_checklist_blocks(self, title: str, items: list) -> list:
        """정적(비인터랙티브) 체크리스트 — ☐ 기호 사용"""
        lines = "\n".join(f"☐  {item}" for item in items)
        return [
            {"type": "header",  "text": {"type": "plain_text", "text": title, "emoji": True}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": lines}},
            {"type": "divider"},
            self._timestamp_block(),
        ]

    # ──────────────────────────────────────────────────────────
    # Block Kit 빌더 — 인터랙티브 체크리스트
    # ──────────────────────────────────────────────────────────

    def _count_tasks(self, items: list, checked_set: set):
        """
        그룹/단독 항목 기준으로 (전체 태스크 수, 완료 태스크 수)를 반환합니다.

        - group 항목: 모든 sub_items 가 checked_set 에 있어야 완료
        - 단독 항목: value 가 checked_set 에 있으면 완료
        """
        total, done = 0, 0
        for item in items:
            total += 1
            if item.get("type") == "group":
                sub_values = [s["value"] for s in item.get("sub_items", [])]
                if sub_values and all(v in checked_set for v in sub_values):
                    done += 1
            else:
                if item.get("value", "") in checked_set:
                    done += 1
        return total, done

    # section 텍스트 한도 (Slack 공식 3000자, 안전 여유 200자)
    _SECTION_TEXT_LIMIT = 2800

    def _truncate_lines(self, lines: list, limit: int) -> str:
        """
        lines 를 '\n'.join 했을 때 limit 자를 초과하면 절단 후 '...외 N건' 추가.
        한도 이내면 그대로 join 반환.
        """
        result = "\n".join(lines)
        if len(result) <= limit:
            return result
        # 한 줄씩 추가하면서 한도 체크
        kept: list = []
        for line in lines:
            candidate = "\n".join(kept + [line])
            if len(candidate) > limit - 20:  # '...외 NN건' 공간 확보
                break
            kept.append(line)
        omitted = len(lines) - len(kept)
        return "\n".join(kept) + f"\n...외 {omitted}건"

    def _build_missed_section_blocks(self, missed_items: list, action_id: str = "checklist_toggle") -> list:
        """
        누락 체크리스트 섹션 블록 생성 (텍스트 리스트 형식, 체크박스 없음).

        Parameters
        ----------
        missed_items : get_missed_items() 반환값
            [{"label": "[일일] 03/04(화)", "items": [{"value":"missed_0_x","text":"...","mentions":[...]}]}, ...]

        Returns: Slack Block Kit 블록 목록
            - block_id="missed_divider"  → 구분자 (sentinel: 로컬 봇이 섹션 위치 식별용)
            - block_id="missed_header"   → "⚠️ 전일 누락 항목" 헤더
            - block_id=f"missed_grp_{i}" → 그룹 레이블 (스케줄별)
            - block_id=f"missed_{i}"     → 누락 항목 텍스트 목록 (체크박스 없음)

        방어: section 텍스트 2800자 초과 시 '...외 N건' 절단.
        """
        blocks = [
            {"type": "divider", "block_id": "missed_divider"},
            {
                "type":     "section",
                "block_id": "missed_header",
                "text": {"type": "mrkdwn", "text": "⚠️ *전일 누락 항목*"},
            },
        ]

        for i, group in enumerate(missed_items):
            label       = group.get("label", "")
            group_items = group.get("items", [])

            # 그룹 레이블 (예: 📋 [일일] 03/09(월))
            blocks.append({
                "type":     "section",
                "block_id": f"missed_grp_{i}",
                "text": {"type": "mrkdwn", "text": f"*📋 {label}*"},
            })

            # 텍스트 리스트 (체크박스 불필요: 조회/확인 목적)
            lines: list = []
            for item in group_items:
                names = [self.user_map.get(uid, "") for uid in item.get("mentions", [])]
                names = [n for n in names if n]
                mention_str = ("  담당: " + ", ".join(names)) if names else ""
                lines.append(f"• *{item['text']}*{mention_str}")

            if lines:
                text = self._truncate_lines(lines, self._SECTION_TEXT_LIMIT)
                blocks.append({
                    "type":     "section",
                    "block_id": f"missed_{i}",
                    "text":     {"type": "mrkdwn", "text": text},
                })

        return blocks

    def _rebuild_missed_blocks_checked(
        self, raw_blocks: list, checked_set: set, action_id: str = "checklist_toggle"
    ) -> list:
        """
        메시지에서 추출한 누락 섹션 raw blocks 의 initial_options 를
        현재 checked_set 기준으로 갱신합니다.

        Parameters
        ----------
        raw_blocks  : 현재 메시지에서 추출한 누락 섹션 블록 목록
        checked_set : 현재 전체 체크 상태 (일반 + missed_ 값 모두 포함)

        Returns: initial_options 갱신된 블록 목록
        """
        result: list = []
        for block in raw_blocks:
            if block.get("type") != "actions":
                result.append(block)
                continue

            new_elements: list = []
            for elem in block.get("elements", []):
                if elem.get("type") != "checkboxes":
                    new_elements.append(elem)
                    continue

                options = elem.get("options", [])
                initial = [opt for opt in options if opt["value"] in checked_set]
                # initial_options, action_id 제거 후 재설정 (action_id 는 동적으로 교체)
                new_elem = {k: v for k, v in elem.items() if k not in ("initial_options", "action_id")}
                new_elem["action_id"] = action_id
                if initial:
                    new_elem["initial_options"] = initial
                new_elements.append(new_elem)

            result.append({**block, "elements": new_elements})
        return result

    @staticmethod
    def _compute_period_label(schedule_type: str, dt: "datetime | None" = None) -> str:
        """
        스케줄 타입에 따른 기간 레이블 반환.

        - weekly : "2026년 3월 2주차" (월 내 몇 번째 주인지 표시)
        - 그 외  : "2026년 3월"
        """
        now = dt or datetime.now()
        if schedule_type == "weekly":
            week_of_month = (now.day - 1) // 7 + 1
            return f"{now.year}년 {now.month}월 {week_of_month}주차"
        return f"{now.year}년 {now.month}월"

    def _build_interactive_blocks(
        self,
        title: str,
        items: list,
        checked_values: list,
        sent_at: str = "",
        missed_section: list = None,
        action_id: str = "checklist_toggle",
        period_label: str = None,
    ) -> list:
        """
        Slack Block Kit 인터랙티브 체크리스트 블록 구성

        Parameters
        ----------
        title          : 헤더 제목 문자열
        items          : 체크리스트 항목 목록
                         단독: {"value": "v", "text": "작업명", "mentions": [...]}
                         그룹: {"type": "group", "group_name": "...",
                                "sub_items": [{"value": ..., "text": ..., "mentions": ...}, ...]}
        checked_values : 현재 체크된 value 목록
        sent_at        : 최초 발송 시각 문자열 (context 블록 표시용)
        period_label   : 진행 상태 헤더에 표시할 기간 레이블
                         (None 이면 "YYYY년 M월" 자동 생성)
        """
        checked_set         = set(checked_values)
        total, done         = self._count_tasks(items, checked_set)
        filled              = int((done / total) * 10) if total > 0 else 0
        bar                 = "▓" * filled + "░" * (10 - filled)

        now = datetime.now()
        if period_label is None:
            period_label = f"{now.year}년 {now.month}월"
        status_text  = f"*{period_label}*  {bar}  {done}/{total} 완료"
        if done == total and total > 0:
            status_text += "  🎉 *모두 완료!*"

        context_text = f"⏰ 발송: {sent_at or now.strftime('%Y-%m-%d %H:%M')}  |  자동 알림"

        # ── 담당자 멘션 수집 ────────────────────────────────────────────────
        # 체크박스 옵션 텍스트 내 <@U...> 는 시각적 표시만 되고 Slack 알림 미발송.
        # section/context 블록의 <@U...> 만 실제 멘션 알림을 트리거합니다.
        unique_mentions: list = []
        seen_uids: set        = set()
        for item in items:
            src_list = item.get("sub_items", []) if item.get("type") == "group" else [item]
            for src in src_list:
                for uid in src.get("mentions", []):
                    if uid not in seen_uids:
                        seen_uids.add(uid)
                        unique_mentions.append(f"<@{uid}>")

        # ── 헤더 + 진행 상태 블록 ────────────────────────────────────────────
        blocks: list = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": title, "emoji": True},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": status_text},
            },
        ]

        # 담당자 멘션 블록 — chat.update 시 재알림 없으므로 중복 걱정 불필요
        # 방어: context elements[].text 1800자(Slack 한도 2000자, 안전 여유 200자) 초과 시 '외 N명' 절단
        if unique_mentions:
            _MENTION_LIMIT = 1800
            prefix = "📌 담당자  "
            mention_text = prefix + "  ".join(unique_mentions)
            if len(mention_text) > _MENTION_LIMIT:
                kept_mentions: list = []
                for m in unique_mentions:
                    candidate = prefix + "  ".join(kept_mentions + [m])
                    if len(candidate) > _MENTION_LIMIT - 15:  # '외 NNN명' 공간 확보
                        break
                    kept_mentions.append(m)
                omitted_count = len(unique_mentions) - len(kept_mentions)
                mention_text = prefix + "  ".join(kept_mentions) + f"  외 {omitted_count}명"
                logger.warning(
                    f"담당자 멘션 텍스트 한도 초과 → {omitted_count}명 절단 "
                    f"(원본 {len(unique_mentions)}명, 유지 {len(kept_mentions)}명)"
                )
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": mention_text}],
            })

        blocks.append({"type": "divider"})

        # ── 항목별 블록 빌드 ─────────────────────────────────────────────────
        for i, item in enumerate(items):
            if item.get("type") == "group":
                group_name = item.get("group_name", f"그룹 {i + 1}")
                sub_items  = item.get("sub_items", [])

                # 그룹 제목 섹션
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*{group_name}*"},
                })

                # 서브 아이템 → 체크박스 옵션 목록
                options: list         = []
                initial_options: list = []
                for sub in sub_items:
                    val      = sub["value"]
                    text     = sub["text"]
                    names    = [self.user_map.get(uid, "") for uid in sub.get("mentions", [])]
                    names    = [n for n in names if n]
                    mention_str = ("  담당: " + ", ".join(names)) if names else ""

                    opt = {
                        "text":  {"type": "mrkdwn", "text": f"*{text}*{mention_str}"},
                        "value": val,
                    }
                    options.append(opt)
                    if val in checked_set:
                        initial_options.append(opt)

                checkbox_elem = {
                    "type":      "checkboxes",
                    "action_id": action_id,
                    "options":   options,
                }
                if initial_options:
                    checkbox_elem["initial_options"] = initial_options

                blocks.append({
                    "type":     "actions",
                    "block_id": f"chk_grp_{i}",
                    "elements": [checkbox_elem],
                })

            else:
                # 단독 항목
                val      = item["value"]
                text     = item["text"]
                names    = [self.user_map.get(uid, "") for uid in item.get("mentions", [])]
                names    = [n for n in names if n]
                mention_str = ("  담당: " + ", ".join(names)) if names else ""

                opt = {
                    "text":  {"type": "mrkdwn", "text": f"*{text}*{mention_str}"},
                    "value": val,
                }
                checkbox_elem = {
                    "type":      "checkboxes",
                    "action_id": action_id,
                    "options":   [opt],
                }
                if val in checked_set:
                    checkbox_elem["initial_options"] = [opt]

                blocks.append({
                    "type":     "actions",
                    "block_id": f"chk_solo_{i}",
                    "elements": [checkbox_elem],
                })

        # ── 전일 누락 섹션 삽입 (있을 때만) ────────────────────────────────
        if missed_section:
            blocks.extend(missed_section)

        blocks.extend([
            {"type": "divider"},
            {
                "type":     "context",
                "elements": [{"type": "mrkdwn", "text": context_text}],
            },
        ])

        # ── 블록 48개 한도 방어 (Slack 상한 50개, 안전 여유 2개) ────────────
        # 초과 시: 후미 고정 블록(divider+context) 보존 후 중간 항목을 단일 section 텍스트로 병합.
        _BLOCK_LIMIT = 48
        if len(blocks) > _BLOCK_LIMIT:
            # 후미 2개(divider, context)는 항상 보존
            tail = blocks[-2:]
            # 앞 고정 헤더 블록: header(0), divider(1), status(2) = 인덱스 0~2 보존
            # 담당자 context 가 있으면 3번 인덱스까지 보존 (type=context)
            head_end = 3
            if len(blocks) > 3 and blocks[3].get("type") == "context":
                head_end = 4
            # head 뒤에 divider 1개
            if len(blocks) > head_end and blocks[head_end].get("type") == "divider":
                head_end += 1
            head = blocks[:head_end]

            # 나머지 중간 블록들 (tail 제외)
            middle = blocks[head_end:-2]

            # 중간 블록에서 텍스트 추출하여 단일 section 으로 병합
            overflow_lines: list = []
            for b in middle:
                txt = b.get("text", {}).get("text", "") if b.get("type") == "section" else ""
                if txt and not txt.startswith("*") or (txt.startswith("*") and len(txt) < 100):
                    # 그룹명·상태 등 짧은 제목 포함
                    overflow_lines.append(txt)
                elif b.get("type") == "actions":
                    # 체크박스 actions → 옵션 텍스트 나열
                    for elem in b.get("elements", []):
                        for opt in elem.get("options", []):
                            overflow_lines.append("• " + opt.get("text", {}).get("text", ""))

            overflow_text = self._truncate_lines(overflow_lines, self._SECTION_TEXT_LIMIT) if overflow_lines else "항목 목록 (한도 초과로 요약)"
            overflow_block = {
                "type":     "section",
                "block_id": "overflow_items",
                "text":     {"type": "mrkdwn", "text": overflow_text},
            }

            logger.warning(
                f"블록 한도 초과: {len(blocks)}개 → {_BLOCK_LIMIT}개 이하로 병합 "
                f"(중간 {len(middle)}개 블록을 1개 section 으로 축약)"
            )
            blocks = head + [overflow_block] + tail

        return blocks

    # ──────────────────────────────────────────────────────────
    # 템플릿 변수 치환
    # ──────────────────────────────────────────────────────────

    def _resolve_templates(self, text: str) -> str:
        """
        메시지 문자열의 템플릿 변수를 현재 날짜 정보로 치환합니다.

        지원 변수
        ---------
        {date}  →  MM.DD(요일)  예: 03.05(목)
        """
        now    = datetime.now()
        day_kr = ["월", "화", "수", "목", "금", "토", "일"][now.weekday()]
        return text.replace("{date}", now.strftime("%m.%d") + f"({day_kr})")

    # ──────────────────────────────────────────────────────────
    # 메시지 전송 — 일반
    # ──────────────────────────────────────────────────────────

    def send(self, channel: str, schedule: dict) -> bool:
        """
        schedule dict 기반으로 메시지 전송
        message_type: 'text' | 'checklist'
        """
        msg_type = schedule.get("message_type", "text")

        if msg_type == "checklist":
            blocks        = self._build_checklist_blocks(
                title = schedule.get("title", "📋 체크리스트"),
                items = schedule.get("items", []),
            )
            fallback_text = schedule.get("title", "체크리스트 알림")
        else:
            message       = self._resolve_templates(schedule.get("message", ""))
            blocks        = self._build_text_blocks(message)
            fallback_text = message[:100] if message else "알림"

        kwargs = {
            "channel": channel,
            "text":    fallback_text,
            "blocks":  blocks,
        }
        if "bot_name"  in schedule: kwargs["username"]   = schedule["bot_name"]
        if "bot_emoji" in schedule: kwargs["icon_emoji"] = schedule["bot_emoji"]

        try:
            response = self.client.chat_postMessage(**kwargs)
            logger.info(
                f"✅ 전송 완료 | [{schedule.get('name', '알림')}] → "
                f"채널: {channel} | ts: {response['ts']}"
            )
            return True
        except SlackApiError as e:
            logger.error(
                f"❌ 전송 실패 | [{schedule.get('name', '알림')}] "
                f"오류: {e.response['error']}"
            )
            return False

    # ──────────────────────────────────────────────────────────
    # 메시지 전송 — 인터랙티브 체크리스트
    # ──────────────────────────────────────────────────────────

    def send_interactive_checklist(
        self, channel: str, schedule: dict, missed_items: list = None
    ):
        """
        인터랙티브 체크리스트 메시지를 전송합니다.

        Parameters
        ----------
        missed_items : 전일 누락 항목 목록 (missed_tracker.get_missed_items() 반환값)
                       없거나 빈 리스트면 누락 섹션 미표시.

        Returns
        -------
        str  : 전송된 메시지의 ts (타임스탬프). 실패 시 None.
        """
        now      = datetime.now()
        sent_at  = now.strftime("%Y-%m-%d %H:%M")
        period_label = self._compute_period_label(schedule.get("type", ""), now)

        # 누락 섹션 블록 빌드 (있을 때만)
        missed_section = (
            self._build_missed_section_blocks(missed_items)
            if missed_items else None
        )

        blocks  = self._build_interactive_blocks(
            title          = schedule.get("title", "📋 체크리스트"),
            items          = schedule.get("items", []),
            checked_values = [],
            sent_at        = sent_at,
            missed_section = missed_section,
            period_label   = period_label,
        )

        kwargs = {
            "channel": channel,
            "text":    schedule.get("title", "월간 체크리스트"),
            "blocks":  blocks,
        }
        if "bot_name"  in schedule: kwargs["username"]   = schedule["bot_name"]
        if "bot_emoji" in schedule: kwargs["icon_emoji"] = schedule["bot_emoji"]

        try:
            res = self.client.chat_postMessage(**kwargs)
            ts  = res["ts"]
            logger.info(
                f"✅ 인터랙티브 체크리스트 전송 | "
                f"[{schedule.get('name', '알림')}] → 채널: {channel} | ts: {ts}"
            )
            return ts
        except SlackApiError as e:
            logger.error(
                f"❌ 인터랙티브 체크리스트 전송 실패 | "
                f"[{schedule.get('name', '알림')}] 오류: {e.response['error']}"
            )
            return None

    def update_interactive_checklist(
        self, channel: str, ts: str, state: dict, missed_section: list = None
    ) -> bool:
        """
        체크 상태 변경 후 기존 메시지를 chat.update 로 갱신합니다.

        Parameters
        ----------
        channel        : Slack 채널 ID
        ts             : 원본 메시지 타임스탬프
        state          : interaction_handler.update_checked() 의 반환값
        missed_section : 현재 메시지에서 추출한 전일 누락 섹션 raw blocks
                         (None 이면 누락 섹션 미포함)
        """
        # ── 동적 action_id 생성 ─────────────────────────────────────────────────
        # chat.update 마다 action_id 를 바꾸면 Slack 클라이언트가 해당 체크박스를
        # "새 컴포넌트"로 인식해 initial_options 기준으로 강제 재렌더링합니다.
        # → A 가 체크한 뒤 B 의 화면도 즉시 갱신되는 핵심 메커니즘.
        dyn_action_id = f"checklist_toggle_{int(time.time() * 1000)}"

        # ── period_label 복원 ────────────────────────────────────────────────
        # 최초 발송 시각(sent_at)과 스케줄 타입으로 주차 레이블을 재계산합니다.
        # 예: weekly → "2026년 3월 2주차" (체크 업데이트 후에도 주차 유지)
        schedule_type = state.get("schedule_type", "")
        sent_at_str   = state.get("sent_at", "")
        try:
            sent_dt = datetime.strptime(sent_at_str, "%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            sent_dt = None
        period_label = self._compute_period_label(schedule_type, sent_dt)

        rebuilt_missed = (
            self._rebuild_missed_blocks_checked(
                missed_section, set(state.get("checked", [])), dyn_action_id
            )
            if missed_section else None
        )

        blocks = self._build_interactive_blocks(
            title          = state.get("title", "📋 체크리스트"),
            items          = state["items"],
            checked_values = state.get("checked", []),
            sent_at        = state.get("sent_at", ""),
            missed_section = rebuilt_missed,
            action_id      = dyn_action_id,
            period_label   = period_label,
        )
        try:
            self.client.chat_update(
                channel = channel,
                ts      = ts,
                text    = state.get("title", "월간 체크리스트"),
                blocks  = blocks,
            )
            logger.info(f"✅ 메시지 업데이트 완료 | ts: {ts}  checked={len(state.get('checked', []))}개")
            return True
        except SlackApiError as e:
            logger.error(f"❌ 메시지 업데이트 실패: {e.response['error']}")
            return False

    # ──────────────────────────────────────────────────────────
    # 전일 누락 항목 별도 메시지 전송 (로컬 봇 전용)
    # ──────────────────────────────────────────────────────────

    def send_missed_items_standalone(
        self, channel: str, missed_items: list
    ) -> str | None:
        """
        전일 누락 항목을 별도 메시지로 채널에 전송합니다.

        로컬 봇이 checklist_state.json 기반으로 계산한 누락 항목을
        일일 체크리스트와 분리된 독립 메시지로 보냅니다.

        Parameters
        ----------
        channel      : 대상 채널 ID
        missed_items : missed_tracker.get_missed_items_from_local_state() 반환값
            [{"label": "[일일] 03/10(월)", "items": [{"text":..., "mentions":...}]}, ...]

        Returns
        -------
        str | None : 전송된 메시지 ts (실패 시 None)
        """
        blocks = self._build_missed_section_blocks(missed_items)

        # _build_missed_section_blocks 의 첫 블록은 divider → 별도 메시지에서는 제거
        if blocks and blocks[0].get("block_id") == "missed_divider":
            blocks = blocks[1:]

        total = sum(len(g["items"]) for g in missed_items)

        try:
            result = self.client.chat_postMessage(
                channel = channel,
                text    = f"⚠️ 전일 누락 항목 ({total}건)",
                blocks  = blocks,
            )
            ts = result.get("ts")
            logger.info(f"⚠️ 전일 누락 메시지 전송 완료 | ch={channel} ts={ts} 누락={total}건")
            return ts
        except SlackApiError as e:
            logger.error(f"❌ 전일 누락 메시지 전송 실패: {e.response['error']}")
            return None

    # ──────────────────────────────────────────────────────────
    # 미션 진행 현황 리마인더
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_progress_bar(progress: int, width: int = 10) -> str:
        """진행율을 이모지 막대 그래프로 변환 (예: ██████░░░░ 60%)"""
        filled = max(0, min(width, round(progress / 100 * width)))
        return "█" * filled + "░" * (width - filled)

    @staticmethod
    def _build_mission_blocks(mission: dict, progress: int) -> list:
        """
        미션 진행 현황 블록 생성.

        Parameters
        ----------
        mission  : config.json 의 mission 딕셔너리
        progress : 현재 전체 진행율 (0~100)

        분기
        ----
        - name 이 "미정" 또는 빈 값  → 미션 선정 독려 포맷
        - name 이 확정된 값          → 진행 현황 포맷
        """
        name           = mission.get("name", "")
        channel_name   = mission.get("channel_name", "")
        mission_number = mission.get("mission_number", "")

        # 날짜 문자열 (Windows 호환)
        now       = datetime.now()
        day_names = ["월", "화", "수", "목", "금", "토", "일"]
        date_str  = f"{now.year}년 {now.month}월 {now.day}일 ({day_names[now.weekday()]})"

        footer = {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"📢  #{channel_name}   |   {date_str}   |   자동 알림",
            }],
        }

        # 미션 번호 접두사 (있으면 "[M-01] " 형식)
        num_prefix = f"[{mission_number}] " if mission_number else ""

        # ── 미션 미정 → 선정 독려 포맷 ───────────────────────────────────────
        if not name or name.strip() in ("미정",):
            return [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "📊 미션 진행 현황", "emoji": True},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"⏳  *{num_prefix}미션 선정 대기 중*"},
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "💬  *아직 이 채널의 미션이 정해지지 않았어요!*\n"
                            "     빨리 미션을 확정하고 도전을 시작해 봐요 🔥"
                        ),
                    },
                },
                {"type": "divider"},
                footer,
            ]

        # ── 미션 확정 → 진행 현황 포맷 ───────────────────────────────────────
        target_str = mission.get("target_date", "")

        # D-day 계산 (target_date 가 비어있거나 파싱 불가하면 "미정" 표시)
        try:
            if not target_str:
                raise ValueError("target_date 미지정")
            target    = _date.fromisoformat(target_str)
            today     = _date.today()
            days_left = (target - today).days
            target_display = f"{target.month:02d}.{target.day:02d}"
            if days_left > 0:
                dday = f"D-{days_left}"
            elif days_left == 0:
                dday = "D-DAY 🔥"
            else:
                dday = f"D+{abs(days_left)} (기한초과)"
        except Exception:
            target_display = target_str if target_str else "미정"
            dday = ""

        # 진행 막대
        bar         = SlackSender._make_progress_bar(progress)
        is_complete = progress >= 100

        # 진행율 텍스트
        progress_text = f"`{bar}`  *{progress}%*"
        if is_complete:
            progress_text = f"✅ *미션 완료!*\n`{bar}`  *{progress}%*"

        # 서브 태스크 텍스트 (있을 때만)
        sub_tasks     = mission.get("sub_tasks", [])
        sub_task_text = "\n".join(f"▸  {t}" for t in sub_tasks) if sub_tasks else ""

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "📊 미션 진행 현황", "emoji": True},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*🎯  {num_prefix}{name}*\n"
                        f"📅  목표일: `{target_display}`"
                        + (f"   ⏰  `{dday}`" if dday else "")
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*전체 진행율*\n{progress_text}",
                },
            },
        ]

        # 서브 태스크 섹션 (config 에 sub_tasks 가 있을 때만 추가)
        if sub_task_text:
            blocks += [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Sub Task*\n{sub_task_text}",
                    },
                },
            ]

        # 진행율 댓글 요청 (미완료 시에만 표시)
        if not is_complete:
            blocks += [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "💬  *업무 마감 전, 이 메시지의 스레드에 현재 진행율(%)을 댓글로 남겨주세요!*\n"
                            "> 예시: `현재 진행율 35%`"
                        ),
                    },
                },
            ]

        blocks.append(footer)
        return blocks

    def _read_thread_progress(self, channel: str, ts: str, default: int) -> int:
        """
        지난 미션 메시지의 스레드에서 최신 진행율(%) 파싱.

        담당자가 스레드에 '65%', '진행율 70%' 등 형태로 업데이트하면
        가장 최근 답글에서 숫자% 를 추출합니다.
        파싱 실패 또는 스레드가 없으면 default 값을 그대로 반환합니다.
        """
        try:
            resp     = self.client.conversations_replies(channel=channel, ts=ts)
            messages = resp.get("messages", [])
            # messages[0] = 봇의 원본 메시지, [1:] = 담당자 스레드 답글
            for msg in reversed(messages[1:]):
                text  = msg.get("text", "")
                match = re.search(r'(\d{1,3})\s*%', text)
                if match:
                    pct = int(match.group(1))
                    if 0 <= pct <= 100:
                        logger.info(
                            f"[미션 스레드] 진행율 파싱: {pct}%  "
                            f"(작성자: {msg.get('user', '?')})"
                        )
                        return pct
        except Exception as e:
            logger.warning(f"[미션 스레드] 읽기 실패 → default({default}%) 유지: {e}")
        return default

    def _find_last_mission_ts(self, channel: str, days_back: int = 7) -> str:
        """
        Railway 재배포로 mission_state.json이 초기화된 경우를 위한 폴백.

        채널 히스토리를 직접 스캔하여 봇이 보낸 가장 최근의 미션 알림
        메시지 ts를 반환합니다.

        Parameters
        ----------
        channel   : Slack 채널 ID
        days_back : 몇 일 전까지 소급 탐색할지 (기본 7일)

        Returns
        -------
        str  : 발견된 메시지 ts. 없으면 None.
        """
        oldest = time.time() - days_back * 86400   # days_back일 전 Unix timestamp

        try:
            resp = self.client.conversations_history(
                channel   = channel,
                oldest    = str(oldest),
                limit     = 20,
            )
        except Exception as e:
            logger.warning(f"[미션 폴백] 채널 히스토리 조회 실패 ({channel}): {e}")
            return None

        for msg in resp.get("messages", []):
            for block in msg.get("blocks", []):
                if (
                    block.get("type") == "header"
                    and "미션 진행 현황" in block.get("text", {}).get("text", "")
                ):
                    ts = msg.get("ts")
                    logger.info(
                        f"[미션 폴백] 채널 히스토리에서 이전 미션 ts 발견: "
                        f"{channel} / ts={ts}"
                    )
                    return ts

        logger.info(f"[미션 폴백] 채널 히스토리에서 이전 미션 메시지 없음: {channel}")
        return None

    def _load_mission_state(self) -> dict:
        """mission_state.json 로드 (없으면 빈 dict)"""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mission_state.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.warning(f"[미션 상태] 로드 실패: {e}")
            return {}

    def _save_mission_state(self, state: dict) -> None:
        """mission_state.json 저장"""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mission_state.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[미션 상태] 저장 실패: {e}")

    def send_mission_reminder(self, schedule: dict):
        """
        미션 진행 현황 리마인더 전송.

        흐름
        ----
        1. mission_state.json 에서 이전 ts 및 진행율 로드
        2. 이전 ts 가 있으면 해당 스레드에서 최신 진행율 파싱
        3. 미션 블록 빌드 후 채널에 전송
        4. 새 ts + 진행율을 mission_state.json 에 저장

        Returns
        -------
        str  : 전송된 메시지 ts. 실패 시 None.
        """
        mission        = schedule.get("mission", {})
        channel        = schedule["channel"]
        mission_id     = schedule["id"]
        mission_number = mission.get("mission_number", "")

        # 이전 상태 로드
        all_state = self._load_mission_state()
        ms        = all_state.get(mission_id, {})
        progress  = ms.get("progress", 0)
        last_ts   = ms.get("last_ts")

        # 스레드에서 최신 진행율 업데이트
        # mission_state.json이 Railway 재배포로 소실된 경우 채널 히스토리에서 복원
        if not last_ts:
            last_ts = self._find_last_mission_ts(channel)
        if last_ts:
            progress = self._read_thread_progress(channel, last_ts, progress)

        # 블록 빌드 & 전송
        blocks = self._build_mission_blocks(mission, progress)
        _name  = mission.get("name", "")
        _num   = f"[{mission_number}] " if mission_number else ""
        _fallback_text = (
            f"📊 {_num}미션 진행 현황 (미선정)"
            if not _name or _name.strip() in ("미정",)
            else f"{_num}{_name}"
        )
        kwargs = {
            "channel": channel,
            "text":    _fallback_text,
            "blocks":  blocks,
        }
        if "bot_name"  in schedule: kwargs["username"]   = schedule["bot_name"]
        if "bot_emoji" in schedule: kwargs["icon_emoji"] = schedule["bot_emoji"]

        try:
            res = self.client.chat_postMessage(**kwargs)
            ts  = res["ts"]
            logger.info(
                f"✅ 미션 리마인더 전송 | [{mission_number}] [{schedule.get('name')}] "
                f"→ {channel} | {progress}% | ts: {ts}"
            )
            # 상태 저장
            all_state[mission_id] = {
                "channel":        channel,
                "mission_number": mission_number,
                "last_ts":        ts,
                "progress":       progress,
            }
            self._save_mission_state(all_state)
            return ts
        except SlackApiError as e:
            logger.error(
                f"❌ 미션 리마인더 전송 실패 | "
                f"[{mission_number}] [{schedule.get('name')}] {e.response['error']}"
            )
            return None

    # ──────────────────────────────────────────────────────────
    # 유틸리티
    # ──────────────────────────────────────────────────────────

    def test_connection(self) -> dict:
        """Slack 연결 상태 확인 (auth.test)"""
        try:
            res = self.client.auth_test()
            return {
                "success": True,
                "bot":     res.get("user",   "unknown"),
                "team":    res.get("team",   "unknown"),
                "bot_id":  res.get("bot_id", ""),
            }
        except SlackApiError as e:
            return {"success": False, "error": e.response["error"]}

    def list_channels(self) -> list:
        """
        접근 가능한 채널 목록 반환
        권한: channels:read, groups:read
        반환: [{"id": "C...", "name": "...", "is_private": bool}, ...]
        """
        channels = []
        cursor   = None
        try:
            while True:
                kwargs = {
                    "limit":            200,
                    "types":            "public_channel,private_channel",
                    "exclude_archived": True,
                }
                if cursor:
                    kwargs["cursor"] = cursor
                res = self.client.conversations_list(**kwargs)
                for ch in res.get("channels", []):
                    channels.append({
                        "id":         ch["id"],
                        "name":       ch["name"],
                        "is_private": ch.get("is_private", False),
                    })
                cursor = res.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
        except SlackApiError as e:
            logger.error(f"채널 목록 조회 실패: {e.response['error']}")
        return sorted(channels, key=lambda x: x["name"])

    def find_users(self, query: str) -> list:
        """
        사용자 검색 (이름/표시명 부분 일치)
        권한: users:read
        반환: [{"id": "U...", "real_name": "...", "name": "...", "display_name": "..."}, ...]
        """
        matches = []
        cursor  = None
        q       = query.lower()
        try:
            while True:
                kwargs = {"limit": 200}
                if cursor:
                    kwargs["cursor"] = cursor
                res = self.client.users_list(**kwargs)
                for u in res.get("members", []):
                    if u.get("deleted") or u.get("is_bot"):
                        continue
                    real = u.get("real_name", "")
                    name = u.get("name", "")
                    disp = u.get("profile", {}).get("display_name", "")
                    if q in real.lower() or q in name.lower() or q in disp.lower():
                        matches.append({
                            "id":           u["id"],
                            "real_name":    real,
                            "name":         name,
                            "display_name": disp,
                        })
                cursor = res.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
        except SlackApiError as e:
            logger.error(f"사용자 검색 실패: {e.response['error']}")
        return matches
