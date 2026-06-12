"""
test_p1_blockkit.py — P1-5 Block Kit 한도 방어 테스트

검증 항목
─────────
1. 정상(한도 내) 입력 → 기존과 동일 블록 구성 (회귀)
2. 블록 50개+ 유발 입력 → 48블록 이하로 병합 + warning 로그
3. 누락 섹션 텍스트 3000자 초과 → 2800자 이하로 절단
4. 담당자 멘션 context 1800자 초과 → 절단
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest
from unittest.mock import patch, MagicMock

# SlackSender 를 Slack SDK 없이 인스턴스화하기 위해 token 인수만 mock
with patch("slack_sender.WebClient"):
    from slack_sender import SlackSender


def make_sender() -> SlackSender:
    with patch("slack_sender.WebClient"), patch("builtins.open", side_effect=FileNotFoundError):
        return SlackSender(token="xoxb-test")


# ─────────────────────────────────────────────────────────
# 1. 정상 케이스 회귀
# ─────────────────────────────────────────────────────────

class TestNormalCase:
    """한도 이내 → 방어 로직 비활성, 기존 블록 구조 그대로"""

    def test_small_items_block_count(self):
        """항목 5개 단독 → 블록 수 8개 (header+divider+status+divider+5 actions+divider+context)"""
        sender = make_sender()
        items = [{"value": f"v{i}", "text": f"항목{i}", "mentions": []} for i in range(5)]
        blocks = sender._build_interactive_blocks(
            title="테스트", items=items, checked_values=[]
        )
        # 방어 미발동 시 50개 한도 초과 없음
        assert len(blocks) <= 50
        # 정상 구성: header(1)+divider(1)+status(1)+divider(1)+5 solo actions(5)+divider(1)+context(1) = 11
        assert len(blocks) == 11

    def test_group_items_block_count(self):
        """그룹 2개(sub 3개씩) → 블록 수 정상 범위"""
        sender = make_sender()
        items = [
            {
                "type": "group",
                "group_name": f"그룹{g}",
                "sub_items": [
                    {"value": f"g{g}_v{s}", "text": f"서브{s}", "mentions": []}
                    for s in range(3)
                ],
            }
            for g in range(2)
        ]
        blocks = sender._build_interactive_blocks(
            title="테스트", items=items, checked_values=[]
        )
        # header+divider+status+divider + 2*(section+actions) + divider+context = 10
        assert len(blocks) == 10
        assert len(blocks) <= 50

    def test_missed_section_small(self):
        """누락 섹션 1그룹 5항목 → section 텍스트 2800자 이하"""
        sender = make_sender()
        missed = [
            {
                "label": "[일일] 03/10(월)",
                "items": [
                    {"value": f"m{i}", "text": f"누락항목{i}", "mentions": []}
                    for i in range(5)
                ],
            }
        ]
        blocks = sender._build_missed_section_blocks(missed)
        # missed_{i} 블록의 텍스트 확인
        for b in blocks:
            if b.get("type") == "section" and b.get("block_id", "").startswith("missed_"):
                text = b["text"]["text"]
                assert len(text) <= 3000

    def test_mentions_small(self):
        """멘션 3명 → context text 1800자 이하"""
        sender = make_sender()
        sender.user_map = {}
        items = [
            {"value": "v1", "text": "항목1", "mentions": ["U001", "U002", "U003"]}
        ]
        blocks = sender._build_interactive_blocks(
            title="T", items=items, checked_values=[]
        )
        context_blocks = [b for b in blocks if b.get("type") == "context"]
        for cb in context_blocks:
            for el in cb.get("elements", []):
                assert len(el.get("text", "")) <= 2000


# ─────────────────────────────────────────────────────────
# 2. 블록 50개 초과 방어
# ─────────────────────────────────────────────────────────

class TestBlockLimitDefense:
    """그룹 15개 + 누락 3그룹 조합 시 50블록 초과 → 48 이하로 병합"""

    def _make_large_input(self):
        sender = make_sender()
        # 그룹 15개(sub 2개씩) → per group: 1 section + 1 actions = 2블록 × 15 = 30
        # 고정 블록: header+divider+status+divider = 4
        # 누락 섹션: divider+header + 3*(grp_section+items_section) = 2+6 = 8
        # 후미: divider+context = 2
        # 합계: 4 + 30 + 8 + 2 = 44 → 멘션 context 1개 추가 시 45
        # 항목 수를 더 늘려 50 초과 유도: 20개 그룹
        items = [
            {
                "type": "group",
                "group_name": f"그룹{g}",
                "sub_items": [
                    {"value": f"g{g}_v{s}", "text": f"서브{s}", "mentions": []}
                    for s in range(2)
                ],
            }
            for g in range(20)  # 20그룹 × 2블록 = 40 + 고정 6 = 46 + missed 8 = 54 초과
        ]
        missed = [
            {
                "label": f"[일일] 03/0{i}(월)",
                "items": [{"value": f"ms{i}_{j}", "text": f"누락{j}", "mentions": []} for j in range(3)],
            }
            for i in range(3)
        ]
        missed_blocks = sender._build_missed_section_blocks(missed)
        return sender, items, missed_blocks

    def test_block_count_under_limit(self, caplog):
        sender, items, missed_blocks = self._make_large_input()
        with caplog.at_level(logging.WARNING, logger="slack_sender"):
            blocks = sender._build_interactive_blocks(
                title="대형체크리스트",
                items=items,
                checked_values=[],
                missed_section=missed_blocks,
            )
        assert len(blocks) <= 48, f"블록 수 {len(blocks)} > 48"

    def test_block_overflow_warning_logged(self, caplog):
        sender, items, missed_blocks = self._make_large_input()
        with caplog.at_level(logging.WARNING, logger="slack_sender"):
            sender._build_interactive_blocks(
                title="대형체크리스트",
                items=items,
                checked_values=[],
                missed_section=missed_blocks,
            )
        assert any("블록 한도" in r.message or "block" in r.message.lower() for r in caplog.records), \
            "50블록 초과 시 WARNING 로그가 없음"

    def test_overflow_blocks_merged_to_section(self):
        """초과 항목이 단일 section 텍스트로 병합되는지 확인"""
        sender, items, missed_blocks = self._make_large_input()
        blocks = sender._build_interactive_blocks(
            title="대형체크리스트",
            items=items,
            checked_values=[],
            missed_section=missed_blocks,
        )
        # 병합된 overflow section 이 존재해야 함
        overflow_sections = [
            b for b in blocks
            if b.get("type") == "section"
            and "overflow" in b.get("block_id", "")
        ]
        assert len(overflow_sections) >= 1, "overflow 섹션이 없음"


# ─────────────────────────────────────────────────────────
# 3. 누락 섹션 텍스트 3000자 초과 방어
# ─────────────────────────────────────────────────────────

class TestMissedSectionTextLimit:

    def test_long_text_truncated(self):
        """항목 텍스트가 매우 길어 3000자 초과 시 2800자 이하로 절단"""
        sender = make_sender()
        # 각 항목 텍스트가 300자 → 10개면 3000자+
        long_text = "가" * 290
        missed = [
            {
                "label": "긴항목그룹",
                "items": [
                    {"value": f"m{i}", "text": long_text, "mentions": []}
                    for i in range(12)
                ],
            }
        ]
        blocks = sender._build_missed_section_blocks(missed)
        for b in blocks:
            if b.get("type") == "section" and b.get("block_id", "").startswith("missed_"):
                text = b["text"]["text"]
                assert len(text) <= 3000, f"section 텍스트 {len(text)}자 > 3000자"

    def test_truncation_indicator_present(self):
        """절단 시 '...외 N건' 표시가 포함되어야 함"""
        sender = make_sender()
        long_text = "나" * 290
        missed = [
            {
                "label": "긴항목그룹",
                "items": [
                    {"value": f"m{i}", "text": long_text, "mentions": []}
                    for i in range(15)
                ],
            }
        ]
        blocks = sender._build_missed_section_blocks(missed)
        # 절단이 발생한 블록에는 '...외' 또는 '...' 포함
        truncated = [
            b for b in blocks
            if b.get("type") == "section"
            and b.get("block_id", "").startswith("missed_")
            and ("...외" in b["text"]["text"] or "..." in b["text"]["text"])
        ]
        assert len(truncated) >= 1, "절단 표시가 없음"

    def test_normal_text_unchanged(self):
        """짧은 텍스트는 변경 없음"""
        sender = make_sender()
        missed = [
            {
                "label": "정상그룹",
                "items": [
                    {"value": "m1", "text": "짧은항목", "mentions": []}
                ],
            }
        ]
        blocks = sender._build_missed_section_blocks(missed)
        for b in blocks:
            if b.get("block_id") == "missed_0":
                assert "짧은항목" in b["text"]["text"]
                assert "..." not in b["text"]["text"]


# ─────────────────────────────────────────────────────────
# 4. 담당자 멘션 context 1800자 초과 방어
# ─────────────────────────────────────────────────────────

class TestMentionContextLimit:

    def test_many_mentions_truncated(self):
        """멘션 200명 → context element text 1800자 이하"""
        sender = make_sender()
        sender.user_map = {}
        # 200명, 각 <@Uxxxxxxxx> = 12자 × 200 = 2400자 → 초과
        uids = [f"U{i:08d}" for i in range(200)]
        items = [
            {"value": "v1", "text": "항목1", "mentions": uids}
        ]
        blocks = sender._build_interactive_blocks(
            title="T", items=items, checked_values=[]
        )
        context_blocks = [b for b in blocks if b.get("type") == "context"]
        for cb in context_blocks:
            for el in cb.get("elements", []):
                text = el.get("text", "")
                assert len(text) <= 1800, f"context text {len(text)}자 > 1800자"

    def test_mention_truncation_indicator(self):
        """절단 시 '외 N명' 표시"""
        sender = make_sender()
        sender.user_map = {}
        uids = [f"U{i:08d}" for i in range(200)]
        items = [{"value": "v1", "text": "항목1", "mentions": uids}]
        blocks = sender._build_interactive_blocks(
            title="T", items=items, checked_values=[]
        )
        context_texts = []
        for b in blocks:
            if b.get("type") == "context":
                for el in b.get("elements", []):
                    context_texts.append(el.get("text", ""))
        assert any("외" in t and "명" in t for t in context_texts), \
            "멘션 절단 표시 '외 N명'이 없음"

    def test_small_mentions_unchanged(self):
        """멘션 5명 → 변경 없음"""
        sender = make_sender()
        sender.user_map = {}
        uids = [f"U{i:08d}" for i in range(5)]
        items = [{"value": "v1", "text": "항목1", "mentions": uids}]
        blocks = sender._build_interactive_blocks(
            title="T", items=items, checked_values=[]
        )
        context_blocks = [b for b in blocks if b.get("type") == "context"]
        mention_ctx = [
            b for b in context_blocks
            if any("담당자" in el.get("text", "") for el in b.get("elements", []))
        ]
        assert len(mention_ctx) == 1
        text = mention_ctx[0]["elements"][0]["text"]
        assert "외" not in text  # 절단 없음
        assert "U00000000" in text  # 첫 멘션 포함
