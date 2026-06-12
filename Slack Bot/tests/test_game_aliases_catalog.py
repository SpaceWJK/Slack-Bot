# -*- coding: utf-8 -*-
"""game_aliases 카탈로그 전환 골든 테스트 (task-195).

전환 전 하드코딩 GAMES의 동작이 카탈로그 전환 후에도 동일해야 함.
+ 리젝(PRH)은 비게임 — 카탈로그 밖 봇 로컬 항목으로 잔류 (Master 확정 2026-06-11).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from game_aliases import (
    GAMES,
    detect_game_in_text,
    get_jira_project_key,
    get_wiki_ancestor_id,
    get_wiki_path_keywords,
    resolve_game,
)


def test_resolve_all_legacy_aliases():
    """전환 전 별칭 전수 — canonical/jira_key 동일."""
    legacy = {
        # 에픽세븐
        "에픽세븐": ("에픽세븐", "GEP"), "에픽": ("에픽세븐", "GEP"),
        "epic7": ("에픽세븐", "GEP"), "ep7": ("에픽세븐", "GEP"), "gep": ("에픽세븐", "GEP"),
        # 카제나
        "카제나": ("카제나", "GCZ"), "카오스제로": ("카제나", "GCZ"), "cz": ("카제나", "GCZ"),
        "chaos zero nightmare": ("카제나", "GCZ"),
        # 리젝 (봇 로컬)
        "리젝": ("리젝", "PRH"), "reject": ("리젝", "PRH"), "prh": ("리젝", "PRH"),
        # 로드나인 / 아시아
        "로드나인": ("로드나인", "LDN"), "ldn": ("로드나인", "LDN"),
        "로드나인 아시아": ("로드나인 아시아", "LNA"), "lna": ("로드나인 아시아", "LNA"),
    }
    for alias, (canonical, jira_key) in legacy.items():
        info = resolve_game(alias)
        assert info is not None, f"resolve_game({alias!r}) None"
        assert info["canonical"] == canonical
        assert info["jira_key"] == jira_key


def test_detect_longest_match_first():
    assert detect_game_in_text("로드나인 아시아 점검 일정")["canonical"] == "로드나인 아시아"
    assert detect_game_in_text("로드나인 점검 일정")["canonical"] == "로드나인"
    assert detect_game_in_text("에픽세븐 2026년 핫픽스 알려줘")["canonical"] == "에픽세븐"
    assert detect_game_in_text("관련 없는 질문") is None


def test_wiki_helpers_preserved():
    assert get_wiki_path_keywords("에픽세븐") == ["에픽세븐", "EP |", "EP7"]
    assert get_wiki_path_keywords("카제나") == ["카제나", "CZ |", "GCZ", "카오스", "Chaoszero"]
    assert get_wiki_ancestor_id("에픽세븐") == 58043932
    assert get_wiki_ancestor_id("카제나") == 650589593
    assert get_wiki_ancestor_id("로드나인") is None
    assert get_jira_project_key("로드나인 아시아") == "LNA"


def test_eclipse_added_from_catalog():
    """EC는 카탈로그 도입으로 신규 노출 (QA Workflow와 동일 소스)."""
    info = resolve_game("eclipse")
    assert info is not None
    assert info["jira_key"] == "EC"


def test_games_count():
    """카탈로그 5종 + 봇 로컬(리젝) = 6."""
    assert len(GAMES) == 6
