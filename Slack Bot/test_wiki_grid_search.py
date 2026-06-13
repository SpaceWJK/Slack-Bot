"""test_wiki_grid_search.py — TDD: wiki 그리드 1순위 검색 (task-193 Phase 2 §A).

검증 케이스:
  T-1: hotfix 카테고리 감지 — 에픽세븐 핫픽스
  T-2: release 카테고리 감지 — 릴리즈 질문
  T-3: qa 카테고리 감지 — 체크리스트 질문
  T-4: 카테고리 미감지 → None 반환 (기존 경로 fallback)
  T-5: 게임 감지 시 game_tag 필터 포함
  T-6: 게임 미감지 시 category만 필터 (game_tag 없음)
  T-7: 그리드 0건 → 빈 list 반환 (기존 경로 fallback 확인)
  T-8: run_wiki_intent_pipeline — 그리드 hits 있으면 ask_claude_fn 호출 확인
  T-9: run_wiki_intent_pipeline — 그리드 0건이면 search_with_ladder 호출 확인
"""

import sys
import os
import sqlite3
from unittest.mock import MagicMock, patch, call
import pytest

# 모듈 경로 추가
sys.path.insert(0, os.path.dirname(__file__))

from intent_pipeline import grid_first_search_wiki, run_wiki_intent_pipeline


# ── 픽스처: mock cache_mgr ──────────────────────────────────────────────────

def _make_cache_mgr(rows=None, db_path=":memory:"):
    """mock CacheManager — get_db_path만 구현. DB는 in-memory SQLite."""
    cm = MagicMock()
    cm.get_db_path.return_value = db_path
    return cm


def _setup_inmemory_db(rows):
    """in-memory SQLite DB에 nodes/doc_content/doc_meta 테이블 + 테스트 행 삽입."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE nodes (
            id INTEGER PRIMARY KEY,
            source_type TEXT, title TEXT, url TEXT,
            category TEXT, game_tag TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE doc_content (
            id INTEGER PRIMARY KEY,
            node_id INTEGER, body_text TEXT, summary TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE doc_meta (
            id INTEGER PRIMARY KEY,
            node_id INTEGER, last_modified TEXT
        )
    """)
    for r in (rows or []):
        conn.execute(
            "INSERT INTO nodes (id, source_type, title, url, category, game_tag) VALUES (?,?,?,?,?,?)",
            (r["id"], r["source_type"], r["title"], r.get("url", ""), r.get("category"), r.get("game_tag"))
        )
        conn.execute(
            "INSERT INTO doc_content (node_id, body_text, summary) VALUES (?,?,?)",
            (r["id"], r.get("body_text", ""), r.get("summary", ""))
        )
        conn.execute(
            "INSERT INTO doc_meta (node_id, last_modified) VALUES (?,?)",
            (r["id"], r.get("last_modified", ""))
        )
    conn.commit()
    return conn


# ── T-1: hotfix 카테고리 감지 ─────────────────────────────────────────────────

def test_T1_hotfix_category_detected(tmp_path):
    db_file = str(tmp_path / "test.db")
    rows = [
        {"id": 1, "source_type": "wiki", "title": "2026_Hot Fix", "category": "hotfix",
         "game_tag": None, "last_modified": "2026-06-11", "body_text": "핫픽스 내용", "summary": "핫픽스"},
    ]
    # 실제 DB 파일 생성
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', '2026_Hot Fix', '', 'hotfix', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '핫픽스 내용', '핫픽스')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-06-11')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("에픽세븐 가장 최근 핫픽스 몇일?", cm)

    assert hits is not None
    assert len(hits) == 1
    assert hits[0].title == "2026_Hot Fix"


# ── T-2: release 카테고리 감지 ───────────────────────────────────────────────

def test_T2_release_category_detected(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', 'Release 2026-06', '', 'release', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '릴리즈 내용', '')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-06-01')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("에픽세븐 릴리즈 배포일정 알려줘", cm)

    assert hits is not None
    assert len(hits) >= 1


# ── T-3: qa 카테고리 감지 ────────────────────────────────────────────────────

def test_T3_qa_category_detected(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', 'QA 체크리스트', '', 'qa', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '체크리스트 내용', '')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-05-01')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("카제나 체크리스트 확인해줘", cm)

    assert hits is not None
    assert len(hits) >= 1


# ── T-4: 카테고리 미감지 → None 반환 ─────────────────────────────────────────

def test_T4_no_category_returns_none(tmp_path):
    db_file = str(tmp_path / "test.db")
    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("에픽세븐 최근 업데이트 내용", cm)
    assert hits is None


# ── T-5: 게임 감지 시 game_tag 필터 ──────────────────────────────────────────

def test_T5_game_tag_filter_applied(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    # 에픽세븐 hotfix
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', '에픽세븐 Hotfix', '', 'hotfix', '에픽세븐')")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '에픽 핫픽스', '')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-06-10')")
    # 카제나 hotfix (game_tag 다름)
    conn.execute("INSERT INTO nodes VALUES (2, 'wiki', '카제나 Hotfix', '', 'hotfix', '카제나')")
    conn.execute("INSERT INTO doc_content VALUES (2, 2, '카제나 핫픽스', '')")
    conn.execute("INSERT INTO doc_meta VALUES (2, 2, '2026-06-09')")
    # 공통 hotfix (game_tag NULL)
    conn.execute("INSERT INTO nodes VALUES (3, 'wiki', '공통 Hotfix', '', 'hotfix', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (3, 3, '공통 핫픽스', '')")
    conn.execute("INSERT INTO doc_meta VALUES (3, 3, '2026-06-08')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("에픽세븐 핫픽스 알려줘", cm)

    assert hits is not None
    titles = [h.title for h in hits]
    assert "에픽세븐 Hotfix" in titles   # game_tag=에픽세븐 포함
    assert "공통 Hotfix" in titles       # game_tag IS NULL 포함
    assert "카제나 Hotfix" not in titles  # 다른 게임 제외


# ── T-6: 게임 미감지 시 category만 필터 ──────────────────────────────────────

def test_T6_no_game_category_only(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', 'General Hotfix', '', 'hotfix', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '일반 핫픽스', '')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-06-01')")
    conn.execute("INSERT INTO nodes VALUES (2, 'wiki', '에픽세븐 Hotfix', '', 'hotfix', '에픽세븐')")
    conn.execute("INSERT INTO doc_content VALUES (2, 2, '에픽 핫픽스', '')")
    conn.execute("INSERT INTO doc_meta VALUES (2, 2, '2026-06-02')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    # 게임명 없는 핫픽스 질문 — 모든 hotfix 반환
    hits = grid_first_search_wiki("최근 핫픽스 알려줘", cm)

    assert hits is not None
    titles = [h.title for h in hits]
    assert "General Hotfix" in titles
    assert "에픽세븐 Hotfix" in titles


# ── T-7: 0건 → 빈 list 반환 (None이 아님) ────────────────────────────────────

def test_T7_zero_results_returns_empty_list(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    hits = grid_first_search_wiki("에픽세븐 핫픽스", cm)

    # 카테고리 감지 O, 결과 0건 → 빈 list (None이 아님 → pipeline은 fallback 분기 진입)
    assert hits is not None
    assert hits == []


# ── T-8: pipeline — 그리드 hits 있으면 ask_claude_fn 호출 ─────────────────────

def test_T8_pipeline_grid_hits_calls_ask_claude(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.execute("INSERT INTO nodes VALUES (1, 'wiki', '2026_Hot Fix', '', 'hotfix', NULL)")
    conn.execute("INSERT INTO doc_content VALUES (1, 1, '핫픽스 내용', '요약')")
    conn.execute("INSERT INTO doc_meta VALUES (1, 1, '2026-06-11')")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    respond = MagicMock()
    ask_claude_fn = MagicMock()

    # ie_mod: extract_intent가 ai_failed=True 반환하면 기존 경로에서 막힘 → 그리드 앞에서 처리하므로 OK
    # grid_first_search_wiki가 hits 반환하면 extract_intent 이전에 처리
    result = run_wiki_intent_pipeline(
        text="에픽세븐 핫픽스 알려줘",
        page_part="",
        question="에픽세븐 핫픽스 알려줘",
        respond=respond,
        cache_mgr=cm,
        ask_claude_fn=ask_claude_fn,
    )

    assert result is True
    ask_claude_fn.assert_called_once()


# ── T-9: pipeline — 그리드 0건이면 기존 경로(extract_intent) 진입 ─────────────

def test_T9_pipeline_grid_zero_falls_through(tmp_path):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE nodes (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, url TEXT, category TEXT, game_tag TEXT)")
    conn.execute("CREATE TABLE doc_content (id INTEGER PRIMARY KEY, node_id INTEGER, body_text TEXT, summary TEXT)")
    conn.execute("CREATE TABLE doc_meta (id INTEGER PRIMARY KEY, node_id INTEGER, last_modified TEXT)")
    conn.commit()
    conn.close()

    cm = _make_cache_mgr(db_path=db_file)
    respond = MagicMock()
    ask_claude_fn = MagicMock()

    # ie_mod mock — ai_failed=False, relax_mod mock — hits 1건 반환
    from dataclasses import dataclass, field
    from typing import Optional
    @dataclass
    class FakeIntent:
        ai_failed: bool = False
        request_type: str = "content_search"
        ambiguity_notes: str = ""

    @dataclass
    class FakeHit:
        node_id: int = 1
        chunk_id: Optional[int] = None
        title: str = "fallback hit"
        snippet: str = "내용"
        score: float = 1.0
        metadata: dict = field(default_factory=dict)

    @dataclass
    class FakeResult:
        hits: list = field(default_factory=list)
        total_count: int = 1
        relaxation_level: int = 0
        history: list = field(default_factory=list)

    ie_mod = MagicMock()
    ie_mod.extract_intent.return_value = FakeIntent()
    relax_mod = MagicMock()
    relax_mod.search_with_ladder.return_value = FakeResult(hits=[FakeHit()], total_count=1)

    result = run_wiki_intent_pipeline(
        text="에픽세븐 핫픽스",
        page_part="",
        question="에픽세븐 핫픽스",
        respond=respond,
        cache_mgr=cm,
        ie_mod=ie_mod,
        relax_mod=relax_mod,
        ask_claude_fn=ask_claude_fn,
    )

    # 그리드 0건 → extract_intent 호출 → search_with_ladder → ask_claude_fn 호출
    assert result is True
    ie_mod.extract_intent.assert_called_once()
    relax_mod.search_with_ladder.assert_called_once()
    ask_claude_fn.assert_called_once()
