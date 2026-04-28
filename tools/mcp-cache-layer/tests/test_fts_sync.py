"""
test_fts_sync.py — task-086 FTS5 자동 동기화 단위 테스트

매트릭스:
  T-86-1: upsert_content 호출 시 search_fts에 rowid 생성
  T-86-2: body_text 업데이트 시 rowid 중복 없이 DELETE+INSERT
  T-86-3: body_text=None 호출 시 search_fts에서 rowid 제거
  T-86-4: Wiki source_type FTS MATCH 검색 가능
  T-86-5: Jira source_type FTS MATCH 검색 가능
  T-86-6: FTS 테이블 미존재 시 silent skip (schema v3 호환)
  T-86-7: Wiki + Jira + GDI 교차 검색으로 정확한 rowid 반환

※ FTS5 contentless 테이블은 값 SELECT 불가, MATCH 쿼리로만 검증.
"""

import gc
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SRC))

from src.cache_manager import CacheManager
from src.models import get_connection


def _make_temp_db() -> Path:
    """임시 DB 파일 생성."""
    fd = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    fd.close()
    return Path(fd.name)


def _safe_unlink(p: Path):
    """Windows 파일 락 우회."""
    gc.collect()
    for _ in range(5):
        try:
            if p.exists():
                p.unlink()
            return
        except PermissionError:
            time.sleep(0.1)


def test_t86_1_upsert_content_autoinsert_fts():
    """upsert_content 호출 → search_fts에 rowid 생성."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("gdi", "test-1", "영웅 데이터")
        cm.upsert_content(node_id, body_raw="raw", body_text="이것은 영웅의 본문 데이터입니다.")

        conn = get_connection(str(db_path))
        try:
            # rowid 존재 확인
            row = conn.execute(
                "SELECT rowid FROM search_fts WHERE rowid = ?",
                (node_id,),
            ).fetchone()
            assert row is not None, "FTS에 rowid 없음"
            # MATCH 검증
            match = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '영웅'"
            ).fetchone()
            assert match is not None
            assert match["rowid"] == node_id
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


def test_t86_2_upsert_content_update_fts():
    """body_text 업데이트 → rowid 중복 없이 DELETE+INSERT."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("gdi", "test-2", "카린")

        cm.upsert_content(node_id, body_text="초기 본문 키린")
        cm.upsert_content(node_id, body_text="갱신된 본문 각성")

        conn = get_connection(str(db_path))
        try:
            # rowid 중복 없음
            rows = conn.execute(
                "SELECT COUNT(*) as c FROM search_fts WHERE rowid = ?",
                (node_id,),
            ).fetchone()
            assert rows["c"] == 1, f"FTS 중복 존재: {rows['c']}"

            # 과거 토큰(키린)으로는 매칭 안 됨
            old = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '키린'"
            ).fetchone()
            assert old is None, "과거 body_text가 FTS에 남아있음"

            # 새 토큰(각성)으로 매칭
            new = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '각성'"
            ).fetchone()
            assert new is not None
            assert new["rowid"] == node_id
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


def test_t86_3_upsert_empty_deletes_fts():
    """body_text=None → FTS에서 제거."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("gdi", "test-3", "제목")

        cm.upsert_content(node_id, body_text="본문")
        # 선 존재 확인
        conn = get_connection(str(db_path))
        try:
            row = conn.execute(
                "SELECT rowid FROM search_fts WHERE rowid = ?", (node_id,)
            ).fetchone()
            assert row is not None
        finally:
            conn.close()

        # 본문 비움
        cm.upsert_content(node_id, body_text=None)

        conn = get_connection(str(db_path))
        try:
            row = conn.execute(
                "SELECT rowid FROM search_fts WHERE rowid = ?", (node_id,)
            ).fetchone()
            assert row is None, "FTS에서 제거되지 않음"
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


def test_t86_4_wiki_source_type():
    """Wiki source_type도 FTS sync 동작."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("wiki", "wiki-page-1", "QA 체크리스트", space_key="QASGP")
        cm.upsert_content(node_id, body_text="QA 프로세스 체크리스트 문서")

        conn = get_connection(str(db_path))
        try:
            match = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '프로세스'"
            ).fetchone()
            assert match is not None
            assert match["rowid"] == node_id
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


def test_t86_5_jira_source_type():
    """Jira source_type도 FTS sync 동작."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("jira", "GEP-1234", "버그 영웅 능력치 오류")
        cm.upsert_content(node_id, body_text="영웅 각성 단계에서 공격력 오류")

        conn = get_connection(str(db_path))
        try:
            match = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '각성'"
            ).fetchone()
            assert match is not None
            assert match["rowid"] == node_id
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


def test_t86_6_no_fts_table_silent_skip():
    """FTS 테이블 없어도 upsert_content 성공 (silent skip)."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        node_id = cm.upsert_node("gdi", "test-6", "제목")

        # FTS 테이블 강제 삭제 (schema v3 시뮬레이션)
        conn = get_connection(str(db_path))
        try:
            conn.execute("DROP TABLE IF EXISTS search_fts")
            conn.commit()
        finally:
            conn.close()

        # upsert_content는 예외 없이 성공해야 함
        cm.upsert_content(node_id, body_text="본문")

        # 본문은 정상 저장
        content = cm.get_content(node_id)
        assert content is not None
        assert content["body_text"] == "본문"
    finally:
        _safe_unlink(db_path)


def test_t88_1_search_content_basic():
    """[task-088] search_content 기본 동작 — 본문 키워드 매칭."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        wiki_id = cm.upsert_node("wiki", "w1", "QA 가이드", space_key="QASGP")
        cm.upsert_content(wiki_id, body_text="영웅 각성 프로세스 단계별 설명")

        results = cm.search_content("영웅", "wiki")
        assert len(results) == 1
        assert results[0]["node_id"] == wiki_id
        assert results[0]["title"] == "QA 가이드"
        assert "영웅" in results[0]["snippet"]
    finally:
        _safe_unlink(db_path)


def test_t88_2_search_content_source_filter():
    """[task-088] source_type 필터 — 다른 source는 제외."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        w = cm.upsert_node("wiki", "w1", "위키 문서")
        cm.upsert_content(w, body_text="영웅 개발 가이드")

        j = cm.upsert_node("jira", "J-1", "지라 이슈")
        cm.upsert_content(j, body_text="영웅 관련 이슈")

        results_wiki = cm.search_content("영웅", "wiki")
        assert len(results_wiki) == 1
        assert results_wiki[0]["node_id"] == w

        results_jira = cm.search_content("영웅", "jira")
        assert len(results_jira) == 1
        assert results_jira[0]["node_id"] == j
    finally:
        _safe_unlink(db_path)


def test_t88_3_search_content_and_keywords():
    """[task-088] 공백 구분 키워드 → AND 매칭.

    ※ unicode61 tokenizer는 공백 기준 토큰화 — "영웅과"는 "영웅"과 별도 토큰.
    """
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        n1 = cm.upsert_node("wiki", "w1", "문서1")
        cm.upsert_content(n1, body_text="영웅 관련 내용")
        n2 = cm.upsert_node("wiki", "w2", "문서2")
        cm.upsert_content(n2, body_text="영웅 각성 가이드")

        # "영웅" 검색: 두 문서 모두
        all_hits = cm.search_content("영웅", "wiki")
        assert len(all_hits) == 2

        # "영웅 각성" 검색 (AND): 2번 문서만
        both = cm.search_content("영웅 각성", "wiki")
        assert len(both) == 1
        assert both[0]["node_id"] == n2

        # "영웅 관련" 검색 (AND): 1번 문서만
        rel = cm.search_content("영웅 관련", "wiki")
        assert len(rel) == 1
        assert rel[0]["node_id"] == n1
    finally:
        _safe_unlink(db_path)


def test_t88_4_search_content_empty_query():
    """[task-088] 빈 쿼리 → 빈 결과 (FTS MATCH 에러 방지)."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        n = cm.upsert_node("wiki", "w", "문서")
        cm.upsert_content(n, body_text="내용")

        assert cm.search_content("", "wiki") == []
        assert cm.search_content("   ", "wiki") == []
    finally:
        _safe_unlink(db_path)


def test_t88_5_search_content_space_key_filter():
    """[task-088] space_key 필터 — 해당 공간만."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        a = cm.upsert_node("wiki", "a", "문서A", space_key="SPACE_A")
        cm.upsert_content(a, body_text="영웅 가이드")
        b = cm.upsert_node("wiki", "b", "문서B", space_key="SPACE_B")
        cm.upsert_content(b, body_text="영웅 가이드")

        hits_a = cm.search_content("영웅", "wiki", space_key="SPACE_A")
        assert len(hits_a) == 1
        assert hits_a[0]["node_id"] == a

        hits_all = cm.search_content("영웅", "wiki")  # space_key 없음
        assert len(hits_all) == 2
    finally:
        _safe_unlink(db_path)


def test_t88_6_search_content_no_fts_table():
    """[task-088] FTS 테이블 없으면 빈 리스트 반환 (예외 없음)."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        n = cm.upsert_node("wiki", "w", "문서")
        cm.upsert_content(n, body_text="내용")

        # FTS 테이블 제거
        import sqlite3 as sq
        conn = sq.connect(str(db_path))
        try:
            conn.execute("DROP TABLE IF EXISTS search_fts")
            conn.commit()
        finally:
            conn.close()

        results = cm.search_content("내용", "wiki")
        assert results == []
    finally:
        _safe_unlink(db_path)


def test_t88_7_search_content_limit():
    """[task-088] limit 적용."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        for i in range(5):
            nid = cm.upsert_node("wiki", f"w{i}", f"문서{i}")
            cm.upsert_content(nid, body_text=f"영웅 {i}번 가이드")

        hits = cm.search_content("영웅", "wiki", limit=3)
        assert len(hits) == 3
    finally:
        _safe_unlink(db_path)


def test_t86_7_cross_source_search():
    """Wiki + Jira + GDI 교차 검색 정확도."""
    db_path = _make_temp_db()
    try:
        cm = CacheManager(db_path=str(db_path))
        wiki_id = cm.upsert_node("wiki", "w1", "테스트 문서")
        cm.upsert_content(wiki_id, body_text="영웅 육성 가이드 문서")

        jira_id = cm.upsert_node("jira", "GEP-1", "제목")
        cm.upsert_content(jira_id, body_text="영웅 레벨업 버그")

        gdi_id = cm.upsert_node("gdi", "g1", "밸런스 데이터")
        cm.upsert_content(gdi_id, body_text="영웅 각성 스탯 테이블")

        conn = get_connection(str(db_path))
        try:
            # "영웅" 검색: 3건 모두
            rows = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '영웅'"
            ).fetchall()
            row_ids = {r["rowid"] for r in rows}
            assert row_ids == {wiki_id, jira_id, gdi_id}, f"매치된 rowid: {row_ids}"

            # "영웅 AND 버그" 검색: jira 1건
            rows = conn.execute(
                "SELECT rowid FROM search_fts WHERE search_fts MATCH '영웅 AND 버그'"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["rowid"] == jira_id
        finally:
            conn.close()
    finally:
        _safe_unlink(db_path)


if __name__ == "__main__":
    import traceback
    # Windows cp949 에러 회피: stdout을 UTF-8로 강제
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    tests = [(n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  PASS: {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL: {name} - {e}")
            traceback.print_exc()

    print(f"\n{'=' * 50}")
    print(f"Total: {len(tests)}, Passed: {passed}, Failed: {failed}")
    sys.exit(0 if failed == 0 else 1)
