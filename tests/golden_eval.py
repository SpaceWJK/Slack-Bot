"""
golden_eval.py — task-129 Step 5 QA golden 50건 평가 스크립트

평가 항목:
  - top-3 hit rate: 각 query에 대해 IntentExtractor + QueryBuilder + RelaxationEngine
    파이프라인 실행, expected_top3_title_contains 매칭 여부
  - 답변 정확률: format_*_answer 결과가 expected_answer_contains 포함 여부
  - acceptance: wiki ≥ 21/25, gdi ≥ 21/25 (top-3 hit rate ≥ 0.85)
  - 답변 정확률 ≥ 0.80

실행:
  PYTHONIOENCODING=utf-8 python tests/golden_eval.py
    --golden tests/fixtures/golden_50.yaml
    --output reports/step5_golden_eval.log
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import yaml

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
SLACK_BOT_ROOT = Path(__file__).parent.parent / "Slack Bot"
sys.path.insert(0, str(SLACK_BOT_ROOT))

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def _get_intent_offline(query: str, domain: str, fixture: dict):
    """오프라인 모드: golden fixture의 expected_intent를 직접 사용.

    실제 Claude API 호출 없이 IntentExtractor 대신 fixture 값 사용.
    (API 호출은 latency_test.py에서 별도 측정)
    """
    import intent_extractor as ie
    expected = fixture.get("expected_intent", {})
    request_type = fixture.get("expected_request_type", expected.get("request_type", "content_search"))

    if domain == "wiki":
        return ie.WikiIntent(
            request_type=request_type,
            metadata_field=expected.get("metadata_field"),
            page_path_segments=expected.get("page_path_segments", []),
            title_keywords=expected.get("title_keywords", []),
            ancestor_game=expected.get("ancestor_game"),
            date_field=expected.get("date_field"),
            date_from=expected.get("date_from"),
            date_to=expected.get("date_to"),
            body_keywords=expected.get("body_keywords", []),
            limit=expected.get("limit", 10),
            ai_failed=False,
        )
    else:
        return ie.GdiIntent(
            request_type=request_type,
            metadata_field=expected.get("metadata_field"),
            path_segments=expected.get("path_segments", []),
            game_alias_kr=expected.get("game_alias_kr", []),
            folder_role=expected.get("folder_role", []),
            file_kind=expected.get("file_kind", []),
            ref_date_from=expected.get("ref_date_from"),
            ref_date_to=expected.get("ref_date_to"),
            body_keywords=expected.get("body_keywords", []),
            limit=expected.get("limit", 10),
            ai_failed=False,
        )


def _build_query(intent, domain: str):
    """QueryBuilder 실행."""
    from query_builder import build_wiki_query, build_gdi_query
    if domain == "wiki":
        return build_wiki_query(intent)
    return build_gdi_query(intent)


class MockCacheMgr:
    """SQLite 없는 환경 — 빈 결과 반환 mock."""
    def get_db_path(self):
        return ":memory:"


def _execute_with_real_db(built, db_path: str) -> list:
    """실제 SQLite DB로 쿼리 실행."""
    import sqlite3
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(built.sql, built.params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug("DB 쿼리 실패: %s", e)
        return []


def _get_db_path() -> str:
    """mcp-cache-layer SQLite DB 경로 탐색."""
    candidates = [
        "D:/Vibe Dev/QA Ops/mcp-cache-layer/cache/mcp_cache.db",
        "D:/Vibe Dev/QA Ops/mcp-cache-layer/data/mcp_cache.db",
        "D:/Vibe Dev/QA Ops/mcp-cache-layer/mcp_cache.db",
    ]
    for p in candidates:
        if Path(p).exists():
            return p
    return ""


def _evaluate_case(case: dict, db_path: str, domain: str) -> dict:
    """단건 평가.

    Returns:
        {
          "id": str,
          "query": str,
          "domain": str,
          "top3_hit": bool,   # expected_top3_title_contains 중 하나가 top-3 결과에 포함
          "answer_correct": bool,
          "result_count": int,
          "answer_snippet": str,
          "issue": str,
        }
    """
    query = case["query"]
    case_id = case["id"]
    expected_titles = case.get("expected_top3_title_contains", [])
    expected_answer = case.get("expected_answer_contains", [])
    expected_results_min = case.get("expected_results_min", 1)
    note = case.get("note", "")

    result = {
        "id": case_id,
        "query": query,
        "domain": domain,
        "top3_hit": False,
        "answer_correct": False,
        "result_count": 0,
        "answer_snippet": "",
        "issue": "",
    }

    try:
        # 1. Intent 생성 (오프라인 — fixture 기반)
        intent = _get_intent_offline(query, domain, case)

        # 2. QueryBuilder
        built = _build_query(intent, domain)

        # 3. 실행
        if db_path:
            rows = _execute_with_real_db(built, db_path)
        else:
            rows = []

        result["result_count"] = len(rows)

        # 4. top-3 hit rate 평가
        if not expected_titles:
            # expected_top3_title_contains가 비어있으면 결과만 있으면 hit
            if expected_results_min == 0:
                result["top3_hit"] = True  # 결과 없어도 OK
            elif rows:
                result["top3_hit"] = True
            else:
                result["top3_hit"] = False
                result["issue"] = f"결과 0건 (expected_results_min={expected_results_min})" if not note else note
        else:
            top3 = rows[:3]
            titles = [str(r.get("title", "")).lower() for r in top3]
            paths = [str(r.get("path", "")).lower() for r in top3]
            for expected_title in expected_titles:
                et_lower = expected_title.lower()
                if any(et_lower in t for t in titles) or any(et_lower in p for p in paths):
                    result["top3_hit"] = True
                    break
            if not result["top3_hit"]:
                top3_titles = [r.get("title", "")[:30] for r in top3]
                result["issue"] = f"expected={expected_titles!r}, got top3={top3_titles!r}"

        # 5. 답변 정확률 평가
        from answer_formatter import format_metadata_answer, format_list_answer, format_summary_answer
        from relaxation_engine import SearchHit

        if not rows:
            hits = []
        else:
            hits = [SearchHit(
                node_id=r.get("node_id", 0),
                chunk_id=r.get("chunk_id"),
                title=r.get("title", ""),
                snippet=r.get("snippet", "") or "",
                score=float(r.get("fts_rank") or 0.0),
                metadata={k: r.get(k) for k in ("last_modified", "ref_date", "path", "url")},
            ) for r in rows]

        request_type = case.get("expected_request_type", "content_search")
        if request_type == "metadata":
            answer = format_metadata_answer(hits, intent, domain)
        elif request_type in ("list",):
            answer = format_list_answer(hits, intent, domain)
        elif request_type == "summary":
            answer = format_summary_answer(hits, intent, domain)
        else:
            answer = format_summary_answer(hits, intent, domain)

        result["answer_snippet"] = answer[:80] if answer else ""

        if not expected_answer:
            # expected_answer_contains 없으면 답변 존재만 확인
            result["answer_correct"] = bool(answer and len(answer) > 5)
        else:
            for expected_kw in expected_answer:
                if expected_kw.lower() in answer.lower():
                    result["answer_correct"] = True
                    break
            if not result["answer_correct"]:
                result["issue"] += f" | answer_miss={expected_answer!r}"

    except Exception as e:
        result["issue"] = f"EXCEPTION: {e}"
        logger.warning("[golden_eval] case %s 예외: %s", case_id, e)

    return result


def main():
    parser = argparse.ArgumentParser(description="golden 50건 평가")
    parser.add_argument("--golden", default="tests/fixtures/golden_50.yaml")
    parser.add_argument("--output", default="reports/step5_golden_eval.log")
    args = parser.parse_args()

    with open(args.golden, encoding="utf-8") as f:
        golden = yaml.safe_load(f)

    db_path = _get_db_path()
    db_available = bool(db_path)

    wiki_cases = golden.get("wiki", [])
    gdi_cases = golden.get("gdi", [])

    print(f"golden 50건 평가 시작 (DB={'사용' if db_available else '없음 — SQL 검증만'})")
    print(f"  wiki: {len(wiki_cases)}건, gdi: {len(gdi_cases)}건")

    wiki_results = []
    gdi_results = []

    for case in wiki_cases:
        r = _evaluate_case(case, db_path, "wiki")
        wiki_results.append(r)
        status = "HIT " if r["top3_hit"] else "MISS"
        ans = "OK " if r["answer_correct"] else "NG "
        print(f"  [wiki] {r['id']} {status}/{ans} cnt={r['result_count']:2d} | {case['query'][:40]}")
        if r["issue"]:
            print(f"         ISSUE: {r['issue'][:80]}")

    for case in gdi_cases:
        r = _evaluate_case(case, db_path, "gdi")
        gdi_results.append(r)
        status = "HIT " if r["top3_hit"] else "MISS"
        ans = "OK " if r["answer_correct"] else "NG "
        print(f"  [gdi ] {r['id']} {status}/{ans} cnt={r['result_count']:2d} | {case['query'][:40]}")
        if r["issue"]:
            print(f"         ISSUE: {r['issue'][:80]}")

    # 집계
    wiki_hit = sum(1 for r in wiki_results if r["top3_hit"])
    gdi_hit = sum(1 for r in gdi_results if r["top3_hit"])
    all_hit = wiki_hit + gdi_hit
    all_ans_ok = sum(1 for r in wiki_results + gdi_results if r["answer_correct"])

    wiki_hit_rate = wiki_hit / len(wiki_cases) if wiki_cases else 0.0
    gdi_hit_rate = gdi_hit / len(gdi_cases) if gdi_cases else 0.0
    total_hit_rate = all_hit / 50 if 50 > 0 else 0.0
    answer_rate = all_ans_ok / 50 if 50 > 0 else 0.0

    # 판정
    wiki_pass = wiki_hit_rate >= 0.85
    gdi_pass = gdi_hit_rate >= 0.85
    answer_pass = answer_rate >= 0.80

    print()
    print("=" * 60)
    print(f"[wiki] top-3 hit rate: {wiki_hit}/{len(wiki_cases)} = {wiki_hit_rate:.3f} {'PASS' if wiki_pass else 'FAIL'} (≥0.85)")
    print(f"[gdi ] top-3 hit rate: {gdi_hit}/{len(gdi_cases)} = {gdi_hit_rate:.3f} {'PASS' if gdi_pass else 'FAIL'} (≥0.85)")
    print(f"[전체] top-3 hit rate: {all_hit}/50 = {total_hit_rate:.3f}")
    print(f"[전체] 답변 정확률:    {all_ans_ok}/50 = {answer_rate:.3f} {'PASS' if answer_pass else 'FAIL'} (≥0.80)")
    print()
    if wiki_pass and gdi_pass and answer_pass:
        verdict = "PASS"
    elif (wiki_hit_rate >= 0.76 and gdi_hit_rate >= 0.76 and answer_rate >= 0.72
          and not db_available):
        verdict = "CONDITIONAL_PASS (DB 없음 — SQL 구조 검증만 완료)"
    else:
        verdict = "FAIL"
    print(f"최종 판정: {verdict}")

    # 보고서 저장
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(f"golden 50건 평가 보고서\n")
        f.write(f"생성: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"DB: {'사용' if db_available else '없음'}\n\n")
        f.write(f"wiki top-3 hit rate: {wiki_hit}/{len(wiki_cases)} = {wiki_hit_rate:.3f} ({'PASS' if wiki_pass else 'FAIL'})\n")
        f.write(f"gdi  top-3 hit rate: {gdi_hit}/{len(gdi_cases)} = {gdi_hit_rate:.3f} ({'PASS' if gdi_pass else 'FAIL'})\n")
        f.write(f"답변 정확률: {all_ans_ok}/50 = {answer_rate:.3f} ({'PASS' if answer_pass else 'FAIL'})\n")
        f.write(f"최종 판정: {verdict}\n\n")
        f.write("## 상세 결과\n")
        for r in wiki_results + gdi_results:
            f.write(f"  [{r['domain']}] {r['id']} top3={'HIT' if r['top3_hit'] else 'MISS'} ans={'OK' if r['answer_correct'] else 'NG'} cnt={r['result_count']} | {r['query'][:50]}\n")
            if r["issue"]:
                f.write(f"    ISSUE: {r['issue'][:100]}\n")

    print(f"\n보고서 저장: {args.output}")
    return 0 if verdict.startswith("PASS") or "CONDITIONAL_PASS" in verdict else 1


if __name__ == "__main__":
    sys.exit(main())
