"""task-132 PR1 운영 검증 — 사용자 5건 케이스 e2e (Stage 1~3 + 기대 분기 검증).

PR1-G 결정론 후처리 효과 검증:
  - game_alias_kr 정확 추출
  - file_kind 정확 추출
  - request_type 정확 분기 (content_search 만으로 수렴 금지)
"""

import sys
from pathlib import Path

SLACK_BOT_ROOT = Path(__file__).parent.parent / "Slack Bot"
sys.path.insert(0, str(SLACK_BOT_ROOT))

from dotenv import load_dotenv
load_dotenv(SLACK_BOT_ROOT.parent / ".env", override=True)

import intent_extractor as ie
import query_builder as qb
import relaxation_engine as re_mod


CASES = [
    {
        "id": "C1", "domain": "gdi", "text": "에픽세븐 \\ 신캐릭터 알려줘",
        "expected": {
            "game_alias_kr_contains": ["에픽세븐"],
            "request_type": "content_search",
            "min_hits": 1,
        },
    },
    {
        "id": "C2", "domain": "wiki", "text": "4월 27일부터 29일까지 업데이트된 페이지",
        "expected": {
            "request_type_in": ["list", "content_search"],
            "min_hits": 1,
        },
    },
    {
        "id": "C3", "domain": "wiki", "text": "카제나 \\ TEST INFO 최근 업데이트 된 날짜?",
        "expected": {
            "request_type": "metadata",
            "metadata_field": "last_modified",
            "min_hits": 1,
        },
    },
    {
        "id": "C4", "domain": "gdi", "text": "카제나 \\ 최근 패치노트 파일명이 뭐야?",
        "expected": {
            "game_alias_kr_contains": ["카제나"],
            "file_kind_contains": ["patch_note"],
            "request_type_in": ["list", "content_search"],
            "min_hits": 1,
        },
    },
    {
        "id": "C5", "domain": "gdi", "text": "카제나 \\ 은하계 재해 기획서",
        "expected": {
            "game_alias_kr_contains": ["카제나"],
            "folder_role_contains": ["planning"],
            "min_hits": 1,
        },
    },
]


def check_expected(intent, expected: dict, hits: int = 0) -> tuple[bool, list]:
    fails = []

    if "min_hits" in expected:
        if hits < expected["min_hits"]:
            fails.append(f"hits={hits} < min_hits={expected['min_hits']}")

    if "request_type" in expected:
        if intent.request_type != expected["request_type"]:
            fails.append(f"request_type={intent.request_type!r} (expected {expected['request_type']!r})")

    if "request_type_in" in expected:
        if intent.request_type not in expected["request_type_in"]:
            fails.append(f"request_type={intent.request_type!r} (expected one of {expected['request_type_in']})")

    if "metadata_field" in expected:
        mf = getattr(intent, "metadata_field", None)
        if mf != expected["metadata_field"]:
            fails.append(f"metadata_field={mf!r} (expected {expected['metadata_field']!r})")

    if "game_alias_kr_contains" in expected:
        actual = getattr(intent, "game_alias_kr", []) or []
        for needle in expected["game_alias_kr_contains"]:
            if needle not in actual:
                fails.append(f"game_alias_kr={actual!r} 미포함: {needle!r}")

    if "file_kind_contains" in expected:
        actual = getattr(intent, "file_kind", []) or []
        for needle in expected["file_kind_contains"]:
            if needle not in actual:
                fails.append(f"file_kind={actual!r} 미포함: {needle!r}")

    if "folder_role_contains" in expected:
        actual = getattr(intent, "folder_role", []) or []
        for needle in expected["folder_role_contains"]:
            if needle not in actual:
                fails.append(f"folder_role={actual!r} 미포함: {needle!r}")

    return len(fails) == 0, fails


def main() -> int:
    sys.path.insert(0, str(SLACK_BOT_ROOT.parent / "tools" / "mcp-cache-layer"))
    from src.cache_manager import CacheManager
    db_path = "D:/Vibe Dev/QA Ops/mcp-cache-layer/cache/mcp_cache.db"
    cache_mgr = CacheManager(db_path=db_path)

    # cache 우회 (LLM 비결정성 + 신선한 결과 검증 의무)
    ie._INTENT_CACHE.clear()

    pass_count = 0
    fail_count = 0
    detail_fails = {}

    for case in CASES:
        cid = case["id"]
        text = case["text"]
        domain = case["domain"]
        print(f"\n{'='*60}")
        print(f"[{cid}] {domain}: {text}")
        print(f"{'='*60}")

        try:
            intent = ie.extract_intent(text, domain=domain)
        except Exception as e:
            print(f"  🔴 Stage 1 exception: {e}")
            fail_count += 1
            continue

        if getattr(intent, "ai_failed", False):
            print(f"  🔴 Stage 1 ai_failed=True")
            fail_count += 1
            continue

        # Intent 출력
        ga = getattr(intent, "game_alias_kr", []) or []
        fk = getattr(intent, "file_kind", []) or []
        fr = getattr(intent, "folder_role", []) or []
        mf = getattr(intent, "metadata_field", None)
        rt = intent.request_type
        print(f"  Stage 1 intent:")
        print(f"    request_type: {rt}")
        print(f"    metadata_field: {mf}")
        print(f"    game_alias_kr: {ga}")
        print(f"    file_kind: {fk}")
        print(f"    folder_role: {fr}")

        # Stage 2 SQL
        try:
            built = qb.build_gdi_query(intent) if domain == "gdi" else qb.build_wiki_query(intent)
        except Exception as e:
            print(f"  🔴 Stage 2 exception: {e}")
            fail_count += 1
            continue

        # Stage 3 hits
        try:
            result = re_mod.search_with_ladder(cache_mgr, intent, domain)
            print(f"  Stage 3 hits: {result.total_count}")
        except Exception as e:
            print(f"  🔴 Stage 3 exception: {e}")
            fail_count += 1
            continue

        # 기대값 검증
        ok, fails = check_expected(intent, case["expected"], hits=result.total_count)
        if ok:
            print(f"  [{cid}] ✅ PASS")
            pass_count += 1
        else:
            print(f"  [{cid}] 🔴 FAIL — {len(fails)}건:")
            for f in fails:
                print(f"      - {f}")
            detail_fails[cid] = fails
            fail_count += 1

    print(f"\n{'='*60}")
    print(f"task-132 PR1 운영 검증 결과: {pass_count}/{len(CASES)} PASS / {fail_count} FAIL")
    print(f"{'='*60}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
