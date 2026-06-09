"""
build_biskit_catalog.py — BISKIT 데이터 카탈로그 정적 빌드

list_projects → get_project_menu_tree → search_datasets 전수 →
get_dataset_parameters로 key_params 추출 → biskit_catalog.json 저장.

원자적 쓰기(.tmp→rename), 부분실패 허용, 실패율>20% 시 기존 보존.
실행: python scripts/build_biskit_catalog.py
"""
import os, sys, json, datetime
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Slack Bot"))
os.environ.pop("ANTHROPIC_AUTH_TOKEN", None)
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"), override=True)

import biskit_client as bc

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "biskit_catalog.json")

# 프로젝트별 데이터셋 수집용 키워드 (메뉴 카테고리 기반)
PROBE_KEYWORDS = [
    ["가입", "접속", "AU", "NRU"], ["이탈", "과금구간"], ["커뮤니티", "감정", "키워드"],
    ["설치", "인스톨"], ["리텐션", "잔존"], ["PVE", "튜토리얼", "가챠"],
    ["성장", "레벨"], ["크리스탈", "재화", "아이템"], ["실시간", "동접", "WarRoom"],
    ["비교", "요약", "동향"],
]


def build():
    result = {"built_at": datetime.datetime.now().isoformat(), "projects": []}
    total, ok, fail = 0, 0, 0

    projects = bc.list_projects()
    for p in projects:
        pid = p.get("id")
        proj = {
            "id": pid, "name": p.get("name"), "aliases": p.get("aliases", []),
            "aiDescription": p.get("aiDescription", ""),
            "menu_tree": None, "datasets": [],
        }
        # 메뉴 트리
        try:
            mt = bc.call_tool("get_project_menu_tree", {"project_id": pid})
            proj["menu_tree"] = mt.get("menuCategories", mt) if isinstance(mt, dict) else mt
        except Exception as e:
            print(f"[WARN] menu_tree 실패 pid={pid}: {e}")

        # 데이터셋 전수 (키워드 합집합)
        seen = {}
        for kws in PROBE_KEYWORDS:
            try:
                r = bc.call_tool("search_datasets", {"project_id": pid, "keywords": kws, "limit": 50})
                ds = r if isinstance(r, list) else r.get("datasets", [])
                for d in ds:
                    did = d.get("datasetId")
                    if did and did not in seen:
                        seen[did] = {
                            "id": did, "menuPath": d.get("menuPath", ""),
                            "description": d.get("description", ""),
                            "metricCategory": d.get("metricCategory", ""),
                            "key_params": [], "params_unknown": True,
                        }
            except Exception as e:
                print(f"[WARN] search_datasets 실패 pid={pid} kws={kws}: {e}")

        # key_params 추출
        for did, entry in seen.items():
            total += 1
            try:
                params = bc.call_tool("get_dataset_parameters", {"dataset_ids": [did]})
                dss = params.get("datasets", []) if isinstance(params, dict) else []
                if dss:
                    keys = [pp.get("key") for pp in dss[0].get("parameters", []) if pp.get("key")]
                    entry["key_params"] = keys
                    entry["params_unknown"] = False
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                fail += 1
                print(f"[WARN] params 실패 did={did}: {e}")

        proj["datasets"] = list(seen.values())
        result["projects"].append(proj)
        print(f"[OK] {proj['name']}(id={pid}): {len(seen)} datasets")

    # 실패율 가드
    fail_rate = fail / total if total else 0
    print(f"\n총 {total}건 · 성공 {ok} · 실패 {fail} · 실패율 {fail_rate:.1%}")
    if fail_rate > 0.20 and os.path.exists(OUT):
        print(f"[ABORT] 실패율 {fail_rate:.1%} > 20% — 기존 카탈로그 보존, 덮어쓰기 안 함")
        return 1

    # 원자적 쓰기
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    tmp = OUT + f".tmp.{os.getpid()}"   # PID 분리 — 동시 빌드 race 방지
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUT)
    print(f"[SAVED] {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(build())
