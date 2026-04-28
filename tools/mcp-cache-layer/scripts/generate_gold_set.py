"""
generate_gold_set.py — task-113 v4.2 gold set 5 stratum 추출기

설계 v4.2 §3 준수:
- gold_2part: target 8 (gdi_query.log 9-field, ask_claude_2part seam)
- gold_simple: target 2 (gdi_query.log search handler 1건 + Step 4 신규 1건)
- gold_folder: target 5 (folder_ai entries, nodes.path LIKE 매칭)
- gold_miss_time_range: target 8 (answer_miss.log MissEntry.category=TIME_RANGE)
- gold_manual: target 15 (R9 manual labeling, scope 외 — placeholder만 생성)

총 38건 (8+2+5+8+15). v4.2 Round 2 web-backend 실측 후 size 갱신.

사용법:
    SLACK_BOT_PATH='D:/Vibe Dev/Slack Bot/Slack Bot' \\
    python scripts/generate_gold_set.py \\
        --db cache/mcp_cache.db \\
        --output data/gold_set.jsonl \\
        [--allow-shortfall]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

# 파일명 추출 정규식 (v3 §4.2 — 공백 포함 한글 파일명 지원)
_FILENAME_RE = re.compile(r"파일:\s*([^,|\n]+?\.[a-zA-Z]{2,5})")
_FOLDER_RE = re.compile(r"폴더:\s*([^,|\n]+)")

# v4.2 stratum 별 target size
TARGET_2PART = 8
TARGET_SIMPLE = 2
TARGET_FOLDER = 5
TARGET_MISS_TIME_RANGE = 8
TARGET_MANUAL = 15  # placeholder only
TARGET_TOTAL = TARGET_2PART + TARGET_SIMPLE + TARGET_FOLDER + TARGET_MISS_TIME_RANGE + TARGET_MANUAL


def _load_failure_analyzer(slack_bot_root: Path):
    """Slack Bot/analytics/failure_analyzer 를 importlib 로 로드."""
    fa_py = slack_bot_root / "analytics" / "failure_analyzer.py"
    if not fa_py.exists():
        raise RuntimeError(f"failure_analyzer.py not found: {fa_py}")
    mod_name = "failure_analyzer_under_bench"
    spec = importlib.util.spec_from_file_location(
        mod_name, str(fa_py),
        submodule_search_locations=[str(fa_py.parent)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"importlib spec 생성 실패: {fa_py}")
    mod = importlib.util.module_from_spec(spec)
    _root_str = str(slack_bot_root)
    _added = False
    if _root_str not in sys.path:
        sys.path.insert(0, _root_str)
        _added = True
    sys.modules[mod_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(mod_name, None)
        raise
    finally:
        if _added:
            sys.path.remove(_root_str)
    return mod


def _resolve_node_ids_by_filename(
    conn: sqlite3.Connection, filename: str
) -> list[int]:
    """nodes.title LIKE %filename% 매칭 (gdi 만)."""
    rows = conn.execute(
        "SELECT id FROM nodes "
        "WHERE source_type='gdi' AND title LIKE ? "
        "ORDER BY id ASC",
        (f"%{filename}%",),
    ).fetchall()
    return sorted(int(r[0]) for r in rows)


def _resolve_node_ids_by_folder_path(
    conn: sqlite3.Connection, folder_path: str, top_k: int = 10
) -> list[int]:
    """nodes.path LIKE 'folder_path%' 매칭 (gdi 만, top-k 제한)."""
    rows = conn.execute(
        "SELECT id FROM nodes "
        "WHERE source_type='gdi' AND path LIKE ? "
        "ORDER BY id ASC LIMIT ?",
        (folder_path.rstrip("/") + "/%", top_k),
    ).fetchall()
    return [int(r[0]) for r in rows]


def _breadcrumb_to_path(breadcrumb: str) -> str:
    """'Chaoszero > Update Review > 20260204' → 'Chaoszero/Update Review/20260204'."""
    parts = [p.strip() for p in breadcrumb.split(">") if p.strip()]
    return "/".join(parts)


def _extract_2part(
    gdi_entries: list, conn: sqlite3.Connection, target: int
) -> list[dict]:
    """gold_2part: ask_claude 2-part seam (pipe 구분 첫 부분 = search_query 키워드)."""
    out: list[dict] = []
    for idx, e in enumerate(gdi_entries, start=1):
        if len(out) >= target:
            break
        if getattr(e, "status", None) != "OK":
            continue
        if getattr(e, "handler", None) != "ask_claude":
            continue
        question = getattr(e, "question", None)
        if not question:
            # n=8 log entry는 question 별도 필드 부재 — query="<search_query> \ <question>"에서 split
            # maxsplit=1로 첫 \ 기준 분리, question에 후속 \ 포함되면 첫 토큰만 추출 (3-piece 오염 방지)
            raw_query = getattr(e, "query", "") or ""
            if "\\" in raw_query:
                _parts = raw_query.split("\\", 1)
                if len(_parts) == 2:
                    question = _parts[1].split("\\")[0].strip()
            if not question:
                continue
        result_text = getattr(e, "result_or_error", "") or ""
        filename = None
        for _line in result_text.split('\n'):
            _line = _line.strip()
            if _line.startswith('파일:'):
                _candidate = _line[3:].strip()
                if re.match(r'^[^,|]+\.[a-zA-Z]{2,5}$', _candidate):
                    filename = _candidate
                    break
        if not filename:
            continue
        node_ids = _resolve_node_ids_by_filename(conn, filename)
        if not node_ids:
            continue
        # 입력: search_query 부분만 production seam에 들어감 (slack_bot.py:2041)
        # log의 query 필드는 "<search_query> \\ <question>" pipe 형태 (maxsplit=1로 일관)
        raw_query = getattr(e, "query", None) or question
        search_query = raw_query.split("\\", 1)[0].strip() if "\\" in raw_query else raw_query
        out.append({
            "id": f"R-{len(out) + 1}",
            "stratum": "gold_2part",
            "query": search_query,
            "expected_node_ids": node_ids,
            "source": f"gdi_query.log:L{idx}",
            "source_line": idx,
            "filename_matched": filename,
            "notes": "ask_claude_2part",
            "original_question": question.strip(),
        })
    return out


def _extract_simple(
    gdi_entries: list, conn: sqlite3.Connection, target: int
) -> list[dict]:
    """gold_simple: /gdi search 또는 search handler (gdi_simple_search seam)."""
    out: list[dict] = []
    for idx, e in enumerate(gdi_entries, start=1):
        if len(out) >= target:
            break
        if getattr(e, "status", None) != "OK":
            continue
        if getattr(e, "handler", None) != "search":
            continue
        result_text = getattr(e, "result_or_error", "") or ""
        filename = None
        for _line in result_text.split('\n'):
            _line = _line.strip()
            if _line.startswith('파일:'):
                _candidate = _line[3:].strip()
                if re.match(r'^[^,|]+\.[a-zA-Z]{2,5}$', _candidate):
                    filename = _candidate
                    break
        if not filename:
            # search handler 결과에 파일명 없을 수 있음 — 쿼리 자체로 nodes.title LIKE 매칭 시도
            raw_query = getattr(e, "query", None) or ""
            keywords = [k for k in raw_query.split() if len(k) >= 3]
            if not keywords:
                continue
            # 첫 키워드 기준 매칭 (단순화)
            node_ids = _resolve_node_ids_by_filename(conn, keywords[0])
            if not node_ids:
                continue
            out.append({
                "id": f"S-{len(out) + 1}",
                "stratum": "gold_simple",
                "query": raw_query.strip(),
                "expected_node_ids": node_ids[:10],
                "source": f"gdi_query.log:L{idx}",
                "source_line": idx,
                "notes": "gdi_simple_search (keyword match)",
            })
            continue
        node_ids = _resolve_node_ids_by_filename(conn, filename)
        if not node_ids:
            continue
        raw_query = getattr(e, "query", None) or filename
        out.append({
            "id": f"S-{len(out) + 1}",
            "stratum": "gold_simple",
            "query": raw_query.strip(),
            "expected_node_ids": node_ids,
            "source": f"gdi_query.log:L{idx}",
            "source_line": idx,
            "filename_matched": filename,
            "notes": "gdi_simple_search",
        })
    return out


def _extract_folder(
    gdi_entries: list, conn: sqlite3.Connection, target: int
) -> list[dict]:
    """gold_folder: folder_ai handler (folder_path navigation seam).

    v4.2 Round 2: nodes.path LIKE 'folder_path%' 직접 쿼리로 expected 추출.
    """
    out: list[dict] = []
    for idx, e in enumerate(gdi_entries, start=1):
        if len(out) >= target:
            break
        if getattr(e, "status", None) != "OK":
            continue
        if getattr(e, "handler", None) != "folder_ai":
            continue
        raw_query = getattr(e, "query", None) or ""
        # folder_ai의 query 필드는 folder breadcrumb 형식
        if ">" not in raw_query:
            continue
        folder_path = _breadcrumb_to_path(raw_query)
        if not folder_path:
            continue
        node_ids = _resolve_node_ids_by_folder_path(conn, folder_path, top_k=20)
        if not node_ids:
            # secondary_retry: 게임명 prefix 추가
            for prefix in ("Chaoszero/", "Epicseven/", "Kazena/"):
                if not folder_path.startswith(prefix):
                    node_ids = _resolve_node_ids_by_folder_path(
                        conn, prefix + folder_path, top_k=20
                    )
                    if node_ids:
                        folder_path = prefix + folder_path
                        break
        if not node_ids:
            continue
        question = getattr(e, "question", "") or ""
        # Round 5 qa-blackbox 보완: query 필드는 변환된 folder_path 사용
        # (routing_replay._folder_lookup → nodes.path LIKE 매칭 가능)
        out.append({
            "id": f"F-{len(out) + 1}",
            "stratum": "gold_folder",
            "query": folder_path,
            "expected_node_ids": node_ids[:10],
            "source": f"gdi_query.log:L{idx}",
            "source_line": idx,
            "raw_breadcrumb": raw_query.strip(),
            "notes": "folder_ai",
            "original_question": question.strip(),
        })
    return out


def _extract_miss_time_range(
    miss_entries: list, target: int
) -> list[dict]:
    """gold_miss_time_range: answer_miss.log TIME_RANGE 카테고리."""
    out: list[dict] = []
    for idx, e in enumerate(miss_entries, start=1):
        if len(out) >= target:
            break
        if getattr(e, "category", None) != "TIME_RANGE":
            continue
        question = getattr(e, "question", None)
        if not question:
            continue
        out.append({
            "id": f"T-{len(out) + 1}",
            "stratum": "gold_miss_time_range",
            "query": question.strip(),
            "expected_node_ids": [],
            "source": f"answer_miss.log:L{idx}",
            "source_line": idx,
            "notes": "TIME_RANGE",
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="task-113 v4.2 gold set 5 stratum 추출기"
    )
    ap.add_argument("--db", required=True, help="mcp_cache.db 경로")
    ap.add_argument("--gdi-log", default=None, help="gdi_query.log 경로")
    ap.add_argument("--miss-log", default=None, help="answer_miss.log 경로")
    ap.add_argument("--output", default="data/gold_set.jsonl", help="JSONL 출력 경로")
    ap.add_argument(
        "--allow-shortfall", action="store_true",
        help="size_target 미달 시에도 가능한 만큼 출력 (진단용, 기본 fail-loud)"
    )
    args = ap.parse_args()

    slack_bot_path = os.environ.get("SLACK_BOT_PATH")
    if not slack_bot_path:
        print(
            "오류: SLACK_BOT_PATH 미설정.",
            file=sys.stderr,
        )
        sys.exit(1)
    slack_bot_root = Path(slack_bot_path).resolve()
    if not slack_bot_root.exists():
        print(f"오류: SLACK_BOT_PATH 경로 없음: {slack_bot_root}", file=sys.stderr)
        sys.exit(1)

    fa = _load_failure_analyzer(slack_bot_root)

    gdi_log = (
        Path(args.gdi_log) if args.gdi_log
        else slack_bot_root.parent / "logs" / "gdi_query.log"
    )
    miss_log = (
        Path(args.miss_log) if args.miss_log
        else slack_bot_root.parent / "logs" / "answer_miss.log"
    )
    if not gdi_log.exists() or not miss_log.exists():
        print(f"오류: 로그 부재 ({gdi_log}, {miss_log})", file=sys.stderr)
        sys.exit(1)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"오류: DB 부재: {db_path}", file=sys.stderr)
        sys.exit(1)

    gdi_entries = fa.parse_gdi_query_log(gdi_log)
    miss_entries = fa.parse_answer_miss_log(miss_log)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        gold_2part = _extract_2part(gdi_entries, conn, TARGET_2PART)
        gold_simple = _extract_simple(gdi_entries, conn, TARGET_SIMPLE)
        gold_folder = _extract_folder(gdi_entries, conn, TARGET_FOLDER)
        gold_miss = _extract_miss_time_range(miss_entries, TARGET_MISS_TIME_RANGE)
    finally:
        conn.close()

    entries = gold_2part + gold_simple + gold_folder + gold_miss
    sizes = {
        "gold_2part": (len(gold_2part), TARGET_2PART),
        "gold_simple": (len(gold_simple), TARGET_SIMPLE),
        "gold_folder": (len(gold_folder), TARGET_FOLDER),
        "gold_miss_time_range": (len(gold_miss), TARGET_MISS_TIME_RANGE),
        "gold_manual": (0, TARGET_MANUAL),  # placeholder, scope 외
    }
    total_auto = sum(actual for actual, _ in sizes.values())
    total_target_auto = sum(target for _, target in sizes.values()) - TARGET_MANUAL

    # Fail-loud: gold_manual 제외한 자동 추출 4 stratum 모두 충족 필요
    if not args.allow_shortfall:
        for name, (actual, target) in sizes.items():
            if name == "gold_manual":
                continue  # scope 외
            if actual < target:
                raise RuntimeError(
                    f"{name} target {target} 미달 (actual {actual}). "
                    "--allow-shortfall 로 진단 진행 가능."
                )
    else:
        msg = " ".join(
            f"{n}={a}/{t}" for n, (a, t) in sizes.items() if n != "gold_manual"
        )
        print(
            f"[gold_set] WARN --allow-shortfall: {msg} "
            f"(gold_manual=0/{TARGET_MANUAL} scope 외)",
            file=sys.stderr,
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(
        f"[gold_set] auto={total_auto}/{total_target_auto} "
        f"(gold_manual={TARGET_MANUAL} scope 외) → {out_path}",
        file=sys.stderr,
    )
    for name, (actual, target) in sizes.items():
        marker = "✓" if actual >= target or name == "gold_manual" else "✗"
        print(
            f"  {marker} {name}: {actual}/{target}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
