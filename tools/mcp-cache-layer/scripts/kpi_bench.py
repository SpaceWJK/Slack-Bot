"""
kpi_bench.py — 200쿼리 KPI 벤치마크 + HTML 리포트 생성 (task-112) + nDCG 모드 (task-113)

사용법:
    # 기존 recall 모드 (task-112)
    python scripts/kpi_bench.py --db /path/to/mcp_cache.db
    python scripts/kpi_bench.py --db after.db --compare before.db --output reports/kpi_report.html

    # nDCG 모드 (task-113)
    set -a; source data/run_recipe.env; set +a
    python scripts/kpi_bench.py --db cache/mcp_cache.db --gold data/gold_set.jsonl \\
        --metric ndcg --adapter production --reference-date 2026-04-24 \\
        --output reports/kpi_ndcg_report.html
"""
import argparse
import datetime
import hashlib
import html as _html
import json
import os
import sqlite3
import statistics as _stats
import sys
from pathlib import Path

# bench_fts는 같은 디렉터리에 위치 — sys.path에 추가하여 직접 import
sys.path.insert(0, str(Path(__file__).parent))
from bench_fts import (  # noqa: E402
    measure,
    measure_chunks,
    measure_ndcg_production,
    measure_ndcg_raw_fts,
    get_fts_index_size,
)


# ── 쿼리 로더 ─────────────────────────────────────────────────────────────────

def load_queries(path: Path) -> list:
    """bench_queries_200.json 로드.

    FileNotFoundError → 에러 메시지 출력 후 sys.exit(1).
    """
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"오류: 쿼리 파일 없음: {path}", file=sys.stderr)
        sys.exit(1)
    return data


# ── 벤치마크 실행 ──────────────────────────────────────────────────────────────

def run_benchmark(conn: sqlite3.Connection, queries: list,
                  chunk_mode: bool = False) -> list:
    """각 쿼리에 대해 measure() 또는 measure_chunks() 호출, 결과 리스트 반환.

    Args:
        chunk_mode: True이면 chunks_fts(청크 기반) 측정. False(기본)이면 search_fts.
                    task-104 청크 검색 활성화 후 실제 운영 경로 KPI 측정용 (task-112).

    Returns:
        list of {"query", "expected", "tags", "recall", "precision", "p95_ms"}
    """
    fn = measure_chunks if chunk_mode else measure
    results = []
    for item in queries:
        q_text = item["query"]
        expected = item.get("expected")
        tags = item.get("tags") or []
        recall, precision, p95_ms = fn(conn, q_text, expected, n_repeat=10)
        results.append({
            "query": q_text,
            "expected": expected,
            "tags": tags,
            "recall": recall,
            "precision": precision,
            "p95_ms": p95_ms,
        })
    return results


# ── body_text 비율 조회 ────────────────────────────────────────────────────────

def get_body_text_ratio(conn: sqlite3.Connection):
    """doc_content 테이블에서 body_text 채워진 비율 반환.

    doc_content 테이블 없으면 None 반환 (sqlite3.OperationalError 처리).
    """
    try:
        row = conn.execute(
            "SELECT "
            "  CAST(SUM(CASE WHEN body_text IS NOT NULL AND char_count > 0 THEN 1 ELSE 0 END) AS REAL) "
            "  / COUNT(*) "
            "FROM doc_content"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0.0
    except sqlite3.OperationalError:
        return None  # 테이블 없음 → KPI 카드 "N/A" 표시


# ── KPI 집계 ──────────────────────────────────────────────────────────────────

def compute_kpi(results: list, body_ratio) -> dict:
    """KPI 지표 계산.

    Args:
        results: run_benchmark() 반환 리스트
        body_ratio: get_body_text_ratio() 반환값 (float | None)

    Returns:
        dict(miss_rate, avg_recall, tr_hit_rate, p90_ms, body_text_ratio,
             n_queries, n_recall_measured)
    """
    recall_vals = [r["recall"] for r in results if r["recall"] is not None]
    avg_recall = sum(recall_vals) / len(recall_vals) if recall_vals else 0.0
    miss_rate = 1.0 - avg_recall

    # 시간범위 서브셋 — recall==1인 비율
    tr_results = [r for r in results if "시간범위" in (r.get("tags") or [])]
    tr_recall_vals = [r["recall"] for r in tr_results if r["recall"] is not None]
    # 0건이면 None (0.0 오탐 방지)
    tr_hit_rate = (
        sum(1 for v in tr_recall_vals if v == 1) / len(tr_recall_vals)
        if tr_recall_vals else None
    )

    # p90 레이턴시 — statistics.quantiles P90 (index 8)
    p95_vals = sorted(r["p95_ms"] for r in results if r.get("p95_ms") is not None)
    if len(p95_vals) >= 10:
        p90_ms = _stats.quantiles(p95_vals, n=10)[8]  # index 8 = P90
    elif p95_vals:
        p90_ms = max(p95_vals)  # n<10이면 worst case
    else:
        p90_ms = None

    return {
        "miss_rate": miss_rate,
        "avg_recall": avg_recall,
        "tr_hit_rate": tr_hit_rate,
        "p90_ms": p90_ms,
        "body_text_ratio": body_ratio,
        "n_queries": len(results),
        "n_recall_measured": len(recall_vals),
    }


# ── HTML 렌더러 ───────────────────────────────────────────────────────────────

def render_html(
    kpi: dict,
    results: list,
    before_kpi,
    before_results,
    db_path: str,
    ts: str,
) -> str:
    """KPI 리포트 HTML 생성.

    Args:
        kpi: compute_kpi() 반환
        results: run_benchmark() 반환 (after)
        before_kpi: --compare 지정 시 before compute_kpi(), 없으면 None
        before_results: --compare 지정 시 before run_benchmark(), 없으면 None
        db_path: 측정 대상 DB 경로 (표시용)
        ts: 타임스탬프 문자열

    Returns:
        UTF-8 HTML 문자열. 모든 사용자 데이터에 html.escape() 적용.
    """
    # ── KPI 카드 색상 판정 ────────────────────────────────────────────────────
    GREEN = "#22c55e"
    RED = "#ef4444"
    GRAY = "#6b7280"

    # miss_rate
    miss_color = GREEN if kpi["miss_rate"] < 0.40 else RED
    miss_val = f"{kpi['miss_rate']:.1%}"

    # tr_hit_rate
    if kpi["tr_hit_rate"] is None:
        tr_color = GRAY
        tr_val = "N/A"
    elif kpi["tr_hit_rate"] >= 0.80:
        tr_color = GREEN
        tr_val = f"{kpi['tr_hit_rate']:.1%}"
    else:
        tr_color = RED
        tr_val = f"{kpi['tr_hit_rate']:.1%}"

    # p90_ms — before_kpi 없으면 gray
    if before_kpi is None:
        p90_color = GRAY
        p90_val = "N/A — --compare 필요"
    elif kpi["p90_ms"] is not None and before_kpi["p90_ms"] is not None and kpi["p90_ms"] < before_kpi["p90_ms"] * 0.70:
        p90_color = GREEN
        p90_val = f"{kpi['p90_ms']:.1f} ms"
    else:
        p90_color = RED
        p90_val = f"{kpi['p90_ms']:.1f} ms" if kpi["p90_ms"] is not None else "N/A"

    # body_text_ratio
    if kpi["body_text_ratio"] is None:
        body_color = GRAY
        body_val = "N/A"
    elif kpi["body_text_ratio"] >= 0.98:
        body_color = GREEN
        body_val = f"{kpi['body_text_ratio']:.1%}"
    else:
        body_color = RED
        body_val = f"{kpi['body_text_ratio']:.1%}"

    # ── KPI 카드 HTML ─────────────────────────────────────────────────────────
    def kpi_card(label: str, value: str, color: str) -> str:
        return (
            f'<div class="kpi-card" style="border-top:4px solid {color}">'
            f'<div class="kpi-label">{_html.escape(label)}</div>'
            f'<div class="kpi-value" style="color:{color}">{_html.escape(value)}</div>'
            f'</div>'
        )

    cards_html = (
        kpi_card("Miss Rate (낮을수록 좋음)", miss_val, miss_color)
        + kpi_card("시간범위 Hit Rate (≥80%)", tr_val, tr_color)
        + kpi_card("P90 레이턴시 (before 대비 -30%)", p90_val, p90_color)
        + kpi_card("Body Text 비율 (≥98%)", body_val, body_color)
    )

    # ── 쿼리 결과 테이블 ──────────────────────────────────────────────────────
    has_delta = before_results is not None
    before_map = {}
    if before_results:
        for br in before_results:
            before_map[br["query"]] = br

    th_delta = (
        '<th>Δ Recall</th><th>Δ P95(ms)</th>'
        if has_delta else ""
    )
    table_header = (
        f'<tr><th>#</th><th>Query</th><th>Tags</th>'
        f'<th>Recall</th><th>Precision</th><th>P95(ms)</th>{th_delta}</tr>'
    )

    rows_html_parts = []
    for i, r in enumerate(results, 1):
        q_esc = _html.escape(str(r["query"]))
        tags_esc = _html.escape(", ".join(r.get("tags") or []))
        recall_str = str(r["recall"]) if r["recall"] is not None else "-"
        prec_str = f"{r['precision']:.2f}" if r["precision"] is not None else "-"
        p95_str = f"{r['p95_ms']:.1f}" if r["p95_ms"] is not None else "-"

        delta_td = ""
        if has_delta:
            br = before_map.get(r["query"])
            if br:
                if r["recall"] is not None and br["recall"] is not None:
                    d_recall = r["recall"] - br["recall"]
                    d_recall_str = f"{d_recall:+.2f}"
                else:
                    d_recall_str = "-"
                if r["p95_ms"] is not None and br["p95_ms"] is not None:
                    d_p95 = r["p95_ms"] - br["p95_ms"]
                    d_p95_str = f"{d_p95:+.1f}"
                else:
                    d_p95_str = "-"
                delta_td = (
                    f"<td>{_html.escape(d_recall_str)}</td>"
                    f"<td>{_html.escape(d_p95_str)}</td>"
                )
            else:
                delta_td = "<td>-</td><td>-</td>"

        row_html = (
            f"<tr>"
            f"<td>{i}</td>"
            f"<td>{q_esc}</td>"
            f"<td>{tags_esc}</td>"
            f"<td>{_html.escape(recall_str)}</td>"
            f"<td>{_html.escape(prec_str)}</td>"
            f"<td>{_html.escape(p95_str)}</td>"
            f"{delta_td}"
            f"</tr>"
        )
        rows_html_parts.append(row_html)

    rows_html = "\n".join(rows_html_parts)

    # ── 요약 정보 ─────────────────────────────────────────────────────────────
    db_path_esc = _html.escape(str(db_path))
    ts_esc = _html.escape(ts)
    n_queries = kpi["n_queries"]
    n_recall = kpi["n_recall_measured"]
    avg_recall_str = f"{kpi['avg_recall']:.2%}"

    # ── 전체 HTML ─────────────────────────────────────────────────────────────
    html_str = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KPI Benchmark Report</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f8fafc; color: #1e293b; padding: 24px; }}
    h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 4px; }}
    .meta {{ color: #64748b; font-size: 0.875rem; margin-bottom: 24px; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
    .kpi-card {{ background: #fff; border-radius: 8px; padding: 20px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
    .kpi-label {{ font-size: 0.75rem; color: #64748b; margin-bottom: 8px; text-transform: uppercase; letter-spacing: .05em; }}
    .kpi-value {{ font-size: 1.75rem; font-weight: 700; }}
    .summary {{ background: #fff; border-radius: 8px; padding: 16px; margin-bottom: 24px;
                box-shadow: 0 1px 3px rgba(0,0,0,.08); font-size: 0.875rem; color: #475569; }}
    .summary span {{ font-weight: 600; color: #1e293b; }}
    h2 {{ font-size: 1.125rem; font-weight: 600; margin-bottom: 12px; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.8125rem; }}
    th {{ background: #f1f5f9; padding: 10px 12px; text-align: left;
          font-weight: 600; border-bottom: 2px solid #e2e8f0; white-space: nowrap; }}
    td {{ padding: 8px 12px; border-bottom: 1px solid #f1f5f9; }}
    tr:hover td {{ background: #f8fafc; }}
    @media (max-width: 900px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
  </style>
</head>
<body>
  <h1>KPI Benchmark Report</h1>
  <div class="meta">DB: {db_path_esc} &nbsp;|&nbsp; 생성: {ts_esc}</div>

  <div class="kpi-grid">
    {cards_html}
  </div>

  <div class="summary">
    총 쿼리: <span>{n_queries}</span>건 &nbsp;|&nbsp;
    Recall 측정: <span>{n_recall}</span>건 &nbsp;|&nbsp;
    평균 Recall: <span>{_html.escape(avg_recall_str)}</span>
  </div>

  <h2>쿼리별 측정 결과</h2>
  <div class="table-wrap">
    <table>
      <thead>{table_header}</thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</body>
</html>"""

    return html_str


# ── task-113: nDCG 모드 ───────────────────────────────────────────────────────

def _load_gold_set(path: Path, allow_shortfall: bool = False) -> list[dict]:
    """gold_set.jsonl 로드 + schema validation.

    필수 필드: id, stratum, query, expected_node_ids, source, source_line.
    16건(8/8)이 아니면 RuntimeError + exit 1 (fail loud).
    allow_shortfall=True 시 v3 §12 risk 진단용 부분 진행 허용.
    """
    if not path.exists():
        print(f"오류: gold set 파일 없음: {path}", file=sys.stderr)
        sys.exit(1)
    entries: list[dict] = []
    required = ("id", "stratum", "query", "expected_node_ids", "source",
                "source_line")
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"gold_set.jsonl L{lineno} 파싱 실패: {exc}"
                ) from exc
            for k in required:
                if k not in e:
                    raise RuntimeError(
                        f"gold_set.jsonl L{lineno} 필수 필드 누락: {k}"
                    )
            # v4.2 5 stratum 유효값
            valid_strata = {
                "gold_2part", "gold_simple", "gold_folder",
                "gold_miss_time_range", "gold_manual",
            }
            if e["stratum"] not in valid_strata:
                raise RuntimeError(
                    f"gold_set.jsonl L{lineno} stratum 값 invalid: "
                    f"{e['stratum']} (v4.2 valid: {valid_strata})"
                )
            entries.append(e)
    # v4.2 stratum 별 size 집계
    sizes = {s: 0 for s in (
        "gold_2part", "gold_simple", "gold_folder",
        "gold_miss_time_range", "gold_manual"
    )}
    for e in entries:
        sizes[e["stratum"]] = sizes.get(e["stratum"], 0) + 1
    if not allow_shortfall:
        # v4.2 target: 8+2+5+8+15 = 38 (gold_manual 0 허용 — scope 외)
        targets = {"gold_2part": 8, "gold_simple": 2, "gold_folder": 5,
                   "gold_miss_time_range": 8, "gold_manual": 0}
        for name, target in targets.items():
            if sizes[name] < target:
                raise RuntimeError(
                    f"{name} target {target} 미달 (actual {sizes[name]}). "
                    "--allow-shortfall 로 진단 진행 가능."
                )
    else:
        print(
            f"[kpi_bench] WARN --allow-shortfall: gold_set v4.2 부분 추출 "
            f"({', '.join(f'{n}={c}' for n, c in sizes.items())} total={len(entries)})",
            file=sys.stderr,
        )
    return entries


def _time_range_probe(adapter, query: str) -> int:
    """miss_only 엔트리의 time_range_hit 카운터.

    기준: production adapter 가 query 에 대해 1건 이상 반환하면 hit=1, else 0.
    """
    try:
        ranked = adapter.search(query, top_k=10)
    except Exception:
        return 0
    return 1 if ranked else 0


def _aggregate_stratum_metrics(stratum_results: list) -> dict:
    """stratum별 3-stage metric 평균 (None 제외)."""
    def _avg_or_none(key: str) -> "float | None":
        vals = [r[key] for r in stratum_results
                if r.get(key) is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "n": len(stratum_results),
        "ingest_coverage": _avg_or_none("ingest_coverage"),
        "retrieval_recall": _avg_or_none("retrieval_recall"),
        "routing_hit": _avg_or_none("routing_hit"),
        "ndcg_production": _avg_or_none("ndcg_production"),
        "ndcg_rawfts_chunk": _avg_or_none("ndcg_rawfts_chunk"),
    }


def _check_regression_gates(
    current_kpi: dict, baseline_kpi: "dict | None",
    current_stratum_kpis: "dict | None" = None,
    baseline_stratum_kpis: "dict | None" = None,
) -> dict:
    """v4.2 §5 회귀 게이트 — stratum별 + 평균 둘 다 검사 (BLOCK/WARN).

    Round 5 qa-functional CRITICAL 보완: stratum별 비교 구현.

    Args:
        current_kpi: 평균 KPI dict
        baseline_kpi: 평균 baseline KPI
        current_stratum_kpis: {stratum_name: {metric: value, ...}}
        baseline_stratum_kpis: {stratum_name: {metric: value, ...}}

    Returns: {status: PASS|WARN|BLOCK, reasons: list[str]}
    """
    if baseline_kpi is None:
        return {"status": "PASS", "reasons": ["baseline 없음 (첫 실행)"]}
    reasons = []
    status = "PASS"
    THRESHOLDS = {
        "retrieval_recall": (0.05, "BLOCK"),
        "ingest_coverage": (0.03, "BLOCK"),
        "routing_hit": (0.10, "WARN"),
    }

    def _eval(scope: str, cur_dict: dict, base_dict: dict) -> None:
        nonlocal status
        for metric, (threshold, level) in THRESHOLDS.items():
            cur = cur_dict.get(metric)
            base = base_dict.get(metric)
            if cur is None or base is None:
                continue
            delta = cur - base
            if delta < -threshold:
                reasons.append(
                    f"[{scope}] {metric} {delta:+.3f} "
                    f"(threshold -{threshold}) → {level}"
                )
                if level == "BLOCK":
                    status = "BLOCK"
                elif level == "WARN" and status != "BLOCK":
                    status = "WARN"

    # 평균 비교
    _eval("avg", current_kpi, baseline_kpi)
    # stratum별 비교 (어느 한 stratum이 위반하면 트리거)
    if current_stratum_kpis and baseline_stratum_kpis:
        for stratum, cur_s in current_stratum_kpis.items():
            base_s = baseline_stratum_kpis.get(stratum)
            if base_s:
                _eval(stratum, cur_s, base_s)

    if not reasons:
        reasons.append("모든 metric 정상 (vs baseline)")
    return {"status": status, "reasons": reasons}


def _update_trend_json(
    trend_path: Path, manifest: dict, kpi: dict,
    stratum_kpis: dict, regression: dict,
    drift_warning: bool = False,
) -> tuple:
    """trend.json 누적 (v4.2 §5 단일 runner 가정).

    Round 5 보완: stratum_kpis 누적 + drift_warning 기록 (qa-functional MAJOR).

    Returns: (baseline_avg, baseline_stratum_kpis) — 둘 다 dict | None
    """
    runs: list[dict] = []
    if trend_path.exists():
        try:
            runs = json.loads(trend_path.read_text(encoding="utf-8")).get("runs", [])
        except (json.JSONDecodeError, OSError):
            runs = []
    # baseline 계산 (평균)
    baseline = None
    baseline_stratum: "dict | None" = None
    keys = ("ingest_coverage", "retrieval_recall", "routing_hit",
            "ndcg_production", "ndcg_rawfts_chunk")
    if len(runs) >= 7:
        last7 = runs[-7:]
        baseline = {}
        for k in keys:
            vals = [r["metrics"].get(k) for r in last7
                    if r.get("metrics", {}).get(k) is not None]
            baseline[k] = sum(vals) / len(vals) if vals else None
        # stratum별 baseline 도 7-run rolling
        baseline_stratum = {}
        for stratum in ("gold_2part", "gold_simple", "gold_folder"):
            baseline_stratum[stratum] = {}
            for k in keys:
                vals = [
                    r.get("stratum_metrics", {}).get(stratum, {}).get(k)
                    for r in last7
                ]
                vals = [v for v in vals if v is not None]
                baseline_stratum[stratum][k] = (
                    sum(vals) / len(vals) if vals else None
                )
    elif len(runs) >= 1:
        baseline = runs[-1].get("metrics", {})
        baseline_stratum = runs[-1].get("stratum_metrics", {})
    # 새 run append
    runs.append({
        "ts": manifest["generated_at"],
        "manifest_sha": manifest.get("routing_spec_sha", ""),
        "metrics": kpi,
        "stratum_metrics": stratum_kpis,
        "drift_warning": drift_warning,
        "regression_status": regression["status"],
    })
    runs = runs[-30:]
    trend_path.parent.mkdir(parents=True, exist_ok=True)
    trend_path.write_text(
        json.dumps({"runs": runs}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return baseline, baseline_stratum


def _render_ndcg_html(manifest: dict, results: dict, output: Path) -> None:
    """nDCG 모드 HTML 리포트 (v4.2: 5 stratum + Stage Attribution).

    헤더에 manifest 주요 항목 (gold_sha256, gdi_client_sha256, env, dates) 포함.
    """
    # v4.2: 5 stratum + 3-stage metric + miss_only
    measurable = []
    for s in ("gold_2part", "gold_simple", "gold_folder"):
        measurable.extend(results.get(s, []))
    miss_only = results.get("gold_miss_time_range", [])

    def _avg(vals: list[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    real_ndcg_prod = [r["ndcg_production"] for r in measurable
                      if r.get("ndcg_production") is not None]
    real_ndcg_raw = [r["ndcg_rawfts_chunk"] for r in measurable
                     if r.get("ndcg_rawfts_chunk") is not None]
    miss_hits = [r["time_range_hit"] for r in miss_only
                 if "time_range_hit" in r]

    avg_prod = _avg(real_ndcg_prod)
    avg_raw = _avg(real_ndcg_raw)
    miss_hit_rate = (sum(miss_hits) / len(miss_hits)) if miss_hits else 0.0
    # 3-stage metric 평균
    avg_ingest = _avg([r["ingest_coverage"] for r in measurable
                        if r.get("ingest_coverage") is not None])
    avg_recall = _avg([r["retrieval_recall"] for r in measurable
                        if r.get("retrieval_recall") is not None])
    avg_routing = _avg([r["routing_hit"] for r in measurable
                         if r.get("routing_hit") is not None])
    real_log = measurable  # 호환 (기존 row 렌더링)

    manifest_rows = "".join(
        f"<tr><th>{_html.escape(str(k))}</th>"
        f"<td>{_html.escape(json.dumps(v, ensure_ascii=False))}</td></tr>"
        for k, v in manifest.items()
    )

    def _fmt(v) -> str:
        return f"{v:.3f}" if isinstance(v, (int, float)) else "—"

    def _real_row(r: dict, i: int) -> str:
        return (
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{_html.escape(r.get('id', ''))}</td>"
            f"<td>{_html.escape(r.get('seam', ''))}</td>"
            f"<td>{_html.escape(r.get('query', ''))[:50]}</td>"
            f"<td>{_fmt(r.get('ingest_coverage'))}</td>"
            f"<td>{_fmt(r.get('retrieval_recall'))}</td>"
            f"<td>{_fmt(r.get('routing_hit'))}</td>"
            f"<td>{_fmt(r.get('ndcg_production'))}</td>"
            f"<td>{_fmt(r.get('ndcg_rawfts_chunk'))}</td>"
            "</tr>"
        )

    def _miss_row(r: dict, i: int) -> str:
        return (
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{_html.escape(r.get('id', ''))}</td>"
            f"<td>{_html.escape(r.get('query', ''))}</td>"
            f"<td>{r.get('time_range_hit', 0)}</td>"
            "</tr>"
        )

    real_rows = "\n".join(_real_row(r, i + 1) for i, r in enumerate(real_log))
    miss_rows = "\n".join(_miss_row(r, i + 1) for i, r in enumerate(miss_only))

    html_str = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>task-113 nDCG Report</title>
  <style>
    body {{ font-family: -apple-system, 'Segoe UI', sans-serif;
            background: #f8fafc; color: #1e293b; padding: 24px; }}
    h1 {{ font-size: 1.5rem; margin-bottom: 8px; }}
    h2 {{ font-size: 1.1rem; margin: 24px 0 8px 0; }}
    .summary {{ background: #fff; border-radius: 8px; padding: 16px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem;
             background: #fff; }}
    th, td {{ padding: 8px 12px; border-bottom: 1px solid #e2e8f0;
              text-align: left; }}
    th {{ background: #f1f5f9; font-weight: 600; }}
    .manifest th {{ width: 30%; background: #eef2ff; }}
  </style>
</head>
<body>
  <h1>task-113 nDCG Benchmark Report</h1>

  <h2>Summary (v4.2 3-stage Metric)</h2>
  <div class="summary">
    <p><strong>Stage 1 ingest_coverage</strong>: {avg_ingest:.3f} (BLOCK if -3%p)</p>
    <p><strong>Stage 2 retrieval_recall@10</strong>: {avg_recall:.3f} (BLOCK if -5%p)</p>
    <p><strong>Stage 3 routing_hit_rate</strong>: {avg_routing:.3f} (WARN if -10%p)</p>
    <hr/>
    <p>보조 nDCG@10 (production): <strong>{avg_prod:.3f}</strong> (informational)</p>
    <p>보조 nDCG@10 (rawFTS chunks): <strong>{avg_raw:.3f}</strong> (informational)</p>
    <p>miss_only time_range_hit rate: <strong>{miss_hit_rate:.1%}</strong>
       ({sum(miss_hits)}/{len(miss_hits)})</p>
  </div>

  <h2>Manifest</h2>
  <table class="manifest">
    {manifest_rows}
  </table>

  <h2>Stage Attribution per Gold Entry (v4.2)</h2>
  <table>
    <thead><tr><th>#</th><th>ID</th><th>Seam</th><th>Query</th>
      <th>Stage 1 (cov)</th><th>Stage 2 (recall)</th><th>Stage 3 (hit)</th>
      <th>nDCG prod</th><th>nDCG raw</th></tr></thead>
    <tbody>{real_rows}</tbody>
  </table>

  <h2>miss_only (TIME_RANGE)</h2>
  <table>
    <thead><tr><th>#</th><th>ID</th><th>Query</th>
      <th>time_range_hit</th></tr></thead>
    <tbody>{miss_rows}</tbody>
  </table>
</body>
</html>"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_str, encoding="utf-8")


def run_ndcg_bench(
    db_path: str,
    gold_path: str,
    ref_date: str,
    output_path: str,
    adapter_kind: str = "production",
    allow_shortfall: bool = False,
) -> tuple[dict, dict]:
    """task-113 메인 엔트리.

    Returns: (manifest, results)
    """
    from production_adapter import ProductionSearchAdapter  # noqa: E402
    from routing_replay import RoutingReplay  # noqa: E402
    from bench_fts import (  # noqa: E402
        compute_ingest_coverage,
        compute_retrieval_recall,
        compute_routing_hit,
    )

    gold_p = Path(gold_path)
    entries = _load_gold_set(gold_p, allow_shortfall=allow_shortfall)
    gold_sha256 = hashlib.sha256(gold_p.read_bytes()).hexdigest()

    # ── adapter + routing_replay 준비 ──────────────────────────────────────
    adapter = ProductionSearchAdapter(cache_db_path=db_path)
    spec_path = Path(__file__).resolve().parent.parent / "data" / "routing_spec.yaml"
    replay: RoutingReplay | None = None
    routing_drift_warning = False
    if spec_path.exists():
        try:
            replay = RoutingReplay(
                str(spec_path), adapter,
                slack_bot_root=Path(adapter.slack_bot_root),
            )
            routing_drift_warning = replay.drift_warning
        except RuntimeError as e:
            print(f"[kpi_bench] routing_replay 비활성: {e}", file=sys.stderr)

    # ── manifest (v4.2: routing_spec_sha + routing_drift_warning) ──────────
    routing_spec_sha = ""
    if spec_path.exists():
        routing_spec_sha = hashlib.sha256(spec_path.read_bytes()).hexdigest()[:16]
    manifest: dict = {
        "task_id": "task-113",
        "design_version": "v4.2",
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "reference_date": ref_date,
        "adapter": adapter_kind,
        "gold_set_sha256": gold_sha256,
        "gold_set_path": str(gold_p.resolve()),
        "gold_set_size": len(entries),
        "env": {
            k: os.environ.get(k, "")
            for k in (
                "SLACK_BOT_PATH",
                "CHUNK_SEARCH_ENABLED",
                "GDI_MODE",
                "TESSDATA_PREFIX",
                "_QUERY_PREPROCESSOR_ENABLED",  # v4.1 §6.3.4
                "_TAXONOMY_ENABLED",            # v4.1 §6.3.4
            )
        },
        "sqlite_version": sqlite3.sqlite_version,
        "cache_warmup": adapter.warmup_done,
        "routing_spec_sha": routing_spec_sha,
        "routing_drift_warning": routing_drift_warning,
    }
    manifest.update(adapter.version_stamp)

    # ── conn (read-only) ───────────────────────────────────────────────────
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    # v4.2: 5 stratum 결과 dict
    results: dict = {
        "gold_2part": [], "gold_simple": [], "gold_folder": [],
        "gold_miss_time_range": [], "gold_manual": [],
    }
    # seam 매핑 (gold_2part → ask_claude_2part 등)
    seam_map = {
        "gold_2part": "ask_claude_2part",
        "gold_simple": "gdi_simple_search",
        "gold_folder": "folder_ai",
        "gold_manual": "ask_claude_2part",  # manual은 2part 가정
    }
    try:
        for e in entries:
            stratum = e["stratum"]
            query = e["query"]
            expected = e["expected_node_ids"]
            entry_result = {"id": e["id"], "query": query}

            if stratum == "gold_miss_time_range":
                # seam 미적용 — production adapter 직접 호출 (1건이라도 반환 시 hit)
                hit = _time_range_probe(adapter, query)
                entry_result["time_range_hit"] = hit
                results[stratum].append(entry_result)
                continue

            seam = seam_map.get(stratum, "ask_claude_2part")

            # Stage 1: ingest_coverage (expected_tokens 기반)
            # v4.2: expected_tokens 는 query 의 token-set (자동 분리)
            expected_tokens = [t for t in query.split() if len(t) >= 2]
            stage1_vals = []
            for nid in expected:
                cov = compute_ingest_coverage(expected_tokens, nid, conn)
                if cov is not None:
                    stage1_vals.append(cov)
            ingest_cov = (
                sum(stage1_vals) / len(stage1_vals) if stage1_vals else None
            )
            entry_result["ingest_coverage"] = ingest_cov

            # Stage 2/3: routing_replay 또는 production adapter
            ranked: list = []
            try:
                if replay is not None:
                    ranked = replay.replay(seam, query, top_k=10)
                else:
                    ranked = adapter.search(query, top_k=10)
            except NotImplementedError:
                # _chain_lookup stub (folder_ai full chain)
                ranked = adapter.search(query, top_k=10)
            except Exception:
                ranked = []

            entry_result["ranked"] = ranked
            entry_result["seam"] = seam

            # Stage 2 + Stage 3
            entry_result["retrieval_recall"] = compute_retrieval_recall(
                ranked, expected, k=10
            )
            entry_result["routing_hit"] = compute_routing_hit(ranked, expected)

            # 보조: nDCG@10
            ndcg_prod, p95_prod = measure_ndcg_production(
                adapter, query, expected
            )
            ndcg_raw, p95_raw = measure_ndcg_raw_fts(
                conn, query, expected, use_chunks=True
            )
            entry_result["ndcg_production"] = ndcg_prod
            entry_result["p95_prod_ms"] = p95_prod
            entry_result["ndcg_rawfts_chunk"] = ndcg_raw
            entry_result["p95_raw_ms"] = p95_raw

            results[stratum].append(entry_result)
    finally:
        conn.close()

    # ── KPI 집계 (5 stratum 평균 — gold_miss_time_range / gold_manual 제외) ──
    measurable_strata = ("gold_2part", "gold_simple", "gold_folder")
    all_measurable = []
    for s in measurable_strata:
        all_measurable.extend(results.get(s, []))
    kpi_average = {
        "ingest_coverage": _avg_metric(all_measurable, "ingest_coverage"),
        "retrieval_recall": _avg_metric(all_measurable, "retrieval_recall"),
        "routing_hit": _avg_metric(all_measurable, "routing_hit"),
        "ndcg_production": _avg_metric(all_measurable, "ndcg_production"),
        "ndcg_rawfts_chunk": _avg_metric(all_measurable, "ndcg_rawfts_chunk"),
    }
    # Round 5 보완: stratum별 KPI 추가 (회귀 게이트 stratum별 비교 입력)
    stratum_kpis: dict = {}
    for s in measurable_strata:
        rows = results.get(s, [])
        if rows:
            stratum_kpis[s] = {
                "ingest_coverage": _avg_metric(rows, "ingest_coverage"),
                "retrieval_recall": _avg_metric(rows, "retrieval_recall"),
                "routing_hit": _avg_metric(rows, "routing_hit"),
                "ndcg_production": _avg_metric(rows, "ndcg_production"),
                "ndcg_rawfts_chunk": _avg_metric(rows, "ndcg_rawfts_chunk"),
            }

    # ── trend.json 누적 + 회귀 게이트 (v4.2 §5) ────────────────────────────
    out = Path(output_path)
    trend_path = out.parent / "trend.json"
    baseline_kpi, baseline_stratum_kpis = _update_trend_json(
        trend_path, manifest, kpi_average, stratum_kpis,
        {"status": "PENDING"},
        routing_drift_warning,
    )
    regression = _check_regression_gates(
        kpi_average, baseline_kpi,
        current_stratum_kpis=stratum_kpis,
        baseline_stratum_kpis=baseline_stratum_kpis,
    )
    # trend.json에 정확한 regression status 갱신 — 마지막 entry만 수정
    if trend_path.exists():
        try:
            data = json.loads(trend_path.read_text(encoding="utf-8"))
            if data.get("runs"):
                data["runs"][-1]["regression_status"] = regression["status"]
                trend_path.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except Exception:
            pass

    manifest["kpi_average"] = kpi_average
    manifest["regression"] = regression

    # ── HTML + manifest 저장 ────────────────────────────────────────────────
    _render_ndcg_html(manifest, results, out)
    manifest_path = out.with_name(out.stem + "_manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    results_path = out.with_name(out.stem + "_results.json")
    results_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest, results


def _avg_metric(rows: list, key: str) -> "float | None":
    """metric 평균 (None 제외)."""
    vals = [r[key] for r in rows if r.get(key) is not None]
    return sum(vals) / len(vals) if vals else None


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="200쿼리 KPI 벤치마크 + HTML 리포트 생성 (task-112) + nDCG (task-113)"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="측정 대상 DB 경로 (기본: config.DB_PATH)"
    )
    parser.add_argument(
        "--compare",
        default=None,
        help="비교 기준 DB 경로 (before; --db가 after)"
    )
    parser.add_argument(
        "--output",
        default="reports/kpi_report.html",
        help="HTML 리포트 출력 경로 (기본: reports/kpi_report.html)"
    )
    parser.add_argument(
        "--queries",
        default=None,
        help="쿼리 JSON 파일 경로 (기본: data/bench_queries_200.json)"
    )
    parser.add_argument(
        "--chunk-mode",
        action="store_true",
        help="chunks_fts(청크 기반) 측정 — task-104 활성화 후 실제 운영 경로 KPI. "
             "기본값은 search_fts(node-level)."
    )
    # task-113 nDCG 모드
    parser.add_argument(
        "--metric",
        choices=("recall", "ndcg"),
        default="recall",
        help="측정 지표 선택. 기본 recall (task-112). ndcg → task-113 모드."
    )
    parser.add_argument(
        "--adapter",
        choices=("production", "raw"),
        default="production",
        help="--metric ndcg 시 adapter. production (Slack Bot unified_search) | raw."
    )
    parser.add_argument(
        "--gold",
        default=None,
        help="--metric ndcg 시 gold_set.jsonl 경로 (16건)."
    )
    parser.add_argument(
        "--reference-date",
        default="2026-04-24",
        help="--metric ndcg 시 reference date (determinism §5.2). 기본 2026-04-24."
    )
    parser.add_argument(
        "--allow-shortfall",
        action="store_true",
        help=(
            "--metric ndcg 시 gold_set v4.2 38건 미달 진단 진행 허용 "
            "(v3 §12 risk realized 검증용)."
        ),
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help=(
            "v4.2 §5: trend.json 누적 + 회귀 게이트 동작. BLOCK 시 exit 1, "
            "WARN 시 exit 0 (informational)."
        ),
    )
    args = parser.parse_args()

    # DB 경로 결정
    if args.db:
        db_path = args.db
    else:
        project_root = Path(__file__).resolve().parent.parent
        sys.path.insert(0, str(project_root))
        from src import config as _cache_config  # noqa: E402
        db_path = str(_cache_config.DB_PATH)

    if not Path(db_path).exists():
        print(f"오류: DB 파일 없음: {db_path}", file=sys.stderr)
        sys.exit(1)

    # ── task-113 nDCG 모드 분기 ────────────────────────────────────────────
    if args.metric == "ndcg":
        gold = args.gold or "data/gold_set.jsonl"
        adapter_kind = args.adapter
        if adapter_kind != "production":
            print(
                "경고: task-113 v3 는 adapter=production 만 정식 지원. "
                "raw 는 비교용입니다.",
                file=sys.stderr,
            )
        # Round 5 qa-structural M-2: SHA prefix [:12] → [:16] 통일 (manifest 와 일치)
        gdi_sha = ""
        try:
            slack_path = os.environ.get("SLACK_BOT_PATH", "")
            if slack_path:
                gp = Path(slack_path) / "gdi_client.py"
                if gp.exists():
                    gdi_sha = hashlib.sha256(gp.read_bytes()).hexdigest()[:16]
        except Exception:
            pass
        print(
            f"[kpi_bench] adapter={adapter_kind} "
            f"gdi_client_sha256={gdi_sha} "
            f"reference_date={args.reference_date}"
        )
        gold_p = Path(gold)
        if gold_p.exists():
            gold_sha = hashlib.sha256(gold_p.read_bytes()).hexdigest()[:16]
            # Round 5 qa-structural M-1: v4.2 5 stratum 카운트 출력
            try:
                import json as _json
                stratum_counts: dict = {}
                with open(gold_p, encoding="utf-8") as f:
                    for ln in f:
                        ln = ln.strip()
                        if not ln:
                            continue
                        try:
                            e = _json.loads(ln)
                            s = e.get("stratum", "?")
                            stratum_counts[s] = stratum_counts.get(s, 0) + 1
                        except Exception:
                            pass
                n_total = sum(stratum_counts.values())
                detail = ", ".join(f"{k}={v}" for k, v in stratum_counts.items())
                print(
                    f"[kpi_bench] gold_set={n_total} entries "
                    f"({detail}) gold_sha256={gold_sha}"
                )
            except Exception:
                pass
        print(
            "[kpi_bench] running v4.2 5-stratum × 10 repeats × 3-stage metric "
            "+ time_range probes"
        )
        manifest, results = run_ndcg_bench(
            db_path=db_path,
            gold_path=gold,
            ref_date=args.reference_date,
            output_path=args.output,
            adapter_kind=adapter_kind,
            allow_shortfall=args.allow_shortfall,
        )
        # v4.2: 5 stratum 합산 (gold_2part + gold_simple + gold_folder)
        measurable = []
        for s in ("gold_2part", "gold_simple", "gold_folder"):
            measurable.extend(results.get(s, []))
        kpi = manifest.get("kpi_average", {})
        regression = manifest.get("regression", {"status": "PASS"})

        print(
            f"[kpi_bench] Stage 1 ingest_coverage={kpi.get('ingest_coverage') or 0.0:.3f}"
        )
        print(
            f"[kpi_bench] Stage 2 retrieval_recall@10={kpi.get('retrieval_recall') or 0.0:.3f}"
        )
        print(
            f"[kpi_bench] Stage 3 routing_hit_rate={kpi.get('routing_hit') or 0.0:.3f}"
        )
        print(
            f"[kpi_bench] avg_ndcg_production={kpi.get('ndcg_production') or 0.0:.3f} (informational)"
        )
        print(
            f"[kpi_bench] avg_ndcg_rawfts_chunk={kpi.get('ndcg_rawfts_chunk') or 0.0:.3f} (informational)"
        )
        if args.baseline:
            print(
                f"[kpi_bench] regression status: {regression['status']} "
                f"({'; '.join(regression.get('reasons', []))})"
            )
        print(f"리포트 저장: {args.output}", file=sys.stderr)
        # 회귀 게이트: BLOCK → exit 1, WARN/PASS → exit 0
        if args.baseline and regression["status"] == "BLOCK":
            print("BLOCK")
            sys.exit(1)
        print(regression["status"] if args.baseline else "PASS")
        sys.exit(0)

    # 쿼리 파일 경로 결정
    if args.queries:
        queries_path = Path(args.queries)
    else:
        queries_path = Path(__file__).resolve().parent.parent / "data" / "bench_queries_200.json"

    queries = load_queries(queries_path)

    # --compare DB 처리
    before_results = None
    before_kpi = None
    if args.compare:
        if not Path(args.compare).exists():
            print(f"오류: 비교 DB 파일 없음: {args.compare}", file=sys.stderr)
            sys.exit(1)
        conn_before = sqlite3.connect(args.compare)
        before_results = run_benchmark(conn_before, queries, chunk_mode=args.chunk_mode)
        before_kpi = compute_kpi(before_results, get_body_text_ratio(conn_before))
        conn_before.close()

    # 측정 실행
    conn = sqlite3.connect(db_path)
    results = run_benchmark(conn, queries, chunk_mode=args.chunk_mode)
    body_ratio = get_body_text_ratio(conn)
    kpi = compute_kpi(results, body_ratio)
    conn.close()

    # HTML 생성 및 저장
    ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    html_str = render_html(kpi, results, before_kpi, before_results, db_path, ts)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_str, encoding="utf-8")
    print(f"리포트 저장: {output}", file=sys.stderr)

    # KPI PASS/FAIL 판정 (None = gray = PASS)
    miss_ok = kpi["miss_rate"] < 0.40
    tr_ok = kpi["tr_hit_rate"] is None or kpi["tr_hit_rate"] >= 0.80
    p90_ok = (
        before_kpi is None
        or (
            kpi["p90_ms"] is not None
            and before_kpi["p90_ms"] is not None
            and kpi["p90_ms"] < before_kpi["p90_ms"] * 0.70
        )
    )
    body_ok = kpi["body_text_ratio"] is None or kpi["body_text_ratio"] >= 0.98

    all_pass = miss_ok and tr_ok and p90_ok and body_ok
    print("PASS" if all_pass else "FAIL")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
