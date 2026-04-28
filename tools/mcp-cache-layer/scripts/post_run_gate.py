"""post_run_gate.py — task-114 R-4 BLOCK 게이트 (routing_hit_rate 격상 1.0 절대값).

design v4 §13.2 M-1 정정: kpi_v4_manifest.json 읽기 + manifest["kpi_average"]["routing_hit"]
kpi_bench.py:477 THRESHOLDS 절대 변경 금지 — 별도 후처리 assertion으로 격상.
"""
import json
import sys
from pathlib import Path

MANIFEST = Path("D:/Vibe Dev/QA Ops/mcp-cache-layer/reports/kpi_v4_manifest.json")

data = json.loads(MANIFEST.read_text(encoding="utf-8"))
avg_routing_hit = data.get("kpi_average", {}).get("routing_hit")
if avg_routing_hit is None or avg_routing_hit < 1.0:
    print(f"BLOCK: routing_hit_rate={avg_routing_hit} < 1.0", file=sys.stderr)
    sys.exit(1)
print(f"PASS: routing_hit_rate={avg_routing_hit:.3f}")
sys.exit(0)
