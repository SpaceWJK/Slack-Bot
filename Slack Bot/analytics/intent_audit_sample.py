"""
intent_audit_sample.py — task-129.6 monthly Intent 정확도 audit script.

운영 1주일 후 (또는 monthly) 실행하여 intent_audit.jsonl에서
100건 sample을 추출하고 정확도 분포를 보고.

사용법:
  cd "D:/Vibe Dev/Slack Bot/Slack Bot"
  python scripts/intent_audit_sample.py --sample 100 --since 7d
  python scripts/intent_audit_sample.py --report

출력:
  - 100건 sample (Master 직접 검증용)
  - request_type 분포
  - ai_failed 비율
  - ambiguity_notes top patterns
"""

import argparse
import json
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter


def _parse_since(since: str) -> datetime:
    """'7d', '30d', '24h' 등 relative time → datetime."""
    if since.endswith("d"):
        days = int(since[:-1])
        return datetime.now() - timedelta(days=days)
    elif since.endswith("h"):
        hours = int(since[:-1])
        return datetime.now() - timedelta(hours=hours)
    else:
        return datetime.fromisoformat(since)


def _load_records(log_path: Path, since: datetime) -> list:
    """intent_audit.jsonl에서 since 이후 레코드 로드."""
    records = []
    if not log_path.exists():
        return records
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = datetime.fromisoformat(rec["ts"])
                if ts >= since:
                    records.append(rec)
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return records


def _print_distribution(records: list) -> None:
    """request_type 분포 + ai_failed 비율."""
    rt_counter = Counter(r.get("request_type", "?") for r in records)
    domain_counter = Counter(r.get("domain", "?") for r in records)
    failed_count = sum(1 for r in records if r.get("ai_failed"))

    print(f"\n=== Intent Audit Distribution (총 {len(records)}건) ===")
    print(f"AI 실패: {failed_count}건 ({failed_count/max(len(records),1)*100:.1f}%)")
    print("\nDomain 분포:")
    for d, c in domain_counter.most_common():
        print(f"  {d}: {c}건 ({c/max(len(records),1)*100:.1f}%)")
    print("\nrequest_type 분포:")
    for rt, c in rt_counter.most_common():
        print(f"  {rt or '(NULL)'}: {c}건 ({c/max(len(records),1)*100:.1f}%)")


def _print_ambiguity_top(records: list, top_n: int = 10) -> None:
    """ambiguity_notes top patterns."""
    notes = [r.get("ambiguity_notes", "") for r in records if r.get("ambiguity_notes")]
    counter = Counter(notes)
    print(f"\n=== ambiguity_notes Top {top_n} ===")
    for note, c in counter.most_common(top_n):
        print(f"  {c}회: {note[:80]!r}")


def _print_sample(records: list, n: int = 100) -> None:
    """random sample n건 출력 (Master 직접 검증용)."""
    if len(records) <= n:
        sample = records
    else:
        sample = random.sample(records, n)

    print(f"\n=== Master 검증 샘플 ({len(sample)}건) ===")
    print("Master 직접 정확도 라벨링:")
    for i, r in enumerate(sample, 1):
        prefix = r.get("text_prefix", "")
        rt = r.get("request_type", "?")
        ambig = r.get("ambiguity_notes", "")
        domain = r.get("domain", "?")
        ai_failed = r.get("ai_failed", False)
        marker = "✗ AI fail" if ai_failed else "✓"
        print(f"  {i:3d}. [{domain}/{rt}] {marker} prefix={prefix!r}{(' | ambig=' + ambig[:40]) if ambig else ''}")


def main():
    parser = argparse.ArgumentParser(description="task-129.6 Intent audit sample")
    parser.add_argument("--sample", type=int, default=100, help="Master 검증 sample size")
    parser.add_argument("--since", default="7d", help="기간 (7d, 30d, 24h, ISO)")
    parser.add_argument("--report", action="store_true", help="분포 보고만 (sample 없이)")
    args = parser.parse_args()

    log_path = Path(__file__).resolve().parent.parent / "logs" / "intent_audit.jsonl"
    print(f"audit log: {log_path}")
    print(f"기간: since={args.since}")

    since = _parse_since(args.since)
    records = _load_records(log_path, since)
    if not records:
        print(f"\n경고: {since.isoformat()} 이후 레코드 0건. 운영 중인지 또는 task-129.5 wiring 적용됐는지 확인.")
        return 1

    _print_distribution(records)
    _print_ambiguity_top(records)
    if not args.report:
        _print_sample(records, args.sample)

    print(f"\n[완료] {len(records)}건 audit 완료. Master 직접 정확도 라벨링 의무.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
