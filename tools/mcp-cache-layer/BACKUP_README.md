# mcp-cache-layer 백업

> Slack Bot repo 안의 코드 백업본. 운영 source는 `D:/Vibe Dev/QA Ops/mcp-cache-layer/`.

## 백업 내역

- **백업 일자**: 2026-04-28 (세션 34)
- **백업 사유**: 기존 별도 repo 미연동 상태 → Slack Bot repo로 통합 백업 (Master 명시)
- **백업 source**: `D:/Vibe Dev/QA Ops/mcp-cache-layer/` (로컬, git remote 미설정)
- **백업 시점 commits**: 6330f12 / 6ad79a4 / 2f3d701 (세션 33 task-115/116 작업)

## 포함 / 제외

### 포함 (코드 + 메타)
- `src/` — cache_manager / sync_engine / config / models 등
- `scripts/` — auto_sync / load_gdi / kpi_bench / post_run_gate 등
- `tests/`, `docs/`, `templates/`, `changelog/`, `data/`
- `requirements.txt`

### 제외 (운영 데이터 / 자동 생성 / 좀비)
- `cache/*.db` (5.4GB mcp_cache.db + 2.2GB bak)
- `gdi-repo/` (S3 mirror)
- `logs/`, `reports/`, `__pycache__/`, `venv/`, `tmp/`, `.pytest_cache/`
- `*.bak*`, `*.pyc`

## 동기화 정책

- **운영 source가 truth**: `D:/Vibe Dev/QA Ops/mcp-cache-layer/`
- **본 백업은 PC 손상 시 코드 복원용**: 단방향 백업
- **변경 시점**: Master 명시 또는 세션 종결 시 robocopy 갱신

## 복원 방법

```bash
# Slack Bot repo clone 후
cp -r tools/mcp-cache-layer/* "D:/Vibe Dev/QA Ops/mcp-cache-layer/"
# venv 재생성 + cache 재적재
cd "D:/Vibe Dev/QA Ops/mcp-cache-layer"
python -m venv venv
venv/Scripts/activate && pip install -r requirements.txt
python scripts/auto_sync.py  # cache 재적재
```
