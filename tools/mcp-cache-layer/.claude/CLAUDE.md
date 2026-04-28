# MCP Cache Layer — 프로젝트 개발 규칙

## 프로젝트 개요
Wiki/Jira/GDI MCP 호출 비용을 줄이는 로컬 캐시/인덱싱 시스템.

## 구조
```
mcp-cache-layer/
├── docs/           # 설계 문서 (ARCHITECTURE, SCHEMA, IMPLEMENTATION_PLAN)
├── src/            # 소스 코드
│   ├── config.py         # 설정
│   ├── models.py         # SQLite 스키마 + 마이그레이션
│   ├── cache_manager.py  # 캐시 CRUD + 유효성 검사
│   ├── cache_logger.py   # 운영 로그 + 성능 타이밍 (logs/cache_ops.log)
│   ├── sync_engine.py    # Full Ingest + Delta Sync
│   └── exporters.py      # Claude Code용 JSON export
├── cache/          # 런타임 캐시 (.gitignore)
│   ├── mcp_cache.db
│   └── exports/    # JSON 인덱스 파일
└── logs/           # 런타임 로그 (.gitignore)
```

## 연동 대상
- **Slack Bot**: `D:\Vibe Dev\Slack Bot\` — wiki_client.py에서 캐시 레이어 사용
- **Claude Code**: `cache/exports/` JSON 파일을 Read 도구로 직접 조회

## 개발 규칙
1. Python 표준 라이브러리 우선 (sqlite3, json, re, logging)
2. 캐시 실패 시 항상 기존 MCP 호출 fallback
3. 새 프로세스/포트/데몬 생성 금지 (사내 보안)
4. DB 스키마 변경은 models.py MIGRATIONS dict에 순차 추가
