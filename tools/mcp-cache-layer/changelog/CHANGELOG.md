# MCP Cache Layer - 변경 이력

> Wiki/Jira/GDI MCP 호출 비용을 줄이는 로컬 캐시/인덱싱 시스템.
> DB: `cache/mcp_cache.db` (SQLite, ~957MB)

---

## [2.3.0] - 2026-03-14

### 확인 (상태 검증)
- **Enrichment Pipeline 전체 완료 확인**
  - GDI: 15,275/15,275 (100%)
  - Jira: 8,968/8,968 (100%)
  - Wiki: 2,527/2,843 (88.9% — 실질 100%, 316건은 본문 없는 페이지)
- **Wiki body repair 실행**: 177건 모두 Wiki 원본 자체가 빈 페이지 (매크로/제목 전용)
- **Wiki 고아 노드**: 11건 repair 시도 → 전부 원본에 ancestor 없음 (루트 레벨)

### Enrichment 아키텍처 (구현 완료 확인)
- `src/enrichment.py`: EnrichmentEngine (summary/keywords 자동 생성)
- `src/cache_manager.py`: enrichment 헬퍼 3개 (get_nodes_missing, update, stats)
- `src/config.py`: ENRICH_BATCH_SIZE=500, SUMMARY_MAX_CHARS=300, KEYWORDS_MAX_COUNT=10
- `scripts/auto_sync.py`: sync 완료 후 자동 enrichment 실행
- **Slack Bot 연동**: wiki_client.py + gdi_client.py에서 summary/keywords 활용 중

---

## [2.2.0] - 2026-03-13

### 추가
- **Enrichment Engine** (`src/enrichment.py` 신규)
  - Wiki: body 첫 3문장 요약 + labels 키워드
  - Jira: issue_type/status/priority + description 2문장 + labels/components
  - GDI: 경로 카테고리 + body 2문장 + 경로 세그먼트 키워드
  - 빈도 기반 키워드 추출 (한글 2~6자 + 영문 3자+, 불용어 제외)
  - CLI: `python -m src.enrichment --all|--wiki|--jira|--gdi [--force]`
- **cache_manager.py 헬퍼 3개**
  - `get_nodes_missing_enrichment(source_type, limit)`
  - `update_enrichment(node_id, summary, keywords)`
  - `get_enrichment_stats()` (by_source별 통계)
- **auto_sync.py enrichment 통합**: sync 완료 후 자동 실행

---

## [2.1.0] - 2026-03-12

### 추가
- **Slack Bot GDI/Jira enrichment 연동**
  - `wiki_client.py`: get_page_content에 summary/keywords 반환
  - `gdi_client.py`: _local_unified_search에 keywords 칼럼 검색 + 우선 정렬
  - `exporters.py`: export_wiki_index에 summary/keywords 포함
- **DB Schema v3**: folder_index 테이블 추가

---

## [2.0.0] - 2026-03-11

### 추가
- **DB Schema v2**: doc_content에 summary/keywords 컬럼 추가
- **Repair 기능** (`src/sync_engine.py`)
  - `repair_missing_content(source_type)`: body 누락 노드 MCP 재가져오기
  - `repair_parent_ids(source_type)`: 고아 노드 parent 복구

---

## [1.0.0] - 2026-03-10 (초기 버전)

### 구현
- 3계층 캐시: L1 인메모리 (5분) → L2 SQLite → L3 MCP HTTP
- Wiki/Jira/GDI 동기화 (full ingest + delta sync)
- JSON export (Claude Code 연동)
- 자동 동기화 스크립트 (Task Scheduler 4~8시간 주기)

### 데이터 현황
- 총 노드: 27,263개 (Wiki 3,020 + Jira 8,968 + GDI 15,275)
- DB 크기: ~957MB
