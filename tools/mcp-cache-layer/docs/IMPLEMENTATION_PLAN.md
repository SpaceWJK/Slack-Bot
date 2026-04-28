# MCP Cache Layer — 구현 절차

> 최종 수정: 2026-03-10
> 관련 문서: [ARCHITECTURE.md](./ARCHITECTURE.md) | [SCHEMA.md](./SCHEMA.md)

## 구현 로드맵 (Phase 1: Wiki)

Phase 1은 Wiki MCP 캐시를 목표로 하며, 6개 Step으로 진행한다.

---

### Step 0: 프로젝트 기반 구축

**목표**: 프로젝트 폴더, 설계 문서, 개발 환경 준비

| 항목 | 상태 |
|------|------|
| 폴더 구조 생성 (`src/`, `docs/`, `cache/`, `logs/`) | ✅ 완료 |
| ARCHITECTURE.md 작성 | ✅ 완료 |
| SCHEMA.md 작성 | ✅ 완료 |
| IMPLEMENTATION_PLAN.md 작성 (이 문서) | ✅ 완료 |
| .gitignore | ✅ 완료 |
| requirements.txt | ✅ 완료 |
| src/__init__.py | ✅ 완료 |

---

### Step 1: 병목 계측

**목표**: 현재 MCP 호출 비용을 정량화하여 개선 기준선(baseline) 확보

**작업 내용:**
1. `D:\Vibe Dev\Slack Bot\logs\wiki_query.log` 분석
   - 총 쿼리 수, 평균 응답시간, 에러율
   - 동일 페이지 재조회 비율 (캐시 hit 가능 비율)
   - 가장 빈번한 조회 대상 Top 10
2. 결과를 `docs/BASELINE_REPORT.md`로 기록

**산출물:**
- `docs/BASELINE_REPORT.md` — 병목 계측 리포트

**판단 기준:**
- 재조회 비율 > 20% → 캐시 효과 높음, 진행
- 재조회 비율 < 5% → 캐시보다 쿼리 최적화가 우선 (전략 재검토)

---

### Step 2: 핵심 모듈 구현

**목표**: SQLite DB + 캐시 CRUD 핵심 코드 작성

**작업 내용:**

#### 2-1. `src/config.py` — 설정 모듈
```python
# 주요 설정값
DB_PATH = "D:/Vibe Dev/QA Ops/mcp-cache-layer/cache/mcp_cache.db"
DEFAULT_TTL_HOURS = 24
MAX_DOCS_PER_QUERY = 20
MAX_BODY_CHARS = 20000
CLEANUP_DAYS = 30
```

#### 2-2. `src/models.py` — DB 스키마 + 마이그레이션
- SCHEMA.md의 DDL을 코드로 구현
- `init_db()`: DB 파일 생성 + 스키마 적용
- `migrate()`: 버전 확인 → 미적용 마이그레이션 순차 실행
- WAL 모드 활성화

#### 2-3. `src/cache_manager.py` — 캐시 매니저
- `CacheManager` 클래스:
  - `__init__(db_path)`: DB 연결 + 스키마 초기화
  - `get_node(source_type, source_id)` → node dict or None
  - `get_page(title, space_key)` → (node, meta, content) or None
  - `put_page(source_type, source_id, title, ...)` → node_id
  - `update_content(node_id, body_raw, body_text, ...)` → None
  - `is_stale(node_id)` → bool (TTL + 수정일 비교)
  - `mark_fresh(node_id)` → None (cached_at 갱신)
  - `cleanup(days=30)` → int (삭제 건수)
  - `get_tree(source_type, space_key)` → list[node] (하위 트리)
  - `get_stats()` → dict (노드 수, 용량, 최근 동기화 등)

**산출물:**
- `src/config.py`
- `src/models.py`
- `src/cache_manager.py`

**검증:**
- 단독 실행으로 DB 생성 + CRUD 테스트
- `python -c "from src.cache_manager import CacheManager; cm = CacheManager(); print(cm.get_stats())"`

---

### Step 3: Slack Bot Wiki 캐시 통합

**목표**: wiki_client.py에 캐시 레이어 삽입, 기존 기능 무파괴

**작업 내용:**

#### 3-1. wiki_client.py 수정

**변경 최소화 원칙:**
- 기존 함수 시그니처 변경 없음
- 캐시 실패 시 기존 MCP 호출 로직으로 fallback
- 캐시는 optional — import 실패 시 캐시 없이 동작

```python
# wiki_client.py 상단 추가
try:
    import sys
    sys.path.insert(0, "D:/Vibe Dev/QA Ops/mcp-cache-layer")
    from src.cache_manager import CacheManager
    _cache = CacheManager()
    _CACHE_ENABLED = True
except Exception:
    _cache = None
    _CACHE_ENABLED = False
```

**캐시 적용 대상 함수:**
1. `search_page(title, space_key, fetch_full)` — 가장 빈번한 호출
2. `get_latest_descendant(parent_title, space_key)` — 트리 탐색

**캐시 미적용 (Phase 1):**
- `cql_search` 직접 호출 경로 — 복잡한 CQL 쿼리는 Phase 2+

#### 3-2. 로그에 캐시 hit/miss 기록

```python
# wiki_query.log에 필드 추가
log_wiki_query(
    user=user,
    query=query,
    cache_status="hit" | "miss" | "stale" | "disabled",
    ...
)
```

**산출물:**
- `wiki_client.py` 수정본 (캐시 통합)
- 로그 포맷 확장

**검증:**
1. 봇 재시작 → `/wiki search QA` → 기존과 동일 응답 (miss)
2. 동일 쿼리 재실행 → 응답 속도 향상 + 로그에 `cache_status=hit`
3. 캐시 모듈 임시 제거 → 기존 로직 fallback 정상 동작

---

### Step 4: 동기화 엔진

**목표**: Full Ingest + Delta Sync 구현

**작업 내용:**

#### 4-1. `src/sync_engine.py`
- `SyncEngine` 클래스:
  - `__init__(cache_manager, mcp_session)`: 의존성 주입
  - `full_ingest(source_type, space_key)`: 전체 트리+메타+본문 수집
  - `delta_sync(source_type, space_key)`: 마지막 동기화 이후 변경분만 갱신
  - `_sync_wiki_space(space_key, full=False)`: Wiki CQL 기반 동기화
  - `_build_tree(pages)`: 페이지 목록 → 트리 구조 생성
  - `_log_sync(...)`: sync_log 테이블 기록

#### 4-2. Wiki Full Ingest 흐름
```
1. CQL: type=page AND space=QASGP (전체 페이지 목록)
2. 각 페이지: source_id, title, parent_id, last_modified 추출
3. nodes 테이블에 upsert
4. doc_meta 테이블에 메타데이터 저장
5. (선택) doc_content에 본문 저장 (Top-N 페이지만)
6. sync_log에 결과 기록
```

#### 4-3. Wiki Delta Sync 흐름
```
1. sync_log에서 마지막 성공 시각 조회
2. CQL: type=page AND space=QASGP AND lastModified > '{last_sync}'
3. 변경된 페이지만 nodes/doc_meta/doc_content 갱신
4. sync_log에 결과 기록
```

**산출물:**
- `src/sync_engine.py`

**검증:**
- `python -m src.sync_engine --full wiki QASGP` → 전체 수집 실행
- `python -m src.sync_engine --delta wiki QASGP` → 증분만 수집
- `cache/mcp_cache.db`에 데이터 적재 확인

---

### Step 5: Slack Bot /wiki-sync 커맨드

**목표**: 수동 동기화 트리거 + 캐시 상태 확인 커맨드

**작업 내용:**

#### 5-1. slack_bot.py에 핸들러 추가

```
/wiki-sync          → Delta Sync 실행 (빠름)
/wiki-sync full     → Full Ingest 실행 (느림, 확인 필요)
/wiki-sync status   → 캐시 통계 (노드 수, 최근 동기화 시각, DB 용량)
```

#### 5-2. Slack Bot 시작 시 자동 Delta Sync (선택)
- `slack_bot.py` 시작 루틴에 Delta Sync 호출 추가
- 비동기 실행 (시작 지연 방지)

**산출물:**
- `slack_bot.py` 수정본 (/wiki-sync 핸들러)

**검증:**
- `/wiki-sync status` → 캐시 통계 표시
- `/wiki-sync` → Delta Sync 실행 + 결과 요약

---

### Step 6: Claude Code 캐시 활용 규칙

**목표**: Claude Code 세션에서 캐시 우선 조회하도록 행동 규칙 설정

**작업 내용:**

#### 6-1. JSON Export
- `src/exporters.py`:
  - `export_wiki_index(space_key)` → `cache/exports/wiki_{space_key}_index.json`
  - 트리 구조 + 요약 필드만 (본문 제외, 토큰 절약)
  - 동기화 후 자동 실행

#### 6-2. CLAUDE.md 규칙 추가
```markdown
## MCP 캐시 활용 규칙
1. Wiki/GDI 조회 전 `cache/exports/` 디렉토리의 인덱스 파일 확인
2. 인덱스에 있는 정보로 답변 가능하면 MCP 호출 생략
3. 인덱스에 없거나 최신성이 필요하면 MCP 도구 사용
```

**산출물:**
- `src/exporters.py`
- `cache/exports/` 디렉토리 + JSON 파일
- `.claude/CLAUDE.md` 규칙 추가 (Slack Bot 프로젝트)

---

## 진행 상태 요약

| Step | 내용 | 상태 | 산출물 |
|------|------|------|--------|
| 0 | 프로젝트 기반 | ✅ 완료 | 폴더, 설계 문서, `__init__.py`, `requirements.txt` |
| 1 | 병목 계측 | ✅ 완료 | `BASELINE_REPORT.md` — 재조회율 70% 확인 |
| 2 | 핵심 모듈 | ✅ 완료 | `config.py`, `models.py`, `cache_manager.py` (CRUD 테스트 통과) |
| 3 | Wiki 캐시 통합 | ✅ 완료 | `wiki_client.py` 수정 — 3계층 캐시 + `cache_status` 로깅 |
| 4 | 동기화 엔진 | ✅ 완료 | `sync_engine.py` — Full Ingest + Delta Sync |
| 5 | /wiki-sync 커맨드 | ✅ 완료 | `slack_bot.py`에 `/wiki-sync`, `/wiki-sync full`, `/wiki-sync status` |
| 6 | Claude Code 규칙 | ✅ 완료 | `exporters.py` + `.claude/CLAUDE.md` 규칙 |
| 7 | 캐시 시스템 로깅 | ✅ 완료 | `cache_logger.py` + `logs/cache_ops.log` — 운영 로그 + 디버그 추적 |

---

## Phase 2+ (향후)

| Phase | 대상 | 상태 | 의존성 |
|-------|------|------|--------|
| Phase 2 | GDI MCP 캐시 통합 | ✅ 완료 | Phase 1 완료 |
| Phase 3 | Jira MCP 캐시 통합 | ✅ 완료 | Phase 1 완료 |
| Phase 4 | 구조화 요약 (LLM 자동 생성) | ⬜ 미착수 | Phase 1-3 데이터 축적 |
| Phase 5 | 로컬 검색 인덱스 (FTS5/벡터) | ⬜ 미착수 | Phase 4 완료 |

### Phase 2 산출물 (2026-03-10)
- `docs/PHASE2_GDI_DESIGN.md` — 설계 문서
- `gdi_client.py` — 3계층 캐시 통합 (L1 메모리 → L2 SQLite → L3 MCP)
- `config.py` — GDI TTL 상수 (`GDI_FOLDER_TTL_HOURS=6`, `GDI_FILE_TTL_HOURS=24`)
- 캐시 대상: `list_files_in_folder` (폴더 목록), `search_by_filename` (파일 내용)

### Phase 3 산출물 (2026-03-10)
- `jira_client.py` — Jira MCP 클라이언트 + 3계층 캐시 (L1 메모리 → L2 SQLite → L3 MCP)
- `slack_bot.py` — `/jira` 슬래시 커맨드 핸들러 + 헬퍼 함수
- `config.py` — Jira TTL 상수 (`JIRA_ISSUE_TTL_HOURS=0.17`, `JIRA_PROJECT_TTL_HOURS=1`, `JIRA_PROJECTS_TTL_HOURS=24`)
- 캐시 대상: `get_issue` (이슈 10분), `get_project` (프로젝트 1시간), `get_all_projects` (목록 24시간)
- 캐시 미적용: `jql_search` (검색 결과는 매번 달라짐)
