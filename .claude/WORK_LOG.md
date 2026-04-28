# Slack Bot 작업 히스토리

> 최신 항목이 위에 위치합니다.
> 각 세션 종료 시 Claude가 자동 업데이트합니다.

---

## 2026-04-28 (화) — 세션 34 (task-118/119 종결 + task-114 carryover cleanup)

### task-118 종결 (gdi_client log injection sanitize, OWASP A09 closeout)
- 변경: `Slack Bot/gdi_client.py`에 `_sanitize_log_field` 헬퍼 신설 + `log_gdi_query` 5개 필드(query/result/error/user_name/user_id) ASCII `|` → 전각 ｜ 치환
- 검증: 단위 10/10 PASS, failure_analyzer 회귀 0 (51→53), KPI exit=0 routing_hit_rate=1.000
- sec-code: CONDITIONAL_PASS → user_name 확장(Slack display_name attacker-controlled) → PASS
- advisor /plan 1회 + /advise 2회 (Phase C 자율성 매트릭스 정합)
- commit: Slack Bot 0c814c6, AI Brain 81df62e (push 완료)
- 봇 재시작: PID 15196, Bolt running 10:15:43

### task-119 종결 (action 필드 defense-in-depth, task-118 follow-up)
- 변경: `log_gdi_query`의 `action` 인자도 `_sanitize_log_field` 적용 (현재 호출처 모두 하드코딩이라 즉시 위험 0이지만 향후 user input 흐름 추가 시 즉시 vector화 차단)
- agent-workflow 5단계 단축 (Step 0.5/2/4 skip)
- 검증: 단위 11/11 PASS, KPI exit=0 routing_hit_rate=1.000
- commit: Slack Bot ae9c944, AI Brain 7927d94 (push 완료)
- 봇 재시작: Bolt running 10:46:00

### task-114 carryover — pre-fix legacy line cleanup
- 발견: `logs/gdi_query.log` line 42-57 비표준 14 lines (n=6 + 인접 n=3 7쌍) 모두 task-114 commit 6569362 escape patch 이전 발생한 1회성 잔존 데이터
- task-120 폐기 → task-114 carryover로 재정의 (advisor reconcile 결과)
- 절차: Consumer grep(영향 0) → `.bak` 백업 → dry-run preview → apply → verify
- 결과: 65→58 lines (merged 7), failure_analyzer warnings **14→0**, parsed entries **51→58 (+7)**
- KPI post_run_gate exit=0 routing_hit_rate=1.000 유지
- 산출: AI Brain `_workspace/tasks/task-118/cleanup_gdi_log_legacy.py` + `cleanup_report.md`

### Master 검수 carryover (Claude 권한 부재)
- C-2 spec mismatch (s3_server.py:774): spec 폐기 vs 신규 요구사항 결정
- C-3 Epicseven depth=7 8건: GDI MCP 또는 AWS 권한으로 S3 직접 비교 필요
- ~~Railway CLI 깨짐~~: 폐기 (Master 정정 2026-04-28 — Railway 사용 안 함)
- mcp-cache-layer 미push 3건 (세션 33 작업): 별건 처리

### 운영 정합 (pm2 ecosystem)
- 8 앱 pm2 정상 가동 + windowsHide:true (CMD 창 0)
- 신규 등록: issue-dashboard(5174) / issue-dashboard-api(9100) / kis-server(9091)
- ecosystem.config.cjs commit: Slack Bot 1d548cc / work-automation 289dac4 / AI Brain 0e2b896

### task-115 안건 C 폐기 (non-issue)
- mcp_server 4 인스턴스 진단 결과 좀비 아님 — Claude Desktop이 prompt-cultivation MCP session별로 spawn하는 정상 패턴
- process-cleanup.js MCP_BLACKLIST에 mcp_server 이미 보호 등록 → 추가 작업 불필요
- reflect 295 (insight, env-windows): 1차 직관 (kill) 무효화 / parent alive + MCP_BLACKLIST 보호 = 정상

### CHANGELOG v1.7.3 (2026-04-28)
- 보안 + 데이터 보정 + 운영 + 진단 4 카테고리 정리
- Patch 버전 (기능 추가 없음, 버그 수정 + 보안 + 운영 정합)

---

## 2026-04-25 (토) — 세션 30 연장 2 (task-113 설계 3라운드 + advisor 3회)

### task-113 Step 3 완료 (design-approved v3)
- **3 라운드 설계 검수 + advisor 3회 자문**
  - Round 1 (qa-structural + qa-functional): FAIL (CRITICAL 4, MAJOR 4)
  - Round 2 (web-architect + data-quality-engineer): FAIL (CRITICAL 5, MAJOR 7)
  - Round 3 (원 검수자 재검증): **PASS** 양쪽 모두
- Advisor 자문:
  - 1회: 설계 방향 (unified_search + adapter seam)
  - 2회: Round 2 conflict reconcile (3-way JOIN 오진 철회, descope 결정)
  - 3회: 최종 pre-commit (dual path patch 리스크, baseline 하향, determinism 추가 3종)

### 최종 설계 핵심
- **측정 경로**: `GdiClient.unified_search` 직접 호출 (`ProductionSearchAdapter`)
- **Gold set**: 16건 (8 real + 8 miss) — manual_rewrite 15건 R9로 descope
- **sys.path 변형 금지** → `importlib.util.spec_from_file_location`
- **Determinism 9 artifacts**: tie-break, reference-date, env pin, FTS tokenizer, gold SHA, alias SHA, page cache warmup, WAL freeze, 5-module SHA
- **Baseline pre-commit**: real_log nDCG@10 = 0.60±0.07 (prod) / 0.45±0.10 (rawFTS)

### Procedural Lesson
> "rowid 매핑 주장은 INSERT-site binding code file:line 인용 후 runtime-output 해석."
Round 1 CRITICAL-3 (search_fts.rowid ≠ nodes.id) 오진 원인 — INSERT 코드 추적 없이 title 불일치 현상만 보고 결론. Round 2 web-architect가 실측으로 철회.

### Step 4 이관 (다음 세션)
- 위치: `D:\Vibe Dev\AI Brain\_workspace\tasks\task-113\resume_prompt.md`
- 🔴 Slack Bot gdi_client.py dual path 필수: `_local_unified_search:822-835` + `_local_chunk_search:627-641`

---

## 2026-04-25 (토) — 세션 30 마감 (task-082 에픽 완결)

### 🎯 최대 성과
- **task-082 에픽 종결** — 기준 6/6 충족
- **task-105 blocked 해제** — 8단계 워크플로 풀적용 (Step 1~7)
  - 성능: 20분/32건 → 152.6초/19,088건 (~90,000배 개선)
  - 원인: DB 쿼리 TEMP B-TREE + 풀스캔 (regex 아님)
  - 해결: MIGRATIONS[9] partial index + seek pagination + WAL checkpoint
  - Step 3 에이전트가 MIGRATIONS[8] 충돌 CRITICAL 1건 포착 (task-108 jira_mirror와)
- **task-112 완료** — kpi_bench --chunk-mode 확장 + 재측정

### 측정 결과
- search_fts baseline: Miss 82.5% / 시간범위 17.9%
- chunks_fts 모드: Miss 81.9% / 시간범위 21.4% (+19% relative)
- 실사용 smoke test 20쿼리: **100% hit** (운영 경로 개선 입증)
- 괴리 사유: kpi_bench 합성 200쿼리 한계 → task-113 신규 후속

### 큐 최종 상태 (v18)
- completed 108 / pending 1 (task-113) / blocked 0 / cancelled 1

---

## 2026-04-24 (금) — 세션 30 연장 (즉시조치 + task-105 진단 + Tesseract + 파서 확장)

### 추가 완료
- **task-082 종결 기준 6개 중 4개 충족**
  - 기준3 SQL parity: `D:\Vibe Dev\AI Brain\_workspace\tasks\task-082\sql_parity.md`
  - 기준5 20쿼리 smoke: **100% hit** (청크 18건, 캐시 2건, 평균 87.8ms)
  - 기준6 이월 버킷: `_workspace/backlog/ai_context_bucket.md` + `retrieval_bucket.md`
  - task-082 상태: `awaiting_deploy` (기준 1/2는 task-105 해제 후)

- **task-105 성능 진단 완료 (수정은 세션 31)**
  - 원인: regex 아님. `LEFT JOIN doc_meta + ORDER BY n.id + offset=0 재페치`가 매 배치 TEMP B-TREE 생성
  - 해결: Partial Index + seek pagination (`_workspace/tasks/task-105/diagnosis.md`)

- **Tesseract 설치 + 파서 확장** (task-110 completed)
  - `winget install UB-Mannheim.TesseractOCR` (관리자 권한 불필요)
  - 한국어 언어팩: `C:\Users\es-wjkim\tessdata\kor.traineddata`
  - `file_parsers.py` 3종 확장:
    - `_autodetect_tesseract()`: Windows 표준 경로 자동 탐지
    - `parse_pptx`: 발표자 노트 추출 추가
    - `parse_html` 신규: alt/title/figure/figcaption/base64 OCR
  - E2E: 74MB PPTX 89슬라이드 75이미지 36.1초 파싱 + 노트 추출 검증

### 교훈
- DB 성능 문제는 cProfile보다 **EXPLAIN QUERY PLAN** 먼저
- Tesseract 같은 도구는 **winget 관리자 권한 불필요** (이전 task-110 "관리자 권한 필요" 가정 교정)
- 세션 내 종결 기준 엄격 체크 후 status 전환 (조기 종결 롤백 사고)

---

## 2026-04-24 (금) — 세션 30 (awaiting_deploy 전수 감사 + Phase A/B 배포)

### 배경
세션 29 Brain 복구 후, awaiting_deploy 14건 전수 감사 + 묶음별 격리 배포 (bench 신뢰성 전제 원칙 준수).

### 완료 (11건)
- **Phase A — bench 신뢰성 검증**
  - `bench_fts.py` 재측정: Recall@10=0.33, P95<5ms — 세션 28 베이스라인과 동일 (회귀 없음)
  - `search_fts` 오염 검증: gdi 19,170 / wiki 0 (오염 제거 반영됨)

- **Phase B 묶음 1 — task-103 body_text 백필**
  - `scripts/body_text_audit.py --apply`: 77건 FTS 재인덱싱
  - UPDATED 0 / STILL_SHORT 77 (원본 파일이 100자 미만이라 backfill 대상 없음)
  - fill_rate 99.6% + FTS coverage 100% 유지

- **Phase B 묶음 3 — task-104 청크 빌더 전량 실행**
  - `scripts/chunk_builder.py --game {lordnine/chaoszero/epicseven} --apply`
  - 19,176 노드 → **1,131,070 청크** (세션 28 dry-run 예상 1.13M과 정확 일치)
  - DB 크기: 2.13GB → 5.09GB (청크 FTS 인덱스 포함)
  - `.env`에 `CHUNK_SEARCH_ENABLED=true` 추가 (봇 재시작 시 `_local_chunk_search()` 경로 활성)
  - 소요: 35.4분 (lordnine 56s + chaoszero 795s + epicseven 1276s)

- **task-106 캐시 레이어 재활성**
  - `.env` `GDI_MODE=local` 이미 적용 확인. `gdi_client.py:68` `_GDI_CACHE_ENABLED=True` 자동 로드 구조 검증

- **task-082/100/101/102/107/108/109/111** awaiting_deploy → completed 전환
  - 코드는 Slack Bot `9791cc0` / `e065cc7` 및 mcp-cache-layer `8773347`에 이미 푸시된 상태
  - Slack Bot push commit `1cb6f42` (문서 v1.7.2 CHANGELOG/WORK_LOG)

### 블로킹 / 보류
- **task-105 blocked (성능 이슈)**
  - `entity_extractor --gdi --apply` 실행 시 20분에 32건/19,170건 (0.2%) → 전체 예상 200시간
  - 원인 추정: regex catastrophic backtracking (긴 body_text 특정 패턴). dry-run 100건에서는 문제 없었음
  - 대책: 별도 세션에서 작은 샘플(`--node N`, `--game 단일`)로 프로파일링 → 코드 수정 후 재시도
- **task-110 보류 (Tesseract 설치)**
  - 코드 구현 완료 (커밋 `e267b47`), 바이너리는 관리자 권한 설치 필요
  - 별도 세션에서 설치 + 파서 최적화 (PPTX/HTML 이미지 OCR 통합)
- **task-112 보류 (회귀 벤치)**
  - dependency 중 task-105 blocked 상태
  - task-105 해결 후 전체 통합 회귀 벤치 재실행 필요

### 교훈 (Brain reflect 대상)
- **ISS-013 자기 비판 적용 교훈**: 대량 DB write 전에는 반드시 작은 샘플로 속도 벤치 먼저. `--game lordnine`(248건)로 56초 확인 → 전체 예상 72분 추정. task-105에서 이 단계 생략했다가 20분 소모.
- **격리 배포 효과 확인**: 묶음별(task-103 → task-104 → flag 활성) 실행으로 각 단계 독립 검증 가능. task-105 blocking 발생해도 task-104 진행 가능.
- **"awaiting_deploy" 의미 혼재**: 단순 push 필요/백필 필요/바이너리 필요/집계 필요가 섞여 있어 각각 다른 처리 필요. task_queue 스키마에 `deploy_blocker` 필드 추가 검토 대상.

---

## 2026-04-24 (금) — 세션 28 (task-082/110 종결)

### 완료
- **task-082: 검색 정확도 고도화 Phase A 종결**
  - `bench_fts.py` `measure()` SQL 수정: raw FTS MATCH → JOIN nodes+doc_content + `source_type='gdi'` + BM25 ORDER — gdi_client._local_unified_search() 패턴 이식
  - `search_fts` wiki 46개 오염 엔트리 삭제 (19,216 → 19,170)
  - KPI 베이스라인: Miss Rate 82.5%, Body Text 99.2%(PASS), P90 3.9ms
  - 커밋: `f3d50c9`
- **task-110: PPTX 이미지 OCR — Tesseract 한국어 (task-110)**
  - `file_parsers.py` `_ocr_image()` 추가: pytesseract+PIL, graceful degradation (Tesseract 미설치 시 경고 1회 후 스킵)
  - 4개 단위 테스트 (`tests/test_file_parsers_ocr.py`) — T-2/T-4/T-5/T-6 PASS
  - 커밋: `e267b47`
- **task-108: jira_mirror.py 버그 수정**
  - `MCPSession` → `McpSession` 클래스명 수정, 인증 헤더 누락 수정
  - 커밋: `19f9954`

### KPI 현황
| KPI | 결과 | 임계값 | 판정 |
|-----|------|--------|------|
| Miss Rate | 82.5% | <40% | FAIL (베이스라인 — 고도화 백로그 이관) |
| 시간범위 Hit | 17.9% | ≥80% | FAIL (베이스라인) |
| P90 레이턴시 | 3.9ms | - | 우수 |
| Body Text 비율 | 99.2% | ≥98% | PASS |

### 다음 단계 (백로그)
- AI 맥락 고도화 bucket: PPTX 텍스트 박스 누락 보완, XLSX 병합 셀, 이미지 OCR (Tesseract 설치 후)
- 구조적 검색 고도화 bucket: 하이브리드 BM25+임베딩, 날짜/타임레인지 필터, query 확장

---

## 2026-04-24 (금) — 세션 27

### 완료
- **task-108: Jira 최소 로컬 미러** (8단계 워크플로우 완료)
  - SQLite `jira_mirror` 테이블 (schema v8, mcp-cache-layer) — MCP 장애 fallback 전용
  - `jira_client.py`: `_mirror_search`/`_mirror_age_fn` 옵셔널 임포트, JIRA_USERNAME 하드코딩 제거
  - `slack_bot.py`: MCP 장애 시 "_(미러 기준 N분 전)_" UX 메시지 추가
- **task-111: CQL 병렬 검색** (8단계 워크플로우 완료)
  - `search/cql_parallel.py` 신규: `run_parallel_cql()` ThreadPoolExecutor + 5s 상한
  - `mcp_session.py`: `threading.Lock` thread-safety, `_initializing` double-init 방지
  - `wiki_client.py`: 1·2·3차 단계 for-loop → `run_parallel_cql` 교체 (최악 300s → 15s)

### 변경 파일
- `Slack Bot/jira_client.py`, `Slack Bot/slack_bot.py`
- `Slack Bot/mcp_session.py`, `Slack Bot/wiki_client.py`
- `Slack Bot/search/__init__.py` (신규), `Slack Bot/search/cql_parallel.py` (신규)

### 커밋
- `e065cc7` — feat: task-082 서브태스크 완료 — Jira 미러 + CQL 병렬화 → Railway 자동 재배포

---

## 2026-03-26 (수) — 세션 26

### 완료
- **Claude 대시보드 고도화** (8단계 워크플로우 완료)
  - Performance Risk 카드를 "Claude 자체 성능" + "MCP 운영 지표"로 분리 리디자인
  - System Status에 로컬 HTTP 서버 5개 헬스체크 추가 (9090/9091/5174/9100/10.5.31.110:9100)
  - 프로세스 설명 자동 매핑 (Bot→Slack QA Bot, KIS→KIS 대시보드 서버 등)
  - slack_bot.py에 jira/gdi elapsed_ms 타이밍 코드 추가
  - ops_metrics.db 쿼리에 elapsed_ms>0 필터, SQLite WAL 모드 적용
- **QA 검수 이슈 7개 수정 완료**

### 변경 파일
- `Slack Bot/slack_bot.py`: jira/gdi elapsed_ms 타이밍 추가
- `tools/s3_server.py`: 로컬 서버 헬스체크 + ops_metrics 쿼리 개선
- `tools/s3_admin.html`: Performance Risk 분리 + System Status 로컬 서버 UI

---

## 2026-03-25 (화) — 세션 25

### 완료
- **ISS-004 Brain 대시보드 Health Score 추가**: brain-metrics API에 health_score(0~100) 필드 추가, Brain 요약 카드에 Health Score/7일경험/7일일지/14일피드백 표시
- **Brain 대시보드 레이아웃 개선**: max-width:1200px 가로 확장 제한, font-size/padding 가독성 향상, 반응형 900px 이하 1열 전환
- **s3_admin.html과 s3_manager.html 동기**: Health Score + 레이아웃 변경을 s3_admin.html(실 운영 파일)에도 적용

### 변경 파일
- `tools/s3_server.py`: brain-metrics API에 health_score 추가 (brain_health.py SSOT 모듈 동적 로드)
- `tools/s3_admin.html`: Health Score UI + 레이아웃 CSS 개선
- `tools/s3_manager.html`: 동일 변경 적용

---

## 2026-03-16~17 (일~월) — 세션 24

### 완료
- **Sysops 대시보드 Brain 탭 완성** (`tools/s3_admin.html`, `tools/s3_manager.html`)
  - Brain 상단 상태바 추가: Live dot (활성/비활성/오류 3색) + 마지막 활동 시간 + "Auto 30s" 라벨
  - 보류/미완료 작업 카드 추가 (`pending_tasks` 테이블 → brain.db)
  - brain-grid `overflow-y: auto` 스크롤 수정 (tab-panel overflow:hidden 충돌 해결)
  - ThreadingHTTPServer 전환 (동시 API 요청 데드락 해결)

- **Prompt Cultivation MCP 서버 통합** (전역)
  - `~/.claude/settings.json`의 mcpServers는 Claude Code가 읽지 않는 것 확인
  - `claude mcp add -s user` 명령으로 `~/.claude.json`에 정상 등록 → **Connected**
  - `PYTHONIOENCODING=utf-8` 환경변수 추가
  - 실패 MCP 서버 정리: jina, sequential-thinking 제거
  - CLAUDE.md에 recall/reflect/feedback 3단계 루프 + MCP 도구명 명시

- **s3_server 자동 시작** (`tools/run_s3_server_silent.vbs`)
  - VBS 래퍼: pythonw.exe로 CMD 없이 실행
  - 시작 프로그램 폴더에 바로가기 등록 (로그인 시 자동 시작)

- **brain.db 스키마 확장**
  - `pending_tasks` 테이블 추가 (id, title, description, status, priority, source, domain)
  - SQLite timeout 5→30초 (`schema.py`)
  - 보류 작업 5건 시드 (MCP_Process_Cleanup, GDI Phase 1, weekly_batch, Context7 중복, Atlassian MCP)

- **Claude Desktop config 최적화**
  - 모든 MCP 서버: npx → node 직접 실행 (CMD 창 제거)
  - `coworkScheduledTasksEnabled: false`, `ccdScheduledTasksEnabled: false`
  - 백업: `claude_desktop_config.backup.20260316.json`

- **Task Scheduler 정리**
  - 중복 삭제: MCP_AutoSync, MCP_AutoSync_4h (MCP-AutoSync-Delta만 유지)
  - MCP_Process_Cleanup용 VBS 래퍼 생성 (`run_cleanup_silent.vbs`)

- **CLAUDE.md 전역 워크플로우 추가**
  - 8단계 개발 프로세스: 요구사항분석 → 설계 → 에이전트 검수 → 구현 → QA → 수정 → 재검수 → 완료

### 미완료 / 후속
- MCP_Process_Cleanup Task Scheduler를 VBS 래퍼로 전환 (관리자 권한 필요)
- Context7 Claude Desktop 중복 인스턴스 제거 (수동)
- Atlassian Cloud MCP 해제 (수동)

---

## 2026-03-12 (수) — 세션 23

### 완료
- **GDI 출처(source label) 표시 개선** (`slack_bot.py`)
  - 2-part/택소노미/통합검색 3개 경로 모두 실제 파일 경로 기반으로 변경
  - `search_data = None` 초기화 누락 버그 수정

- **키워드 규칙 대폭 확장** (hot reload)
  - `gdi_keyword_rules.json` v1.1: 1→17 규칙 (밸런스/스킬/장비/가챠/상점 등)
  - `jira_keyword_rules.json` v1.1: 2→10 규칙 (시간필터/미배정/버그/우선순위)
  - `wiki_keyword_rules.json` v1.1: 문서 보강

- **GDI MCP/S3 장애 분석** (코드 변경 없음)
  - S3 파일 업로드 정상 확인 (14,584건, 한글 인코딩 정상)
  - curl 한글 깨짐 → Windows CP949 터미널 표시 이슈 (S3 데이터 이상 아님)
  - **OpenSearch 인덱스 3개 전체 소실** 확인 (gdi_generic_xlsx/tsv/pptx 모두 404)
  - → LLMOps 팀에 인덱스 복구 + 리인덱싱 요청 필요

- **v1.6.1 릴리즈** + 문서 현행화 (CHANGELOG, WORK_LOG, DEV_RULES, README)

- **S3 업로드/다운로드 경량 페이지** 생성 (`tools/s3_manager.html`)

### 미완료 / 후속
- OpenSearch 인덱스 복구 요청 (LLMOps 팀)
- `GDI_MODE=cloud` 전환 (OpenSearch 복구 후)

---

## 2026-03-12 (수) — 세션 22

### 완료
- **GDI 로컬/클라우드 모드 스위치** (`gdi_client.py`)
  - `GDI_MODE` 환경변수 (`local` | `cloud`) 기반 전환 시스템
  - local 모드: 캐시(SQLite) 전용, MCP 폴백 완전 차단
  - cloud 모드: 기존 동작 유지 (캐시 → MCP 폴백)
  - MCP 호출 4개 지점 모드 분기:
    - `unified_search()`: local → SQLite LIKE 검색 대체
    - `search_by_filename()`: local → 캐시 전용
    - `list_files_in_folder()`: local → 캐시 전용
    - `get_file_content_full()`: local → 캐시 전용
  - 검증: unified_search 10건 반환, get_file_content_full 19,735자 반환

- **auto_sync.py 모드 스위치** (`mcp-cache-layer`)
  - `GDI_MODE`에 따라 `load_gdi_local` / `load_gdi` 자동 선택
  - 드라이런 검증: 2,794건 스캔, 오류 0

- **auto_sync 주기 변경**: 4시간→8시간 (08:00, 16:00, 00:00)
- **`.env` 설정 추가**: `GDI_MODE=local`

### 미완료 / 후속
- **Epicseven/Lordnine 적재**: Chaoszero만 완료
- **GDI MCP 서버 동기화 이슈 해결 시**: `GDI_MODE=cloud` 전환

### 로드맵

| # | 작업 | 상태 | 비고 |
|---|------|------|------|
| **Phase 1** | GDI 로컬/클라우드 모드 스위치 | ✅ 완료 | `GDI_MODE` 환경변수 기반 전환 |
| **Phase 2** | 로컬 원본 파싱 | ✅ 완료 | 2,934파일 0에러 (Test Result 포함) |
| **Phase 3** | 폴더 택소노미 인덱스 | ✅ 완료 | 질의해석 고도화 + 날짜 정규식 수정 완료 |

---

## 2026-03-12 (수) — 세션 21

### 완료
- **택소노미 질의 해석 고도화** (세션 20 후속)
  - `taxonomy_search()` — `question` 파라미터 추가, 키워드+질문 결합 파싱
    - 예) `검색어="카제나 2/4 3차"` + `질문="테스트 결과에서 FAIL?"` → 결합 파싱
    - `slack_bot.py` 2-part 모드에서 `question=question` 전달
  - e2e 시뮬레이션 테스트 8/8 PASS (커밋 `9733f68`)

- **Test Result 파일 적재**
  - 로컬 `gdi-repo/Chaoszero/Test Result/` 8개 날짜 폴더, 134개 파일 적재 (에러 0)
  - 적재 후 전체: Chaoszero 2,934파일 (95.2MB)

- **택소노미 폴더 날짜 정규식 수정** (`folder_taxonomy.py`)
  - **문제**: `_FOLDER_DATE_RE` 정규식이 `$` 앵커로 끝나 "260204 타겟" 형식 미매칭
    → Test Result의 `date_mmdd` 전부 NULL → 날짜 조건 질의 시 결과 0건
  - **수정**: `(?:\s.*)?$` 패턴 추가 — 공백 이후 임의 텍스트 허용
  - 인덱스 재빌드: Test Result 84폴더 전부 date_mmdd 정상 추출 (NULL 0개)
  - 검증 시뮬레이션 8/8 PASS (날짜+빌드+핫픽스+약어 조합)

- **일일 체크리스트 링크 추가** (`config.json`)
  - 서비스 장애, 커뮤니티 이슈: 제목 자체에 Wiki/Jira 링크
  - 핫픽스 내역, Next Checklist, TEST INFO, Release INFO: 제목 + `(EP7 / CZN)` 게임별 링크
  - 코드 수정 없이 config.json group_name만 변경 (mrkdwn 링크 문법)

### 미완료 / 후속
- **Epicseven/Lordnine 적재**: Chaoszero만 완료
- **Phase 1: MCP 청크 재조합 고도화**: 미착수

### 로드맵

| # | 작업 | 상태 | 비고 |
|---|------|------|------|
| **Phase 1** | MCP 청크 재조합 고도화 | 🔜 진행 예정 | `load_gdi.py` `_reconstruct_*()` |
| **Phase 2** | 로컬 원본 파싱 | ✅ 완료 | 2,934파일 0에러 (Test Result 포함) |
| **Phase 3** | 폴더 택소노미 인덱스 | ✅ 완료 | 질의해석 고도화 + 날짜 정규식 수정 완료 |

---

## 2026-03-12 (수) — 세션 20

### 완료
- **Phase 3: 폴더 택소노미 인덱스 — 핵심 모듈 + 통합**
  - `folder_taxonomy.py` 신규 (~690줄, `mcp-cache-layer/scripts/`)
    - 한영 별칭 사전: 게임명(카제나↔Chaoszero) + 카테고리(테스트결과↔Test Result)
    - 날짜 정규화: "2/4", "2월4일", "0204" → MMDD "0204" → YYMMDD/YYYYMMDD 매칭
    - 빌드 분류: 정규/핫픽스/범위/납품/클라이언트 등 10+ 실전 패턴
    - `FolderIndex`: gdi-repo 스캔 → SQLite folder_index 테이블 빌드 (79폴더)
    - `QueryParser`: 자연어 → {game, category, date_mmdd, build_type, build_num, source_type}
  - `models.py` v3 마이그레이션: folder_index 테이블 + 인덱스 3개
  - `load_gdi_local.py`: 적재 완료 후 `FolderIndex.build()` 자동 호출
  - `gdi_client.py`: 택소노미 옵셔널 임포트 + 3개 공개 함수
    - `taxonomy_search()`: 질의→폴더/파일 직접 해석 (MCP 호출 불필요)
    - `format_taxonomy_results()`: Slack 포맷
    - `get_taxonomy_context_text()`: Claude AI용 컨텍스트 추출
  - `slack_bot.py`: 택소노미 우선 → MCP 폴백 (통합검색 + 2-part 모드)
  - 통합 테스트 3건: 폴더/파일 해석 정상 확인

- **Phase 2: 로컬 원본 파싱 + 적재** (이전 세션에서 완료)
  - `file_parsers.py`: XLSX/TSV/PPTX/PNG 4형식 파서 (전수 배치 0에러)
  - `load_gdi_local.py`: Chaoszero 2,660파일 적재 완료
  - GDI MCP 서버 7,046줄 분석 → 다운로드 불가 확인

### 미완료 / 후속
- ~~택소노미 질의 해석 고도화~~: ✅ 세션 21에서 완료
- ~~e2e 시뮬레이션 테스트~~: ✅ 세션 21에서 완료
- **Epicseven/Lordnine 적재**: Chaoszero만 완료, 나머지 게임 미적재

### 로드맵

| # | 작업 | 상태 | 비고 |
|---|------|------|------|
| **Phase 1** | MCP 청크 재조합 고도화 | 🔜 진행 예정 | `load_gdi.py` `_reconstruct_*()` |
| **Phase 2** | 로컬 원본 파싱 | ✅ 완료 | 2,660파일 0에러 |
| **Phase 3** | 폴더 택소노미 인덱스 | ✅ 핵심 완료 | 질의해석 고도화 남음 |

---

## 2026-03-12 (수) — 세션 19

### 완료
- **GDI 파일 파서 완성** (`mcp-cache-layer/scripts/file_parsers.py`)
  - TSV `csv.field_size_limit` 오류 수정 (10MB로 확장) — 대용량 스토리 TSV 파싱 성공
  - 전체 배치 테스트 통과: XLSX 89건, TSV 727건(91,769행), PPTX 95건 — **0 에러**
  - Q&A 시뮬레이션 5건 검증: QA BVT Summary, 기획서, actor TSV, PPTX 기획서, 대용량 스토리
- **`load_gdi_local.py` 신규** (`mcp-cache-layer/scripts/`)
  - 로컬 `gdi-repo/` 디렉토리 순회 → `file_parsers.py`로 파싱 → SQLite 저장
  - `--delta` (신규만), `--test` (10건), `--all`, `--stats` CLI 지원
  - GDI 원본 파일 다운로드 가능 시 즉시 사용 가능한 상태
- **GDI MCP 원본 파일 다운로드 가능 여부 조사**
  - GDI MCP 서버 소스코드 7,046줄 전수 분석 → **다운로드 불가 확인**
  - 9개 도구 모두 텍스트 청크만 반환, 바이너리 파일 스트리밍/다운로드 기능 없음

### 의사결정
- **로컬 원본 파싱 방식 → Phase 2로 진행** (이전 보류에서 변경)
- **폴더 택소노미 인덱스 → Phase 3로 구현 완료**

### GDI 폴더 구조 분석 (택소노미 설계용)
- **날짜 형식 혼재**: Chaoszero Test Result=YYMMDD(260204), Update Review=YYYYMMDD(20260204)
- **빌드 명명 패턴**: `3-1차`~`3-9차`, `Hotfix 2차`, `정규 3차`, `1차빌드`
- **카테고리**: Test Result, TSV, Update Review, Live Issue (게임별 상이)
- 폴더 트리 데이터: `mcp-cache-layer/cache/exports/gdi_folder_tree.json`

---

## 2026-03-11 (화) — 세션 18

### 완료
- **v1.5.6 GDI 청크 메타데이터 정제**
  - `_clean_chunk_text()` 정규식 정제 함수: `load_gdi.py` + `gdi_client.py` 양쪽 적용
  - 적재 시(load_gdi.py): 청크 수집 단계에서 메타데이터 제거
  - 조회 시(gdi_client.py): `get_file_content_text()`, `get_file_content_full()`, `get_search_context_text()` 4개 경로
  - 기존 DB 5건 일괄 정제 (2,933자 절감, 10~23%)
- **v1.5.5 스케줄 발송 시각 분산 + README 현행화** (세션 17 후반)
  - config.json 5건 시각 변경 + README 버전/항목수/시각 업데이트 + Railway push

---

## 2026-03-11 (화) — 세션 17

### 완료
- **v1.5.4 Wiki 4단계 Fallback 파이프라인**
  - `slack_bot.py`: `_wiki_call_claude()` 분리 + `_NOT_FOUND_PATTERN` 감지 + 4단계 자동 fallback
  - `wiki_client.py`: `get_descendant_pages()`, `fetch_page_live()`, `search_content_live()` 신규 메서드
  - `wiki_client.py`: `get_page_by_title()` CQL 정확 매칭 우선 + 유사도 스코어링 개선
  - `slack_bot.py`: `_log_answer_miss()` — 모든 fallback 실패 시 `logs/answer_miss.log` 기록
  - `scripts/analyze_answer_miss.py` 신규 — 실패 분석 도구 (빈도/추이/개선제안/CSV)
  - py_compile 통과 + 봇 재시작 완료

---

## 2026-03-10 (화) — 세션 16

### 완료
- **v1.5.3 통합 응답 포맷 (3단 구조)**
  - `response_formatter.py` 신규: `parse_answer_sections()` + `format_ai_response()` + `ANSWER_FORMAT_INSTRUCTION`
  - `slack_bot.py`: 3개 claude_call 함수 포맷 교체 + 5개 프롬프트 지시문 추가
  - Jira: `source_url=jc._issue_url(key)` 전달 (이슈 링크 포함)
  - GDI: URL 없음 (텍스트 출처만)

- **v1.5.2a 메시지 만료 핫픽스**
  - 버그: `ack()` body 없음 → `replace_original=True`가 새 메시지 생성
  - 수정: `ack(text="⏳ 처리 중...")` + ExpiringResponder 항상 `replace_original=True`
  - `_call_count`/`is_first` 로직 제거

---

## 2026-03-10 (화) — 세션 15

### 완료
- **v1.5.2 답변 메시지 자동 만료**
  - `message_expiry.py` 신규: ExpiringResponder 래퍼 (threading.Timer + response_url POST)
  - `slack_bot.py`: import + main() 환경변수 + /wiki, /gdi, /jira 핸들러 래핑 (5개소)
  - `.env`: MESSAGE_EXPIRY_SECONDS=600, MESSAGE_EXPIRY_ENABLED=true 추가
  - 비적용: /claim, /wiki-sync, 스케줄러 알림
  - py_compile 통과 + 봇 재시작 완료

- **v1.5.1 규칙 기반 키워드→페이지/쿼리 매핑**
  - `keyword_rules.py` 신규: 공용 로더 + 3개 매칭 함수 (hot reload)
  - `wiki_keyword_rules.json` / `jira_keyword_rules.json` / `gdi_keyword_rules.json` 신규
  - `wiki_client.py`: Stage 0 키워드 규칙 매칭 추가 (기존 4단계 검색 앞)
  - `jira_client.py`: `_inject_before_order()` + 규칙 매칭 (question_to_jql, question_to_jql_variants)
  - `slack_bot.py`: GDI 2파트 파이프 핸들러 규칙 매칭 추가
  - py_compile 통과 + 봇 재시작 완료

### 논의
- 메시지 만료 기능: response_url 30분 제한 내에서 ephemeral 답변 교체 가능
- 10분 만료 → response_url replace_original 방식으로 구현 가능 (v1.5.2 예정)

---

## 2026-03-10 (화) — 세션 14

### 완료
- **v1.5.0: /claim 커맨드 + 안전 가드 + Wiki/Jira 버그 수정**
  - **Wiki ancestor CQL 버그 수정** (critical)
    - `game_aliases.py`: `wiki_ancestor_id` 필드 추가 (에픽세븐=58043932, 카제나=650589593)
    - `wiki_client.py`: ancestor CQL을 텍스트 → 정수 ID 기반으로 수정
    - 문제: `ancestor = "에픽세븐"` → 항상 파싱 에러 → Stage 3 폴백으로 카제나 질문에도 에픽세븐 페이지 반환
  - **Jira 활성 이슈 정의 수정**
    - `jira_client.py`: `_DONE_STATUSES`에 "닫힘" 추가
  - **읽기 전용 안전 가드** (신규)
    - `safety_guard.py`: 쓰기 의도 감지 정규식 + READ_ONLY_INSTRUCTION 상수
    - 2계층 방어: 파이프 핸들러 사전 필터 + Claude 프롬프트 삽입
    - 적용 대상: /wiki, /gdi, /jira 전체
  - **`/claim` 슬래시 커맨드** (신규)
    - `claim_handler.py`: 카테고리 파싱, JSON 저장, 조회, 통계, 로깅
    - 카테고리: 개선, 건의, 이슈, 기타 (한/영 별칭)
    - 저장소: `data/claims.json`, 로그: `logs/claim.log`
  - 전체 6파일 py_compile 통과 + 봇 재시작 정상 (PID 19500)
- **문서 업데이트**
  - `changelog/CHANGELOG.md`: v1.5.0 섹션 추가
  - `.claude/DEV_RULES.md`: 버전 1.5.0, 프로젝트 구조에 safety_guard.py + claim_handler.py 추가

### 버전
- v1.4.2 → **v1.5.0**

### 수정 파일
- `Slack Bot/game_aliases.py` (수정 — wiki_ancestor_id 필드 + get_wiki_ancestor_id())
- `Slack Bot/wiki_client.py` (수정 — ancestor CQL ID 기반 수정)
- `Slack Bot/jira_client.py` (수정 — _DONE_STATUSES "닫힘" 추가)
- `Slack Bot/safety_guard.py` (신규 — 쓰기 의도 감지 + 차단)
- `Slack Bot/claim_handler.py` (신규 — /claim 비즈니스 로직)
- `Slack Bot/slack_bot.py` (수정 — /claim 핸들러 + 안전 가드 적용)
- `changelog/CHANGELOG.md` (수정 — v1.5.0 기록)
- `.claude/DEV_RULES.md` (수정 — 버전 + 구조)

---

## 2026-03-10 (화) — 세션 13

### 완료
- **v1.4.2: 게임명 별칭 매핑 + 검색 고도화**
  - `game_aliases.py` 신규 생성 — 게임명 별칭 중앙 관리 모듈
    - 5개 게임 정의: 에픽세븐(EP7), 카제나(GCZ), 리젝(PRH), 로드나인(LDN), 로드나인 아시아(LNA)
    - `resolve_game()`, `detect_game_in_text()`, `get_jira_project_key()` 등 API 제공
  - `wiki_client.py`: `search_with_context()` → 4단계 계단식 검색
    - Stage 1: 게임 ancestor + 연도 + 키워드 CQL
    - Stage 2: 게임 ancestor + 키워드만
    - Stage 3: 연도 + 키워드 (기존 로직)
    - Stage 4: get_page_by_title 폴백
    - `_try_smart_cql()` 헬퍼 추가
  - `jira_client.py`: 자연어 상태 의도 감지
    - `_detect_status_intent()`: "액티브/활성/열린" → `status NOT IN (Closed, Done, ...)`
    - `question_to_jql()`, `question_to_jql_variants()`에 `project_key` 파라미터 추가
  - `slack_bot.py`: 하드코딩 `_JIRA_PROJECT_NAMES` → `game_aliases` 통합
    - `_resolve_jira_project()` game_aliases 우선 조회
    - 파이프 핸들러 JQL 구성 간소화 (project_key 내부 주입)
  - 전체 4파일 py_compile 통과 + 로직 테스트 정상
- **문서 업데이트**
  - `changelog/CHANGELOG.md`: v1.4.2 섹션 추가
  - `.claude/DEV_RULES.md`: 버전 1.4.2, 프로젝트 구조에 game_aliases.py 추가

### 버전
- v1.4.1 → **v1.4.2**

### 수정 파일
- `Slack Bot/game_aliases.py` (신규 — 게임명 별칭 매핑)
- `Slack Bot/wiki_client.py` (수정 — 4단계 계단식 검색)
- `Slack Bot/jira_client.py` (수정 — 상태 의도 감지, project_key 주입)
- `Slack Bot/slack_bot.py` (수정 — game_aliases 통합)
- `changelog/CHANGELOG.md` (수정 — v1.4.2 기록)
- `.claude/DEV_RULES.md` (수정 — 버전 + 구조)

---

## 2026-03-10 (화) — 세션 12

### 완료
- **v1.4.1: 검색 정확도 전면 개선 (Wiki + Jira + GDI)**
  - `wiki_client.py`: `search_with_context()` 메서드 추가
    - 질문에서 연도(20XX) 추출 → 제목 키워드와 조합하여 CQL 검색
    - CamelCase 분리: "HotFix" → ["Hot", "Fix"] (Confluence 토크나이저 대응)
    - 예: "2026년 핫픽스 알려줘" + 제목 "HotFix 내역" → CQL `title ~ "2026" AND title ~ "Hot"` → "2026_Hot Fix" 페이지 발견
    - 실패 시 `get_page_by_title()` 폴백
  - `slack_bot.py`: `_wiki_fetch_page()`에 `question` 파라미터 추가
    - 파이프(`\`) 핸들러에서 질문 맥락을 검색에 전달
  - `jira_client.py`: `_extract_keywords()`, `question_to_jql_variants()` 추가
    - 점진적 확장: 전체 키워드 → 앞 2개 → 첫 키워드만
    - `question_to_jql()`: `summary ~` → `text ~` (전체 필드 검색)
    - 한국어 불용어 제거 (알려줘, 보여줘, 관련, 최근 등)
  - `slack_bot.py`: Jira 프로젝트 파이프 핸들러에 broadening 패턴 적용
    - `jql_variants` 순회하며 첫 결과 발견 시 break
- **MCP 캐시 auto_sync 완료 확인**
  - Wiki Full Ingest: 2000페이지 스캔, 1462 추가, 538 갱신, 0 에러 (978.95초)
  - Jira Delta Sync: 6 프로젝트 (EP7, GCZ, LDN, LNA, PRH, SMQA), 0건 신규
- **봇 재시작**: PID 43524 → PID 41436 (전체 변경사항 적용)
- **3개 파일 문법 검증 통과** (`py_compile`)

### 보류
- **자기 학습 검색 시스템**: 사용자 제안 3가지 방향 중 선택 대기
  1. 질의 패턴 학습 (로그 기반 매핑 테이블)
  2. AI 질의 재구성 (Haiku로 검색어 전처리)
  3. 벡터/시맨틱 검색 (임베딩 기반)
- **auto_sync Windows 작업 스케줄러 등록** (2시간 주기 자동 실행)

### 버전
- v1.4.0 → **v1.4.1**

### 수정 파일
- `Slack Bot/wiki_client.py` (수정 — search_with_context 추가)
- `Slack Bot/jira_client.py` (수정 — _extract_keywords, question_to_jql_variants 추가)
- `Slack Bot/slack_bot.py` (수정 — _wiki_fetch_page question 파라미터, jira broadening)
- `changelog/CHANGELOG.md` (수정 — v1.4.1 기록)

---

## 2026-03-10 (화) — 세션 11

### 완료
- **v1.4.0: `/jira` 슬래시 커맨드 + Jira MCP 캐시 통합 (Phase 3)**
  - `jira_client.py` 신규 생성 (480줄)
    - JiraClient 클래스: search_issues, get_issue, get_all_projects, get_project
    - 3계층 캐시: L1 인메모리 5분 → L2 SQLite (이슈 10분, 프로젝트 1시간, 목록 24시간) → L3 MCP
    - 자동 JQL 변환: 텍스트 → `summary ~ "..." ORDER BY updated DESC`
    - 이슈 키 감지: `^[A-Z][A-Z0-9]+-\d+$` 패턴
    - Slack 포맷 헬퍼: format_search_results, format_issue, format_project, format_projects_list
    - Claude AI 컨텍스트 추출: get_issue_context_text, get_search_context_text
    - 조회 로거: logs/jira_query.log
  - `slack_bot.py` 수정
    - /jira 핸들러: search, issue, project, projects, help, 파이프 AI 질문
    - _jira_* 헬퍼 10개 추가
  - `.env`에 JIRA_MCP_URL, JIRA_USERNAME, JIRA_TOKEN, JIRA_BASE_URL 추가
  - `mcp-cache-layer/src/config.py`에 Jira TTL 상수 추가
  - `IMPLEMENTATION_PLAN.md` Phase 3 완료 표시 + 산출물 기록

### 버전
- v1.3.5 → **v1.4.0**

### 수정 파일
- `Slack Bot/jira_client.py` (신규)
- `Slack Bot/slack_bot.py` (수정 — import + 헬퍼 + 핸들러)
- `.env` (수정 — Jira 환경변수)
- `changelog/CHANGELOG.md` (수정 — v1.4.0 기록)
- `.claude/DEV_RULES.md` (수정 — 구조 + 버전)
- `mcp-cache-layer/src/config.py` (수정 — Jira TTL)
- `mcp-cache-layer/docs/IMPLEMENTATION_PLAN.md` (수정 — Phase 3 완료)

---

## 2026-03-10 (화) — 세션 10

### 완료
- **Wiki 캐시 통합 커밋 + Railway 재배포 (v1.3.4)**
  - wiki_client.py 3계층 캐시 + /wiki-sync 커맨드 + 구분자 변경
  - `git push origin main` → Railway 자동 배포
- **MCP Cache Phase 2: GDI MCP 캐시 통합 (v1.3.5)**
  - 설계 문서: `mcp-cache-layer/docs/PHASE2_GDI_DESIGN.md`
  - `gdi_client.py`: 3계층 캐시 통합 (L1 메모리 → L2 SQLite → L3 MCP)
    - `list_files_in_folder`: page=1 캐시 (폴더 TTL 6시간)
    - `search_by_filename`: page=1 + page_size≤20 캐시 (파일 TTL 24시간)
    - `unified_search`: 캐시 미적용 (검색 결과 매번 다름)
  - `config.py`: GDI 전용 TTL 상수 추가
  - `log_gdi_query()`: `cache_status` 필드 추가
  - 봇 재시작 완료 (PID 30760), DB init 2회 확인 (wiki + gdi)

### 보류
- Railway 재배포 (v1.3.5): `/gdi` 캐시는 로컬만 사용 가능, Railway는 다음 커밋+push 시 배포

---

## 2026-03-10 (화) — 세션 9

### 완료
- **daily/weekly 중복 발송 버그 수정 (v1.3.3)**
  - 원인: v1.3.1에서 daily/weekly `misfire_grace_time=3600` 추가 → Railway 10:00~10:05 사이 3번 재시작 → 각각 1시간 이내 판단으로 3번 발동 → 체크리스트 3개 중복 발송
  - 수정: `scheduler.py` `_add_daily()` / `_add_weekly()` `misfire_grace_time`: 3600 → **60**
  - 중복 메시지 2개(ts=1773104401.221139, 1773104687.395599) 삭제는 사용자가 직접 처리
  - 남길 메시지: ts=1773104704.056779 (checklist_state.json에 등록된 최신 메시지)
- **03/10 체크리스트 전일 누락 섹션 재수정**
  - `fix_missed_0310.py` 실행 → ts=1773104704.056779 메시지 `chat.update`
  - 올바른 전일 날짜: `"[일일] 03/09(월)"` (이전 스크립트: `"03/06(금)"` 오류)
  - 올바른 누락 4개: `[에픽세븐] Next Checklist`, `[에픽세븐] 커뮤니티 이슈`, `[에픽세븐] GDI 데이터 파일 업로드`, `[카제나] GDI 데이터 파일 업로드`
  - 체크 상태 6개 유지 (g0_cazena, g1_cazena, g4_cazena, g5_cazena, g3_cazena, g6_cazena)
- **`extract_flat_items()` group_name 결합 수정** (`missed_tracker.py`)
  - group 항목 sub_item에 group_name 접두사 제거 후 결합 (`"[에픽세븐] 서비스 장애"`)
- **`_build_missed_section_blocks()` 체크박스 제거** (`slack_sender.py`)
  - action/checkboxes → section/mrkdwn 텍스트 리스트 (`• *업무명*  담당: 이름`)

### Railway 재배포 필요
- `scheduler.py` 변경(misfire_grace_time 60초) → Railway 재배포해야 적용됨

---

## 2026-03-10 (월) — 세션 8

### 완료
- **03.10 체크리스트 메시지에 전일(03.06 금) 누락 항목 추가**
  - 재발송 없이 `chat.update`로 기존 메시지(ts=1773104704.056779) 수정
  - `groups:history` 없으므로 전일 체크 상태 불가 → 로그(slack_bot.log)에서 오늘 체크된 6개 항목 파악 후 나머지 9개를 "[일일] 03/06(금)" 누락으로 표시
  - `add_missed_0310.py` 1회성 스크립트 실행 후 `_legacy/`로 이동
- **재발 방지 대책 구현 (v1.3.2)**
  - `_notify_startup()`: Railway 재시작 시 즉시 `monitor_alert_channel`에 알림 발송
    → 기존에는 18:00 모니터링 전까지 재시작 여부 인지 불가 → 이제 즉시 감지
  - `schedule-monitor` 잡 `misfire_grace_time=3600` 추가
    → 18:00 모니터링 잡 자체도 Railway 재시작 시 누락 방지
  - v1.3.2 커밋/Push → Railway 자동 재배포

### 재발 방지 구조 요약

| 레이어 | 조치 | 커버 케이스 |
|--------|------|-------------|
| v1.3.1 | daily/weekly misfire_grace_time=3600 | 재시작 후 1시간 이내 스케줄 자동 발송 |
| v1.3.2 | Railway 재시작 즉시 Slack 알림 | 재시작 즉시 인지 → 수동 대응 |
| v1.3.2 | monitor 잡 misfire_grace_time=3600 | 18:00 모니터링 잡 자체 누락 방지 |
| 기존 | 18:00 schedule_monitor check_and_alert | 당일 미발송 잡 18:00에 알림 |

### 미해결 케이스 (수동 대응 필요)
- Railway가 10:00~11:00 사이가 아닌 더 늦게 재시작되는 경우 (예: 12:00 재시작)
  → misfire_grace_time=3600 초과 → 스케줄 누락
  → **_notify_startup 알림으로 즉시 인지 → 수동 발송으로 대응**
  → 18:00 모니터링도 재확인

---

## 2026-03-10 (월) — 세션 7

### 완료
- **03.10 일일 QA 체크리스트 누락 수동 발송**
  - Railway 재시작으로 인해 10:00 스케줄 미실행 → 로컬에서 수동 발송 스크립트 실행
  - `ts=1773104704.056779` — 발송 성공, 상태 등록 + missed_tracker 로그 기록 완료
- **Railway misfire_grace_time 누락 버그 수정 (v1.3.1)**
  - `scheduler.py` `_add_daily` / `_add_weekly` 에 `misfire_grace_time=3600` 추가
  - Railway 재시작 후 1시간 이내 → 즉시 발송 보장 (미션은 이미 7200초 적용 중)
- **/gdi AI 답변 품질 개선** (v1.3.1)
  - 질문 의도 감지: 목록 질문 vs 내용 질문 분기
  - 목록 질문(종류/뭐가 있는지) → 파일 리스트+경로 중심 답변
  - 내용 질문(요약/분석) → 최관련 파일 1개 내용 가져와서 요약
  - 전용 Claude 프롬프트 3종 분리: `_gdi_ask_claude_content`, `_gdi_ask_claude_list`, `_gdi_ask_claude`
  - `_fetch_file_content` 헬퍼 생성 (4단계 폴백 파일 내용 조회)
- **v1.3.1 커밋/Push → Railway 자동 재배포**

### 원인/교훈
- Railway 재시작 원인 불명 (크래시 또는 롤링 재시작 추정)
- **교훈**: APScheduler `misfire_grace_time` 미설정 시 Railway 재시작 타이밍에 따라 스케줄 누락 발생
  - mission은 jitter 때문에 7200초 설정했는데, daily/weekly는 누락되어 있었음

### 핵심 변경 파일
| 파일 | 유형 | 설명 |
|------|------|------|
| `scheduler.py` | 수정 | daily/weekly misfire_grace_time=3600 추가 |
| `slack_bot.py` | 수정 | 질문 의도 감지 + 전용 Claude 프롬프트 분리, _fetch_file_content 헬퍼 |
| `changelog/CHANGELOG.md` | 수정 | v1.3.1 기록 |

---

## 2026-03-09 (일) — 세션 6

### 완료
- **v1.3.0 /gdi 슬래시 커맨드 구현** (GDI MCP 연동)
  - `mcp_session.py` 신규: `_McpSession`을 공용 `McpSession` 클래스로 추출
  - `wiki_client.py` 리팩토링: `_McpSession` 삭제 → `McpSession` import
  - `gdi_client.py` 신규: GdiClient (unified_search, search_by_filename, list_files_in_folder)
  - `slack_bot.py`: `/gdi` 핸들러 + 헬퍼 함수 추가 (search/file/folder/AI 답변)
  - `logs/gdi_query.log` 조회 로거 추가
- **`/calendar` 슬래시 커맨드 제거** — MCP 서버 미지원으로 사용 안 함

### 핵심 변경 파일
| 파일 | 유형 | 설명 |
|------|------|------|
| `mcp_session.py` | 신규 | MCP 세션 공용 모듈 |
| `gdi_client.py` | 신규 | GDI MCP 클라이언트 |
| `wiki_client.py` | 수정 | McpSession import로 리팩토링 |
| `slack_bot.py` | 수정 | /gdi 핸들러, /calendar 제거 |

---

## 2026-03-09 (일) — 세션 5

### 완료
- **미션 넘버링 시스템 구축** (M-01 ~ M-06)
  - `config.json`: 6개 미션에 `mission_number` 필드 추가
  - `slack_sender.py`: `_build_mission_blocks` + `send_mission_reminder`에 `[M-XX]` 접두사 반영
  - 미선정 미션: `⏳ [M-04] 미션 선정 대기 중`, 확정 미션: `🎯 [M-01] Slack QA - Supporter 활용 가이드`
  - 로그/fallback 텍스트/상태 저장에 mission_number 포함
- **mission_state.json 초기화**: 6개 미션 progress: 0으로 생성
- **CHANGELOG.md 업데이트**: v1.2.0에 미션 넘버링 시스템, 레거시 정리, Market Rank 제거 기록

### 수동 진행율 업데이트 흐름 (임시 방편)
1. 사용자가 Claude에게 "M-01 진행율 80%" 전달
2. Claude가 `mission_state.json`의 해당 미션 `progress` 값 직접 수정
3. 다음 스케줄러 실행 시 progress 로드 → 메시지에 반영

### 미션 넘버링 매핑
| 번호 | 미션 ID | 상태 |
|------|---------|------|
| M-01 | mission-ai-driven-qa | 확정 |
| M-02 | mission-tech-support | 확정 |
| M-03 | mission-roundtable | 확정 |
| M-04 | mission-balance-engineering | 미정 |
| M-05 | mission-data-analysis | 미정 |
| M-06 | mission-knowledge-management | 미정 |

### 보류
- HTML 미션 관리 웹페이지 제작 (사용자 향후 계획)

---

## 2026-03-09 (일) — 세션 4

### 완료
- **Market Rank 폴더 통합**: `D:\Vibe Dev\Slack Bot\Market Rank\`와 `D:\Vibe Dev\Maker Store Rank\` 비교
  - 비교 결과: 소스 파일 9개 동일, README.md는 줄끝 문자 차이만 (내용 동일)
  - Maker Store Rank이 상위 집합 (.env, .gitignore, .github, data/ 보유)
  - Slack Bot에서 Market Rank 폴더 `git rm -r` 제거
- **레거시 파일 정리**: 루트 잔여물 33개를 `_legacy/` 폴더로 이동
  - 로그 20개, 테스트 스크립트 7개, 텍스트/데이터 5개, 루트 CHANGELOG.md
  - `_legacy/EXPIRY.md` 생성 (이동일: 3/9, 삭제 예정일: 3/23)
- **DEV_RULES.md v1.1.0**: 레거시 정리 프로세스 섹션 추가 (§7)
  - 파악 → 이동 → 메타 기록 → 2주 유예 → 삭제/복원 절차
  - 레거시 분류 기준 테이블, 주의사항 포함
- **.gitignore 업데이트**: `_legacy/` 추가

### 배경
- Slack Bot 레포가 원래 Market Rank 프로젝트로 시작 → 이후 Slack Bot이 주 프로젝트로 전환
- Market Rank는 `D:\Vibe Dev\Maker Store Rank\`에서 별도 관리 중이라 Slack Bot 내 복사본 불필요

---

## 2026-03-09 (일) — 세션 3

### 완료
- **CHANGELOG.md 초기화**: 기존 루트 CHANGELOG.md를 `changelog/` 폴더로 이동, v1.2.0 섹션 추가
- **wiki 조회 전용 로거 구현**: `wiki_client.py`에 `log_wiki_query()` 함수 + `logs/wiki_query.log` 파일 핸들러 추가
  - 기록 내용: 사용자(ID+이름), 동작(search/get_page/ask_claude), 검색어, 결과, 에러, 소요시간
  - `slack_bot.py`의 `/wiki` 핸들러 3개 분기(search, ask_claude, get_page)에 로깅 호출 추가
- **MEMORY.md 업데이트**: 프로젝트 구조 트리, 봇 경로(`Slack Bot/`), 새 폴더 구조, 제약사항 테이블 반영

---

## 2026-03-09 (일) — 세션 2

### 완료
- **체크리스트 그룹 헤더 소실 복구**: 구 봇 프로세스(PID 9408, 30480)가 이전 경로에서 실행 중이라 config.json을 찾지 못해 그룹 헤더 소실. 구 프로세스 종료 후 신 경로에서 봇 재시작. `repair_checklist.py` 스크립트로 Slack 메시지 직접 복구.
- **전체 메시지 검수**: 로그 분석 결과 손상된 메시지는 3/9 일일 QA 체크리스트 1건만 확인. 다른 메시지(주간 QA 보고, 3/6 일일, 3/8 일일)는 모두 정상.
- **메모리/로그/버전 시스템 구축**: `.claude/`, `logs/`, `changelog/` 폴더 구조 생성, DEV_RULES.md + WORK_LOG.md 초기화
- **wiki 조회 로깅 설계**: wiki_client.py에 별도 로그 파일 기록 기능 설계

### 원인/교훈
- `refactor: 레포 구조 정리` 후 구 봇 프로세스를 재시작하지 않아 경로 불일치 발생
- **교훈**: 파일 이동 후 반드시 실행 중인 프로세스 재시작 확인 필요
- `groups:history` 없이는 `conversations.history` 사용 불가 → 로그 기반 상태 복구 방식 사용

### 보류
- `groups:history` 대안 구현 (슬래시 커맨드 기반 진행율 입력 — B안 권장)
- config.json 핫 리로드 (팀 규모상 긴급하지 않음)

---

## 2026-03-09 (일) — 세션 1

### 완료
- **Anthropic API Key 교체**: 구 키 만료 → 신규 "Slack Bot" 키로 교체
- **Railway 환경변수 업데이트**: 신규 API Key 반영
- **코드 무결성 검증**: slack_bot.py, wiki_client.py 등 주요 파일 정상 확인
- **Windows 작업 스케줄러 재등록**: `SlackQABot` 태스크 경로를 `Slack Bot/Slack Bot/`으로 수정
- **groups:history 필요 이유 분석**: 4곳에서 사용 중 (미션 진행율, 누락 추적 등). 비공개 채널이라 필수.

### 원인/교훈
- API Key 갑작스런 무효화 원인 불명 — 크레딧 부족은 아니었음
- **교훈**: API 호출 에러 로그를 별도 파일에 기록해야 디버깅 가능

### 보류
- 구 API Key Anthropic Console에서 삭제 (사용자 직접 처리)

---

## 2026-03-06 (목)

### 완료
- **미션 진행 현황 리마인더** 기능 구현 (v1.1.0~v1.1.5)
  - 6개 채널 미션 등록 (AI-Driven QA, Balance Engineering, Data Analysis, Knowledge Management, Roundtable, Tech Support)
  - 미정 채널 선정 독려 포맷 분기 추가
  - 스레드 기반 진행율 추적
- **Wiki 검색 개선**: breadcrumb 경로 지원, CQL-unsafe 문자 필터링
- **검색 전략 예외처리 시스템** 추가 (wiki_search_rules.json)
- **CHANGELOG.md 도입** 및 버전 관리 체계 수립 (v1.1.4)
- **스케줄 충돌 수정**: 동시 발송 시 jitter 적용

---

## 2026-03-05 (수)

### 완료
- **인터랙티브 체크리스트 아키텍처 전면 개편**
  - 그룹 체크리스트 구조 도입 (group_name + sub_items)
  - 분기별 QA 체크리스트 추가
  - 주간 QA 보고 스케줄 추가
- **실시간 체크박스 동기화** 구현
  - 동적 action_id로 Slack 클라이언트 강제 재렌더링
  - body[actions] delta 기반 merge 전략
- **GDI 데이터 파일 업로드 항목** 추가
- **전일 누락 항목 표시** 기능 구현

---

## 2026-03-04 (화)

### 완료
- **프로젝트 초기 설정**
  - Slack Bot 기본 구조 생성 (slack_bot.py, slack_sender.py, scheduler.py)
  - 환경변수 설정 (.env)
  - Windows 자동 시작 설정 (autostart_setup.bat)
  - Railway 클라우드 배포 설정 (Procfile, runtime.txt)
- **일일 QA 체크리스트** 기본 구현
- **/wiki 검색 명령어** 구현 (Confluence MCP 연동)

---

*각 항목은 세션 종료 시 Claude가 작성합니다.*
*'슬랙 봇 개발 규칙과 히스토리 불러와' 명령으로 이 파일을 로드합니다.*
