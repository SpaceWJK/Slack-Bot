# 변경 이력 (Changelog)

퍼블리싱QA1팀 **Slack 알림 봇 (QA Supporter)** 버전 기록입니다.

> **버전 규칙 (Semantic Versioning)**
> - **Major** `x.0.0` : 기존 버전과 호환되지 않는 파격적 변경 (아키텍처 전면 개편 등)
> - **Minor** `0.x.0` : **새로운 기능 카테고리 추가 시에만** 올림
>   - 현재 기능: ① 슬랙 알리미(스케줄 알림)  ② /wiki 슬래시 커맨드  ③ /gdi 슬래시 커맨드  ④ /jira 슬래시 커맨드
>   - 다섯 번째 기능 카테고리 추가 시 → v1.5.0
> - **Patch** `0.0.x` : 기능 추가 없는 버그 수정 / 기존 기능 개선 / 설정 변경
>   - 알리미 기능 개선(미션 리마인더 등), wiki 안정화 등은 모두 Patch
>
> **롤백 방법**: 각 버전은 git tag로 관리됩니다.
> ```powershell
> git checkout v1.1.3      # 특정 버전 코드 확인
> git checkout main        # 최신으로 복귀
> ```
>
> **🔖 롤백 포인트**: 대규모 작업 전 표시된 안전 복원 지점. `⚠️ ROLLBACK POINT` 블록에 복원 명령어 포함.

---

## [1.7.4] - 2026-04-29

### 개선 (task-128)
- `/jira` 자연어 질의 엔진 전면 재설계 — Intent JSON 분리 아키텍처 도입
  - **Stage 1**: `extract_intent()` — Claude Haiku가 질문을 JSON Intent로 구조화 (날짜/상태/우선순위/담당자/타입/키워드 분리 추출)
  - **Stage 2**: `build_jql_from_intent()` — Intent → 결정론적 JQL (인젝션 방어 포함)
  - **Stage 3**: `search_with_ladder()` — 0건 시 자동 완화 사다리 (L0~L3, max 4 query, 중복 JQL skip)
  - **Stage 4**: `format_intent_summary()` — "🔍 이해한 내용" + 완화 이력 UX 노출
- 날짜 범위 지원: "4월 27일부터 29일까지 생성된 이슈" → `created >= "2026-04-27" AND created <= "2026-04-29"` (기존 0건 → 정상 조회)
- AI 실패 시 기존 키워드 검색 fallback 보장 (회귀 없음)
- 보안: `order_by` 화이트리스트, `status`/`priority`/`issue_type` 내부 따옴표 escape
- `_jira_claude_call`/`_jira_ask_claude` prefix 파라미터 추가 (응답 앞에 해석 내용 표시)

---

## [1.7.3] - 2026-04-28

### 보안 (task-118 / task-119)
- `gdi_client.log_gdi_query`: log injection 차단 — ASCII `|`(U+007C) → 전각 `｜`(U+FF5C) 치환 (`_sanitize_log_field` 헬퍼)
  - 적용 필드: `query`, `result`, `error`, `user_name`, `user_id` (task-118), `action` (task-119, defense-in-depth)
  - sec-code OWASP A09 closeout PASS, 단위 11/11 PASS
- 분리자 ` | ` 보존 → failure_analyzer baseline 회귀 0 (backfill 19179건 무영향)

### 데이터 보정 (task-114 follow-up)
- `logs/gdi_query.log` pre-fix legacy data cleanup — task-114 commit 6569362 (escape patch) 이전 발생한 7쌍 (14 lines) → 7 정상 entries로 reconstruct
  - 결과: failure_analyzer parse warnings 14 → 0, parsed entries 51 → 58 (+7)
  - 백업: `logs/gdi_query.log.bak-task114-cleanup-20260428` (rollback 보장)
  - 절차: AI Brain `_workspace/tasks/task-118/cleanup_report.md` 참조
- KPI post_run_gate exit=0, routing_hit_rate=1.000 유지

### 운영 (pm2 ecosystem 정합)
- `ecosystem.config.cjs`: `python.exe` → `pythonw.exe` + `windowsHide: true` 적용 (콘솔 창 hide)
- pm2 ecosystem 통합 정리: 5 앱 → 8 앱 online (issue-dashboard 5174 / issue-dashboard-api 9100 / kis-server 9091 신규 등록)
- 운영 정정: Railway 사용 안 함 (Master 명시 2026-04-28) — 로컬 pm2 단일 인스턴스만 운영
- 좀비 프로세스 정리: 5174 vite chain / 9100 server.py chain / 9091 s3_server / slack_bot launcher 잔존 9 PIDs 종료

### 진단 보고 (Master 검수 위임)
- task-121 — Epicseven Update Review depth=7 비정상 8건 GDI MCP 진단 (S3 직접 비교 권한 필요)
- task-118 carryover — s3_server.py:774 spec mismatch (`_dash_queries` query/result 미렌더링, 코드 변경 0)

---

## [1.7.2] - 2026-04-24

### 개선
- **PPTX 이미지 OCR** (`mcp-cache-layer/scripts/file_parsers.py`) — Tesseract 한국어+영어 OCR 통합. 미설치 환경에서 graceful degradation (경고 1회 후 기존 동작 유지)
- **벤치마크 측정 방법론 수정** (`mcp-cache-layer/scripts/bench_fts.py`) — `measure()` SQL을 `gdi_client._local_unified_search()` 패턴으로 교체. `source_type='gdi'` 필터 + BM25 랭킹 + `doc_content` JOIN으로 wiki 오염 원천 차단
- **KPI 베이스라인 확보** — 200쿼리 kpi_bench 결과: Miss Rate 82.5%, Body Text 99.2%(PASS), P90 3.9ms. Phase A/B 검색 품질 향상은 고도화 백로그로 이관

### 수정
- `search_fts` wiki 46개 오염 엔트리 삭제 (19,216 → 19,170) — bench_fts miss_rate 허위 92.1% 원인 제거
- `jira_mirror.py`: `MCPSession` → `McpSession` 클래스명 수정, 인증 헤더 누락 수정

---

## [1.7.1] - 2026-04-24

### 개선
- **Jira 로컬 미러 fallback** (`jira_client.py`, `mcp_session.py`) — MCP 장애 시 SQLite 캐시에서 즉시 반환. "미러 기준 N분 전" UX 메시지로 사용자에게 캐시 기준 시각 명시 (`slack_bot.py`)
- **CQL 병렬 검색** (`wiki_client.py`, `search/cql_parallel.py`) — Confluence CQL variant 순차 실행(최악 300s) → ThreadPoolExecutor 병렬 실행(최악 15s, -95%). 단계당 5s 상한 적용
- **McpSession thread-safety** (`mcp_session.py`) — `threading.Lock`으로 상태 변수 경쟁 조건 차단. `call_tool(timeout=30)` 파라미터 추가

### 보안
- `jira_client.py`: `JIRA_USERNAME` 하드코딩 제거 → `os.getenv()` 전환, HTTP URL 평문 경고 추가
- `jira_mirror.py`: `project_key` JQL 인젝션 방어 (`re.fullmatch` 정규식 검증)

---

## [1.7.0] - 2026-03-26

### 추가
- **Performance Risk 카드 분리 리디자인** (`s3_admin.html`)
  - 기존 단일 카드를 "Claude 자체 성능" + "MCP 운영 지표" 2개 카드로 분리
  - Wiki MCP 단독으로 HIGH 판정하던 오류 해소
- **System Status 로컬 HTTP 서버 헬스체크** (`s3_server.py`, `s3_admin.html`)
  - 로컬 서버 5개 헬스체크 추가 (9090/9091/5174/9100/10.5.31.110:9100)
  - 프로세스 설명 자동 매핑 (Bot→Slack QA Bot, KIS→KIS 대시보드 서버 등)
- **Jira/GDI 타이밍 코드** (`slack_bot.py`)
  - jira/gdi 핸들러에 elapsed_ms 타이밍 측정 추가

### 수정
- **ops_metrics.db 쿼리 개선** (`s3_server.py`)
  - elapsed_ms>0 필터 적용 (무효 데이터 제외)
  - SQLite WAL 모드 적용으로 동시 읽기 성능 향상

---

## [1.6.4] - 2026-03-13

### 수정 (Critical)
- **Wiki 페이지 라우팅 근본 수정** (`wiki_client.py`)
  - `search_with_context()`: 스마트 검색(게임/연도 추론) 이전에 **정확 일치 우선 검색** 추가 (Stage -1)
  - 사용자가 `2026_MGQA`처럼 정확한 페이지명을 입력하면 게임명 감지/ancestor 스코핑을 건너뜀
  - 밑줄→공백 변환 variant도 정확 일치에 포함 (`2026_MGQA` → `2026 MGQA`)
  - 연도 추출 범위 확장: question에 없으면 title(page_part)에서도 20XX 추출
  - `get_page_by_title()`: 정확 일치 단계에서 밑줄→공백 변환 variant 시도
- **GDI 검색 enrichment 통합** (`gdi_client.py`, `slack_bot.py`)
  - `_local_unified_search()`: SQL에 `dc.summary`, `dc.keywords` 추가 → 결과에 포함
  - `get_search_context_text()`: summary/keywords를 Claude 컨텍스트에 반영
  - `_gdi_ask_claude()`: enrichment 가이드를 프롬프트에 포함
- **Jira 검색 메타데이터 강화** (`jira_client.py`, `slack_bot.py`)
  - `get_search_context_text()`: issuetype, priority, labels, components, fixVersions 추가
  - `_jira_ask_claude()`: 구조화 메타데이터 가이드를 프롬프트에 포함
- **Dashboard 스케줄러 UI 개편** (`s3_manager.html`, `s3_server.py`)
  - 필터: "체크리스트/미션/텍스트" → "알림/QA Task" 2카테고리 단순화
  - 채널 태그 "undefined" 수정 → 전체 채널 매핑
  - "미션" → "QA Task" 명칭 통일
  - QA Task 행에 미션명 표시
  - UI 간격 압축 (gap 8→4px, padding 축소)

---

## [1.6.3] - 2026-03-13

### 추가
- **Enrichment Pipeline** (`mcp-cache-layer/src/enrichment.py`)
  - 캐시 DB 26,727개 노드에 summary/keywords 자동 생성 (로컬 추출, API 비용 0)
  - Wiki: body 첫 3문장 요약 + labels 기반 키워드
  - Jira: issue_type/status/priority 접두 + description 요약 + labels/components 키워드
  - GDI: path 카테고리 + body 요약 + 경로 세그먼트 + 빈도 키워드
  - CLI: `python -m src.enrichment --all|--wiki|--jira|--gdi [--force] [--stats]`

### 수정
- **Wiki 검색 답변 품질 향상** (`wiki_client.py`, `slack_bot.py`)
  - `_cql_result_to_page_dict()`: summary/keywords를 캐시에서 조회하여 반환
  - `_wiki_call_claude()`: enrichment 컨텍스트(요약+키워드)를 프롬프트 앞에 배치
  - Claude가 페이지 맥락을 빠르게 파악 → 답변 정합성 향상
- **GDI 로컬 검색 keywords 우선** (`gdi_client.py`)
  - `_local_unified_search()`: keywords 칼럼 OR 본문 매칭으로 검색 범위 확대
  - keywords 히트 결과를 body_text 히트보다 상위 정렬
- **auto_sync 자동 enrichment** (`mcp-cache-layer/scripts/auto_sync.py`)
  - Wiki/Jira/GDI 동기화 완료 후 enrichment 자동 실행 단계 추가
- **Wiki 인덱스 export 보강** (`mcp-cache-layer/src/exporters.py`)
  - `export_wiki_index()`: 각 entry에 summary/keywords 필드 포함

---

## [1.6.2] - 2026-03-13

### 수정
- **Wiki 핸들러 — 매크로 전용 페이지 감지** (`slack_bot.py`)
  - `_is_macro_only_content()`: Confluence 매크로 JSON(childpages, toc 등)만 포함된 페이지 조기 감지
  - Stage 1 진입 전 빈 콘텐츠 + 매크로 콘텐츠 체크 → 불필요한 Claude 호출 차단
  - Stage 2 자식 페이지 검색: `ORDER BY lastmodified DESC` + `limit=10`으로 최신 페이지 우선 탐색
  - Stage 2 매크로 전용 자식 페이지 스킵 로직 추가
- **Dashboard 로딩 버그 수정** (`s3_manager.html`, `s3_server.py`)
  - `loadDashboard()` 셀렉터 오류 수정 (`#dashboard-panel` → `#panelDashboard`)
  - API 실패 시 스피너 영구 회전 버그 수정 (무조건 에러 메시지 교체)
  - System Health 봇 프로세스 감지: `tasklist` → `wmic` 변경 (커맨드라인 검색)

### 추가
- **Dashboard 한글 툴팁** — 카드 제목 마우스 호버 시 1줄 설명 표시
- **Dashboard 카드 스크롤** — `max-height: 280px` + 개별 스크롤 적용 (레이아웃 뚫림 방지)
- **Dashboard 필터 기능** — Scheduler(채널별), Claims(카테고리별), Activity Log(이벤트별)
- **Dashboard 적재 상태 표시** — Cache Status 카드에 소스별 body 적재율(%) 표시
- **오토싱크 백그라운드 실행** — `run_sync_silent.vbs` + Task Scheduler 등록
  - `MCP-AutoSync-Delta`: 4시간 주기 델타 싱크 (CMD 창 없이 백그라운드)
  - `MCP-AutoSync-FullWiki`: 매주 월요일 08:00 전체 적재
- **sync_engine 복구 함수** (`sync_engine.py`, `cache_manager.py`)
  - `repair_missing_content()`: body 누락 177개 노드 MCP 재가져오기
  - `repair_parent_ids()`: 고아 노드 부모 관계 복구
  - 3개 헬퍼 쿼리 추가 (`get_nodes_missing_content`, `get_orphan_nodes`, `update_parent_id`)
  - `auto_sync.py`: full_ingest 후 repair 자동 호출

---

## [1.6.1] - 2026-03-12

### 수정
- **GDI 출처(source label) 표시 정확도 개선** (`slack_bot.py`)
  - 2-part 모드(`키워드 \ 질문`): 검색 결과의 실제 파일 경로에서 폴더 추출하여 표시
  - 택소노미 모드: 매칭된 폴더 `full_path` 표시 (기존: 검색 쿼리 문자열)
  - 통합검색 AI 모드: 상위 5건 결과에서 공통 폴더 경로 추출하여 표시
  - `search_data = None` 초기화 누락 버그 수정

### 개선
- **키워드 규칙 대폭 확장** (hot reload — 봇 재시작 불필요)
  - `gdi_keyword_rules.json` v1.1: 1개 → 17개 규칙
    - 밸런스, 체크리스트, 스킬, 장비, 가챠, 상점, 보상, 몬스터, 퀘스트, 아이템,
      던전, 길드, PVP, 이벤트, 우편, 업데이트, BAT 파일 검색
  - `jira_keyword_rules.json` v1.1: 2개 → 10개 규칙
    - 시간 필터(오늘/어제/이번주/지난주/이번달/지난달), 미배정, 버그, 높은우선순위
  - `wiki_keyword_rules.json` v1.1: 문서 보강 (규칙 추가 기준, 게임 canonical명 목록)

---

## [1.6.0] - 2026-03-12

### 추가
- **AWS S3 → gdi-repo/ 자동 동기화** (`auto_sync.py`)
  - `_sync_s3()`: S3 단방향 다운로드 (업로드/삭제 구조적 차단)
  - `_build_s3_sync_cmd()`: 인자 순서 고정으로 역방향 불가
  - 이미지 제외 (*.png, *.jpg) → 텍스트 데이터만 동기화
  - 에러 로깅 통합: CMD + STDERR + 소요시간 상세 기록
  - 환경변수 제어: `GDI_S3_SYNC`, `GDI_S3_BUCKET`, `GDI_S3_TIMEOUT` 등
  - 실패 시 기존 gdi-repo/ 그대로 유지 (fallback)
- **GDI MCP 읽기 전용 안전장치** (`gdi_client.py`)
  - `GDI_MCP_READONLY_TOOLS`: 허용 도구 화이트리스트 (frozenset)
  - `_safe_call_tool()`: MCP 호출 전 허용 목록 검증, 비허용 시 차단 + 에러 로그
  - MCP 경유 쓰기/업로드/삭제 원천 차단 (공유 서버 보호)

### S3 안전 원칙
- **자동 동기화**: S3 → gdi-repo/ 다운로드만 (업로드/삭제 절대 불가)
- **로컬 CLI 수동**: 업로드(사용자 승인 1회) / 삭제(사용자 승인 2회)
- **GDI MCP 경유**: 읽기 전용 — 쓰기/삭제 원천 차단

### 의존 (mcp-cache-layer)
- `.env` 신규: S3 환경변수 (`GDI_S3_BUCKET`, `GDI_S3_SYNC` 등)
- `~/.aws/config`: S3 성능 설정 (max_concurrent_requests=20, multipart)
- AWS CLI v2 설치 필수 (`aws s3 sync` 명령 사용)

---

## [1.5.9] - 2026-03-12 ← 🔖 롤백 포인트 `pre-aws-s3`

> **⚠️ ROLLBACK POINT** — AWS S3 통합 작업 시작 전 안정 버전
> ```powershell
> # Slack Bot 롤백
> cd "D:\Vibe Dev\Slack Bot" && git reset --hard pre-aws-s3
> # mcp-cache-layer 롤백
> cd "D:\Vibe Dev\QA Ops\mcp-cache-layer" && git reset --hard c22b1eb
> ```

### 추가
- **GDI 로컬/클라우드 모드 스위치** (`gdi_client.py`)
  - `GDI_MODE` 환경변수 (`local` | `cloud`) 기반 전환 시스템
  - local 모드: 캐시(SQLite) 전용, MCP 폴백 완전 차단
  - cloud 모드: 기존 동작 유지 (캐시 → MCP 폴백)
  - `_local_unified_search()`: SQLite LIKE 검색으로 MCP 대체
  - `.env` 변경만으로 모드 전환 가능 (재시작 필요)

### 의존 (mcp-cache-layer, 비 Git)
- `auto_sync.py`: `GDI_MODE`에 따라 `load_gdi_local` / `load_gdi` 자동 선택
- auto_sync 주기 변경: 4시간 → 8시간 (08:00, 16:00, 00:00)

---

## [1.5.8] - 2026-03-12

### 개선
- **택소노미 질의 해석 고도화** (`gdi_client.py`, `slack_bot.py`)
  - `taxonomy_search()`: `question` 파라미터 추가 — 키워드+질문 결합 파싱
  - 2-part 모드(`키워드 \ 질문`)에서 질문의 카테고리 힌트도 택소노미에 반영
  - e2e 시뮬레이션 테스트 16/16 PASS

### 변경
- **일일 체크리스트 링크 추가** (`config.json`)
  - 서비스 장애, 커뮤니티 이슈: 제목에 Wiki/Jira 하이퍼링크
  - 핫픽스 내역, Next Checklist, TEST INFO, Release INFO: `(EP7 / CZN)` 게임별 링크

### 의존 (mcp-cache-layer, 비 Git)
- `folder_taxonomy.py`: `_FOLDER_DATE_RE` 정규식 수정 — "YYMMDD 타겟" 폴더명 지원
- Test Result 134개 파일 적재 (Chaoszero 2,934파일 총 95.2MB)

---

## [1.5.7] - 2026-03-12

### 추가
- **GDI 폴더 택소노미 인덱스** (`gdi_client.py`, `slack_bot.py`)
  - 자연어 질의 → 폴더/파일 자동 해석 (MCP 호출 없이 로컬 DB 직접 조회)
  - 한영 별칭(카제나↔Chaoszero), 날짜(2/4→0204), 빌드(3차/핫픽스) 정규화
  - `gdi_client.py`: `taxonomy_search()`, `format_taxonomy_results()`, `get_taxonomy_context_text()` 3개 함수
  - `slack_bot.py`: 택소노미 우선 검색 → MCP 폴백 구조 적용 (통합검색 + AI 질의 모드)
  - `gdi_client.py`: XLSX/PPTX/TSV 청크 메타데이터 정제 함수 (`_clean_any_chunk`, `_parse_xlsx_chunk` 등)

### 의존
- `mcp-cache-layer/scripts/folder_taxonomy.py` (핵심 모듈, ~690줄)
- `mcp-cache-layer/src/models.py` v3 마이그레이션 (folder_index 테이블)

---

## [1.5.6] - 2026-03-11

### 개선
- **GDI 청크 메타데이터 정제** (`gdi_client.py`, `load_gdi.py`)
  - GDI MCP 청크의 반복 메타데이터 접두사(index_mode/file_type/content_type) 자동 제거
  - `_clean_chunk_text()`: 정규식 기반 정제 함수 — 적재·조회 양쪽 적용
  - `load_gdi.py`: 적재 시 청크 정제 (신규 적재 데이터 깨끗하게)
  - `gdi_client.py`: 4개 조회 경로 정제 (캐시 반환, MCP 폴백, 검색 컨텍스트)
  - 기존 DB 5건 일괄 정제 완료 (총 2,933자 / 10~23% 토큰 절감)

### 변경
- **스케줄 발송 시각 분산** (`config.json`)
  - weekly-qa-report: 10:05 → 09:50
  - epic7/cazena-update-checklist: 10:00 → 09:55
  - monthly-qa-checklist: 10:10 → 09:45
  - quarterly-qa-checklist: 10:00 → 09:40
- **README 현행화** (`README.md`)
  - 버전 v1.4.1 → v1.5.5, 일일 체크리스트 7→9항목, 스케줄 시각 반영

---

## [1.5.5] - 2026-03-11

### 추가
- **Wiki answer_miss 로깅 확장** (`slack_bot.py`, `analyze_answer_miss.py`)
  - `_log_answer_miss()` — `level` 파라미터 추가 (CACHE_MISS / ALL_MISS)
  - Stage 1(캐시 적재 데이터) 실패 시 `CACHE_MISS` 레벨로 별도 기록
  - 분석 스크립트: 레벨 구분 파싱 + `--level` 필터 + 후속 단계 해결율 분석
- **GDI 일괄 적재 시스템** (`scripts/load_gdi.py` 신규)
  - GDI MCP의 list_files_in_folder → search_by_filename 전체 청크 수집 → SQLite 저장
  - Delta 적재: DB에 없는 신규 파일만 적재 (불변 데이터 특성 활용)
  - CLI: `--delta`, `--folder`, `--all`, `--stats`, 폴더 단위 분할 적재
- **GDI 캐시 우선 조회** (`gdi_client.py`)
  - `get_file_content_full()`: SQLite 적재 데이터 우선 → MCP 폴백
  - `/gdi` 슬래시 커맨드 MAX_CHARS 20,000 → 50,000 확대
- **자동 동기화 GDI 연동** (`auto_sync.py`)
  - `sync_gdi()`: 2시간 주기 auto_sync에 GDI delta 적재 추가
- **시스템 헬스체크 도구** (`scripts/system_healthcheck.py` 신규)
  - 7가지 검수: 모듈 임포트, 환경변수, MCP 연결, 캐시 DB, 설정 파일, 레거시 탐지, 로그 분석
  - `--quick` (MCP 제외), `--module [name]` (특정 카테고리), `--fix` (수정 제안)

---

## [1.5.4] - 2026-03-11

### 추가
- **Wiki 4단계 Fallback 파이프라인** (`slack_bot.py`)
  - Stage 1: 적재 데이터(캐시) → Claude 답변
  - Stage 2: 하위 페이지(descendant) 검색 → Claude 재질의
  - Stage 3-A: MCP 실시간 원본 페이지 재조회 → Claude 재질의
  - Stage 3-B: MCP 본문 전문 검색(CQL text~) → Claude 재질의
  - "찾을 수 없습니다" 패턴 감지 시 자동 단계 진행
- **Wiki 검색 정확도 개선** (`wiki_client.py`)
  - `get_page_by_title()`: 2단계 CQL — 정확 매칭 우선 → 퍼지 매칭 + 제목 유사도 스코어링
  - `get_descendant_pages()`: 하위 페이지 검색 신규 메서드
  - `fetch_page_live()`: 캐시 우회 MCP 실시간 페이지 조회
  - `search_content_live()`: MCP CQL 본문 전문 검색
- **답변 실패(Answer Miss) 로깅** (`slack_bot.py`)
  - 모든 fallback 단계 실패 시 `logs/answer_miss.log`에 자동 기록
  - 로그 포맷: `timestamp | MISS | user | page | question | stages`
- **답변 실패 분석 스크립트** (`scripts/analyze_answer_miss.py`)
  - 페이지별/사용자별/키워드별 실패 빈도, 일별 추이, 개선 제안 출력
  - `--days N` (최근 N일 분석), `--csv` (CSV 내보내기) 옵션

---

## [1.5.3] - 2026-03-11

### 추가
- **통합 응답 포맷 (`response_formatter.py`)**
  - `/wiki`, `/gdi`, `/jira` AI 답변을 일관된 3단 구조로 통일
  - 📋 질문 → 💬 답변 → 📎 근거 → 🔗 출처 (근거 없으면 자동 생략)
  - Claude 프롬프트에 `[답변]`/`[근거]` 분리 지시문 추가 (파싱 실패 시 안전 폴백)
  - Wiki: 원본 페이지 링크 포함, Jira: 이슈 URL 포함, GDI: 텍스트 출처만

### 수정
- **Wiki HTML 파서 인라인 태그 경계 공백 버그** (`wiki_client.py`)
  - `_ConfluenceHTMLExtractor.handle_endtag`에서 인라인 태그 종료 시 공백 삽입
  - 기존 regex 파서의 `re.sub(r'<[^>]+>', ' ', text)` 동작과 동일하게 보정
  - 수정 전: new_better 33.4%, old_better 53.3% → 수정 후: new_better 69.7%, old_better 15.8%
  - regression 10건 분석: 순수 텍스트 100% 동일(9/10), 나머지 1건은 200K 절단 아티팩트
- **v1.5.2a 메시지 만료 핫픽스**
  - `replace_original`이 동작하지 않던 버그 수정
  - 원인: `ack()` 에 body가 없어 "original" 메시지 부재
  - 수정: `ack(text="⏳ 처리 중...")` → 교체 가능한 원본 생성
  - ExpiringResponder: 항상 `replace_original=True` (단일 메시지 패턴 보장)

---

## [1.5.2] - 2026-03-10

### 추가
- **답변 메시지 자동 만료 (`message_expiry.py`)**
  - `/wiki`, `/gdi`, `/jira` 답변이 10분 후 자동으로 만료 텍스트로 교체
  - 보안 목적: 퇴근 후/외부에서 민감 답변 무기한 열람 방지
  - `ExpiringResponder` 래퍼: 단일 메시지 패턴 (progress → 답변이 같은 메시지 내에서 갱신)
  - `response_url` + `replace_original`로 구현 (추가 Slack 권한 불필요)
  - 환경변수로 즉시 비활성화 가능: `MESSAGE_EXPIRY_ENABLED=false`
  - 비적용 대상: `/claim`, `/wiki-sync`, 스케줄러 알림

---

## [1.5.1] - 2026-03-10

### 추가
- **규칙 기반 키워드→페이지/쿼리 매핑 (`keyword_rules.py`)**
  - Wiki/Jira/GDI 각각 별도 JSON 규칙 파일로 키워드 매핑 관리
  - `wiki_keyword_rules.json`: 게임별 핫픽스 등 키워드 → Wiki 페이지 직접 매핑
  - `jira_keyword_rules.json`: 긴급/이번주 등 키워드 → JQL 조건 자동 추가
  - `gdi_keyword_rules.json`: 밸런스 등 키워드 → 파일명 검색으로 전환
  - Hot reload 지원 (mtime 비교, 봇 재시작 없이 규칙 변경 즉시 반영)
  - 공용 로더 + 3개 매칭 함수: `match_wiki_keyword_rule()`, `match_jira_keyword_rule()`, `match_gdi_keyword_rule()`

### 개선
- **Wiki 검색 Stage 0 추가**: 키워드 규칙 매칭을 기존 4단계 검색 앞에 삽입
  - 규칙 매칭 성공 시 페이지 직접 조회, 실패 시 기존 검색 로직 폴스루
- **Jira JQL 규칙 합성**: `question_to_jql()` + `question_to_jql_variants()`에 규칙 매칭 통합
  - `_inject_before_order()` 헬퍼: ORDER BY 앞에 AND 조건 삽입
  - 상태 의도 + 규칙 조건 동시 적용 가능
- **GDI 2파트 파이프 검색 규칙 적용**: 키워드에 따라 `unified_search` → `search_by_filename` 자동 전환

---

## [1.5.0] - 2026-03-10

### 추가
- **`/claim` 슬래시 커맨드**: 개선/건의/이슈/기타 사용자 제보 접수 시스템
  - `claim_handler.py` 신규 모듈: 카테고리 파싱, JSON 저장, 일별 조회, 통계
  - 커맨드: `/claim [카테고리] [내용]`, `/claim list`, `/claim stats`
  - 저장소: `data/claims.json` (날짜별 분류, CLM-YYYYMMDD-NNN ID 체계)
- **읽기 전용 안전 가드 (`safety_guard.py`)**: 봇의 원본 수정/삭제 요청 차단
  - 2계층 방어: 사전 필터 (정규식) + Claude 프롬프트 (READ_ONLY_INSTRUCTION)
  - 한국어/영어 쓰기 의도 패턴 감지 (삭제해, 변경해, delete, modify 등)
  - 과거형/이력 조회 예외 처리 (삭제된, 변경 이력 등은 읽기로 판정)

### 수정
- **Wiki ancestor CQL 버그 수정**: 텍스트 기반 → 페이지 ID 기반으로 전환
  - `ancestor = "에픽세븐"` (파싱 에러) → `ancestor = 58043932` (정상)
  - `game_aliases.py`에 `wiki_ancestor_id` 필드 추가
  - 에픽세븐(58043932), 카제나(650589593) ID 매핑
- **Jira 활성 이슈 정의 수정**: `_DONE_STATUSES`에 "닫힘" 추가
  - 사용자 정의: 활성 이슈 = 닫힘 상태를 제외한 모든 이슈

---

## [1.4.2] - 2026-03-10

### 추가
- **게임명 별칭 매핑 모듈 (`game_aliases.py`)**: Wiki/Jira 공용 게임명 정규화
  - 에픽세븐: 에픽, Epic, Epicseven, EP7 등 8개 별칭
  - 카제나: 카오스제로, ChaosZero, CZ, GCZ 등 11개 별칭
  - 리젝, 로드나인, 로드나인 아시아 포함
  - `resolve_game()`, `detect_game_in_text()`, `get_jira_project_key()`, `get_wiki_path_keywords()`

### 개선
- **Wiki 게임명 필터링 검색 (`search_with_context`)**: 질문에서 게임명을 감지하여 ancestor CQL 조건 추가
  - 예: `/wiki HotFix 내역 \ 에픽세븐 2026년 핫픽스` → `ancestor="에픽세븐" AND title~"2026" AND title~"Hot"`
  - 4단계 검색: 게임+연도 → 게임만 → 연도만 → 일반 검색
  - `_try_smart_cql()` 내부 헬퍼로 중복 제거
- **Jira 상태 의도 감지**: 자연어 질문에서 액티브/완료 이슈 의도 자동 인식
  - "액티브 이슈 몇개야?" → `status NOT IN ("Closed", "Done", "Resolved", ...)`
  - "완료된 이슈" → `status IN ("Closed", "Done", "Resolved", ...)`
  - `_detect_status_intent()` 함수 추가, `question_to_jql()`·`question_to_jql_variants()`에 통합
- **Jira project_key 자동 주입**: `question_to_jql_variants(project_key=)` 파라미터 추가
  - slack_bot.py 핸들러에서 수동 JQL prefix 조합 로직 제거 → 함수 내부에서 처리
- **Jira 프로젝트 매핑 통합**: `slack_bot.py`의 하드코딩 매핑 → `game_aliases.py` 기반으로 교체
  - `_resolve_jira_project()` 내부에서 `get_jira_project_key()` 호출

---

## [1.4.1] - 2026-03-10

### 개선
- **Wiki 질문 맥락 인식 검색 (`search_with_context`)**: 질문에서 연도·키워드를 추출하여 맥락에 맞는 페이지 우선 검색
  - 예: `/wiki HotFix 내역 \ 2026년 핫픽스 알려줘` → 인덱스 페이지 대신 `2026_Hot Fix` 페이지 발견
  - CamelCase 분리 ("HotFix" → "Hot"+"Fix") + 연도 CQL 조합으로 정확도 향상
  - `_wiki_fetch_page()`에 `question` 파라미터 추가, 파이프 핸들러 연동
- **Jira 검색 점진적 확장 (broadening fallback)**: 0건 결과 시 키워드를 줄여가며 자동 재시도
  - `question_to_jql_variants()`: 전체 키워드 → 앞 2개 → 첫 키워드만 (점진적 확장)
  - `_extract_keywords()`: 한국어 불용어 제거 후 핵심 키워드 추출
  - 프로젝트 파이프 핸들러에서 JQL variants 순회 로직 적용
- **Jira 자연어→JQL 변환 개선**: `summary ~` → `text ~`로 변경 (summary+description+comments 전체 검색)
  - 한국어 불용어(알려줘, 보여줘, 관련 등) 제거 로직 추가
- **GDI 게임명 접두사 자동 보정**: 폴더 미발견 시 Chaoszero/Epicseven 접두사 자동 시도
- **Wiki MAX_PAGE_CHARS 확장**: 20,000 → 40,000자 (긴 페이지 본문 절단 방지)

### 추가
- **`auto_sync.py` (mcp-cache-layer)**: 2시간 주기 캐시 동기화 스크립트
  - Wiki Full Ingest: 전체 페이지 스캔 + 캐시 갱신 (2000페이지 ~16분)
  - Jira Delta Sync: 프로젝트별 최근 이슈 동기화
  - 경로: `D:\Vibe Dev\QA Ops\mcp-cache-layer\scripts\auto_sync.py`

---

## [1.4.0] - 2026-03-10

### 추가
- **`/jira` 슬래시 커맨드**: Jira MCP 연동 + 3계층 캐시 통합 (Phase 3)
  - `jira_client.py` 신규 생성 — JiraClient 클래스 + Slack 포맷 헬퍼
  - 서브커맨드: `search`, `issue`, `project`, `projects`, `help`
  - `\` 구분자로 AI 질문 지원: `/jira QASGP-123 \ 이 이슈 요약해줘`
  - 자동 JQL 변환: 단순 텍스트 → `summary ~ "..." ORDER BY updated DESC`
  - JQL 직접 입력도 지원 (키워드 자동 감지)
  - 캐시 정책: 이슈 10분, 프로젝트 1시간, 프로젝트목록 24시간, JQL검색 미캐시
  - L1 인메모리 (5분) → L2 SQLite → L3 MCP HTTP
  - `logs/jira_query.log`에 조회 내역 기록
- **MCP 캐시 Jira TTL 설정**: `config.py`에 `JIRA_ISSUE_TTL_HOURS=0.17`, `JIRA_PROJECT_TTL_HOURS=1`, `JIRA_PROJECTS_TTL_HOURS=24`

---

## [1.3.5] - 2026-03-10

### 추가
- **GDI MCP 캐시 통합 (Phase 2)**: `/gdi` 커맨드에 3계층 캐시 적용
  - L1 인메모리 (5분 TTL) → L2 SQLite (폴더 6시간, 파일 24시간) → L3 MCP HTTP
  - 캐시 대상: `list_files_in_folder` (폴더 목록), `search_by_filename` (파일 내용)
  - 반복 조회 시 응답시간 5-14초 → <100ms로 단축
  - `gdi_query.log`에 `cache=` 필드 추가 (HIT_MEM, HIT_DB, MISS, STORE 등)
- **MCP 캐시 GDI TTL 설정**: `config.py`에 `GDI_FOLDER_TTL_HOURS=6`, `GDI_FILE_TTL_HOURS=24`

---

## [1.3.4] - 2026-03-10

### 추가
- **Wiki 캐시 통합 (Phase 1)**: `/wiki` 커맨드에 3계층 캐시 적용
  - L1 인메모리 (5분 TTL) → L2 SQLite (24시간) → L3 MCP HTTP
  - `wiki_query.log`에 `cache=` 필드 추가
- **`/wiki-sync` 커맨드**: 캐시 동기화 관리 (status/full/delta)

### 변경
- `/wiki`, `/gdi` 커맨드 구분자: `|` → `\` (Jira 필터 충돌 방지)

---

## [1.3.3] - 2026-03-10

### 수정
- **daily/weekly 중복 발송 버그 수정**: `misfire_grace_time` 3600 → 60초
  - v1.3.1에서 daily/weekly 잡에 1시간 grace 적용 → Railway 재시작 3회(10:00~10:05) 시 각각 발동 → 체크리스트 3번 중복 발송
  - 60초로 축소: Railway 재시작(수초~수십초) 1회는 허용, 연속 재시작 시 중복 방지
- **전일 누락 항목 업무명 누락 수정** (`missed_tracker.py`)
  - `extract_flat_items()`: group 타입 항목에서 `group_name` 접두사(`[각 프로젝트]` 등)를 제거하고 sub_item text와 결합
  - 예: `group_name="[각 프로젝트] 서비스 장애"`, sub text=`"[에픽세븐]"` → `"[에픽세븐] 서비스 장애"`
  - 기존: `"[에픽세븐]"` 만 표시 (group_name 유실)
- **전일 누락 항목 체크박스 제거** (`slack_sender.py`)
  - `_build_missed_section_blocks()`: `action/checkboxes` 블록 → `section/mrkdwn` 텍스트 리스트
  - 형식: `• *업무명*  담당: 이름` (체크박스 없이 누락 현황만 표시)

---

## [1.3.2] - 2026-03-10

### 수정
- **Railway 재시작 즉시 Slack 알림**: 스케줄러 시작 시 `monitor_alert_channel`에 재시작 시각 알림 발송
  - 기존에는 18:00 모니터링 체크 전까지 재시작 여부를 알 수 없었음
  - 이제 재시작 즉시 알림 → 조기 감지 + 수동 대응 가능
- **`schedule-monitor` 잡 misfire 보호**: `misfire_grace_time=3600` 추가
  - 18:00 모니터링 잡 자체도 Railway 재시작 시 누락되는 케이스 방지

---

## [1.3.1] - 2026-03-10

### 수정
- **Railway 스케줄 누락 방지**: `daily`/`weekly` 스케줄에 `misfire_grace_time=3600` 추가
  - Railway 재시작 후 1시간 이내면 즉시 발송 (기존: 기본값 1초 → 재시작 시 누락)
  - 미션 리마인더는 이미 7200초 설정되어 있었음
- **`/gdi` AI 답변 품질 개선** (질문 의도 감지)
  - "종류/목록" 질문 → 파일 목록+경로 중심 답변 (`_gdi_ask_claude_list`)
  - "요약/내용/분석" 질문 → 가장 관련도 높은 파일 1개 내용 가져와서 답변 (`_gdi_ask_claude_content`)
  - 불필요한 검색 통계/메타데이터 제거, 질문에 집중하는 전용 프롬프트 적용
- **`_fetch_file_content` 헬퍼 분리**: 4단계 폴백 파일 내용 조회 (exact→text→#제거→unified)

---

## [1.3.0] - 2026-03-09

### 추가
- **`/gdi` 슬래시 커맨드** — GDI(Game Doc Insight) MCP 연동 (세 번째 기능 카테고리)
  - `/gdi search [검색어]` — 크로스 컬렉션 통합 검색
  - `/gdi file [파일명]` — 파일명 기반 검색 (청크 내용 포함)
  - `/gdi folder [경로]` — 폴더 내 파일 목록 조회
  - `/gdi [검색어] | [질문]` — 검색 결과 기반 Claude AI 답변
  - `/gdi [폴더명] | [파일명] | [질문]` — 폴더+파일 지정 후 Claude AI 답변
  - GDI MCP 서버: `http://mcp-dev.sginfra.net/game-doc-insight-mcp` (인증 불필요)
- **`mcp_session.py` 신규** — MCP Streamable HTTP 세션 공용 모듈
  - `wiki_client.py`의 `_McpSession` 클래스를 `McpSession`으로 추출
  - wiki, gdi 등 여러 MCP 클라이언트에서 공유
- **`gdi_client.py` 신규** — GDI MCP 클라이언트 + Slack 포맷 헬퍼 + 조회 로거
  - `logs/gdi_query.log`에 조회 내역 기록

### 수정
- **`wiki_client.py` 리팩토링** — `_McpSession` → `mcp_session.McpSession` 사용으로 변경

### 제거
- **`/calendar` 슬래시 커맨드** 제거 — 사용하지 않음 (MCP 서버 미지원)

---

## [1.2.0] - 2026-03-09

### 추가
- **미션 넘버링 시스템** (`config.json`, `slack_sender.py`)
  - 6개 미션에 M-01 ~ M-06 번호 부여 (`mission_number` 필드)
  - 일일 미션 리마인더 메시지에 `[M-XX]` 접두사 표시
  - `mission_state.json` 상태 저장에 mission_number 포함
  - 로그 메시지에 미션 번호 포함
  - 향후 수동 진행율 업데이트 및 웹 관리 페이지의 기반
- **스케줄 모니터링** (`schedule_monitor.py` 신규)
  - APScheduler 등록 스케줄 목록, 다음 실행 시각 조회 기능
- **레포지토리 구조 정리** — 봇 소스 파일을 `Slack Bot/` 서브 폴더로 이동
  - 프로젝트 루트 정리, 소스와 설정/문서 분리
- **개발 관리 시스템 도입** (`.claude/`, `logs/`, `changelog/` 폴더)
  - `.claude/DEV_RULES.md` — 개발 규칙 문서
  - `.claude/WORK_LOG.md` — 일일 작업 히스토리
  - `changelog/CHANGELOG.md` — 이 파일 (루트에서 이동)
  - `logs/wiki_query.log` — /wiki 조회 로그
- **wiki 조회 전용 로그** (`wiki_client.py`)
  - 사용자, 검색어, 결과, 에러 내역을 `logs/wiki_query.log`에 별도 기록
- **체크리스트 복구 스크립트** (`repair_checklist.py` 신규)
  - `groups:history` 없이 로그 파일에서 체크 상태 파싱 → `chat.update` 직접 호출
- **레거시 파일 정리 프로세스** (`.claude/DEV_RULES.md` §7)
  - `_legacy/` 폴더로 이동 → 2주 유예 → 삭제/복원 절차 공식화
  - 분류 기준 테이블, 주의사항 포함

### 수정
- **미션 진행율 0% 버그** — 채널 히스토리 폴백 추가 (`missed_tracker.py`)
- **미션 스케줄러 misfire_grace_time** 2시간 설정 (jitter 구간 중 재시작 대응)
- **`missed_tracker.py` Railway 재시작 대응** — 채널 히스토리 폴백 로직 추가
- **구 봇 프로세스 경로 불일치** — 레포 구조 정리 후 구 프로세스 종료 + 신 경로 재시작

### 삭제
- **Market Rank 폴더 제거** — `D:\Vibe Dev\Maker Store Rank\`에서 별도 관리 중이라 Slack Bot 내 복사본 불필요 (`git rm -r`)
- **레거시 파일 33개** → `_legacy/` 이동 (로그 20, 테스트 7, 텍스트/데이터 5, CHANGELOG 1)

### 등록된 미션 넘버링
| 번호 | 미션 ID | 미션명 |
|------|---------|--------|
| M-01 | mission-ai-driven-qa | Slack QA - Supporter 활용 가이드 |
| M-02 | mission-tech-support | Testrail TC 업로드 자동화 |
| M-03 | mission-roundtable | 토론 진행 기획 |
| M-04 | mission-balance-engineering | (미정) |
| M-05 | mission-data-analysis | (미정) |
| M-06 | mission-knowledge-management | (미정) |

### 교훈
- 파일 이동(refactor) 후 반드시 실행 중인 프로세스를 재시작해야 함
- `groups:history` 미부여 시 `conversations.history` 사용 불가 → 로그 기반 상태 복구로 우회

---

## [1.1.5] - 2026-03-06

### 추가
- **알리미: 미션 미정 채널 전용 "선정 독려" 포맷** (`slack_sender.py`)
  - `mission.name == "미정"` 또는 빈 값이면 진행 현황 대신 독려 메시지 자동 전환
  - 진행율 막대 · 서브태스크 · 댓글 요청 블록 없이 심플한 구조
  - 포맷 내용: `(hourglass) 미션 선정 대기 중` + `(speech_balloon) 빨리 미션을 확정하고 도전을 시작해 봐요 (fire)`
  - 미션 확정 시 `config.json`의 `name` 값만 변경하면 즉시 진행 현황 포맷으로 전환
- **`test_mission.py` 멀티 채널 지원**
  - 인수 없이 실행 시 확정 미션 3개 전체 전송
  - 인수 지정 시 특정 채널만 선택 전송 (예: `python test_mission.py mission-roundtable`)

---

## [1.1.4] - 2026-03-06

### 추가
- **WIKI 페이지별 검색 전략 예외처리 시스템** (`wiki_search_rules.json` 신규)
  - 페이지 제목 + 질문 키워드 조합으로 다른 CQL 조회 전략 자동 적용
  - `match_type` 지원: `exact` / `contains` / `startswith` / `regex`
  - 봇 재시작 없이 파일 수정만으로 즉시 반영 (**hot reload**)
- **`get_latest_descendant()` 메서드 추가** (`wiki_client.py`)
  - `ancestor = "페이지" ORDER BY created DESC LIMIT 1` CQL
  - 상위 페이지 전체 본문 로드 대비 토큰 대폭 절약
- **버전 관리 체계 도입** — `CHANGELOG.md` 생성, `git tag` 기반 롤백 지원

### 등록된 예외 규칙
| 규칙 ID | 페이지 | 트리거 | 전략 |
|---------|--------|--------|------|
| `rule-001` | `2026_MGQA` | 최근 / 최신 / 마지막 등 | `get_latest_descendant` |

> 배경: `2026_MGQA` 상위 페이지 본문에는 월별 하위 링크가 1월부터 순서대로 나열됨.
> "가장 최근 업무" 질문 시 1월 데이터를 답변하는 오류를 방지하기 위한 규칙.

---

## [1.1.3] - 2026-03-05

### 추가
- **WIKI: Confluence 브레드크럼 경로(` / ` 구분자) 지원**
  - Confluence UI에서 경로 그대로 복사하여 `/wiki` 커맨드에 붙여넣기 가능
  - `>` (봇 포맷)와 ` / ` (Confluence 포맷) 모두 지원
- **WIKI: 3단계 폴백 전략 완성**
  1. ancestor CQL 포함 검색
  2. CQL 파싱 오류 시 제목만으로 재시도
  3. 결과 없을 시 ancestor 조건 완전 제거 후 재시도
- **WIKI: 구조화된 디버그 로그** (`[wiki][1차]`, `[wiki][2차]`, `[wiki][결과] N건`)
- **WIKI: 실패 시 힌트 메시지** — 단순 제목 검색 / search 커맨드로 유도
- **알리미: Roundtable 채널 추가** (C0AKGRX2BKJ) — 토론 진행 기획
- **알리미: Tech Support 채널 추가** (C0AKGRTU36U) — Testrail TC 업로드 자동화
- **알리미: 스레드 진행율 댓글 요청 메시지** — 미완료 미션에만 표시

### 수정
- WIKI: CQL 특수문자(`[ ] { } ( )`) 포함 ancestor 자동 제외
  - `[Team Weekly Report]` 등 대괄호 포함 경로명으로 인한 CQL 파싱 오류 방지
- 알리미: Roundtable 채널 내용 TBD -> 실제 내용으로 업데이트

---

## [1.1.2] - 2026-02-28

### 추가
- **알리미: 미션 진행 현황 리마인더** (신규 스케줄 타입 `mission`)
  - 6개 채널 평일 09:00~09:30 랜덤 지연 자동 발송
  - D-Day 카운터, 진행율 막대, 서브태스크 목록 포함
  - 전날 스레드 댓글에서 `N%` 패턴 자동 파싱 -> 다음 날 진행율 반영
  - `mission_state.json` 채널별 상태 파일 관리
  - APScheduler `jitter=1800` 랜덤 지연

### 등록된 미션 채널 (6개, v1.1.3에서 일부 업데이트)
| 채널 | 미션 |
|------|------|
| AI-Driven QA Efficiency | Slack QA Supporter 활용 가이드 |
| Balance Engineering | (TBD) |
| Data analysis | (TBD) |
| Knowledge Management | (TBD) |
| Roundtable | (TBD -> v1.1.3에서 업데이트) |
| Tech Support | (-> v1.1.3에서 추가) |

---

## [1.1.1] - 2026-02-20

### 수정
- **WIKI: SSE 응답 인코딩 버그 수정**
  - `r.text` (ISO-8859-1 자동 디코딩) -> `r.content.decode('utf-8')` 명시 처리
  - 한글 페이지 제목/내용 깨짐 문제 해결
- **WIKI: `get_page_by_title` MCP 서버 버그** -> `cql_search`로 대체
- **WIKI: 전체 본문 조회** — `cql_search` excerpt 대신 `get_page_by_id + expand: body.view`
- **WIKI: HTML 테이블 구조 보존** — `</td><td>` -> ` | `, `<tr>` -> `\n` 변환
- **WIKI: MCP 세션 만료 자동 재연결** — `_is_session_error()` + `_retry` 메커니즘
- **WIKI: Claude 답변 연도 범위 혼용 버그 수정** — 프롬프트 지침 추가
- **중복 프로세스 방지** — `_ensure_single_instance()` + PID 파일

---

## [1.1.0] - 2026-02-15

### 추가 (두 번째 기능 카테고리: Wiki 응답)
- **`/wiki` 슬래시 커맨드** (Confluence Wiki MCP 연동)
  - 페이지 제목 직접 검색
  - 계층 경로 지정 검색 (`>` 구분자)
  - Claude AI 기반 질문 답변 (`claude-haiku-4-5-20251001`)
  - 키워드 페이지 목록 검색 (`/wiki search`)
- **`/calendar` 슬래시 커맨드** (Confluence 캘린더 조회/등록)
- `_McpSession` 클래스 — Streamable HTTP 프로토콜 구현
- 페이지 본문 TTL 캐시 (300초)
- MCP 서버 연동: `http://mcp.sginfra.net/confluence-wiki-mcp`

---

## [1.0.0] - 2026-02-01

### 추가 (최초 릴리즈 — 첫 번째 기능 카테고리: 슬랙 알리미)
- **인터랙티브 체크리스트** (Slack Block Kit 기반)
  - 그룹 항목 / 단독 항목 혼용 구조
  - 다중 사용자 동시 체크 — merge 전략 (actions delta 기반)
  - 진행율 막대 실시간 업데이트 (`chat.update`)
  - 전일 누락 섹션 표시
- **등록 스케줄 (7종)**
  - 일일 업무 헤더 (09:00)
  - 일일 QA 체크리스트 (10:00)
  - 주간 QA 보고 (금 10:00)
  - 에픽세븐 업데이트 차수 체크리스트 (3주 간격)
  - 카제나 업데이트 차수 체크리스트 (3주 간격)
  - 월간 QA 체크리스트 (매월 마지막 금요일)
  - 분기 QA 체크리스트 (분기 첫째 주 월요일)
- **스케줄 타입 7종**: `daily` / `weekly` / `monthly` / `monthly_last_weekday` / `biweekly` / `nweekly` / `quarterly_first_monday` / `specific`
- **Railway + 로컬 PC 분리 아키텍처**
  - Railway: `--scheduler-only` (스케줄 발송 전담)
  - 로컬 PC: `--commands-only` (인터랙션 + 슬래시 커맨드)
- **상태 파일 fallback** — 로컬 상태 없을 때 Slack body + config.json으로 재구성

---

*각 버전 항목은 변경 작업 시 Claude가 업데이트합니다.*
