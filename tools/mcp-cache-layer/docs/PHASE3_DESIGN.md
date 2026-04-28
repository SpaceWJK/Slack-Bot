# Phase 3: 폴더 택소노미 인덱스 — 설계서

## 1. 문제 정의

슬랙봇에서 GDI 데이터를 조회할 때, 사용자가 자연어로 입력하는 질의를
DB 경로(source_id)로 변환해야 한다.

**현재 문제**: 슬랙봇의 `/gdi` 명령은 GDI MCP `unified_search`에 의존하며,
이는 텍스트 기반 검색만 가능 → 폴더 구조 기반 필터링 불가.

**예시**:
```
질의: "카제나 2/4타겟 3차 빌드 테스트 결과"
필요: Chaoszero/Test Result/260204/3-*차/ 아래 파일들
현재: unified_search("3차 빌드 테스트 결과") → 무관한 결과 다수
```

## 2. 폴더 경로 패턴 분석

### 2.1 계층 구조

```
{게임명}/{카테고리}/{날짜폴더}/{빌드폴더}/{상태폴더}/{파일}
```

| 레벨 | 이름 | 예시 | 패턴 |
|------|------|------|------|
| L1 | 게임 | Chaoszero, Epicseven, Lordnine_Asia | 고정값 |
| L2 | 카테고리 | Test Result, TSV, Update Review, Dashboard, Live Issue | 고정값 |
| L3 | 날짜 | 260114, 260204, 20260225, 20260225b | YYMMDD 또는 YYYYMMDD(+접미사) |
| L4 | 빌드 | 3-1차, Hotfix 2차, 정규 3차, 2026.01.20 1차 전달 | 가변 패턴 |
| L5 | 상태 | 완료, 진행, 진행중, QA, QA/BVT | 선택적 |

### 2.2 날짜 폴더 패턴

| 카테고리 | 게임 | 형식 | 예시 |
|----------|------|------|------|
| Test Result | Chaoszero | YYMMDD | 260114, 260204 |
| TSV | Chaoszero | YYYYMMDD+접미사 | 20260225b, 20260309a |
| Update Review | Chaoszero | YYYYMMDD | 20260114, 20260204 |
| TSV | Epicseven | YYYYMMDD | 20260101 |
| Update Review | Epicseven | YYMMDD | 260101 |

### 2.3 빌드 폴더 패턴

**Test Result (Chaoszero)**:
- `정규 N차` (정규 1,2차, 정규 3차, 정규 4차, 정규 5차)
- `Hotfix N차` / `Hotifx N차` (오타 존재)
- `N-M차` (2-4차, 3-1차, 3-2차, 3-7~9차)
- `1.0.NNN 클라이언트/Client`

**Update Review (Chaoszero)**:
- `YYYY.MM.DD N차 전달` (2026.01.20 1차 전달)
- `YYYY.MM.DD N-M차 전달` (2026.02.01 3-2차 전달)
- `YYYY.MM.DD 핫픽스 N차 전달` (2026.02.05 핫픽스 2차 전달)
- `YYYY.MM.DD N차 핫픽스 전달` (2026.02.25 1차 핫픽스 전달)
- `YYYY.MM.DD N-M차 핫픽스 전달` (2026.02.27 2-1차 핫픽스 전달)
- `QA`, `QA/BVT`

## 3. 설계

### 3.1 핵심 구조

```
folder_taxonomy.py (새 파일)
├── ALIASES: dict          — 별칭 사전 (게임/카테고리)
├── DATE_PATTERNS: list    — 날짜 정규식
├── BUILD_PATTERNS: list   — 빌드 정규식
├── FolderIndex class      — 폴더 인덱스 (SQLite)
│   ├── build()            — gdi-repo 스캔 → DB 저장
│   ├── resolve_query()    — 자연어 → 경로 리스트
│   └── get_files()        — 경로 → source_id 리스트
└── QueryParser class      — 질의 파서
    ├── extract_game()
    ├── extract_category()
    ├── extract_date()
    └── extract_build()
```

### 3.2 별칭 사전

```python
GAME_ALIASES = {
    # 한글 → 영문 폴더명
    "카제나": "Chaoszero",
    "카오스제로": "Chaoszero",
    "카제": "Chaoszero",
    "cz": "Chaoszero",
    "에픽세븐": "Epicseven",
    "에픽": "Epicseven",
    "e7": "Epicseven",
    "로드나인": "Lordnine_Asia",
    "로나": "Lordnine_Asia",
    "ln": "Lordnine_Asia",
}

CATEGORY_ALIASES = {
    # 한글 → 영문 폴더명
    "테스트 결과": "Test Result",
    "테스트결과": "Test Result",
    "테스트": "Test Result",
    "TR": "Test Result",
    "업데이트 리뷰": "Update Review",
    "업데이트리뷰": "Update Review",
    "업리": "Update Review",
    "UR": "Update Review",
    "tsv": "TSV",
    "데이터": "TSV",
    "대시보드": "Dashboard",
    "라이브 이슈": "Live Issue",
    "라이브이슈": "Live Issue",
}
```

### 3.3 날짜 파서

사용자 입력 패턴 → 날짜 폴더 매칭:

| 입력 | 추출 | 매칭 대상 |
|------|------|-----------|
| 2/4, 0204 | MM=02, DD=04 | 260204, 20260204 |
| 2/4타겟 | MM=02, DD=04 | 260204, 20260204 |
| 2월4일 | MM=02, DD=04 | 260204, 20260204 |
| 3/18 | MM=03, DD=18 | 260318, 20260318 |
| 1월14일 | MM=01, DD=14 | 260114, 20260114 |

**매칭 로직**:
```python
# 추출된 MMDD에 대해, 폴더 인덱스에서 다음을 모두 매칭:
# - YYMMDD: ?60204 (2026→26)
# - YYYYMMDD: 20260204
# - YYYYMMDD+접미사: 20260204a, 20260204b, ...
```

### 3.4 빌드 파서

| 입력 | 추출 | 매칭 대상 |
|------|------|-----------|
| 3차 | build_num=3 | 3-*차, 정규 3차, N차 전달 |
| 3-1차 | build_range=3-1 | 3-1차 (정확 매칭) |
| 핫픽스 2차 | hotfix, num=2 | Hotfix 2차, Hotifx 2차, 핫픽스 2차 |
| 정규 3차 | regular, num=3 | 정규 3차 |

### 3.5 DB 스키마 (folder_index 테이블)

```sql
CREATE TABLE IF NOT EXISTS folder_index (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game        TEXT NOT NULL,              -- Chaoszero
    category    TEXT NOT NULL,              -- Test Result
    date_folder TEXT NOT NULL,              -- 260204
    date_mmdd   TEXT,                       -- 0204 (정규화)
    build_folder TEXT DEFAULT '',           -- 3-1차
    status_folder TEXT DEFAULT '',          -- 완료
    full_path   TEXT NOT NULL UNIQUE,       -- Chaoszero/Test Result/260204/3-1차
    file_count  INTEGER DEFAULT 0,         -- 폴더 내 파일 수
    scanned_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fi_game_cat ON folder_index(game, category);
CREATE INDEX IF NOT EXISTS idx_fi_date ON folder_index(date_mmdd);
```

### 3.6 resolve_query() 흐름

```
입력: "카제나 2/4타겟 3차 빌드 테스트 결과"

1. extract_game("카제나 2/4타겟 3차 빌드 테스트 결과")
   → game = "Chaoszero", 나머지 = "2/4타겟 3차 빌드 테스트 결과"

2. extract_category("2/4타겟 3차 빌드 테스트 결과")
   → category = "Test Result", 나머지 = "2/4타겟 3차 빌드"

3. extract_date("2/4타겟 3차 빌드")
   → date_mmdd = "0204", 나머지 = "3차 빌드"

4. extract_build("3차 빌드")
   → build_num = 3, build_type = None

5. DB 조회:
   SELECT full_path FROM folder_index
   WHERE game='Chaoszero' AND category='Test Result'
     AND date_mmdd='0204'
     AND (build_folder LIKE '3-%차' OR build_folder LIKE '정규 3%차')

   → ['Chaoszero/Test Result/260204/3-1차',
      'Chaoszero/Test Result/260204/3-2차',
      'Chaoszero/Test Result/260204/3-7차',
      ...]

6. source_id로 변환:
   SELECT source_id FROM nodes
   WHERE source_type='gdi'
     AND (source_id LIKE 'Chaoszero/Test Result/260204/3-%차/%'
          OR ...)
```

### 3.7 슬랙봇 연동 지점

`gdi_client.py` 내 기존 `search_gdi()` 함수에 분기 추가:

```python
async def search_gdi(question: str, ...):
    # 1. 폴더 택소노미 해석 시도
    taxonomy_results = taxonomy.resolve_query(question)

    if taxonomy_results:
        # 택소노미로 해석 성공 → 캐시 DB에서 직접 조회
        return _fetch_from_cache(taxonomy_results)

    # 2. 해석 실패 시 기존 MCP unified_search 폴백
    return await _mcp_unified_search(question, ...)
```

## 4. 파일 계획

| 파일 | 작업 | 설명 |
|------|------|------|
| `scripts/folder_taxonomy.py` | **신규** | FolderIndex + QueryParser + 별칭 사전 |
| `src/models.py` | **수정** | folder_index 테이블 마이그레이션 추가 |
| `scripts/load_gdi_local.py` | **수정** | 적재 완료 후 `FolderIndex.build()` 자동 호출 |
| `Slack Bot/gdi_client.py` | **수정** | taxonomy 분기 추가 |

## 5. 구현 순서

1. `folder_taxonomy.py` 작성 (별칭 + 파서 + 인덱스 빌드 + resolve_query)
2. `models.py`에 folder_index 마이그레이션 추가
3. 인덱스 빌드 테스트 (로컬 gdi-repo 스캔)
4. resolve_query 테스트 (5개 시나리오)
5. `gdi_client.py` 연동
6. 슬랙봇 e2e 테스트
