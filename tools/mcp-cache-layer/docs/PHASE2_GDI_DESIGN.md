# Phase 2: GDI MCP 캐시 통합 설계

## 1. 현황 분석

### 1-1. GDI 사용 패턴 (gdi_query.log 기반)

| 패턴 | 비율 | MCP 호출 흐름 | 평균 응답시간 |
|------|------|---------------|---------------|
| `folder_ai` (내용 분석) | 67% | `list_files_in_folder` → `search_by_filename` | 5-7초 |
| `folder_ai_list` (목록 질문) | 20% | `list_files_in_folder` | 5초 |
| `search`/`file`/`folder` (단건) | 13% | 개별 MCP 호출 1건 | 1-5초 |

### 1-2. 반복 조회 패턴 (캐시 효과 높음)

- 동일 폴더 반복 조회: `Chaoszero > Update Review > 20260204` 10회
- 동일 파일 반복 조회: `나인(30047) 사양서` 4회, `은하계 재해` 3회, `Bug Verification` 2회
- **예상 캐시 히트율: 60-70%**

### 1-3. Wiki vs GDI 차이점

| 항목 | Wiki | GDI |
|------|------|-----|
| 데이터 구조 | 계층적 (공간 → 페이지) | 검색 기반 (폴더 → 파일 → 청크) |
| 전체 수집 | Full Ingest 가능 (CQL 페이지네이션) | 불가 (검색 API만 존재) |
| 식별자 | page_id (고정) | file_path (고정), file_name (유사 검색) |
| 변경 빈도 | 수시 (Wiki 편집) | 낮음 (기획서 업로드 시점에만 변경) |
| 인증 | username + token 필요 | 불필요 |

---

## 2. 캐시 전략

### 2-1. 온디맨드 캐시 (Demand-Fill)

GDI는 Full Ingest 불가 → **조회 시점에 캐시 적재**하고, 이후 동일 요청은 캐시에서 응답.

```
요청 → 캐시 조회 → HIT  → 캐시 응답 (0ms)
                 → MISS → MCP 호출 → 응답 + 캐시 저장
```

### 2-2. 캐시 대상 (2개 MCP 도구)

| MCP 도구 | 캐시 키 | 저장 내용 | TTL |
|----------|---------|-----------|-----|
| `list_files_in_folder` | `folder_path` | 파일 목록 JSON | 6시간 |
| `search_by_filename` | `file_name + exact_match` | 청크 내용 텍스트 | 24시간 |

**TTL 근거:**
- 폴더 목록 (6시간): 업무 시간 중 새 파일 업로드 가능, 반나절 주기 갱신
- 파일 내용 (24시간): 기획서 내용은 업로드 후 변경 거의 없음

### 2-3. 기존 스키마 재사용

현재 `nodes` 테이블의 `source_type` 필드가 `'gdi'`를 이미 지원합니다.

```
nodes 테이블:
  source_type = 'gdi'
  source_id   = file_path (폴더: "folder:<path>", 파일: file_path)
  title       = file_name 또는 folder_path
  node_type   = 'folder' | 'file'
  path        = file_path

doc_content 테이블:
  body_text   = 청크 내용 (파일) 또는 파일 목록 JSON (폴더)
```

---

## 3. 구현 설계

### 3-1. gdi_client.py 변경 (캐시 통합)

wiki_client.py와 동일한 **옵셔널 임포트 패턴** 사용:

```python
# gdi_client.py 상단
try:
    import sys as _sys
    _sys.path.insert(0, "D:/Vibe Dev/QA Ops/mcp-cache-layer")
    from src.cache_manager import CacheManager as _CacheManager
    from src.cache_logger import ops_log as _ops_log, perf as _perf
    _gdi_cache = _CacheManager()
    _GDI_CACHE_ENABLED = True
except Exception:
    _gdi_cache = None
    _ops_log = None
    _perf = None
    _GDI_CACHE_ENABLED = False
```

### 3-2. 캐시 레이어 추가 위치

#### A. `list_files_in_folder` 캐시

`GdiClient.list_files_in_folder()` 메서드에 캐시 적용:

```python
def list_files_in_folder(self, folder_path, page=1, page_size=20):
    # page=1만 캐시 (첫 페이지가 대부분)
    if _GDI_CACHE_ENABLED and page == 1:
        cache_key = f"folder:{folder_path}"
        cached = _gdi_cache.get_page(cache_key, source_type="gdi")
        if cached and cached["content"] and cached["content"]["body_text"]:
            node = cached["node"]
            if not _gdi_cache.is_stale(node["id"]):
                return json.loads(cached["content"]["body_text"]), None

    # MCP 호출
    raw, err = self._mcp.call_tool("list_files_in_folder", {...})
    if err:
        return None, err
    data = self._parse_raw(raw)

    # 캐시 저장
    if _GDI_CACHE_ENABLED and page == 1 and data:
        _gdi_cache.put_page(
            "gdi", f"folder:{folder_path}", folder_path,
            node_type="folder", path=folder_path,
            body_text=json.dumps(data, ensure_ascii=False),
        )
        _gdi_cache.upsert_meta(node_id, ttl_hours=6)  # 폴더: 6시간

    return data, None
```

#### B. `search_by_filename` 캐시 (파일 내용)

파일 내용(청크)은 `_fetch_file_content()`에서 조회됨 → `GdiClient.search_by_filename()`에 캐시 적용:

```python
def search_by_filename(self, filename_query, page=1, ...):
    if _GDI_CACHE_ENABLED and page == 1:
        # file_name 기반 캐시 조회
        cached = _gdi_cache.get_node_by_title(filename_query, source_type="gdi")
        if cached:
            content = _gdi_cache.get_content(cached["id"])
            if content and content["body_text"] and not _gdi_cache.is_stale(cached["id"]):
                return json.loads(content["body_text"]), None

    # MCP 호출
    raw, err = self._mcp.call_tool("search_by_filename", {...})
    ...

    # 캐시 저장 (파일 정보가 있을 때만)
    if _GDI_CACHE_ENABLED and page == 1 and data and data.get("file"):
        file_info = data["file"]
        file_path = file_info.get("file_path", filename_query)
        _gdi_cache.put_page(
            "gdi", file_path, file_info.get("file_name", filename_query),
            node_type="file", path=file_path,
            body_text=json.dumps(data, ensure_ascii=False),
        )
        # 파일 내용: 24시간 TTL (기본값)
```

### 3-3. 인메모리 L1 캐시 (선택적)

wiki_client.py의 `_PAGE_CACHE` (5분 TTL 딕셔너리)와 동일한 패턴:

```python
_GDI_MEM_CACHE: dict = {}  # {key: (data, timestamp)}
_GDI_MEM_TTL = 300  # 5분

def _mem_get(key):
    entry = _GDI_MEM_CACHE.get(key)
    if entry and (time.time() - entry[1]) < _GDI_MEM_TTL:
        return entry[0]
    return None

def _mem_set(key, data):
    _GDI_MEM_CACHE[key] = (data, time.time())
```

→ `list_files_in_folder` 결과에 적합 (동일 폴더 연속 조회가 빈번)

### 3-4. 캐시 로깅

기존 `cache_logger.py`의 `ops_log`/`perf` 활용:
- `cache_hit(title, source="memory"/"sqlite")` — 캐시 적중
- `cache_miss(title, reason="not_found"/"stale")` — 캐시 미스
- `cache_store(title, ...)` — 캐시 저장

`log_gdi_query()`에 `cache_status` 필드 추가 (wiki_client와 동일):
```python
log_gdi_query(..., cache_status="HIT_MEM|HIT_DB|MISS|MISS_STALE|STORE|DISABLED")
```

---

## 4. 파일 변경 목록

| 파일 | 변경 유형 | 설명 |
|------|-----------|------|
| `Slack Bot/gdi_client.py` | **수정** | 캐시 임포트 + L1 메모리 + L2 SQLite 통합 |
| `mcp-cache-layer/src/config.py` | **수정** | GDI 전용 TTL 상수 추가 |
| `mcp-cache-layer/docs/IMPLEMENTATION_PLAN.md` | **수정** | Phase 2 완료 표시 |

---

## 5. 검증 방법

1. 봇 재시작 후 `/gdi Chaoszero > Update Review > 20260204 \ 나인 \ 기획서 요약`
2. **첫 호출**: MISS → MCP 호출 → 5-7초 → 캐시 저장
3. **동일 호출**: HIT → 캐시 응답 → <100ms
4. `logs/cache_ops.log`에서 GDI 캐시 hit/miss/store 로그 확인
5. `/wiki-sync status`에서 GDI 노드 카운트 확인

---

## 6. 예상 효과

| 지표 | 현재 | 캐시 적용 후 |
|------|------|-------------|
| 폴더 목록 조회 | 500-700ms | <10ms (캐시 HIT) |
| 파일 내용 조회 | 3-7초 | <10ms (캐시 HIT) |
| folder_ai 전체 | 5-14초 | 1-3초 (Claude API만 대기) |
| 반복 쿼리 응답 | 5-14초 | 1-3초 |
