# MCP Cache Layer — SQLite 스키마 설계

> 최종 수정: 2026-03-10
> DB 파일: `cache/mcp_cache.db`
> 엔진: Python 내장 `sqlite3` (WAL 모드)

## 1. 스키마 개요

```
┌─────────────────────┐
│      nodes          │  ← 트리 구조 (L1 인덱스)
│  source_type        │     wiki 공간, jira 프로젝트, gdi 폴더
│  parent_id → nodes  │
└────────┬────────────┘
         │ 1:1
         ▼
┌─────────────────────┐
│      doc_meta       │  ← 메타데이터 (L2)
│  node_id → nodes    │     수정일, 버전, 해시
└────────┬────────────┘
         │ 1:1
         ▼
┌─────────────────────┐
│      doc_content    │  ← 본문 (L3)
│  node_id → nodes    │     원문, 요약, 키워드
└─────────────────────┘

┌─────────────────────┐
│      sync_log       │  ← 동기화 이력
│  source_type + scope│     실행 시각, 변경 건수
└─────────────────────┘
```

---

## 2. 테이블 정의

### 2-1. `nodes` — 트리 인덱스 (L1)

문서/폴더/이슈의 트리 위치. 모든 소스 타입 통합.

```sql
CREATE TABLE nodes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type   TEXT    NOT NULL,  -- 'wiki', 'jira', 'gdi'
    source_id     TEXT    NOT NULL,  -- 원본 시스템 고유 ID (page_id, issue_key, doc_id)
    parent_id     INTEGER,           -- 자기참조 FK → nodes.id (NULL = 루트)
    title         TEXT    NOT NULL,
    path          TEXT,              -- 계층 경로 (예: '/QASGP/QA Process/Checklist')
    node_type     TEXT    NOT NULL DEFAULT 'page',  -- 'space','folder','page','issue','file'
    space_key     TEXT,              -- wiki: 'QASGP', jira: 'PROJ', gdi: game_name
    url           TEXT,              -- 원본 URL
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    updated_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),

    FOREIGN KEY (parent_id) REFERENCES nodes(id) ON DELETE SET NULL,
    UNIQUE (source_type, source_id)
);

CREATE INDEX idx_nodes_source   ON nodes(source_type, source_id);
CREATE INDEX idx_nodes_parent   ON nodes(parent_id);
CREATE INDEX idx_nodes_space    ON nodes(source_type, space_key);
CREATE INDEX idx_nodes_title    ON nodes(title COLLATE NOCASE);
CREATE INDEX idx_nodes_path     ON nodes(path);
```

**필드 설명:**

| 필드 | 설명 | 예시 |
|------|------|------|
| source_type | MCP 소스 구분 | `wiki`, `jira`, `gdi` |
| source_id | 원본 시스템 ID | wiki: `"123456"`, jira: `"PROJ-123"`, gdi: `"doc_abc"` |
| parent_id | 부모 노드 (트리 구조용) | wiki: 상위 페이지, jira: 프로젝트, gdi: 폴더 |
| path | `/`로 구분된 계층 경로 | `/QASGP/QA Process/Checklist` |
| node_type | 노드 종류 | `space`, `folder`, `page`, `issue`, `file` |
| space_key | 네임스페이스 식별자 | wiki: `QASGP`, jira: `QAOPS`, gdi: `epicseven` |

---

### 2-2. `doc_meta` — 메타데이터 (L2)

캐시 무효화 판단에 쓰이는 메타 정보.

```sql
CREATE TABLE doc_meta (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL UNIQUE,
    last_modified   TEXT,              -- 원본 시스템 수정 시각 (ISO 8601)
    version         INTEGER,           -- 원본 시스템 버전 번호
    content_hash    TEXT,              -- SHA-256 of body (본문 조회 시 계산)
    author          TEXT,              -- 최종 수정자
    status          TEXT,              -- wiki: 'current'/'draft', jira: 'Open'/'Closed'
    labels          TEXT,              -- JSON 배열 (예: '["QA","checklist"]')
    extra_meta      TEXT,              -- 소스별 추가 메타 (JSON)
    cached_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    ttl_hours       INTEGER NOT NULL DEFAULT 24,

    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX idx_meta_node      ON doc_meta(node_id);
CREATE INDEX idx_meta_modified  ON doc_meta(last_modified);
CREATE INDEX idx_meta_cached    ON doc_meta(cached_at);
```

**캐시 무효화 판단 로직:**

```python
def is_stale(meta: DocMeta) -> bool:
    """캐시 유효성 검사 (ARCHITECTURE.md §4 참조)"""
    # 1. TTL 초과 → stale
    if hours_since(meta.cached_at) > meta.ttl_hours:
        return True

    # 2. 원본 메타 조회 (가벼운 MCP 호출 1회)
    remote = fetch_remote_meta(meta.node_id)

    # 3. Primary 비교: last_modified + version
    if remote.last_modified != meta.last_modified:
        return True
    if remote.version and remote.version != meta.version:
        return True

    # 4. 유효 → cached_at 갱신 (TTL 리셋)
    meta.cached_at = now()
    return False
```

---

### 2-3. `doc_content` — 본문 캐시 (L3)

실제 문서 본문과 구조화된 요약.

```sql
CREATE TABLE doc_content (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL UNIQUE,
    body_raw        TEXT,              -- 원문 본문 (HTML/Markdown/plain)
    body_text       TEXT,              -- 순수 텍스트 (HTML 태그 제거)
    body_truncated  INTEGER NOT NULL DEFAULT 0,  -- MAX_CHARS로 잘렸는지 여부
    summary         TEXT,              -- 1-2문장 요약 (향후 LLM 자동 생성)
    keywords        TEXT,              -- JSON 배열 (향후 자동 추출)
    char_count      INTEGER DEFAULT 0, -- 원문 글자 수
    cached_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),

    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX idx_content_node   ON doc_content(node_id);
```

**L2 vs L3 분리 이유:**
- 메타만 조회(L2)하면 본문(L3) 로드 불필요 → 메모리 절약
- 탐색/검색 쿼리는 L1+L2로 완결 (본문 불필요)
- 분석 쿼리만 L3 접근 → 토큰 소모 예측 가능

---

### 2-4. `sync_log` — 동기화 이력

동기화 실행 기록. 디버깅 + "최근 변경" 질문 답변에 활용.

```sql
CREATE TABLE sync_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type     TEXT    NOT NULL,  -- 'wiki', 'jira', 'gdi'
    scope           TEXT    NOT NULL,  -- 범위 (예: 'QASGP', 'PROJ-*', 'epicseven')
    sync_type       TEXT    NOT NULL,  -- 'full', 'delta'
    started_at      TEXT    NOT NULL,
    finished_at     TEXT,
    status          TEXT    NOT NULL DEFAULT 'running',  -- 'running','success','error'
    pages_scanned   INTEGER DEFAULT 0,
    pages_updated   INTEGER DEFAULT 0,
    pages_added     INTEGER DEFAULT 0,
    pages_deleted   INTEGER DEFAULT 0,
    error_message   TEXT,
    duration_sec    REAL
);

CREATE INDEX idx_sync_source    ON sync_log(source_type, scope);
CREATE INDEX idx_sync_time      ON sync_log(started_at);
```

---

## 3. DB 초기화 스크립트

```sql
-- WAL 모드 (읽기/쓰기 동시 가능, Slack Bot + 스케줄러 동시 접근 대비)
PRAGMA journal_mode = WAL;

-- 외래키 강제
PRAGMA foreign_keys = ON;

-- 스키마 버전 관리
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime'))
);

INSERT INTO schema_version (version) VALUES (1);
```

---

## 4. 주요 쿼리 패턴

### 4-1. 제목으로 페이지 조회 (캐시 hit 경로)

```sql
SELECT n.id, n.source_id, n.title, n.path,
       m.last_modified, m.version, m.content_hash, m.cached_at, m.ttl_hours
FROM nodes n
JOIN doc_meta m ON m.node_id = n.id
WHERE n.source_type = 'wiki'
  AND n.space_key = ?
  AND n.title = ? COLLATE NOCASE;
```

### 4-2. 트리 하위 목록 (탐색 쿼리)

```sql
-- 직계 자식
SELECT id, title, node_type, path
FROM nodes
WHERE parent_id = ?
ORDER BY node_type, title;

-- 전체 하위 (path LIKE 활용)
SELECT id, title, node_type, path
FROM nodes
WHERE path LIKE ? || '%'
  AND source_type = ?
ORDER BY path;
```

### 4-3. 변경분 감지 (Delta Sync 후보)

```sql
SELECT n.id, n.source_id, n.title,
       m.last_modified, m.cached_at
FROM nodes n
JOIN doc_meta m ON m.node_id = n.id
WHERE n.source_type = ?
  AND n.space_key = ?
  AND m.cached_at < datetime('now', 'localtime', '-' || m.ttl_hours || ' hours');
```

### 4-4. 최근 동기화 이력

```sql
SELECT sync_type, started_at, status,
       pages_scanned, pages_updated, pages_added,
       duration_sec
FROM sync_log
WHERE source_type = ? AND scope = ?
ORDER BY started_at DESC
LIMIT 10;
```

### 4-5. 오래된 캐시 정리 (30일 미조회)

```sql
DELETE FROM nodes
WHERE id IN (
    SELECT n.id FROM nodes n
    JOIN doc_meta m ON m.node_id = n.id
    WHERE m.cached_at < datetime('now', 'localtime', '-30 days')
);
-- CASCADE로 doc_meta, doc_content도 자동 삭제
```

---

## 5. 마이그레이션 전략

```python
MIGRATIONS = {
    1: """
        -- 초기 스키마 (이 문서의 전체 DDL)
        CREATE TABLE nodes (...);
        CREATE TABLE doc_meta (...);
        CREATE TABLE doc_content (...);
        CREATE TABLE sync_log (...);
        CREATE TABLE schema_version (...);
    """,
    # 향후 추가:
    # 2: "ALTER TABLE doc_content ADD COLUMN embedding BLOB;",
    # 3: "CREATE VIRTUAL TABLE doc_fts USING fts5(title, body_text, content=doc_content);",
}

def migrate(db_path: str):
    """현재 버전 확인 → 미적용 마이그레이션 순차 실행"""
    conn = sqlite3.connect(db_path)
    current = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
    for ver, sql in sorted(MIGRATIONS.items()):
        if ver > current:
            conn.executescript(sql)
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (ver,))
    conn.commit()
```

---

## 6. 용량 추정

| 항목 | 가정 | 예상 용량 |
|------|------|----------|
| QASGP Wiki 페이지 | ~200페이지, 평균 10KB 본문 | ~2MB |
| GDI 문서 | ~500건, 평균 5KB | ~2.5MB |
| Jira 이슈 | ~1000건, 평균 2KB | ~2MB |
| 트리 인덱스 + 메타 | 전체 | ~0.5MB |
| **총계** | | **~7MB** |

3TB 디스크 기준 무시할 수 있는 수준. 30일 정리 정책으로 무한 증가 방지.
