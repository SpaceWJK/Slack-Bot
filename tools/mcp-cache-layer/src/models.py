"""SQLite 스키마 정의 및 마이그레이션.

SCHEMA.md의 DDL을 코드로 구현.
DB는 WAL 모드로 운영 (Slack Bot + 동기화 동시 접근 대비).
"""

import sqlite3
import logging
from pathlib import Path

from . import config

log = logging.getLogger("mcp_cache")

# ── 마이그레이션 정의 ────────────────────────────────────────

MIGRATIONS: dict[int, str] = {
    10: """
-- schema v10: nodes 데이터 계층 라벨링 메타데이터 (task-115 R-1)
-- task-115 본질 = 축 1 (AI 맥락 이해 + 원본 보존 적재). 의미 폴더/게임 alias/파일 종류/날짜 메타.
-- IF NOT EXISTS 미사용: Python sqlite3 + SQLite 3.50.4 ALTER syntax error 회피 (v7 패턴 따름).
-- migrate()의 ver > current 조건이 멱등성 보장.

ALTER TABLE nodes ADD COLUMN folder_role TEXT;
ALTER TABLE nodes ADD COLUMN game_alias_kr TEXT;
ALTER TABLE nodes ADD COLUMN file_kind TEXT;
ALTER TABLE nodes ADD COLUMN ref_date TEXT;

CREATE INDEX IF NOT EXISTS idx_nodes_folder_role ON nodes(folder_role);
CREATE INDEX IF NOT EXISTS idx_nodes_game_alias  ON nodes(game_alias_kr);
CREATE INDEX IF NOT EXISTS idx_nodes_file_kind   ON nodes(file_kind);
CREATE INDEX IF NOT EXISTS idx_nodes_ref_date    ON nodes(ref_date DESC);

INSERT OR IGNORE INTO schema_version (version) VALUES (10);
""",
    9: """
-- schema v9: doc_meta.entities 미처리 노드 전용 partial index (task-105 성능 수정)
-- extract_batch 쿼리가 매 배치 doc_meta 전체 풀스캔하던 문제 해결.
-- partial index는 조건을 만족하는 row만 포함 → O(미처리 건수).
-- v8(jira_mirror)과 키 충돌 방지로 9로 배정 (Step 3 qa-structural/qa-functional CRITICAL-1).
CREATE INDEX IF NOT EXISTS idx_meta_entities_empty
ON doc_meta(node_id)
WHERE entities IS NULL OR entities = '[]';

INSERT OR IGNORE INTO schema_version (version) VALUES (9);
""",
    8: """
-- schema v8: jira_mirror 테이블 (task-108)
-- CREATE TABLE이므로 IF NOT EXISTS 적용 가능 (ALTER TABLE과 다름)
-- nodes FK 없는 독립 미러 테이블 — MCP 장애 시 fallback 전용

CREATE TABLE IF NOT EXISTS jira_mirror (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key   TEXT NOT NULL UNIQUE,
    project_key TEXT NOT NULL,
    summary     TEXT,
    status      TEXT,
    assignee    TEXT,
    updated     TEXT,
    synced_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime'))
);

CREATE INDEX IF NOT EXISTS idx_jm_project ON jira_mirror(project_key);
CREATE INDEX IF NOT EXISTS idx_jm_status  ON jira_mirror(project_key, status);
CREATE INDEX IF NOT EXISTS idx_jm_key     ON jira_mirror(issue_key);

INSERT OR IGNORE INTO schema_version (version) VALUES (8);
""",
    7: """
-- schema v7: doc_meta.entities 컬럼 추가 (task-105)
-- entities: 엔티티 추출 결과 JSON 배열. 미추출 시 '[]'.
-- IF NOT EXISTS 미사용: Python sqlite3 + SQLite 3.50.4 실측에서 syntax error 발생.
-- migrate()의 ver > current 조건이 멱등성을 보장하므로 IF NOT EXISTS 불필요.
ALTER TABLE doc_meta ADD COLUMN entities TEXT NOT NULL DEFAULT '[]';
INSERT OR IGNORE INTO schema_version (version) VALUES (7);
""",
    6: """
-- schema v6: doc_chunks 테이블 + chunks_fts FTS5 trigram (task-104)
-- doc_chunks: node별 800자 청크 저장. chunks_fts: contentless trigram 인덱스.
-- DELETE 순서: chunks_fts 먼저(contentless_delete=1 필요), doc_chunks 나중.

CREATE TABLE IF NOT EXISTS doc_chunks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id      INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    text         TEXT    NOT NULL,
    section_path TEXT,
    char_count   INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
    UNIQUE (node_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_chunks_node ON doc_chunks(node_id);
CREATE INDEX IF NOT EXISTS idx_chunks_seq  ON doc_chunks(node_id, seq);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    title, text,
    content='', contentless_delete=1,
    tokenize='trigram'
);

INSERT OR IGNORE INTO schema_version (version) VALUES (6);
""",
    5: """
-- schema v5: FTS5 trigram 토크나이저 (task-102)
-- trigram은 2자 이하 키워드를 직접 인덱싱하지 못하므로 gdi_client.py에서 LIKE 폴백 사용
-- case_sensitive=0 (기본값): 대소문자 구분 없이 매칭 (T-2 Chaos→Chaoszero 포함)
DROP TABLE IF EXISTS search_fts;
CREATE VIRTUAL TABLE search_fts USING fts5(
    title, body_text, summary, keywords,
    content='', contentless_delete=1,
    tokenize='trigram'
);
INSERT OR IGNORE INTO schema_version (version) VALUES (5);
""",
    4: """
-- schema v4: FTS5 전문 검색 인덱스 (task-077)
-- contentless 모드 + contentless_delete=1로 용량 효율 + 삭제 지원
-- tokenize unicode61이 한국어 phrase match 지원 (실측: '영웅' 1605건 매칭)

CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(
    title, body_text, summary, keywords,
    content='', contentless_delete=1,
    tokenize='unicode61'
);

INSERT OR IGNORE INTO schema_version (version) VALUES (4);
""",
    3: """
-- schema v3: 폴더 택소노미 인덱스

CREATE TABLE IF NOT EXISTS folder_index (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    game          TEXT NOT NULL,
    category      TEXT NOT NULL,
    date_folder   TEXT NOT NULL,
    date_mmdd     TEXT,
    build_folder  TEXT DEFAULT '',
    status_folder TEXT DEFAULT '',
    full_path     TEXT NOT NULL UNIQUE,
    file_count    INTEGER DEFAULT 0,
    scanned_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fi_game_cat ON folder_index(game, category);
CREATE INDEX IF NOT EXISTS idx_fi_date ON folder_index(date_mmdd);
CREATE INDEX IF NOT EXISTS idx_fi_path ON folder_index(full_path);

INSERT OR IGNORE INTO schema_version (version) VALUES (3);
""",
    2: """
-- schema v2: Jira 이슈 전용 인덱스 + FTS 보조 테이블

-- jira 이슈 메타 전용 테이블 (nodes + doc_content 보완)
CREATE TABLE IF NOT EXISTS jira_issues (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL UNIQUE,
    issue_key       TEXT    NOT NULL,
    project_key     TEXT    NOT NULL,
    issue_type      TEXT,
    status          TEXT,
    priority        TEXT,
    resolution      TEXT,
    assignee        TEXT,
    reporter        TEXT,
    components      TEXT,   -- JSON array of component names
    labels          TEXT,   -- JSON array of labels
    fix_versions    TEXT,   -- JSON array of fix version names
    environment     TEXT,
    duedate         TEXT,
    created         TEXT,
    updated         TEXT,
    comment_count   INTEGER DEFAULT 0,
    cached_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_jira_node       ON jira_issues(node_id);
CREATE INDEX IF NOT EXISTS idx_jira_key        ON jira_issues(issue_key);
CREATE INDEX IF NOT EXISTS idx_jira_project    ON jira_issues(project_key);
CREATE INDEX IF NOT EXISTS idx_jira_status     ON jira_issues(project_key, status);
CREATE INDEX IF NOT EXISTS idx_jira_assignee   ON jira_issues(assignee);
CREATE INDEX IF NOT EXISTS idx_jira_type       ON jira_issues(issue_type);

INSERT OR IGNORE INTO schema_version (version) VALUES (2);
""",
    1: """
-- schema v1: 초기 스키마

CREATE TABLE IF NOT EXISTS nodes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type   TEXT    NOT NULL,
    source_id     TEXT    NOT NULL,
    parent_id     INTEGER,
    title         TEXT    NOT NULL,
    path          TEXT,
    node_type     TEXT    NOT NULL DEFAULT 'page',
    space_key     TEXT,
    url           TEXT,
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    updated_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    FOREIGN KEY (parent_id) REFERENCES nodes(id) ON DELETE SET NULL,
    UNIQUE (source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_nodes_source   ON nodes(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_nodes_parent   ON nodes(parent_id);
CREATE INDEX IF NOT EXISTS idx_nodes_space    ON nodes(source_type, space_key);
CREATE INDEX IF NOT EXISTS idx_nodes_title    ON nodes(title COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_nodes_path     ON nodes(path);

CREATE TABLE IF NOT EXISTS doc_meta (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL UNIQUE,
    last_modified   TEXT,
    version         INTEGER,
    content_hash    TEXT,
    author          TEXT,
    status          TEXT,
    labels          TEXT,
    extra_meta      TEXT,
    cached_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    ttl_hours       INTEGER NOT NULL DEFAULT 24,
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_meta_node      ON doc_meta(node_id);
CREATE INDEX IF NOT EXISTS idx_meta_modified  ON doc_meta(last_modified);
CREATE INDEX IF NOT EXISTS idx_meta_cached    ON doc_meta(cached_at);

CREATE TABLE IF NOT EXISTS doc_content (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id         INTEGER NOT NULL UNIQUE,
    body_raw        TEXT,
    body_text       TEXT,
    body_truncated  INTEGER NOT NULL DEFAULT 0,
    summary         TEXT,
    keywords        TEXT,
    char_count      INTEGER DEFAULT 0,
    cached_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_content_node   ON doc_content(node_id);

CREATE TABLE IF NOT EXISTS sync_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type     TEXT    NOT NULL,
    scope           TEXT    NOT NULL,
    sync_type       TEXT    NOT NULL,
    started_at      TEXT    NOT NULL,
    finished_at     TEXT,
    status          TEXT    NOT NULL DEFAULT 'running',
    pages_scanned   INTEGER DEFAULT 0,
    pages_updated   INTEGER DEFAULT 0,
    pages_added     INTEGER DEFAULT 0,
    pages_deleted   INTEGER DEFAULT 0,
    error_message   TEXT,
    duration_sec    REAL
);

CREATE INDEX IF NOT EXISTS idx_sync_source    ON sync_log(source_type, scope);
CREATE INDEX IF NOT EXISTS idx_sync_time      ON sync_log(started_at);

CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime'))
);

INSERT INTO schema_version (version) VALUES (1);
""",
}


# ── DB 연결 ──────────────────────────────────────────────────

def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """SQLite 연결 생성 (WAL 모드, 외래키 활성화)."""
    path = db_path or config.DB_PATH
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_current_version(conn: sqlite3.Connection) -> int:
    """현재 스키마 버전 조회. 테이블 없으면 0."""
    try:
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        return row[0] or 0
    except sqlite3.OperationalError:
        return 0


# ── 마이그레이션 ─────────────────────────────────────────────

def migrate(db_path: str | None = None) -> int:
    """미적용 마이그레이션 순차 실행. 최종 버전 반환."""
    conn = get_connection(db_path)
    current = _get_current_version(conn)

    applied = 0
    for ver in sorted(MIGRATIONS):
        if ver > current:
            log.info("마이그레이션 v%d 적용 중...", ver)
            conn.executescript(MIGRATIONS[ver])
            applied += 1
            log.info("마이그레이션 v%d 적용 완료", ver)

    conn.close()

    tmp_conn = get_connection(db_path)
    ver = _get_current_version(tmp_conn)
    tmp_conn.close()
    if applied:
        log.info("총 %d건 마이그레이션 적용 (현재 v%d)", applied, ver)
    return ver


def init_db(db_path: str | None = None) -> str:
    """DB 초기화 (없으면 생성 + 마이그레이션). DB 경로 반환."""
    path = db_path or config.DB_PATH
    version = migrate(path)
    log.info("DB 준비 완료: %s (schema v%d)", path, version)
    return path


# ── CLI 진입점 ───────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    path = init_db()
    print(f"DB 초기화 완료: {path}")
