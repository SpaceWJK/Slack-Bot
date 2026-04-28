"""
load_jira.py — Jira 프로젝트 이슈 일괄 적재 스크립트

사용법:
    python scripts/load_jira.py PRH EP7

동작:
  1. Jira MCP로 전체 이슈 목록 페이지네이션 조회
  2. 각 이슈를 nodes + doc_content + doc_meta + jira_issues 에 적재
  3. 완료 후 통계 출력

저장 구조:
  nodes:       source_type="jira", source_id=이슈키, title=제목, space_key=프로젝트키
  doc_content: body_text = 검색 가능한 전체 텍스트 (제목+설명+컴포넌트+상태 등)
  doc_meta:    status, labels(컴포넌트 JSON), extra_meta(우선순위/담당자/등 전체)
  jira_issues: 이슈 전용 메타 (상태, 담당자, 컴포넌트, 레이블 등 구조화)
"""

import sys
import os
import json
import re
import time
import sqlite3
from html import unescape
from datetime import datetime

# 경로 설정
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")
sys.path.insert(0, "D:/Vibe Dev/QA Ops/mcp-cache-layer")

from src.models import init_db, get_connection, migrate
from mcp_session import McpSession
from dotenv import load_dotenv

load_dotenv("D:/Vibe Dev/Slack Bot/.env")

# ── 설정 ────────────────────────────────────────────────────────────────────

JIRA_MCP_URL = os.getenv("JIRA_MCP_URL", "http://mcp.sginfra.net/confluence-jira-mcp")
JIRA_USERNAME = os.getenv("JIRA_USERNAME", "es-wjkim")
JIRA_TOKEN = os.getenv("JIRA_TOKEN", "")
# 절대 경로 사용: Task Scheduler의 CWD와 무관하게 동작하도록
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cache", "mcp_cache.db")

# 이슈 조회 시 가져올 필드 (필요한 것만, 속도 개선)
JIRA_FIELDS = ",".join([
    "summary", "status", "issuetype", "priority", "assignee", "reporter",
    "components", "labels", "fixVersions", "versions", "description",
    "created", "updated", "comment", "environment", "duedate",
    "resolution", "resolutiondate", "issuelinks", "subtasks",
])

PAGE_SIZE = 100

# ── 헬퍼 함수 ───────────────────────────────────────────────────────────────

def _strip_jira_markup(text: str) -> str:
    """Jira 마크업 및 HTML 태그 제거 → 순수 텍스트 변환."""
    if not text:
        return ""
    # Jira 노션 마크업 (h1~h6, *, **, {code}, {panel}, etc.)
    text = re.sub(r'\{[^}]+\}', ' ', text)       # {code}, {panel:...} 등
    text = re.sub(r'h[1-6]\.\s*', '', text)      # h1. h2. 등 헤더
    text = re.sub(r'!([^!|]+)(\|[^!]*)!', '', text)  # 이미지 !image.png!
    text = re.sub(r'\[([^\]|]+)\|[^\]]+\]', r'\1', text)  # [text|url]
    text = re.sub(r'\[([^\]]+)\]', r'\1', text)  # [url]
    text = re.sub(r'[*_\-]{1,2}([^*_\-]+)[*_\-]{1,2}', r'\1', text)  # *bold* _italic_
    text = re.sub(r'<[^>]+>', ' ', text)          # HTML 태그
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _build_search_text(issue_key: str, fields: dict) -> str:
    """검색용 전체 텍스트 생성."""
    parts = []

    # 이슈키 (가장 중요)
    parts.append(f"[{issue_key}]")

    # 제목
    if fields.get("summary"):
        parts.append(fields["summary"])

    # 이슈 타입
    if fields.get("issuetype"):
        parts.append(f"[{fields['issuetype'].get('name', '')}]")

    # 상태
    if fields.get("status"):
        parts.append(fields["status"].get("name", ""))

    # 우선순위
    if fields.get("priority"):
        parts.append(fields["priority"].get("name", ""))

    # 담당자
    if fields.get("assignee"):
        parts.append(fields["assignee"].get("displayName", ""))

    # 컴포넌트
    comps = [c.get("name", "") for c in fields.get("components", []) if c.get("name")]
    if comps:
        parts.append("컴포넌트: " + ", ".join(comps))

    # 레이블
    labels = fields.get("labels", [])
    if labels:
        parts.append("레이블: " + ", ".join(labels))

    # Fix 버전
    fix_vers = [v.get("name", "") for v in fields.get("fixVersions", []) if v.get("name")]
    if fix_vers:
        parts.append("버전: " + ", ".join(fix_vers))

    # 설명
    if fields.get("description"):
        desc = _strip_jira_markup(fields["description"])
        parts.append(desc[:5000])  # 최대 5000자

    # 환경
    if fields.get("environment"):
        parts.append("환경: " + _strip_jira_markup(fields["environment"])[:500])

    return "\n".join(p for p in parts if p.strip())


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ── MCP 세션 ─────────────────────────────────────────────────────────────────

def get_mcp() -> McpSession:
    return McpSession(
        url=JIRA_MCP_URL,
        headers={
            "x-confluence-jira-username": JIRA_USERNAME,
            "x-confluence-jira-token": JIRA_TOKEN,
        },
        label="jira",
    )


def fetch_updated_issues(mcp: McpSession, project: str, since: str) -> list[dict]:
    """마지막 동기화 이후 변경된 이슈만 JQL로 조회.

    since: 'YYYY-MM-DD HH:MM' 형식 (Jira JQL 날짜 포맷)
    """
    jql = f'project = {project} AND updated >= "{since}" ORDER BY updated ASC'
    all_issues = []
    start = 0
    while True:
        result, err = mcp.call_tool("jql_search", {
            "jql_request": jql,
            "fields": JIRA_FIELDS,
            "limit": PAGE_SIZE,
            "start": start,
        })
        if err:
            print(f"  [ERROR] JQL 검색 실패: {err}")
            break

        raw = result
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                break

        if isinstance(raw, dict):
            issues = raw.get("issues", raw.get("result", []))
        elif isinstance(raw, list):
            issues = raw
        else:
            break

        if not issues:
            break

        all_issues.extend(issues)
        fetched = len(issues)
        print(f"  delta start={start}: {fetched}건 (누적: {len(all_issues)})")

        if fetched < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(0.3)

    return all_issues


def fetch_project_issues(mcp: McpSession, project: str) -> list[dict]:
    """프로젝트 전체 이슈 페이지네이션 조회."""
    all_issues = []
    start = 0
    while True:
        result, err = mcp.call_tool("get_all_project_issues", {
            "project": project,
            "fields": JIRA_FIELDS,
            "limit": PAGE_SIZE,
            "start": start,
        })
        if err:
            print(f"  [ERROR] {err}")
            break

        # result 파싱
        raw = result
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                break

        # MCP 응답 구조: {"result": [...]} 또는 직접 리스트
        if isinstance(raw, dict):
            issues = raw.get("result", raw.get("issues", []))
        elif isinstance(raw, list):
            issues = raw
        else:
            break

        if not issues:
            break

        all_issues.extend(issues)
        fetched = len(issues)
        print(f"  페이지 start={start}: {fetched}건 조회 (누적: {len(all_issues)})")

        if fetched < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(0.3)  # API 부하 방지

    return all_issues


# ── DB 저장 ──────────────────────────────────────────────────────────────────

def upsert_issue(conn: sqlite3.Connection, issue: dict, project_key: str):
    """단일 이슈를 nodes + doc_content + doc_meta + jira_issues 에 저장."""
    issue_key = issue.get("key", "")
    fields = issue.get("fields", {})
    if not issue_key or not fields:
        return

    summary = fields.get("summary") or "(제목 없음)"
    status_name = fields.get("status", {}).get("name", "") if fields.get("status") else ""
    issue_type = fields.get("issuetype", {}).get("name", "") if fields.get("issuetype") else ""
    priority = fields.get("priority", {}).get("name", "") if fields.get("priority") else ""
    resolution = fields.get("resolution", {}).get("name", "") if fields.get("resolution") else ""
    assignee = fields.get("assignee", {}).get("displayName", "") if fields.get("assignee") else ""
    reporter = fields.get("reporter", {}).get("displayName", "") if fields.get("reporter") else ""
    components = [c.get("name", "") for c in fields.get("components", []) if c.get("name")]
    labels = fields.get("labels", [])
    fix_versions = [v.get("name", "") for v in fields.get("fixVersions", []) if v.get("name")]
    environment = fields.get("environment") or ""
    duedate = fields.get("duedate") or ""
    created = fields.get("created") or ""
    updated = fields.get("updated") or ""
    description = fields.get("description") or ""

    # 댓글 수
    comment_data = fields.get("comment", {})
    comment_count = comment_data.get("total", 0) if isinstance(comment_data, dict) else 0

    # 검색 텍스트
    body_text = _build_search_text(issue_key, fields)

    path = f"/{project_key}/{issue_key}"

    # ── 1. nodes 테이블 ──
    existing = conn.execute(
        "SELECT id FROM nodes WHERE source_type='jira' AND source_id=?",
        (issue_key,)
    ).fetchone()

    if existing:
        node_id = existing[0]
        conn.execute(
            "UPDATE nodes SET title=?, updated_at=? WHERE id=?",
            (summary, _now(), node_id)
        )
    else:
        cur = conn.execute(
            "INSERT INTO nodes (source_type, source_id, title, path, node_type, space_key, url, created_at, updated_at) "
            "VALUES ('jira', ?, ?, ?, 'issue', ?, ?, ?, ?)",
            (
                issue_key, summary, path, project_key,
                f"https://jira.smilegate.net/browse/{issue_key}",
                _now(), _now(),
            )
        )
        node_id = cur.lastrowid

    # ── 2. doc_content 테이블 ──
    existing_content = conn.execute(
        "SELECT id FROM doc_content WHERE node_id=?", (node_id,)
    ).fetchone()

    char_count = len(body_text)
    desc_text = _strip_jira_markup(description)

    if existing_content:
        conn.execute(
            "UPDATE doc_content SET body_raw=?, body_text=?, char_count=?, cached_at=? WHERE node_id=?",
            (description[:10000], body_text, char_count, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO doc_content (node_id, body_raw, body_text, char_count, cached_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (node_id, description[:10000], body_text, char_count, _now())
        )

    # ── 3. doc_meta 테이블 ──
    extra_meta = json.dumps({
        "issue_type": issue_type,
        "priority": priority,
        "resolution": resolution,
        "assignee": assignee,
        "reporter": reporter,
        "fix_versions": fix_versions,
        "comment_count": comment_count,
        "created": created,
        "updated": updated,
        "duedate": duedate,
    }, ensure_ascii=False)

    existing_meta = conn.execute(
        "SELECT id FROM doc_meta WHERE node_id=?", (node_id,)
    ).fetchone()

    if existing_meta:
        conn.execute(
            "UPDATE doc_meta SET status=?, labels=?, extra_meta=?, last_modified=?, cached_at=? WHERE node_id=?",
            (status_name, json.dumps(components, ensure_ascii=False),
             extra_meta, updated[:10] if updated else None, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO doc_meta (node_id, status, labels, extra_meta, last_modified, cached_at, ttl_hours) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (node_id, status_name, json.dumps(components, ensure_ascii=False),
             extra_meta, updated[:10] if updated else None, _now(), 24)
        )

    # ── 4. jira_issues 테이블 ──
    existing_ji = conn.execute(
        "SELECT id FROM jira_issues WHERE node_id=?", (node_id,)
    ).fetchone()

    if existing_ji:
        conn.execute(
            "UPDATE jira_issues SET issue_type=?, status=?, priority=?, resolution=?, "
            "assignee=?, reporter=?, components=?, labels=?, fix_versions=?, environment=?, "
            "duedate=?, created=?, updated=?, comment_count=?, cached_at=? WHERE node_id=?",
            (issue_type, status_name, priority, resolution, assignee, reporter,
             json.dumps(components, ensure_ascii=False),
             json.dumps(labels, ensure_ascii=False),
             json.dumps(fix_versions, ensure_ascii=False),
             environment[:500] if environment else "",
             duedate, created, updated, comment_count, _now(), node_id)
        )
    else:
        conn.execute(
            "INSERT INTO jira_issues (node_id, issue_key, project_key, issue_type, status, "
            "priority, resolution, assignee, reporter, components, labels, fix_versions, "
            "environment, duedate, created, updated, comment_count, cached_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (node_id, issue_key, project_key, issue_type, status_name, priority, resolution,
             assignee, reporter,
             json.dumps(components, ensure_ascii=False),
             json.dumps(labels, ensure_ascii=False),
             json.dumps(fix_versions, ensure_ascii=False),
             environment[:500] if environment else "",
             duedate, created, updated, comment_count, _now())
        )


# ── 메인 ─────────────────────────────────────────────────────────────────────

def load_project(project_key: str):
    print(f"\n{'='*60}")
    print(f"프로젝트 적재 시작: {project_key}")
    print(f"{'='*60}")

    mcp = get_mcp()
    t0 = time.time()

    print(f"이슈 목록 조회 중...")
    issues = fetch_project_issues(mcp, project_key)
    print(f"  → 총 {len(issues)}건 조회 완료 ({time.time()-t0:.1f}s)")

    if not issues:
        print("이슈 없음, 종료")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    saved = 0
    errors = 0
    t1 = time.time()

    print(f"DB 저장 중...")
    for i, issue in enumerate(issues, 1):
        try:
            upsert_issue(conn, issue, project_key)
            saved += 1
        except Exception as e:
            errors += 1
            print(f"  [WARN] {issue.get('key','?')}: {e}")

        if i % 100 == 0:
            conn.commit()
            print(f"  {i}/{len(issues)} 완료...")

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\n[{project_key}] 적재 완료:")
    print(f"  조회: {len(issues)}건")
    print(f"  저장: {saved}건, 오류: {errors}건")
    print(f"  소요: {elapsed:.1f}초")


# ── Delta Sync ─────────────────────────────────────────────────────────────────

def _get_last_sync_time(conn: sqlite3.Connection, project_key: str) -> str | None:
    """sync_log에서 마지막 성공 동기화 시각 조회."""
    row = conn.execute(
        "SELECT finished_at FROM sync_log "
        "WHERE source_type='jira' AND scope=? AND status='success' "
        "ORDER BY finished_at DESC LIMIT 1",
        (project_key,)
    ).fetchone()
    return row[0] if row else None


def _log_sync(conn: sqlite3.Connection, project_key: str, sync_type: str,
              started: str, status: str, scanned: int, added: int,
              updated: int, errors: int, duration: float):
    """sync_log에 동기화 기록 저장."""
    conn.execute(
        "INSERT INTO sync_log (source_type, scope, sync_type, started_at, "
        "finished_at, status, pages_scanned, pages_added, pages_updated, "
        "duration_sec, error_message) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("jira", project_key, sync_type, started, _now(), status,
         scanned, added, updated, duration,
         f"errors={errors}" if errors else None)
    )
    conn.commit()


def delta_sync_project(project_key: str) -> dict:
    """프로젝트 변경된 이슈만 적재 (Delta Sync).

    Returns: {"scanned": int, "added": int, "updated": int, "errors": int}
    """
    print(f"\n[{project_key}] Delta Sync 시작")
    started = _now()
    t0 = time.time()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # 마지막 동기화 시각 조회
    last_sync = _get_last_sync_time(conn, project_key)

    if not last_sync:
        # 동기화 기록 없으면 DB에서 가장 최근 updated 시각 사용
        row = conn.execute(
            "SELECT MAX(updated) FROM jira_issues WHERE project_key=?",
            (project_key,)
        ).fetchone()
        last_sync = row[0] if row and row[0] else None

    if not last_sync:
        print(f"  [SKIP] 적재 기록 없음 — 먼저 전체 적재(full)를 실행하세요.")
        conn.close()
        return {"scanned": 0, "added": 0, "updated": 0, "errors": 0}

    # Jira JQL 날짜 포맷: "YYYY-MM-DD HH:MM" 또는 "YYYY/MM/DD HH:mm"
    # DB의 ISO 포맷(2026-03-10T18:30:00+09:00)을 JQL 포맷으로 변환
    since_jql = last_sync[:16].replace("T", " ")
    print(f"  마지막 동기화: {since_jql}")

    mcp = get_mcp()
    issues = fetch_updated_issues(mcp, project_key, since_jql)
    print(f"  → 변경 이슈: {len(issues)}건 ({time.time()-t0:.1f}s)")

    stats = {"scanned": len(issues), "added": 0, "updated": 0, "errors": 0}

    if issues:
        for issue in issues:
            try:
                issue_key = issue.get("key", "")
                # 기존 노드 있는지 확인
                existing = conn.execute(
                    "SELECT id FROM nodes WHERE source_type='jira' AND source_id=?",
                    (issue_key,)
                ).fetchone()
                upsert_issue(conn, issue, project_key)
                if existing:
                    stats["updated"] += 1
                else:
                    stats["added"] += 1
            except Exception as e:
                stats["errors"] += 1
                print(f"  [WARN] {issue.get('key','?')}: {e}")
        conn.commit()

    duration = round(time.time() - t0, 2)
    _log_sync(conn, project_key, "delta", started, "success",
              stats["scanned"], stats["added"], stats["updated"],
              stats["errors"], duration)
    conn.close()

    print(f"  → 신규: {stats['added']}건, 갱신: {stats['updated']}건, "
          f"오류: {stats['errors']}건 ({duration}s)")
    return stats


def delta_sync_all() -> dict:
    """DB에 적재된 모든 프로젝트 Delta Sync."""
    conn = sqlite3.connect(DB_PATH)
    projects = [r[0] for r in conn.execute(
        "SELECT DISTINCT project_key FROM jira_issues ORDER BY project_key"
    ).fetchall()]
    conn.close()

    if not projects:
        print("적재된 Jira 프로젝트 없음")
        return {}

    print(f"Delta Sync 대상 프로젝트: {', '.join(projects)}")
    results = {}
    for pk in projects:
        results[pk] = delta_sync_project(pk)

    # 전체 요약
    total = {"scanned": 0, "added": 0, "updated": 0, "errors": 0}
    for stats in results.values():
        for k in total:
            total[k] += stats.get(k, 0)

    print(f"\n{'='*60}")
    print(f"Delta Sync 전체 완료:")
    print(f"  프로젝트: {len(projects)}개")
    print(f"  스캔: {total['scanned']}건, 신규: {total['added']}건, "
          f"갱신: {total['updated']}건, 오류: {total['errors']}건")
    return results


# ── CLI ────────────────────────────────────────────────────────────────────────

def _print_stats(projects: list[str]):
    """적재 통계 출력."""
    conn = sqlite3.connect(DB_PATH)
    for pk in projects:
        row = conn.execute(
            "SELECT COUNT(*), COUNT(CASE WHEN dc.body_text != '' AND dc.body_text IS NOT NULL THEN 1 END) "
            "FROM nodes n LEFT JOIN doc_content dc ON dc.node_id=n.id "
            "WHERE n.source_type='jira' AND n.space_key=?",
            (pk,)
        ).fetchone()
        statuses = conn.execute(
            "SELECT status, COUNT(*) FROM jira_issues WHERE project_key=? GROUP BY status ORDER BY COUNT(*) DESC",
            (pk,)
        ).fetchall()
        print(f"\n[{pk}] 노드: {row[0]}건, 본문 있음: {row[1]}건")
        print(f"  상태 분포: {dict(statuses)}")
    conn.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    migrate()

    if "--delta" in args:
        # Delta Sync 모드
        args.remove("--delta")
        if args:
            for proj in args:
                delta_sync_project(proj.upper())
        else:
            delta_sync_all()
    else:
        # Full Ingest 모드
        projects = [a.upper() for a in args] if args else ["PRH", "EP7"]
        for proj in projects:
            load_project(proj)

        print("\n\n전체 적재 완료!")
        _print_stats(projects)
