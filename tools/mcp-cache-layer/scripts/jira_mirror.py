# -*- coding: utf-8 -*-
"""
jira_mirror.py — Jira 이슈 최소 로컬 미러 (task-108)

MCP 장애 시 fallback용 SQLite 미러. 핵심 5개 필드만 저장:
  issue_key, project_key, summary, status, assignee, updated

공개 API:
  sync_project(project_key, max_issues=500, db_path=None) -> dict
  search_mirror(query_text, project_key=None, limit=10, db_path=None) -> list[dict]
  get_mirror_age_str(project_key=None, db_path=None) -> str
  is_mirror_fresh(project_key=None, max_age_min=30, db_path=None) -> bool
"""

import os
import re
import sqlite3
import sys
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── DB 경로 기본값 ─────────────────────────────────────────────────────────
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB = os.path.join(_SCRIPTS_DIR, "../cache/mcp_cache.db")

# ── MCP 세션 (옵셔널 — 임포트 실패 시 sync_project 미사용) ─────────────────
_mcp_session = None
try:
    _slack_bot_path = os.path.join(_SCRIPTS_DIR, "../../Slack Bot/Slack Bot")
    if _slack_bot_path not in sys.path:
        sys.path.insert(0, _slack_bot_path)
    from mcp_session import McpSession as _MCPSession
    import os as _os
    _JIRA_MCP_URL = _os.getenv(
        "JIRA_MCP_URL", "http://mcp.sginfra.net/confluence-jira-mcp"
    )
    _mcp_session = _MCPSession(
        url=_JIRA_MCP_URL,
        headers={
            "x-confluence-jira-username": _os.getenv("JIRA_USERNAME", ""),
            "x-confluence-jira-token": _os.getenv("JIRA_TOKEN", ""),
        },
    )
    logger.info("[jira_mirror] MCP 세션 초기화 완료")
except Exception as _e:
    logger.info("[jira_mirror] MCP 세션 미사용: %s", _e)


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """SQLite 연결 (WAL 모드)."""
    path = db_path or _DEFAULT_DB
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _parse_issues(raw) -> list:
    """MCP jql_search 반환값에서 이슈 목록 추출."""
    if not raw:
        return []
    if isinstance(raw, list):
        raw_text = raw[0] if raw else ""
    elif isinstance(raw, dict):
        raw_text = str(raw)
    else:
        raw_text = str(raw)

    # MCP 반환은 TextContent 객체 또는 dict 형태
    issues = []
    if isinstance(raw, dict):
        for item in raw.get("issues", []):
            key = item.get("key", "")
            fields = item.get("fields", {})
            if not key:
                continue
            issues.append({
                "issue_key": key,
                "project_key": key.split("-")[0] if "-" in key else "",
                "summary": fields.get("summary") or "",
                "status": (fields.get("status") or {}).get("name") or "",
                "assignee": (fields.get("assignee") or {}).get("displayName") or "",
                "updated": fields.get("updated") or "",
            })
    return issues


def sync_project(project_key: str, max_issues: int = 500, db_path: str = None) -> dict:
    """Jira 프로젝트 이슈를 미러 테이블에 동기화한다.

    Args:
        project_key: 동기화할 Jira 프로젝트 키 (예: "EI")
        max_issues: 동기화할 최대 이슈 수
        db_path: SQLite DB 경로 (None이면 기본값)

    Returns:
        {"synced": N, "updated": M, "errors": [...]}
    """
    if not _mcp_session:
        return {"synced": 0, "updated": 0, "errors": ["MCP 세션 미사용"]}

    if not re.fullmatch(r"[A-Z][A-Z0-9_\-]{0,9}", project_key or ""):
        return {"synced": 0, "updated": 0, "errors": [f"유효하지 않은 project_key: {project_key!r}"]}

    jql = f"project = {project_key} ORDER BY updated DESC"
    errors = []
    raw = None

    try:
        raw, err = _mcp_session.call_tool("jql_search", {
            "jql_request": jql,
            "limit": max_issues,
        })
        if err:
            return {"synced": 0, "updated": 0, "errors": [str(err)]}
    except Exception as e:
        return {"synced": 0, "updated": 0, "errors": [str(e)]}

    issues = _parse_issues(raw)
    if not issues:
        return {"synced": 0, "updated": 0, "errors": errors}

    conn = _get_conn(db_path)
    synced = 0
    updated = 0
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        with conn:
            for iss in issues:
                cur = conn.execute(
                    "SELECT id FROM jira_mirror WHERE issue_key = ?",
                    (iss["issue_key"],)
                )
                exists = cur.fetchone()

                if exists:
                    conn.execute(
                        """INSERT INTO jira_mirror (issue_key, project_key, summary, status, assignee, updated, synced_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(issue_key) DO UPDATE SET
                             project_key=excluded.project_key,
                             summary=excluded.summary,
                             status=excluded.status,
                             assignee=excluded.assignee,
                             updated=excluded.updated,
                             synced_at=excluded.synced_at""",
                        (iss["issue_key"], iss["project_key"], iss["summary"],
                         iss["status"], iss["assignee"], iss["updated"], now)
                    )
                    updated += 1
                else:
                    conn.execute(
                        """INSERT INTO jira_mirror (issue_key, project_key, summary, status, assignee, updated, synced_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (iss["issue_key"], iss["project_key"], iss["summary"],
                         iss["status"], iss["assignee"], iss["updated"], now)
                    )
                    synced += 1
    except Exception as e:
        errors.append(str(e))
        logger.error("[jira_mirror] sync_project 오류: %s", e)
    finally:
        conn.close()

    return {"synced": synced, "updated": updated, "errors": errors}


def search_mirror(query_text: str, project_key: str = None, limit: int = 10,
                  db_path: str = None) -> list:
    """미러 테이블에서 이슈를 검색한다 (LIKE best-effort).

    Args:
        query_text: 검색어 (단어 공백 분리)
        project_key: 프로젝트 필터 (None이면 전체)
        limit: 최대 반환 수
        db_path: SQLite DB 경로

    Returns:
        list[dict] — issue_key, summary, status, assignee, updated, synced_at
    """
    words = [w.strip() for w in query_text.split() if len(w.strip()) >= 2]
    if not words:
        words = [query_text.strip()]

    conn = _get_conn(db_path)
    try:
        parts = " AND ".join("(summary LIKE ?)" for _ in words)
        params = [f"%{w}%" for w in words]

        if project_key:
            where = f"project_key = ? AND ({parts})"
            params = [project_key] + params
        else:
            where = parts or "1=1"

        sql = f"""
            SELECT issue_key, project_key, summary, status, assignee, updated, synced_at
            FROM jira_mirror
            WHERE {where}
            ORDER BY updated DESC
            LIMIT ?
        """
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[jira_mirror] search_mirror 오류: %s", e)
        return []
    finally:
        conn.close()


def get_mirror_age_str(project_key: str = None, db_path: str = None) -> str:
    """마지막 동기화로부터 경과 시간을 "N분 전" 형식으로 반환한다.

    Args:
        project_key: 프로젝트 키 (None이면 전체 미러 기준)
        db_path: SQLite DB 경로

    Returns:
        "N분 전" / "N시간 전" / "미러 없음"
    """
    conn = _get_conn(db_path)
    try:
        if project_key:
            row = conn.execute(
                "SELECT MAX(synced_at) FROM jira_mirror WHERE project_key = ?",
                (project_key,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT MAX(synced_at) FROM jira_mirror"
            ).fetchone()

        synced_at_str = row[0] if row else None
        if not synced_at_str:
            return "미러 없음"

        synced_at = datetime.strptime(synced_at_str, "%Y-%m-%dT%H:%M:%S")
        diff_sec = (datetime.now() - synced_at).total_seconds()
        diff_min = int(diff_sec / 60)

        if diff_min < 60:
            return f"{diff_min}분 전"
        diff_hr = int(diff_min / 60)
        return f"{diff_hr}시간 전"

    except Exception as e:
        logger.error("[jira_mirror] get_mirror_age_str 오류: %s", e)
        return "미러 없음"
    finally:
        conn.close()


def is_mirror_fresh(project_key: str = None, max_age_min: int = 30,
                    db_path: str = None) -> bool:
    """미러가 max_age_min 이내에 동기화됐으면 True를 반환한다."""
    conn = _get_conn(db_path)
    try:
        if project_key:
            row = conn.execute(
                "SELECT MAX(synced_at) FROM jira_mirror WHERE project_key = ?",
                (project_key,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT MAX(synced_at) FROM jira_mirror"
            ).fetchone()

        synced_at_str = row[0] if row else None
        if not synced_at_str:
            return False

        synced_at = datetime.strptime(synced_at_str, "%Y-%m-%dT%H:%M:%S")
        diff_min = (datetime.now() - synced_at).total_seconds() / 60
        return diff_min <= max_age_min

    except Exception as e:
        logger.error("[jira_mirror] is_mirror_fresh 오류: %s", e)
        return False
    finally:
        conn.close()
