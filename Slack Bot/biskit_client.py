"""
biskit_client.py — BISKIT MCP HTTP 클라이언트

BISKIT: 내부 게임 데이터 지표 서비스
엔드포인트: https://mcp.sginfra.net/biskit-report-mcp
인증: Bearer ${BISKIT_TOKEN}

McpSession 패턴 (jira_client.py 기반)

스키마 (tools/list 실측 2026-06-04):
  search_datasets:       project_id(int), keywords(array[str])
  get_dataset_parameters: dataset_ids(array[int])
  execute_query:         dataset_id(int), parameters(?)
"""

import os
import logging
from mcp_core import McpSession

logger = logging.getLogger(__name__)

BISKIT_MCP_URL = os.getenv(
    "BISKIT_MCP_URL", "https://mcp.sginfra.net/biskit-report-mcp"
)

_mcp_session: "McpSession | None" = None


def _get_mcp() -> McpSession:
    global _mcp_session
    if _mcp_session is None:
        token = os.getenv("BISKIT_TOKEN", "").strip()
        if not token:
            logger.warning("[biskit] BISKIT_TOKEN 환경변수가 설정되지 않았습니다.")
        _mcp_session = McpSession(
            url=BISKIT_MCP_URL,
            headers={"Authorization": f"Bearer {token}"},
            label="biskit",
        )
    return _mcp_session


def call_tool(tool_name: str, arguments: dict) -> dict:
    """BISKIT MCP 도구 호출. 성공 시 dict/list 반환, 실패 시 RuntimeError."""
    mcp = _get_mcp()
    result, err = mcp.call_tool(tool_name, arguments, timeout=30)
    if err:
        raise RuntimeError(f"BISKIT MCP 오류 ({tool_name}): {err}")
    if result is None:
        return {}
    if isinstance(result, str):
        import json
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"raw": result}
    return result if isinstance(result, (dict, list)) else {"raw": result}


def list_projects() -> list:
    """프로젝트 목록 조회."""
    result = call_tool("list_projects", {})
    if isinstance(result, list):
        return result
    return result.get("projects", result.get("data", []))


def search_datasets(project_id, keyword: str) -> list:
    """데이터셋 검색.
    project_id: int (스키마 강제)
    keywords: array[str] (스키마 강제)
    """
    keywords_list = [k.strip() for k in keyword.split() if k.strip()] if keyword else []
    result = call_tool("search_datasets", {
        "project_id": int(project_id),
        "keywords": keywords_list,
    })
    if isinstance(result, list):
        return result
    return result.get("datasets", result.get("data", []))


def get_dataset_parameters(dataset_id) -> dict:
    """데이터셋 파라미터 정보 조회.
    dataset_ids: array[int] (스키마 강제)
    """
    return call_tool("get_dataset_parameters", {"dataset_ids": [int(dataset_id)]})


def execute_query(dataset_id, parameters: dict) -> dict:
    """데이터셋 쿼리 실행.
    dataset_id: int (스키마 강제)
    """
    params = {"dataset_id": int(dataset_id)}
    if parameters:
        params["parameters"] = parameters
    return call_tool("execute_query", params)
