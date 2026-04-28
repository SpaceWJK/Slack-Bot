"""MCP Cache Layer 설정."""

import os
from pathlib import Path

# ── 프로젝트 경로 ────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"
LOGS_DIR = PROJECT_ROOT / "logs"

# ── SQLite ────────────────────────────────────────────────────
DB_PATH = str(CACHE_DIR / "mcp_cache.db")

# ── 캐시 정책 ────────────────────────────────────────────────
DEFAULT_TTL_HOURS = 24          # 캐시 기본 유효 시간
CLEANUP_DAYS = 30               # 미조회 문서 자동 정리 기준 (일)

# ── GDI 캐시 정책 ────────────────────────────────────────────
GDI_FOLDER_TTL_HOURS = 6        # 폴더 목록 캐시 TTL (업무 중 새 파일 업로드 대비)
GDI_FILE_TTL_HOURS = 24         # 파일 내용 캐시 TTL (기획서는 변경 빈도 낮음)
GDI_MEM_TTL_SEC = 300           # 인메모리 L1 캐시 TTL (5분)

# ── Jira 캐시 정책 ────────────────────────────────────────────
JIRA_ISSUE_TTL_HOURS = 0.17     # 이슈 캐시 TTL (~10분, 상태 변경 대비)
JIRA_PROJECT_TTL_HOURS = 1      # 프로젝트 상세 캐시 TTL
JIRA_PROJECTS_TTL_HOURS = 24    # 프로젝트 목록 캐시 TTL (거의 변하지 않음)
JIRA_MEM_TTL_SEC = 300          # 인메모리 L1 캐시 TTL (5분)

# ── 쿼리 예산 ────────────────────────────────────────────────
MAX_DOCS_PER_QUERY = 20         # 단일 쿼리에서 처리할 최대 문서 수
MAX_BODY_CHARS = 500_000        # 캐시 저장 시 본문 최대 글자 수 (20K→200K→500K)
MAX_TREE_DEPTH = 5              # 트리 탐색 최대 깊이

# ── MCP 소스별 설정 ──────────────────────────────────────────
WIKI_SPACE_KEY = os.getenv("WIKI_SPACE_KEY", "QASGP")

# ── Enrichment 설정 ─────────────────────────────────────────
ENRICH_BATCH_SIZE = 500         # 배치 처리 단위
SUMMARY_MAX_CHARS = 300         # 요약 최대 글자 수
KEYWORDS_MAX_COUNT = 10         # 키워드 최대 개수
ENRICH_MIN_BODY_LEN = 20       # 이 미만이면 enrichment 스킵
ENTITY_MIN_BODY_LEN = 50       # 이 미만이면 entity 추출 스킵

# ── 디렉토리 자동 생성 ──────────────────────────────────────
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
