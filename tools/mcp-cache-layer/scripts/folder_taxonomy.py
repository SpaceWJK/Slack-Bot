"""
folder_taxonomy.py — GDI 폴더 택소노미 인덱스

gdi-repo/ 폴더 구조를 스캔하여 SQLite 인덱스를 생성하고,
자연어 질의를 폴더 경로로 해석한다.

사용법:
    # 인덱스 빌드
    python scripts/folder_taxonomy.py --build

    # 질의 테스트
    python scripts/folder_taxonomy.py --query "카제나 2/4타겟 3차 빌드 테스트 결과"

    # 인덱스 통계
    python scripts/folder_taxonomy.py --stats
"""

import re
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger("folder_taxonomy")

# ── 경로 설정 ────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GDI_REPO = PROJECT_ROOT / "gdi-repo"
DB_PATH = str(PROJECT_ROOT / "cache" / "mcp_cache.db")

# ── 별칭 사전 ────────────────────────────────────────────────────────────────

GAME_ALIASES: dict[str, str] = {
    # 한글
    "카제나": "Chaoszero",
    "카오스제로": "Chaoszero",
    "카제": "Chaoszero",
    "에픽세븐": "Epicseven",
    "에픽": "Epicseven",
    "로드나인": "Lordnine_Asia",
    "로나": "Lordnine_Asia",
    # 영문 / 약어
    "chaoszero": "Chaoszero",
    "cz": "Chaoszero",
    "epicseven": "Epicseven",
    "e7": "Epicseven",
    "lordnine": "Lordnine_Asia",
    "lordnine_asia": "Lordnine_Asia",
    "ln": "Lordnine_Asia",
}

CATEGORY_ALIASES: dict[str, str] = {
    # 한글
    "테스트 결과": "Test Result",
    "테스트결과": "Test Result",
    "테스트": "Test Result",
    "테결": "Test Result",
    "업데이트 리뷰": "Update Review",
    "업데이트리뷰": "Update Review",
    "업리": "Update Review",
    "데이터": "TSV",
    "대시보드": "Dashboard",
    "라이브 이슈": "Live Issue",
    "라이브이슈": "Live Issue",
    # 영문 / 약어
    "test result": "Test Result",
    "tr": "Test Result",
    "update review": "Update Review",
    "ur": "Update Review",
    "tsv": "TSV",
    "dashboard": "Dashboard",
    "live issue": "Live Issue",
}

# ── 날짜 정규식 ──────────────────────────────────────────────────────────────

# 사용자 입력에서 날짜를 추출하는 패턴 (순서 중요: 구체적 → 일반적)
_DATE_PATTERNS: list[tuple[re.Pattern, str]] = [
    # M/D타겟, M/D 타겟
    (re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})\s*타겟"), "slash_target"),
    # M.D타겟, M.DD 타겟 (점 구분자)
    (re.compile(r"(?<!\d)(\d{1,2})\s*\.\s*(\d{1,2})\s*타겟"), "dot_target"),
    # M월D일, M월 D일
    (re.compile(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일"), "korean"),
    # M/D (단독)
    (re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})"), "slash"),
    # M.D (점 구분자, 단독 — 버전 번호 1.0.244 오매칭 방지: lookbehind+lookahead)
    (re.compile(r"(?<!\d)(\d{1,2})\s*\.\s*(\d{1,2})(?!\d)"), "dot"),
    # MMDD (4자리 숫자, 앞뒤가 숫자가 아닌 경우)
    (re.compile(r"(?<!\d)(\d{2})(\d{2})(?!\d)"), "mmdd"),
]

# 폴더명에서 날짜를 추출하는 패턴
_FOLDER_DATE_RE = re.compile(
    r"^(?:20)?(\d{2})(\d{2})(\d{2})([a-z])?(?:\s.*)?$"  # YYMMDD or YYYYMMDD + 선택적 접미사 + 공백 후 임의 텍스트
)


def _extract_mmdd(folder_name: str) -> str | None:
    """폴더명에서 MMDD 추출. 예: '260204' → '0204', '20260225b' → '0225'."""
    m = _FOLDER_DATE_RE.match(folder_name)
    if m:
        return m.group(2) + m.group(3)  # MM + DD
    return None


# ── 빌드 정규식 ──────────────────────────────────────────────────────────────

# 사용자 입력에서 빌드 정보를 추출하는 패턴
_BUILD_PATTERNS: list[tuple[re.Pattern, str]] = [
    # 핫픽스 N차, 핫픽스 N-M차
    (re.compile(r"핫픽스\s*(\d+)(?:-(\d+))?\s*차"), "hotfix_kr"),
    # hotfix N차
    (re.compile(r"(?:hotfix|hotifx)\s*(\d+)\s*차", re.IGNORECASE), "hotfix_en"),
    # 정규 N차
    (re.compile(r"정규\s*(\d+)\s*차"), "regular"),
    # N-M차 (예: 3-1차, 2-4차)
    (re.compile(r"(\d+)-(\d+)\s*차"), "range"),
    # N차빌드, N차 빌드
    (re.compile(r"(\d+)\s*차\s*빌드"), "build_suffix"),
    # N차 (단독, 가장 느슨한 패턴이므로 마지막)
    (re.compile(r"(\d+)\s*차"), "simple"),
]

# 폴더명에서 빌드 정보를 추출하는 패턴
_FOLDER_BUILD_PATTERNS: list[tuple[re.Pattern, str]] = [
    # 정규 N,M차 / 정규 N차
    (re.compile(r"^정규\s+(.+)차$"), "regular"),
    # Hotfix N차 / Hotifx N차
    (re.compile(r"^(?:Hotfix|Hotifx)\s+(\d+)차$", re.IGNORECASE), "hotfix"),
    # hotfix #N / hotfix#N / hotfix # N~M (해시 형태)
    (re.compile(r"^(?:Hotfix|Hotifx)\s*#\s*(\d+)(?:[~-](\d+))?$", re.IGNORECASE), "hotfix_hash"),
    # N-M차
    (re.compile(r"^(\d+)-(\d+)차$"), "range"),
    # YYYY.MM.DD ... N차 전달 / N-M차 전달
    (re.compile(r"^\d{4}\.\d{2}\.\d{2}\s+(.+)\s+전달$"), "delivery"),
    # YYYY.MM.DD ... N차 (전달 없는 형태, 예: "2026.01.10 정규 3차")
    (re.compile(r"^\d{4}\.\d{2}\.\d{2}\s+(.+)$"), "delivery"),
    # N차빌드, N차 빌드 (공백 선택적)
    (re.compile(r"^(\d+)차\s*빌드$"), "build_suffix"),
    # N차 (단독, 예: "2차", "3차" — 빌드 접미사 없음)
    (re.compile(r"^(\d+)차$"), "simple"),
    # 클라이언트 버전 (1.0.NNN ...) — 연도(2xxx)는 제외
    (re.compile(r"^([01]\.\d+\.\d+)\s+(.+)$"), "client_version"),
    # QA, QA/BVT
    (re.compile(r"^QA"), "qa"),
    # 완료, 진행, 진행중
    (re.compile(r"^(완료|진행중?|QA)$"), "status"),
]


def _classify_build_folder(folder_name: str) -> dict:
    """빌드 폴더명을 분류한다. 빌드 번호, 타입 등 반환."""
    info: dict = {"raw": folder_name, "type": "unknown", "numbers": []}

    for pat, btype in _FOLDER_BUILD_PATTERNS:
        m = pat.match(folder_name)
        if not m:
            continue

        info["type"] = btype

        if btype == "regular":
            # "1,2" or "3" 등
            nums_str = m.group(1)
            info["numbers"] = [int(n) for n in re.findall(r"\d+", nums_str)]
        elif btype == "hotfix":
            info["numbers"] = [int(m.group(1))]
        elif btype == "hotfix_hash":
            # hotfix #N or hotfix #N~M
            info["type"] = "hotfix"  # 통합 분류
            info["numbers"] = [int(m.group(1))]
            if m.group(2):
                info["numbers"].append(int(m.group(2)))
        elif btype == "range":
            info["numbers"] = [int(m.group(1)), int(m.group(2))]
        elif btype == "delivery":
            # "1차", "3-2차", "핫픽스 2차", "1차 핫픽스", "정규 3차"
            detail = m.group(1)
            nums = [int(n) for n in re.findall(r"\d+", detail)]
            info["numbers"] = nums
            if "핫픽스" in detail or "hotfix" in detail.lower():
                info["type"] = "hotfix_delivery"
            elif "정규" in detail:
                info["type"] = "regular_delivery"
            else:
                info["type"] = "delivery"
        elif btype == "build_suffix":
            info["numbers"] = [int(m.group(1))]
        elif btype == "simple":
            info["numbers"] = [int(m.group(1))]
        elif btype == "client_version":
            info["numbers"] = []
            info["version"] = m.group(1)
        elif btype in ("qa", "status"):
            info["numbers"] = []

        return info

    return info


# ── QueryParser ──────────────────────────────────────────────────────────────

class QueryParser:
    """자연어 질의에서 게임/카테고리/날짜/빌드를 추출한다."""

    @staticmethod
    def extract_game(query: str) -> tuple[str | None, str]:
        """게임명 추출. (게임 폴더명, 나머지 텍스트) 반환."""
        q_lower = query.lower()
        # 긴 별칭부터 매칭 (카오스제로 > 카제)
        for alias in sorted(GAME_ALIASES.keys(), key=len, reverse=True):
            if alias in q_lower:
                game = GAME_ALIASES[alias]
                # 매칭된 부분 제거
                idx = q_lower.find(alias)
                remainder = query[:idx] + query[idx + len(alias):]
                return game, remainder.strip()
        return None, query

    @staticmethod
    def extract_category(query: str) -> tuple[str | None, str]:
        """카테고리 추출. (카테고리 폴더명, 나머지 텍스트) 반환."""
        q_lower = query.lower()
        for alias in sorted(CATEGORY_ALIASES.keys(), key=len, reverse=True):
            if alias in q_lower:
                cat = CATEGORY_ALIASES[alias]
                idx = q_lower.find(alias)
                remainder = query[:idx] + query[idx + len(alias):]
                return cat, remainder.strip()
        return None, query

    @staticmethod
    def extract_date(query: str) -> tuple[str | None, str]:
        """날짜(MMDD) 추출. (MMDD, 나머지 텍스트) 반환."""
        for pat, _ in _DATE_PATTERNS:
            m = pat.search(query)
            if m:
                mm = m.group(1).zfill(2)
                dd = m.group(2).zfill(2)
                mmdd = mm + dd
                # 유효성 검증
                if 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                    remainder = query[:m.start()] + query[m.end():]
                    return mmdd, remainder.strip()
        return None, query

    @staticmethod
    def extract_build(query: str) -> tuple[dict | None, str]:
        """빌드 정보 추출. (빌드 dict, 나머지 텍스트) 반환."""
        for pat, btype in _BUILD_PATTERNS:
            m = pat.search(query)
            if m:
                info: dict = {"type": btype, "numbers": []}
                if btype == "hotfix_kr":
                    info["numbers"] = [int(m.group(1))]
                    if m.group(2):
                        info["numbers"].append(int(m.group(2)))
                elif btype == "hotfix_en":
                    info["numbers"] = [int(m.group(1))]
                elif btype == "regular":
                    info["numbers"] = [int(m.group(1))]
                elif btype == "range":
                    info["numbers"] = [int(m.group(1)), int(m.group(2))]
                elif btype == "build_suffix":
                    info["numbers"] = [int(m.group(1))]
                elif btype == "simple":
                    info["numbers"] = [int(m.group(1))]
                remainder = query[:m.start()] + query[m.end():]
                return info, remainder.strip()
        return None, query

    @classmethod
    def parse(cls, query: str) -> dict:
        """질의를 파싱하여 구조화된 dict 반환."""
        result: dict = {
            "game": None,
            "category": None,
            "date_mmdd": None,
            "build": None,
            "remainder": "",
        }

        q = query.strip()
        result["game"], q = cls.extract_game(q)
        result["category"], q = cls.extract_category(q)
        result["date_mmdd"], q = cls.extract_date(q)
        result["build"], q = cls.extract_build(q)
        result["remainder"] = q.strip()

        return result


# ── FolderIndex ──────────────────────────────────────────────────────────────

class FolderIndex:
    """gdi-repo 폴더 구조를 스캔하여 SQLite에 인덱스를 저장하고,
    파싱된 질의 조건으로 폴더 경로를 조회한다."""

    def __init__(self, db_path: str = DB_PATH, repo_path: Path = GDI_REPO):
        self.db_path = db_path
        self.repo_path = repo_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    # ── 인덱스 빌드 ────────────────────────────────────────────

    def build(self, game_filter: str | None = None) -> dict:
        """gdi-repo를 스캔하여 folder_index 테이블을 (재)빌드한다.

        Returns:
            {"scanned": int, "indexed": int, "games": list[str]}
        """
        conn = self._connect()

        # 테이블 생성 (마이그레이션 미적용 시 대비)
        conn.executescript("""
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
        """)

        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        scanned = 0
        indexed = 0
        games_found: list[str] = []

        # 게임 디렉토리 순회
        for game_dir in sorted(self.repo_path.iterdir()):
            if not game_dir.is_dir():
                continue
            game_name = game_dir.name
            if game_filter and game_name.lower() != game_filter.lower():
                continue

            games_found.append(game_name)

            # 기존 인덱스 삭제 (해당 게임만)
            conn.execute("DELETE FROM folder_index WHERE game=?", (game_name,))

            # 카테고리 디렉토리 순회
            for cat_dir in sorted(game_dir.iterdir()):
                if not cat_dir.is_dir():
                    continue
                category = cat_dir.name
                scanned += 1

                # 날짜 폴더 순회
                for date_dir in sorted(cat_dir.iterdir()):
                    if not date_dir.is_dir():
                        continue
                    date_folder = date_dir.name
                    date_mmdd = _extract_mmdd(date_folder)
                    scanned += 1

                    # 빌드/하위 폴더 순회
                    has_subfolders = False
                    for build_dir in sorted(date_dir.iterdir()):
                        if not build_dir.is_dir():
                            continue
                        has_subfolders = True
                        build_folder = build_dir.name
                        scanned += 1

                        # 상태 폴더 확인 (완료/진행/QA 등)
                        status_dirs = [
                            d for d in build_dir.iterdir()
                            if d.is_dir() and d.name in (
                                "완료", "진행", "진행중", "QA"
                            )
                        ]

                        if status_dirs:
                            for status_dir in status_dirs:
                                full_path = (
                                    f"{game_name}/{category}/{date_folder}"
                                    f"/{build_folder}/{status_dir.name}"
                                )
                                file_count = self._count_files(status_dir)
                                conn.execute(
                                    "INSERT OR REPLACE INTO folder_index "
                                    "(game, category, date_folder, date_mmdd, "
                                    "build_folder, status_folder, full_path, "
                                    "file_count, scanned_at) "
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (game_name, category, date_folder,
                                     date_mmdd, build_folder, status_dir.name,
                                     full_path, file_count, now),
                                )
                                indexed += 1
                        else:
                            full_path = (
                                f"{game_name}/{category}/{date_folder}"
                                f"/{build_folder}"
                            )
                            file_count = self._count_files(build_dir)
                            conn.execute(
                                "INSERT OR REPLACE INTO folder_index "
                                "(game, category, date_folder, date_mmdd, "
                                "build_folder, status_folder, full_path, "
                                "file_count, scanned_at) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (game_name, category, date_folder,
                                 date_mmdd, build_folder, "",
                                 full_path, file_count, now),
                            )
                            indexed += 1

                    # 날짜 폴더 자체에 파일이 있고 하위 폴더가 없는 경우
                    if not has_subfolders:
                        full_path = f"{game_name}/{category}/{date_folder}"
                        file_count = self._count_files(date_dir)
                        if file_count > 0:
                            conn.execute(
                                "INSERT OR REPLACE INTO folder_index "
                                "(game, category, date_folder, date_mmdd, "
                                "build_folder, status_folder, full_path, "
                                "file_count, scanned_at) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (game_name, category, date_folder,
                                 date_mmdd, "", "",
                                 full_path, file_count, now),
                            )
                            indexed += 1

        conn.commit()
        conn.close()

        return {"scanned": scanned, "indexed": indexed, "games": games_found}

    @staticmethod
    def _count_files(directory: Path) -> int:
        """디렉토리 내 지원 확장자 파일 수 (재귀, _images 제외)."""
        exts = {".xlsx", ".tsv", ".pptx", ".docx", ".png", ".jpg", ".jpeg"}
        count = 0
        for f in directory.rglob("*"):
            if f.is_file() and f.suffix.lower() in exts and "_images" not in f.parts:
                count += 1
        return count

    # ── 질의 해석 ──────────────────────────────────────────────

    def resolve_query(self, query: str) -> list[dict]:
        """자연어 질의를 해석하여 매칭되는 폴더 목록을 반환한다.

        Returns:
            list of {"full_path": str, "file_count": int, ...}
            빈 리스트면 해석 실패.
        """
        parsed = QueryParser.parse(query)
        game = parsed["game"]
        category = parsed["category"]
        date_mmdd = parsed["date_mmdd"]
        build = parsed["build"]

        # 최소 게임명이 있어야 택소노미 조회 시도
        if not game:
            return []

        conn = self._connect()
        conditions: list[str] = ["game = ?"]
        params: list = [game]

        if category:
            conditions.append("category = ?")
            params.append(category)

        if date_mmdd:
            conditions.append("date_mmdd = ?")
            params.append(date_mmdd)

        where = " AND ".join(conditions)
        sql = f"SELECT * FROM folder_index WHERE {where} ORDER BY full_path"

        rows = conn.execute(sql, params).fetchall()

        # 빌드 필터링 (SQL로 표현하기 어려운 복합 조건)
        if build and rows:
            rows = self._filter_by_build(rows, build)

        conn.close()

        return [dict(r) for r in rows]

    def resolve_to_source_ids(self, query: str) -> list[str]:
        """자연어 질의 → nodes 테이블의 source_id 리스트."""
        folders = self.resolve_query(query)
        if not folders:
            return []

        conn = self._connect()
        source_ids: list[str] = []

        for folder in folders:
            path_prefix = folder["full_path"] + "/"
            cur = conn.execute(
                "SELECT source_id FROM nodes "
                "WHERE source_type = 'gdi' AND source_id LIKE ?",
                (path_prefix + "%",),
            )
            source_ids.extend(row[0] for row in cur.fetchall())

        conn.close()
        return source_ids

    def get_files_with_content(self, query: str, max_files: int = 20) -> list[dict]:
        """자연어 질의 → 파일 내용 포함 결과 리스트.

        Returns:
            list of {"source_id": str, "title": str, "body_text": str,
                     "char_count": int, "source_type": str}
        """
        folders = self.resolve_query(query)
        if not folders:
            return []

        conn = self._connect()
        results: list[dict] = []

        for folder in folders:
            if len(results) >= max_files:
                break
            path_prefix = folder["full_path"] + "/"
            cur = conn.execute(
                "SELECT n.source_id, n.title, n.source_type, "
                "       dc.body_text, dc.char_count "
                "FROM nodes n "
                "JOIN doc_content dc ON dc.node_id = n.id "
                "WHERE n.source_type = 'gdi' AND n.source_id LIKE ? "
                "ORDER BY n.source_id "
                "LIMIT ?",
                (path_prefix + "%", max_files - len(results)),
            )
            for row in cur.fetchall():
                results.append(dict(row))

        conn.close()
        return results

    @staticmethod
    def _filter_by_build(rows: list, build: dict) -> list:
        """빌드 조건으로 폴더 행을 필터링한다."""
        filtered = []
        btype = build.get("type", "")
        nums = build.get("numbers", [])

        if not nums:
            return list(rows)

        primary = nums[0]  # 주 빌드 번호
        secondary = nums[1] if len(nums) > 1 else None

        for row in rows:
            bf = row["build_folder"]
            if not bf:
                continue

            bf_info = _classify_build_folder(bf)
            bf_type = bf_info["type"]
            bf_nums = bf_info["numbers"]

            matched = False

            if btype == "range":
                # N-M차 → 정확 매칭
                if bf_type == "range" and bf_nums == [primary, secondary]:
                    matched = True
                elif bf_type in ("delivery", "regular_delivery", "hotfix_delivery") \
                        and bf_nums == [primary, secondary]:
                    matched = True
            elif btype in ("hotfix_kr", "hotfix_en"):
                # 핫픽스 N차 → hotfix 계열만
                if bf_type in ("hotfix", "hotfix_delivery"):
                    if primary in bf_nums:
                        matched = True
            elif btype == "regular":
                # 정규 N차 → regular 계열 매칭
                if bf_type in ("regular", "regular_delivery") and primary in bf_nums:
                    matched = True
            elif btype in ("simple", "build_suffix"):
                # N차 → 넓은 매칭 (N으로 시작하는 모든 빌드)
                # 단, 사용자가 핫픽스를 명시하지 않았으므로 hotfix 폴더는 제외
                if bf_type in ("hotfix", "hotfix_delivery"):
                    pass  # 핫픽스 폴더 제외
                elif primary in bf_nums:
                    matched = True
                # N-*차 패턴도 포함 (3차 → 3-1차, 3-2차 등)
                elif bf_nums and bf_nums[0] == primary:
                    matched = True

            if matched:
                filtered.append(row)

        return filtered

    # ── 통계 ───────────────────────────────────────────────────

    def stats(self) -> dict:
        """인덱스 통계 반환."""
        conn = self._connect()
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM folder_index"
            ).fetchone()[0]

            by_game = {}
            cur = conn.execute(
                "SELECT game, category, COUNT(*), SUM(file_count) "
                "FROM folder_index GROUP BY game, category ORDER BY game, category"
            )
            for row in cur.fetchall():
                game = row[0]
                if game not in by_game:
                    by_game[game] = {}
                by_game[game][row[1]] = {
                    "folders": row[2],
                    "files": row[3] or 0,
                }

            return {"total_folders": total, "by_game": by_game}
        finally:
            conn.close()


# ── CLI ──────────────────────────────────────────────────────────────────────

def _main():
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    args = sys.argv[1:]
    idx = FolderIndex()

    if "--build" in args:
        game = None
        for a in args:
            if a != "--build" and not a.startswith("-"):
                game = a
                break
        print(f"인덱스 빌드 시작... (game={game or 'all'})")
        result = idx.build(game_filter=game)
        print(f"  스캔: {result['scanned']}개 폴더")
        print(f"  인덱싱: {result['indexed']}개 항목")
        print(f"  게임: {', '.join(result['games'])}")

    elif "--query" in args:
        qi = args.index("--query")
        query = " ".join(args[qi + 1:])
        if not query:
            print("사용법: --query <질의문>")
            return

        print(f"질의: {query}")
        print()

        parsed = QueryParser.parse(query)
        print("파싱 결과:")
        for k, v in parsed.items():
            print(f"  {k}: {v}")
        print()

        folders = idx.resolve_query(query)
        print(f"매칭 폴더: {len(folders)}개")
        for f in folders:
            print(f"  {f['full_path']} ({f['file_count']}파일)")
        print()

        source_ids = idx.resolve_to_source_ids(query)
        print(f"매칭 파일: {len(source_ids)}개")
        for sid in source_ids[:10]:
            print(f"  {sid}")
        if len(source_ids) > 10:
            print(f"  ... 외 {len(source_ids) - 10}개")

    elif "--stats" in args:
        s = idx.stats()
        print(f"전체 폴더: {s['total_folders']}개")
        for game, cats in s["by_game"].items():
            print(f"\n{game}:")
            for cat, info in cats.items():
                print(f"  {cat}: {info['folders']}폴더, {info['files']}파일")

    else:
        print(__doc__)


if __name__ == "__main__":
    _main()
