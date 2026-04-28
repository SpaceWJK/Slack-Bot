"""Entity Extractor — 로컬 규칙 기반 엔티티 추출.

캐시 DB의 body_text / title에서 game_name / team_name / date / issue_number 엔티티를
Python 표준 라이브러리만으로 추출하여 doc_meta.entities(JSON 배열)에 저장합니다.

외부 API / LLM 호출 완전 금지. 순수 정규식 + 화이트리스트 규칙 기반.

사용법:
    # Python
    from src.entity_extractor import EntityExtractor
    e = EntityExtractor()
    entities = e.extract("에픽세븐 EPIC-123 2026-04-23", "테스트")

    # CLI (드라이런)
    python -m src.entity_extractor --gdi --dry-run

    # 전체 적용
    python -m src.entity_extractor --gdi --apply

    # 통계 확인
    python -m src.entity_extractor --stats
"""

import json
import logging
import re
import sqlite3
import time

from . import config

log = logging.getLogger("mcp_cache")


# ── 게임명 별칭 사전 ──────────────────────────────────────────
# alias → space_key 매핑.
# 주의: 2자 이하 alias (e7, cz, l9)는 false positive 위험으로 제외.
#   "E7 포트", "CZ 파일", "L9 레벨" 등 기술 문맥에서 오탐 가능성 높음.
#   space_key는 nodes 테이블 메타에서 이미 확인 가능하므로 body_text 매칭 불필요.
_GAME_ALIASES: dict[str, str] = {
    "에픽세븐": "epicseven",
    "에픽7": "epicseven",
    "epic seven": "epicseven",
    "epic7": "epicseven",
    "epicseven": "epicseven",
    "카오스제로": "chaoszero",
    "카오스 제로": "chaoszero",
    "chaos zero": "chaoszero",
    "chaoszero": "chaoszero",
    "로드나인": "lordnine_asia",
    "lord nine": "lordnine_asia",
    "lordnine": "lordnine_asia",
    "카제나": "kazena",
    "kazena": "kazena",
}

# ── 이슈번호 프로젝트 화이트리스트 ───────────────────────────
# 주의: E7은 game_name alias 삭제로 충돌 해소. LNG 근거 없으므로 제거.
_KNOWN_PROJECTS: frozenset[str] = frozenset({
    "EPIC", "CHAOS", "QA", "OPS", "LN", "DEV",
})

# ── 팀명 코드 매핑 ────────────────────────────────────────────
_TEAM_CANONICAL: dict[str, str] = {
    "QA": "QA",
    "서버": "SERVER",
    "클라이언트": "CLIENT",
    "기획": "PLANNING",
    "아트": "ART",
    "개발": "DEV",
    "운영": "OPS",
}


# ── 정규식 패턴 (모듈 임포트 시 1회 컴파일) ──────────────────

# 게임명: 긴 alias 먼저 (greedy precedence).
# \b가 한글 경계에서 작동하지 않으므로, 한글 포함 alias는 lookaround로 보완.
_GAME_PATTERN = re.compile(
    r'(?<![가-힣A-Za-z0-9_])('
    + '|'.join(re.escape(k) for k in sorted(_GAME_ALIASES, key=len, reverse=True))
    + r')(?![가-힣A-Za-z0-9_])',
    re.IGNORECASE,
)

# 팀명 — 한국어 \b는 ASCII \w 기준이므로 lookbehind로 한글 선행 차단.
# (?<![가-힣\w]): 한글 또는 영숫자 직전에 오는 경우 제외 (예: "A사QA팀" 오탐 방지).
_TEAM_PATTERN = re.compile(
    # lookbehind: 한글/영숫자 직전 차단 ("A사QA팀" 오탐 방지)
    # lookahead: 팀원·팀장 등 합성어 차단하되, 조사(에서/이/을 등)는 허용
    r'(?<![가-힣\w])(QA|서버|클라이언트|기획|아트|개발|운영)(\s?팀|\s?[Tt]eam)(?!원|장|님)'
)

# 날짜 패턴들
_DATE_ISO = re.compile(r'\b(\d{4})-(\d{2})-(\d{2})\b', re.ASCII)
_DATE_KOREAN = re.compile(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일')
_DATE_SLASH = re.compile(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b')
_DATE_RELATIVE = re.compile(r'(오늘|어제|이번\s*주|저번\s*주|이번\s*달|저번\s*달)')

# 이슈번호
_ISSUE_PATTERN = re.compile(r'\b([A-Z]{2,10})-(\d{1,6})\b')

# 배치 처리 단위
BATCH_SIZE = 200


# ── EntityExtractor ───────────────────────────────────────────

class EntityExtractor:
    """로컬 규칙 기반 엔티티 추출기.

    외부 API / LLM 없이 Python 표준 라이브러리만 사용.
    추출 결과는 JSON 배열(list[dict])로 반환.
    """

    # ── 공개 API ─────────────────────────────────────────────

    def extract(
        self,
        text: str,
        title: str = "",
        space_key: str = "",
    ) -> list[dict]:
        """text + title에서 엔티티를 추출하여 반환.

        Args:
            text: 본문 텍스트
            title: 문서 제목 (제목 우선 적용)
            space_key: 현재 노드의 space_key (미사용, 미래 확장용)

        Returns:
            엔티티 dict 목록. 각 항목은 type, value, (canonical), (format), (project) 포함.
        """
        combined = f"{title}\n{text}" if title else text
        results: list[dict] = []
        seen: set[tuple[str, str]] = set()

        # 1. game_name — 긴 alias 먼저 매칭 (re.compile에서 정렬됨)
        for m in _GAME_PATTERN.finditer(combined):
            val_lower = m.group().lower()
            key = ("game_name", val_lower)
            if key not in seen:
                seen.add(key)
                canonical = _GAME_ALIASES.get(val_lower, val_lower)
                results.append({
                    "type": "game_name",
                    "value": m.group(),
                    "canonical": canonical,
                    "span": [m.start(), m.end()],
                })

        # 2. team_name
        for m in _TEAM_PATTERN.finditer(combined):
            team_key_raw = m.group(1)
            key = ("team_name", m.group().lower())
            if key not in seen:
                seen.add(key)
                results.append({
                    "type": "team_name",
                    "value": m.group(),
                    "canonical": _TEAM_CANONICAL.get(team_key_raw, team_key_raw),
                })

        # 3. date — ISO (신뢰도 높음) → 한국어 → 슬래시 → 상대
        for m in _DATE_ISO.finditer(combined):
            y_val, mo_val, d_val = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo_val <= 12 and 1 <= d_val <= 31:
                key = ("date", m.group())
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "type": "date",
                        "value": m.group(),
                        "format": "iso",
                    })

        for m in _DATE_KOREAN.finditer(combined):
            mo_val, d_val = int(m.group(2)), int(m.group(3))
            if 1 <= mo_val <= 12 and 1 <= d_val <= 31:
                key = ("date", m.group())
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "type": "date",
                        "value": m.group(),
                        "format": "korean",
                    })

        for m in _DATE_SLASH.finditer(combined):
            mo_val, d_val = int(m.group(1)), int(m.group(2))
            if 1 <= mo_val <= 12 and 1 <= d_val <= 31:
                key = ("date", m.group())
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "type": "date",
                        "value": m.group(),
                        "format": "slash_mmdd",
                    })

        for m in _DATE_RELATIVE.finditer(combined):
            key = ("date", m.group())
            if key not in seen:
                seen.add(key)
                results.append({
                    "type": "date",
                    "value": m.group(),
                    "format": "relative",
                })

        # 4. issue_number — 화이트리스트 프로젝트는 known, 나머지는 UNKNOWN
        for m in _ISSUE_PATTERN.finditer(combined):
            proj = m.group(1)
            key = ("issue_number", m.group())
            if key not in seen:
                seen.add(key)
                results.append({
                    "type": "issue_number",
                    "value": m.group(),
                    "project": proj if proj in _KNOWN_PROJECTS else "UNKNOWN",
                })

        return results

    def extract_batch(
        self,
        conn: sqlite3.Connection,
        source_type: str = "gdi",
        force: bool = False,
        batch_size: int = BATCH_SIZE,
        space_key: "str | None" = None,
    ) -> dict:
        """일괄 추출 + doc_meta.entities 저장 (멱등 — 재실행 시 덮어쓰기).

        force=False: entities='[]' 인 노드만 처리 (기본값, partial index 활용 빠름)
        force=True: 전체 재처리 (중단-재개 시 처음부터 재처리 — 멱등이라 안전)

        Phase 2 성능 수정 (task-105, schema v9 partial index + seek pagination):
          - offset=0 재페치 제거 → `n.id > last_id` seek cursor
          - TEMP B-TREE 제거 (SQLite 쿼리 플랜 확인)
          - 짧은 body_text(< ENTITY_MIN_BODY_LEN)는 WHERE에서 제외 → --stats 정확도
          - WAL PASSIVE checkpoint 1000건마다
          - 속도 이상 감지 (5분/100건 미만) → 조기 탈출 (세션 30 200h 재발 방지)

        Args:
            conn: SQLite 연결
            source_type: 'gdi' | 'wiki' | 'jira'
            force: True이면 이미 추출된 노드도 재처리 (중단-재개 시 처음부터 재시작)
            batch_size: 배치 크기
            space_key: 특정 space_key 필터 (--game 옵션용, None이면 전체)

        Returns:
            {"total", "extracted", "skipped", "errors", "duration_sec"}
        """
        conn.execute("PRAGMA busy_timeout = 10000")

        t0 = time.time()
        total_processed = 0
        extracted = 0
        skipped = 0
        errors = 0
        last_id = 0  # seek pagination cursor (Phase 2 fix)
        min_body_len = config.ENTITY_MIN_BODY_LEN

        # space_key 조건 + 파라미터 — force/non-force 양쪽 공용
        space_clause = "AND n.space_key = ?" if space_key else ""
        space_params = (space_key,) if space_key else ()

        while True:
            if force:
                # force: 전체 재처리. partial index 미활용이지만 seek cursor로 TEMP B-TREE 제거.
                rows = conn.execute(
                    f"""
                    SELECT n.id, n.title, n.space_key, dc.body_text
                    FROM nodes n
                    JOIN doc_content dc ON dc.node_id = n.id
                    WHERE n.source_type = ?
                      {space_clause}
                      AND n.id > ?
                    ORDER BY n.id
                    LIMIT ?
                    """,
                    (source_type, *space_params, last_id, batch_size),
                ).fetchall()
            else:
                # 미처리만: partial index(idx_meta_entities_empty) + LENGTH 필터로 짧은 본문 제외
                rows = conn.execute(
                    f"""
                    SELECT n.id, n.title, n.space_key, dc.body_text
                    FROM nodes n
                    JOIN doc_content dc ON dc.node_id = n.id
                    LEFT JOIN doc_meta dm ON dm.node_id = n.id
                    WHERE n.source_type = ?
                      {space_clause}
                      AND (dm.entities IS NULL OR dm.entities = '[]')
                      AND LENGTH(dc.body_text) >= ?
                      AND n.id > ?
                    ORDER BY n.id
                    LIMIT ?
                    """,
                    (source_type, *space_params, min_body_len, last_id, batch_size),
                ).fetchall()

            if not rows:
                break

            for row in rows:
                total_processed += 1
                node_id = row[0]
                title = row[1] or ""
                sk = row[2] or ""
                body_text = row[3] or ""

                try:
                    # body_text가 너무 짧으면 스킵 (force 모드만 도달 — force=False는 WHERE에서 걸러짐)
                    if len(body_text.strip()) < min_body_len:
                        skipped += 1
                        continue

                    entities = self.extract(body_text, title=title, space_key=sk)
                    entities_json = json.dumps(entities, ensure_ascii=False)

                    # orphan 노드(doc_meta 없음): INSERT OR IGNORE 후 UPDATE
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO doc_meta (node_id, cached_at)
                        VALUES (?, strftime('%Y-%m-%dT%H:%M:%S','now','localtime'))
                        """,
                        (node_id,),
                    )
                    conn.execute(
                        "UPDATE doc_meta SET entities = ? WHERE node_id = ?",
                        (entities_json, node_id),
                    )
                    extracted += 1

                except (sqlite3.OperationalError, sqlite3.DatabaseError, ValueError) as exc:
                    errors += 1
                    if errors <= 5:
                        log.warning(
                            "[Entity] node#%d (%s) 오류: %s",
                            node_id, title, exc,
                        )

            # seek cursor 갱신 (offset=0 재페치 제거)
            last_id = rows[-1][0]

            # 배치 단위 COMMIT + 진행 로그
            conn.commit()
            log.info(
                "[Entity %s] 진행: %d건 (extracted=%d, skip=%d, err=%d) last_id=%d",
                source_type, total_processed, extracted, skipped, errors, last_id,
            )

            # WAL PASSIVE checkpoint (chunk_builder 패턴) — 1000건마다
            if total_processed and total_processed % 1000 < batch_size:
                try:
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except sqlite3.OperationalError:
                    pass  # WAL 미사용 환경 무시

            # 속도 이상 조기 탈출 (Step 3 MAJOR-4 — 세션 30 200h 재발 방지)
            elapsed = time.time() - t0
            if elapsed > 300 and total_processed < 100:
                raise RuntimeError(
                    f"[Entity] 속도 이상 감지: {elapsed:.0f}초에 {total_processed}건만 처리. "
                    f"중단합니다. EXPLAIN QUERY PLAN 확인 필요."
                )

            if len(rows) < batch_size:
                break

        # 마지막 배치 COMMIT
        conn.commit()

        duration = round(time.time() - t0, 1)
        result = {
            "total": total_processed,
            "extracted": extracted,
            "skipped": skipped,
            "errors": errors,
            "duration_sec": duration,
        }
        log.info("[Entity %s] 완료: %s", source_type, result)
        return result

    def get_stats(self, conn: sqlite3.Connection) -> dict:
        """entities 컬럼 채움 현황 통계.

        Returns:
            {"by_source": {source_type: {"total": int, "extracted": int, "pct": float}}}
        """
        rows = conn.execute(
            """
            SELECT n.source_type,
                   COUNT(*) AS total,
                   SUM(CASE WHEN dm.entities != '[]' AND dm.entities IS NOT NULL THEN 1 ELSE 0 END) AS extracted
            FROM nodes n
            LEFT JOIN doc_meta dm ON dm.node_id = n.id
            GROUP BY n.source_type
            """
        ).fetchall()

        by_source: dict[str, dict] = {}
        for row in rows:
            src = row[0]
            total = row[1] or 0
            extr = row[2] or 0
            pct = round(extr / total * 100, 1) if total > 0 else 0.0
            by_source[src] = {
                "total": total,
                "extracted": extr,
                "pct": pct,
            }

        return {"by_source": by_source}


# ── CLI 진입점 ───────────────────────────────────────────────

def main() -> None:
    """커맨드라인에서 entity 추출 실행."""
    import argparse
    from .models import get_connection, migrate

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="MCP Cache Entity Extractor")
    parser.add_argument("--gdi", action="store_true", help="GDI 노드 추출")
    parser.add_argument("--wiki", action="store_true", help="Wiki 노드 추출")
    parser.add_argument("--jira", action="store_true", help="Jira 노드 추출")
    parser.add_argument("--all", action="store_true", help="전체 소스 추출")
    parser.add_argument("--apply", action="store_true", help="실제 DB에 저장")
    parser.add_argument("--dry-run", action="store_true", help="저장 없이 미리보기 (100건)")
    parser.add_argument("--force", action="store_true", help="이미 추출된 노드도 재처리")
    parser.add_argument("--node", type=int, help="단일 node_id 추출 (디버그용)")
    parser.add_argument("--stats", action="store_true", help="추출 통계만 출력")
    parser.add_argument("--game", help="특정 space_key 필터 (chaoszero/epicseven/lordnine_asia 등)")
    args = parser.parse_args()

    # DB 마이그레이션 (v7 entities 컬럼 + v9 partial index 자동 적용)
    migrate()
    conn = get_connection()
    extractor = EntityExtractor()

    if args.force:
        log.warning(
            "[Entity] --force 모드: 중단-재개 시 처음부터 재처리됩니다. "
            "이미 추출된 entities는 덮어쓰기됩니다 (멱등적이라 데이터 손상은 없음)."
        )

    if args.stats:
        # Step 3 MAJOR-3: WAL 지연 방지용 PASSIVE checkpoint
        try:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.OperationalError:
            pass
        stats = extractor.get_stats(conn)
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        conn.close()
        return

    if args.node:
        # 단일 노드 디버그
        row = conn.execute(
            """
            SELECT n.id, n.title, n.space_key, n.source_type,
                   dc.body_text
            FROM nodes n
            JOIN doc_content dc ON dc.node_id = n.id
            WHERE n.id = ?
            """,
            (args.node,),
        ).fetchone()
        if not row:
            print(f"node_id={args.node} 없음")
            conn.close()
            return
        entities = extractor.extract(row[4] or "", title=row[1] or "", space_key=row[2] or "")
        print(json.dumps(entities, indent=2, ensure_ascii=False))
        conn.close()
        return

    if args.dry_run:
        # 드라이런: 첫 100건 미리보기 (저장 안 함)
        source_type = "gdi"
        if args.wiki:
            source_type = "wiki"
        elif args.jira:
            source_type = "jira"

        rows = conn.execute(
            """
            SELECT n.id, n.title, n.space_key, dc.body_text
            FROM nodes n
            JOIN doc_content dc ON dc.node_id = n.id
            WHERE n.source_type = ?
            LIMIT 100
            """,
            (source_type,),
        ).fetchall()

        sample_results = []
        for row in rows:
            body = row[3] or ""
            if len(body.strip()) < config.ENTITY_MIN_BODY_LEN:
                continue
            entities = extractor.extract(body, title=row[1] or "", space_key=row[2] or "")
            if entities:
                sample_results.append({
                    "node_id": row[0],
                    "title": row[1],
                    "entities": entities,
                })

        print(json.dumps(sample_results[:20], indent=2, ensure_ascii=False))
        print(f"\n총 {len(rows)}건 중 엔티티 있음: {len(sample_results)}건 (드라이런, 저장 안 함)")
        conn.close()
        return

    if not args.apply:
        parser.print_help()
        conn.close()
        return

    # 실제 추출 + 저장
    sources: list[str] = []
    if args.all:
        sources = ["wiki", "jira", "gdi"]
    else:
        if args.wiki:
            sources.append("wiki")
        if args.jira:
            sources.append("jira")
        if args.gdi:
            sources.append("gdi")

    if not sources:
        parser.print_help()
        conn.close()
        return

    results: dict[str, dict] = {}
    for src in sources:
        results[src] = extractor.extract_batch(
            conn, source_type=src, force=args.force, space_key=args.game
        )

    print("\n=== Entity 추출 결과 ===")
    for src, r in results.items():
        print(f"  {src}: {r}")

    print("\n=== Entity 추출 통계 ===")
    stats = extractor.get_stats(conn)
    for src, s in stats.get("by_source", {}).items():
        print(f"  {src}: {s['extracted']}/{s['total']} ({s['pct']}%)")

    conn.close()


if __name__ == "__main__":
    main()
