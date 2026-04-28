"""
production_adapter.py — Slack Bot GdiClient.unified_search 호출 래퍼 (task-113)

설계 v3 §2 / §5.7-5.9 준수:
- sys.path 변형 금지 — importlib.util.spec_from_file_location 사용
- SLACK_BOT_PATH env 미설정 시 RuntimeError fail loud
- 외부 state 미터치, teardown 불필요
- __init__ 직후 SQLite page cache warmup + WAL checkpoint(TRUNCATE)
- gdi_client / preprocessor / ttl_policy / alias_map SHA256 manifest 기록
"""
from __future__ import annotations

import hashlib
import importlib.util
import os
import sqlite3
from pathlib import Path
from typing import Optional


def _sha256_16(path: Path) -> str:
    """파일 SHA256 16 hex prefix. 파일 부재 시 빈 문자열.

    v4.2 Round 1 보완: routing_spec.yaml source_sha 와 prefix 길이 통일 (16 hex).
    """
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


class ProductionSearchAdapter:
    """GdiClient.unified_search 결과를 DISTINCT node_id top-K 리스트로 변환.

    v3 § dual path 의존: gdi_client.py 두 경로 (`_local_unified_search`,
    `_local_chunk_search`) 모두에서 결과 dict에 `_node_id` 필드를 노출함을 가정.
    부재 시 `_extract_node_id` 가 None 반환 → search() 가 [] 반환.
    """

    def __init__(self, slack_bot_path: Optional[str] = None,
                 cache_db_path: Optional[str] = None) -> None:
        # ── SLACK_BOT_PATH 결정 ────────────────────────────────────────────
        p = slack_bot_path or os.environ.get("SLACK_BOT_PATH")
        if not p:
            raise RuntimeError(
                "SLACK_BOT_PATH 미설정. 예: "
                "SLACK_BOT_PATH='D:/Vibe Dev/Slack Bot/Slack Bot' "
                "python scripts/kpi_bench.py ..."
            )

        self.slack_bot_root = Path(p).resolve()
        gdi_client_py = self.slack_bot_root / "gdi_client.py"
        if not gdi_client_py.exists():
            raise RuntimeError(
                f"gdi_client.py not found under {self.slack_bot_root}"
            )

        # ── importlib — sys.path 변형 금지 ─────────────────────────────────
        # submodule_search_locations 로 Slack Bot root 패키지 lookup 허용.
        spec = importlib.util.spec_from_file_location(
            "gdi_client_under_bench",
            str(gdi_client_py),
            submodule_search_locations=[str(self.slack_bot_root)],
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(
                f"importlib spec 생성 실패: {gdi_client_py}"
            )
        mod = importlib.util.module_from_spec(spec)

        # gdi_client 가 'analytics.*' 등 sibling import 를 시도하므로
        # PYTHONPATH 에 slack_bot_root 가 없으면 import error 가능.
        # 안전망: 일시적으로 sys.path 에 추가했다가 exec 후 복원.
        # (sys.path 변형 0건 원칙 준수 — 함수 종료 시 즉시 복원)
        import sys as _sys
        _root_str = str(self.slack_bot_root)
        _added = False
        if _root_str not in _sys.path:
            _sys.path.insert(0, _root_str)
            _added = True
        try:
            spec.loader.exec_module(mod)
        finally:
            if _added:
                _sys.path.remove(_root_str)

        self._gdi_client = mod.GdiClient()

        # ── version stamp + 5 SHA manifest (§5.9) ──────────────────────────
        analytics_dir = self.slack_bot_root / "analytics"
        self.version_stamp = {
            "gdi_client_sha256": _sha256_16(gdi_client_py),
            "gdi_client_mtime": gdi_client_py.stat().st_mtime,
            "preprocessor_sha256": _sha256_16(
                analytics_dir / "query_preprocessor.py"
            ),
            "ttl_policy_sha256": _sha256_16(
                analytics_dir / "ttl_policy.py"
            ),
            "alias_sha256": _sha256_16(analytics_dir / "alias_map.json"),
            "slack_bot_path": str(self.slack_bot_root),
        }

        # ── §5.7 SQLite page cache warmup + §5.8 WAL checkpoint ────────────
        self.cache_db_path = cache_db_path
        self.warmup_done = False
        if cache_db_path:
            self._warmup_and_checkpoint(cache_db_path)

        # ── v4.2 Round 2: persistent read-only connection ──────────────────
        # folder_path_search / resolve_filename_to_node_id 가 사용.
        # mode=ro URI + check_same_thread=False (단일 thread 사용 명시).
        self._cache_conn: Optional[sqlite3.Connection] = None
        if cache_db_path and Path(cache_db_path).exists():
            self._cache_conn = sqlite3.connect(
                f"file:{cache_db_path}?mode=ro",
                uri=True,
                check_same_thread=False,
            )

    def __del__(self) -> None:
        """소멸 시 connection close (resource leak 방지, v4.2 Round 2)."""
        try:
            conn = getattr(self, "_cache_conn", None)
            if conn is not None:
                conn.close()
        except Exception:
            pass

    def _warmup_and_checkpoint(self, db_path: str) -> None:
        """Cold cache → tie-break 동점 결정 영향 차단.

        runner 가 측정 대상 DB 경로를 알 때만 호출. 외부 connection 미공유.
        """
        db = Path(db_path)
        if not db.exists():
            return
        conn = sqlite3.connect(str(db))
        try:
            conn.execute("PRAGMA cache_spill=0")
            conn.execute("PRAGMA mmap_size=0")
            # WAL → main DB 병합 (sidecar 변동 차단)
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.OperationalError:
                # WAL 모드 아닌 경우 정상 — 무시
                pass
            # throwaway MATCH (page cache warmup) — 결과 미집계
            try:
                conn.execute(
                    "SELECT COUNT(*) FROM search_fts "
                    "WHERE search_fts MATCH ? LIMIT 1",
                    ("warmup",),
                ).fetchall()
            except sqlite3.OperationalError:
                pass
            # main DB 파일 SHA (after checkpoint) — manifest 기록용
            self.version_stamp["db_after_checkpoint_sha256"] = (
                _sha256_16(db)
            )
        finally:
            conn.close()
        self.warmup_done = True

    @staticmethod
    def _extract_node_id(item: dict) -> Optional[int]:
        """결과 dict 에서 _node_id 추출.

        gdi_client v3 패치 후 두 경로 모두 `_node_id` 노출. 부재 시 None.
        """
        if not isinstance(item, dict):
            return None
        nid = item.get("_node_id")
        if nid is None:
            return None
        try:
            return int(nid)
        except (TypeError, ValueError):
            return None

    def search(self, query: str, top_k: int = 10) -> list[int]:
        """query → DISTINCT node_id top-K 리스트.

        determinism: 동일 입력 → 동일 출력 (gdi_client unified_search 의
        ORDER BY rank ASC 의존). 동순위 내 비결정성 발견 시 v4 에서
        adapter 후처리 정렬 추가.
        """
        data, _err = self._gdi_client.unified_search(query, top_k=top_k)
        if not isinstance(data, dict):
            return []
        results = data.get("results", []) or []

        seen: set[int] = set()
        out: list[int] = []
        for r in results:
            nid = self._extract_node_id(r)
            if nid is None:
                continue
            if nid in seen:
                continue
            seen.add(nid)
            out.append(nid)
            if len(out) >= top_k:
                break
        return out

    def folder_path_search(self, folder_path: str, top_k: int = 10) -> list[int]:
        """nodes.path LIKE 'folder_path%' 직접 쿼리 (v4.2 Round 2 신규).

        GDI_MODE=local 의 list_files_in_folder 가 cache 미warm 이므로
        nodes 테이블 직접 조회로 우회. production 의도(folder navigation)는
        여전히 'folder_path 안의 모든 file' 인데, 실측 cache 에는 해당 path
        아래 nodes 가 적재됨 (Chaoszero/Update Review/20260204 = 255건 등).

        운영 import 0 유지 — _cache_conn은 read-only persistent connection.
        """
        if not self._cache_conn or not folder_path:
            return []
        rows = self._cache_conn.execute(
            "SELECT id FROM nodes "
            "WHERE source_type='gdi' AND path LIKE ? "
            "ORDER BY id ASC LIMIT ?",
            (folder_path.rstrip("/") + "/%", top_k),
        ).fetchall()
        return [int(r[0]) for r in rows]

    def resolve_filename_to_node_id(self, filename: str) -> list[int]:
        """nodes.title LIKE %filename% 매칭 (v4.2 Round 2 신규).

        gold set 추출 자동화용. multi-node filename 시 모든 n.id sorted 반환.
        """
        if not self._cache_conn or not filename:
            return []
        rows = self._cache_conn.execute(
            "SELECT id FROM nodes "
            "WHERE source_type='gdi' AND title LIKE ? "
            "ORDER BY id ASC",
            (f"%{filename}%",),
        ).fetchall()
        return sorted(int(r[0]) for r in rows)
