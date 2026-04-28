"""
routing_replay.py — Slack Bot routing 재현 (task-113 v4.2)

설계 v4.2 §2 준수:
- routing_spec.yaml 기반 self-contained 재현
- 운영 봇 import 0 (Slack SDK / Claude API client / 환경변수 chain 격리)
- 4 seam 지원: ask_claude_2part / ask_claude_3part_fallback / gdi_simple_search / folder_ai
- drift detection (T-21)
- fail-loud: routing_spec.yaml 부재 / 파싱 실패 / seams 부재 시 RuntimeError

v4.2 Round 2 보완:
- _folder_lookup → ProductionSearchAdapter.folder_path_search 사용 (list_files_in_folder 우회)
- _apply_transform: extract step 분기 추가
- _chain_lookup: NotImplementedError stub (folder_ai full chain은 후속 task)
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Optional

try:
    import yaml  # type: ignore
except ImportError as _e:  # pragma: no cover
    yaml = None  # type: ignore


class RoutingReplay:
    """slack_bot.py routing 재현 — 운영 import 0.

    routing_spec.yaml 로드 + ProductionSearchAdapter 호출.
    """

    def __init__(
        self,
        spec_path: str,
        adapter,
        slack_bot_root: Optional[Path] = None,
    ) -> None:
        if yaml is None:
            raise RuntimeError(
                "PyYAML 미설치. pip install pyyaml 필요."
            )
        spec_p = Path(spec_path)
        if not spec_p.exists():
            raise RuntimeError(
                f"routing_spec.yaml not found: {spec_p}. "
                "Run pm-analyst Round 0 extraction or check SLACK_BOT_PATH."
            )
        try:
            self.spec = yaml.safe_load(spec_p.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            raise RuntimeError(
                f"routing_spec.yaml parse error: {e}"
            ) from e
        if (
            not isinstance(self.spec, dict)
            or "seams" not in self.spec
            or not self.spec["seams"]
        ):
            raise RuntimeError(
                "routing_spec.yaml missing 'seams' section."
            )
        self.adapter = adapter
        self.slack_bot_root = slack_bot_root or (
            Path(adapter.slack_bot_root) if hasattr(adapter, "slack_bot_root") else None
        )
        self.drift_warning = self._detect_drift()

    def _detect_drift(self) -> bool:
        """spec.source_sha ↔ 현재 slack_bot.py SHA[:16] 비교 (v4.2 §2).

        Round 5 qa-structural M-3: source_sha 부재 시 stderr 경고 출력 (silent 비활성 회피).
        """
        import sys as _sys
        spec_sha = (self.spec.get("spec") or {}).get("source_sha", "")
        if not spec_sha:
            print(
                "[routing_replay] WARN: source_sha not in routing_spec.yaml "
                "— drift detection disabled.",
                file=_sys.stderr,
            )
            return False
        if not self.slack_bot_root:
            print(
                "[routing_replay] WARN: slack_bot_root unknown "
                "— drift detection disabled.",
                file=_sys.stderr,
            )
            return False
        sb_py = Path(self.slack_bot_root) / "slack_bot.py"
        if not sb_py.exists():
            return True
        actual_sha = hashlib.sha256(sb_py.read_bytes()).hexdigest()[:16]
        return actual_sha != spec_sha

    @staticmethod
    def _apply_transform(x: Any, step: dict) -> Any:
        """transform 단계 self-contained 적용 (v4.2 Round 2 보완).

        지원 step:
        - {split: '\\\\'} → x.split('\\\\')
        - {take_index: 0} → x[0]
        - {strip: true} → x.strip()
        - {extract: {search_kw: 0, file_name: 1}} → list[idx] → dict (v4.2 신규)
        - {format: '{search_kw} {file_name}'} → str.format(**x)

        Thread-safe: instance state 미변경.
        """
        if not isinstance(step, dict):
            return x
        if "split" in step:
            return x.split(step["split"]) if isinstance(x, str) else x
        if "take_index" in step:
            try:
                return x[step["take_index"]] if isinstance(x, list) else x
            except IndexError:
                return ""
        if step.get("strip"):
            return x.strip() if isinstance(x, str) else x
        if "extract" in step:
            mapping = step["extract"]
            if not isinstance(mapping, dict) or not isinstance(x, list):
                return x
            out: dict[str, str] = {}
            for key, idx in mapping.items():
                try:
                    out[key] = x[idx] if isinstance(idx, int) else ""
                except (IndexError, TypeError):
                    out[key] = ""
            return out
        if "format" in step:
            return step["format"].format(**x) if isinstance(x, dict) else x
        return x

    def replay(
        self, seam: str, raw_input: str, top_k: int = 10
    ) -> list[int]:
        """seam 별 raw_input → top-K node_ids 재현.

        Returns: list[int] (DISTINCT node_id top-K)
        """
        s = self.spec["seams"].get(seam)
        if s is None:
            raise RuntimeError(f"Unknown seam: {seam}")
        x: Any = raw_input
        for step in s.get("transform", []):
            x = self._apply_transform(x, step)
        method = (s.get("downstream") or {}).get("method", "")
        if method == "gdi_client.unified_search":
            # x 가 str이 아닌 경우 (extract 결과 dict 등 → format 미적용 케이스) skip
            if not isinstance(x, str):
                return []
            return self.adapter.search(x, top_k=top_k)
        elif method == "gdi_client.list_files_in_folder":
            # v4.2: list_files_in_folder 우회 → folder_path_search 사용
            if not isinstance(x, str):
                return []
            return self._folder_lookup(x, top_k=top_k)
        else:
            return self._chain_lookup(s, x, top_k=top_k)

    def _folder_lookup(self, folder_path: str, top_k: int) -> list[int]:
        """folder_ai stub — v4.2 Round 2 재설계 (nodes.path LIKE 직접).

        list_files_in_folder 우회 — adapter.folder_path_search 호출.
        secondary_retry: 게임명 prefix 추가 후 재호출 (§6.3.2).
        """
        node_ids = self.adapter.folder_path_search(folder_path, top_k=top_k)
        if node_ids:
            return node_ids
        for prefix in ("Chaoszero/", "Epicseven/", "Kazena/"):
            if not folder_path.startswith(prefix):
                node_ids = self.adapter.folder_path_search(
                    prefix + folder_path, top_k=top_k
                )
                if node_ids:
                    return node_ids
        return []

    def _chain_lookup(
        self, seam_spec: dict, raw_input: Any, top_k: int
    ) -> list[int]:
        """folder_ai full downstream_chain (5-step fallback) — v4 §10 Out of Scope.

        v4.2에서는 stub. 실제 chain (gc.get_file_content_full → search_by_filename
        → unified_search) 재현은 후속 task. v4.2는 _folder_lookup (step1만) 지원.
        """
        raise NotImplementedError(
            "folder_ai full chain (downstream_chain step0~4) — 후속 task. "
            "v4.2는 _folder_lookup (folder_path_search + secondary_retry) 만 지원."
        )
