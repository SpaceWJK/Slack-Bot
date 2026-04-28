"""Claude Code용 JSON 인덱스 export.

캐시 DB의 트리 구조 + 요약을 JSON 파일로 내보내어,
Claude Code 세션에서 Read 도구로 바로 조회할 수 있게 합니다.

사용법:
    from src.exporters import export_wiki_index
    export_wiki_index("QASGP")
    # → cache/exports/wiki_QASGP_index.json 생성
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from . import config
from .cache_logger import ops_log
from .cache_manager import CacheManager

log = logging.getLogger("mcp_cache")

EXPORTS_DIR = config.CACHE_DIR / "exports"


def export_wiki_index(
    space_key: str,
    cache: CacheManager | None = None,
    output_dir: Path | None = None,
) -> str:
    """Wiki 공간의 트리 인덱스를 JSON으로 export.

    Returns: 생성된 파일 경로
    """
    cm = cache or CacheManager()
    out_dir = output_dir or EXPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    nodes = cm.get_tree("wiki", space_key)
    stats = cm.get_stats()

    # 노드별로 메타 + enrichment 정보 병합
    index_entries = []
    for node in nodes:
        meta = cm.get_meta(node["id"])
        content = cm.get_content(node["id"])
        entry = {
            "title": node["title"],
            "path": node["path"],
            "source_id": node["source_id"],
            "node_type": node["node_type"],
            "url": node["url"],
        }
        if meta:
            entry["last_modified"] = meta["last_modified"]
            entry["version"] = meta["version"]
            entry["author"] = meta["author"]
            entry["cached_at"] = meta["cached_at"]
        # enrichment 데이터 (summary/keywords)
        if content:
            if content.get("summary"):
                entry["summary"] = content["summary"]
            kw_raw = content.get("keywords") or ""
            if kw_raw:
                try:
                    entry["keywords"] = json.loads(kw_raw)
                except (ValueError, TypeError):
                    pass
        index_entries.append(entry)

    output = {
        "space_key": space_key,
        "exported_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_pages": len(index_entries),
        "stats": {
            "total_nodes": stats["total_nodes"],
            "total_content": stats["total_content"],
            "total_chars": stats["total_chars"],
            "db_size_kb": stats["db_size_kb"],
        },
        "pages": index_entries,
    }

    out_path = out_dir / f"wiki_{space_key}_index.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Wiki 인덱스 export: %s (%d건)", out_path, len(index_entries))
    ops_log.export(space_key, pages=len(index_entries), path=str(out_path))
    return str(out_path)


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    space = sys.argv[1] if len(sys.argv) > 1 else "QASGP"
    path = export_wiki_index(space)
    print(f"Export 완료: {path}")
