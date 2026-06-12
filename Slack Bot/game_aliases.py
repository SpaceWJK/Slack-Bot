"""
game_aliases.py — 게임명 별칭 매핑 (Wiki + Jira 공용)

게임의 한국어명·영어명·약어 등 다양한 입력을 정규화하여
캐시 DB 경로 필터링, Jira 프로젝트 키 매핑에 활용합니다.

게임 정의 SSoT: D:/Vibe Dev/_lib/game_catalog.json (task-195, 2026-06-11)
신규 게임 추가는 카탈로그 JSON 1곳 수정. 이 모듈은 봇 전용 뷰 + 헬퍼만 유지.

사용법:
    from game_aliases import resolve_game, get_wiki_path_keywords, get_jira_project_key

    info = resolve_game("에픽세븐")
    # => {"canonical": "에픽세븐", "jira_key": "GEP", "wiki_path_keywords": [...], ...}
"""

import json
import os
from pathlib import Path

_CATALOG_PATH = Path(
    os.environ.get("GAME_CATALOG_PATH", "D:/Vibe Dev/_lib/game_catalog.json")
)

# ── 비게임 Jira 프로젝트 (카탈로그 스코프 밖 — 봇 로컬 정의) ─────
# 리젝(PRH)은 게임 프로젝트가 아니므로 카탈로그 미포함 (Master 확정 2026-06-11).
# 봇의 Jira/Wiki 검색 기능은 유지해야 하므로 여기에 잔류.
_LOCAL_PROJECTS = [
    {
        "canonical": "리젝",
        "aliases": ["리젝", "reject", "prh"],
        "jira_key": "PRH",
        "wiki_path_keywords": ["리젝", "PRH"],
        "wiki_ancestor_id": None,
    },
]


# 카탈로그 로드 실패 시 fallback (pm2 데몬 — import crash 시 무한 재시작 루프 방지)
_FALLBACK_GAMES = [
    {
        "canonical": "에픽세븐",
        "aliases": ["에픽세븐", "에픽 세븐", "에픽", "epic", "epicseven",
                    "epic seven", "epic7", "ep7", "gep"],
        "jira_key": "GEP",
        "wiki_path_keywords": ["에픽세븐", "EP |", "EP7"],
        "wiki_ancestor_id": 58043932,
    },
    {
        "canonical": "카제나",
        "aliases": ["카제나", "카오스제로", "카오스 제로", "카제나 카오스 나이트메어",
                    "chaoszero", "chaos zero", "chaoszero nightmare",
                    "chaos zero nightmare", "gcz", "cz"],
        "jira_key": "GCZ",
        "wiki_path_keywords": ["카제나", "CZ |", "GCZ", "카오스", "Chaoszero"],
        "wiki_ancestor_id": 650589593,
    },
    {
        "canonical": "로드나인",
        "aliases": ["로드나인", "lord nine", "lordnine", "ldn"],
        "jira_key": "LDN",
        "wiki_path_keywords": ["로드나인", "LDN"],
        "wiki_ancestor_id": None,
    },
    {
        "canonical": "로드나인 아시아",
        "aliases": ["로드나인아시아", "로드나인 아시아", "lord nine asia",
                    "lordnine asia", "lna"],
        "jira_key": "LNA",
        "wiki_path_keywords": ["로드나인 아시아", "LNA", "Lordnine_Asia"],
        "wiki_ancestor_id": None,
    },
]


def _load_games() -> list[dict]:
    """카탈로그 게임 + 봇 로컬 항목 → 기존 GAMES 형식. 로드 실패 시 fallback."""
    try:
        with open(_CATALOG_PATH, encoding="utf-8") as f:
            catalog = json.load(f)
        games = [
            {
                "canonical": g["canonical_ko"],
                "aliases": g["aliases"],
                "jira_key": g["jira_project_key"],
                "wiki_path_keywords": g["wiki_path_keywords"],
                "wiki_ancestor_id": g["wiki_ancestor_id"],
                "gdi_space_key": g.get("gdi_space_key"),
            }
            for g in catalog["games"]
        ]
        if not games:
            raise ValueError("카탈로그 게임 0건")
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        import logging
        logging.getLogger("slack_bot").error(
            "게임 카탈로그 로드 실패 (%s): %s — fallback GAMES 사용", _CATALOG_PATH, exc
        )
        games = _FALLBACK_GAMES
    return games + _LOCAL_PROJECTS


GAMES = _load_games()

# ── GDI 폴더 prefix (SSoT 도출 — predeploy P2-8) ─────────────────
# gdi_space_key('chaoszero' 등) → 실제 GDI 폴더 1depth('Chaoszero' 등).
# 신규 게임은 game_catalog.json에 gdi_space_key 추가만으로 반영.
GDI_GAME_PREFIXES = sorted({
    g["gdi_space_key"].capitalize()
    for g in GAMES
    if g.get("gdi_space_key")
})

# ── 별칭 → 게임 인덱스 (빌드) ────────────────────────────────────
_ALIAS_MAP: dict[str, dict] = {}

for _game in GAMES:
    for _alias in _game["aliases"]:
        _ALIAS_MAP[_alias.lower()] = _game


def resolve_game(text: str) -> "dict | None":
    """사용자 입력 텍스트에서 게임 정보를 해석합니다.

    텍스트 전체가 게임명 별칭이면 해당 게임 정보를 반환합니다.
    매칭 안 되면 None.

    Returns
    -------
    dict | None
        {"canonical", "aliases", "jira_key", "wiki_path_keywords"} 또는 None
    """
    key = text.strip().lower()
    return _ALIAS_MAP.get(key)


def detect_game_in_text(text: str) -> "dict | None":
    """텍스트(질문) 내에서 게임명을 감지합니다.

    별칭 중 가장 긴 매치를 우선으로, 텍스트 내 어디든 포함되면 감지.
    (예: "에픽세븐 2026년 핫픽스 알려줘" → 에픽세븐 게임 반환)

    Returns
    -------
    dict | None
    """
    text_lower = text.strip().lower()
    if not text_lower:
        return None

    # 긴 별칭부터 매칭 (예: "로드나인 아시아"가 "로드나인"보다 먼저)
    sorted_aliases = sorted(_ALIAS_MAP.keys(), key=len, reverse=True)
    for alias in sorted_aliases:
        if alias in text_lower:
            return _ALIAS_MAP[alias]

    return None


def get_wiki_path_keywords(game_name: str) -> "list[str] | None":
    """게임명 → Wiki 경로 필터링 키워드 목록.

    Returns: ["에픽세븐", "EP |", "EP7"] 또는 None
    """
    game = resolve_game(game_name)
    if game:
        return game["wiki_path_keywords"]
    return None


def get_jira_project_key(game_name: str) -> "str | None":
    """게임명 → Jira 프로젝트 키.

    Returns: "EP7" 또는 None
    """
    game = resolve_game(game_name)
    if game:
        return game["jira_key"]
    return None


def get_wiki_ancestor_id(game_name: str) -> "int | None":
    """게임명 → Wiki ancestor 페이지 ID (CQL ancestor 연산자용).

    Returns: 58043932 또는 None
    """
    game = resolve_game(game_name)
    if game:
        return game.get("wiki_ancestor_id")
    return None
