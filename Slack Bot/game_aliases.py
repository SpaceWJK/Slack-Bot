"""
game_aliases.py — 게임명 별칭 매핑 (Wiki + Jira 공용)

게임의 한국어명·영어명·약어 등 다양한 입력을 정규화하여
캐시 DB 경로 필터링, Jira 프로젝트 키 매핑에 활용합니다.

사용법:
    from game_aliases import resolve_game, get_wiki_path_keywords, get_jira_project_key

    info = resolve_game("에픽세븐")
    # => {"canonical": "에픽세븐", "jira_key": "EP7", "wiki_keywords": ["에픽세븐", "EP"], ...}
"""

import re

# ── 게임 정의 ────────────────────────────────────────────────────
# canonical: 정규 이름 (한국어 표시용)
# aliases: 사용자가 입력할 수 있는 모든 이름 변형 (소문자 비교)
# jira_key: Jira 프로젝트 키
# wiki_path_keywords: Wiki 캐시 DB path에서 게임을 식별하는 키워드
#   (nodes 테이블의 title 또는 ancestor 경로에 포함됨)

GAMES = [
    {
        "canonical": "에픽세븐",
        "aliases": [
            "에픽세븐", "에픽 세븐", "에픽", "epic", "epicseven",
            "epic seven", "epic7", "ep7", "gep",
        ],
        "jira_key": "GEP",
        "wiki_path_keywords": ["에픽세븐", "EP |", "EP7"],
        "wiki_ancestor_id": 58043932,       # 에픽세븐 루트 페이지 ID
    },
    {
        "canonical": "카제나",
        "aliases": [
            "카제나", "카오스제로", "카오스 제로", "카제나 카오스 나이트메어",
            "chaoszero", "chaos zero", "chaoszero nightmare",
            "chaos zero nightmare", "gcz", "cz",
        ],
        "jira_key": "GCZ",
        "wiki_path_keywords": ["카제나", "CZ |", "GCZ", "카오스", "Chaoszero"],
        "wiki_ancestor_id": 650589593,      # CZ | Hotfix 이슈 부모 페이지 ID
    },
    {
        "canonical": "리젝",
        "aliases": ["리젝", "reject", "prh"],
        "jira_key": "PRH",
        "wiki_path_keywords": ["리젝", "PRH"],
        "wiki_ancestor_id": None,
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
        "aliases": [
            "로드나인아시아", "로드나인 아시아", "lord nine asia",
            "lordnine asia", "lna",
        ],
        "jira_key": "LNA",
        "wiki_path_keywords": ["로드나인 아시아", "LNA", "Lordnine_Asia"],
        "wiki_ancestor_id": None,
    },
]

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
