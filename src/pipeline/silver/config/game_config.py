from typing import Any
from functools import lru_cache
from src.pipeline.silver.inputs.connectors.pokeapi_evolution import get_species_evolution_rules

STARTER_CHOICES_BY_VERSION: dict[str, list[str]] = {
    "red": ["bulbasaur", "charmander", "squirtle"],
    "blue": ["bulbasaur", "charmander", "squirtle"],
    "gold": ["chikorita", "cyndaquil", "totodile"],
    "silver": ["chikorita", "cyndaquil", "totodile"],
    "ruby": ["treecko", "torchic", "mudkip"],
    "sapphire": ["treecko", "torchic", "mudkip"],
    "diamond": ["turtwig", "chimchar", "piplup"],
    "pearl": ["turtwig", "chimchar", "piplup"],
    "black": ["snivy", "tepig", "oshawott"],
    "white": ["snivy", "tepig", "oshawott"],
    "x": ["chespin", "fennekin", "froakie"],
    "y": ["chespin", "fennekin", "froakie"],
}

@lru_cache(maxsize=128)
def _starter_family_rules(base_starter: str) -> dict[str, dict[str, Any]]:
    base = str(base_starter).strip().lower()
    if not base:
        return {}
    try:
        return get_species_evolution_rules(base)
    except Exception:
        return {
            base: {
                "species_name": base,
                "base_species": base,
                "evolution_stage": 1,
                "min_valid_level": None,
                "min_level_from_previous": None,
                "special_evolution_conditions": [],
            }
        }


@lru_cache(maxsize=1)
def _starter_family_root_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    starter_bases = {starter for starters in STARTER_CHOICES_BY_VERSION.values() for starter in starters}
    for base in starter_bases:
        rules = _starter_family_rules(base)
        for species_name, info in rules.items():
            species = str(species_name).strip().lower()
            root = str(info.get("base_species") or base).strip().lower()
            if species:
                lookup[species] = root
    return lookup

BASE_GAME_GROUPS = [
    {
        "versions": ["red", "blue"],
        "root_title": "Walkthrough:Pokémon Red and Blue",
        "version_root_titles": {
            "red": ["Walkthrough:Pokémon Red Version", "Walkthrough:Pokémon Red"],
            "blue": ["Walkthrough:Pokémon Blue Version", "Walkthrough:Pokémon Blue"],
        },
        "route_prefix": "kanto-route",
        "bosses": [
            "Brock", "Misty", "Lt. Surge", "Erika", "Koga", "Sabrina", "Blaine",
            "Giovanni", "Lorelei", "Bruno", "Agatha", "Lance", "Blue",
        ],
    },
    {
        "versions": ["gold", "silver"],
        "root_title": "Walkthrough:Pokémon Gold and Silver",
        "version_root_titles": {
            "gold": ["Walkthrough:Pokémon Gold Version", "Walkthrough:Pokémon Gold"],
            "silver": ["Walkthrough:Pokémon Silver Version", "Walkthrough:Pokémon Silver"],
        },
        "route_prefix": "johto-route",
        "bosses": [
            "Falkner", "Bugsy", "Whitney", "Morty", "Chuck", "Jasmine", "Pryce",
            "Clair", "Will", "Koga", "Bruno", "Karen", "Lance",
        ],
    },
    {
        "versions": ["ruby", "sapphire"],
        "root_title": "Walkthrough:Pokémon Ruby and Sapphire",
        "version_root_titles": {
            "ruby": ["Walkthrough:Pokémon Ruby Version", "Walkthrough:Pokémon Ruby"],
            "sapphire": ["Walkthrough:Pokémon Sapphire Version", "Walkthrough:Pokémon Sapphire"],
        },
        "route_prefix": "hoenn-route",
        "bosses": [
            "Roxanne", "Brawly", "Wattson", "Flannery", "Norman", "Winona", "Tate and Liza",
            "Wallace", "Sidney", "Phoebe", "Glacia", "Drake", "Steven",
        ],
    },
    {
        "versions": ["diamond", "pearl"],
        "root_title": "Walkthrough:Pokémon Diamond and Pearl",
        "version_root_titles": {
            "diamond": ["Walkthrough:Pokémon Diamond Version", "Walkthrough:Pokémon Diamond"],
            "pearl": ["Walkthrough:Pokémon Pearl Version", "Walkthrough:Pokémon Pearl"],
        },
        "route_prefix": "sinnoh-route",
        "bosses": [
            "Roark", "Gardenia", "Maylene", "Crasher Wake", "Fantina", "Byron", "Candice",
            "Volkner", "Aaron", "Bertha", "Flint", "Lucian", "Cynthia",
        ],
    },
    {
        "versions": ["black", "white"],
        "root_title": "Walkthrough:Pokémon Black and White",
        "version_root_titles": {
            "black": ["Walkthrough:Pokémon Black Version", "Walkthrough:Pokémon Black"],
            "white": ["Walkthrough:Pokémon White Version", "Walkthrough:Pokémon White"],
        },
        "route_prefix": "unova-route",
        "bosses": [
            "Cilan", "Lenora", "Burgh", "Elesa", "Clay", "Skyla", "Brycen", "Drayden",
            "Shauntal", "Grimsley", "Caitlin", "Marshal", "N", "Ghetsis", "Alder",
        ],
    },
    {
        "versions": ["x", "y"],
        "root_title": "Walkthrough:Pokémon X and Y",
        "version_root_titles": {
            "x": ["Walkthrough:Pokémon X"],
            "y": ["Walkthrough:Pokémon Y"],
        },
        "route_prefix": "kalos-route",
        "bosses": [
            "Viola", "Grant", "Korrina", "Ramos", "Clemont", "Valerie", "Olympia", "Wulfric",
            "Malva", "Siebold", "Wikstrom", "Drasna", "Diantha",
        ],
    },
]


def get_games_config() -> list[dict[str, Any]]:
    games_config: list[dict[str, Any]] = []
    for group in BASE_GAME_GROUPS:
        for version in group["versions"]:
            version_titles = group.get("version_root_titles", {}).get(version, [])
            games_config.append(
                {
                    "game_key": version,
                    "root_title": group["root_title"],
                    "candidate_root_titles": version_titles + [group["root_title"]],
                    "route_prefix": group["route_prefix"],
                    "bosses": group["bosses"],
                    "starter_choices": STARTER_CHOICES_BY_VERSION.get(version, []),
                }
            )
    return games_config


def get_starter_choices(version: str) -> list[str]:
    return STARTER_CHOICES_BY_VERSION.get(version, [])


def resolve_starter_species_for_level(base_starter: str, level: int) -> str:
    base = str(base_starter).strip().lower()
    if not base:
        return ""

    rules = _starter_family_rules(base)
    if not rules:
        return base

    best_species = base
    best_rank = (-1, -1)
    for species, info in rules.items():
        if str(info.get("base_species") or "").strip().lower() != base:
            continue
        min_valid = info.get("min_valid_level")
        if min_valid is not None and int(level) < int(min_valid):
            continue
        stage = int(info.get("evolution_stage") or 1)
        threshold = int(min_valid) if min_valid is not None else 0
        rank = (stage, threshold)
        if rank >= best_rank:
            best_rank = rank
            best_species = str(species).strip().lower()

    return best_species



def get_starter_family_members(base_starter: str) -> list[str]:
    base = str(base_starter).strip().lower()
    if not base:
        return []

    rules = _starter_family_rules(base)
    members: list[tuple[int, int, str]] = []
    for species, info in rules.items():
        if str(info.get("base_species") or "").strip().lower() != base:
            continue
        stage = int(info.get("evolution_stage") or 1)
        min_valid = int(info.get("min_valid_level")) if info.get("min_valid_level") is not None else 0
        members.append((stage, min_valid, str(species).strip().lower()))

    if not members:
        return [base]

    members.sort(key=lambda item: (item[0], item[1], item[2]))
    return [species for _, _, species in members]


def get_starter_family_root(species: str) -> str:
    normalized = str(species).strip().lower()
    if not normalized:
        return ""
    return _starter_family_root_lookup().get(normalized, normalized)


