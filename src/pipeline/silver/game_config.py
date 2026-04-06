from typing import Any

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

# Level thresholds follow the standard starter progression (typically 16 / 36).
STARTER_EVOLUTION_CHAINS_BY_BASE: dict[str, list[tuple[int, str]]] = {
    "bulbasaur": [(1, "bulbasaur"), (16, "ivysaur"), (36, "venusaur")],
    "charmander": [(1, "charmander"), (16, "charmeleon"), (36, "charizard")],
    "squirtle": [(1, "squirtle"), (16, "wartortle"), (36, "blastoise")],
    "chikorita": [(1, "chikorita"), (16, "bayleef"), (32, "meganium")],
    "cyndaquil": [(1, "cyndaquil"), (14, "quilava"), (36, "typhlosion")],
    "totodile": [(1, "totodile"), (18, "croconaw"), (30, "feraligatr")],
    "treecko": [(1, "treecko"), (16, "grovyle"), (36, "sceptile")],
    "torchic": [(1, "torchic"), (16, "combusken"), (36, "blaziken")],
    "mudkip": [(1, "mudkip"), (16, "marshtomp"), (36, "swampert")],
    "turtwig": [(1, "turtwig"), (18, "grotle"), (32, "torterra")],
    "chimchar": [(1, "chimchar"), (14, "monferno"), (36, "infernape")],
    "piplup": [(1, "piplup"), (16, "prinplup"), (36, "empoleon")],
    "snivy": [(1, "snivy"), (17, "servine"), (36, "serperior")],
    "tepig": [(1, "tepig"), (17, "pignite"), (36, "emboar")],
    "oshawott": [(1, "oshawott"), (17, "dewott"), (36, "samurott")],
    "chespin": [(1, "chespin"), (16, "quilladin"), (36, "chesnaught")],
    "fennekin": [(1, "fennekin"), (16, "braixen"), (36, "delphox")],
    "froakie": [(1, "froakie"), (16, "frogadier"), (36, "greninja")],
}

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
    chain = STARTER_EVOLUTION_CHAINS_BY_BASE.get(base_starter.lower())
    if not chain:
        return base_starter.lower()

    resolved = chain[0][1]
    for threshold, species in chain:
        if level >= threshold:
            resolved = species
        else:
            break
    return resolved


def get_starter_family_members(base_starter: str) -> list[str]:
    chain = STARTER_EVOLUTION_CHAINS_BY_BASE.get(base_starter.lower())
    if not chain:
        return [base_starter.lower()]
    return [species for _, species in chain]

