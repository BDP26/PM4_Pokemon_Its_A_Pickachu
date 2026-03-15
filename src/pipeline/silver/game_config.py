from typing import Any

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
                }
            )
    return games_config