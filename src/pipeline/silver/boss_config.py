"""
Boss alias and endgame configuration.

- BOSS_ALIASES: heading aliases used to detect bosses directly
- ELITE_FOUR_BY_GAME: canonical Elite Four order per game
- CHAMPION_BY_GAME: canonical champion per game
"""

import re

BOSS_ALIASES: dict[str, dict[str, list[str]]] = {
    "red": {
        "Brock": ["brock", "pewter gym", "pewter city"],
        "Misty": ["misty", "cerulean gym", "cerulean city"],
        "Lt. Surge": ["lt surge", "lt. surge", "vermilion gym", "vermilion city"],
        "Erika": ["erika", "celadon gym", "celadon city"],
        "Koga": ["koga", "fuchsia gym", "fuchsia city"],
        "Sabrina": ["sabrina", "saffron gym", "saffron city"],
        "Blaine": ["blaine", "cinnabar gym", "cinnabar island"],
        "Giovanni": ["giovanni", "viridian gym", "viridian city"],
        "Lorelei": ["lorelei", "elite four lorelei"],
        "Bruno": ["bruno", "elite four bruno"],
        "Agatha": ["agatha", "elite four agatha"],
        "Lance": ["lance", "elite four lance"],
        "Blue": ["champion blue", "blue"],
    },
    "blue": {
        "Brock": ["brock", "pewter gym", "pewter city"],
        "Misty": ["misty", "cerulean gym", "cerulean city"],
        "Lt. Surge": ["lt surge", "lt. surge", "vermilion gym", "vermilion city"],
        "Erika": ["erika", "celadon gym", "celadon city"],
        "Koga": ["koga", "fuchsia gym", "fuchsia city"],
        "Sabrina": ["sabrina", "saffron gym", "saffron city"],
        "Blaine": ["blaine", "cinnabar gym", "cinnabar island"],
        "Giovanni": ["giovanni", "viridian gym", "viridian city"],
        "Lorelei": ["lorelei", "elite four lorelei"],
        "Bruno": ["bruno", "elite four bruno"],
        "Agatha": ["agatha", "elite four agatha"],
        "Lance": ["lance", "elite four lance"],
        "Blue": ["champion blue", "blue"],
    },
    "gold": {
        "Falkner": ["falkner", "violet gym", "violet city"],
        "Bugsy": ["bugsy", "azalea gym", "azalea town"],
        "Whitney": ["whitney", "goldenrod gym", "goldenrod city"],
        "Morty": ["morty", "ecruteak gym", "ecruteak city"],
        "Chuck": ["chuck", "cianwood gym", "cianwood city"],
        "Jasmine": ["jasmine", "olivine gym", "olivine city"],
        "Pryce": ["pryce", "mahogany gym", "mahogany town"],
        "Clair": ["clair", "blackthorn gym", "blackthorn city"],
        "Will": ["will", "elite four will"],
        "Koga": ["koga", "elite four koga"],
        "Bruno": ["bruno", "elite four bruno"],
        "Karen": ["karen", "elite four karen"],
        "Lance": ["champion lance", "lance"],
    },
    "silver": {
        "Falkner": ["falkner", "violet gym", "violet city"],
        "Bugsy": ["bugsy", "azalea gym", "azalea town"],
        "Whitney": ["whitney", "goldenrod gym", "goldenrod city"],
        "Morty": ["morty", "ecruteak gym", "ecruteak city"],
        "Chuck": ["chuck", "cianwood gym", "cianwood city"],
        "Jasmine": ["jasmine", "olivine gym", "olivine city"],
        "Pryce": ["pryce", "mahogany gym", "mahogany town"],
        "Clair": ["clair", "blackthorn gym", "blackthorn city"],
        "Will": ["will", "elite four will"],
        "Koga": ["koga", "elite four koga"],
        "Bruno": ["bruno", "elite four bruno"],
        "Karen": ["karen", "elite four karen"],
        "Lance": ["champion lance", "lance"],
    },
    "ruby": {
        "Roxanne": ["roxanne", "rustboro gym", "rustboro city"],
        "Brawly": ["brawly", "dewford gym", "dewford town"],
        "Wattson": ["wattson", "mauville gym", "mauville city"],
        "Flannery": ["flannery", "lavaridge gym", "lavaridge town"],
        "Norman": ["norman", "petalburg gym", "petalburg city"],
        "Winona": ["winona", "fortree gym", "fortree city"],
        "Tate and Liza": ["tate and liza", "tate & liza", "mossdeep gym", "mossdeep city"],
        "Wallace": ["wallace", "sootopolis gym", "sootopolis city"],
        "Sidney": ["sidney", "elite four sidney"],
        "Phoebe": ["phoebe", "elite four phoebe"],
        "Glacia": ["glacia", "elite four glacia"],
        "Drake": ["drake", "elite four drake"],
        "Steven": ["champion steven", "steven"],
    },
    "sapphire": {
        "Roxanne": ["roxanne", "rustboro gym", "rustboro city"],
        "Brawly": ["brawly", "dewford gym", "dewford town"],
        "Wattson": ["wattson", "mauville gym", "mauville city"],
        "Flannery": ["flannery", "lavaridge gym", "lavaridge town"],
        "Norman": ["norman", "petalburg gym", "petalburg city"],
        "Winona": ["winona", "fortree gym", "fortree city"],
        "Tate and Liza": ["tate and liza", "tate & liza", "mossdeep gym", "mossdeep city"],
        "Wallace": ["wallace", "sootopolis gym", "sootopolis city"],
        "Sidney": ["sidney", "elite four sidney"],
        "Phoebe": ["phoebe", "elite four phoebe"],
        "Glacia": ["glacia", "elite four glacia"],
        "Drake": ["drake", "elite four drake"],
        "Steven": ["champion steven", "steven"],
    },
    "diamond": {
        "Roark": ["roark", "oreburgh gym", "oreburgh city"],
        "Gardenia": ["gardenia", "eterna gym", "eterna city"],
        "Maylene": ["maylene", "veilstone gym", "veilstone city"],
        "Crasher Wake": ["crasher wake", "pastoria gym", "pastoria city"],
        "Fantina": ["fantina", "hearthome gym", "hearthome city"],
        "Byron": ["byron", "canalave gym", "canalave city"],
        "Candice": ["candice", "snowpoint gym", "snowpoint city"],
        "Volkner": ["volkner", "sunyshore gym", "sunyshore city"],
        "Aaron": ["aaron", "elite four aaron"],
        "Bertha": ["bertha", "elite four bertha"],
        "Flint": ["flint", "elite four flint"],
        "Lucian": ["lucian", "elite four lucian"],
        "Cynthia": ["champion cynthia", "cynthia"],
    },
    "pearl": {
        "Roark": ["roark", "oreburgh gym", "oreburgh city"],
        "Gardenia": ["gardenia", "eterna gym", "eterna city"],
        "Maylene": ["maylene", "veilstone gym", "veilstone city"],
        "Crasher Wake": ["crasher wake", "pastoria gym", "pastoria city"],
        "Fantina": ["fantina", "hearthome gym", "hearthome city"],
        "Byron": ["byron", "canalave gym", "canalave city"],
        "Candice": ["candice", "snowpoint gym", "snowpoint city"],
        "Volkner": ["volkner", "sunyshore gym", "sunyshore city"],
        "Aaron": ["aaron", "elite four aaron"],
        "Bertha": ["bertha", "elite four bertha"],
        "Flint": ["flint", "elite four flint"],
        "Lucian": ["lucian", "elite four lucian"],
        "Cynthia": ["champion cynthia", "cynthia"],
    },
    "black": {
        "Cilan": ["cilan", "striaton gym", "striaton city"],
        "Lenora": ["lenora", "nacrene gym", "nacrene city"],
        "Burgh": ["burgh", "castelia gym", "castelia city"],
        "Elesa": ["elesa", "nimbasa gym", "nimbasa city"],
        "Clay": ["clay", "driftveil gym", "driftveil city"],
        "Skyla": ["skyla", "mistralton gym", "mistralton city"],
        "Brycen": ["brycen", "icirrus gym", "icirrus city"],
        "Drayden": ["drayden", "opelucid gym", "opelucid city"],
        "Shauntal": ["shauntal"],
        "Grimsley": ["grimsley"],
        "Caitlin": ["caitlin"],
        "Marshal": ["marshal"],
        "N": ["n's castle", "the champion's temple"],
        "Ghetsis": ["ghetsis"],
        "Alder": ["the champion, alder", "alder"],
    },
    "white": {
        "Cilan": ["cilan", "striaton gym", "striaton city"],
        "Lenora": ["lenora", "nacrene gym", "nacrene city"],
        "Burgh": ["burgh", "castelia gym", "castelia city"],
        "Elesa": ["elesa", "nimbasa gym", "nimbasa city"],
        "Clay": ["clay", "driftveil gym", "driftveil city"],
        "Skyla": ["skyla", "mistralton gym", "mistralton city"],
        "Brycen": ["brycen", "icirrus gym", "icirrus city"],
        "Drayden": ["drayden", "opelucid gym", "opelucid city"],
        "Shauntal": ["shauntal"],
        "Grimsley": ["grimsley"],
        "Caitlin": ["caitlin"],
        "Marshal": ["marshal"],
        "N": ["n's castle", "the champion's temple"],
        "Ghetsis": ["ghetsis"],
        "Alder": ["the champion, alder", "alder"],
    },
    "x": {
        "Viola": ["viola", "santalune gym", "santalune city"],
        "Grant": ["grant", "cyllage gym", "cyllage city"],
        "Korrina": ["korrina", "shalour gym", "shalour city"],
        "Ramos": ["ramos", "coumarine gym", "coumarine city"],
        "Clemont": ["clemont", "lumiose gym", "lumiose city"],
        "Valerie": ["valerie", "laverre gym", "laverre city"],
        "Olympia": ["olympia", "anistar gym", "anistar city"],
        "Wulfric": ["wulfric", "snowbelle gym", "snowbelle city"],
        "Malva": ["malva", "elite four malva"],
        "Siebold": ["siebold", "elite four siebold"],
        "Wikstrom": ["wikstrom", "elite four wikstrom"],
        "Drasna": ["drasna", "elite four drasna"],
        "Diantha": ["champion diantha", "diantha"],
    },
    "y": {
        "Viola": ["viola", "santalune gym", "santalune city"],
        "Grant": ["grant", "cyllage gym", "cyllage city"],
        "Korrina": ["korrina", "shalour gym", "shalour city"],
        "Ramos": ["ramos", "coumarine gym", "coumarine city"],
        "Clemont": ["clemont", "lumiose gym", "lumiose city"],
        "Valerie": ["valerie", "laverre gym", "laverre city"],
        "Olympia": ["olympia", "anistar gym", "anistar city"],
        "Wulfric": ["wulfric", "snowbelle gym", "snowbelle city"],
        "Malva": ["malva", "elite four malva"],
        "Siebold": ["siebold", "elite four siebold"],
        "Wikstrom": ["wikstrom", "elite four wikstrom"],
        "Drasna": ["drasna", "elite four drasna"],
        "Diantha": ["champion diantha", "diantha"],
    },
}

ELITE_FOUR_BY_GAME: dict[str, list[str]] = {
    "red": ["Lorelei", "Bruno", "Agatha", "Lance"],
    "blue": ["Lorelei", "Bruno", "Agatha", "Lance"],
    "gold": ["Will", "Koga", "Bruno", "Karen"],
    "silver": ["Will", "Koga", "Bruno", "Karen"],
    "ruby": ["Sidney", "Phoebe", "Glacia", "Drake"],
    "sapphire": ["Sidney", "Phoebe", "Glacia", "Drake"],
    "diamond": ["Aaron", "Bertha", "Flint", "Lucian"],
    "pearl": ["Aaron", "Bertha", "Flint", "Lucian"],
    "x": ["Malva", "Siebold", "Wikstrom", "Drasna"],
    "y": ["Malva", "Siebold", "Wikstrom", "Drasna"],
}

CHAMPION_BY_GAME: dict[str, str] = {
    "red": "Blue",
    "blue": "Blue",
    "gold": "Lance",
    "silver": "Lance",
    "ruby": "Steven",
    "sapphire": "Steven",
    "diamond": "Cynthia",
    "pearl": "Cynthia",
    "black": "Alder",
    "white": "Alder",
    "x": "Diantha",
    "y": "Diantha",
}


def boss_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def boss_id(version: str, canonical_boss: str) -> str:
    return f"{version}:{boss_slug(canonical_boss)}"


def dataset_game_name(version: str) -> str:
    return version.upper() if len(version) == 1 else version.capitalize()


def _normalize_alias(alias: str) -> str:
    cleaned = " ".join(alias.replace("&", " & ").split())
    words = [w if w.isupper() else w.capitalize() for w in cleaned.split(" ")]
    return " ".join(words)


def _looks_like_person_alias(alias: str) -> bool:
    alias_l = alias.lower()
    blocked_tokens = (
        "gym",
        "city",
        "town",
        "route",
        "island",
        "plateau",
        "hall",
        "castle",
        "temple",
    )
    return not any(token in alias_l for token in blocked_tokens)


def dataset_boss_candidates(version: str, canonical_boss: str) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    def add_candidate(value: str) -> None:
        if value not in seen:
            seen.add(value)
            candidates.append(value)

    add_candidate(canonical_boss)

    for alias in BOSS_ALIASES.get(version, {}).get(canonical_boss, []):
        if not _looks_like_person_alias(alias):
            continue

        add_candidate(alias)
        add_candidate(_normalize_alias(alias))

    return candidates
