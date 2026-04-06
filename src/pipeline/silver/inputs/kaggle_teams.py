"""Extract gym leader and elite four teams from Kaggle dataset for simulation."""
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Collection

from src.pipeline.common.io import write_parquet
from src.pipeline.silver.inputs.game_config import (
    STARTER_EVOLUTION_CHAINS_BY_BASE,
    get_starter_choices,
    resolve_starter_species_for_level,
)

_STARTER_FAMILY_LOOKUP: dict[str, str] = {
    species: base
    for base, chain in STARTER_EVOLUTION_CHAINS_BY_BASE.items()
    for _, species in chain
}


def _family_root_for_species(species: str) -> str:
    normalized = species.lower().strip()
    return _STARTER_FAMILY_LOOKUP.get(normalized, normalized)


def _dedupe_details_by_family(details: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    seen_roots: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in details:
        name = item.get("name")
        if not isinstance(name, str):
            continue
        family_root = _family_root_for_species(name)
        if family_root in seen_roots:
            continue
        seen_roots.add(family_root)
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _build_starter_variant(base_team: dict[str, Any], starter_base: str) -> dict[str, Any]:
    version = str(base_team.get("game_version", "unknown"))
    avg_level = int(base_team.get("avg_level") or 20)
    evolved_starter = resolve_starter_species_for_level(starter_base, avg_level)

    starter_member = {
        "name": evolved_starter,
        "level": avg_level,
        "moves": [],
        "origin": "starter",
    }

    base_details = base_team.get("details", [])
    team_details = [starter_member]
    if isinstance(base_details, list):
        for item in base_details:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str):
                continue
            team_details.append(
                {
                    "name": name,
                    "level": int(item.get("level") or avg_level),
                    "moves": item.get("moves", []),
                    "origin": "kaggle",
                }
            )

    team_details = _dedupe_details_by_family(team_details, limit=6)
    levels = [int(member.get("level") or avg_level) for member in team_details]
    team_avg_level = int(sum(levels) / len(levels)) if levels else avg_level

    return {
        "team_id": f"STARTER_{version}_{starter_base}_{base_team.get('team_id')}",
        "boss_name": None,
        "gym": base_team.get("gym"),
        "game_version": version,
        "pokemon": [member["name"] for member in team_details],
        "levels": levels,
        "avg_level": team_avg_level,
        "details": team_details,
        "starter_base": starter_base,
        "starter_evolved_species": evolved_starter,
        "source_team_id": base_team.get("team_id"),
        "is_player_candidate": True,
    }


def _append_starter_variants(teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for team in teams:
        game_version = team.get("game_version")
        if not isinstance(game_version, str):
            continue
        starters = get_starter_choices(game_version)
        if not starters:
            continue
        for starter in starters:
            variants.append(_build_starter_variant(team, starter))
    return teams + variants


def extract_kaggle_teams(
    bronze_dir: Path,
    simulation_dir: Path,
    allowed_versions: Collection[str] | None = None,
) -> list[dict]:
    """
    Extract gym leader and elite four teams from Kaggle CSV.

    Returns list of team dicts with structure:
    {
        'team_id': 'KAGGLE_game_leader_idx',
        'boss_name': 'Leader Name',
        'gym': 'Gym Location',
        'game_version': 'game',
        'pokemon': ['pikachu', 'raichu', ...],
        'levels': [25, 30, ...],
        'avg_level': 28,
        'details': [{'name': 'pikachu', 'level': 25, 'moves': [...]}, ...]
    }
    """
    kaggle_file = bronze_dir / 'kagglehub' / 'gym_leaders_elite_four.csv'

    if not kaggle_file.exists():
        print("[kaggle_teams] gym_leaders_elite_four.csv not found, skipping")
        return []

    allowed_versions_set = {version.lower() for version in allowed_versions} if allowed_versions else None
    skipped_versions: dict[str, int] = defaultdict(int)

    # Parse Kaggle CSV
    teams_by_leader: dict[str, dict[str, Any]] = defaultdict(lambda: {'pokemon': [], 'game': None, 'gym': None})

    try:
        with open(kaggle_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                game = row['Game'].strip().lower()
                if allowed_versions_set is not None and game not in allowed_versions_set:
                    skipped_versions[game] += 1
                    continue
                gym_leader = row['Gym leader'].lower()
                pokemon = row['Pokemon'].lower()
                level = int(row['Level']) if row['Level'] else 20
                moves = [
                    m.strip().lower() for m in [
                        row.get('Move 1', ''),
                        row.get('Move 2', ''),
                        row.get('Move 3', ''),
                        row.get('Move 4', '')
                    ] if m and m.strip()
                ]

                key = f"{game}:{gym_leader}"
                teams_by_leader[key]['game'] = game  # type: ignore
                teams_by_leader[key]['gym_leader'] = gym_leader  # type: ignore
                teams_by_leader[key]['gym'] = row['Gym']  # type: ignore
                pokemon_list = teams_by_leader[key]['pokemon']
                if isinstance(pokemon_list, list):
                    pokemon_list.append({
                        'name': pokemon,
                        'level': level,
                        'moves': moves[:4]
                    })
    except Exception as e:
        print(f"[kaggle_teams] Error reading Kaggle CSV: {e}")
        return []

    # Convert to team format
    teams = []
    for idx, (key, data) in enumerate(sorted(teams_by_leader.items())):
        game = data.get('game', 'unknown')
        gym_leader = data.get('gym_leader', 'unknown')
        gym = data.get('gym', 'unknown')
        pokemon_list = data.get('pokemon', [])

        team_id = f"KAGGLE_{game}_{gym_leader}_{idx}"
        team_entry = {
            'team_id': team_id,
            'boss_name': gym_leader.title() if isinstance(gym_leader, str) else 'Unknown',
            'gym': gym,
            'game_version': game,
            'pokemon': [p['name'] for p in pokemon_list] if isinstance(pokemon_list, list) else [],
            'levels': [p['level'] for p in pokemon_list] if isinstance(pokemon_list, list) else [],
            'avg_level': sum(p['level'] for p in pokemon_list) // len(pokemon_list) if isinstance(pokemon_list, list) and pokemon_list else 20,
            'details': pokemon_list if isinstance(pokemon_list, list) else []
        }
        teams.append(team_entry)

    teams = _append_starter_variants(teams)

    if skipped_versions:
        skipped_preview = ", ".join(
            f"{version}={count}" for version, count in sorted(skipped_versions.items())
        )
        print(f"[kaggle_teams] skipped non-config versions: {skipped_preview}")

    # Write to simulation folder
    simulation_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(simulation_dir / 'teams.parquet', teams)

    print(f"[kaggle_teams] extracted {len(teams)} teams (including starter variants)")
    return teams





