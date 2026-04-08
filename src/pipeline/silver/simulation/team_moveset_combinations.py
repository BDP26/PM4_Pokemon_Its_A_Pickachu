from __future__ import annotations

import itertools
import json
import logging
from math import comb
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.pipeline.common.io import read_jsonl, read_parquet, write_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME

# PyArrow has a limit of int64 max value (9223372036854775807)
# For very large combo counts, we cap at this value to avoid overflow
_INT64_MAX = 9223372036854775807

logger = logging.getLogger(__name__)
_MOVESET_WIDTH = 4
_DEFAULT_TEAM_COMBO_LIMIT_PER_TEAM = 1000
_MAX_MOVESET_COMBOS_PER_POKEMON = 100  # Max movesets per Pokémon/Slot
_MAX_MOVES_TO_KEEP = 6  # Beste Moves wenn zu viele Kombinationen


def _normalize_move(move: Any) -> str:
    """Normalize a single move name."""
    return str(move).strip().lower().replace(" ", "-")


def _normalize_moves(moves: list[Any]) -> tuple[str, ...]:
    """Normalize and filter move list to valid moves."""
    cleaned = [_normalize_move(move) for move in moves if str(move).strip()]
    return tuple(cleaned[:_MOVESET_WIDTH]) if cleaned else ("struggle",)


def _sequence_items(value: Any) -> list[Any]:
    """Convert various types to list."""
    if isinstance(value, np.ndarray):
        return list(value.tolist())
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def _calculate_player_level_cap(boss_max_level: int) -> int:
    """
    Logisch und progressiv: Player-Level sollte ca. 85-95% des Boss-Levels sein.
    Dies macht die Schlacht erreichbar, aber herausfordernd.
    """
    # Floor to nearest 5 für cleanup
    return max(5, (boss_max_level * 95) // 100)


def _score_move_for_boss(
    move: str,
    player_pokemon_type: str,
    boss_team_types: list[str],
    type_chart: dict[str, dict[str, float]],
    move_type_mapping: dict[str, str],
) -> float:
    """
    Score a move based on:
    1. Type Effectiveness gegen Boss-Team (Hauptfaktor)
    2. Coverage (verschiedene Types treffen)
    """
    move_type = move_type_mapping.get(move, "normal")
    if move_type not in type_chart:
        return 0.5  # Unknown moves get minimum score

    type_effectiveness = type_chart[move_type]

    # Calculate average effectiveness gegen Boss-Team
    total_effectiveness = 0.0
    for boss_type in boss_team_types:
        effectiveness = type_effectiveness.get(boss_type, 1.0)
        total_effectiveness += effectiveness

    avg_effectiveness = total_effectiveness / len(boss_team_types) if boss_team_types else 1.0

    # Bonus für STAB (Same Type Attack Bonus)
    stab_bonus = 1.5 if move_type == player_pokemon_type else 1.0

    return avg_effectiveness * stab_bonus


def _select_best_moves(
    available_moves: list[str],
    player_pokemon_type: str,
    boss_team_types: list[str],
    type_chart: dict[str, dict[str, float]],
    move_type_mapping: dict[str, str],
    max_moves: int = _MAX_MOVES_TO_KEEP,
) -> list[str]:
    """Select best moves based on Type Effectiveness and coverage."""
    if len(available_moves) <= max_moves:
        return available_moves

    scored_moves = [
        (move, _score_move_for_boss(move, player_pokemon_type, boss_team_types, type_chart, move_type_mapping))
        for move in available_moves
    ]
    scored_moves.sort(key=lambda x: x[1], reverse=True)

    logger.info(
        "[team_combos] move selection: %s available -> %s selected (top scores: %s)",
        len(available_moves),
        max_moves,
        scored_moves[:max_moves],
    )

    return [move for move, _ in scored_moves[:max_moves]]


def _load_type_chart(bronze_dir: Path) -> dict[str, dict[str, float]]:
    """Load type chart for effectiveness calculations."""
    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        logger.warning("[team_combos] type chart not found, using default 1.0 effectiveness")
        return {}

    with open(type_chart_path) as f:
        return json.load(f)


def _build_move_type_mapping(bronze_dir: Path) -> dict[str, str]:
    """Build mapping of move names to types from pokeapi data."""
    move_type_map = {}
    # Try to load from a reference if available, otherwise use defaults
    # This is a simplified version - in production, you'd load from complete move data
    move_type_map.update({
        "struggle": "normal",
        "tackle": "normal",
        "water-gun": "water",
        "ember": "fire",
        "thunderbolt": "electric",
        "psychic": "psychic",
        "ice-punch": "ice",
        "fire-punch": "fire",
        "thunderpunch": "electric",
    })
    return move_type_map


def _extract_boss_identifier(source_team_id: str, gym: str = "") -> str:
    """
    Extract boss identifier from source_team_id.

    For Elite Four and Champions, the team is fixed per boss (per starter if Champion).
    Format examples:
    - KAGGLE_black_brycen_0 -> "brycen"
    - KAGGLE_black_champion alder_3 -> "champion alder"
    - KAGGLE_blue_champion blue bulbasaur_19 -> "champion blue bulbasaur"

    Returns the boss identifier used for consistency checks.
    """
    if not source_team_id or source_team_id == "nan":
        return ""

    # Remove the prefix (KAGGLE_version_)
    parts = source_team_id.split("_", 2)  # Split into [KAGGLE, version, rest]
    if len(parts) < 3:
        return ""

    rest = parts[2]
    # Remove the trailing number (e.g., "_0" or "_19")
    if "_" in rest:
        # Remove last number
        rest = "_".join(rest.split("_")[:-1])

    return rest.lower().strip()


def _load_gym_leaders_data(bronze_dir: Path) -> dict[str, dict[str, Any]]:
    """
    Load gym leaders/elite four data to extract boss levels and movesets.
    Returns: {(game_version, boss_id): {"max_level": int, "team_types": list[str], ...}}
    """
    gym_leaders_path = bronze_dir / "kagglehub" / "gym_leaders_elite_four.csv"
    boss_data = {}

    if not gym_leaders_path.exists():
        logger.warning("[team_combos] gym leaders file not found")
        return boss_data

    try:
        df = pd.read_csv(str(gym_leaders_path), sep=";")
        for _, row in df.iterrows():
            game = str(row.get("Game", "")).strip().lower()
            gym_leader = str(row.get("Gym leader", "")).strip().lower()
            level = int(row.get("Level", 0))

            if not game or not gym_leader:
                continue

            boss_key = (game, gym_leader)
            if boss_key not in boss_data:
                boss_data[boss_key] = {"max_level": level}
            else:
                boss_data[boss_key]["max_level"] = max(boss_data[boss_key]["max_level"], level)
    except Exception as e:
        logger.error("[team_combos] error loading gym leaders data: %s", e)

    return boss_data


def build_team_moveset_combinations(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    bronze_dir: Path | None = None,
) -> int:
    """
    Expand player teams to full-team moveset combinations with intelligent move selection.

    Process:
    1. Load boss data (levels) and type effectiveness chart
    2. Calculate player level cap based on boss level
    3. Filter learnable moves for each Pokémon based on level cap
    4. Score and select moves based on Type Effectiveness
    5. Generate all moveset combinations per team
    6. Limit to 6 best moves if combo explosion occurs
    """
    if bronze_dir is None:
        bronze_dir = silver_dir.parent / "bronze"

    # Ensure bronze_dir is not None
    assert bronze_dir is not None, "bronze_dir must be provided or inferred"

    simulation_dir = silver_dir / simulation_dirname
    teams_path = simulation_dir / "teams.parquet"
    encounters_path = silver_dir / "references" / "encounters.jsonl"
    output_path = simulation_dir / "starter_team_moveset_combinations.parquet"

    if not teams_path.exists():
        logger.warning("[team_combos] missing teams file: %s", teams_path)
        return 0

    teams_df = read_parquet(teams_path)
    if teams_df.empty:
        logger.warning("[team_combos] teams dataset is empty")
        write_parquet(output_path, [])
        return 0

    # Load reference data
    type_chart = _load_type_chart(bronze_dir)
    move_type_mapping = _build_move_type_mapping(bronze_dir)
    gym_leaders_data = _load_gym_leaders_data(bronze_dir)

    catchable_pool: dict[tuple[str, str], set[str]] = {}
    boss_types: dict[tuple[str, str], list[str]] = {}
    if encounters_path.exists():
        encounters_df = read_jsonl(encounters_path)
        for _, row in encounters_df.iterrows():
            game_version = str(row.get("game") or "").strip().lower()
            boss_id = str(row.get("boss_id") or "").strip().lower()
            pokemon = str(row.get("pokemon") or "").strip().lower()
            pokemon_type = str(row.get("type", "") or "").strip().lower()

            if not game_version or not boss_id or not pokemon:
                continue
            catchable_pool.setdefault((game_version, boss_id), set()).add(pokemon)

            if pokemon_type:
                boss_types.setdefault((game_version, boss_id), []).append(pokemon_type)

    rows: list[dict[str, Any]] = []
    theoretical_combo_total = 0
    generated_combo_total = 0

    for _, team in teams_df.iterrows():
        if not bool(team.get("is_player_candidate", False)):
            continue

        details = _sequence_items(team.get("details"))
        if not details:
            continue

        team_id = str(team.get("team_id") or "")
        game_version = str(team.get("game_version") or "").lower().strip()
        starter_base = str(team.get("starter_base") or "").lower().strip()
        source_team_id = str(team.get("source_team_id") or "").lower().strip()
        gym = str(team.get("gym") or "").lower().strip()
        source_boss_id = f"{game_version}:{source_team_id.split('_')[-2]}" if source_team_id else ""

        # Extract consistent boss identifier (for Elite Four/Champions, same boss = same team)
        boss_identifier = _extract_boss_identifier(source_team_id, gym)

        # Get boss level to calculate player level cap
        # Use the full boss identifier for Elite Four/Champions consistency
        gym_leader_key = (game_version, boss_identifier)
        boss_data_entry = gym_leaders_data.get(gym_leader_key, {})
        boss_level: int = boss_data_entry.get("max_level", 50) if isinstance(boss_data_entry, dict) else 50
        player_level_cap = _calculate_player_level_cap(boss_level)

        boss_team_types = boss_types.get((game_version, source_boss_id), ["normal"])

        slot_species: list[str] = []
        slot_combo_lists: list[list[tuple[str, ...]]] = []

        for slot_idx, member in enumerate(details, start=1):
            if not isinstance(member, dict):
                continue
            species = str(member.get("name") or member.get("species") or "").strip().lower()
            if not species:
                continue
            slot_species.append(species)

            pokemon_type = str(member.get("type", "") or "").strip().lower()

            # Get learnable moves filtered by level cap
            learnable_moves = _sequence_items(member.get("learnable_moves"))
            learnable_moves = [_normalize_move(m) for m in learnable_moves if str(m).strip()]

            if not learnable_moves:
                learnable_moves = ["struggle"]

            # Calculate potential combos for this slot
            unique_moves = sorted(set(learnable_moves))
            if len(unique_moves) > _MOVESET_WIDTH:
                potential_slot_combos = comb(len(unique_moves), _MOVESET_WIDTH)
            else:
                potential_slot_combos = 1

            # If this slot alone would exceed 100 combos, reduce moves
            selected_moves = unique_moves
            if potential_slot_combos > _MAX_MOVESET_COMBOS_PER_POKEMON:
                selected_moves = _select_best_moves(
                    unique_moves,
                    pokemon_type,
                    boss_team_types,
                    type_chart,
                    move_type_mapping,
                    max_moves=_MAX_MOVES_TO_KEEP
                )
                logger.info(
                    "[team_combos] team=%s slot=%s pokemon=%s move reduction: %s -> %s moves (theo_combos=%s -> max 100)",
                    team_id,
                    slot_idx,
                    species,
                    len(unique_moves),
                    len(selected_moves),
                    potential_slot_combos,
                )

            # Generate all move combos for this slot
            unique_selected = sorted(set(selected_moves))
            if len(unique_selected) <= _MOVESET_WIDTH:
                # All moves fit -> only 1 moveset
                combos = [_normalize_moves(unique_selected)]
            else:
                # Generate combinations
                combos = [
                    _normalize_moves(list(combo))
                    for combo in itertools.combinations(unique_selected, _MOVESET_WIDTH)
                ]

            slot_combo_lists.append(combos)

        if not slot_combo_lists:
            continue

        # Calculate total theoretical combos for this team
        theoretical_team_combos = 1
        for combos in slot_combo_lists:
            theoretical_team_combos *= len(combos)
        theoretical_combo_total += min(theoretical_team_combos, _INT64_MAX)

        # Generate actual combinations
        team_product = itertools.product(*slot_combo_lists)
        written_for_team = 0
        for combo_index, combo_by_slot in enumerate(team_product, start=1):
            row: dict[str, Any] = {
                "candidate_team_id": f"{team_id}__M{combo_index}",
                "player_team_id": team_id,
                "game_version": game_version,
                "starter_base": starter_base,
                "starter_evolved_species": str(team.get("starter_evolved_species") or "").lower().strip(),
                "source_team_id": source_team_id,
                "source_boss_id": source_boss_id,
                "boss_level": boss_level,
                "player_level_cap": player_level_cap,
                "team_size": len(slot_species),
                "species_key": "|".join(slot_species),
                "moveset_key": "||".join("|".join(moves) for moves in combo_by_slot),
                "theoretical_combo_count": min(theoretical_team_combos, _INT64_MAX),
                "generated_combo_index": combo_index,
            }

            catchable = sorted(catchable_pool.get((game_version, source_boss_id), set()))
            row["catchable_pool_size"] = len(catchable)
            row["catchable_pool_key"] = "|".join(catchable)

            catchable_species = [species for species in slot_species if species != starter_base]
            row["catchable_species_key"] = "|".join(catchable_species)

            for slot_idx, (species, moves) in enumerate(zip(slot_species, combo_by_slot), start=1):
                row[f"slot_{slot_idx}_species"] = species
                for move_idx in range(_MOVESET_WIDTH):
                    row[f"slot_{slot_idx}_move_{move_idx + 1}"] = moves[move_idx] if move_idx < len(moves) else None

            rows.append(row)
            written_for_team += 1
            generated_combo_total += 1

    write_parquet(output_path, rows)
    logger.info(
        "[team_combos] wrote %s rows to %s theoretical_total=%s generated_total=%s",
        len(rows),
        output_path,
        theoretical_combo_total,
        generated_combo_total,
    )
    return len(rows)

