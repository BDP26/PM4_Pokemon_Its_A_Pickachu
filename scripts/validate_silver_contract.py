from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq


GAME_SHARDS = [
    "black-white",
    "black",
    "blue",
    "diamond",
    "gold",
    "pearl",
    "red",
    "ruby",
    "sapphire",
    "silver",
    "white",
    "x",
    "y",
]

SIMULATION_PREFIXES = [
    "source_teams",
    "source_team_members",
    "member_move_options",
    "member_moveset_combos",
    "pokemon_combat_pool",
    "pokemon_moveset_options",
    "simulation_sampling_plan",
]

REQUIRED_NON_NULL: dict[str, list[str]] = {
    "references/games.parquet": [
        "game_version",
        "version_group",
        "generation",
        "is_supported",
        "region",
    ],
    "references/bosses.parquet": [
        "boss_id",
        "boss_name_canonical",
        "boss_order",
        "boss_index",
        "game_version",
        "boss_role",
    ],
    "references/boss_team_members.parquet": [
        "boss_id",
        "boss_name",
        "slot",
        "pokemon_species",
        "level",
        "move_name",
        "move_slot",
        "source",
        "game_version",
        "boss_role",
    ],
    "references/locations.parquet": [
        "location_id",
        "walkthrough_location_name",
        "normalized_location_name",
        "game_version",
        "mapping_status",
    ],
    "references/encounters.parquet": [
        "boss_id",
        "location",
        "pokemon",
        "level_min",
        "level_max",
        "encounter_chance_min",
        "encounter_chance_max",
        "game",
    ],
    "references/progression_depth.parquet": [
        "boss_id",
        "boss_name",
        "boss_index",
        "max_boss_index",
        "available_species_count",
        "max_species_count",
        "progression_depth",
        "starter_condition",
        "game_version",
    ],
    "references/pokemon_reference.parquet": [
        "pokemon_species",
        "name",
        "url",
    ],
    "references/pokemon_data.parquet": [
        "name",
        "pokemon_species",
        "pokeapi_id",
        "source_url",
        "type_1",
        "base_hp",
        "base_attack",
        "base_defense",
        "base_special_attack",
        "base_special_defense",
        "base_speed",
    ],
    "references/move_reference.parquet": [
        "move_name",
        "damage_class",
        "type",
        "effective_power",
        "power_handling",
        "is_status_move",
        "is_damage_move",
        "is_null_power",
    ],
    "references/learnable_moves.parquet": [
        "move_name",
        "learned_level",
        "learn_method",
        "game_version",
        "pokemon_species",
    ],
    "simulation/source_teams": [
        "source_team_id",
        "game_version",
        "team_role",
        "origin",
        "boss_id",
        "boss_name",
        "avg_level",
        "member_count",
        "is_player_candidate",
        "boss_index",
        "max_boss_index",
        "available_species_count",
        "max_species_count",
        "progression_depth",
    ],
    "simulation/source_team_members": [
        "team_member_id",
        "source_team_id",
        "game_version",
        "team_role",
        "origin",
        "boss_id",
        "boss_name",
        "slot",
        "pokemon_species",
        "level",
        "is_starter",
    ],
    "simulation/member_move_options": [
        "team_member_id",
        "source_team_id",
        "game_version",
        "slot",
        "pokemon_species",
        "level",
        "move_name",
        "option_rank",
        "option_score",
        "moveset_context_id",
    ],
    "simulation/member_moveset_combos": [
        "moveset_combo_id",
        "team_id",
        "pokemon_instance_id",
        "slot_index",
        "game_version",
        "pokemon_name",
        "level",
        "moves",
        "move_count",
        "combo_rank",
        "combo_score",
        "source",
    ],
    "simulation/pokemon_combat_pool": [
        "game_version",
        "pokemon_species",
        "level",
    ],
    "simulation/pokemon_moveset_options": [
        "moveset_context_id",
        "game_version",
        "pokemon_species",
        "level",
        "move_policy",
    ],
    "simulation/simulation_sampling_plan": [
        "source_team_id",
        "sampling_seed",
        "move_policy",
        "max_moves_per_member",
        "estimated_combo_space",
    ],
    "simulation/move_data.parquet": [
        "pokemon_instance_id",
        "team_id",
        "species",
        "level",
        "game_version",
        "provided_moves",
        "learnable_moves",
        "move_details",
        "slot_index",
    ],
}


@dataclass
class Finding:
    level: str
    code: str
    message: str


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _warn(message: str) -> None:
    print(f"[WARN] {message}")


def _error(message: str) -> None:
    print(f"[ERROR] {message}")


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _count_rows(path: Path) -> int:
    if path.is_dir():
        return ds.dataset(path, format="parquet", partitioning="hive").count_rows()
    return pq.read_table(path).num_rows


def _schema_summary(path: Path) -> list[dict[str, Any]]:
    if path.is_dir():
        schema = ds.dataset(path, format="parquet", partitioning="hive").schema
    else:
        schema = pq.read_table(path).schema
    return [
        {"name": field.name, "type": str(field.type), "nullable": bool(field.nullable)}
        for field in schema
    ]


def _partition_columns(path: Path) -> list[str]:
    if not path.is_dir():
        return []
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    return sorted(
        {
            part.split("=", 1)[0]
            for fragment in dataset.get_fragments()
            for part in Path(fragment.path).parts
            if "=" in part
        }
    )


def _load_simulation_shards(simulation_dir: Path, prefix: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for game_version in GAME_SHARDS:
        path = simulation_dir / f"{prefix}_{game_version}.parquet"
        if path.exists():
            frames.append(_read_parquet(path))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _record(findings: list[Finding], level: str, code: str, message: str) -> None:
    findings.append(Finding(level=level, code=code, message=message))
    if level == "error":
        _error(f"{code}: {message}")
    else:
        _warn(f"{code}: {message}")


def _check_required_non_null(
    findings: list[Finding],
    dataset_name: str,
    frame: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        _record(findings, "error", "MISSING_COLUMN", f"{dataset_name} missing columns {missing_columns}")
        return

    for column in required_columns:
        null_count = int(frame[column].isna().sum())
        if null_count:
            _record(
                findings,
                "error",
                "NULL_REQUIRED",
                f"{dataset_name}.{column} has {null_count} null rows",
            )


def _check_unique(
    findings: list[Finding],
    dataset_name: str,
    frame: pd.DataFrame,
    key_columns: list[str],
    code: str,
) -> None:
    missing = [column for column in key_columns if column not in frame.columns]
    if missing:
        _record(findings, "error", "MISSING_COLUMN", f"{dataset_name} missing key columns {missing}")
        return
    duplicate_count = int(frame.duplicated(key_columns).sum())
    if duplicate_count:
        _record(
            findings,
            "error",
            code,
            f"{dataset_name} duplicates for key {key_columns}: {duplicate_count}",
        )
    else:
        _ok(f"{dataset_name} unique on {key_columns}")


def _left_orphan_count(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: list[str],
    right_on: list[str] | None = None,
) -> int:
    if right_on is None:
        right_on = left_on
    right_keys = right[right_on].drop_duplicates()
    merged = left.merge(right_keys, left_on=left_on, right_on=right_on, how="left", indicator=True)
    return int((merged["_merge"] == "left_only").sum())


def _artifact_inventory(silver_dir: Path) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    top_level_jsons = [
        silver_dir / "_state" / "location_enrichment_diagnostics.json",
        silver_dir / "_state" / "silver_state.json",
        silver_dir / "diagnostics" / "performance_summary.json",
        silver_dir / "diagnostics" / "relational_validation.json",
        silver_dir / "diagnostics" / "unmapped_locations.json",
        silver_dir / "diagnostics" / "unmapped_locations_detailed.json",
        silver_dir / "diagnostics" / "unmapped_locations_summary.json",
        silver_dir / "manifest.json",
        silver_dir / "mappings" / "boss_mapping_by_version.json",
        silver_dir / "mappings" / "location_to_area_map.json",
        silver_dir / "mappings" / "location_to_pokemon_map.json",
        silver_dir / "references" / "encounter_methods_reference.json",
    ]
    for path in top_level_jsons:
        if not path.exists():
            continue
        payload = _read_json(path)
        key_summary: list[str] = []
        if isinstance(payload, dict):
            key_summary = list(payload.keys())[:20]
            rows = len(payload)
        elif isinstance(payload, list):
            rows = len(payload)
            if payload and isinstance(payload[0], dict):
                key_summary = list(payload[0].keys())
        else:
            rows = None
        inventory.append(
            {
                "path": str(path.relative_to(silver_dir)),
                "kind": "json",
                "rows": rows,
                "keys": key_summary,
            }
        )

    for path in sorted((silver_dir / "snapshots").glob("*_boss_snapshots.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            first_line = handle.readline()
            rows = 1 + sum(1 for _ in handle) if first_line else 0
            keys = list(json.loads(first_line).keys()) if first_line else []
        inventory.append(
            {
                "path": str(path.relative_to(silver_dir)),
                "kind": "jsonl",
                "rows": rows,
                "keys": keys,
            }
        )

    encounters_jsonl = silver_dir / "references" / "encounters.jsonl"
    if encounters_jsonl.exists():
        with encounters_jsonl.open("r", encoding="utf-8") as handle:
            first_line = handle.readline()
            rows = 1 + sum(1 for _ in handle) if first_line else 0
            keys = list(json.loads(first_line).keys()) if first_line else []
        inventory.append(
            {
                "path": str(encounters_jsonl.relative_to(silver_dir)),
                "kind": "jsonl",
                "rows": rows,
                "keys": keys,
            }
        )

    parquet_paths = [
        silver_dir / "references" / "boss_team_members.parquet",
        silver_dir / "references" / "bosses.parquet",
        silver_dir / "references" / "encounters.parquet",
        silver_dir / "references" / "games.parquet",
        silver_dir / "references" / "learnable_moves.parquet",
        silver_dir / "references" / "locations.parquet",
        silver_dir / "references" / "move_reference.parquet",
        silver_dir / "references" / "pokemon_data.parquet",
        silver_dir / "references" / "pokemon_reference.parquet",
        silver_dir / "references" / "progression_depth.parquet",
        silver_dir / "simulation" / "move_data.parquet",
    ]
    for prefix in SIMULATION_PREFIXES:
        for game_version in GAME_SHARDS:
            parquet_paths.append(silver_dir / "simulation" / f"{prefix}_{game_version}.parquet")

    for path in parquet_paths:
        if not path.exists():
            continue
        inventory.append(
            {
                "path": str(path.relative_to(silver_dir)),
                "kind": "parquet_dataset" if path.is_dir() else "parquet_file",
                "rows": _count_rows(path),
                "partition_columns": _partition_columns(path),
                "schema": _schema_summary(path),
            }
        )

    return inventory


def validate(silver_dir: Path) -> list[Finding]:
    findings: list[Finding] = []
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"

    games = _read_parquet(references_dir / "games.parquet")
    bosses = _read_parquet(references_dir / "bosses.parquet")
    boss_team_members = _read_parquet(references_dir / "boss_team_members.parquet")
    locations = _read_parquet(references_dir / "locations.parquet")
    encounters = _read_parquet(references_dir / "encounters.parquet")
    progression_depth = _read_parquet(references_dir / "progression_depth.parquet")
    pokemon_reference = _read_parquet(references_dir / "pokemon_reference.parquet")
    pokemon_data = _read_parquet(references_dir / "pokemon_data.parquet")
    move_reference = _read_parquet(references_dir / "move_reference.parquet")
    learnable_moves = _read_parquet(references_dir / "learnable_moves.parquet")
    source_teams = _load_simulation_shards(simulation_dir, "source_teams")
    source_team_members = _load_simulation_shards(simulation_dir, "source_team_members")
    member_move_options = _load_simulation_shards(simulation_dir, "member_move_options")
    member_moveset_combos = _load_simulation_shards(simulation_dir, "member_moveset_combos")
    pokemon_combat_pool = _load_simulation_shards(simulation_dir, "pokemon_combat_pool")
    pokemon_moveset_options = _load_simulation_shards(simulation_dir, "pokemon_moveset_options")
    simulation_sampling_plan = _load_simulation_shards(simulation_dir, "simulation_sampling_plan")
    move_data = _read_parquet(simulation_dir / "move_data.parquet")

    for dataset_name, frame in [
        ("references/games.parquet", games),
        ("references/bosses.parquet", bosses),
        ("references/boss_team_members.parquet", boss_team_members),
        ("references/locations.parquet", locations),
        ("references/encounters.parquet", encounters),
        ("references/progression_depth.parquet", progression_depth),
        ("references/pokemon_reference.parquet", pokemon_reference),
        ("references/pokemon_data.parquet", pokemon_data),
        ("references/move_reference.parquet", move_reference),
        ("references/learnable_moves.parquet", learnable_moves),
        ("simulation/source_teams", source_teams),
        ("simulation/source_team_members", source_team_members),
        ("simulation/member_move_options", member_move_options),
        ("simulation/member_moveset_combos", member_moveset_combos),
        ("simulation/pokemon_combat_pool", pokemon_combat_pool),
        ("simulation/pokemon_moveset_options", pokemon_moveset_options),
        ("simulation/simulation_sampling_plan", simulation_sampling_plan),
        ("simulation/move_data.parquet", move_data),
    ]:
        required = REQUIRED_NON_NULL.get(dataset_name)
        if required is not None:
            _check_required_non_null(findings, dataset_name, frame, required)

    _check_unique(findings, "references/games.parquet", games, ["game_version"], "DUP_GAME_VERSION")
    _check_unique(findings, "references/bosses.parquet", bosses, ["boss_id"], "DUP_BOSS_ID")
    _check_unique(
        findings,
        "references/progression_depth.parquet",
        progression_depth,
        ["game_version", "boss_id"],
        "DUP_PROGRESSION_BOSS",
    )
    _check_unique(
        findings,
        "references/boss_team_members.parquet",
        boss_team_members,
        ["game_version", "boss_id", "slot", "move_slot"],
        "DUP_BOSS_MEMBER_MOVE",
    )
    _check_unique(findings, "references/locations.parquet", locations, ["game_version", "location_id"], "DUP_LOCATION")
    _check_unique(
        findings,
        "references/encounters.parquet",
        encounters,
        ["game", "boss_id", "location", "pokemon", "level_min", "level_max", "encounter_chance_min", "encounter_chance_max"],
        "DUP_ENCOUNTER",
    )
    _check_unique(
        findings,
        "references/learnable_moves.parquet",
        learnable_moves,
        ["game_version", "pokemon_species", "move_name"],
        "DUP_LEARNABLE_MOVE",
    )
    _check_unique(findings, "references/pokemon_reference.parquet", pokemon_reference, ["pokemon_species"], "DUP_POKEMON_REF")
    _check_unique(findings, "references/pokemon_data.parquet", pokemon_data, ["pokemon_species"], "DUP_POKEMON_SPECIES")
    _check_unique(findings, "references/move_reference.parquet", move_reference, ["move_name"], "DUP_MOVE_NAME")
    _check_unique(findings, "simulation/source_teams", source_teams, ["source_team_id"], "DUP_SOURCE_TEAM")
    _check_unique(
        findings,
        "simulation/source_team_members",
        source_team_members,
        ["team_member_id"],
        "DUP_TEAM_MEMBER_ID",
    )
    _check_unique(
        findings,
        "simulation/source_team_members",
        source_team_members,
        ["source_team_id", "slot"],
        "DUP_TEAM_MEMBER_SLOT",
    )
    _check_unique(
        findings,
        "simulation/member_move_options",
        member_move_options,
        ["team_member_id", "move_name"],
        "DUP_MEMBER_MOVE_OPTION",
    )
    _check_unique(
        findings,
        "simulation/member_move_options",
        member_move_options,
        ["team_member_id", "option_rank"],
        "DUP_MEMBER_MOVE_OPTION_RANK",
    )
    _check_unique(
        findings,
        "simulation/member_moveset_combos",
        member_moveset_combos,
        ["moveset_combo_id"],
        "DUP_MOVESET_COMBO",
    )
    _check_unique(
        findings,
        "simulation/member_moveset_combos",
        member_moveset_combos,
        ["pokemon_instance_id", "combo_rank"],
        "DUP_MEMBER_COMBO_RANK",
    )
    _check_unique(
        findings,
        "simulation/pokemon_combat_pool",
        pokemon_combat_pool,
        ["game_version", "pokemon_species", "level"],
        "DUP_COMBAT_POOL",
    )
    _check_unique(
        findings,
        "simulation/pokemon_moveset_options",
        pokemon_moveset_options,
        ["moveset_context_id", "move_name"],
        "DUP_CONTEXT_MOVE",
    )
    _check_unique(
        findings,
        "simulation/simulation_sampling_plan",
        simulation_sampling_plan,
        ["source_team_id", "sampling_seed"],
        "DUP_SAMPLING_SEED",
    )
    _check_unique(
        findings,
        "simulation/move_data.parquet",
        move_data,
        ["pokemon_instance_id"],
        "DUP_MOVE_DATA_MEMBER",
    )

    if _left_orphan_count(progression_depth, bosses, ["game_version", "boss_id"]) > 0:
        _record(findings, "error", "FK_PROGRESSION_BOSS", "progression_depth contains unknown boss keys")
    else:
        _ok("progression_depth -> bosses validated")

    locations_orphans = _left_orphan_count(locations, games, ["game_version"])
    if locations_orphans:
        _record(findings, "warning", "FK_LOCATION_GAME", f"locations rows with unknown game_version: {locations_orphans}")
    else:
        _ok("locations -> games validated")

    encounters_orphans = _left_orphan_count(
        encounters,
        progression_depth.rename(columns={"game_version": "game"}),
        ["game", "boss_id"],
    )
    if encounters_orphans:
        _record(
            findings,
            "warning",
            "FK_ENCOUNTER_PROGRESSION",
            f"encounters rows missing progression_depth match: {encounters_orphans}",
        )
    else:
        _ok("encounters -> progression_depth validated")

    learnable_species_orphans = _left_orphan_count(learnable_moves, pokemon_data, ["pokemon_species"])
    if learnable_species_orphans:
        _record(
            findings,
            "warning",
            "FK_LEARNABLE_SPECIES",
            f"learnable_moves rows missing pokemon_data species: {learnable_species_orphans}",
        )
    else:
        _ok("learnable_moves -> pokemon_data validated")

    if _left_orphan_count(learnable_moves, games, ["game_version"]) == 0:
        _ok("learnable_moves -> games validated")
    else:
        _record(findings, "error", "FK_LEARNABLE_GAME", "learnable_moves contains unknown game_version")

    if _left_orphan_count(learnable_moves, move_reference, ["move_name"]) == 0:
        _ok("learnable_moves -> move_reference validated")
    else:
        _record(findings, "error", "FK_LEARNABLE_MOVE", "learnable_moves contains unknown move_name")

    if _left_orphan_count(source_teams, games, ["game_version"]) == 0:
        _ok("source_teams -> games validated")
    else:
        _record(findings, "error", "FK_TEAM_GAME", "source_teams contains unknown game_version")

    boss_team_rows = source_teams[source_teams["team_role"].eq("boss")]
    if _left_orphan_count(boss_team_rows, bosses, ["game_version", "boss_id"]) == 0:
        _ok("boss source_teams -> bosses validated")
    else:
        _record(findings, "error", "FK_BOSS_TEAM_BOSS", "boss source_teams contain unknown boss_id")

    player_team_rows = source_teams[source_teams["team_role"].eq("player")]
    if _left_orphan_count(player_team_rows, progression_depth, ["game_version", "boss_id"]) == 0:
        _ok("player source_teams -> progression_depth validated")
    else:
        _record(findings, "error", "FK_PLAYER_TEAM_PROGRESSION", "player source_teams contain unknown progression boss_id")

    if _left_orphan_count(source_team_members, source_teams, ["source_team_id"]) == 0:
        _ok("source_team_members -> source_teams validated")
    else:
        _record(findings, "error", "FK_MEMBER_TEAM", "source_team_members contains unknown source_team_id")

    if _left_orphan_count(member_move_options, source_team_members, ["team_member_id"]) == 0:
        _ok("member_move_options -> source_team_members validated")
    else:
        _record(findings, "error", "FK_MOVE_OPTION_MEMBER", "member_move_options contains unknown team_member_id")

    if _left_orphan_count(member_move_options, source_teams, ["source_team_id"]) == 0:
        _ok("member_move_options -> source_teams validated")
    else:
        _record(findings, "error", "FK_MOVE_OPTION_TEAM", "member_move_options contains unknown source_team_id")

    if _left_orphan_count(member_moveset_combos, source_teams, ["team_id"], ["source_team_id"]) == 0:
        _ok("member_moveset_combos.team_id -> source_teams.source_team_id validated")
    else:
        _record(findings, "error", "FK_COMBO_TEAM", "member_moveset_combos contains unknown team_id")

    if _left_orphan_count(member_moveset_combos, source_team_members, ["pokemon_instance_id"], ["team_member_id"]) == 0:
        _ok("member_moveset_combos -> source_team_members validated")
    else:
        _record(findings, "error", "FK_COMBO_MEMBER", "member_moveset_combos contains unknown pokemon_instance_id")

    if _left_orphan_count(member_move_options, pokemon_moveset_options, ["moveset_context_id", "move_name"]) == 0:
        _ok("member_move_options -> pokemon_moveset_options validated")
    else:
        _record(findings, "error", "FK_MOVE_OPTION_CONTEXT", "member_move_options contains unknown moveset context row")

    if _left_orphan_count(pokemon_moveset_options, pokemon_combat_pool, ["game_version", "pokemon_species", "level"]) == 0:
        _ok("pokemon_moveset_options -> pokemon_combat_pool validated")
    else:
        _record(findings, "error", "FK_CONTEXT_POOL", "pokemon_moveset_options contains unknown combat-pool context")

    if _left_orphan_count(simulation_sampling_plan, source_teams, ["source_team_id"]) == 0:
        _ok("simulation_sampling_plan -> source_teams validated")
    else:
        _record(findings, "error", "FK_SAMPLING_TEAM", "simulation_sampling_plan contains unknown source_team_id")

    if _left_orphan_count(move_data, source_team_members, ["pokemon_instance_id"], ["team_member_id"]) == 0:
        _ok("move_data -> source_team_members validated")
    else:
        _record(findings, "error", "FK_MOVE_DATA_MEMBER", "move_data contains unknown pokemon_instance_id")

    if _left_orphan_count(move_data, source_teams, ["team_id"], ["source_team_id"]) == 0:
        _ok("move_data -> source_teams validated")
    else:
        _record(findings, "error", "FK_MOVE_DATA_TEAM", "move_data contains unknown team_id")

    if _left_orphan_count(move_data.rename(columns={"species": "pokemon_species"}), pokemon_data, ["pokemon_species"]) == 0:
        _ok("move_data.species -> pokemon_data validated")
    else:
        _record(findings, "error", "FK_MOVE_DATA_SPECIES", "move_data contains unknown species")

    if _left_orphan_count(move_data, games, ["game_version"]) == 0:
        _ok("move_data -> games validated")
    else:
        _record(findings, "error", "FK_MOVE_DATA_GAME", "move_data contains unknown game_version")

    duplicate_team_members = int(source_team_members.duplicated(["source_team_id", "slot"]).sum())
    if duplicate_team_members:
        _record(findings, "error", "DUPLICATE_TEAM_MEMBERS", f"duplicate team members by (source_team_id, slot): {duplicate_team_members}")
    else:
        _ok("no duplicate team members by (source_team_id, slot)")

    move_names = set(move_reference["move_name"])
    combo_invalid = {
        column: int((~member_moveset_combos[column].dropna().isin(move_names)).sum())
        for column in ["move_1", "move_2", "move_3", "move_4"]
        if column in member_moveset_combos.columns
    }
    invalid_combo_total = sum(combo_invalid.values())
    if invalid_combo_total:
        _record(findings, "error", "INVALID_MOVE_REFERENCE", f"member_moveset_combos invalid move references: {combo_invalid}")
    else:
        _ok("member_moveset_combos move references validated")

    invalid_move_data_moves = 0
    invalid_move_detail_keys = 0
    for _, row in move_data.iterrows():
        invalid_move_data_moves += sum(move not in move_names for move in row["provided_moves"])
        invalid_move_data_moves += sum(move not in move_names for move in row["learnable_moves"])
        invalid_move_detail_keys += sum(move not in move_names for move in (row["move_details"] or {}).keys())
    if invalid_move_data_moves or invalid_move_detail_keys:
        _record(
            findings,
            "error",
            "INVALID_MOVE_DATA_REFERENCE",
            "move_data contains move names missing from move_reference",
        )
    else:
        _ok("move_data move references validated")

    species_names = set(pokemon_data["pokemon_species"])
    invalid_species_checks = {
        "source_team_members": int((~source_team_members["pokemon_species"].isin(species_names)).sum()),
        "pokemon_combat_pool": int((~pokemon_combat_pool["pokemon_species"].isin(species_names)).sum()),
        "move_data": int((~move_data["species"].isin(species_names)).sum()),
        "learnable_moves": int((~learnable_moves["pokemon_species"].isin(species_names)).sum()),
    }
    invalid_species_total = sum(invalid_species_checks.values())
    if invalid_species_total:
        _record(findings, "warning", "MISSING_POKEMON_REFERENCE", f"species missing from pokemon_data: {invalid_species_checks}")
    else:
        _ok("species references validated against pokemon_data")

    boss_without_members = int((~boss_team_rows["source_team_id"].isin(set(source_team_members["source_team_id"]))).sum())
    if boss_without_members:
        _record(findings, "error", "BOSS_TEAM_WITHOUT_MEMBERS", f"boss teams without members: {boss_without_members}")
    else:
        _ok("all boss teams have members")

    player_without_move_options = int((~player_team_rows["source_team_id"].isin(set(member_move_options["source_team_id"]))).sum())
    if player_without_move_options:
        _record(findings, "error", "PLAYER_TEAM_WITHOUT_MOVE_OPTIONS", f"player teams without move options: {player_without_move_options}")
    else:
        _ok("all player teams have move options")

    duplicate_moves_within_combo = int(
        member_moveset_combos[["move_1", "move_2", "move_3", "move_4"]]
        .apply(
            lambda row: len([value for value in row if pd.notna(value)])
            != len({value for value in row if pd.notna(value)}),
            axis=1,
        )
        .sum()
    )
    if duplicate_moves_within_combo:
        _record(
            findings,
            "error",
            "DUPLICATE_MOVES_WITHIN_COMBO",
            f"moveset combos with duplicate moves: {duplicate_moves_within_combo}",
        )
    else:
        _ok("moveset combos do not repeat moves within a row")

    invalid_game_refs = {
        "locations": int((~locations["game_version"].isin(set(games["game_version"]))).sum()),
        "source_teams": int((~source_teams["game_version"].isin(set(games["game_version"]))).sum()),
        "source_team_members": int((~source_team_members["game_version"].isin(set(games["game_version"]))).sum()),
        "member_move_options": int((~member_move_options["game_version"].isin(set(games["game_version"]))).sum()),
        "member_moveset_combos": int((~member_moveset_combos["game_version"].isin(set(games["game_version"]))).sum()),
        "pokemon_combat_pool": int((~pokemon_combat_pool["game_version"].isin(set(games["game_version"]))).sum()),
        "pokemon_moveset_options": int((~pokemon_moveset_options["game_version"].isin(set(games["game_version"]))).sum()),
        "move_data": int((~move_data["game_version"].isin(set(games["game_version"]))).sum()),
    }
    invalid_game_total = sum(invalid_game_refs.values())
    if invalid_game_total:
        _record(findings, "warning", "UNKNOWN_GAME_VERSION", f"rows with game_version not in games: {invalid_game_refs}")
    else:
        _ok("game_version references validated")

    header_rows = int(pokemon_moveset_options["move_name"].isna().sum())
    if header_rows:
        _warn(
            "simulation/pokemon_moveset_options contains header/context rows with null move_name, "
            f"option_rank, and option_score: {header_rows}"
        )

    optional_boss_gaps = int((~source_team_members["team_member_id"].isin(set(member_move_options["team_member_id"]))).sum())
    if optional_boss_gaps:
        _warn(
            "source_team_members rows without member_move_options exist; current data shows these are boss/kaggle rows "
            f"with fixed moves only: {optional_boss_gaps}"
        )

    manifest_path = silver_dir / "manifest.json"
    if manifest_path.exists():
        manifest = _read_json(manifest_path)
        datasets = manifest.get("datasets", {})
        for dataset_name, meta in datasets.items():
            if "file" in meta:
                path = silver_dir / meta["file"]
                if not path.exists():
                    _record(findings, "error", "MANIFEST_MISSING_FILE", f"{dataset_name} points to missing path {meta['file']}")
            for rel in meta.get("files", []):
                path = silver_dir / rel
                if not path.exists():
                    _record(findings, "error", "MANIFEST_MISSING_FILE", f"{dataset_name} points to missing shard {rel}")

    return findings


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the current Silver physical contract from persisted artifacts.")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--inventory-json", help="Optional path to write an artifact inventory JSON report.")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)
    inventory = _artifact_inventory(silver_dir)
    if args.inventory_json:
        output_path = Path(args.inventory_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
        _ok(f"wrote inventory report to {output_path}")

    findings = validate(silver_dir)
    error_count = sum(1 for finding in findings if finding.level == "error")
    warning_count = sum(1 for finding in findings if finding.level == "warning")
    print()
    print(f"Inventory artifacts: {len(inventory)}")
    print(f"Findings: errors={error_count} warnings={warning_count}")
    if args.fail_on_error and error_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
