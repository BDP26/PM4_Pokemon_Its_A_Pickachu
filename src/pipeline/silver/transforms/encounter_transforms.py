"""DataFrame transforms for encounter and boss team data."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import pandas as pd

from src.pipeline.common.cast import to_list
from src.pipeline.silver.config.boss_config import BOSS_ALIASES, STRIATON_CONDITIONAL_BOSSES, boss_id
from src.pipeline.silver.inputs.reference_context import normalize_move_name, normalize_species_slug
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, normalize_key_part

logger = logging.getLogger(__name__)


def _coerce_alias_values(value: Any) -> list[Any]:
    return to_list(value, drop_nullish=True)


def _validation_profile(values_by_column: dict[str, set[str]], row_count: int) -> dict[str, Any]:
    return {"row_count": row_count, "columns": {column: sorted(values) for column, values in values_by_column.items()}}


def _collect_kaggle_boss_species_and_moves(boss_teams: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    species: set[str] = set()
    moves: set[str] = set()
    for team in boss_teams:
        pokemon_entries = team.get("pokemon")
        if isinstance(pokemon_entries, list):
            for pokemon in pokemon_entries:
                species_slug = normalize_species_slug(pokemon)
                if species_slug:
                    species.add(species_slug)
        move_entries = team.get("moves")
        if isinstance(move_entries, list):
            for member_moves in move_entries:
                if not isinstance(member_moves, list):
                    continue
                for move in member_moves:
                    move_slug = normalize_move_name(move)
                    if move_slug:
                        moves.add(move_slug)
    return species, moves


def _expand_striaton_encounters(encounters_df: pd.DataFrame) -> pd.DataFrame:
    if encounters_df.empty:
        return encounters_df

    required = {"game", "boss_id", "location", "pokemon"}
    if not required.issubset(encounters_df.columns):
        return encounters_df

    cloned_rows: list[dict[str, Any]] = []
    striaton_boss_ids = {
        normalize_key_part(str(conditional_boss["boss_name"]))
        for conditional_boss in STRIATON_CONDITIONAL_BOSSES
    }
    for game_version in ("black", "white"):
        source_rows = encounters_df[
            encounters_df["game"].astype(str).str.strip().str.lower().eq(game_version)
            & encounters_df["boss_id"].astype(str).str.strip().str.lower().isin(
                {f"{game_version}:{boss_name}" for boss_name in striaton_boss_ids}
            )
        ].copy()
        if source_rows.empty:
            continue
        seed_row = source_rows.sort_values("boss_id").iloc[0].to_dict()
        for conditional_boss in STRIATON_CONDITIONAL_BOSSES:
            cloned = dict(seed_row)
            cloned["game"] = game_version
            cloned["boss_id"] = boss_id(game_version, str(conditional_boss["boss_name"]))
            cloned_rows.append(cloned)

    if not cloned_rows:
        return encounters_df

    expanded = pd.concat([encounters_df, pd.DataFrame(cloned_rows)], ignore_index=True)

    def _freeze_value(value: Any) -> Any:
        if isinstance(value, list):
            return tuple(_freeze_value(item) for item in value)
        if isinstance(value, dict):
            return tuple(sorted((str(key), _freeze_value(item)) for key, item in value.items()))
        return value

    seen: set[tuple[Any, ...]] = set()
    deduped_rows: list[dict[str, Any]] = []
    for row in expanded.to_dict(orient="records"):
        identity = tuple((column, _freeze_value(value)) for column, value in sorted(row.items()))
        if identity in seen:
            continue
        seen.add(identity)
        deduped_rows.append(row)

    return pd.DataFrame(deduped_rows).reset_index(drop=True)


def _canonicalize_encounter_boss_ids(
    encounters_df: pd.DataFrame,
    bosses_reference_df: pd.DataFrame,
) -> pd.DataFrame:
    if encounters_df.empty or bosses_reference_df.empty:
        return encounters_df
    required_encounter_columns = {"game", "boss_id"}
    required_boss_columns = {"game_version", "boss_id", "boss_name_canonical"}
    if not required_encounter_columns.issubset(encounters_df.columns):
        return encounters_df
    if not required_boss_columns.issubset(bosses_reference_df.columns):
        return encounters_df

    bosses = bosses_reference_df.copy()
    bosses["game_version"] = bosses["game_version"].astype(str).str.strip().str.lower()
    bosses["boss_id"] = bosses["boss_id"].astype(str).str.strip().str.lower()
    bosses["boss_slug"] = bosses["boss_name_canonical"].map(normalize_key_part)
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_id"] != "") & (bosses["boss_slug"] != "")]
    if bosses.empty:
        return encounters_df

    boss_id_by_game_slug: dict[tuple[str, str], str] = {}
    for row in bosses[["game_version", "boss_slug", "boss_id"]].drop_duplicates().itertuples(index=False):
        boss_id_by_game_slug[(str(row.game_version), str(row.boss_slug))] = str(row.boss_id)

    out = encounters_df.copy()
    out["game"] = out["game"].astype(str).str.strip().str.lower()
    out["boss_id"] = out["boss_id"].astype(str).str.strip().str.lower()
    out["boss_slug"] = out["boss_id"].map(lambda value: normalize_key_part(str(value).split(":")[-1]))
    out["canonical_boss_id"] = out.apply(
        lambda row: boss_id_by_game_slug.get((str(row["game"]), str(row["boss_slug"]))),
        axis=1,
    )
    out["boss_id"] = out["canonical_boss_id"].fillna(out["boss_id"])
    out = out.drop(columns=["boss_slug", "canonical_boss_id"], errors="ignore")
    return out


def _filter_bosses_with_encounter_pools(
    bosses_df: pd.DataFrame,
    encounters_df: pd.DataFrame,
) -> pd.DataFrame:
    """Keep only boss rows that have at least one encounter row in the same game."""
    if bosses_df.empty or encounters_df.empty:
        return bosses_df

    required_boss_columns = {"game_version", "boss_id"}
    required_encounter_columns = {"game", "boss_id"}
    if not required_boss_columns.issubset(bosses_df.columns):
        return bosses_df
    if not required_encounter_columns.issubset(encounters_df.columns):
        return bosses_df

    encounter_pairs = {
        (str(row.game).strip().lower(), str(row.boss_id).strip().lower())
        for row in encounters_df[["game", "boss_id"]].drop_duplicates().itertuples(index=False)
        if str(row.game).strip() and str(row.boss_id).strip()
    }
    if not encounter_pairs:
        return bosses_df

    normalized_pairs = [
        (str(game_version).strip().lower(), str(boss_id_val).strip().lower())
        for game_version, boss_id_val in bosses_df[["game_version", "boss_id"]].itertuples(index=False, name=None)
    ]
    keep_mask = [pair in encounter_pairs for pair in normalized_pairs]
    filtered = bosses_df.loc[keep_mask].copy()

    dropped = bosses_df.loc[[not keep for keep in keep_mask]].copy()
    if not dropped.empty:
        sample = [
            {
                "game_version": str(row.get("game_version") or "").strip().lower(),
                "boss_id": str(row.get("boss_id") or "").strip().lower(),
                "boss_name": str(row.get("boss_name_canonical") or row.get("boss_name") or "").strip().lower() or None,
            }
            for row in dropped.head(10).to_dict(orient="records")
        ]
        logger.warning(
            "[silver/bosses] dropping bosses without encounter pools count=%s sample=%s",
            len(dropped),
            sample,
        )

    return filtered


def _normalize_boss_teams_with_conditional_striaton(
    boss_teams: list[dict[str, Any]],
    boss_move_data: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not boss_teams:
        return boss_teams, boss_move_data

    normalized_teams: list[dict[str, Any]] = []

    def _source_priority(team: dict[str, Any]) -> tuple[int, str]:
        game_version = str(team.get("game_version") or "").strip().lower()
        priority = 0 if game_version == "black" else 1 if game_version == "white" else 2
        return (priority, str(team.get("team_id") or ""))

    striaton_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for team in boss_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_name = normalize_key_part(team.get("boss_name") or team.get("gym"))
        if game_version in {"black", "white"} and boss_name in {"chili", "cilan", "cress"}:
            striaton_candidates[boss_name].append(team)
            continue
        cloned = dict(team)
        if boss_name and game_version:
            cloned["boss_id"] = boss_id(game_version, boss_name)
        normalized_teams.append(cloned)

    condition_by_boss = {
        normalize_key_part(entry["boss_name"]): str(entry["starter_condition"])
        for entry in STRIATON_CONDITIONAL_BOSSES
    }
    for boss_name, candidates in sorted(striaton_candidates.items()):
        chosen = min(candidates, key=_source_priority)
        cloned = dict(chosen)
        cloned["team_id"] = boss_id("black-white", boss_name)
        cloned["boss_id"] = cloned["team_id"]
        cloned["game_version"] = "black-white"
        cloned["boss_name"] = boss_name
        cloned["gym"] = boss_name
        cloned["starter_condition"] = condition_by_boss.get(boss_name)
        original_member_ids = list(chosen.get("pokemon_instance_ids") or []) if isinstance(chosen.get("pokemon_instance_ids"), list) else []
        member_ids = []
        for slot, species in enumerate(list(cloned.get("pokemon") or []), start=1):
            member_ids.append(make_pokemon_instance_id(cloned["team_id"], slot, normalize_species_slug(species)))
        cloned["pokemon_instance_ids"] = member_ids
        move_map = list(zip(original_member_ids, member_ids, strict=False))
        cloned["_original_member_id_map"] = move_map
        normalized_teams.append(cloned)

    filtered_move_data: dict[str, Any] = {}
    for team in normalized_teams:
        member_ids = team.get("pokemon_instance_ids", [])
        if not isinstance(member_ids, list):
            continue
        original_map = team.pop("_original_member_id_map", [])
        if isinstance(original_map, list) and original_map:
            for original_id, cloned_id in original_map:
                payload = boss_move_data.get(str(original_id))
                if payload is None:
                    continue
                remapped_payload = dict(payload)
                remapped_payload["pokemon_instance_id"] = str(cloned_id)
                remapped_payload["team_id"] = str(team.get("team_id") or "")
                filtered_move_data[str(cloned_id)] = remapped_payload
            continue
        for member_id in member_ids:
            payload = boss_move_data.get(str(member_id))
            if payload is not None:
                filtered_move_data[str(member_id)] = payload

    return normalized_teams, filtered_move_data


def _canonicalize_boss_teams_to_references(
    boss_teams: list[dict[str, Any]],
    boss_move_data: dict[str, Any],
    bosses_reference_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not boss_teams or bosses_reference_df.empty:
        return boss_teams, boss_move_data

    required_columns = {"game_version", "boss_name_canonical", "boss_name_kaggle"}
    missing_columns = required_columns - set(bosses_reference_df.columns)
    if missing_columns:
        raise ValueError(f"bosses reference missing required columns for boss team canonicalization: {sorted(missing_columns)}")

    reference_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    alias_to_reference_key: dict[tuple[str, str], tuple[str, str]] = {}

    for row in bosses_reference_df.to_dict(orient="records"):
        game_version = normalize_key_part(row.get("game_version"))
        canonical_name_raw = str(row.get("boss_name_canonical") or "").strip()
        canonical_name = normalize_key_part(canonical_name_raw)
        if not game_version or not canonical_name:
            continue

        ref_key = (game_version, canonical_name)
        reference_by_key[ref_key] = row

        alias_values = _coerce_alias_values(row.get("boss_name_aliases"))
        aliases = {
            canonical_name,
            normalize_key_part(row.get("boss_name_kaggle")),
            *(normalize_key_part(alias) for alias in alias_values),
            *(normalize_key_part(alias) for alias in BOSS_ALIASES.get(game_version, {}).get(canonical_name_raw, [])),
        }
        for alias in aliases:
            if alias:
                alias_to_reference_key[(game_version, alias)] = ref_key

    selected_by_team_id: dict[str, dict[str, Any]] = {}
    dropped_team_ids: set[str] = set()
    unmatched_teams: list[dict[str, Any]] = []
    grouped_candidates: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for team in boss_teams:
        team_id = str(team.get("team_id") or "").strip()
        game_version = normalize_key_part(team.get("game_version"))
        boss_label = normalize_key_part(team.get("boss_name") or team.get("gym"))
        reference_key = alias_to_reference_key.get((game_version, boss_label))
        team_copy = dict(team)
        team_copy["_raw_boss_label"] = boss_label
        if reference_key is None:
            unmatched_teams.append(team_copy)
            continue

        canonical_name = reference_key[1]
        ref_row = reference_by_key[reference_key]
        team_copy["boss_name"] = canonical_name
        team_copy["boss_id"] = str(ref_row.get("boss_id") or team_copy.get("boss_id") or "").strip().lower()
        grouped_candidates[reference_key].append(team_copy)

    for reference_key, candidates in grouped_candidates.items():
        ref_row = reference_by_key[reference_key]
        alias_values = _coerce_alias_values(ref_row.get("boss_name_aliases"))
        preferred_aliases = [
            normalize_key_part(ref_row.get("boss_name_kaggle")),
            reference_key[1],
            *(normalize_key_part(alias) for alias in alias_values),
            *(normalize_key_part(alias) for alias in BOSS_ALIASES.get(reference_key[0], {}).get(str(ref_row.get("boss_name_canonical") or "").strip(), [])),
        ]

        def _candidate_rank(team: dict[str, Any]) -> tuple[int, str]:
            label = normalize_key_part(team.get("_raw_boss_label"))
            try:
                alias_rank = preferred_aliases.index(label)
            except ValueError:
                alias_rank = len(preferred_aliases)
            return (alias_rank, str(team.get("team_id") or ""))

        selected_team = min(candidates, key=_candidate_rank)
        selected_team_id = str(selected_team.get("team_id") or "").strip()
        if selected_team_id:
            selected_by_team_id[selected_team_id] = selected_team

        if len(candidates) > 1:
            duplicate_ids = [str(team.get("team_id") or "").strip() for team in candidates if str(team.get("team_id") or "").strip()]
            dropped_ids = [team_id for team_id in duplicate_ids if team_id != selected_team_id]
            dropped_team_ids.update(dropped_ids)
            logger.warning(
                "[silver/teams] collapsed duplicate boss variants game=%s canonical_boss=%s kept_team_id=%s dropped_team_ids=%s",
                reference_key[0],
                reference_key[1],
                selected_team_id,
                dropped_ids,
            )

    canonicalized_teams: list[dict[str, Any]] = []
    for team in boss_teams:
        team_id = str(team.get("team_id") or "").strip()
        if not team_id or team_id in dropped_team_ids:
            continue
        selected = selected_by_team_id.get(team_id)
        if selected is not None:
            selected.pop("_raw_boss_label", None)
            canonicalized_teams.append(selected)

    if unmatched_teams:
        logger.warning(
            "[silver/teams] dropped boss teams without matching boss reference rows count=%s sample=%s",
            len(unmatched_teams),
            [
                {
                    "game_version": str(team.get("game_version") or "").strip().lower(),
                    "boss_id": str(team.get("boss_id") or "").strip().lower(),
                    "boss_name": str(team.get("boss_name") or team.get("gym") or "").strip().lower(),
                }
                for team in unmatched_teams[:10]
            ],
        )

    selected_member_ids = {
        str(member_id).strip()
        for team in canonicalized_teams
        for member_id in (team.get("pokemon_instance_ids", []) if isinstance(team.get("pokemon_instance_ids"), list) else [])
        if str(member_id).strip()
    }
    filtered_move_data = {
        member_id: payload
        for member_id, payload in boss_move_data.items()
        if member_id in selected_member_ids
    }

    return canonicalized_teams, filtered_move_data
