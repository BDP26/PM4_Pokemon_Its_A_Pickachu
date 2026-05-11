from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline.common.io import read_parquet
from src.pipeline.silver.inputs.reference_context import normalize_species_slug
from src.pipeline.silver.validation.contract_utils import load_simulation_shards

POKEMON_REQUIRED_COLUMNS = [
    "pokemon_species",
    "name",
    "type_1",
    "base_hp",
    "base_attack",
    "base_defense",
    "base_special_attack",
    "base_special_defense",
    "base_speed",
]


def _ok(label: str) -> None:
    print(f"[OK] {label}")


def _error(label: str) -> None:
    print(f"[ERROR] {label}")


def _read(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset: {path}")
    return read_parquet(path)


def _load_sharded(simulation_dir: Path, prefix: str) -> pd.DataFrame:
    return load_simulation_shards(simulation_dir, prefix)


def validate(silver_dir: Path) -> list[str]:
    errors: list[str] = []

    pokemon_data_path = silver_dir / "references" / "pokemon_data.parquet"
    pokemon_reference_path = silver_dir / "references" / "pokemon_reference.parquet"
    move_reference_path = silver_dir / "references" / "move_reference.parquet"
    simulation_dir = silver_dir / "simulation"

    pokemon_data = _read(pokemon_data_path)
    pokemon_reference = _read(pokemon_reference_path)

    if pokemon_data_path.exists():
        _ok("pokemon_data.parquet exists")

    missing_columns = [column for column in POKEMON_REQUIRED_COLUMNS if column not in pokemon_data.columns]
    if missing_columns:
        errors.append(f"pokemon_data missing required columns: {missing_columns}")
    else:
        missing_mask = pokemon_data[POKEMON_REQUIRED_COLUMNS].isna().any(axis=1)
        if missing_mask.any():
            species_preview = ",".join(
                sorted(
                    {
                        normalize_species_slug(value)
                        for value in pokemon_data.loc[missing_mask, "pokemon_species"].fillna("")
                        if normalize_species_slug(value)
                    }
                )[:20]
            )
            errors.append(
                "pokemon_data contains null combat fields: "
                f"rows={int(missing_mask.sum())} first_20_species=[{species_preview}]"
            )
        else:
            _ok("pokemon_data has no null required combat fields")

    for column in [
        "base_hp",
        "base_attack",
        "base_defense",
        "base_special_attack",
        "base_special_defense",
        "base_speed",
    ]:
        if column not in pokemon_data.columns:
            errors.append(f"pokemon_data missing stat column: {column}")
            continue
        numeric = pd.to_numeric(pokemon_data[column], errors="coerce")
        invalid = numeric.isna() | (numeric < 1) | (numeric > 255)
        if invalid.any():
            errors.append(f"pokemon_data.{column} has invalid values count={int(invalid.sum())}")
    if not errors:
        _ok("pokemon_data stat columns are numeric in 1..255")

    if list(pokemon_data.columns) == list(pokemon_reference.columns) and pokemon_data.equals(pokemon_reference):
        errors.append("pokemon_data is identical to pokemon_reference")
    else:
        _ok("pokemon_data is distinct from pokemon_reference")

    members = _load_sharded(simulation_dir, "source_team_members")
    known_species = {
        normalize_species_slug(value)
        for value in pokemon_data.get("pokemon_species", pd.Series([], dtype="object")).fillna("")
        if normalize_species_slug(value)
    }
    missing_member_species = sorted(
        {
            normalize_species_slug(value)
            for value in members.get("pokemon_species", pd.Series([], dtype="object")).fillna("")
            if normalize_species_slug(value) and normalize_species_slug(value) not in known_species
        }
    )
    if missing_member_species:
        errors.append(
            "source_team_members includes species missing from pokemon_data after normalization: "
            f"count={len(missing_member_species)} first_20=[{','.join(missing_member_species[:20])}]"
        )
    else:
        _ok("source_team_members species are covered by pokemon_data")

    move_reference = _read(move_reference_path)
    move_data = _read(simulation_dir / "move_data.parquet")
    required_move_cols = {"move_name", "type", "damage_class", "power", "effective_power", "power_handling"}
    if required_move_cols.issubset(set(move_reference.columns)) and not required_move_cols.issubset(set(move_data.columns)):
        _ok("move profile loader target is reference schema (not simulation move_data table)")
    else:
        _error("move profile loader target check requires move_reference schema; inspect manually")

    if normalize_species_slug("mr. mime") != "mr-mime":
        errors.append("species normalization mismatch: mr. mime should normalize to mr-mime")
    else:
        _ok("mr. mime normalizes to mr-mime")

    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    errors = validate(Path(args.silver_dir))
    if errors:
        for message in errors:
            _error(message)
        if args.fail_on_error:
            raise SystemExit(1)
    else:
        _ok("Silver validation passed")


if __name__ == "__main__":
    main()
