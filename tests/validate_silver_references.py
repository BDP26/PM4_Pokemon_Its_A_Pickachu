from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


GAMES = [
    "red",
    "blue",
    "gold",
    "silver",
    "ruby",
    "sapphire",
    "diamond",
    "pearl",
    "black",
    "white",
    "x",
    "y",
]

POKEMON_STAT_COLS = [
    "base_hp",
    "base_attack",
    "base_defense",
    "base_special_attack",
    "base_special_defense",
    "base_speed",
]

POKEMON_REQUIRED_COLS = [
    "pokemon_species",
    "name",
    "type_1",
    *POKEMON_STAT_COLS,
]

MOVE_REQUIRED_COLS = [
    "move_name",
    "type",
    "damage_class",
]

NULL_LIKE_STRINGS = {
    "",
    " ",
    "none",
    "null",
    "nan",
    "na",
    "n/a",
    "unknown",
}


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file/dataset: {path}")
    return pd.read_parquet(path)


def normalize_null_like_values(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            values = out[col].astype("string").str.strip()
            out[col] = out[col].mask(values.str.lower().isin(NULL_LIKE_STRINGS))

    return out


def print_section(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def check_required_columns(df: pd.DataFrame, required_cols: list[str], dataset: str) -> bool:
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        print(f"[ERROR] {dataset}: missing required columns: {missing}")
        print(f"[INFO] available columns: {list(df.columns)}")
        return False

    print(f"[OK] {dataset}: required columns exist")
    return True


def check_nulls(
    df: pd.DataFrame,
    cols: list[str],
    dataset: str,
    id_cols: list[str],
    limit: int,
) -> bool:
    existing_cols = [col for col in cols if col in df.columns]

    if not existing_cols:
        print(f"[WARN] {dataset}: no checked columns exist from {cols}")
        return True

    null_mask = df[existing_cols].isna()
    bad = df[null_mask.any(axis=1)].copy()

    if bad.empty:
        print(f"[OK] {dataset}: no nulls in {existing_cols}")
        return True

    print(f"[ERROR] {dataset}: {len(bad)} rows have nulls")
    print()
    print("Null counts:")
    print(null_mask.sum()[lambda s: s > 0].sort_values(ascending=False).to_string())

    display_cols = [col for col in id_cols + existing_cols if col in bad.columns]
    print()
    print(f"First {limit} bad rows:")
    print(bad[display_cols].head(limit).to_string(index=False))

    return False


def check_numeric_range(
    df: pd.DataFrame,
    ranges: dict[str, tuple[int | None, int | None]],
    dataset: str,
    id_cols: list[str],
    limit: int,
) -> bool:
    ok = True

    for col, (min_value, max_value) in ranges.items():
        if col not in df.columns:
            print(f"[WARN] {dataset}.{col}: column missing, range check skipped")
            continue

        numeric = pd.to_numeric(df[col], errors="coerce")

        bad_mask = numeric.isna()
        if min_value is not None:
            bad_mask |= numeric < min_value
        if max_value is not None:
            bad_mask |= numeric > max_value

        bad = df[bad_mask].copy()

        if bad.empty:
            print(f"[OK] {dataset}.{col}: valid range")
            continue

        ok = False
        print(
            f"[ERROR] {dataset}.{col}: {len(bad)} invalid values "
            f"(expected min={min_value}, max={max_value})"
        )

        display_cols = [c for c in id_cols + [col] if c in bad.columns]
        print(bad[display_cols].head(limit).to_string(index=False))

    return ok


def check_duplicates(df: pd.DataFrame, subset: list[str], dataset: str, limit: int) -> bool:
    missing = [col for col in subset if col not in df.columns]
    if missing:
        print(f"[WARN] {dataset}: duplicate check skipped, missing {missing}")
        return True

    dupes = df[df.duplicated(subset, keep=False)].copy()

    if dupes.empty:
        print(f"[OK] {dataset}: no duplicates by {subset}")
        return True

    print(f"[ERROR] {dataset}: {len(dupes)} duplicate rows by {subset}")
    print(dupes[subset].head(limit).to_string(index=False))
    return False


def find_species_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "pokemon_species",
        "species",
        "pokemon_name",
        "name",
        "member_name",
    ]

    for col in candidates:
        if col in df.columns:
            return col

    return None


def find_move_columns(df: pd.DataFrame) -> list[str]:
    exact = ["move_name", "move_1", "move_2", "move_3", "move_4"]
    cols = [col for col in exact if col in df.columns]

    if cols:
        return cols

    return [
        col
        for col in df.columns
        if col.startswith("move_") or col.endswith("_move") or "move_name" in col
    ]


def load_game_split_files(folder: Path, prefix: str) -> pd.DataFrame:
    frames = []

    for game in GAMES:
        path = folder / f"{prefix}_{game}.parquet"
        if not path.exists():
            print(f"[WARN] missing expected file: {path}")
            continue

        df = read_parquet(path)
        df = normalize_null_like_values(df)
        df["__game_file"] = game
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No files found for prefix={prefix} in {folder}")

    return pd.concat(frames, ignore_index=True)


def validate_pokemon_data(silver_dir: Path, limit: int) -> bool:
    print_section("references/pokemon_data.parquet")

    path = silver_dir / "references" / "pokemon_data.parquet"
    df = normalize_null_like_values(read_parquet(path))

    print(f"path={path}")
    print(f"rows={len(df)} columns={len(df.columns)}")
    print(f"columns={list(df.columns)}")

    ok = True
    ok &= check_required_columns(df, POKEMON_REQUIRED_COLS, "pokemon_data")

    ok &= check_nulls(
        df=df,
        cols=POKEMON_REQUIRED_COLS,
        dataset="pokemon_data",
        id_cols=["pokemon_species", "name"],
        limit=limit,
    )

    ok &= check_numeric_range(
        df=df,
        ranges={
            "base_hp": (1, 255),
            "base_attack": (1, 255),
            "base_defense": (1, 255),
            "base_special_attack": (1, 255),
            "base_special_defense": (1, 255),
            "base_speed": (1, 255),
        },
        dataset="pokemon_data",
        id_cols=["pokemon_species", "name"],
        limit=limit,
    )

    ok &= check_duplicates(
        df=df,
        subset=["pokemon_species"],
        dataset="pokemon_data",
        limit=limit,
    )

    return ok


def validate_move_data(silver_dir: Path, limit: int) -> bool:
    print_section("simulation/move_data.parquet")

    path = silver_dir / "simulation" / "move_data.parquet"
    df = normalize_null_like_values(read_parquet(path))

    print(f"path={path}")
    print(f"rows={len(df)} columns={len(df.columns)}")
    print(f"columns={list(df.columns)}")

    ok = True
    ok &= check_required_columns(df, MOVE_REQUIRED_COLS, "move_data")

    nullable_numeric_cols = [
        col for col in ["power", "accuracy", "priority"] if col in df.columns
    ]

    ok &= check_nulls(
        df=df,
        cols=MOVE_REQUIRED_COLS,
        dataset="move_data",
        id_cols=["move_name"],
        limit=limit,
    )

    # For moves, power and accuracy can be null in real Pokémon data for status/fixed-damage moves.
    # This check only validates values when they are present.
    for col in nullable_numeric_cols:
        numeric = pd.to_numeric(df[col], errors="coerce")
        non_null = df[df[col].notna()].copy()
        if non_null.empty:
            print(f"[WARN] move_data.{col}: all values are null")
            continue

        ranges = {
            "power": (0, 300),
            "accuracy": (0, 100),
            "priority": (-10, 10),
        }

        ok &= check_numeric_range(
            df=non_null,
            ranges={col: ranges[col]},
            dataset="move_data",
            id_cols=["move_name"],
            limit=limit,
        )

    ok &= check_duplicates(
        df=df,
        subset=["move_name"],
        dataset="move_data",
        limit=limit,
    )

    return ok


def validate_source_team_member_coverage(silver_dir: Path, limit: int) -> bool:
    print_section("simulation/source_team_members_*.parquet coverage")

    simulation_dir = silver_dir / "simulation"
    members = load_game_split_files(simulation_dir, "source_team_members")
    pokemon = normalize_null_like_values(read_parquet(silver_dir / "references" / "pokemon_data.parquet"))

    print(f"source team member rows={len(members)}")
    print(f"source team member columns={list(members.columns)}")

    member_species_col = find_species_column(members)
    pokemon_species_col = "pokemon_species" if "pokemon_species" in pokemon.columns else find_species_column(pokemon)

    if member_species_col is None or pokemon_species_col is None:
        print("[ERROR] could not detect species columns")
        print(f"members columns={list(members.columns)}")
        print(f"pokemon columns={list(pokemon.columns)}")
        return False

    member_species = set(
        members[member_species_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )

    known_species = set(
        pokemon[pokemon_species_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )

    missing_species = sorted(member_species - known_species)

    if not missing_species:
        print("[OK] every Pokémon used in source_team_members exists in pokemon_data")
        return True

    print(
        f"[ERROR] {len(missing_species)} Pokémon used in source_team_members "
        f"are missing from pokemon_data"
    )
    print("\n".join(missing_species[:limit]))

    bad_rows = members[
        members[member_species_col].astype(str).str.strip().str.lower().isin(missing_species)
    ]

    display_cols = [
        col
        for col in [
            "__game_file",
            "source_team_id",
            "team_id",
            "member_id",
            "slot",
            member_species_col,
            "level",
            "origin",
        ]
        if col in bad_rows.columns
    ]

    print()
    print(f"First {limit} affected source_team_members rows:")
    print(bad_rows[display_cols].head(limit).to_string(index=False))

    return False


def validate_moveset_coverage(silver_dir: Path, limit: int) -> bool:
    print_section("simulation/member_moveset_combos_*.parquet coverage")

    simulation_dir = silver_dir / "simulation"
    movesets = load_game_split_files(simulation_dir, "member_moveset_combos")
    moves = normalize_null_like_values(read_parquet(simulation_dir / "move_data.parquet"))

    print(f"moveset rows={len(movesets)}")
    print(f"moveset columns={list(movesets.columns)}")

    if "move_name" not in moves.columns:
        print("[ERROR] move_data has no move_name column")
        return False

    move_cols = find_move_columns(movesets)
    if not move_cols:
        print("[ERROR] could not detect move columns in member_moveset_combos")
        print(f"columns={list(movesets.columns)}")
        return False

    known_moves = set(
        moves["move_name"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )

    used_moves = set()
    for col in move_cols:
        used_moves |= set(
            movesets[col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.lower()
        )

    missing_moves = sorted(used_moves - known_moves)

    if not missing_moves:
        print("[OK] every move used in member_moveset_combos exists in move_data")
        return True

    print(
        f"[ERROR] {len(missing_moves)} moves used in member_moveset_combos "
        f"are missing from move_data"
    )
    print("\n".join(missing_moves[:limit]))

    bad_mask = False
    for col in move_cols:
        bad_mask = bad_mask | movesets[col].astype(str).str.strip().str.lower().isin(missing_moves)

    bad_rows = movesets[bad_mask].copy()

    display_cols = [
        col
        for col in [
            "__game_file",
            "source_team_id",
            "team_id",
            "member_id",
            "slot",
            *move_cols,
        ]
        if col in bad_rows.columns
    ]

    print()
    print(f"First {limit} affected moveset rows:")
    print(bad_rows[display_cols].head(limit).to_string(index=False))

    return False


def validate_combat_pool_stats(silver_dir: Path, limit: int) -> bool:
    print_section("simulation/pokemon_combat_pool_*.parquet")

    simulation_dir = silver_dir / "simulation"
    combat_pool = load_game_split_files(simulation_dir, "pokemon_combat_pool")

    print(f"combat pool rows={len(combat_pool)}")
    print(f"combat pool columns={list(combat_pool.columns)}")

    stat_cols = [col for col in POKEMON_STAT_COLS if col in combat_pool.columns]
    species_col = find_species_column(combat_pool)

    if not stat_cols:
        print("[WARN] combat pool has no base stat columns; skipped")
        return True

    id_cols = ["__game_file"]
    if species_col:
        id_cols.append(species_col)

    ok = True
    ok &= check_nulls(
        df=combat_pool,
        cols=stat_cols,
        dataset="pokemon_combat_pool",
        id_cols=id_cols,
        limit=limit,
    )

    ok &= check_numeric_range(
        df=combat_pool,
        ranges={col: (1, 255) for col in stat_cols},
        dataset="pokemon_combat_pool",
        id_cols=id_cols,
        limit=limit,
    )

    return ok


def quick_show_null_pokemon(silver_dir: Path) -> None:
    print_section("QUICK VIEW: Pokémon with null base stats")

    path = silver_dir / "references" / "pokemon_data.parquet"
    df = normalize_null_like_values(read_parquet(path))

    existing_stats = [col for col in POKEMON_STAT_COLS if col in df.columns]
    bad = df[df[existing_stats].isna().any(axis=1)].copy()

    print(f"rows with null stats: {len(bad)}")

    if bad.empty:
        return

    cols = [
        col
        for col in ["pokemon_species", "name", "type_1", "type_2", *existing_stats]
        if col in bad.columns
    ]

    print(bad[cols].to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--fail-on-error", action="store_true")
    parser.add_argument(
        "--quick-null-pokemon",
        action="store_true",
        help="Only show Pokémon rows with null base stats.",
    )
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)

    if args.quick_null_pokemon:
        quick_show_null_pokemon(silver_dir)
        return

    ok = True
    ok &= validate_pokemon_data(silver_dir, args.limit)
    ok &= validate_move_data(silver_dir, args.limit)
    ok &= validate_source_team_member_coverage(silver_dir, args.limit)
    ok &= validate_moveset_coverage(silver_dir, args.limit)
    ok &= validate_combat_pool_stats(silver_dir, args.limit)

    print_section("SUMMARY")
    if ok:
        print("[OK] Silver validation passed")
    else:
        print("[ERROR] Silver validation found issues")

    if args.fail_on_error and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()