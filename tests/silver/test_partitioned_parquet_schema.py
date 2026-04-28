from __future__ import annotations

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet


def test_partitioned_parquet_preserves_nullable_string_schema(tmp_path) -> None:
    path = tmp_path / "partitioned_strings.parquet"
    rows = pd.DataFrame(
        [
            {"game_version": "red", "boss_role": "gym", "label": "brock", "starter_condition": None},
            {"game_version": "black", "boss_role": "gym", "label": "chili", "starter_condition": "grass"},
        ]
    )

    write_parquet(path, rows, partition_cols=["game_version", "boss_role"])

    restored = read_parquet(path).sort_values(["game_version", "label"]).reset_index(drop=True)

    assert restored["game_version"].tolist() == ["black", "red"]
    assert restored["label"].tolist() == ["chili", "brock"]
    assert restored.loc[0, "starter_condition"] == "grass"
    assert pd.isna(restored.loc[1, "starter_condition"])
