from __future__ import annotations

import pandas as pd

from src.pipeline.silver.orchestration.build_silver import _filter_bosses_with_encounter_pools


def test_filter_bosses_with_encounter_pools_drops_missing_pairs() -> None:
    bosses_df = pd.DataFrame(
        [
            {"game_version": "gold", "boss_id": "boss:gold:falkner:aaa", "boss_name_canonical": "Falkner"},
            {"game_version": "gold", "boss_id": "boss:gold:lt-surge:bbb", "boss_name_canonical": "Lt. Surge"},
            {"game_version": "blue", "boss_id": "boss:blue:misty:ccc", "boss_name_canonical": "Misty"},
        ]
    )
    encounters_df = pd.DataFrame(
        [
            {"game": "gold", "boss_id": "boss:gold:falkner:aaa", "pokemon": "pidgey"},
            {"game": "blue", "boss_id": "boss:blue:misty:ccc", "pokemon": "staryu"},
        ]
    )

    filtered = _filter_bosses_with_encounter_pools(bosses_df, encounters_df)

    assert filtered["boss_id"].tolist() == ["boss:gold:falkner:aaa", "boss:blue:misty:ccc"]


def test_filter_bosses_with_encounter_pools_keeps_input_when_encounters_empty() -> None:
    bosses_df = pd.DataFrame(
        [
            {"game_version": "gold", "boss_id": "boss:gold:falkner:aaa", "boss_name_canonical": "Falkner"},
            {"game_version": "gold", "boss_id": "boss:gold:lt-surge:bbb", "boss_name_canonical": "Lt. Surge"},
        ]
    )

    filtered = _filter_bosses_with_encounter_pools(bosses_df, pd.DataFrame())

    assert filtered.equals(bosses_df)

