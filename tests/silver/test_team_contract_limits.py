from __future__ import annotations

import pytest

from src.pipeline.silver.schemas.contracts import TeamContract


def test_team_contract_rejects_more_than_six_members() -> None:
    with pytest.raises(ValueError, match="team exceeds max member limit"):
        TeamContract.from_payload(
            team_id="player_red_oversized",
            game_version="red",
            team_role="player_candidate",
            boss_name=None,
            gym=None,
            pokemon=["a", "b", "c", "d", "e", "f", "g"],
            levels=[10, 10, 10, 10, 10, 10, 10],
            moves=[["tackle"]] * 7,
            pokemon_instance_ids=[f"id_{idx}" for idx in range(7)],
            avg_level=10,
            is_player_candidate=True,
        ).validate()
