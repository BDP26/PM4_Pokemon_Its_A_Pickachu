"""Explicit schema contracts for Silver layer outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from src.pipeline.silver.config.team_config import DEFAULT_TEAM_MEMBER_LIMIT
from src.pipeline.silver.transforms.keys import (
    make_boss_snapshot_id,
    make_pokemon_instance_id,
    normalize_key_part,
)


@dataclass(frozen=True)
class BossSnapshotContract:
    snapshot_id: str
    boss_id: str
    boss_slug: str
    boss_name: str
    game: str
    version: str
    boss_order: int
    heading: str
    part: str
    reachable_location_count: int
    reachable_locations: list[str] = field(default_factory=list)
    reachable_pokemon_count: int = 0

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "BossSnapshotContract":
        boss_id = str(record.get("boss_id") or "").strip()
        version = str(record.get("version") or record.get("game") or "").strip().lower()
        return cls(
            snapshot_id=make_boss_snapshot_id(version, boss_id, record.get("part"), record.get("heading")),
            boss_id=boss_id,
            boss_slug=str(record.get("boss_slug") or "").strip(),
            boss_name=str(record.get("boss_name_canonical") or record.get("boss_name") or "").strip(),
            game=str(record.get("game") or "").strip(),
            version=version,
            boss_order=int(record.get("boss_order") or 0),
            heading=str(record.get("heading") or "").strip(),
            part=str(record.get("part") or "").strip(),
            reachable_location_count=int(record.get("location_count") or record.get("reachable_location_count") or 0),
            reachable_locations=[str(value).strip() for value in record.get("reachable_locations", []) if str(value).strip()],
            reachable_pokemon_count=int(record.get("reachable_pokemon_count") or 0),
        )

    def validate(self) -> None:
        if not self.snapshot_id:
            raise ValueError("boss snapshot_id is required")
        if not self.boss_id:
            raise ValueError("boss_id is required")
        if not self.version:
            raise ValueError("version is required")
        if self.boss_order <= 0:
            raise ValueError(f"boss_order must be positive for boss_id={self.boss_id}")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TeamMemberContract:
    pokemon_instance_id: str
    name: str
    level: int
    moves: list[str]
    origin: str

    @classmethod
    def build(
        cls,
        team_id: str,
        slot_index: int,
        species: str,
        level: int,
        moves: list[str],
        origin: str,
    ) -> "TeamMemberContract":
        return cls(
            pokemon_instance_id=make_pokemon_instance_id(team_id, slot_index, species),
            name=normalize_key_part(species),
            level=int(level),
            moves=[normalize_key_part(move) for move in moves if str(move).strip()],
            origin=str(origin).strip(),
        )


@dataclass(frozen=True)
class TeamContract:
    team_id: str
    game_version: str
    team_role: str
    boss_name: str | None
    gym: str | None
    pokemon: list[str]
    levels: list[int]
    moves: list[list[str]]
    pokemon_instance_ids: list[str]
    avg_level: int
    is_player_candidate: bool
    source_team_id: str | None = None
    starter_base: str | None = None
    starter_evolved_species: str | None = None

    def validate(self) -> None:
        if not self.team_id:
            raise ValueError("team_id is required")
        if not self.game_version:
            raise ValueError(f"game_version is required for team_id={self.team_id}")
        if len(self.pokemon) != len(self.levels) or len(self.pokemon) != len(self.moves):
            raise ValueError(f"team arrays must be aligned for team_id={self.team_id}")
        if self.pokemon_instance_ids and len(self.pokemon_instance_ids) != len(self.pokemon):
            raise ValueError(f"pokemon_instance_ids must align with pokemon for team_id={self.team_id}")
        if not self.pokemon:
            raise ValueError(f"team must contain at least one pokemon for team_id={self.team_id}")
        if len(self.pokemon) > DEFAULT_TEAM_MEMBER_LIMIT:
            raise ValueError(
                f"team exceeds max member limit ({DEFAULT_TEAM_MEMBER_LIMIT}) for team_id={self.team_id}"
            )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_payload(
        cls,
        *,
        team_id: str,
        game_version: str,
        team_role: str,
        boss_name: str | None,
        gym: str | None,
        pokemon: list[str],
        levels: list[int],
        moves: list[list[str]],
        pokemon_instance_ids: list[str],
        avg_level: int,
        is_player_candidate: bool,
        source_team_id: str | None = None,
        starter_base: str | None = None,
        starter_evolved_species: str | None = None,
    ) -> "TeamContract":
        return cls(
            team_id=team_id,
            game_version=game_version,
            team_role=team_role,
            boss_name=boss_name,
            gym=gym,
            pokemon=[normalize_key_part(name) for name in pokemon],
            levels=[int(level) for level in levels],
            moves=[[normalize_key_part(move) for move in member_moves if str(move).strip()] for member_moves in moves],
            pokemon_instance_ids=[str(value).strip() for value in pokemon_instance_ids],
            avg_level=int(avg_level),
            is_player_candidate=bool(is_player_candidate),
            source_team_id=source_team_id,
            starter_base=starter_base,
            starter_evolved_species=starter_evolved_species,
        )


def validate_boss_snapshots(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for record in records:
        snapshot = BossSnapshotContract.from_record(record)
        snapshot.validate()
        validated.append(snapshot.as_dict())
    return validated


def validate_team_payloads(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for record in records:
        team = TeamContract.from_payload(**record)
        team.validate()
        validated.append(team.as_dict())
    return validated


