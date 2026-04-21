from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


def validate_reconstructed_teams(teams: list[dict[str, Any]], strict: bool = True) -> list[str]:
    errors: list[str] = []
    seen_team_ids: set[str] = set()

    for index, team in enumerate(teams):
        prefix = f"[team_validation] team_index={index}"

        team_id = str(team.get("team_id") or "").strip()
        if not team_id:
            errors.append(f"{prefix} missing team_id")
            continue

        if team_id in seen_team_ids:
            errors.append(f"{prefix} duplicate team_id={team_id}")
        seen_team_ids.add(team_id)

        pokemon = team.get("pokemon", [])
        levels = team.get("levels", [])
        moves = team.get("moves", [])
        instance_ids = team.get("pokemon_instance_ids", [])

        if not isinstance(pokemon, list) or not pokemon:
            errors.append(f"{prefix} team_id={team_id} missing pokemon list")
            continue

        if not isinstance(levels, list):
            errors.append(f"{prefix} team_id={team_id} levels is not a list")
            levels = []

        if not isinstance(moves, list):
            errors.append(f"{prefix} team_id={team_id} moves is not a list")
            moves = []

        if not isinstance(instance_ids, list):
            errors.append(f"{prefix} team_id={team_id} pokemon_instance_ids is not a list")
            instance_ids = []

        if len(pokemon) != len(levels):
            errors.append(
                f"{prefix} team_id={team_id} pokemon/levels length mismatch pokemon={len(pokemon)} levels={len(levels)}"
            )

        if moves and len(pokemon) != len(moves):
            errors.append(
                f"{prefix} team_id={team_id} pokemon/moves length mismatch pokemon={len(pokemon)} moves={len(moves)}"
            )

        if instance_ids and len(pokemon) != len(instance_ids):
            errors.append(
                f"{prefix} team_id={team_id} pokemon/instance_ids length mismatch pokemon={len(pokemon)} ids={len(instance_ids)}"
            )

        if len(pokemon) > 6:
            errors.append(f"{prefix} team_id={team_id} has more than 6 active members count={len(pokemon)}")

        for slot, species in enumerate(pokemon, start=1):
            if not isinstance(species, str) or not species.strip():
                errors.append(f"{prefix} team_id={team_id} empty species at slot={slot}")

        for slot, level in enumerate(levels, start=1):
            try:
                numeric_level = int(level)
            except Exception:
                errors.append(f"{prefix} team_id={team_id} invalid level={level!r} at slot={slot}")
                continue
            if numeric_level < 1:
                errors.append(f"{prefix} team_id={team_id} level < 1 at slot={slot}: {numeric_level}")

        for slot, member_moves in enumerate(moves, start=1):
            if not isinstance(member_moves, list):
                errors.append(f"{prefix} team_id={team_id} moves for slot={slot} is not a list")
                continue
            move_counter = Counter(str(move).strip().lower() for move in member_moves if str(move).strip())
            duplicates = [move for move, count in move_counter.items() if count > 1]
            if duplicates:
                errors.append(f"{prefix} team_id={team_id} duplicate moves at slot={slot}: {sorted(duplicates)}")

        boss_name = team.get("boss_name")
        is_player_candidate = bool(team.get("is_player_candidate", False))
        if boss_name and is_player_candidate:
            errors.append(f"{prefix} team_id={team_id} marked as both boss and player candidate")

    if strict and errors:
        raise ValueError("Reconstructed team validation failed:\n" + "\n".join(errors[:200]))
    return errors


def validate_normalized_team_rows(
    teams_rows: list[dict[str, Any]],
    team_member_rows: list[dict[str, Any]],
    team_member_move_rows: list[dict[str, Any]],
    strict: bool = True,
) -> list[str]:
    errors: list[str] = []

    team_ids = {str(row.get("team_id") or "").strip() for row in teams_rows if str(row.get("team_id") or "").strip()}
    members_by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_member_ids: set[str] = set()

    for row in team_member_rows:
        member_id = str(row.get("team_member_id") or "").strip()
        team_id = str(row.get("team_id") or "").strip()
        slot = row.get("slot")

        if not member_id:
            errors.append("[normalized_validation] missing team_member_id in team_members row")
            continue
        if member_id in seen_member_ids:
            errors.append(f"[normalized_validation] duplicate team_member_id={member_id}")
        seen_member_ids.add(member_id)

        if not team_id:
            errors.append(f"[normalized_validation] team_member_id={member_id} missing team_id")
            continue
        if team_ids and team_id not in team_ids:
            errors.append(f"[normalized_validation] team_member_id={member_id} references unknown team_id={team_id}")

        try:
            slot_num = int(slot)
            if slot_num < 1:
                errors.append(f"[normalized_validation] team_member_id={member_id} invalid slot={slot_num}")
        except Exception:
            errors.append(f"[normalized_validation] team_member_id={member_id} invalid slot={slot!r}")
            continue

        members_by_team[team_id].append(row)

    for team_id, members in members_by_team.items():
        slot_counter = Counter(int(member.get("slot") or 0) for member in members)
        dup_slots = [slot for slot, count in slot_counter.items() if count > 1]
        if dup_slots:
            errors.append(f"[normalized_validation] team_id={team_id} duplicate member slots={sorted(dup_slots)}")

    seen_move_keys: set[tuple[str, int]] = set()
    for row in team_member_move_rows:
        member_id = str(row.get("team_member_id") or "").strip()
        move_slot = row.get("move_slot")
        move_name = str(row.get("move_name") or "").strip().lower()

        if not member_id:
            errors.append("[normalized_validation] missing team_member_id in team_member_moves row")
            continue
        if member_id not in seen_member_ids:
            errors.append(f"[normalized_validation] team_member_moves references unknown member_id={member_id}")

        try:
            move_slot_num = int(move_slot)
            if move_slot_num < 1:
                errors.append(f"[normalized_validation] member_id={member_id} invalid move_slot={move_slot_num}")
                continue
        except Exception:
            errors.append(f"[normalized_validation] member_id={member_id} invalid move_slot={move_slot!r}")
            continue

        if not move_name:
            errors.append(f"[normalized_validation] member_id={member_id} empty move_name at move_slot={move_slot_num}")
            continue

        key = (member_id, move_slot_num)
        if key in seen_move_keys:
            errors.append(f"[normalized_validation] duplicate move slot for member_id={member_id} move_slot={move_slot_num}")
        seen_move_keys.add(key)

    if strict and errors:
        raise ValueError("Normalized team row validation failed:\n" + "\n".join(errors[:200]))
    return errors