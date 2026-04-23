from __future__ import annotations

from pathlib import Path

from src.pipeline.silver.orchestration import stages


class _DummyMapper:
    misses: list[dict[str, str]] = []


def test_run_parse_stage_returns_typed_payload(monkeypatch, tmp_path: Path) -> None:
    game_file = tmp_path / "red.json"
    game_file.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(stages, "read_json", lambda _: {"game_key": "red", "bosses": ["Brock"]})
    monkeypatch.setattr(
        stages,
        "extract_game_data",
        lambda payload, mapper: [{"reachable_locations": ["route-1"], "boss_name": "brock"}],
    )
    monkeypatch.setattr(stages, "enforce_parser_coverage", lambda **_: None)
    monkeypatch.setattr(stages, "build_harmonized_candidates_by_boss", lambda **_: {"brock": []})
    monkeypatch.setattr(stages, "enrich_boss_records", lambda records, *_: records)
    monkeypatch.setattr(stages, "build_boss_mapping_payload", lambda *_, **__: {"brock": {"mapped": True}})

    result = stages.run_parse_stage(
        game_files=[game_file],
        mapper=_DummyMapper(),
        kaggle_rows_by_game={"red": []},
    )

    assert len(result.all_records) == 1
    assert result.all_slugs == ["route-1"]
    assert result.records_with_game_keys[0][0] == "red"
    assert "red" in result.boss_mapping_by_version
