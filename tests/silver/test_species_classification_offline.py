from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import write_parquet
from src.pipeline.silver.inputs import species_classification


def test_get_species_classification_uses_persisted_pokemon_data(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {"pokemon_species": "mewtwo", "is_legendary": True, "is_mythical": False},
            {"pokemon_species": "mew", "is_legendary": False, "is_mythical": True},
            {"pokemon_species": "pikachu", "is_legendary": False, "is_mythical": False},
        ],
    )

    monkeypatch.setattr(species_classification, "SILVER_DIR", silver_dir)
    species_classification._persisted_species_classification.cache_clear()
    species_classification.get_species_classification.cache_clear()

    assert species_classification.get_species_classification("Mewtwo") == {
        "is_legendary": True,
        "is_mythical": False,
    }
    assert species_classification.get_species_classification("mew") == {
        "is_legendary": False,
        "is_mythical": True,
    }
    assert species_classification.get_species_classification("pikachu") == {
        "is_legendary": False,
        "is_mythical": False,
    }


def test_get_species_classification_defaults_false_when_species_missing(monkeypatch) -> None:
    species_classification._persisted_species_classification.cache_clear()
    species_classification.get_species_classification.cache_clear()
    monkeypatch.setattr(species_classification, "_persisted_species_classification", lambda: {})

    assert species_classification.get_species_classification("audino") == {
        "is_legendary": False,
        "is_mythical": False,
    }


def test_get_species_classification_keeps_null_flags_as_non_restricted_defaults(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {"pokemon_species": "reshiram", "is_legendary": None, "is_mythical": None},
            {"pokemon_species": "pikachu", "is_legendary": None, "is_mythical": None},
        ],
    )

    monkeypatch.setattr(species_classification, "SILVER_DIR", silver_dir)
    species_classification._persisted_species_classification.cache_clear()
    species_classification.get_species_classification.cache_clear()

    assert species_classification.get_species_classification("reshiram") == {
        "is_legendary": False,
        "is_mythical": False,
    }
    assert species_classification.get_species_classification("pikachu") == {
        "is_legendary": False,
        "is_mythical": False,
    }
