"""Compatibility shim for moved Gold battle seed generation."""

from __future__ import annotations

import logging
from pathlib import Path

from src.pipeline.gold.simulation.battle_seeds import build_battle_seeds as _gold_build_battle_seeds
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME

logger = logging.getLogger(__name__)


def build_battle_seeds(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
) -> None:
    logger.warning("[gold/simulation] deprecated silver simulation path used")
    _gold_build_battle_seeds(gold_dir=silver_dir, simulation_dirname=simulation_dirname, silver_dir=silver_dir)
