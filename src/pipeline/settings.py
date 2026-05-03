from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

BRONZE_CONFIG_DIRNAME = "config"
BRONZE_MANIFESTS_DIRNAME = "manifests"
BRONZE_STATE_DIRNAME = "_state"
SILVER_SNAPSHOTS_DIRNAME = "snapshots"
SILVER_MAPPINGS_DIRNAME = "mappings"
SILVER_REFERENCES_DIRNAME = "references"
SILVER_DIAGNOSTICS_DIRNAME = "diagnostics"
SILVER_SIMULATION_DIRNAME = "simulation"
GOLD_SIMULATION_DIRNAME = "simulation"


BULBA_API = "https://bulbapedia.bulbagarden.net/w/api.php"

KAGGLE_GYM_LEADERS_DATASET = "maxiboo/pokemon-gen-1-9-gym-leaders-elite-four"
KAGGLE_GYM_LEADERS_FILE_PATH = ""

SIMULATION_CONFIG = {
    "max_overlevel": 2,
    "max_underlevel": 10,
    "default_trials": 4,
    "damage_randomness_min": 0.85,
    "damage_randomness_max": 1.00,
    "crit_chance": 0.0625,
    "max_turns_per_duel": 80,
}


def ensure_medallion_dirs() -> None:
    for directory in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def get_bronze_subdirs(base_dir: Path = BRONZE_DIR) -> dict[str, Path]:
    return {
        "config": base_dir / BRONZE_CONFIG_DIRNAME,
        "manifests": base_dir / BRONZE_MANIFESTS_DIRNAME,
        "state": base_dir / BRONZE_STATE_DIRNAME,
    }


def get_silver_subdirs(base_dir: Path = SILVER_DIR) -> dict[str, Path]:
    return {
        "snapshots": base_dir / SILVER_SNAPSHOTS_DIRNAME,
        "mappings": base_dir / SILVER_MAPPINGS_DIRNAME,
        "references": base_dir / SILVER_REFERENCES_DIRNAME,
        "diagnostics": base_dir / SILVER_DIAGNOSTICS_DIRNAME,
        "simulation": base_dir / SILVER_SIMULATION_DIRNAME,
    }


def get_gold_subdirs(base_dir: Path = GOLD_DIR) -> dict[str, Path]:
    return {
        "simulation": base_dir / GOLD_SIMULATION_DIRNAME,
    }
