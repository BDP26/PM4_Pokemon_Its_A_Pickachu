from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

LEGACY_SILVER_DIR = ROOT_DIR / "pokemon_big_data_outputs"

BULBA_API = "https://bulbapedia.bulbagarden.net/w/api.php"
POKEAPI = "https://pokeapi.co/api/v2"


def ensure_medallion_dirs() -> None:
    for directory in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        directory.mkdir(parents=True, exist_ok=True)

