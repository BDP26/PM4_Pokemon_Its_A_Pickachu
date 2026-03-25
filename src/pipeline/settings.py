from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

TYPE_CHART_CSV_PATH = BRONZE_DIR / "type_chart.csv"
TYPE_CHART_JSON_PATH = BRONZE_DIR / "type_chart.json"


BULBA_API = "https://bulbapedia.bulbagarden.net/w/api.php"
POKEAPI = "https://pokeapi.co/api/v2"

KAGGLE_GYM_LEADERS_DATASET = "maxiboo/pokemon-gen-1-9-gym-leaders-elite-four"
KAGGLE_GYM_LEADERS_FILE_PATH = ""


def ensure_medallion_dirs() -> None:
    for directory in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        directory.mkdir(parents=True, exist_ok=True)

