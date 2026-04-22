"""Shared configuration for Silver team/moveset preparation."""
import os


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else minimum


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}

# Common moveset size used across team member and team-combination builders.
MOVESET_WIDTH = 4

# Map game keys to PokeAPI version-group names.
GAME_TO_VERSION_GROUP: dict[str, str] = {
    "red": "red-blue",
    "blue": "red-blue",
    "gold": "gold-silver",
    "silver": "gold-silver",
    "ruby": "ruby-sapphire",
    "sapphire": "ruby-sapphire",
    "diamond": "diamond-pearl",
    "pearl": "diamond-pearl",
    "black": "black-white",
    "white": "black-white",
    "x": "x-y",
    "y": "x-y",
}

# Species form fallbacks to improve PokeAPI slug resolution.
FORM_LOOKUP_FALLBACKS: dict[str, tuple[str, ...]] = {
    "meowstic": ("meowstic-male", "meowstic-female"),
    "pyroar": ("pyroar-male", "pyroar-female"),
    "jellicent": ("jellicent-male", "jellicent-female"),
    "gourgeist": ("gourgeist-average", "gourgeist-small", "gourgeist-large", "gourgeist-super"),
    "aegislash": ("aegislash-shield", "aegislash-blade"),
}

GENERIC_FORM_SUFFIXES: tuple[str, ...] = ("male", "female", "average", "normal")

# Moveset combination limits and safety checks.
DEFAULT_MEMBER_COMBO_LIMIT = _env_int("PM4_MEMBER_COMBO_LIMIT", 25)
# 0 means "no explicit cap"; effective truncation can still occur due to finite combination space.
DEFAULT_TEAM_VARIANT_LIMIT = _env_int("PM4_TEAM_VARIANT_LIMIT", 15000, minimum=0)
TEAM_VARIANT_CONFIRMATION_THRESHOLD = _env_int("PM4_TEAM_VARIANT_CONFIRMATION_THRESHOLD", 8000)
ALLOW_LARGE_TEAM_VARIANTS = _env_bool("PM4_ALLOW_LARGE_TEAM_VARIANTS", True)

# Species / team diversity controls
DEFAULT_CATCH_POOL_SIZE = _env_int("PM4_CATCH_POOL_SIZE", 5)
DEFAULT_SOURCE_TEAM_POOL_SIZE = _env_int("PM4_SOURCE_TEAM_POOL_SIZE", 12)
DEFAULT_SOURCE_TEAM_COMBO_LIMIT = _env_int("PM4_SOURCE_TEAM_COMBO_LIMIT", 40)

# Moveset expansion per team (VERY important balance lever). 0 disables this safety cap.
DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM = _env_int("PM4_MOVESET_VARIANT_LIMIT_PER_TEAM", 120, minimum=0)

# Team structure
DEFAULT_TEAM_MEMBER_LIMIT = _env_int("PM4_TEAM_MEMBER_LIMIT", 10)
DEFAULT_MEMBER_LEVEL = _env_int("PM4_MEMBER_LEVEL", 20)

KAGGLE_CSV_DELIMITER = ";"
CSV_PROGRESS_LOG_INTERVAL = 250

REQUIRED_MOVES_PROGRESS_INTERVAL = 100
SLOW_LOOKUP_WARNING_SECONDS = 2.0

SPECIES_SLUG_ALIASES: dict[str, str] = {
    "mr mime": "mr-mime",
    "mr. mime": "mr-mime",
    "mime jr": "mime-jr",
    "farfetch'd": "farfetchd",
    "nidoran f": "nidoran-f",
    "nidoran m": "nidoran-m",
}
