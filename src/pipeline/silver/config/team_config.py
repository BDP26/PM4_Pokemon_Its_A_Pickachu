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
DEFAULT_MEMBER_MOVE_POOL_CAP = _env_int("PM4_MEMBER_MOVE_POOL_CAP", 12)
DEFAULT_MEMBER_COMBO_LIMIT = _env_int("PM4_MEMBER_COMBO_LIMIT", 64)
DEFAULT_TEAM_VARIANT_LIMIT = _env_int("PM4_TEAM_VARIANT_LIMIT", 1200)
TEAM_VARIANT_CONFIRMATION_THRESHOLD = _env_int("PM4_TEAM_VARIANT_CONFIRMATION_THRESHOLD", 5000)
ALLOW_LARGE_TEAM_VARIANTS = _env_bool("PM4_ALLOW_LARGE_TEAM_VARIANTS", False)

DEFAULT_TEAM_MEMBER_LIMIT = 6
DEFAULT_MEMBER_LEVEL = 20

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
