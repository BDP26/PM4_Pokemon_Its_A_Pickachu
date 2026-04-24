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

# Silver stores compact logical teams/members and per-member moveset combos only.
# Full-team expansion/sampling is deferred to Gold/simulation.
DEFAULT_MEMBER_MOVESET_COMBO_LIMIT = _env_int(
    "PM4_MEMBER_MOVESET_COMBO_LIMIT",
    _env_int("PM4_MEMBER_COMBO_LIMIT", 10),
)
DEFAULT_MEMBER_COMBO_LIMIT = DEFAULT_MEMBER_MOVESET_COMBO_LIMIT
# Deprecated in Silver (kept for compatibility until callers migrate).
DEFAULT_TEAM_VARIANT_LIMIT = _env_int("PM4_TEAM_VARIANT_LIMIT", 100, minimum=0)
TEAM_VARIANT_CONFIRMATION_THRESHOLD = _env_int("PM4_TEAM_VARIANT_CONFIRMATION_THRESHOLD", 8000)
ALLOW_LARGE_TEAM_VARIANTS = _env_bool("PM4_ALLOW_LARGE_TEAM_VARIANTS", True)

# Compact Silver move option controls.
DEFAULT_MEMBER_MOVE_OPTION_LIMIT = _env_int("PM4_MEMBER_MOVE_OPTION_LIMIT", 8)

# Species / team diversity controls
DEFAULT_CATCH_POOL_SIZE = _env_int("PM4_CATCH_POOL_SIZE", 5)
DEFAULT_SOURCE_TEAM_POOL_SIZE = _env_int("PM4_SOURCE_TEAM_POOL_SIZE", 12)
DEFAULT_SOURCE_TEAM_COMBO_LIMIT = _env_int("PM4_SOURCE_TEAM_COMBO_LIMIT", 40)

# Deprecated in Silver. Use Gold simulation sampling limits instead.
DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM = _env_int("PM4_MOVESET_VARIANT_LIMIT_PER_TEAM", 30, minimum=0)
DEFAULT_SIMULATION_TEAM_SAMPLE_LIMIT = _env_int("PM4_SIMULATION_TEAM_SAMPLE_LIMIT", 64, minimum=1)
DEFAULT_SIMULATION_ENUMERATION_THRESHOLD = _env_int("PM4_SIMULATION_ENUMERATION_THRESHOLD", 256, minimum=1)

# Team structure
# Pokémon battle teams are capped at six members in core games.
DEFAULT_TEAM_MEMBER_LIMIT = _env_int("PM4_TEAM_MEMBER_LIMIT", 6)
DEFAULT_MEMBER_LEVEL = _env_int("PM4_MEMBER_LEVEL", 20)

KAGGLE_CSV_DELIMITER = ";"
CSV_PROGRESS_LOG_INTERVAL = 250
PLAYER_TEAM_PROGRESS_LOG_INTERVAL = _env_int("PM4_PLAYER_TEAM_PROGRESS_LOG_INTERVAL", 100)
PARSER_MIN_BOSS_COVERAGE = max(0.0, min(1.0, float(os.getenv("PM4_PARSER_MIN_BOSS_COVERAGE", "0.85"))))

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


def resolve_runtime_team_config() -> dict[str, object]:
    return {
        "member_combo_limit": DEFAULT_MEMBER_COMBO_LIMIT,
        "member_moveset_combo_limit": DEFAULT_MEMBER_MOVESET_COMBO_LIMIT,
        "team_variant_limit": DEFAULT_TEAM_VARIANT_LIMIT,
        "team_variant_confirmation_threshold": TEAM_VARIANT_CONFIRMATION_THRESHOLD,
        "allow_large_team_variants": ALLOW_LARGE_TEAM_VARIANTS,
        "catch_pool_size": DEFAULT_CATCH_POOL_SIZE,
        "source_team_pool_size": DEFAULT_SOURCE_TEAM_POOL_SIZE,
        "source_team_combo_limit": DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
        "moveset_variant_limit_per_team": DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM,
        "simulation_team_sample_limit": DEFAULT_SIMULATION_TEAM_SAMPLE_LIMIT,
        "simulation_enumeration_threshold": DEFAULT_SIMULATION_ENUMERATION_THRESHOLD,
        "member_move_option_limit": DEFAULT_MEMBER_MOVE_OPTION_LIMIT,
        "team_member_limit": DEFAULT_TEAM_MEMBER_LIMIT,
        "member_level": DEFAULT_MEMBER_LEVEL,
        "parser_min_boss_coverage": PARSER_MIN_BOSS_COVERAGE,
    }
