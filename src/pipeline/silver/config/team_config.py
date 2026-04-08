"""Shared configuration for Silver team/moveset preparation."""

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

# Moveset combination limits - increased to handle Pokemon with many moves (e.g., escavalier)
DEFAULT_MEMBER_MOVE_POOL_CAP = 20  # Up from 12: allow up to 20 learnable moves before filtering
DEFAULT_MEMBER_COMBO_LIMIT = 500  # Up from 128: allow up to 500 move combinations per member

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


