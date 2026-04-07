import argparse
import logging
import sys
import time

from src.pipeline.bronze.orchestration.fetch_sources import fetch_bronze_sources
from src.pipeline.gold.orchestration.build_gold import build_gold_from_silver
from src.pipeline.gold.simulation.validate_simulation import main as validate_simulation_main
from src.pipeline.silver.orchestration.build_silver import build_silver_from_bronze


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Pokemon medallion pipeline layers")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    subparsers.add_parser(
        "all",
        help="Run all layers in order: bronze -> silver -> gold",
    )
    all_parser = subparsers.choices["all"]
    all_parser.add_argument(
        "--hard-cleanup",
        action="store_true",
        help="When silver runs, remove deprecated artifacts before building outputs",
    )

    layer_parser = subparsers.add_parser(
        "layers",
        help="Run one or more specific layers",
    )
    layer_parser.add_argument(
        "names",
        nargs="+",
        choices=["bronze", "silver", "gold"],
        help="Layer names to execute in the provided order",
    )
    layer_parser.add_argument(
        "--hard-cleanup",
        action="store_true",
        help="When silver is selected, remove deprecated artifacts before building outputs",
    )

    subparsers.add_parser(
        "validate-simulation",
        help="Run smoke checks for data/gold/simulation artifacts",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info("[pipeline] mode=%s", args.mode)

    if args.mode == "validate-simulation":
        sys.exit(validate_simulation_main())

    hard_cleanup = bool(getattr(args, "hard_cleanup", False))
    layer_runners = {
        "bronze": fetch_bronze_sources,
        "silver": lambda: build_silver_from_bronze(hard_cleanup=hard_cleanup),
        "gold": build_gold_from_silver,
    }

    if args.mode == "all":
        selected_layers = ["bronze", "silver", "gold"]
    else:
        selected_layers = args.names

    deduped_layers = list(dict.fromkeys(selected_layers))
    total = len(deduped_layers)
    for index, layer in enumerate(deduped_layers, start=1):
        started_at = time.perf_counter()
        logger.info("[pipeline] (%s/%s) starting layer=%s", index, total, layer)
        layer_runners[layer]()
        elapsed = time.perf_counter() - started_at
        logger.info("[pipeline] (%s/%s) finished layer=%s in %.2fs", index, total, layer, elapsed)



if __name__ == "__main__":
    main()

