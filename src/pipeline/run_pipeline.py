import argparse

from src.pipeline.bronze.fetch_sources import fetch_bronze_sources
from src.pipeline.gold.build_gold import build_gold_from_silver
from src.pipeline.silver.bootstrap_legacy import bootstrap_legacy_silver
from src.pipeline.silver.build_silver import build_silver_from_bronze


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Pokemon medallion pipeline layers")
    parser.add_argument(
        "--layer",
        choices=["bootstrap-silver", "bronze", "silver", "gold", "all"],
        required=True,
        help="Layer to execute",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.layer == "bootstrap-silver":
        bootstrap_legacy_silver()
    elif args.layer == "bronze":
        fetch_bronze_sources()
    elif args.layer == "silver":
        build_silver_from_bronze()
    elif args.layer == "gold":
        build_gold_from_silver()
    elif args.layer == "all":
        fetch_bronze_sources()
        build_silver_from_bronze()
        build_gold_from_silver()



if __name__ == "__main__":
    fetch_bronze_sources()
    #main()

