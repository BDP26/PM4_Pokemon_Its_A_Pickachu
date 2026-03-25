import argparse

from src.pipeline.bronze.fetch_sources import fetch_bronze_sources
from src.pipeline.gold.build_gold import build_gold_from_silver
from src.pipeline.silver.build_silver import build_silver_from_bronze


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Pokemon medallion pipeline layers")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    subparsers.add_parser(
        "all",
        help="Run all layers in order: bronze -> silver -> gold",
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    layer_runners = {
        "bronze": fetch_bronze_sources,
        "silver": build_silver_from_bronze,
        "gold": build_gold_from_silver,
    }

    if args.mode == "all":
        selected_layers = ["bronze", "silver", "gold"]
    else:
        selected_layers = args.names

    for layer in dict.fromkeys(selected_layers):
        layer_runners[layer]()



if __name__ == "__main__":
    main()

