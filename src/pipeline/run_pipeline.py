import argparse
import logging
import os
import subprocess
import sys
import warnings
from pathlib import Path

# Suppress tqdm IProgress warning
warnings.filterwarnings("ignore", message=".*IProgress not found.*")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _configure_ssl_certificates() -> None:
    """
    Ensure HTTPS clients can find a CA bundle on macOS/Homebrew/Python.org setups
    where OpenSSL default cert paths are not present.
    """
    if os.getenv("SSL_CERT_FILE") and Path(os.environ["SSL_CERT_FILE"]).exists():
        return

    try:
        import certifi
    except Exception:
        return

    cert_path = certifi.where()
    if cert_path and Path(cert_path).exists():
        os.environ.setdefault("SSL_CERT_FILE", cert_path)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", cert_path)
        os.environ.setdefault("CURL_CA_BUNDLE", cert_path)


def _run_silver_with_contract_gate(*, hard_cleanup: bool) -> None:
    from src.pipeline.silver.orchestration.build_silver import build_silver_from_bronze

    build_silver_from_bronze(hard_cleanup=hard_cleanup)
    project_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = (
        f"{project_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(project_root)
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.pipeline.silver.validation.validate_silver_contract",
            "--fail-on-error",
        ],
        check=False,
        cwd=str(project_root),
        env=env,
    )
    if result.returncode != 0:
        raise SystemExit(result.returncode)


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
    layer_parser.add_argument(
        "--hard-cleanup",
        action="store_true",
        help="When silver is selected, remove deprecated artifacts before building outputs",
    )
    return parser.parse_args()


def main() -> None:
    _configure_ssl_certificates()
    args = parse_args()

    def _run_bronze() -> None:
        from src.pipeline.bronze.orchestration.fetch_sources import fetch_bronze_sources

        fetch_bronze_sources()

    def _run_gold() -> None:
        from src.pipeline.gold.orchestration.build_gold import build_gold_from_silver

        build_gold_from_silver()

    layer_runners = {
        "bronze": _run_bronze,
        "silver": lambda: _run_silver_with_contract_gate(
            hard_cleanup=bool(getattr(args, "hard_cleanup", False))
        ),
        "gold": _run_gold,
    }

    if args.mode == "all":
        selected_layers = ["bronze", "silver", "gold"]
    else:
        selected_layers = args.names

    deduped_layers = list(dict.fromkeys(selected_layers))
    for layer in deduped_layers:
        layer_runners[layer]()



if __name__ == "__main__":
    main()
