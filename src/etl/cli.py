"""CLI entry point: `python -m etl run <config.yaml>`."""

from __future__ import annotations

import argparse
import sys

from etl.pipeline import run_config
from etl.transform import list_transforms


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="etl", description="Run YAML-configured ETL pipelines.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run a pipeline from a YAML config")
    run.add_argument("config", help="Path to YAML config")

    sub.add_parser("transforms", help="List built-in transform types")

    args = parser.parse_args(argv)

    if args.cmd == "run":
        report = run_config(args.config)
        print(report.format())
        return 0
    if args.cmd == "transforms":
        for name in list_transforms():
            print(name)
        return 0
    return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
