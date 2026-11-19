"""Command-line entry point."""
import argparse
import json
from datetime import date
from pathlib import Path

from .api import Client
from .pipeline import REPORTS, benchmark, load_cached, report, sync


def main():
    parser = argparse.ArgumentParser(description="Build and query a local ClinicalTrials.gov warehouse.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--condition", default="Type 1 Diabetes")
    scope.add_argument("--all", action="store_true", help="Use the full registry cohort.")
    commands = parser.add_subparsers(dest="command", required=True)
    ingestion = commands.add_parser("sync", help="Land and load the latest available snapshot.")
    ingestion.add_argument("--since", type=date.fromisoformat, help="Inclusive bootstrap date (YYYY-MM-DD).")
    ingestion.add_argument("--page-size", type=int)
    ingestion.add_argument("--bronze-only", action="store_true", help="Download and cache without transforming.")
    ingestion.add_argument("--engine", choices=("spark", "duckdb"),
                           help="Defaults to Spark for --all and DuckDB for condition searches.")
    cached = commands.add_parser("load", help="Load a cached bronze snapshot without network access.")
    cached.add_argument("bronze", type=Path)
    cached.add_argument("--engine", choices=("spark", "duckdb"),
                        help="Defaults to Spark for --all and DuckDB for condition searches.")
    comparison = commands.add_parser("benchmark", help="Compare both silver engines on the same cached pages.")
    comparison.add_argument("bronze", type=Path)
    reports = commands.add_parser("report", help="Execute a warehouse SQL report.")
    reports.add_argument("name", choices=REPORTS)
    args = parser.parse_args()

    condition = None if args.all else args.condition.strip()
    default_engine = "spark" if args.all else "duckdb"
    if condition == "":
        parser.error("--condition must contain a search term")
    if args.command == "sync":
        page_size = args.page_size if args.page_size is not None else (1000 if args.all else 100)
        if not 1 <= page_size <= 1000:
            ingestion.error("--page-size must be between 1 and 1000")
        if args.all and args.since:
            ingestion.error("--all always starts with a complete registry bootstrap")
        result = sync(Client(), args.data_dir, condition, page_size, args.since, args.bronze_only,
                      args.engine or default_engine)
    elif args.command == "load":
        result = load_cached(args.data_dir, condition, args.bronze, args.engine or default_engine)
    elif args.command == "benchmark":
        result = benchmark(args.bronze)
    else:
        result = report(args.data_dir, condition, args.name)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
