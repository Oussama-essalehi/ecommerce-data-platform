"""Command line entry point: `python -m lakehouse <command>`."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .config import Config
from .session import build_session

PARIS = ZoneInfo("Europe/Paris")


def _yesterday() -> date:
    return datetime.now(PARIS).date() - timedelta(days=1)


def _print(summary: dict) -> None:
    """One JSON line per job run: easy to read in logs, easy to parse later."""
    print(json.dumps(summary, ensure_ascii=False, default=str), flush=True)


def cmd_bronze_api(args: argparse.Namespace, config: Config) -> int:
    from .bronze import api

    spark = build_session("bronze-api", config)
    entities = None if args.entity == "all" else [args.entity]
    for summary in api.run(spark, config, entities=entities, full=args.full,
                           lookback_minutes=args.lookback_minutes):
        _print(summary)
    return 0


def cmd_bronze_marketplace(args: argparse.Namespace, config: Config) -> int:
    from .bronze import marketplace

    if args.all and (args.date or args.start or args.until):
        raise SystemExit("--all cannot be combined with --date, --from or --until")
    if args.date and (args.start or args.until):
        raise SystemExit("--date cannot be combined with --from or --until")
    start = args.date or args.start or args.until or _yesterday()
    end = args.date or args.until or args.start or _yesterday()
    if start > end:
        raise SystemExit(f"--from {start} is after --until {end}")

    spark = build_session("bronze-marketplace", config)
    try:
        summary = marketplace.run(spark, config, start=start, end=end,
                                  all_files=args.all, skip_missing=args.skip_missing)
    except marketplace.MissingFilesError as error:
        logging.getLogger("lakehouse").error("%s", error)
        return 1
    _print(summary)
    return 0


def cmd_bronze_events(args: argparse.Namespace, config: Config) -> int:
    from .bronze import order_events

    spark = build_session("bronze-events", config)
    _print(order_events.run(spark, config, continuous=args.continuous,
                            max_offsets=args.max_offsets,
                            interval_seconds=args.interval_seconds))
    return 0


def cmd_status(args: argparse.Namespace, config: Config) -> int:
    from . import status

    spark = build_session("status", config)
    report = status.collect(spark, config)
    if args.json:
        _print({"bronze": report})
    else:
        print(status.render(report))
    return 0


def cmd_warmup(args: argparse.Namespace, config: Config) -> int:
    """Start Spark once so the Delta and Kafka jars are downloaded and cached."""
    spark = build_session("warmup", config)
    print(f"Spark {spark.version} is ready")
    spark.stop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lakehouse", description="Spark jobs of the e-commerce data platform."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    api = commands.add_parser("bronze-api", help="load customers and products from the Shop API")
    api.add_argument("--entity", choices=["all", "customers", "products"], default="all")
    api.add_argument("--full", action="store_true",
                     help="ask the API for everything instead of recent changes only")
    api.add_argument("--lookback-minutes", type=int, default=10,
                     help="safety margin subtracted from the newest updated_at (default: 10)")
    api.set_defaults(handler=cmd_bronze_api)

    marketplace = commands.add_parser(
        "bronze-marketplace",
        help="load marketplace CSV exports (default: yesterday's file)",
    )
    marketplace.add_argument("--date", type=date.fromisoformat, help="one day, YYYY-MM-DD")
    marketplace.add_argument("--from", dest="start", type=date.fromisoformat,
                             help="first day of a range")
    marketplace.add_argument("--until", type=date.fromisoformat, help="last day of a range")
    marketplace.add_argument("--all", action="store_true",
                             help="every file present in the landing zone")
    marketplace.add_argument("--skip-missing", action="store_true",
                             help="load what is there instead of failing on a missing day")
    marketplace.set_defaults(handler=cmd_bronze_marketplace)

    events = commands.add_parser("bronze-events", help="load order events from Kafka")
    events.add_argument("--continuous", action="store_true",
                        help="keep running instead of stopping once caught up")
    events.add_argument("--max-offsets", type=int, default=100_000,
                        help="largest micro-batch, in messages (default: 100000)")
    events.add_argument("--interval-seconds", type=int, default=15,
                        help="pause between micro-batches in continuous mode (default: 15)")
    events.set_defaults(handler=cmd_bronze_events)

    status = commands.add_parser("status", help="row counts and freshness of the bronze tables")
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=cmd_status)

    warmup = commands.add_parser("warmup", help="download and cache the Spark jars")
    warmup.set_defaults(handler=cmd_warmup)

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = build_parser().parse_args(argv)
    return args.handler(args, Config.from_env())
