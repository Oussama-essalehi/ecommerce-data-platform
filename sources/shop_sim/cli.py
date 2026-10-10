"""Command line entry point: `python -m shop_sim <command>`."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from .csv_export import export_range
from .events import EventStream
from .orders import OrderBook
from .producer import Checkpoint, run
from .settings import PARIS, Settings
from .sinks import KafkaSink, LineSink
from .world import World


def _yesterday() -> date:
    return datetime.now(PARIS).date() - timedelta(days=1)


def cmd_api(args: argparse.Namespace, settings: Settings) -> int:
    import uvicorn

    from .api import create_app

    uvicorn.run(create_app(settings), host=args.host, port=args.port)
    return 0


def _export_once(args: argparse.Namespace, settings: Settings, book: OrderBook) -> int:
    """Write the missing export files up to yesterday. Returns how many were written."""
    start = args.start or settings.start_date
    end = args.until or _yesterday()
    out_dir = args.out or settings.landing_dir
    results = export_range(book, start, end, out_dir, force=args.force)
    written = [r for r in results if r.written]
    if written or not args.watch:
        print(
            f"{len(written)} files written to {out_dir} "
            f"({sum(r.rows for r in written):,} rows), "
            f"{len(results) - len(written)} already there",
            flush=True,
        )
    return len(written)


def cmd_export_csv(args: argparse.Namespace, settings: Settings) -> int:
    book = OrderBook(World(settings))
    if not args.watch:
        _export_once(args, settings, book)
        return 0

    # Stand-in for the partner's nightly job: each time a day ends, its file
    # appears. Checking every few minutes is enough, and writing is
    # idempotent, so a restart never duplicates or skips a file.
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"watching: a new export is written every day, checked every {args.interval:g}s",
          flush=True)
    while not stopping:
        _export_once(args, settings, book)
        waited = 0.0
        while waited < args.interval and not stopping:
            time.sleep(min(1.0, args.interval - waited))
            waited += 1.0
    return 0


def cmd_produce(args: argparse.Namespace, settings: Settings) -> int:
    log = logging.getLogger("shop_sim")
    checkpoint = Checkpoint(settings.state_dir / f"producer_{args.sink}.json")
    if args.reset:
        checkpoint.clear()

    if args.sink == "kafka":
        sink = KafkaSink(settings.kafka_bootstrap, settings.kafka_topic)
        log.info("publishing to topic %s on %s", settings.kafka_topic, settings.kafka_bootstrap)
    elif args.sink == "file":
        sink = LineSink(args.out or settings.state_dir / "events.jsonl")
    else:
        sink = LineSink()

    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    stream = EventStream(OrderBook(World(settings)))
    try:
        run(
            stream,
            sink,
            checkpoint,
            follow=not args.once,
            backfill=not args.from_now,
            tick_seconds=args.tick,
            should_stop=lambda: stopping,
        )
    finally:
        sink.close()
    return 0


def cmd_summary(args: argparse.Namespace, settings: Settings) -> int:
    end = args.until or _yesterday()
    start = args.start or max(settings.start_date, end - timedelta(days=6))
    world = World(settings)
    book = OrderBook(world)
    print(f"seed={settings.seed}  customers={len(world.customers):,}  products={len(world.products)}")
    print(f"{'day':<12}{'orders':>8}{'marketplace':>13}{'web':>7}{'mobile':>8}{'revenue EUR':>14}")
    day = start
    while day <= end:
        orders = book.day(day)
        channels = Counter(order.channel for order in orders)
        revenue = sum(order.total_amount for order in orders)
        print(
            f"{day.isoformat():<12}{len(orders):>8}{channels['marketplace']:>13}"
            f"{channels['web']:>7}{channels['mobile']:>8}{revenue:>14,.2f}"
        )
        day += timedelta(days=1)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shop_sim", description="Simulated source systems of the e-commerce platform."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    api = commands.add_parser("api", help="serve customers and products over HTTP")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8000)
    api.set_defaults(handler=cmd_api)

    export = commands.add_parser("export-csv", help="write the marketplace daily CSV exports")
    export.add_argument("--from", dest="start", type=date.fromisoformat,
                        help="first day, YYYY-MM-DD (default: SIM_START_DATE)")
    export.add_argument("--until", type=date.fromisoformat,
                        help="last day, YYYY-MM-DD (default: yesterday)")
    export.add_argument("--out", type=Path, help="output folder (default: SIM_LANDING_DIR)")
    export.add_argument("--force", action="store_true", help="overwrite existing files")
    export.add_argument("--watch", action="store_true",
                        help="keep running and write each new day's file as it becomes due")
    export.add_argument("--interval", type=float, default=300.0,
                        help="seconds between two checks with --watch (default: 300)")
    export.set_defaults(handler=cmd_export_csv)

    produce = commands.add_parser("produce", help="publish web shop order events")
    produce.add_argument("--sink", choices=["kafka", "stdout", "file"], default="kafka")
    produce.add_argument("--out", type=Path, help="target file when --sink file")
    produce.add_argument("--once", action="store_true",
                         help="catch up to now, then exit instead of following live")
    produce.add_argument("--from-now", action="store_true",
                         help="skip the history replay when there is no checkpoint")
    produce.add_argument("--reset", action="store_true", help="forget the checkpoint first")
    produce.add_argument("--tick", type=float, default=5.0,
                         help="seconds between two live batches")
    produce.set_defaults(handler=cmd_produce)

    summary = commands.add_parser("summary", help="print daily order counts and revenue")
    summary.add_argument("--from", dest="start", type=date.fromisoformat)
    summary.add_argument("--until", type=date.fromisoformat)
    summary.set_defaults(handler=cmd_summary)

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = build_parser().parse_args(argv)
    return args.handler(args, Settings.from_env())
