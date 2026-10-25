from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date

from dataflow import generate
from dataflow.benchmark import benchmark
from dataflow.pipeline_benchmark import benchmark_pipeline
from dataflow.recovery_check import verify_recovery
from dataflow.config import WAREHOUSE
from dataflow.ingest import ingest, validate_raw
from dataflow.process import process
from dataflow.util import new_run_id, safe_run_id, utcnow, write_json_atomic
from dataflow.warehouse import load, model, publish, query


def _pipeline(run_id: str, full: bool = False, backfill: tuple[date, date] | None = None) -> None:
    started_at = utcnow().isoformat()
    started = time.perf_counter()
    stages = {}
    for name, step in (
        ("ingest", lambda: ingest(run_id, full=full)),
        ("validate", lambda: validate_raw(run_id)),
        ("process", lambda: process(run_id, rebuild=full or backfill is not None)),
        ("load", lambda: load(run_id)),
        ("model", lambda: model(run_id, full_refresh=full, backfill=backfill)),
        ("publish", lambda: publish(run_id)),
    ):
        mark = time.perf_counter()
        step()
        stages[name] = round(time.perf_counter() - mark, 3)
        print(f"{name}: {stages[name]}s", flush=True)
    write_json_atomic(WAREHOUSE / "runs" / f"{run_id}.json", {
        "run_id": run_id, "started_at": started_at,
        "mode": "backfill" if backfill else "full" if full else "incremental",
        "stages_seconds": stages, "total_seconds": round(time.perf_counter() - started, 3),
    })


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dataflow")
    sub = parser.add_subparsers(dest="command", required=True)
    seed = sub.add_parser("seed", help="Generate synthetic local sources")
    seed.add_argument("--scenario", choices=("baseline", "updates"), default="baseline")
    seed.add_argument("--orders", type=int, default=10_000)
    seed.add_argument("--months", type=int, default=3)
    seed.add_argument("--seed", type=int, default=42)
    for name in ("ingest", "validate", "process", "load", "model", "publish", "run"):
        command = sub.add_parser(name)
        command.add_argument("--run-id", default=None)
        if name in ("ingest", "run"):
            command.add_argument("--full", action="store_true")
        if name == "process":
            command.add_argument("--rebuild", action="store_true")
        if name == "model":
            command.add_argument("--full-refresh", action="store_true")
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--start-date", type=date.fromisoformat, required=True)
    backfill.add_argument("--end-date", type=date.fromisoformat, required=True)
    backfill.add_argument("--run-id", default=None)
    benchmark_cmd = sub.add_parser("benchmark")
    benchmark_cmd.add_argument("--sizes", nargs="+", type=int, default=[10_000, 100_000, 500_000])
    pipeline_benchmark_cmd = sub.add_parser("benchmark-pipeline")
    pipeline_benchmark_cmd.add_argument("--sizes", nargs="+", type=int, default=[10_000, 100_000, 500_000])
    pipeline_benchmark_cmd.add_argument("--keep-data", action="store_true")
    recovery_cmd = sub.add_parser("verify-recovery")
    recovery_cmd.add_argument("--keep-data", action="store_true")
    query_cmd = sub.add_parser("query")
    query_cmd.add_argument("sql")
    args = parser.parse_args(argv)
    if args.command == "seed":
        if args.scenario == "baseline":
            generate.baseline(args.orders, args.months, args.seed)
        else:
            generate.updates(args.seed)
        return
    if args.command == "benchmark":
        print(benchmark(args.sizes))
        return
    if args.command == "benchmark-pipeline":
        print(benchmark_pipeline(args.sizes, keep_data=args.keep_data))
        return
    if args.command == "verify-recovery":
        print(verify_recovery(keep_data=args.keep_data))
        return
    if args.command == "query":
        for row in query(args.sql):
            print(row)
        return
    run_id = safe_run_id(args.run_id or new_run_id())
    if args.command == "backfill":
        if args.start_date > args.end_date:
            parser.error("start-date must not exceed end-date")
        if not (WAREHOUSE / "latest.json").exists():
            parser.error("backfill requires an existing published warehouse")
        _pipeline(run_id, backfill=(args.start_date, args.end_date))
    elif args.command == "run":
        _pipeline(run_id, full=args.full or not (WAREHOUSE / "latest.json").exists())
    elif args.command == "ingest":
        print(ingest(run_id, full=args.full))
    elif args.command == "validate":
        validate_raw(run_id)
    elif args.command == "process":
        print(process(run_id, rebuild=args.rebuild))
    elif args.command == "load":
        print(load(run_id))
    elif args.command == "model":
        model(run_id, full_refresh=args.full_refresh)
    elif args.command == "publish":
        print(publish(run_id))


if __name__ == "__main__":
    main(sys.argv[1:])
