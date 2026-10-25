"""Isolated, reproducible end-to-end pipeline benchmarks."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import duckdb
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from dataflow.config import PG_DSN, ROOT
from dataflow.util import utcnow, write_json_atomic


def _memory_bytes() -> int | None:
    path = Path("/sys/fs/cgroup/memory.current")
    return int(path.read_text()) if path.exists() else None


def _database(size: int) -> tuple[str, str, str]:
    name = f"dataflow_bench_{size}_{uuid.uuid4().hex[:8]}"
    source = conninfo_to_dict(PG_DSN)
    admin = make_conninfo("", **{**source, "user": "postgres", "dbname": "postgres"})
    target = make_conninfo("", **{**source, "dbname": name})
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {} OWNER dataflow").format(sql.Identifier(name)))
    try:
        with psycopg.connect(target) as connection:
            connection.execute((ROOT / "infra" / "source.sql").read_text(encoding="utf-8"))
    except Exception:
        _drop_database(admin, name)
        raise
    return name, admin, target


def _drop_database(admin: str, name: str) -> None:
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _command(args: list[str], env: dict[str, str], log: Path) -> dict:
    start = time.perf_counter()
    before = _memory_bytes()
    peak = [before or 0]
    finished = threading.Event()

    def sample() -> None:
        while not finished.wait(0.1):
            peak[0] = max(peak[0], _memory_bytes() or 0)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        with log.open("w", encoding="utf-8") as output:
            result = subprocess.run([sys.executable, "-m", "dataflow.cli", *args], env=env,
                                    stdout=output, stderr=subprocess.STDOUT, check=False)
    finally:
        finished.set()
        sampler.join()
    elapsed = time.perf_counter() - start
    if result.returncode:
        tail = "\n".join(log.read_text(encoding="utf-8", errors="replace").splitlines()[-20:])
        raise RuntimeError(f"{' '.join(args)} failed; log: {log}\n{tail}")
    return {"seconds": round(elapsed, 3), "container_memory_before_bytes": before,
            "container_memory_after_bytes": _memory_bytes(),
            "sampled_peak_container_memory_bytes": peak[0] or None, "log": str(log)}


def _warehouse_metrics(data: Path) -> dict:
    pointer = json.loads((data / "warehouse" / "latest.json").read_text(encoding="utf-8"))
    path = data / "warehouse" / pointer["file"]
    con = duckdb.connect(str(path), read_only=True)
    try:
        start = time.perf_counter()
        answer = con.execute("SELECT coalesce(sum(revenue_cents),0), count(*) FROM fact_orders").fetchone()
        query_ms = round((time.perf_counter() - start) * 1000, 3)
        counts = con.execute("SELECT (SELECT count(*) FROM fact_order_items), "
                             "(SELECT count(*) FROM mart_daily_sales)").fetchone()
        return {"revenue_cents": answer[0], "orders": answer[1], "order_items": counts[0],
                "daily_rows": counts[1], "query_latency_ms": query_ms,
                "warehouse_bytes": path.stat().st_size}
    finally:
        con.close()


def benchmark_pipeline(sizes: list[int], keep_data: bool = False) -> Path:
    """Seed and run independent full, changed, and rebuild profiles for each size."""
    if not sizes or any(size < 1 for size in sizes):
        raise ValueError("Benchmark sizes must be positive")
    root = ROOT / "benchmark_results" / f"pipeline_{utcnow().strftime('%Y%m%dT%H%M%S')}"
    root.mkdir(parents=True)
    results = []
    report = root / "results.json"
    for size in sizes:
        name, admin, target = _database(size)
        workspace = root / f"orders_{size}"
        data = workspace / "data"
        workspace.mkdir()
        env = os.environ.copy()
        env["DATAFLOW_PG_DSN"] = target
        env["DATAFLOW_DATA_DIR"] = str(data)
        entry: dict = {"orders_seeded": size}
        results.append(entry)
        try:
            entry["seed"] = _command(["seed", "--orders", str(size)], env, workspace / "seed.log")
            run_ids = {"full": "b01_full", "incremental": "b02_incremental", "rebuild": "b03_rebuild"}
            for label, args in (
                ("full", ["run", "--run-id", "b01_full", "--full"]),
                ("seed_updates", ["seed", "--scenario", "updates"]),
                ("incremental", ["run", "--run-id", "b02_incremental"]),
                ("rebuild", ["run", "--run-id", "b03_rebuild", "--full"]),
            ):
                entry[label] = _command(args, env, workspace / f"{label}.log")
                if label in ("full", "incremental", "rebuild"):
                    entry[label]["stages"] = json.loads(
                        (data / "warehouse" / "runs" / f"{run_ids[label]}.json").read_text())
                    entry[label]["metrics"] = _warehouse_metrics(data)
                    entry[label]["processed"] = json.loads(
                        (data / "processed" / run_ids[label] / "manifest.json").read_text())
                    dbt_results = json.loads((ROOT / "dbt_project" / "target" / "run_results.json").read_text())
                    test_results = [item for item in dbt_results["results"]
                                    if item["unique_id"].startswith("test.")]
                    entry[label]["quality_tests"] = {
                        "total": len(test_results),
                        "passed": sum(item["status"] == "pass" for item in test_results),
                    }
            assert entry["incremental"]["metrics"]["revenue_cents"] == entry["rebuild"]["metrics"]["revenue_cents"]
            assert entry["incremental"]["metrics"]["orders"] == entry["rebuild"]["metrics"]["orders"]
            entry["rebuild_matches_incremental"] = True
            entry["records_per_second"] = {}
            for label in ("full", "incremental", "rebuild"):
                source_rows = sum(entry[label]["processed"]["metrics"][table]["input_rows"]
                                  for table in ("customers", "orders", "order_items", "transactions", "events"))
                entry[label]["source_input_rows"] = source_rows
                entry["records_per_second"][label] = round(
                    source_rows / entry[label]["stages"]["total_seconds"], 2)
        except Exception as exc:
            entry["error"] = str(exc)
            write_json_atomic(report, {"created_at": utcnow().isoformat(), "results": results})
            raise
        finally:
            _drop_database(admin, name)
            if not keep_data:
                shutil.rmtree(data, ignore_errors=True)
            write_json_atomic(report, {"created_at": utcnow().isoformat(), "results": results,
                                       "note": "Each size uses an isolated PostgreSQL database and data directory. Time includes all pipeline stages; memory samples are container-wide and include the JVM, not just Python."})
    return report
