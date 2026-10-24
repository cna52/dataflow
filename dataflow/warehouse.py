from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import duckdb
import psycopg

from dataflow.config import PG_DSN, PROCESSED, ROOT, WAREHOUSE, ensure_dirs
from dataflow.util import safe_run_id, utcnow, write_json_atomic

DATASETS = ("customers", "orders", "order_items", "transactions", "products", "categories", "pricing", "events", "joined_order_items")


def _candidate(run_id: str) -> Path:
    return WAREHOUSE / "candidates" / f"{safe_run_id(run_id)}.duckdb"


def _published() -> Path | None:
    pointer = WAREHOUSE / "latest.json"
    if not pointer.exists():
        return None
    return WAREHOUSE / json.loads(pointer.read_text(encoding="utf-8"))["file"]


def load(run_id: str) -> Path:
    ensure_dirs()
    processed = PROCESSED / safe_run_id(run_id)
    info = json.loads((processed / "manifest.json").read_text(encoding="utf-8"))
    if info["counts"]["quarantine"]:
        raise ValueError(f"Run {run_id} has {info['counts']['quarantine']} quarantined rows; publication blocked")
    candidate = _candidate(run_id)
    candidate.parent.mkdir(parents=True, exist_ok=True)
    if candidate.exists():
        candidate.unlink()
    previous = _published()
    if previous is not None:
        shutil.copy2(previous, candidate)
    con = duckdb.connect(str(candidate))
    try:
        con.execute("SET memory_limit='3GB'")
        con.execute("SET threads=2")
        for name in DATASETS:
            path = str(processed / name / "**" / "*.parquet")
            con.execute(f"CREATE OR REPLACE TABLE source_{name} AS SELECT * FROM read_parquet(?, hive_partitioning=true)", [path])
        con.execute("CHECKPOINT")
    finally:
        con.close()
    write_json_atomic(WAREHOUSE / "candidates" / f"{run_id}.json", {"run_id": run_id, "candidate": str(candidate), "loaded_at": utcnow().isoformat()})
    return candidate


def model(run_id: str, full_refresh: bool = False, backfill: tuple[date, date] | None = None) -> None:
    candidate = _candidate(run_id)
    if not candidate.exists():
        raise FileNotFoundError(candidate)
    env = os.environ.copy()
    env["DATAFLOW_WAREHOUSE_PATH"] = str(candidate)
    env["DBT_PROFILES_DIR"] = str(ROOT / "dbt_project")
    command = ["dbt", "build", "--project-dir", str(ROOT / "dbt_project"), "--profiles-dir", str(ROOT / "dbt_project")]
    if full_refresh:
        command.append("--full-refresh")
    if backfill:
        command += ["--vars", json.dumps({"backfill_start": backfill[0].isoformat(), "backfill_end": backfill[1].isoformat()})]
    subprocess.run(command, check=True, env=env)
    con = duckdb.connect(str(candidate), read_only=True)
    try:
        duplicate_orders = con.execute("SELECT count(*)-count(DISTINCT order_id) FROM fact_orders").fetchone()[0]
        duplicate_items = con.execute("SELECT count(*)-count(DISTINCT order_item_id) FROM fact_order_items").fetchone()[0]
        inconsistent = con.execute("""
            SELECT count(*) FROM fact_orders o
            LEFT JOIN (SELECT order_id, sum(revenue_cents) AS item_revenue FROM fact_order_items GROUP BY 1) i USING (order_id)
            WHERE o.revenue_cents <> coalesce(i.item_revenue,0)
        """).fetchone()[0]
        if duplicate_orders or duplicate_items or inconsistent:
            raise ValueError(f"Warehouse gate failed: duplicate_orders={duplicate_orders}, duplicate_items={duplicate_items}, inconsistent={inconsistent}")
    finally:
        con.close()
    write_json_atomic(WAREHOUSE / "candidates" / f"{run_id}.validated.json", {"run_id": run_id, "validated_at": utcnow().isoformat()})


def publish(run_id: str) -> Path:
    safe_run_id(run_id)
    if not (WAREHOUSE / "candidates" / f"{run_id}.validated.json").exists():
        raise RuntimeError("Candidate has not passed dbt and reconciliation checks")
    candidate = _candidate(run_id)
    version = WAREHOUSE / "versions" / f"{run_id}.duckdb"
    version.parent.mkdir(parents=True, exist_ok=True)
    if not version.exists():
        os.replace(candidate, version)
    write_json_atomic(WAREHOUSE / "latest.json", {"run_id": run_id, "file": str(version.relative_to(WAREHOUSE)), "published_at": utcnow().isoformat()})
    with psycopg.connect(PG_DSN) as connection:
        connection.execute("UPDATE control.runs SET status='published',completed_at=now() WHERE run_id=%s", (run_id,))
    return version


def query(sql: str) -> list[tuple]:
    warehouse = _published()
    if warehouse is None:
        raise RuntimeError("No published warehouse")
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
