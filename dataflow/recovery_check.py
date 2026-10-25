"""End-to-end failure, correction, rerun, and historical backfill exercise."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb
import psycopg

from dataflow.config import ROOT
from dataflow.pipeline_benchmark import _command, _database, _drop_database
from dataflow.util import utcnow, write_json_atomic


def _pointer(data: Path) -> dict:
    return json.loads((data / "warehouse" / "latest.json").read_text(encoding="utf-8"))


def _query(data: Path, statement: str, parameters: list | None = None) -> tuple:
    pointer = _pointer(data)
    con = duckdb.connect(str(data / "warehouse" / pointer["file"]), read_only=True)
    try:
        return con.execute(statement, parameters or []).fetchone()
    finally:
        con.close()


def verify_recovery(keep_data: bool = False) -> Path:
    name, admin, target = _database(100)
    root = ROOT / "benchmark_results" / f"recovery_{uuid.uuid4().hex[:8]}"
    data = root / "data"
    root.mkdir(parents=True)
    env = os.environ.copy()
    env["DATAFLOW_PG_DSN"] = target
    env["DATAFLOW_DATA_DIR"] = str(data)
    report: dict = {"created_at": utcnow().isoformat()}
    result_path = root / "results.json"
    try:
        _command(["seed", "--orders", "100"], env, root / "seed.log")
        _command(["run", "--run-id", "r01_baseline", "--full"], env, root / "baseline.log")
        baseline_pointer = _pointer(data)
        event_dir = data / "sources" / "events"
        bad_file = event_dir / "recovery.jsonl"
        event = {"event_id": "recover-1", "customer_id": 1, "product_id": 1,
                 "event_type": "product_view", "occurred_at": "invalid-timestamp"}
        bad_file.write_text(json.dumps(event) + "\n", encoding="utf-8")
        with (root / "bad.log").open("w", encoding="utf-8") as output:
            failed = subprocess.run([sys.executable, "-m", "dataflow.cli", "run", "--run-id", "r02_bad"],
                                    env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
        assert failed.returncode != 0, "Bad event unexpectedly passed"
        assert _pointer(data) == baseline_pointer, "Failed run changed the published pointer"
        bad_manifest = json.loads((data / "processed" / "r02_bad" / "manifest.json").read_text())
        assert bad_manifest["counts"]["quarantine"] == 1
        assert (data / "raw" / "r02_bad" / "events" / "recovery.jsonl").exists()
        report["invalid_event_blocked_publication"] = True
        report["raw_bad_batch_retained"] = True

        event["occurred_at"] = "2026-08-01T12:00:00+00:00"
        bad_file.write_text(json.dumps(event) + "\n", encoding="utf-8")
        _command(["run", "--run-id", "r03_repaired"], env, root / "repaired.log")
        repaired = json.loads((data / "processed" / "r03_repaired" / "manifest.json").read_text())
        assert repaired["counts"]["quarantine"] == 0
        assert _query(data, "SELECT count(*) FROM source_events WHERE event_id='recover-1'")[0] == 1
        assert _query(data, "SELECT occurred_at FROM source_events WHERE event_id='recover-1'")[0].isoformat().startswith("2026-08-01")
        report["correction_cleared_quarantine"] = True

        before_rerun = _query(data, "SELECT count(*), coalesce(sum(revenue_cents),0) FROM fact_orders")
        _command(["run", "--run-id", "r03_repaired"], env, root / "rerun.log")
        assert _query(data, "SELECT count(*), coalesce(sum(revenue_cents),0) FROM fact_orders") == before_rerun
        report["rerun_idempotent"] = True

        order = _query(data, "SELECT order_id, order_date, revenue_cents FROM fact_orders "
                       "WHERE status='paid' AND revenue_cents>0 ORDER BY order_id LIMIT 1")
        assert order is not None
        order_id, order_date, prior_revenue = order
        daily_before = _query(data, "SELECT revenue_cents FROM mart_daily_sales WHERE order_date=?", [order_date])[0]
        with psycopg.connect(target) as connection:
            connection.execute("UPDATE source.orders SET status='cancelled',updated_at=now() WHERE order_id=%s", (order_id,))
        pricing_path = data / "sources" / "catalog" / "pricing.csv"
        original_pricing = pricing_path.read_text(encoding="utf-8")
        assert "1,537,USD," in original_pricing
        pricing_path.write_text(original_pricing.replace("1,537,USD,", "1,-1,USD,", 1), encoding="utf-8")
        pointer_before_processing_failure = _pointer(data)
        with (root / "processing_failure.log").open("w", encoding="utf-8") as output:
            failed = subprocess.run([sys.executable, "-m", "dataflow.cli", "run",
                                     "--run-id", "r04_processing_failure"],
                                    env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
        assert failed.returncode != 0, "Invalid pricing unexpectedly passed"
        assert _pointer(data) == pointer_before_processing_failure
        assert not (data / "processed" / "r04_processing_failure" / "manifest.json").exists()
        report["processing_failure_blocked_publication"] = True

        pricing_path.write_text(original_pricing, encoding="utf-8")
        _command(["run", "--run-id", "r05_late_change"], env, root / "late_change.log")
        daily_after = _query(data, "SELECT revenue_cents FROM mart_daily_sales WHERE order_date=?", [order_date])[0]
        assert daily_after == daily_before - prior_revenue
        assert json.loads((data / "processed" / "r05_late_change" / "manifest.json").read_text())["metrics"]["orders"]["input_rows"] == 1
        report["failed_raw_batch_replayed"] = True
        report["historical_date_updated"] = str(order_date)
        report["historical_revenue_delta_cents"] = daily_after - daily_before

        _command(["backfill", "--run-id", "r06_backfill", "--start-date", str(order_date),
                  "--end-date", str(order_date)], env, root / "backfill.log")
        assert _query(data, "SELECT revenue_cents FROM mart_daily_sales WHERE order_date=?", [order_date])[0] == daily_after
        report["backfill_reconciled"] = True
    except Exception as exc:
        report["error"] = str(exc)
        raise
    finally:
        _drop_database(admin, name)
        if not keep_data:
            shutil.rmtree(data, ignore_errors=True)
        write_json_atomic(result_path, report)
    return result_path
