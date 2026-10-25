from __future__ import annotations

import json

import duckdb
import pytest

from dataflow.config import PROCESSED, WAREHOUSE
from dataflow import ingest as ingestion
from dataflow import process as processing
from dataflow.util import safe_run_id


def test_run_id_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        safe_run_id("../data")


def test_raw_validation_detects_tampering(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ingestion, "RAW", tmp_path)
    batch = tmp_path / "damaged"
    batch.mkdir()
    (batch / "orders.jsonl").write_text('{"order_id":1}\n', encoding="utf-8")
    (batch / "manifest.json").write_text(json.dumps({
        "tables": {"orders": {"sha256": "incorrect", "rows": 1}}, "files": [],
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        ingestion.validate_raw("damaged")


def test_raw_history_uses_ingestion_time_not_run_name(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ingestion, "RAW", tmp_path)
    for name, created, digest in (("z_old", "2026-01-01T00:00:00+00:00", "first"),
                                  ("a_new", "2026-02-01T00:00:00+00:00", "second")):
        batch = tmp_path / name
        batch.mkdir()
        (batch / "manifest.json").write_text(json.dumps({
            "created_at": created,
            "files": [{"source_path": "catalog/pricing.csv", "sha256": digest}],
        }), encoding="utf-8")
    assert [path.parent.name for path in ingestion.manifests()] == ["z_old", "a_new"]
    assert ingestion._latest_file_hashes()["catalog/pricing.csv"] == "second"


def test_processing_replays_raw_batches_after_previous_snapshot(tmp_path, monkeypatch) -> None:
    history = [tmp_path / name / "manifest.json" for name in ("published", "failed", "repaired")]
    monkeypatch.setattr(processing, "manifests", lambda: history)
    assert [path.name for path in processing._raw_batches("repaired", "published", False)] == ["failed", "repaired"]


def test_published_warehouse_contract() -> None:
    pointer = WAREHOUSE / "latest.json"
    if not pointer.exists():
        pytest.skip("Run the pipeline before warehouse contract tests")
    run_id = json.loads(pointer.read_text())["run_id"]
    warehouse = WAREHOUSE / json.loads(pointer.read_text())["file"]
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        assert con.execute("select count(*) from fact_orders").fetchone()[0] > 0
        assert con.execute("select count(*)-count(distinct order_id) from fact_orders").fetchone()[0] == 0
        assert con.execute("select count(*)-count(distinct order_item_id) from fact_order_items").fetchone()[0] == 0
        assert con.execute("select count(*) from fact_order_items where order_status <> 'paid' and revenue_cents <> 0").fetchone()[0] == 0
        assert con.execute("""
            select count(*) from fact_orders o
            left join (select order_id,sum(revenue_cents) as amount from fact_order_items group by 1) i using(order_id)
            where o.revenue_cents <> coalesce(i.amount,0)
        """).fetchone()[0] == 0
        assert con.execute("select count(*) from mart_monthly_retention where retention_rate < 0 or retention_rate > 1").fetchone()[0] == 0
    finally:
        con.close()
    assert (PROCESSED / run_id / "manifest.json").exists()
