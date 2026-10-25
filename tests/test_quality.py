from __future__ import annotations

import json

import duckdb
import pytest

from dataflow import process as processing
from dataflow import warehouse


def test_bad_event_is_quarantined(tmp_path, monkeypatch) -> None:
    raw = tmp_path / "raw"
    processed = tmp_path / "processed"
    batch = raw / "bad_event"
    batch.mkdir(parents=True)
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    timestamp = "2026-08-01T00:00:00+00:00"
    rows = {
        "customers": {"customer_id": 1, "email": "one@example.test", "region": "BC", "created_at": timestamp, "updated_at": timestamp},
        "orders": {"order_id": 1, "customer_id": 1, "ordered_at": timestamp, "status": "paid", "updated_at": timestamp},
        "order_items": {"order_item_id": 1, "order_id": 1, "product_id": 1, "quantity": 1, "unit_price_cents": 1000, "currency": "USD", "updated_at": timestamp},
        "transactions": {"transaction_id": 1, "order_id": 1, "kind": "payment", "status": "settled", "amount_cents": 1000, "updated_at": timestamp},
    }
    for table, row in rows.items():
        (batch / f"{table}.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    (batch / "events").mkdir()
    (batch / "events" / "bad.jsonl").write_text(json.dumps({
        "event_id": "bad", "customer_id": 1, "product_id": 1,
        "event_type": "product_view", "occurred_at": "not-a-timestamp",
    }) + "\n", encoding="utf-8")
    (batch / "manifest.json").write_text(json.dumps({
        "run_id": "bad_event", "files": [{"source_path": "events/bad.jsonl"}],
    }), encoding="utf-8")
    (catalog / "products.csv").write_text(
        "product_id,product_name,category_id,valid_from,valid_to\n1,One,1,2026-01-01T00:00:00+00:00,\n",
        encoding="utf-8",
    )
    (catalog / "categories.csv").write_text("category_id,category_name\n1,Test\n", encoding="utf-8")
    (catalog / "pricing.csv").write_text(
        "product_id,price_cents,currency,valid_from,valid_to\n1,1000,USD,2026-01-01T00:00:00+00:00,\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(processing, "RAW", raw)
    monkeypatch.setattr(processing, "PROCESSED", processed)
    monkeypatch.setattr(processing, "manifests", lambda: [batch / "manifest.json"])
    monkeypatch.setattr(processing, "_latest_catalog", lambda name: catalog / f"{name}.csv")
    result = processing.process("bad_event")
    manifest = json.loads(result.read_text(encoding="utf-8"))
    assert manifest["counts"]["quarantine"] == 1
    assert manifest["counts"]["events"] == 0
    assert manifest["counts"]["joined_order_items"] == 1
    monkeypatch.setattr(warehouse, "PROCESSED", processed)
    monkeypatch.setattr(warehouse, "WAREHOUSE", tmp_path / "warehouse")
    with pytest.raises(ValueError, match="quarantined rows"):
        warehouse.load("bad_event")
    assert not (tmp_path / "warehouse" / "latest.json").exists()

    repair = raw / "repair_event"
    repair.mkdir()
    for table in rows:
        (repair / f"{table}.jsonl").touch()
    (repair / "events").mkdir()
    (repair / "events" / "fixed.jsonl").write_text(json.dumps({
        "event_id": "bad", "customer_id": 1, "product_id": 1,
        "event_type": "product_view", "occurred_at": "2026-07-01T00:00:00+00:00",
    }) + "\n", encoding="utf-8")
    (repair / "manifest.json").write_text(json.dumps({
        "run_id": "repair_event", "files": [{"source_path": "events/fixed.jsonl"}],
    }), encoding="utf-8")
    monkeypatch.setattr(processing, "manifests", lambda: [batch / "manifest.json", repair / "manifest.json"])
    repaired = json.loads(processing.process("repair_event").read_text(encoding="utf-8"))
    assert repaired["counts"]["quarantine"] == 0
    assert repaired["counts"]["events"] == 1
    assert repaired["metrics"]["orders"]["changed_buckets"] == 0
    candidate = warehouse.load("repair_event")
    with duckdb.connect(str(candidate), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM source_events WHERE event_id='bad'").fetchone()[0] == 1
    assert (batch / "events" / "bad.jsonl").exists()

    earlier = raw / "earlier_event"
    earlier.mkdir()
    for table in rows:
        (earlier / f"{table}.jsonl").touch()
    (earlier / "events").mkdir()
    (earlier / "events" / "earlier.jsonl").write_text(json.dumps({
        "event_id": "bad", "customer_id": 1, "product_id": 1,
        "event_type": "product_view", "occurred_at": "2026-06-01T00:00:00+00:00",
    }) + "\n", encoding="utf-8")
    (earlier / "manifest.json").write_text(json.dumps({
        "run_id": "earlier_event", "files": [{"source_path": "events/earlier.jsonl"}],
    }), encoding="utf-8")
    monkeypatch.setattr(processing, "manifests", lambda: [batch / "manifest.json", repair / "manifest.json", earlier / "manifest.json"])
    processing.process("earlier_event")
    candidate = warehouse.load("earlier_event")
    with duckdb.connect(str(candidate), read_only=True) as connection:
        occurred_at = connection.execute("SELECT occurred_at FROM source_events WHERE event_id='bad'").fetchone()[0]
    assert occurred_at.isoformat().startswith("2026-06-01")


def test_quarantine_blocks_warehouse_publication(tmp_path, monkeypatch) -> None:
    processed = tmp_path / "processed"
    run = processed / "bad"
    run.mkdir(parents=True)
    (run / "manifest.json").write_text(json.dumps({"counts": {"quarantine": 2}}), encoding="utf-8")
    monkeypatch.setattr(warehouse, "PROCESSED", processed)
    monkeypatch.setattr(warehouse, "WAREHOUSE", tmp_path / "warehouse")
    with pytest.raises(ValueError, match="quarantined rows"):
        warehouse.load("bad")
    assert not (tmp_path / "warehouse" / "latest.json").exists()


def test_product_version_change_only_affects_matching_purchase_dates(tmp_path) -> None:
    spark = processing._spark()
    try:
        orders = spark.createDataFrame([
            (1, "2026-08-01T00:00:00"), (2, "2026-10-01T00:00:00"),
        ], "order_id long, ordered_at string").selectExpr("order_id", "cast(ordered_at as timestamp) as ordered_at")
        items = spark.createDataFrame([(11, 1, 1), (22, 2, 1)],
                                      "order_item_id long, order_id long, product_id long")
        old = spark.createDataFrame([(1, 1, "2026-01-01T00:00:00", None)],
                                    "product_id long, category_id long, valid_from string, valid_to string")
        current = spark.createDataFrame([
            (1, 1, "2026-01-01T00:00:00", "2026-09-01T00:00:00"),
            (1, 2, "2026-09-01T00:00:00", None),
        ], "product_id long, category_id long, valid_from string, valid_to string")
        def timestamps(frame):
            return frame.selectExpr("product_id", "category_id",
                                    "cast(valid_from as timestamp) as valid_from",
                                    "cast(valid_to as timestamp) as valid_to")
        previous = tmp_path / "previous"
        timestamps(old).write.parquet(str(previous / "products"))
        affected = processing._orders_with_changed_product_version(
            items, orders, timestamps(current), previous, [1], spark)
        assert [row.order_id for row in affected.collect()] == [2]
    finally:
        spark.stop()
