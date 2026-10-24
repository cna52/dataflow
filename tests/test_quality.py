from __future__ import annotations

import json

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
