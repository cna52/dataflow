from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("DATAFLOW_ROOT", Path(__file__).resolve().parents[1]))
DATA = ROOT / "data"
SOURCES = DATA / "sources"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
WAREHOUSE = DATA / "warehouse"
PG_DSN = os.environ.get("DATAFLOW_PG_DSN", "postgresql://dataflow:dataflow@localhost:5433/dataflow")
TABLE_KEYS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "transactions": "transaction_id",
}


def ensure_dirs() -> None:
    for path in (SOURCES / "catalog", SOURCES / "events", RAW, PROCESSED, WAREHOUSE):
        path.mkdir(parents=True, exist_ok=True)
