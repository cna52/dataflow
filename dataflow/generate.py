from __future__ import annotations

import csv
import json
import random
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from dataflow.config import PG_DSN, SOURCES, ensure_dirs
from dataflow.util import utcnow

REGIONS = ("BC", "ON", "QC", "AB", "NS")
CATEGORIES = ("Electronics", "Home", "Books", "Outdoors", "Beauty")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def baseline(orders: int = 10_000, months: int = 3, seed: int = 42) -> None:
    if orders < 1 or months < 1:
        raise ValueError("orders and months must be positive")
    ensure_dirs()
    rng = random.Random(seed)
    start = utcnow() - timedelta(days=months * 30)
    customer_count = max(100, orders // 8)
    product_count = 200
    with psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM source.orders")
            if cursor.fetchone()[0]:
                raise RuntimeError("Source already contains orders; use seed updates or reset the local volume")
            customers = [
                (i, f"customer{i}@example.test", rng.choice(REGIONS), start, start)
                for i in range(1, customer_count + 1)
            ]
            with cursor.copy("COPY source.customers (customer_id,email,region,created_at,updated_at) FROM STDIN") as copy:
                for row in customers:
                    copy.write_row(row)
            transaction_id = 0
            item_id = 0
            events_path = SOURCES / "events" / "events_baseline.jsonl"
            with tempfile.TemporaryDirectory() as temp_dir:
                temp = Path(temp_dir)
                paths = {name: temp / f"{name}.csv" for name in ("orders", "order_items", "transactions")}
                with events_path.open("w", encoding="utf-8") as events, \
                     paths["orders"].open("w", newline="", encoding="utf-8") as order_file, \
                     paths["order_items"].open("w", newline="", encoding="utf-8") as item_file, \
                     paths["transactions"].open("w", newline="", encoding="utf-8") as tx_file:
                    order_writer = csv.writer(order_file)
                    item_writer = csv.writer(item_file)
                    tx_writer = csv.writer(tx_file)
                    for order_id in range(1, orders + 1):
                        ordered_at = start + timedelta(seconds=rng.randint(0, months * 30 * 86400 - 1))
                        status = rng.choices(("paid", "pending", "cancelled"), (82, 10, 8))[0]
                        customer_id = rng.randint(1, customer_count)
                        order_writer.writerow((order_id, customer_id, ordered_at, status, ordered_at))
                        total = 0
                        for _ in range(rng.randint(1, 4)):
                            item_id += 1
                            product_id = rng.randint(1, product_count)
                            quantity = rng.randint(1, 3)
                            unit_price_cents = 500 + product_id * 37
                            total += quantity * unit_price_cents
                            item_writer.writerow((item_id, order_id, product_id, quantity, unit_price_cents, "USD", ordered_at))
                            event = {
                                "event_id": f"view-{item_id}", "customer_id": customer_id,
                                "product_id": product_id, "event_type": "product_view",
                                "occurred_at": (ordered_at - timedelta(minutes=10)).isoformat(),
                            }
                            events.write(json.dumps(event) + "\n")
                        events.write(json.dumps({
                            "event_id": f"checkout-{order_id}", "customer_id": customer_id,
                            "product_id": None, "event_type": "checkout",
                            "occurred_at": ordered_at.isoformat(),
                        }) + "\n")
                        if status in ("paid", "pending"):
                            transaction_id += 1
                            tx_writer.writerow((transaction_id, order_id, "payment", "settled" if status == "paid" else "pending", total, ordered_at))
                columns = {
                    "orders": "order_id,customer_id,ordered_at,status,updated_at",
                    "order_items": "order_item_id,order_id,product_id,quantity,unit_price_cents,currency,updated_at",
                    "transactions": "transaction_id,order_id,kind,status,amount_cents,updated_at",
                }
                for name, cols in columns.items():
                    with cursor.copy(f"COPY source.{name} ({cols}) FROM STDIN WITH (FORMAT CSV)") as copy:
                        with paths[name].open("rb") as source:
                            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                                copy.write(chunk)

    category_rows = [{"category_id": i + 1, "category_name": name} for i, name in enumerate(CATEGORIES)]
    product_rows = [{
        "product_id": i, "product_name": f"Product {i}", "category_id": ((i - 1) % len(CATEGORIES)) + 1,
        "valid_from": start.isoformat(), "valid_to": "",
    } for i in range(1, product_count + 1)]
    pricing_rows = [{
        "product_id": i, "price_cents": 500 + i * 37, "currency": "USD",
        "valid_from": start.isoformat(), "valid_to": "",
    } for i in range(1, product_count + 1)]
    _write_csv(SOURCES / "catalog" / "categories.csv", list(category_rows[0]), category_rows)
    _write_csv(SOURCES / "catalog" / "products.csv", list(product_rows[0]), product_rows)
    _write_csv(SOURCES / "catalog" / "pricing.csv", list(pricing_rows[0]), pricing_rows)


def updates(seed: int = 42) -> None:
    """Apply deterministic changed-order scenarios and a late activity batch."""
    ensure_dirs()
    rng = random.Random(seed)
    with psycopg.connect(PG_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT max(updated_at) FROM (
                    SELECT max(updated_at) AS updated_at FROM source.customers
                    UNION ALL SELECT max(updated_at) FROM source.orders
                    UNION ALL SELECT max(updated_at) FROM source.order_items
                    UNION ALL SELECT max(updated_at) FROM source.transactions
                ) changed
            """)
            latest_update = cursor.fetchone()[0]
            now = max(utcnow(), latest_update + timedelta(microseconds=1)) if latest_update else utcnow()
            cursor.execute("SELECT count(*) FROM source.orders")
            count = cursor.fetchone()[0]
            if not count:
                raise RuntimeError("Seed the baseline before updates")
            selected = rng.sample(range(1, count + 1), min(100, count))
            for order_id in selected:
                cursor.execute("SELECT status FROM source.orders WHERE order_id=%s", (order_id,))
                old_status = cursor.fetchone()[0]
                if old_status == "paid":
                    new_status = "refunded" if order_id % 2 == 0 else "cancelled"
                elif old_status == "pending":
                    new_status = "paid"
                else:
                    continue
                cursor.execute("UPDATE source.orders SET status=%s,updated_at=%s WHERE order_id=%s", (new_status, now, order_id))
                cursor.execute("SELECT coalesce(sum(quantity*unit_price_cents),0) FROM source.order_items WHERE order_id=%s", (order_id,))
                total = cursor.fetchone()[0]
                if new_status == "paid":
                    cursor.execute("UPDATE source.transactions SET status='settled',updated_at=%s WHERE order_id=%s AND kind='payment'", (now, order_id))
                elif new_status == "refunded":
                    cursor.execute("SELECT coalesce(max(transaction_id),0)+1 FROM source.transactions")
                    tx_id = cursor.fetchone()[0]
                    cursor.execute("INSERT INTO source.transactions VALUES (%s,%s,'refund','settled',%s,%s)", (tx_id, order_id, total, now))
            cursor.execute("SELECT customer_id,product_id,ordered_at FROM source.orders JOIN source.order_items USING(order_id) ORDER BY order_id LIMIT 1")
            customer_id, product_id, ordered_at = cursor.fetchone()
            cursor.execute("SELECT coalesce(max(order_id),0)+1 FROM source.orders")
            new_order_id = cursor.fetchone()[0]
            cursor.execute("SELECT coalesce(max(order_item_id),0)+1 FROM source.order_items")
            new_item_id = cursor.fetchone()[0]
            cursor.execute("SELECT coalesce(max(transaction_id),0)+1 FROM source.transactions")
            new_tx_id = cursor.fetchone()[0]
            new_ordered_at = now
            cursor.execute("INSERT INTO source.orders VALUES (%s,1,%s,'paid',%s)", (new_order_id, new_ordered_at, new_ordered_at))
            cursor.execute("INSERT INTO source.order_items VALUES (%s,%s,1,1,537,'USD',%s)",
                           (new_item_id, new_order_id, new_ordered_at))
            cursor.execute("INSERT INTO source.transactions VALUES (%s,%s,'payment','settled',537,%s)",
                           (new_tx_id, new_order_id, new_ordered_at))
    events_path = SOURCES / "events" / f"events_update_{now.strftime('%Y%m%dT%H%M%S%f')}.jsonl"
    with events_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "event_id": f"late-{now.timestamp()}", "customer_id": customer_id,
            "product_id": product_id, "event_type": "add_to_cart",
            "occurred_at": (ordered_at - timedelta(minutes=15)).isoformat(),
        }) + "\n")
    products_path = SOURCES / "catalog" / "products.csv"
    with products_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        if int(row["product_id"]) == 1 and not row["valid_to"]:
            row["valid_to"] = now.isoformat()
            rows.append({**row, "category_id": "2", "valid_from": now.isoformat(), "valid_to": ""})
            break
    _write_csv(products_path, list(rows[0]), rows)
