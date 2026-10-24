from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from dataflow.config import PROCESSED, RAW, TABLE_KEYS, ensure_dirs
from dataflow.ingest import manifests
from dataflow.util import safe_run_id, write_json_atomic

SCHEMAS = {
    "customers": "customer_id long,email string,region string,created_at string,updated_at string,_corrupt_record string",
    "orders": "order_id long,customer_id long,ordered_at string,status string,updated_at string,_corrupt_record string",
    "order_items": "order_item_id long,order_id long,product_id long,quantity int,unit_price_cents long,currency string,updated_at string,_corrupt_record string",
    "transactions": "transaction_id long,order_id long,kind string,status string,amount_cents long,updated_at string,_corrupt_record string",
    "events": "event_id string,customer_id long,product_id long,event_type string,occurred_at string,_corrupt_record string",
}


def _spark() -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName("DataFlow")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .getOrCreate())


def _read_json(spark: SparkSession, paths: list[Path], schema: str) -> DataFrame:
    return spark.read.schema(schema).option("mode", "PERMISSIVE").json([str(p) for p in paths])


def _dedup(frame: DataFrame, key: str, time_col: str) -> DataFrame:
    rank = Window.partitionBy(key).orderBy(F.col(time_col).desc_nulls_last())
    return frame.withColumn("_row", F.row_number().over(rank)).where("_row = 1").drop("_row")


def _latest_catalog(name: str) -> Path:
    for manifest_path in reversed(manifests()):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        relative = f"catalog/{name}.csv"
        if any(item["source_path"] == relative for item in manifest["files"]):
            return manifest_path.parent / relative
    raise FileNotFoundError(f"No archived catalog/{name}.csv")


def process(run_id: str, rebuild: bool = False) -> Path:
    safe_run_id(run_id)
    ensure_dirs()
    final = PROCESSED / run_id
    if (final / "manifest.json").exists():
        write_json_atomic(PROCESSED / "latest.json", {"run_id": run_id})
        return final / "manifest.json"
    current_raw = RAW / run_id
    if not (current_raw / "manifest.json").exists():
        raise FileNotFoundError(f"Raw batch {run_id} is missing")
    previous_path = PROCESSED / "latest.json"
    previous = None if rebuild or not previous_path.exists() else json.loads(previous_path.read_text())["run_id"]
    pending = PROCESSED / f".{run_id}.pending"
    if pending.exists():
        shutil.rmtree(pending)
    pending.mkdir(parents=True)
    spark = _spark()
    counts: dict[str, int] = {}
    try:
        snapshots: dict[str, DataFrame] = {}
        quarantine: list[DataFrame] = []
        raw_batches = [path.parent for path in manifests()] if rebuild else [current_raw]
        for table, key in TABLE_KEYS.items():
            paths = [batch / f"{table}.jsonl" for batch in raw_batches]
            incoming = _read_json(spark, paths, SCHEMAS[table])
            incoming = incoming.withColumn("updated_at", F.try_to_timestamp("updated_at"))
            if table == "customers":
                incoming = incoming.withColumn("created_at", F.try_to_timestamp("created_at"))
                valid = (F.col(key).isNotNull() & F.col("email").isNotNull() &
                         F.col("region").isNotNull() & F.col("created_at").isNotNull() & F.col("updated_at").isNotNull())
            elif table == "orders":
                incoming = incoming.withColumn("ordered_at", F.try_to_timestamp("ordered_at"))
                valid = (F.col(key).isNotNull() & F.col("customer_id").isNotNull() &
                         F.col("ordered_at").isNotNull() & F.col("updated_at").isNotNull() &
                         F.col("status").isin("pending", "paid", "cancelled", "refunded"))
            elif table == "order_items":
                valid = (F.col(key).isNotNull() & F.col("order_id").isNotNull() & F.col("product_id").isNotNull() &
                         (F.col("quantity") > 0) & (F.col("unit_price_cents") >= 0) &
                         (F.col("currency") == "USD") & F.col("updated_at").isNotNull())
            else:
                valid = (F.col(key).isNotNull() & F.col("order_id").isNotNull() &
                         (F.col("amount_cents") >= 0) & F.col("updated_at").isNotNull() &
                         F.col("kind").isin("payment", "refund") &
                         F.col("status").isin("pending", "settled", "failed"))
            quarantine.append(incoming.where(~valid).withColumn("source_table", F.lit(table))
                              .withColumn("reason", F.lit("invalid_schema_or_value"))
                              .select("source_table", "reason", F.to_json(F.struct(*incoming.columns)).alias("payload")))
            cleaned = incoming.where(valid).drop("_corrupt_record")
            if previous:
                old = spark.read.parquet(str(PROCESSED / previous / table))
                cleaned = old.unionByName(cleaned)
            snapshots[table] = _dedup(cleaned, key, "updated_at").cache()

        # Keep the latest full catalog snapshot; version rows encode historical category membership.
        products = spark.read.option("header", True).csv(str(_latest_catalog("products")))
        products = (products.select(F.col("product_id").cast("long").alias("product_id"), "product_name",
                                    F.col("category_id").cast("long").alias("category_id"),
                                    F.try_to_timestamp("valid_from").alias("valid_from"),
                                    F.try_to_timestamp("valid_to").alias("valid_to")))
        categories = (spark.read.option("header", True).csv(str(_latest_catalog("categories")))
                      .select(F.col("category_id").cast("long").alias("category_id"), "category_name"))
        pricing = (spark.read.option("header", True).csv(str(_latest_catalog("pricing")))
                   .select(F.col("product_id").cast("long").alias("product_id"),
                           F.col("price_cents").cast("long").alias("price_cents"), "currency",
                           F.try_to_timestamp("valid_from").alias("valid_from"),
                           F.try_to_timestamp("valid_to").alias("valid_to")))
        if products.where("product_id is null or category_id is null or valid_from is null").limit(1).count():
            raise ValueError("Invalid product catalog")
        if pricing.where("product_id is null or price_cents < 0 or currency != 'USD'").limit(1).count():
            raise ValueError("Invalid pricing catalog")

        event_paths = [base / item["source_path"] for path in manifests()
                       for base, item in [(path.parent, entry) for entry in json.loads(path.read_text())["files"]]
                       if item["source_path"].startswith("events/")]
        if event_paths:
            events = _read_json(spark, event_paths, SCHEMAS["events"])
            events = events.withColumn("occurred_at", F.try_to_timestamp("occurred_at"))
            event_valid = (F.col("event_id").isNotNull() & F.col("customer_id").isNotNull() &
                           F.col("occurred_at").isNotNull() &
                           F.col("event_type").isin("product_view", "add_to_cart", "checkout"))
            quarantine.append(events.where(~event_valid).withColumn("source_table", F.lit("events"))
                              .withColumn("reason", F.lit("invalid_event"))
                              .select("source_table", "reason", F.to_json(F.struct(*events.columns)).alias("payload")))
            events = _dedup(events.where(event_valid).drop("_corrupt_record"), "event_id", "occurred_at")
        else:
            events = spark.createDataFrame([], "event_id string,customer_id long,product_id long,event_type string,occurred_at timestamp")

        orders = snapshots["orders"].join(snapshots["customers"].select("customer_id"), "customer_id", "left_semi")
        bad_orders = snapshots["orders"].join(snapshots["customers"].select("customer_id"), "customer_id", "left_anti")
        quarantine.append(bad_orders.withColumn("source_table", F.lit("orders"))
                          .withColumn("reason", F.lit("missing_customer"))
                          .select("source_table", "reason", F.to_json(F.struct(*bad_orders.columns)).alias("payload")))
        items = snapshots["order_items"].join(orders.select("order_id"), "order_id", "left_semi")
        bad_items = snapshots["order_items"].join(orders.select("order_id"), "order_id", "left_anti")
        quarantine.append(bad_items.withColumn("source_table", F.lit("order_items"))
                          .withColumn("reason", F.lit("missing_order"))
                          .select("source_table", "reason", F.to_json(F.struct(*bad_items.columns)).alias("payload")))
        tx = snapshots["transactions"].join(orders.select("order_id"), "order_id", "left_semi")
        joined = (items.alias("i").join(orders.alias("o"), "order_id")
                  .join(products.alias("p"), (F.col("i.product_id") == F.col("p.product_id")) &
                        (F.col("o.ordered_at") >= F.col("p.valid_from")) &
                        ((F.col("p.valid_to").isNull()) | (F.col("o.ordered_at") < F.col("p.valid_to"))), "left"))
        bad_product = joined.where(F.col("p.product_id").isNull()).select("i.*")
        quarantine.append(bad_product.withColumn("source_table", F.lit("order_items"))
                          .withColumn("reason", F.lit("missing_product_version"))
                          .select("source_table", "reason", F.to_json(F.struct(*bad_product.columns)).alias("payload")))
        joined = joined.where(F.col("p.product_id").isNotNull()).select(
            F.col("i.order_item_id"), F.col("i.order_id"), F.col("o.customer_id"), F.col("i.product_id"),
            F.col("p.category_id"), F.col("o.ordered_at"), F.col("o.status").alias("order_status"),
            F.col("i.quantity"), F.col("i.unit_price_cents"), F.col("i.currency"),
            F.greatest(F.col("i.updated_at"), F.col("o.updated_at")).alias("source_updated_at"),
        )
        output = {**snapshots, "orders": orders, "order_items": items, "transactions": tx,
                  "products": products, "categories": categories, "pricing": pricing,
                  "events": events, "joined_order_items": joined}
        for name, frame in output.items():
            destination = pending / name
            if name == "events":
                frame = frame.withColumn("event_date", F.to_date("occurred_at"))
                frame.write.mode("overwrite").partitionBy("event_date").parquet(str(destination))
            elif name == "joined_order_items":
                frame = frame.withColumn("order_date", F.to_date("ordered_at"))
                frame.write.mode("overwrite").partitionBy("order_date").parquet(str(destination))
            else:
                frame.write.mode("overwrite").parquet(str(destination))
            counts[name] = frame.count()
        all_bad = quarantine[0]
        for frame in quarantine[1:]:
            all_bad = all_bad.unionByName(frame)
        if previous:
            all_bad = all_bad.unionByName(spark.read.parquet(str(PROCESSED / previous / "quarantine")))
        all_bad.write.mode("overwrite").parquet(str(pending / "quarantine"))
        counts["quarantine"] = all_bad.count()
        manifest = {"run_id": run_id, "previous_run_id": previous, "rebuild": rebuild, "counts": counts}
        write_json_atomic(pending / "manifest.json", manifest)
        os.replace(pending, final)
        write_json_atomic(PROCESSED / "latest.json", {"run_id": run_id})
        return final / "manifest.json"
    finally:
        spark.stop()
