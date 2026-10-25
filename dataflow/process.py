"""Incremental Spark cleaning, joins, and active data-quality state."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from dataflow.config import PROCESSED, RAW, TABLE_KEYS, ensure_dirs
from dataflow.ingest import manifests, validate_raw
from dataflow.state import BUCKET_COUNT, update_state
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


def _latest_catalog(name: str) -> Path:
    for manifest_path in reversed(manifests()):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        relative = f"catalog/{name}.csv"
        if any(item["source_path"] == relative for item in manifest["files"]):
            return manifest_path.parent / relative
    raise FileNotFoundError(f"No archived catalog/{name}.csv")


def _quarantine(frame: DataFrame, table: str, key: str, reason: str) -> DataFrame:
    return frame.select(
        F.lit(table).alias("source_table"),
        F.col(key).cast("string").alias("source_key"),
        F.lit(reason).alias("reason"),
        F.to_json(F.struct(*frame.columns)).alias("payload"),
    )


def _latest_rows(frame: DataFrame, key: str, timestamp: str) -> DataFrame:
    ranked = frame.where(F.col(key).isNotNull())
    window = Window.partitionBy(key).orderBy(F.col(timestamp).desc_nulls_last())
    ranked = ranked.withColumn("_rank", F.row_number().over(window)).where("_rank=1").drop("_rank")
    return ranked.unionByName(frame.where(F.col(key).isNull()))


def _changed_products(current: DataFrame, previous: Path | None, spark: SparkSession) -> list[int]:
    if previous is None:
        return []
    old = spark.read.parquet(str(previous / "products"))
    columns = ["product_id", "category_id", "valid_from", "valid_to"]
    changed = current.select(*columns).exceptAll(old.select(*columns)).select("product_id")
    changed = changed.unionByName(old.select(*columns).exceptAll(current.select(*columns)).select("product_id"))
    return [row.product_id for row in changed.distinct().collect() if row.product_id is not None]


def _orders_with_changed_product_version(
    items: DataFrame, orders: DataFrame, products: DataFrame,
    previous: Path, product_ids: list[int], spark: SparkSession,
) -> DataFrame:
    """Find purchases whose effective product category/version actually changed."""
    relevant = (items.where(F.col("product_id").isin(product_ids))
                .join(orders.select("order_id", "ordered_at"), "order_id"))

    def effective(catalog: DataFrame, prefix: str) -> DataFrame:
        return (relevant.alias("i")
                .join(catalog.alias("p"),
                      (F.col("i.product_id") == F.col("p.product_id")) &
                      (F.col("i.ordered_at") >= F.col("p.valid_from")) &
                      (F.col("p.valid_to").isNull() | (F.col("i.ordered_at") < F.col("p.valid_to"))),
                      "left")
                .select(F.col("i.order_item_id"), F.col("i.order_id"),
                        F.col("p.category_id").alias(f"{prefix}_category"),
                        F.col("p.valid_from").alias(f"{prefix}_version_start")))

    old = effective(spark.read.parquet(str(previous / "products")), "old")
    new = effective(products, "new")
    compared = old.join(new, ["order_item_id", "order_id"], "full_outer")
    changed = compared.where(
        ~(F.col("old_category").eqNullSafe(F.col("new_category")) &
          F.col("old_version_start").eqNullSafe(F.col("new_version_start")))
    )
    return changed.select("order_id").distinct()


def _raw_batches(run_id: str, previous_id: str | None, rebuild: bool) -> list[Path]:
    history = manifests()
    run_ids = [path.parent.name for path in history]
    current_index = run_ids.index(run_id)
    if rebuild or previous_id is None:
        return [path.parent for path in history[:current_index + 1]]
    previous_index = run_ids.index(previous_id)
    if current_index <= previous_index:
        raise ValueError(f"Raw batch {run_id} does not follow processed batch {previous_id}")
    return [path.parent for path in history[previous_index + 1:current_index + 1]]


def _event_input(spark: SparkSession, raw_batches: list[Path]) -> DataFrame:
    frames: list[DataFrame] = []
    for sequence, batch in enumerate(raw_batches):
        manifest_path = batch / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for item in manifest["files"]:
            if item["source_path"].startswith("events/"):
                frame = _read_json(spark, [manifest_path.parent / item["source_path"]], SCHEMAS["events"])
                frames.append(frame.withColumn("_ingest_sequence", F.lit(sequence)))
    if not frames:
        return spark.createDataFrame([], SCHEMAS["events"] + ",_ingest_sequence int")
    events = frames[0]
    for frame in frames[1:]:
        events = events.unionByName(frame)
    return _latest_rows(events, "event_id", "_ingest_sequence")


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
    latest_path = PROCESSED / "latest.json"
    previous_id = None if rebuild or not latest_path.exists() else json.loads(latest_path.read_text())["run_id"]
    previous = PROCESSED / previous_id if previous_id else None
    if previous and json.loads((previous / "manifest.json").read_text()).get("schema_version") != 3:
        rebuild, previous, previous_id = True, None, None
    pending = PROCESSED / f".{run_id}.pending"
    if pending.exists():
        shutil.rmtree(pending)
    pending.mkdir(parents=True)
    spark = _spark()
    counts: dict[str, int] = {}
    metrics: dict[str, dict[str, int]] = {}
    try:
        snapshots: dict[str, DataFrame] = {}
        new_clean: dict[str, DataFrame] = {}
        bad: list[DataFrame] = []
        raw_batches = _raw_batches(run_id, previous_id, rebuild)
        if not rebuild:
            for skipped in raw_batches[:-1]:
                validate_raw(skipped.name)
        for table, key in TABLE_KEYS.items():
            incoming = _read_json(spark, [batch / f"{table}.jsonl" for batch in raw_batches], SCHEMAS[table])
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
            incoming = _latest_rows(incoming, key, "updated_at")
            valid = F.coalesce(valid, F.lit(False))
            bad.append(_quarantine(incoming.where(~valid), table, key, "invalid_schema_or_value"))
            clean = incoming.where(valid).drop("_corrupt_record")
            new_clean[table] = clean.cache()
            snapshots[table], metrics[table] = update_state(
                spark, clean, key, "updated_at", pending / table,
                previous / table if previous else None,
            )
            snapshots[table] = snapshots[table].cache()
            counts[table] = snapshots[table].count()

        products = (spark.read.option("header", True).csv(str(_latest_catalog("products")))
                    .select(F.col("product_id").cast("long").alias("product_id"), "product_name",
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
        changed_products = _changed_products(products, previous, spark)
        for name, frame in (("products", products), ("categories", categories), ("pricing", pricing)):
            frame.write.mode("overwrite").parquet(str(pending / name))
            counts[name] = frame.count()

        event_input = _event_input(spark, raw_batches)
        event_input = event_input.withColumn("occurred_at", F.try_to_timestamp("occurred_at"))
        event_valid = F.coalesce(
            F.col("event_id").isNotNull() & F.col("customer_id").isNotNull() &
            F.col("occurred_at").isNotNull() &
            F.col("event_type").isin("product_view", "add_to_cart", "checkout"), F.lit(False))
        bad.append(_quarantine(event_input.where(~event_valid), "events", "event_id", "invalid_event"))
        clean_events = event_input.where(event_valid).drop("_corrupt_record", "_ingest_sequence")
        # A corrected event supersedes its previous version even when its
        # occurred_at timestamp moves backwards.
        event_replace_ids = ([row.event_id for row in clean_events.select("event_id").collect()]
                             if previous else None)
        events, metrics["events"] = update_state(
            spark, clean_events, "event_id", "occurred_at", pending / "events",
            previous / "events" if previous else None, event_replace_ids,
        )
        counts["events"] = events.count()

        customers, orders, items, transactions = (snapshots[name] for name in TABLE_KEYS)
        valid_orders = orders.join(customers.select("customer_id"), "customer_id", "left_semi")
        bad.append(_quarantine(orders.join(customers.select("customer_id"), "customer_id", "left_anti"),
                               "orders", "order_id", "missing_customer"))
        valid_items = items.join(valid_orders.select("order_id"), "order_id", "left_semi")
        bad.append(_quarantine(items.join(valid_orders.select("order_id"), "order_id", "left_anti"),
                               "order_items", "order_item_id", "missing_order"))
        bad.append(_quarantine(transactions.join(valid_orders.select("order_id"), "order_id", "left_anti"),
                               "transactions", "transaction_id", "missing_order"))

        affected_ids: list[int] | None = None
        if previous:
            affected = new_clean["orders"].select("order_id")
            affected = affected.unionByName(new_clean["order_items"].select("order_id"))
            affected = affected.unionByName(new_clean["transactions"].select("order_id"))
            affected = affected.unionByName(
                orders.join(new_clean["customers"].select("customer_id"), "customer_id").select("order_id"))
            if changed_products:
                changed_product_orders = _orders_with_changed_product_version(
                    items, orders, products, previous, changed_products, spark)
                metrics["catalog"] = {"affected_orders": changed_product_orders.count()}
                affected = affected.unionByName(changed_product_orders)
            affected_ids = [row.order_id for row in affected.distinct().collect() if row.order_id is not None]
            metrics["joined_order_items"] = {"affected_orders": len(affected_ids)}
            join_orders = valid_orders.where(F.col("order_id").isin(affected_ids))
            join_items = valid_items.where(F.col("order_id").isin(affected_ids))
        else:
            join_orders, join_items = valid_orders, valid_items
            metrics["joined_order_items"] = {"affected_orders": valid_orders.count()}
        joined = (join_items.alias("i").join(join_orders.alias("o"), "order_id")
                  .join(products.alias("p"), (F.col("i.product_id") == F.col("p.product_id")) &
                        (F.col("o.ordered_at") >= F.col("p.valid_from")) &
                        (F.col("p.valid_to").isNull() | (F.col("o.ordered_at") < F.col("p.valid_to"))), "left"))
        missing_product = joined.where(F.col("p.product_id").isNull()).select("i.*")
        bad.append(_quarantine(missing_product, "order_items", "order_item_id", "missing_product_version"))
        joined_delta = joined.where(F.col("p.product_id").isNotNull()).select(
            F.col("i.order_item_id"), F.col("i.order_id"), F.col("o.customer_id"), F.col("i.product_id"),
            F.col("p.category_id"), F.col("o.ordered_at"), F.to_date(F.col("o.ordered_at")).alias("order_date"),
            F.col("o.status").alias("order_status"), F.col("i.quantity"), F.col("i.unit_price_cents"),
            F.col("i.currency"), F.greatest(F.col("i.updated_at"), F.col("o.updated_at")).alias("source_updated_at"),
        )
        joined_state, join_metrics = update_state(
            spark, joined_delta, "order_item_id", "source_updated_at", pending / "joined_order_items",
            previous / "joined_order_items" if previous else None, affected_ids, "order_id",
        )
        metrics["joined_order_items"].update(join_metrics)
        counts["joined_order_items"] = joined_state.count()

        active_bad = bad[0]
        for frame in bad[1:]:
            active_bad = active_bad.unionByName(frame)
        if previous:
            prior_bad = spark.read.parquet(str(previous / "quarantine"))
            prior_bad = prior_bad.where(~F.col("reason").isin("missing_customer", "missing_order"))
            resolved = None
            for table, key in TABLE_KEYS.items():
                keys = new_clean[table].select(F.lit(table).alias("source_table"),
                                               F.col(key).cast("string").alias("source_key"))
                resolved = keys if resolved is None else resolved.unionByName(keys)
            event_keys = clean_events.select(F.lit("events").alias("source_table"),
                                             F.col("event_id").alias("source_key"))
            joined_keys = joined_delta.select(F.lit("order_items").alias("source_table"),
                                              F.col("order_item_id").cast("string").alias("source_key"))
            resolved = resolved.unionByName(event_keys).unionByName(joined_keys).distinct()
            prior_bad = prior_bad.join(resolved, ["source_table", "source_key"], "left_anti")
            active_bad = active_bad.unionByName(prior_bad)
        active_bad = active_bad.dropDuplicates(["source_table", "source_key", "reason", "payload"])
        active_bad.write.mode("overwrite").parquet(str(pending / "quarantine"))
        counts["quarantine"] = active_bad.count()
        manifest = {"run_id": run_id, "previous_run_id": previous_id, "rebuild": rebuild,
                    "schema_version": 3, "bucket_count": BUCKET_COUNT, "counts": counts, "metrics": metrics}
        write_json_atomic(pending / "manifest.json", manifest)
        os.replace(pending, final)
        write_json_atomic(PROCESSED / "latest.json", {"run_id": run_id})
        return final / "manifest.json"
    except BaseException:
        shutil.rmtree(pending, ignore_errors=True)
        raise
    finally:
        spark.stop()
