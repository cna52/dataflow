"""Immutable, hash-bucketed Parquet state shared by incremental Spark runs."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

BUCKET_COUNT = 256


def _bucket(frame: DataFrame, key: str) -> DataFrame:
    return frame.withColumn("bucket", F.pmod(F.xxhash64(F.col(key)), F.lit(BUCKET_COUNT)))


def _dedup(frame: DataFrame, key: str, updated: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(F.col(updated).desc_nulls_last())
    return frame.withColumn("_rank", F.row_number().over(window)).where("_rank=1").drop("_rank")


def _link_unchanged(previous: Path, destination: Path, changed: set[int]) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for bucket_dir in previous.glob("bucket=*"):
        if int(bucket_dir.name.split("=", 1)[1]) not in changed:
            shutil.copytree(bucket_dir, destination / bucket_dir.name, copy_function=os.link)


def update_state(
    spark: SparkSession,
    incoming: DataFrame,
    key: str,
    updated: str,
    destination: Path,
    previous: Path | None = None,
    replace_ids: list[int | str] | None = None,
    bucket_key: str | None = None,
) -> tuple[DataFrame, dict[str, int]]:
    """Replace only touched key buckets; hard-link untouched immutable files."""
    bucket_key = bucket_key or key
    incoming = _bucket(incoming, bucket_key).cache()
    changed = {int(row.bucket) for row in incoming.select("bucket").distinct().collect()}
    if replace_ids:
        replace_buckets = _bucket(
            spark.createDataFrame([(value,) for value in replace_ids],
                                  f"{bucket_key} {incoming.schema[bucket_key].dataType.simpleString()}"),
            bucket_key,
        )
        changed.update(int(row.bucket) for row in replace_buckets.select("bucket").distinct().collect())
    destination.mkdir(parents=True, exist_ok=True)
    old_parts = []
    if previous is not None:
        _link_unchanged(previous, destination, changed)
        old_parts = [str(previous / f"bucket={number}") for number in changed if (previous / f"bucket={number}").exists()]
    if old_parts:
        old = spark.read.parquet(*old_parts)
        # Reading individual partition paths omits the directory-derived bucket column.
        old = _bucket(old, bucket_key)
        if replace_ids:
            old = old.where(~F.col(bucket_key).isin(replace_ids))
        changed_rows = old.unionByName(incoming)
    else:
        changed_rows = incoming
    merged = _dedup(changed_rows, key, updated)
    written_rows = merged.count() if changed else 0
    if changed:
        merged.write.mode("append").partitionBy("bucket").parquet(str(destination))
    if not any(destination.rglob("*.parquet")):
        incoming.limit(0).write.mode("append").parquet(str(destination))
    frame = spark.read.parquet(str(destination))
    return frame, {"input_rows": incoming.count(), "changed_buckets": len(changed),
                   "written_rows": written_rows}
