from __future__ import annotations

import json
import resource
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

from dataflow.config import ROOT
from dataflow.util import utcnow, write_json_atomic


def _container_peak_bytes() -> int | None:
    path = Path("/sys/fs/cgroup/memory.peak")
    if path.exists():
        return int(path.read_text().strip())
    return None


def benchmark(sizes: list[int]) -> Path:
    output = ROOT / "benchmark_results"
    output.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master("local[2]").appName("DataFlowBenchmark")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    results = []
    try:
        for size in sizes:
            if size < 1:
                raise ValueError("Benchmark sizes must be positive")
            path = output / f"orders_{size}.parquet"
            table = pa.table({
                "order_id": pa.array(range(size), type=pa.int64()),
                "category_id": pa.array([i % 5 for i in range(size)], type=pa.int32()),
                "quantity": pa.array([i % 3 + 1 for i in range(size)], type=pa.int32()),
                "unit_price_cents": pa.array([500 + (i % 200) * 37 for i in range(size)], type=pa.int64()),
            })
            pq.write_table(table, path)
            start = time.perf_counter()
            spark_result = (spark.read.parquet(str(path))
                            .groupBy("category_id")
                            .agg(F.sum(F.col("quantity") * F.col("unit_price_cents")).alias("revenue_cents"))
                            .orderBy("category_id").collect())
            spark_seconds = time.perf_counter() - start
            con = duckdb.connect()
            con.execute("SET threads=2")
            start = time.perf_counter()
            duck_result = con.execute(
                "SELECT category_id, sum(quantity*unit_price_cents) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                [str(path)],
            ).fetchall()
            duck_seconds = time.perf_counter() - start
            con.close()
            assert [(r["category_id"], r["revenue_cents"]) for r in spark_result] == duck_result
            results.append({
                "rows": size, "spark_seconds": spark_seconds, "duckdb_seconds": duck_seconds,
                "spark_rows_per_second": size / spark_seconds,
                "duckdb_rows_per_second": size / duck_seconds,
                "python_peak_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "container_peak_bytes": _container_peak_bytes(),
            })
    finally:
        spark.stop()
    target = output / f"benchmark_{utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    write_json_atomic(target, {"created_at": utcnow().isoformat(), "sizes": sizes, "results": results,
                               "note": "Single warm process; Spark includes a DataFrame action, DuckDB includes query execution. Container peak includes the Spark JVM where cgroups are available; Python RSS does not."})
    return target
