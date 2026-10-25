# DataFlow

DataFlow is a local e-commerce analytics warehouse. PostgreSQL holds application records, CSV and JSONL files simulate other sources, PySpark maintains clean Parquet snapshots, and dbt builds a DuckDB star schema. Airflow schedules the same pipeline commands available from the CLI. No cloud account is needed.

## Requirements

- Docker Desktop with at least 8 GB assigned to Docker (the host used for development has 16 GB RAM)
- Free disk space for images, raw batches, processed snapshots, and warehouse versions

The image uses Python 3.12, Java 17, and the pinned `requirements.lock`. Pipeline packages live in a separate virtual environment from Airflow. Host Python is not used.

## Quick start

```bash
cp .env.example .env
docker compose build airflow-init
docker compose up -d postgres airflow-apiserver airflow-scheduler airflow-dag-processor
docker compose run --rm --entrypoint dataflow airflow-init seed --orders 10000 --months 3
docker compose run --rm --entrypoint dataflow airflow-init run
docker compose run --rm --entrypoint dataflow airflow-init query "select * from mart_daily_sales order by order_date desc limit 5"
```

Airflow is at <http://localhost:8081>. The SimpleAuth username is `admin`; read its generated password with `docker compose exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated`. Unpause `dataflow_pipeline` after the first seed. The DAG runs daily while Docker is running and keeps one active warehouse run at a time.

The `airflow-init` service is used above as a short-lived CLI container. For an interactive shell, run `docker compose run --rm --entrypoint bash airflow-init`.

## Change and backfill scenarios

```bash
docker compose run --rm --entrypoint dataflow airflow-init seed --scenario updates
docker compose run --rm --entrypoint dataflow airflow-init run
docker compose run --rm --entrypoint dataflow airflow-init backfill --start-date 2026-08-01 --end-date 2026-08-31
docker compose run --rm --entrypoint dataflow airflow-init benchmark --sizes 10000 100000 500000
docker compose run --rm --entrypoint dataflow airflow-init benchmark-pipeline --sizes 10000 100000 500000
docker compose run --rm --entrypoint dataflow airflow-init verify-recovery
```

Choose dates inside the generated source period for the backfill command. It reconstructs source state from immutable raw batches and merges facts and daily sales for the selected business dates. The `updates` scenario changes payment and order states, adds a late event, versions one product's category, and creates one paid order in the new category.

`dataflow run --full` extracts all current PostgreSQL rows and performs a dbt full refresh. A baseline seed is allowed only when the source orders table is empty. For a fresh project, stop Compose and remove the project volumes and generated `data/` directory before reseeding. This removes local data.

## Data flow and grain

1. `ingest` reads source rows in `(updated_at, ID)` order and copies changed files to `data/raw/<run_id>/`. Each manifest records checksums and counts. File changes are compared with the latest archived version, so reverting a file to older contents creates a new raw batch. The source cursor advances after the immutable batch is published.
2. `validate` verifies checksums and row counts.
3. `process` validates, deduplicates, joins, and writes Parquet snapshots under `data/processed/<run_id>/`. Changed key buckets are rewritten, and unchanged Parquet files are hard-linked into the new immutable snapshot. Joined items are bucketed by order ID, so a changed order updates only the buckets it touches. Invalid rows are retained in an active `quarantine` with reasons; a valid correction clears its keyed error. Incremental runs replay every raw batch since the prior processed snapshot, including batches left by a failed processing run. Full and backfill runs replay raw history in ingestion-time order.
4. `load` copies the last published DuckDB file to a candidate and replaces its source tables with the current Parquet snapshot. Any quarantined records block this step.
5. `model` runs dbt models and tests, then checks fact uniqueness and order/item revenue reconciliation.
6. `publish` moves the validated candidate into `data/warehouse/versions/` and atomically updates `latest.json`. Queries resolve that pointer, so a failed run leaves the preceding warehouse published.

`fact_order_items` has one row per order item. `fact_orders` has one row per order. Merchandise revenue uses integer cents, counts only paid orders with settled payment and no settled refund, and excludes shipping and tax. `dim_product` is a type 2 dimension, so facts retain their purchase-time category. `dim_customer` shows current customer attributes. UTC dates and USD are used throughout.

Useful queries:

```sql
-- Monthly revenue by category
select month_start, category_name, sum(revenue_cents)/100.0 as revenue_usd
from mart_product_revenue group by 1,2 order by 1,2;

-- Repeat purchasers
select count(*) filter (where is_repeat_customer) as repeat_customers,
       count(*) as purchasing_customers
from mart_customer_repeat;

-- Retention by first-purchase cohort
select cohort_month, months_since_first_purchase, retention_rate
from mart_monthly_retention order by 1,2;
```

## Development and verification

```bash
docker compose run --rm --entrypoint pytest airflow-init -q
docker compose run --rm airflow-init dags list
```

`dataflow model` sets the dbt warehouse path and runs tests as part of `dbt build`. Benchmark results are written to ignored `benchmark_results/`. `benchmark` compares Spark and DuckDB on one aggregation. `benchmark-pipeline` creates an isolated PostgreSQL database and data directory per size, runs full, incremental, and full rebuild profiles, then checks that the incremental and rebuild warehouse totals match. It records stage times, throughput, DuckDB query latency, processed-row counts, and sampled container memory. The temporary databases and datasets are removed after each size unless `--keep-data` is set. `verify-recovery` runs a separate isolated exercise of invalid-event rejection, correction, idempotent rerun, a historical order change, and a date backfill.

All generated source, raw, processed, warehouse, logs, and benchmark files are ignored by Git. The repository stores only code and configuration.

### Measured local runs

See [BENCHMARKS.md](BENCHMARKS.md) for the full-pipeline measurements and their limits. These are single-machine runs, not scaling claims.
