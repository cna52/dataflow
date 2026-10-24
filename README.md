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
```

Choose dates inside the generated source period for the backfill command. It reconstructs source state from immutable raw batches and merges facts and daily sales for the selected business dates. The `updates` scenario changes payment and order states, adds a late event, versions one product's category, and creates one paid order in the new category.

`dataflow run --full` extracts all current PostgreSQL rows and performs a dbt full refresh. A baseline seed is allowed only when the source orders table is empty. For a fresh project, stop Compose and remove the project volumes and generated `data/` directory before reseeding. This removes local data.

## Data flow and grain

1. `ingest` reads source rows in `(updated_at, ID)` order and copies changed files to `data/raw/<run_id>/`. Each manifest records checksums and counts. The source cursor advances after the immutable batch is published.
2. `validate` verifies checksums and row counts.
3. `process` validates, deduplicates, joins, and writes Parquet snapshots under `data/processed/<run_id>/`. Invalid rows are retained in `quarantine` with reasons. Every processed snapshot represents the latest known source state. Incremental runs combine the preceding snapshot with the new raw batch; full and backfill runs replay raw history.
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

`dataflow model` sets the dbt warehouse path and runs tests as part of `dbt build`. Benchmark results are written to ignored `benchmark_results/`. They compare Spark and DuckDB on the same generated Parquet input and record query time, rows per second, and process peak memory. The numbers are local measurements, not scaling claims.

All generated source, raw, processed, warehouse, logs, and benchmark files are ignored by Git. The repository stores only code and configuration.

### Measured local run

On a 16 GB Apple Silicon laptop, a 10,000-order seed produced 25,210 order items and 35,210 events. The first pipeline run took 17.6 seconds; an update batch with 91 changed orders, 56 changed transactions, and one late event took 17.0 seconds. The current suite passes 28 dbt tests with zero quarantined rows. Spark rewrites the current Parquet snapshot on each run, so incremental extraction and dbt merges did not yet make the whole pipeline faster at this size. A one-process aggregation benchmark across 10,000, 100,000, and 500,000 rows found DuckDB faster than local Spark; see the generated JSON in `benchmark_results/` for timings and memory figures. These are single-run measurements, not broad performance claims.
