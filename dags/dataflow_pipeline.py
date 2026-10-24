"""Run the same pipeline commands used by the standalone CLI."""
from __future__ import annotations

import hashlib
import subprocess
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task


def _run_id() -> str:
    airflow_id = get_current_context()["run_id"]
    return "airflow_" + hashlib.sha256(airflow_id.encode()).hexdigest()[:20]


def _command(*args: str) -> None:
    subprocess.run(["dataflow", *args, "--run-id", _run_id()], check=True)


@dag(
    dag_id="dataflow_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["dataflow", "local"],
)
def pipeline():
    @task
    def extract():
        _command("ingest")

    @task
    def validate():
        _command("validate")

    @task
    def spark_process():
        _command("process")

    @task
    def warehouse_load():
        _command("load")

    @task
    def dbt_models_and_tests():
        _command("model")

    @task
    def publish_results():
        _command("publish")

    extract() >> validate() >> spark_process() >> warehouse_load() >> dbt_models_and_tests() >> publish_results()


pipeline()
