#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U postgres -d postgres -v dataflow_password="$DATAFLOW_PASSWORD" -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE USER dataflow WITH PASSWORD :'dataflow_password';
CREATE USER airflow WITH PASSWORD :'airflow_password';
CREATE DATABASE dataflow OWNER dataflow;
CREATE DATABASE airflow OWNER airflow;
SQL
psql -v ON_ERROR_STOP=1 -U dataflow -d dataflow -f /docker-entrypoint-initdb.d/source.sql
