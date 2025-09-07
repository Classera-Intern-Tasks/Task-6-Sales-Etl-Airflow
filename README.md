# Task-6-Sales-Etl-Airflow
An end-to-end ETL built with **Apache Airflow** (Docker) that loads a CSV of sales records into **PostgreSQL** and then syncs it into **ClickHouse** for analytics.

**Flow:** `Sales_DataSet.csv → Postgres (salesdb.sales_data) → ClickHouse (dwh.sales_data)`

---

## Stack

- **Airflow 2.9 (Py3.11)** – web UI on **http://localhost:8088**
- **PostgreSQL 16** (service name: `pg_source`, port `5432`)
- **ClickHouse** (service name: `ch_dwh`, ports `9000` TCP / `8123` HTTP)
- Python libs: `psycopg2`, `pandas`, `clickhouse-driver`

---

## Repository structure
```
.
├─ docker-compose.yml
├─ .env # environment variables (not committed)
├─ Sales_DataSet.csv # source CSV (kept local)
├─ dags/
│ ├─ csv_to_postgres.py # DAG 1: CSV → Postgres
│ └─ postgres_to_clickhouse.py # DAG 2: Postgres → ClickHouse
```
## What the DAGs do

### csv_to_postgres.py

- Reads Sales_DataSet.csv

- Normalizes column names → lower_snake_case

- Drops/creates salesdb.sales_data (all columns TEXT)

- Bulk-loads via COPY (fast)

### postgres_to_clickhouse.py

- Streams rows from Postgres with a server-side cursor

- Drops/creates dwh.sales_data as MergeTree ORDER BY tuple()

- Columns Nullable(String), batched inserts via clickhouse-driver
