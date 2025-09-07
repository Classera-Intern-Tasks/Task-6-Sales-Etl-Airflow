
import os
from pathlib import Path
import pandas as pd
import psycopg2
from dotenv import load_dotenv

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


ENV = Path("/opt/airflow/.env")
load_dotenv(ENV)

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")           
DB_HOST = os.getenv("DB_HOST", "pg_source")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
TABLE   = os.getenv("TABLE_NAME", "sales_data")

CSV_PATH = Path("/opt/airflow") / os.getenv("CSV_FILE", "Sales_DataSet.csv")

def load_csv_to_postgres():
    assert CSV_PATH.exists(), f"CSV not found: {CSV_PATH}"

    df = pd.read_csv(CSV_PATH)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()

    cols_sql = ", ".join(f'{c} TEXT' for c in df.columns)
    cur.execute(f'DROP TABLE IF EXISTS {TABLE}')
    cur.execute(f'CREATE TABLE {TABLE} ({cols_sql})')

    tmp = CSV_PATH.with_suffix(".normalized.csv")
    df.to_csv(tmp, index=False)

    with open(tmp, "r", encoding="utf-8") as f:
        col_list = ", ".join(df.columns)
        cur.copy_expert(f"COPY {TABLE} ({col_list}) FROM STDIN WITH CSV HEADER", f)

    cur.close()
    conn.close()

with DAG(
    dag_id="insert_csv_to_postgres",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,         
    catchup=False,
    default_args={"owner": "airflow"},
    doc_md="Load Sales_DataSet.csv into Postgres (recreates table as TEXT columns).",
):
    PythonOperator(task_id="load_csv", python_callable=load_csv_to_postgres)
