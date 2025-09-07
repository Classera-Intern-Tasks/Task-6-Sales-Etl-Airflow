import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from clickhouse_driver import Client
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv(Path("/opt/airflow/.env"))


DB_USER=os.getenv("DB_USER"); DB_PASS=os.getenv("DB_PASS"); DB_NAME=os.getenv("DB_NAME")
DB_HOST=os.getenv("DB_HOST","pg_source"); DB_PORT=int(os.getenv("DB_PORT","5432"))
PG_TABLE=os.getenv("TABLE_NAME","sales_data")


CH_HOST=os.getenv("CH_HOST","ch_dwh"); CH_PORT=int(os.getenv("CH_PORT","9000"))
CH_DB=os.getenv("CH_DB","dwh"); CH_USER=os.getenv("CH_USER","default")
CH_PASS=os.getenv("CH_PASS",""); CH_TABLE=os.getenv("CH_TABLE","sales_data")

BATCH=10000

def run_sync():
   
    pg = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

  
    kw = dict(host=CH_HOST, port=CH_PORT, user=CH_USER)
    if CH_PASS != "": kw["password"] = CH_PASS
    ch = Client(**kw)

   
    ch.execute(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

  
    with pg.cursor() as cur:
        cur.execute(f"SELECT * FROM {PG_TABLE} LIMIT 0")
        cols = [d.name for d in cur.description]

 
    ch.execute(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    ddl = ", ".join(f"`{c}` Nullable(String)" for c in cols)
    ch.execute(f"CREATE TABLE {CH_DB}.{CH_TABLE} ({ddl}) ENGINE=MergeTree ORDER BY tuple()")

   
    with pg.cursor(name="stream") as cur:
        cur.itersize = BATCH
        cur.execute(f"SELECT * FROM {PG_TABLE}")
        col_list = ", ".join(f"`{c}`" for c in cols)
        rows = cur.fetchmany(BATCH); total=0
        while rows:
            data = [tuple(None if v is None else str(v) for v in r) for r in rows]
            ch.execute(f"INSERT INTO {CH_DB}.{CH_TABLE} ({col_list}) VALUES", data)
            total += len(data)
            rows = cur.fetchmany(BATCH)

    pg.close()
    print(f"Loaded rows into ClickHouse: {total}")

with DAG(
    dag_id="postgres_to_clickhouse",
    start_date=pendulum.datetime(2025,1,1,tz="UTC"),
    schedule=None, catchup=False, default_args={"owner":"airflow"}
):
    PythonOperator(task_id="sync", python_callable=run_sync)
