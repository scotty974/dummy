from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime
import requests
from pathlib import Path
import json
from psycopg2.extras import execute_values



@dag(start_date=datetime.datetime(2021,1,1),schedule="@daily")
def ingest_dummy_products():
    
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_bronze",
        conn_id="warehouse_dummy",
        sql="sql/create_bronze.sql"
    )
    

    @task()
    def get_products(**context)->str:
        URL = Variable.get("api_dummy_products", default=None)
        ds = context["ds"]
        try:
            r = requests.get(f"{URL}?limit=0", timeout=30).json()
            path = f"/opt/airflow/data/bronze/products_{ds}.json"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(r))
            return {"path":path, "variable":"products"}
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    
    @task
    def ingest_data(products):
        data = json.loads(Path(products["path"]).read_text(encoding="utf-8"))
        records = data["products"]
        
        rows = [
                (record["id"], json.dumps(record, ensure_ascii=False))
                for record in records
            ]
           
        hook = PostgresHook(postgres_conn_id="warehouse_dummy")
        hook.insert_rows(
        table="bronze_products",
        rows=rows,
        target_fields=["product_id", "content"],
        commit_every=1000,
        replace=True,
        replace_index="product_id", 
        )
        return f"{len(rows)} produits traites"
        
    _get_products = get_products()
    ingest_task = ingest_data(_get_products)
    
    create_bronze >> _get_products >> ingest_task

ingest_dummy_products()