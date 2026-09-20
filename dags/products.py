from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime
import requests
from pathlib import Path
import json



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
            return path
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
        
    @task()
    def ingest_db(path:str):
        hook = PostgresHook(postgres_conn_id="warehouse_dummy")
        data = Path(path).read_text()
        
        print(data)
    

    create_bronze >> ingest_db(get_products())

ingest_dummy_products()