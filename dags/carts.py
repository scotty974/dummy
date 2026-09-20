from airflow.sdk import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from utils.get_data import get_data_from_api
import datetime
import json
from pathlib import Path
from datetime import timedelta

@dag(start_date=datetime.datetime(2021,1,1), schedule="@daily")
def ingest_dummy_carts():
    
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_bronze_carts",
        conn_id="warehouse_dummy",
        sql="sql/create_bronze_carts.sql"
    )
    
    
    get_carts = PythonOperator(
        task_id="fetch_carts",
        python_callable=get_data_from_api,
        op_kwargs={"variable_name":"api_dummy_carts","type":"carts", "run":"{{run_id}}"},
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    
    @task
    def ingest_data(carts):
        data = json.loads(Path(carts["path"]).read_text(encoding="utf-8"))
        records = data["carts"]
        
        rows = [
                (record["id"], json.dumps(record, ensure_ascii=False))
                for record in records
            ]
           
        hook = PostgresHook(postgres_conn_id="warehouse_dummy")
        # TODO - AJout d'un check si les product_id existe deja dans la db
        hook.insert_rows(
        table="bronze_carts",
        rows=rows,
        target_fields=["cart_id", "content"],
        commit_every=1000,
        replace=True,
        replace_index="cart_id", 
        )
        return f"{len(rows)} carts traites"
    
    ingest_task = ingest_data(get_carts.output)
    
    create_bronze >> get_carts >> ingest_task

ingest_dummy_carts()