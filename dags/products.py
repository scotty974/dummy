from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from utils.get_data import get_data_from_api
import datetime
from pathlib import Path
import json



@dag(start_date=datetime.datetime(2021,1,1),schedule="@daily")
def ingest_dummy_products():
    
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_bronze",
        conn_id="warehouse_dummy",
        sql="sql/create_bronze_products.sql"
    )
    
    get_products = PythonOperator(
        task_id="fetch_products",
        python_callable=get_data_from_api,
        op_kwargs={'variable_name':"api_dummy_products", 'type':"products", 'run':'{{run_id}}'}
    )
        
    
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
        
    
    ingest_task = ingest_data(get_products.output)
    
    create_bronze >> get_products >> ingest_task

ingest_dummy_products()