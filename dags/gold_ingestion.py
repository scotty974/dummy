from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from pathlib import Path
import datetime
import pendulum

@dag(start_date=pendulum.datetime(2021, 1, 1, tz="Europe/Paris"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
)
def ingest_gold():
    
    create_tables = SQLExecuteQueryOperator(
        task_id="create_gold_tables",
        conn_id="warehouse_dummy",
        sql="sql/create_gold.sql"
    )
    
    insert_by_categories = SQLExecuteQueryOperator(
        task_id="insert_by_categories",
        conn_id="warehouse_dummy",
        sql="sql/insert_gold_by_categories.sql"
    )
    
    insert_by_top_products = SQLExecuteQueryOperator(
        task_id="insert_by_top_products",
        conn_id="warehouse_dummy",
        sql="sql/insert_gold_by_top_products.sql"
    )
    
    
    create_tables >> [insert_by_categories,insert_by_top_products]
    
ingest_gold()