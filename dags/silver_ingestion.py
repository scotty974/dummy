from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime

@dag(start_date=datetime.datetime(2021,1,1), schedule="@daily")
def ingest_silver():
    
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables_silver",
        conn_id="warehouse_dummy",
        sql="sql/create_silver_table.sql"
    )
    
    insert_products_silver = SQLExecuteQueryOperator(
        task_id="insert_silver_products",
        conn_id="warehouse_dummy",
        sql="sql/insert_silver_products.sql"
    )
    
    insert_carts_silver = SQLExecuteQueryOperator(
        task_id="insert_silver_carts",
        conn_id="warehouse_dummy",
        sql="sql/insert_silver_carts.sql"
    )
    
    ingest_carts_items_silver = SQLExecuteQueryOperator(
        task_id="insert_carts_items_silver",
        conn_id="warehouse_dummy",
        sql="sql/insert_silver_cart_items.sql"
    )
    
    create_tables >> [insert_products_silver, insert_carts_silver] >> ingest_carts_items_silver

ingest_silver()
        