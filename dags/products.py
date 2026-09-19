from airflow.sdk import dag, task, Variable
import datetime
import requests


@dag(start_date=datetime.datetime(2021,1,1),schedule="@daily")
def ingest_dummy_products():
    
    @task()
    def get_products():
        URL = Variable.get("api_dummy_products", default=None)
        
        try:
            r = requests.get(URL)
            return r.json()
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


    get_products()

ingest_dummy_products()