from airflow.sdk import dag, task
import datetime

@dag(start_date=datetime.datetime(2021,1,1),schedule="@daily")
def hello_dag():
    @task
    def hello():
        print("Hello")
        return {"Hello":"World"}
    
    hello()

hello_dag()