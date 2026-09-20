import json
from airflow.sdk import Variable
import requests
from pathlib import Path

def get_data_from_api(variable_name,type, run):
        URL = Variable.get(variable_name, default=None)
        try:
            r = requests.get(f"{URL}?limit=0", timeout=30).json()
            path = f"/opt/airflow/data/bronze/products_{run}.json"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(r))
            return {"path":path, "variable":type}
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)