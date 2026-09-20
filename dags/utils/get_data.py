import json
from airflow.sdk import Variable
import requests
from pathlib import Path
from airflow.exceptions import AirflowException

def get_data_from_api(variable_name,type, run):
        URL = Variable.get(variable_name, default=None)
        try:
            r = requests.get(f"{URL}?limit=0", timeout=30)
            
            r.raise_for_status()
            
            if not r.text.strip():
                raise AirflowException(
                    'Réponse API vide'
                )
            data = r.json()
            if not r:
                raise AirflowException(
                    "L'API à renvoyé une réponse vide"
                )
            
            path = f"/opt/airflow/data/bronze/products_{run}.json"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(data))
            return {"path":path, "variable":type}
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)