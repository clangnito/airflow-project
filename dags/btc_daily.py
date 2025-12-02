from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import os
import json
import requests
from jsonschema import validate, ValidationError

# ----------------------------
# Paramètres
# ----------------------------
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

default_args = {
    "owner": "you",
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

# ----------------------------
# DAG
# ----------------------------
with DAG(
    "btc_daily_coingecko",
    start_date=datetime(2025, 11, 29),
    schedule="0 7 * * *",  # daily à 07:00
    catchup=False,
    default_args=default_args,
    tags=["btc", "coingecko", "daily"]
) as dag:

    # ----------------------------
    # Task 1: fetch API
    # ----------------------------
    @task(execution_timeout=timedelta(seconds=20))
    def fetch_bitcoin_price(**kwargs):
        params = {
            "vs_currency": "usd",
            "days": "30"  # récupère les 30 derniers jours
        }
        response = requests.get(COINGECKO_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data

    # ----------------------------
    # Task 2: validate JSON schema
    # ----------------------------
    @task
    def validate_schema(data: dict):
        BTC_SCHEMA = {
            "type": "object",
            "properties": {
                "prices": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": {"type": "number"}  # chaque élément doit être un nombre
                    }
                }
            },
            "required": ["prices"]
        }
        try:
            validate(instance=data, schema=BTC_SCHEMA)
        except ValidationError as e:
            raise ValueError(f"Schema invalid: {e}")
        return data

    # ----------------------------
    # Task 3: duplicate detection
    # ----------------------------
    @task
    def detect_duplicates(data, **kwargs):
        ds = kwargs['ds']
        filename = f"/opt/airflow/logs/btc_{ds}.json"
        if os.path.exists(filename):
            raise AirflowSkipException(f"File already exists: {filename}")
        return filename

    # ----------------------------
    # Task 4: save file locally
    # ----------------------------
    @task
    def save_file(data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved file: {filename}")

    # ----------------------------
    # Flow
    # ----------------------------
    raw = fetch_bitcoin_price()
    checked = validate_schema(raw)
    filepath = detect_duplicates(checked)
    save_file(checked, filepath)
