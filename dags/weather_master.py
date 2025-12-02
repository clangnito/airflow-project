from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

with DAG(
    "weather_master",
    start_date=datetime(2025, 11, 29),
    schedule="0 * * * *",
    catchup=False,
    tags=["weather", "pipeline"]
) as dag:

    @task
    def weather_full_paris():
        print("== ETAPE 1 : Weather FULL Paris ==")
        # Ici tu mets ton traitement FULL
        return "full_done"

    @task
    def weather_to_snowflake_full():
        print("== ETAPE 2 : Weather → Snowflake FULL ==")
        # Ici tu mets ton traitement Snowflake
        return "snowflake_loaded"

    step1 = weather_full_paris()
    step2 = weather_to_snowflake_full()

    step1 >> step2
