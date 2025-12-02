from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "weather_to_snowflake_full",
    start_date=datetime(2025, 11, 29),
    schedule="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["weather", "snowflake", "full", "truncate"],
) as dag:

    @task
    def truncate_weather_full():
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        hook.run("TRUNCATE TABLE weather_full;")
    

    @task
    def copy_full_file_into_incremental(ds=None):
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)

        file_name = f"weather_full.json"

        sql = f"""
        COPY INTO weather_incremental
        FROM @airflow_stage/{file_name}
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE';
        """

        hook.run(sql)


    @task
    def load_weather_full():
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sql = """
        INSERT INTO weather_full (
            ts,
            temperature,
            humidity,
            latitude,
            longitude,
            load_date
        )
        SELECT
            TO_TIMESTAMP(hour.value::string) AS ts,
            temperature.value::float AS temperature,
            humidity.value::float AS humidity,
            data:latitude::float AS latitude,
            data:longitude::float AS longitude,
            CURRENT_TIMESTAMP()
        FROM weather_incremental,
             LATERAL FLATTEN(input => data:hourly.time) AS hour,
             LATERAL FLATTEN(input => data:hourly.temperature_2m) AS temperature,
             LATERAL FLATTEN(input => data:hourly.relativehumidity_2m) AS humidity
        WHERE hour.index = temperature.index
          AND hour.index = humidity.index;
        """

        hook.run(sql)


    # Workflow
    t1 = truncate_weather_full()
    t2 = copy_full_file_into_incremental()
    t3 = load_weather_full()

    t1 >> t2 >> t3
