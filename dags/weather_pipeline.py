from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import os

# ========== CONFIGURATION ==========
SNOWFLAKE_CONN_ID = "snowflake_conn"
DATA_DIR = "/opt/airflow/logs/data/weather"
FILE_NAME = "weather_full.json"

# Coordonnées de Paris
PARIS_LAT = 48.8566
PARIS_LON = 2.3522

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ========== DAG PRINCIPAL ==========
with DAG(
    "weather_pipeline_paris",
    start_date=datetime(2025, 11, 29),
    schedule="0 * * * *",  # Toutes les heures
    catchup=False,
    default_args=default_args,
    tags=["weather", "paris", "snowflake"],
    description="Pipeline complet : API météo Paris → Snowflake (FULL RELOAD)",
) as dag:

    # ========== TASK 1 : Récupération des données météo ==========
    @task
    def fetch_weather_data():
        """
        Appelle l'API Open-Meteo pour récupérer les données météo de Paris
        et sauvegarde le JSON dans /opt/airflow/logs/data/weather
        """
        print("🌤  ETAPE 1 : Récupération des données météo de Paris")
        
        # Construction de l'URL de l'API
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={PARIS_LAT}&longitude={PARIS_LON}"
            f"&hourly=temperature_2m,relativehumidity_2m"
            f"&past_days=7"  # 7 jours passés + prévisions
            f"&timezone=Europe/Paris"
        )
        
        print(f"📡 Appel API : {url}")
        
        # Appel à l'API
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Lève une exception si erreur HTTP
        
        weather_data = response.json()
        
        # Ajouter les coordonnées dans le JSON pour Snowflake
        weather_data['latitude'] = PARIS_LAT
        weather_data['longitude'] = PARIS_LON
        
        # Créer le répertoire s'il n'existe pas
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Chemin complet du fichier
        file_path = os.path.join(DATA_DIR, FILE_NAME)
        
        # Sauvegarde du JSON
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(weather_data, f, indent=2)
        
        print(f"✅ Fichier sauvegardé : {file_path}")
        print(f"📊 Nombre d'heures récupérées : {len(weather_data.get('hourly', {}).get('time', []))}")
        
        return file_path

    # ========== TASK 2 : Upload vers Snowflake Stage ==========
    @task
    def upload_to_snowflake_stage(file_path: str):
        """
        Upload le fichier JSON vers le stage Snowflake @airflow_stage
        """
        print("📤 ETAPE 2 : Upload du fichier vers Snowflake Stage")
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Commande PUT pour upload
        sql_put = f"PUT file://{file_path} @airflow_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
        
        print(f"🔧 Exécution : {sql_put}")
        hook.run(sql_put)
        
        print(f"✅ Fichier {FILE_NAME} uploadé vers @airflow_stage")
        
        return FILE_NAME

    # ========== TASK 3 : Truncate de la table weather_full ==========
    @task
    def truncate_weather_full():
        """
        Vide complètement la table weather_full avant le rechargement
        """
        print("🗑  ETAPE 3 : Truncate de la table weather_full")
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        sql_truncate = "TRUNCATE TABLE IF EXISTS weather_full;"
        
        print(f"🔧 Exécution : {sql_truncate}")
        hook.run(sql_truncate)
        
        print("✅ Table weather_full vidée")

    # ========== TASK 4 : Chargement dans table temporaire ==========
    @task
    def load_to_temp_table(file_name: str):
        """
        Charge le JSON brut dans une table temporaire
        """
        print("📥 ETAPE 4 : Chargement dans table temporaire")
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Créer une table temporaire pour stocker le JSON brut
        sql_create_temp = """
        CREATE OR REPLACE TEMPORARY TABLE weather_temp (
            data VARIANT
        );
        """
        
        print("🔧 Création de la table temporaire...")
        hook.run(sql_create_temp)
        
        # COPY INTO depuis le stage
        sql_copy = f"""
        COPY INTO weather_temp
        FROM @airflow_stage/{file_name}
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'ABORT_STATEMENT';
        """
        
        print(f"🔧 Chargement des données depuis @airflow_stage/{file_name}")
        hook.run(sql_copy)
        
        print("✅ Données chargées dans weather_temp")

    # ========== TASK 5 : Transformation et insertion dans weather_full ==========
    @task
    def transform_and_load():
        """
        Transforme les données JSON et les insère dans weather_full
        """
        print("🔄 ETAPE 5 : Transformation et insertion dans weather_full")
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        sql_insert = """
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
            CURRENT_TIMESTAMP() AS load_date
        FROM weather_temp,
             LATERAL FLATTEN(input => data:hourly.time) AS hour,
             LATERAL FLATTEN(input => data:hourly.temperature_2m) AS temperature,
             LATERAL FLATTEN(input => data:hourly.relativehumidity_2m) AS humidity
        WHERE hour.index = temperature.index
          AND hour.index = humidity.index;
        """
        
        print("🔧 Exécution de la transformation...")
        result = hook.run(sql_insert)
        
        # Compter le nombre de lignes insérées
        sql_count = "SELECT COUNT(*) as nb_rows FROM weather_full;"
        count_result = hook.get_first(sql_count)
        
        print(f"✅ Transformation terminée")
        print(f"📊 Nombre de lignes dans weather_full : {count_result[0] if count_result else 'N/A'}")

    # ========== TASK 6 : Nettoyage du stage (optionnel) ==========
    @task
    def cleanup_stage(file_name: str):
        """
        Supprime le fichier du stage Snowflake (optionnel, pour garder propre)
        """
        print("🧹 ETAPE 6 : Nettoyage du stage")
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        sql_remove = f"REMOVE @airflow_stage/{file_name};"
        
        print(f"🔧 Suppression de {file_name} du stage")
        hook.run(sql_remove)
        
        print("✅ Stage nettoyé")

    # ========== ORCHESTRATION DU PIPELINE ==========
    # Définir l'ordre d'exécution des tâches
    
    file_path = fetch_weather_data()
    file_name = upload_to_snowflake_stage(file_path)
    truncate = truncate_weather_full()
    load_temp = load_to_temp_table(file_name)
    transform = transform_and_load()
    cleanup = cleanup_stage(file_name)
    
    # Dépendances entre les tâches
    file_path >> file_name >> [truncate, load_temp]
    truncate >> transform
    load_temp >> transform
    transform >> cleanup