from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import urllib3
import psycopg2
import os

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Elasticsearch Config
ES_HOST_CONTAINER = "https://elasticsearch:9200"
ES_USER = "elastic"
ES_PASSWORD = "ehXJ=8SMElGSu531uaba"
INDEX_NAME = "usa_covid_cases"

# PostgreSQL Config
PG_HOST = "postgres"
PG_PORT = 5432
PG_DATABASE = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

# Checkpoint file path (make sure this path is accessible from your local machine)
CHECKPOINT_FILE = '/opt/airflow/checkpoints/postgres_checkpoint.json'

# Ensure the checkpoints directory exists
os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="etl_elasticsearch_to_postgres",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elasticsearch", "postgres", "etl"],
)
def etl_pipeline():

    @task(task_id='load_checkpoint')
    def load_checkpoint():
        # Load the last processed date from the checkpoint file
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                last_processed_date = json.load(f).get("last_processed_date")
                print(f"Loaded last processed date: {last_processed_date}")  # Debugging info
                return last_processed_date
        return None

    @task(task_id='save_checkpoint')
    def save_checkpoint(last_processed_date):
        # Save the last processed date to the checkpoint file
        print(f"Saving last processed date: {last_processed_date}")  # Debugging info
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({"last_processed_date": last_processed_date}, f)

    @task(task_id='extract_from_elasticsearch')
    def extract_from_elasticsearch(last_processed_date=None):
        url = f"{ES_HOST_CONTAINER}/{INDEX_NAME}/_search"
        query = {
            "query": {
                "range": {
                    "REPORT_DATE": {
                        "gt": last_processed_date if last_processed_date else "1970-01-01"
                    }
                }
            },
            "size": 1000  # Adjust size for batch processing
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            url,
            headers=headers,
            auth=(ES_USER, ES_PASSWORD),
            verify=False,
            json=query
        )
        if response.status_code != 200:
            raise Exception(f"Failed to read from Elasticsearch: {response.status_code}, {response.text}")
        hits = response.json().get("hits", {}).get("hits", [])
        print(f"Extracted {len(hits)} records from Elasticsearch")  # Debugging info
        return [hit["_source"] for hit in hits]

    @task(task_id='transform_data')
    def transform_data(records: list):
        transformed = []
        for rec in records:
            try:
                transformed.append({
                    "date": rec.get("REPORT_DATE"),
                    "state": rec.get("PROVINCE_STATE_NAME", "").strip().title(),
                    "county": rec.get("COUNTY_NAME", "").strip().title(),
                    "new_cases": int(rec.get("PEOPLE_POSITIVE_NEW_CASES_COUNT", 0)),
                    "new_deaths": int(rec.get("PEOPLE_DEATH_NEW_COUNT", 0)),
                })
            except Exception as e:
                print(f"Error transforming record: {rec}, error: {e}")  # Debugging info
                continue  # skip malformed entries
        print(f"Transformed {len(transformed)} records")  # Debugging info
        return transformed

    @task(task_id='load_to_postgres')
    def load_to_postgres(data: list):
        if not data:
            print("No data to process")  # Debugging info
            return  # No data to process

        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor()

        # Read and execute SQL from script file
        with open('./scripts/postgres_db_scripts.sql', 'r') as file:
            sql_script = file.read()

        cur.execute(sql_script)
        conn.commit()

        # Insert data in batches
        batch_size = 100  # Define your batch size
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            for row in batch:
                cur.execute(
                    "INSERT INTO covid_cases (date, state, county, new_cases, new_deaths) VALUES (%s, %s, %s, %s, %s)",
                    (row["date"], row["state"], row["county"], row["new_cases"], row["new_deaths"])
                )
            conn.commit()
            # Save checkpoint for the last processed date
            last_processed_date = batch[-1]["date"]
            save_checkpoint(last_processed_date)

        cur.close()
        conn.close()

    # Task chaining
    last_processed_date = load_checkpoint()
    raw = extract_from_elasticsearch(last_processed_date)
    clean = transform_data(raw)
    load_to_postgres(clean)

dag = etl_pipeline()