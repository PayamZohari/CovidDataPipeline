from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import urllib3
import psycopg2


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

    @task()
    def extract_from_elasticsearch():
        url = f"{ES_HOST_CONTAINER}/{INDEX_NAME}/_search"
        query = {
            "query": {"match_all": {}},
            "size": 10000
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
        return [hit["_source"] for hit in hits]

    @task()
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
            except Exception:
                continue  # skip malformed entries
        return transformed

    @task()
    def load_to_postgres(data: list):
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor()

        # Create table if it doesn't exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS covid_cases (
            date DATE,
            state TEXT,
            county TEXT,
            new_cases INTEGER,
            new_deaths INTEGER
        );
        """)

        # Insert data
        for row in data:
            cur.execute(
                "INSERT INTO covid_cases (date, state, county, new_cases, new_deaths) VALUES (%s, %s, %s, %s, %s)",
                (row["date"], row["state"], row["county"], row["new_cases"], row["new_deaths"])
            )

        conn.commit()
        cur.close()
        conn.close()

    # Task chaining
    raw = extract_from_elasticsearch()
    clean = transform_data(raw)
    load_to_postgres(clean)

dag = etl_pipeline()
