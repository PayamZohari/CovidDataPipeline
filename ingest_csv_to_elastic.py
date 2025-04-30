import csv
import requests
from time import sleep
import urllib3
import os
import json
from decouple import config

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Elasticsearch settings
ES_HOST = config("ES_HOST_OUT")
ES_USER = config("ES_USER")
ES_PASSWORD = config("ES_PASSWORD")
INDEX_NAME = config("INDEX_NAME")

CSV_FILE_PATH = "datasource/COVID-19-Activity.csv"
CHECKPOINT_FILE = "elastic_checkpoint.json"
BATCH_SIZE = 100

# Function to read checkpoint
def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"last_processed_row": 0}

# Function to update checkpoint
def update_checkpoint(last_processed_row):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"last_processed_row": last_processed_row}, f)

# Check if inde ingest_csv:
def index_exists():
    url = f"{ES_HOST}/{INDEX_NAME}"
    response = requests.head(url, auth=(ES_USER, ES_PASSWORD), verify=False)
    return response.status_code == 200

# Create the index if not exists
def create_index():
    if index_exists():
        print(f"Index {INDEX_NAME} already exists, skipping creation.")
        return

    url = f"{ES_HOST}/{INDEX_NAME}"
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    response = requests.put(url, json=settings, auth=(ES_USER, ES_PASSWORD), verify=False)
    print(f"Create index status: {response.status_code}, {response.text}")

# Truncate the index
def truncate_index():
    if not index_exists():
        print(f"Index {INDEX_NAME} does not exist, nothing to truncate.")
        return

    url = f"{ES_HOST}/{INDEX_NAME}/_delete_by_query"
    query = {
        "query": {
            "match_all": {}
        }
    }
    response = requests.post(url, json=query, auth=(ES_USER, ES_PASSWORD), verify=False)
    print(f"Truncate status: {response.status_code}, {response.text}")

# Ingest data in batches
def ingest_data():
    checkpoint = read_checkpoint()
    last_processed_row = checkpoint["last_processed_row"]

    # Read all rows into memory once to count them
    with open(CSV_FILE_PATH, mode="r") as file:
        reader = list(csv.DictReader(file))
        total_rows = len(reader)
        print(f"Total rows in CSV: {total_rows}")

    batch = []
    batch_number = 1  # Start batch numbering from 1
    for row_index, row in enumerate(reader):
        if row_index < last_processed_row:
            continue  # Skip already-processed rows

        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            print(f"Inserting Batch {batch_number} (rows {row_index - BATCH_SIZE + 2} to {row_index + 1})...")
            bulk_insert(batch, batch_number)
            batch = []
            batch_number += 1
            last_processed_row = row_index + 1
            update_checkpoint(last_processed_row)
            sleep(1)

    if batch:
        print(f"Inserting Final Batch {batch_number} (rows {total_rows - len(batch) + 1} to {total_rows})...")
        bulk_insert(batch, batch_number)
        update_checkpoint(row_index + 1)

def bulk_insert(batch, batch_number):
    bulk_data = ""
    for doc in batch:
        bulk_data += json.dumps({"index": {}}) + "\n"
        bulk_data += json.dumps(doc) + "\n"

    url = f"{ES_HOST}/{INDEX_NAME}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}
    response = requests.post(url, data=bulk_data, headers=headers, auth=(ES_USER, ES_PASSWORD), verify=False)
    print(f"Batch {batch_number} insert status: {response.status_code}")

if __name__ == "__main__":
    truncate_index()  # Remove old data
    create_index()    # Create if not exists
    ingest_data()     # Start ingestion
