import io
import os
import pandas as pd
from datetime import datetime
import json
from invoke import task
from minio import Minio

# Constants for MinIO configuration
MINIO_HOST = 'localhost:9050'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'dataops'

# Configuration constants 
DATA_DIR = os.path.expanduser('~/Quan/dataops-demo/code/data/batch_ingestion/')
CHECKPOINT_FILE = 'checkpoint.json'
BATCH_SIZE = 500

# Initialize MinIO client
minio_client = Minio(
    MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False
)

# Get a list of CSV files from the specified directory
def get_files():
    try:
        return [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    except FileNotFoundError as e:
        print(f"Error: {e}. Check if DATA_DIR is correct.")
        return []

# Read a specific batch of rows from a CSV file
def read_batch(file_path, start_row, batch_size):
    try:
        df = pd.read_csv(file_path)
        return df.iloc[start_row:start_row + batch_size]
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return pd.DataFrame()

# Update the checkpoint with the last processed row for a file
def update_checkpoint(file_name, last_row):
    checkpoint = load_checkpoint()
    checkpoint[file_name] = last_row
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)
    except Exception as e:
        print(f"Error updating checkpoint: {e}")

# Load the checkpoint from the checkpoint file
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
    return {}

# Save processed data to MinIO
def save_to_minio(file_name, data):
    data_io = io.BytesIO(data.encode())
    object_name = f"ingested_data/{file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    try:
        minio_client.put_object(MINIO_BUCKET, object_name, data_io, len(data_io.getvalue()))
        print(f"Successfully uploaded {object_name} to MinIO.")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

# Batch ingestion task
@task
def batch_ingestion(ctx):
    checkpoint = load_checkpoint()
    files = get_files()

    if not files:
        print("No files found for processing.")
        return

    for file_name in files:
        file_path = os.path.join(DATA_DIR, file_name)
        start_row = checkpoint.get(file_name, 0)

        # Process file in batches
        while True:
            df = read_batch(file_path, start_row, BATCH_SIZE)

            if df.empty:
                print(f"Completed processing {file_name}.")
                break

            save_to_minio(file_name, df.to_csv(index=False))
            start_row += len(df)
            update_checkpoint(file_name, start_row)

        print(f"File {file_name} processed successfully.")
