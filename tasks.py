from datetime import datetime
from invoke import task
import os
import pandas as pd
import json
import io
from minio import Minio

# Global configurations
DATA_DIR = os.path.expanduser("~/Quan/dataops-demo/code/data/batch_ingestion/")  # Root folder containing train/test data
CHECKPOINT_FILE = "checkpoint.json"  # File to store batch progress
MINIO_BUCKET = "batch-ingestion"  # MinIO bucket name
BATCH_SIZE = 500  # Number of rows to process per batch

# Initialize MinIO Client
minio_client = Minio(
    "localhost:9050",  # MinIO endpoint
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Ensure MinIO bucket exists
def ensure_minio_bucket():
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"Created MinIO bucket: {MINIO_BUCKET}")
    else:
        print(f"MinIO bucket {MINIO_BUCKET} already exists.")

# Load or initialize checkpoint
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}

# Update checkpoint file
def update_checkpoint(file_name, row):
    checkpoint = load_checkpoint()
    checkpoint[file_name] = row
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)

# Read a batch of rows from CSV file
def read_batch(file_path, start_row, batch_size):
    try:
        df = pd.read_csv(file_path, skiprows=range(1, start_row + 1), nrows=batch_size)
        return df
    except Exception as e:
        print(f"Error reading batch: {e}")
        return pd.DataFrame()

# Save processed data to MinIO with dynamic folder structure
def save_to_minio(file_name, subfolder, data, data_type='train'):
    """
    Upload CSV batch to MinIO with a dynamic folder structure based on ingestion time.
    Args:
        file_name (str): Name of the original file.
        subfolder (str): Subfolder like 'DDoS', 'DoS', etc.
        data (str): CSV data as string.
        data_type (str): 'train' or 'test'.
    """
    # Get the current timestamp for folder structure
    now = datetime.now()
    year, month, day, hour = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), now.strftime("%H")

    # Build the object path dynamically
    object_path = f"{data_type}/year={year}/month={month}/day={day}/hour={hour}/{subfolder}/{file_name}_{now.strftime('%Y%m%d%H%M%S')}.csv"

    # Upload to MinIO
    data_io = io.BytesIO(data.encode())
    try:
        minio_client.put_object(MINIO_BUCKET, object_path, data_io, len(data_io.getvalue()))
        print(f"Successfully uploaded {object_path} to MinIO.")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

@task
def batch_ingestion(ctx):
    """
    Perform batch ingestion of CSV files from train/test folders into MinIO.
    """
    ensure_minio_bucket()
    checkpoint = load_checkpoint()

    # Duyệt qua thư mục 'train' và 'test'
    for data_type in ['train', 'test']:
        data_type_path = os.path.join(DATA_DIR, data_type)

        # Duyệt qua các thư mục con (DDoS, DoS, MQTT, ...)
        for subfolder in os.listdir(data_type_path):
            subfolder_path = os.path.join(data_type_path, subfolder)

            # Kiểm tra xem có phải thư mục hợp lệ không
            if not os.path.isdir(subfolder_path):
                continue

            print(f"Processing folder: {subfolder_path}")
            files = [f for f in os.listdir(subfolder_path) if f.endswith('.csv')]
            for file_name in files:
                file_path = os.path.join(subfolder_path, file_name)
                start_row = checkpoint.get(file_name, 0)

                # Xử lý file theo batch
                while True:
                    df = read_batch(file_path, start_row, BATCH_SIZE)

                    if df.empty:
                        print(f"Completed processing {file_name} in {subfolder}.")
                        break

                    save_to_minio(file_name, subfolder, df.to_csv(index=False), data_type=data_type)
                    start_row += len(df)
                    update_checkpoint(file_name, start_row)

                print(f"File {file_name} in {subfolder} processed successfully.")

@task
def reset_checkpoint(ctx):
    """
    Reset checkpoint file to start processing from the beginning.
    """
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file removed. Starting fresh ingestion.")
    else:
        print("No checkpoint file found.")

@task
def list_minio_objects(ctx):
    """
    List all objects in the MinIO bucket.
    """
    objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
    print(f"Objects in bucket {MINIO_BUCKET}:")
    for obj in objects:
        print(f" - {obj.object_name} (Size: {obj.size} bytes)")
