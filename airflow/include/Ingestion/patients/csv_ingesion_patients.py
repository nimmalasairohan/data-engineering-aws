import pandas as pd
import boto3
from datetime import datetime
import os


# ---------- CONFIG ----------
file_path = r"/usr/local/airflow/include/data/synthea_sample_data_csv_latest/patients.csv"
bucket_name = "rohan-healthcare-project"

# ---------- READ ----------
df = pd.read_csv(file_path)

# Normalize columns
df.columns = [col.lower() for col in df.columns]

# ---------- VALIDATION ----------
if df.empty:
    raise ValueError(f"{file_path} is empty. Ingestion failed.")

required_cols = ['id', 'first']
missing_columns = [col for col in required_cols if col not in df.columns]

if missing_columns:
    raise ValueError(f"{file_path} is missing required columns: {missing_columns}")

# ---------- METADATA ----------
df['ingestion_time'] = pd.Timestamp.utcnow()
df['source'] = 'csv_patients'

# ---------- PREPARE FILE ----------
os.makedirs("temp_patients", exist_ok=True)

temp_file = "temp_patients/patients_csv_ingested.csv"
df.to_csv(temp_file, index=False)

# ---------- S3 PATH ----------
load_date = datetime.utcnow().strftime("%Y-%m-%d")

s3_key = f"bronze/csv/patients/load_date={load_date}/csv_patients.csv"

# ---------- UPLOAD ----------
s3_client = boto3.client('s3')

s3_client.upload_file(temp_file, bucket_name, s3_key)

print(f"Uploaded to s3://{bucket_name}/{s3_key}")