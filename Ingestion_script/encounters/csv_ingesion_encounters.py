import pandas as pd
import boto3
from datetime import datetime
import os

# ---------- CONFIG ----------
file_path = "D:\\rohan_heath_care_project\\data-engineering-aws\\data\\synthea_sample_data_csv_latest\\encounters.csv"
bucket_name = "rohan-healthcare-project"

# ---------- READ ----------
df = pd.read_csv(file_path)

# Normalize columns
df.columns = [col.lower() for col in df.columns]

# ---------- VALIDATION ----------
if df.empty:
    raise ValueError(f"{file_path} is empty. Ingestion failed.")

required_cols = ['id', 'patient']
missing_columns = [col for col in required_cols if col not in df.columns]

if missing_columns:
    raise ValueError(f"{file_path} is missing required columns: {missing_columns}")

# ---------- METADATA ----------
df['ingestion_time'] = pd.Timestamp.utcnow()
df['source'] = 'csv_encounters'

# ---------- PREPARE FILE ----------
os.makedirs("temp_encounters", exist_ok=True)

temp_file = "temp_encounters/csv_encounters.csv"
df.to_csv(temp_file, index=False)

# ---------- S3 PATH ----------
load_date = datetime.utcnow().strftime("%Y-%m-%d")

s3_key = f"bronze/csv/encounters/load_date={load_date}/csv_encounters.csv"

# ---------- UPLOAD ----------
s3_client = boto3.client('s3')

s3_client.upload_file(temp_file, bucket_name, s3_key)

print(f"Uploaded to s3://{bucket_name}/{s3_key}")