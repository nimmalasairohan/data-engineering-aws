import json
import boto3
import pandas as pd
import os
from datetime import datetime


# ---------- PATH ----------
folder_path = r"/usr/local/airflow/include/data/synthea_sample_data_fhir_latest"
bucket_name = "rohan-healthcare-project"
encounters = []

# ---------- LOOP FILES ----------
for file_name in os.listdir(folder_path):

    if file_name.endswith(".json"):

        file_path = os.path.join(folder_path, file_name)

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

            entries = data.get("entry", [])

            # ---------- LOOP ENTRIES ----------
            for entry in entries:

                resource = entry.get("resource", {})

                # Only Encounter resources
                if resource.get("resourceType") == "Encounter":

                    # ---------- BASIC ----------
                    encounter_id = resource.get("id", "")
                    status = resource.get("status", "")

                    # ---------- SUBJECT ----------
                    subject = resource.get("subject", {})

                    patient_reference = subject.get("reference", "")
                    patient_id = patient_reference.replace("urn:uuid:", "")

                    patient_name = subject.get("display", "")

                    # ---------- PERIOD ----------
                    period = resource.get("period", {})

                    start_time = period.get("start", "")
                    end_time = period.get("end", "")

                    # ---------- CLASS ----------
                    encounter_class = (
                        resource.get("class", {})
                        .get("code", "")
                    )

                    # ---------- TYPE ----------
                    # ---------- TYPE ----------
                    encounter_type = ""
                    encounter_type_code = ""

                    type_list = resource.get("type", [])

                    if len(type_list) > 0:

                        coding_list = type_list[0].get("coding", [])

                        if len(coding_list) > 0:

                            encounter_type_code = coding_list[0].get("code", "")
                            encounter_type = coding_list[0].get("display", "")

                    # ---------- PROVIDER ----------
                    provider_name = ""

                    participants = resource.get("participant", [])

                    if len(participants) > 0:

                        individual = participants[0].get("individual", {})

                        provider_name = individual.get("display", "")

                    # ---------- REASON ----------
                    reason_code = ""
                    reason_description = ""

                    reason_list = resource.get("reasonCode", [])

                    if len(reason_list) > 0:

                        coding_list = reason_list[0].get("coding", [])

                        if len(coding_list) > 0:

                            reason_code = coding_list[0].get("code", "")
                            reason_description = coding_list[0].get("display", "")

                    # ---------- ORGANIZATION ----------
                    organization_name = (
                        resource.get("serviceProvider", {})
                        .get("display", "")
                    )

                    # ---------- LOAD DATE ----------
                    load_date = datetime.now().strftime("%Y-%m-%d")

                    # ---------- APPEND ----------
                    encounters.append({
                        "encounter_id": encounter_id,
                        "status": status,
                        "patient_id": patient_id,
                        "patient_name": patient_name,
                        "start_time": start_time,
                        "end_time": end_time,
                        "encounter_class": encounter_class,
                        "encounter_type": encounter_type,
                        "encounter_type_code": encounter_type_code,
                        "provider_name": provider_name,
                        "organization_name": organization_name,
                        "reason_code": reason_code,
                        "reason_description": reason_description,
                        "load_date": load_date
                    })

        except Exception as e:
            print(f"Error processing {file_name}: {e}")

df = pd.DataFrame(encounters)

df["ingestion_time"] = pd.Timestamp.utcnow()
df["source"] = "api_fhir_encounters"

# -------- SAVE TEMP --------
os.makedirs("temp", exist_ok=True)

temp_file = "temp/encounters_api_ingested.csv"
df.to_csv(temp_file, index=False)

# -------- S3 UPLOAD --------
load_date = datetime.utcnow().strftime("%Y-%m-%d")
s3_key = f"bronze/api/encounters/load_date={load_date}/api_encounters.csv"

s3_client = boto3.client("s3")
s3_client.upload_file(temp_file, bucket_name, s3_key)

print(f"Uploaded to s3://{bucket_name}/{s3_key}")
