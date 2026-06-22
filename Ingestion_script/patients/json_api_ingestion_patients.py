import json
import boto3
import pandas as pd
import os
from datetime import datetime

folder_path = "D:\\rohan_heath_care_project\\data-engineering-aws\\data\\synthea_sample_data_fhir_latest"
bucket_name = "rohan-healthcare-project"

patients = []

for file_name in os.listdir(folder_path):

    if file_name.endswith(".json"):
        file_path = os.path.join(folder_path, file_name)

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

            entries = data.get("entry", [])

            for entry in entries:
                resource = entry.get("resource", {})

                if resource.get("resourceType") == "Patient":

                    # -------- BASIC --------
                    patient_id = resource.get("id", "")
                    gender = resource.get("gender", "")
                    birth_date = resource.get("birthDate", "")

                    # -------- NAME --------
                    first_name = ""
                    middle_name = ""
                    last_name = ""

                    names = resource.get("name", [])
                    if names:
                        first_name = names[0].get("given", [""])[0]

                        if len(names[0].get("given", [])) > 1:
                            middle_name = names[0].get("given")[1]

                        last_name = names[0].get("family", "")

                    # -------- CONTACT --------
                    phone = ""
                    telecom = resource.get("telecom", [])
                    if telecom:
                        phone = telecom[0].get("value", "")

                    # -------- IDENTIFIERS --------
                    mrn = ""
                    ssn = ""
                    driver_license = ""
                    passport = ""

                    identifiers = resource.get("identifier", [])

                    for ident in identifiers:
                        id_type = ident.get("type", {}).get("coding", [{}])[0].get("code", "")

                        if id_type == "MR":
                            mrn = ident.get("value", "")
                        elif id_type == "SS":
                            ssn = ident.get("value", "")
                        elif id_type == "DL":
                            driver_license = ident.get("value", "")
                        elif id_type == "PPN":
                            passport = ident.get("value", "")

                    # -------- ADDRESS (TEMP: first address only) --------
                    city = ""
                    state = ""
                    postal_code = ""
                    country = ""

                    addresses = resource.get("address", [])
                    if addresses:
                        addr = addresses[0]   # ⚠️ TEMP assumption
                        city = addr.get("city", "")
                        state = addr.get("state", "")
                        postal_code = addr.get("postalCode", "")
                        country = addr.get("country", "")

                    # -------- EXTENSIONS --------
                    race = ""
                    ethnicity = ""
                    birth_city = ""
                    birth_state = ""
                    birth_country = ""
                    mother_maiden_name = ""
                    birth_sex = ""
                    daly = None
                    qaly = None

                    extensions = resource.get("extension", [])

                    for ext in extensions:
                        url = ext.get("url", "")

                        if "race" in url:
                            race = ext.get("extension", [{}])[1].get("valueString", "")

                        elif "ethnicity" in url:
                            ethnicity = ext.get("extension", [{}])[1].get("valueString", "")

                        elif "mothersMaidenName" in url:
                            mother_maiden_name = ext.get("valueString", "")

                        elif "birthsex" in url:
                            birth_sex = ext.get("valueCode", "")

                        elif "birthPlace" in url:
                            birth_place = ext.get("valueAddress", {})
                            birth_city = birth_place.get("city", "")
                            birth_state = birth_place.get("state", "")
                            birth_country = birth_place.get("country", "")

                        elif "disability-adjusted-life-years" in url:
                            daly = ext.get("valueDecimal", None)

                        elif "quality-adjusted-life-years" in url:
                            qaly = ext.get("valueDecimal", None)

                    # -------- MARITAL STATUS --------
                    marital_status = resource.get("maritalStatus", {}).get("text", "")

                    # -------- APPEND --------
                    patients.append({
                        "patient_id": patient_id,
                        "first_name": first_name,
                        "middle_name": middle_name,
                        "last_name": last_name,
                        "gender": gender,
                        "birth_date": birth_date,
                        "phone": phone,

                        "mrn": mrn,
                        "ssn": ssn,
                        "driver_license": driver_license,
                        "passport": passport,

                        "city": city,
                        "state": state,
                        "postal_code": postal_code,
                        "country": country,

                        "race": race,
                        "ethnicity": ethnicity,
                        "birth_city": birth_city,
                        "birth_state": birth_state,
                        "birth_country": birth_country,

                        "birth_sex": birth_sex,
                        "mother_maiden_name": mother_maiden_name,
                        "marital_status": marital_status,

                        "daly": daly,
                        "qaly": qaly,

                        "source_file": file_name
                    })

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

# -------- DATAFRAME --------
df = pd.DataFrame(patients)

df["ingestion_time"] = pd.Timestamp.utcnow()
df["source"] = "api_fhir_patients"

# -------- SAVE TEMP --------
os.makedirs("temp_patients", exist_ok=True)

temp_file = "temp_patients/patients_api_ingested.csv"
df.to_csv(temp_file, index=False)

# -------- S3 UPLOAD --------
load_date = datetime.utcnow().strftime("%Y-%m-%d")
s3_key = f"bronze/api/patients/load_date={load_date}/api_patients.csv"

s3_client = boto3.client("s3")
s3_client.upload_file(temp_file, bucket_name, s3_key)

print(f"Uploaded to s3://{bucket_name}/{s3_key}")