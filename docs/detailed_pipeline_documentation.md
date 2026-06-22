# Detailed Healthcare Pipeline Documentation

## 1. Project Purpose

This project builds a healthcare analytics pipeline using patient and encounter data. It starts with raw Synthea-style CSV and FHIR JSON files, lands normalized files in AWS S3, loads them into Databricks Delta bronze tables, transforms them into silver business-ready tables, and finally creates dbt gold models for analytics.

The main goal is to create a clear medallion architecture:

```text
Raw files -> S3 bronze landing -> Databricks bronze -> Databricks silver -> dbt gold
```

## 2. Technology Stack

| Layer | Tool | Purpose |
| --- | --- | --- |
| Orchestration | Apache Airflow / Astronomer | Runs ingestion scripts, triggers Databricks jobs, and triggers dbt Cloud |
| Storage | AWS S3 | Stores ingested patient and encounter files |
| Processing | Databricks / PySpark | Loads bronze Delta tables and creates silver transformed tables |
| Lakehouse format | Delta Lake | Stores bronze and silver managed tables |
| Modeling | dbt Cloud | Builds gold dimensional and mart models |
| Language | Python, PySpark, SQL | Ingestion, transformation, and modeling |

## 3. High-Level Architecture

```text
Local source data in Airflow container
        |
        | Python ingestion scripts
        v
AWS S3
        |
        | Databricks Auto Loader
        v
Bronze Delta tables
        |
        | PySpark transformations
        v
Silver Delta tables
        |
        | dbt Cloud job
        v
Gold reporting models
```

## 4. Main Airflow DAG

The main pipeline DAG is:

```text
airflow/dags/healthcare_pipeline.py
```

The DAG is named:

```text
healthcare_pipeline
```

Current DAG configuration:

| Setting | Value |
| --- | --- |
| Start date | `2025-01-01` |
| Schedule | `None` |
| Catchup | `False` |
| Tags | `healthcare` |

Because `schedule=None`, the DAG is manually triggered unless a schedule is added later.

## 5. Airflow Task Order

The DAG dependency chain is:

```text
[patient_ingestion, encounter_ingestion]
        >> bronze
        >> bronze_databrics
        >> silver_databrics
        >> trigger_dbt_cloud
```

This means patient and encounter ingestion run in parallel. After both finish successfully, the pipeline continues into Databricks bronze processing, then Databricks silver processing, and finally dbt Cloud.

## 6. Ingestion Layer

The ingestion layer reads raw files from inside the Airflow container and uploads processed CSV outputs to S3.

Expected source folders:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/
```

Current S3 bucket:

```text
rohan-healthcare-project
```

### 6.1 Patient JSON API/FHIR Ingestion

Script:

```text
airflow/include/Ingestion/patients/json_api_ingestion_paients.py
```

Input:

```text
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/*.json
```

Logic:

1. Loops through all `.json` files in the FHIR source folder.
2. Reads each file as JSON.
3. Looks inside the `entry` array.
4. Selects resources where `resourceType` is `Patient`.
5. Extracts fields such as:
   - `patient_id`
   - `first_name`
   - `middle_name`
   - `last_name`
   - `gender`
   - `birth_date`
   - `phone`
   - `mrn`
   - `ssn`
   - `driver_license`
   - `passport`
   - address fields
   - race and ethnicity
   - birth place fields
   - marital status
   - DALY and QALY values
6. Adds metadata:
   - `ingestion_time`
   - `source = api_fhir_patients`
   - `source_file`
7. Writes a temporary CSV file:

```text
temp_patients/patients_api_ingested.csv
```

8. Uploads it to S3:

```text
s3://rohan-healthcare-project/bronze/api/patients/load_date=YYYY-MM-DD/api_patients.csv
```

### 6.2 Patient CSV Ingestion

Script:

```text
airflow/include/Ingestion/patients/csv_ingesion_patients.py
```

Input:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/patients.csv
```

Logic:

1. Reads `patients.csv` using pandas.
2. Converts all column names to lowercase.
3. Checks that the file is not empty.
4. Validates required columns:
   - `id`
   - `first`
5. Adds metadata:
   - `ingestion_time`
   - `source = csv_patients`
6. Writes a temporary CSV file:

```text
temp_patients/patients_csv_ingested.csv
```

7. Uploads it to S3:

```text
s3://rohan-healthcare-project/bronze/csv/patients/load_date=YYYY-MM-DD/csv_patients.csv
```

### 6.3 Encounter JSON API/FHIR Ingestion

Script:

```text
airflow/include/Ingestion/encounters/json_ingesion_encounters.py
```

Input:

```text
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/*.json
```

Logic:

1. Loops through all `.json` files in the FHIR source folder.
2. Reads each file as JSON.
3. Looks inside the `entry` array.
4. Selects resources where `resourceType` is `Encounter`.
5. Extracts fields such as:
   - `encounter_id`
   - `status`
   - `patient_id`
   - `patient_name`
   - `start_time`
   - `end_time`
   - `encounter_class`
   - `encounter_type`
   - `encounter_type_code`
   - `provider_name`
   - `organization_name`
   - `reason_code`
   - `reason_description`
   - `load_date`
6. Adds metadata:
   - `ingestion_time`
   - `source = api_fhir_encounters`
7. Writes a temporary CSV file:

```text
temp/encounters_api_ingested.csv
```

8. Uploads it to S3:

```text
s3://rohan-healthcare-project/bronze/api/encounters/load_date=YYYY-MM-DD/api_encounters.csv
```

### 6.4 Encounter CSV Ingestion

Script:

```text
airflow/include/Ingestion/encounters/csv_ingesion_encounters.py
```

Input:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/encounters.csv
```

Logic:

1. Reads `encounters.csv` using pandas.
2. Converts all column names to lowercase.
3. Checks that the file is not empty.
4. Validates required columns:
   - `id`
   - `patient`
5. Adds metadata:
   - `ingestion_time`
   - `source = csv_encounters`
6. Writes a temporary CSV file:

```text
temp_encounters/csv_encounters.csv
```

7. Uploads it to S3:

```text
s3://rohan-healthcare-project/bronze/csv/encounters/load_date=YYYY-MM-DD/csv_encounters.csv
```

## 7. S3 Bronze Landing Structure

The ingestion scripts write data into four S3 source zones:

| Data | Source type | S3 prefix |
| --- | --- | --- |
| Patients | FHIR JSON/API flattened to CSV | `bronze/api/patients/` |
| Patients | CSV | `bronze/csv/patients/` |
| Encounters | FHIR JSON/API flattened to CSV | `bronze/api/encounters/` |
| Encounters | CSV | `bronze/csv/encounters/` |

Each upload uses a `load_date=YYYY-MM-DD` folder. This gives a simple partition-like layout for daily loads.

## 8. Databricks Bronze Layer

The bronze layer reads files from S3 and stores them as Delta tables.

### 8.1 Patient Bronze Ingestion

Script:

```text
Databrics/bronze/patients/patients_ingestion.py
```

Configured datasets:

| Source path | Target table |
| --- | --- |
| `s3://rohan-healthcare-project/bronze/api/patients/` | `healthcare.bronze.raw_patients_api` |
| `s3://rohan-healthcare-project/bronze/csv/patients/` | `healthcare.bronze.raw_patients_csv` |

Processing details:

1. Uses Databricks Auto Loader with `cloudFiles`.
2. Reads CSV files with headers.
3. Stores schema information under the configured checkpoint schema path.
4. Allows schema evolution with `cloudFiles.schemaEvolutionMode = addNewColumns`.
5. Adds metadata columns:
   - `_ingestion_timestamp`
   - `_source_file`
   - `_load_date`
6. Writes to Delta tables using `trigger(availableNow=True)`.

### 8.2 Encounter Bronze Ingestion

Script:

```text
Databrics/bronze/encounters/encounters_ingestion.py
```

Configured datasets:

| Source path | Target table |
| --- | --- |
| `s3://rohan-healthcare-project/bronze/api/encounters/` | `healthcare.bronze.raw_encounters_api` |
| `s3://rohan-healthcare-project/bronze/csv/encounters/` | `healthcare.bronze.raw_encounters_csv` |

The processing pattern is the same as patient bronze ingestion: Auto Loader reads from S3, adds metadata columns, and writes Delta tables.

## 9. Databricks Silver Layer

The silver layer creates cleaned, standardized, and analytics-ready records.

### 9.1 Silver Patients

Script:

```text
Databrics/silver/patients/patients_transformation.py
```

Input tables:

```text
healthcare.bronze.raw_patients_csv
healthcare.bronze.raw_patients_api
```

The script filters bronze data to recent records:

```text
_ingestion_timestamp >= date_sub(current_date(), 1)
```

Main transformations:

1. Standardizes CSV column names:
   - `id` -> `patient_id`
   - `first` -> `first_name`
   - `middle` -> `middle_name`
   - `last` -> `last_name`
   - `birthdate` -> `birth_date`
   - `drivers` -> `driver_license`
   - `zip` -> `postal_code`
   - `marital` -> `marital_status`
2. Splits `birthplace` into:
   - `birth_city`
   - `birth_state`
   - `birth_country`
3. Creates a `person_key` using SHA-256 from first name, last name, and birth date.
4. Full outer joins CSV and API patient data on `patient_id`.
5. Builds a golden patient record using `coalesce`, generally preferring API values where available.
6. Keeps financial fields from CSV:
   - `healthcare_expenses`
   - `healthcare_coverage`
   - `income`
7. Keeps health-adjusted metrics from API:
   - `daly`
   - `qaly`
8. Adds:
   - `silver_load_time`
   - `record_type = golden_record`
9. Deduplicates by `patient_id`, keeping the latest `ingestion_time`.
10. Appends the result to:

```text
healthcare.silver.silver_patients
```

### 9.2 Silver Encounters

Script:

```text
Databrics/silver/encounters/encounters_transformation.py
```

Input tables:

```text
healthcare.bronze.raw_encounters_api
healthcare.bronze.raw_encounters_csv
```

The script filters bronze data to recent records:

```text
_ingestion_timestamp >= date_sub(current_date(), 1)
```

Main transformations:

1. Joins API encounters to CSV encounters:

```text
api.encounter_id = csv.id
```

2. Standardizes timestamp fields:
   - `start_time`
   - `end_time`
3. Standardizes selected codes using `upper` and `trim`.
4. Adds cost and coverage fields from CSV:
   - `base_encounter_cost`
   - `total_claim_cost`
   - `payer_coverage`
5. Fills missing values:
   - `reason_description = UNKNOWN`
   - cost fields = `0`
6. Calculates:

```text
encounter_duration_minutes = (end_time - start_time) / 60
```

7. Applies data quality filters:
   - `encounter_id` is not null
   - `encounter_duration_minutes >= 0`
   - `total_claim_cost >= 0`
   - `payer_coverage <= total_claim_cost`
8. Deduplicates by `encounter_id`, keeping the latest `ingestion_time`.
9. Appends the result to:

```text
healthcare.silver.silver_encounters
```

## 10. dbt Gold Layer

The dbt project is in:

```text
Dbt/
```

Project file:

```text
Dbt/dbt_project.yml
```

Current project name:

```text
my_new_project
```

Configured model behavior:

| Folder | Materialization | Schema |
| --- | --- | --- |
| `models/dimensions` | incremental | gold |
| `models/facts` | table | gold |
| `models/marts` | table | gold |

### 10.1 Patient Dimension

Model:

```text
Dbt/models/dimensions/dim_patient.sql
```

Input:

```text
silver.silver_patients
```

Purpose:

Creates the latest patient-level dimension record by ranking patients by `ingestion_time` and keeping `rn = 1`.

Important fields:

- `patient_id`
- `first_name`
- `middle_name`
- `last_name`
- `full_name`
- `gender`
- `birth_date`
- `race`
- `ethnicity`
- `marital_status`
- location fields
- birth place fields
- `ingestion_time`

Materialization:

```text
incremental
```

Unique key:

```text
patient_id
```

### 10.2 Encounter Fact

Model:

```text
Dbt/models/facts/fact_encounter.sql
```

Input:

```text
silver.silver_encounters
```

Purpose:

Creates an encounter-level fact table with encounter identifiers, patient relationship, timing, reason, provider, payer, and cost fields.

Important metrics:

- `encounter_duration_minutes`
- `base_encounter_cost`
- `total_claim_cost`
- `payer_coverage`

Materialization:

```text
table
```

### 10.3 Patient Summary Mart

Model:

```text
Dbt/models/marts/mart_patient_summary.sql
```

Inputs:

```text
dim_patient
fact_encounter
```

Purpose:

Creates one row per patient with aggregated encounter and cost metrics.

Metrics:

- `total_encounters`
- `total_claim_cost`
- `total_payer_coverage`
- `avg_claim_cost`
- `avg_encounter_duration`

Join logic:

```text
dim_patient.patient_id = fact_encounter.patient_id
```

## 11. Required Airflow Configuration

### 11.1 AWS Connection

Connection id:

```text
healthcare_aws
```

Used by:

```text
airflow/dags/test_aws_connection.py
airflow/include/utils/s3_utils.py
```

The ingestion scripts currently use `boto3.client("s3")` directly, so the container or runtime environment must have AWS credentials available.

### 11.2 Databricks Connection

Connection id:

```text
healthcare_databricks
```

Used by:

```text
DatabricksRunNowOperator
```

The DAG expects this connection to have permission to trigger Databricks jobs.

### 11.3 dbt Cloud Token

Airflow variable:

```text
DBT_SERVICE_TOKEN
```

Used by:

```text
trigger_dbt_cloud
```

Current dbt Cloud settings in the DAG:

| Setting | Value |
| --- | --- |
| Account id | `70506183137352` |
| Job id | `70506183132521` |
| API base | `https://kw833.us1.dbt.com/api/v2/` |

## 12. How to Run the Pipeline

### Step 1: Start Airflow Locally

From the `airflow` folder:

```bash
astro dev start
```

Airflow should be available at:

```text
http://localhost:8080
```

### Step 2: Add Source Data

Make sure the Airflow container has these folders and files:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/patients.csv
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/encounters.csv
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/*.json
```

### Step 3: Configure Airflow

Create or verify:

- `healthcare_aws`
- `healthcare_databricks`
- `DBT_SERVICE_TOKEN`

### Step 4: Test AWS

Run the DAG:

```text
test_aws_connection
```

Expected result:

The DAG lists available S3 buckets using the `healthcare_aws` connection.

### Step 5: Trigger Main Pipeline

Run:

```text
healthcare_pipeline
```

Expected result:

1. Patient files are processed and uploaded to S3.
2. Encounter files are processed and uploaded to S3.
3. Databricks bronze job loads S3 files into bronze Delta tables.
4. Databricks silver job creates silver patient and encounter tables.
5. dbt Cloud builds gold models.

## 13. Data Lineage Summary

| Stage | Patients | Encounters |
| --- | --- | --- |
| Source CSV | `patients.csv` | `encounters.csv` |
| Source FHIR JSON | `Patient` resources | `Encounter` resources |
| S3 API path | `bronze/api/patients/` | `bronze/api/encounters/` |
| S3 CSV path | `bronze/csv/patients/` | `bronze/csv/encounters/` |
| Bronze API table | `raw_patients_api` | `raw_encounters_api` |
| Bronze CSV table | `raw_patients_csv` | `raw_encounters_csv` |
| Silver table | `silver_patients` | `silver_encounters` |
| Gold model | `dim_patient` | `fact_encounter` |
| Mart | `mart_patient_summary` | `mart_patient_summary` |

## 14. Common Troubleshooting

### Airflow ingestion task fails with file not found

Check that the source files exist inside the Airflow container, not only on your local machine.

Expected paths:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/
```

### S3 upload fails

Check:

- AWS credentials are available inside the Airflow runtime.
- The bucket `rohan-healthcare-project` exists.
- The runtime has `s3:PutObject` permission.
- The AWS region is configured correctly.

### Databricks task fails

Check:

- Airflow connection `healthcare_databricks`.
- Databricks job ids in the DAG.
- Databricks cluster has access to the S3 bucket.
- Unity Catalog objects exist:
  - catalog/database `healthcare`
  - schemas `bronze` and `silver`
- Checkpoint volume paths exist and are writable:

```text
/Volumes/healthcare/bronze/checkpoints/
```

### dbt Cloud trigger fails

Check:

- Airflow variable `DBT_SERVICE_TOKEN`.
- dbt Cloud account id and job id.
- Token permissions.
- Network access from Airflow to dbt Cloud.

### Silver tables have duplicate historical rows

The silver scripts deduplicate only inside the current processed batch and then append to the target table. If the same entity appears across multiple runs, the table can still contain older records. The dbt `dim_patient` model handles this for patients by keeping the latest record, but you may want to upgrade the silver layer to use Delta `MERGE`.

## 15. Recommended Improvements

These upgrades would make the project easier to maintain and more production-ready.

### 15.1 Move Hardcoded Values to Airflow Variables

Currently, values like S3 bucket name, dbt account id, dbt job id, and Databricks job ids are hardcoded. Move them to Airflow Variables or environment variables.

Good candidates:

- `S3_BUCKET_NAME`
- `DBT_ACCOUNT_ID`
- `DBT_JOB_ID`
- `DATABRICKS_BRONZE_JOB_ID`
- `DATABRICKS_SILVER_JOB_ID`

### 15.2 Use Airflow S3Hook Consistently

The repo has:

```text
airflow/include/utils/s3_utils.py
```

This utility uses Airflow's `S3Hook`, but the ingestion scripts use raw `boto3`. Using `S3Hook` everywhere would make credentials easier to manage through Airflow connections.

### 15.3 Replace Placeholder Airflow Tasks

The DAG currently has a `bronze` placeholder task that only prints a message. It can be renamed to something like `ready_for_databricks` or removed if it is not needed.

### 15.4 Use Delta MERGE in Silver

The silver scripts currently append records after deduplicating the current batch. For stronger incremental behavior, use Delta `MERGE INTO` by:

- `patient_id` for `silver_patients`
- `encounter_id` for `silver_encounters`

### 15.5 Add dbt Tests

Recommended dbt tests:

- `not_null` on `dim_patient.patient_id`
- `unique` on `dim_patient.patient_id`
- `not_null` on `fact_encounter.encounter_id`
- `not_null` on `fact_encounter.patient_id`
- accepted values or range tests for cost fields

### 15.6 Add Source Freshness and Documentation in dbt

Add dbt `sources.yml` and model descriptions so dbt Cloud can show lineage, documentation, and freshness checks.

### 15.7 Improve Folder and File Names

Some current names contain spelling mistakes, such as:

- `Databrics`
- `json_api_ingestion_paients.py`
- `csv_ingesion_patients.py`
- `json_ingesion_encounters.py`
- `csv_ingesion_encounters.py`

Renaming them would improve readability, but update DAG paths at the same time.

## 16. Mental Model for Re-Understanding the Project

When reviewing the project, think about it in five layers:

1. Ingestion
   - Python reads local CSV and FHIR JSON files.
   - Python uploads normalized CSV outputs to S3.

2. S3 landing
   - S3 stores raw ingested files by source type, entity, and load date.

3. Bronze
   - Databricks Auto Loader reads new S3 files.
   - Delta bronze tables preserve raw-ish data plus ingestion metadata.

4. Silver
   - Databricks standardizes columns, joins CSV and API sources, handles nulls, calculates metrics, and deduplicates records.

5. Gold
   - dbt creates dimensional models and reporting marts for analytics.

If you want to debug the pipeline, follow the same order: Airflow logs, S3 files, bronze tables, silver tables, dbt models.
