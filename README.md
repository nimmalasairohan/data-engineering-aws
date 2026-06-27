# Healthcare Data Engineering Pipeline on AWS

This project is an end-to-end healthcare data engineering pipeline for Synthea-style patient and encounter data. It uses Airflow for orchestration, AWS S3 for raw landing storage, Databricks with Delta tables for bronze and silver processing, and dbt Cloud for gold dimensional models and analytics marts.

## Architecture

```text
Synthea CSV + FHIR JSON files
        |
        v
Airflow ingestion scripts
        |
        v
AWS S3 bronze landing paths
        |
        v
Databricks Auto Loader bronze Delta tables
        |
        v
Databricks silver transformations
        |
        v
dbt gold dimension, fact, and mart models
```

## Main Pipeline Flow

The main DAG is `airflow/dags/healthcare_pipeline.py`.

1. `patient_ingestion`
   - Runs the patient FHIR JSON ingestion script.
   - Runs the patient CSV ingestion script.
   - Uploads flattened patient files to S3.

2. `encounter_ingestion`
   - Runs the encounter FHIR JSON ingestion script.
   - Runs the encounter CSV ingestion script.
   - Uploads flattened encounter files to S3.

3. `bronze`
   - Placeholder Airflow task used before the Databricks bronze job.

4. `bronze_databricks`
   - Triggers the Databricks job with job id ``.
   - Loads S3 files into Delta bronze tables using Databricks Auto Loader.

5. `silver_databricks`
   - Triggers the Databricks job with job id ``.
   - Standardizes, joins, deduplicates, and quality-checks bronze tables into silver Delta tables.

6. `trigger_dbt_cloud`
   - Triggers a dbt Cloud job using the Airflow variable `DBT_SERVICE_TOKEN`.
   - Builds gold models for reporting.

## Data Sources

The ingestion scripts expect source files inside the Airflow container:

```text
/usr/local/airflow/include/data/synthea_sample_data_csv_latest/
/usr/local/airflow/include/data/synthea_sample_data_fhir_latest/
```

Expected source files include:

- `patients.csv`
- `encounters.csv`
- FHIR JSON bundle files containing `Patient` and `Encounter` resources

## S3 Landing Paths

The current bucket is:

```text
rohan-healthcare-project
```

The ingestion scripts write date-partitioned files to:

```text
s3://rohan-healthcare-project/bronze/api/patients/load_date=YYYY-MM-DD/api_patients.csv
s3://rohan-healthcare-project/bronze/csv/patients/load_date=YYYY-MM-DD/csv_patients.csv
s3://rohan-healthcare-project/bronze/api/encounters/load_date=YYYY-MM-DD/api_encounters.csv
s3://rohan-healthcare-project/bronze/csv/encounters/load_date=YYYY-MM-DD/csv_encounters.csv
```

## Databricks Tables

Bronze tables:

- `healthcare.bronze.raw_patients_api`
- `healthcare.bronze.raw_patients_csv`
- `healthcare.bronze.raw_encounters_api`
- `healthcare.bronze.raw_encounters_csv`

Silver tables:

- `healthcare.silver.silver_patients`
- `healthcare.silver.silver_encounters`

Gold dbt models:

- `dim_patient`
- `fact_encounter`
- `mart_patient_summary`

## Required Airflow Connections and Variables

Create these in Airflow before running the DAG:

- AWS connection: `healthcare_aws`
- Databricks connection: `healthcare_databricks`
- Airflow variable: `DBT_API_BASE_URL`
- Airflow variable: `DBT_ACCOUNT_ID`
- Airflow variable: `DBT_JOB_ID`
- Airflow variable: `DBT_SERVICE_TOKEN`

The AWS connection is used by the S3 test DAG and can also be reused by ingestion utilities. The Databricks connection is used by `DatabricksRunNowOperator`. The dbt token is used to call the dbt Cloud API.

## Running Locally

This Airflow project is based on Astronomer Runtime.

```bash
cd airflow
astro dev start
```

Then open the Airflow UI at:

```text
http://localhost:8080
```

In the Airflow UI:

1. Confirm the `healthcare_aws` connection works by running `test_aws_connection`.
2. Confirm `healthcare_databricks` is configured.
3. Add `DBT_API_BASE_URL`, `DBT_ACCOUNT_ID`, `DBT_JOB_ID`, and `DBT_SERVICE_TOKEN` as Airflow variables.
4. Trigger the `healthcare_pipeline` DAG manually.

## Repository Layout

```text
airflow/
  dags/
    healthcare_pipeline.py
    test_aws_connection.py
  include/
    Ingestion/
      patients/
      encounters/
    utils/

Databricks/
  bronze/
    patients/
    encounters/
  silver/
    patients/
    encounters/

Dbt/
  models/
    dimensions/
    facts/
    marts/

docs/
  detailed_pipeline_documentation.md
```

## Detailed Documentation

For a full step-by-step explanation of ingestion, Airflow orchestration, S3 storage, Databricks bronze/silver processing, dbt gold models, troubleshooting, and suggested upgrades, see:

[Detailed Pipeline Documentation](docs/detailed_pipeline_documentation.md)
