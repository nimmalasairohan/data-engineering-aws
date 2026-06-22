from airflow.sdk import dag, task
from pendulum import datetime
import subprocess
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["healthcare"],
)
def healthcare_pipeline():
    
    

    @task
    def patient_ingestion():
            print("Running patient ingestion")
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/patients/json_api_ingestion_patients.py"
            ],
            check=True
            )
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/patients/csv_ingestion_patients.py"
            ],
            check=True
            )

    @task
    def encounter_ingestion():
            print("Running encounter ingestion")
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/encounters/json_ingestion_encounters.py"
            ],
            check=True
            )
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/encounters/csv_ingestion_encounters.py"
            ],
            check=True
            )
        
    @task
    def bronze():
        print("going to bronze")
        
        
    @task
    def bronze_databricks():
        print("bronze databricks ingestion")
        
    @task
    def silver_databricks():
        print("silver databricks transformation")
        

        
        
    @task
    def trigger_dbt_cloud():

        import requests
    
        DBT_API_BASE_URL = Variable.get("DBT_API_BASE_URL")
        ACCOUNT_ID = Variable.get("DBT_ACCOUNT_ID")
        JOB_ID = Variable.get("DBT_JOB_ID")
        DBT_SERVICE_TOKEN = Variable.get("DBT_SERVICE_TOKEN")
    
        url = (
            f"{DBT_API_BASE_URL.rstrip('/')}/"
            f"accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/"
        )
    
        headers = {
            "Authorization": f"Token {DBT_SERVICE_TOKEN}",
            "Content-Type": "application/json"
        }
    
        payload = {
            "cause": "Triggered from Airflow"
        }
    
        response = requests.post(
            url,
            headers=headers,
            json=payload
        )
    
        response.raise_for_status()
    
        print(response.json())
        
        
            
    patient = patient_ingestion()
    encounter = encounter_ingestion()
    bronze_task = bronze()
    bronze = DatabricksRunNowOperator(
    task_id="bronze_databricks",
    databricks_conn_id="healthcare_databricks",
    job_id=1069224034326071
        )

    silver = DatabricksRunNowOperator(
    task_id="silver_databricks",
    databricks_conn_id="healthcare_databricks",
    job_id=1061440484747266
        )
    trigger_dbt_cloud_task=trigger_dbt_cloud()

    
    
    [patient, encounter] >> bronze_task>>bronze>>silver>>trigger_dbt_cloud_task
healthcare_pipeline()
