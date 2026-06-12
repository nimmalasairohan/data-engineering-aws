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
                "/usr/local/airflow/include/Ingestion/patients/json_api_ingestion_paients.py"
            ],
            check=True
            )
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/patients/csv_ingesion_patients.py"
            ],
            check=True
            )

    @task
    def encounter_ingestion():
            print("Running encounter ingestion")
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/encounters/json_ingesion_encounters.py"
            ],
            check=True
            )
            subprocess.run(
            [
                "python",
                "/usr/local/airflow/include/Ingestion/encounters/csv_ingesion_encounters.py"
            ],
            check=True
            )
        
    @task
    def bronze():
        print("going to broze")
        
        
    @task
    def bronze_databrics():
        print("broze databrics ingestion")
        
    @task
    def silver_databrics():
        print("silver databrics transformation")
        

        
        
    @task
    def trigger_dbt_cloud():

        import requests
    
        ACCOUNT_ID = "70506183137352"
        JOB_ID = "70506183132521"
        DBT_SERVICE_TOKEN = Variable.get("DBT_SERVICE_TOKEN")
    
        url = (
            f"https://kw833.us1.dbt.com/api/v2/"
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
    task_id="bronze_databrics",
    databricks_conn_id="healthcare_databricks",
    job_id=1069224034326071
        )

    silver = DatabricksRunNowOperator(
    task_id="silver_databrics",
    databricks_conn_id="healthcare_databricks",
    job_id=1061440484747266
        )
    trigger_dbt_cloud_task=trigger_dbt_cloud()

    
    
    [patient, encounter] >> bronze_task>>bronze>>silver>>trigger_dbt_cloud_task
healthcare_pipeline()