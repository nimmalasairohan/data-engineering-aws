from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["healthcare_aws"],
)
def test_aws_connection():

    @task
    def test_aws_connection_1():

        hook = S3Hook(aws_conn_id="healthcare_aws")

        buckets = hook.get_conn().list_buckets()

        print(buckets)

    test_aws_connection_1()


test_aws_connection()