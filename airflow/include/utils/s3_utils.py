from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_to_s3(
    local_file,
    bucket_name,
    s3_key,
    aws_conn_id="healthcare_aws"
):
    hook = S3Hook(aws_conn_id=aws_conn_id)

    hook.load_file(
        filename=local_file,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )