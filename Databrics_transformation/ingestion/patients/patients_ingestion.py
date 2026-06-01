
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================
# CONFIGURATION
# ==============================

datasets = [
    {
        "table_name": "raw_patients_api",
        "source_path": "s3://rohan-healthcare-project/bronze/api/patients/",
        "checkpoint_path": "/Volumes/healthcare/bronze/checkpoints/raw_patients_api/",
        "target_table": "healthcare.bronze.raw_patients_api"
    },
    {
        "table_name": "raw_patients_csv",
        "source_path": "s3://rohan-healthcare-project/bronze/csv/patients/",
        "checkpoint_path": "/Volumes/healthcare/bronze/checkpoints/raw_patients_csv/",
        "target_table": "healthcare.bronze.raw_patients_csv"
    }
]

# ==============================
# INGESTION FUNCTION
# ==============================

def ingest_bronze_table(config):

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", config["checkpoint_path"] + "_schema")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(config["source_path"])
    )

    bronze_df = (
        df.withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_load_date", current_date())
    )

    (
        bronze_df.writeStream
            .format("delta")
            .option("checkpointLocation", config["checkpoint_path"])
            .trigger(availableNow=True)
            .toTable(config["target_table"])
    )

# ==============================
# EXECUTE INGESTION
# ==============================

for dataset in datasets:
    ingest_bronze_table(dataset)

