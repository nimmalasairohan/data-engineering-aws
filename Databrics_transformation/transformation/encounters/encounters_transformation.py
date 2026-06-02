from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ==========================================
# READ INCREMENTAL BRONZE DATA
# ==========================================

df_api = (
    spark.read.table("healthcare.bronze.raw_encounters_api")
    .filter(
        col("_ingestion_timestamp") >= date_sub(current_date(), 1)
    )
)

df_csv = (
    spark.read.table("healthcare.bronze.raw_encounters_csv")
    .filter(
        col("_ingestion_timestamp") >= date_sub(current_date(), 1)
    )
)

# ==========================================
# ALIAS
# ==========================================

api = df_api.alias("api")
csv = df_csv.alias("csv")

# ==========================================
# JOIN
# ==========================================

df_joined = (
    api.join(
        csv,
        api.encounter_id == csv.id,
        "left"
    )
)

# ==========================================
# SELECT & STANDARDIZE
# ==========================================

df_final = df_joined.select(

    col("api.encounter_id"),

    col("api.status"),

    col("api.patient_id"),

    col("api.patient_name"),

    to_timestamp(
        col("api.start_time")
    ).alias("start_time"),

    to_timestamp(
        col("api.end_time")
    ).alias("end_time"),

    upper(
        trim(col("api.encounter_class"))
    ).alias("encounter_class_code"),

    col("api.encounter_type_code"),

    col("api.encounter_type"),

    col("api.provider_name"),

    col("api.organization_name"),

    upper(
        trim(col("api.reason_code"))
    ).alias("reason_code"),

    col("api.reason_description"),

    col("csv.PAYER").alias("payer"),

    col("csv.PROVIDER").alias("provider"),

    col("csv.ENCOUNTERCLASS").alias("encounter_class"),

    col("csv.BASE_ENCOUNTER_COST")
        .cast("double")
        .alias("base_encounter_cost"),

    col("csv.TOTAL_CLAIM_COST")
        .cast("double")
        .alias("total_claim_cost"),

    col("csv.PAYER_COVERAGE")
        .cast("double")
        .alias("payer_coverage"),

    coalesce(
        col("api._ingestion_timestamp"),
        col("csv._ingestion_timestamp")
    ).alias("ingestion_time"),

    current_timestamp().alias("silver_load_time")
)

# ==========================================
# NULL HANDLING
# ==========================================

df_final = (
    df_final.fillna({
        "reason_description": "UNKNOWN",
        "base_encounter_cost": 0,
        "total_claim_cost": 0,
        "payer_coverage": 0
    })
)

# ==========================================
# ENCOUNTER DURATION
# ==========================================

df_final = (
    df_final.withColumn(
        "encounter_duration_minutes",
        (
            unix_timestamp(col("end_time"))
            - unix_timestamp(col("start_time"))
        ) / 60
    )
)

# ==========================================
# DATA QUALITY FILTERS
# ==========================================

df_final = (
    df_final.filter(
        col("encounter_id").isNotNull()
    )
)

df_final = (
    df_final.filter(
        col("encounter_duration_minutes") >= 0
    )
)

df_final = (
    df_final.filter(
        col("total_claim_cost") >= 0
    )
)

df_final = (
    df_final.filter(
        col("payer_coverage")
        <= col("total_claim_cost")
    )
)

# ==========================================
# DEDUP CURRENT BATCH
# ==========================================

window_spec = (
    Window.partitionBy("encounter_id")
    .orderBy(
        col("ingestion_time").desc()
    )
)

df_final = (
    df_final
    .withColumn(
        "rn",
        row_number().over(window_spec)
    )
    .filter(col("rn") == 1)
    .drop("rn")
)

# ==========================================
# APPEND TO SILVER
# ==========================================

(
    df_final.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "healthcare.silver.silver_encounters"
        )
)