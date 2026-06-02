from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ==========================================
# READ INCREMENTAL BRONZE DATA
# ==========================================

df_csv = (
    spark.read.table("healthcare.bronze.raw_patients_csv")
    .filter(
        col("_ingestion_timestamp") >= date_sub(current_date(), 1)
    )
)

df_api = (
    spark.read.table("healthcare.bronze.raw_patients_api")
    .filter(
        col("_ingestion_timestamp") >= date_sub(current_date(), 1)
    )
)

# ==========================================
# STANDARDIZE CSV DATA
# ==========================================

df_csv_std = (
    df_csv
    .withColumnRenamed("id", "patient_id")
    .withColumnRenamed("first", "first_name")
    .withColumnRenamed("middle", "middle_name")
    .withColumnRenamed("last", "last_name")
    .withColumnRenamed("birthdate", "birth_date")
    .withColumnRenamed("drivers", "driver_license")
    .withColumnRenamed("zip", "postal_code")
    .withColumnRenamed("marital", "marital_status")

    .withColumn(
        "birth_city",
        trim(split(col("birthplace"), "  ").getItem(0))
    )
    .withColumn(
        "birth_state",
        trim(split(col("birthplace"), "  ").getItem(1))
    )
    .withColumn(
        "birth_country",
        trim(split(col("birthplace"), "  ").getItem(2))
    )
)

# ==========================================
# CREATE MATCH KEY
# ==========================================

df_csv_std = df_csv_std.withColumn(
    "person_key",
    sha2(
        concat_ws(
            "|",
            lower(col("first_name")),
            lower(col("last_name")),
            col("birth_date")
        ),
        256
    )
)

df_api = df_api.withColumn(
    "person_key",
    sha2(
        concat_ws(
            "|",
            lower(col("first_name")),
            lower(col("last_name")),
            col("birth_date")
        ),
        256
    )
)

# ==========================================
# FULL OUTER JOIN
# ==========================================

df_joined = (
    df_csv_std.alias("csv")
    .join(
        df_api.alias("api"),
        on="patient_id",
        how="full_outer"
    )
)

# ==========================================
# GOLDEN RECORD CREATION
# ==========================================

df_final = df_joined.select(

    coalesce(
        col("api.patient_id"),
        col("csv.patient_id")
    ).alias("patient_id"),

    coalesce(
        col("api.first_name"),
        col("csv.first_name")
    ).alias("first_name"),

    coalesce(
        col("api.middle_name"),
        col("csv.middle_name")
    ).alias("middle_name"),

    coalesce(
        col("api.last_name"),
        col("csv.last_name")
    ).alias("last_name"),

    coalesce(
        col("api.gender"),
        col("csv.gender")
    ).alias("gender"),

    coalesce(
        col("api.birth_date"),
        col("csv.birth_date")
    ).alias("birth_date"),

    coalesce(
        col("api.birth_city"),
        col("csv.birth_city")
    ).alias("birth_city"),

    coalesce(
        col("api.birth_state"),
        col("csv.birth_state")
    ).alias("birth_state"),

    coalesce(
        col("api.birth_country"),
        col("csv.birth_country")
    ).alias("birth_country"),

    coalesce(
        col("api.phone"),
        lit(None)
    ).alias("phone"),

    coalesce(
        col("api.ssn"),
        col("csv.ssn")
    ).alias("ssn"),

    coalesce(
        col("api.driver_license"),
        col("csv.driver_license")
    ).alias("driver_license"),

    coalesce(
        col("api.passport"),
        col("csv.passport")
    ).alias("passport"),

    coalesce(
        col("api.race"),
        col("csv.race")
    ).alias("race"),

    coalesce(
        col("api.ethnicity"),
        col("csv.ethnicity")
    ).alias("ethnicity"),

    coalesce(
        col("api.marital_status"),
        col("csv.marital_status")
    ).alias("marital_status"),

    coalesce(
        col("api.city"),
        col("csv.city")
    ).alias("city"),

    coalesce(
        col("api.state"),
        col("csv.state")
    ).alias("state"),

    coalesce(
        col("api.postal_code"),
        col("csv.postal_code")
    ).alias("postal_code"),

    col("csv.healthcare_expenses").alias("healthcare_expenses"),

    col("csv.healthcare_coverage").alias("healthcare_coverage"),

    col("csv.income").alias("income"),

    col("api.daly").alias("daly"),

    col("api.qaly").alias("qaly"),

    coalesce(
        col("api._ingestion_timestamp"),
        col("csv._ingestion_timestamp")
    ).alias("ingestion_time"),

    current_timestamp().alias("silver_load_time"),

    lit("golden_record").alias("record_type")
)

# ==========================================
# DEDUPLICATE INSIDE CURRENT BATCH
# ==========================================

window_spec = (
    Window
    .partitionBy("patient_id")
    .orderBy(col("ingestion_time").desc())
)

df_patients_final = (
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
    df_patients_final.write
        .format("delta")
        .mode("append")
        .saveAsTable("healthcare.silver.silver_patients")
)