{{ config(
    materialized='incremental',
    unique_key='patient_id'
) }}

WITH ranked_patients AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM silver.silver_patients

)

SELECT

    patient_id,
    first_name,
    middle_name,
    last_name,

    CONCAT(
        first_name,
        ' ',
        last_name
    ) AS full_name,

    gender,
    birth_date,
    race,
    ethnicity,
    marital_status,

    city,
    state,
    postal_code,

    birth_city,
    birth_state,
    birth_country,

    ingestion_time

FROM ranked_patients

WHERE rn = 1