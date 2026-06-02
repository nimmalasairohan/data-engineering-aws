{{ config(
    materialized='table'
) }}

SELECT

    encounter_id,

    patient_id,

    start_time,

    end_time,

    encounter_duration_minutes,

    encounter_class_code,

    encounter_type_code,

    encounter_type,

    reason_code,

    reason_description,

    provider_name,

    organization_name,

    payer,

    provider,

    encounter_class,

    base_encounter_cost,

    total_claim_cost,

    payer_coverage,

    ingestion_time

FROM silver.silver_encounters