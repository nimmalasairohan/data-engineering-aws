{{ config(
    materialized='table'
) }}

SELECT

    dp.patient_id,

    dp.full_name,

    dp.gender,

    dp.city,

    dp.state,

    COUNT(fe.encounter_id) AS total_encounters,

    SUM(fe.total_claim_cost) AS total_claim_cost,

    SUM(fe.payer_coverage) AS total_payer_coverage,

    AVG(fe.total_claim_cost) AS avg_claim_cost,

    AVG(fe.encounter_duration_minutes) AS avg_encounter_duration

FROM {{ ref('dim_patient') }} dp

LEFT JOIN {{ ref('fact_encounter') }} fe
    ON dp.patient_id = fe.patient_id

GROUP BY

    dp.patient_id,
    dp.full_name,
    dp.gender,
    dp.city,
    dp.state