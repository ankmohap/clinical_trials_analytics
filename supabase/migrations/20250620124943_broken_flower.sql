{{ config(
    materialized='table',
    tags=['marts', 'dimension']
) }}

select
    trial_key,
    nct_id,
    brief_title,
    official_title,
    overall_status,
    study_type,
    phase,
    condition_clean,
    sponsor_name_clean,
    location_country_clean,
    location_state,
    location_city,
    intervention_name,
    primary_outcome_measure,
    study_start_date,
    completion_date,
    is_active,
    is_completed,
    is_international,
    extraction_date,
    data_source,
    batch_id,
    created_at,
    updated_at
from {{ ref('stg_clinical_trials') }}