{{ config(
    materialized='table',
    tags=['marts', 'fact']
) }}

select
    t.trial_key,
    t.nct_id,
    tc.therapeutic_area,
    tc.sponsor_category,
    tc.phase_category,
    td.duration_days,
    td.start_year,
    td.start_month,
    td.completion_year,
    td.completion_month,
    t.is_active,
    t.is_completed,
    t.is_international,
    t.extraction_date,
    t.batch_id
from {{ ref('stg_clinical_trials') }} t
left join {{ ref('int_trial_categorization') }} tc
    on t.trial_key = tc.trial_key
left join {{ ref('int_trial_duration') }} td
    on t.trial_key = td.trial_key