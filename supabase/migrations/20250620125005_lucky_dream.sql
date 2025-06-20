{{ config(
    materialized='table',
    tags=['marts', 'analysis']
) }}

select
    location_country_clean,
    count(*) as total_studies,
    count(case when is_active then 1 end) as active_studies,
    count(case when is_completed then 1 end) as completed_studies,
    count(distinct therapeutic_area) as unique_therapeutic_areas,
    count(distinct sponsor_category) as unique_sponsor_types,
    count(case when phase_category = 'Early Phase' then 1 end) as early_phase_studies,
    count(case when phase_category = 'Late Phase' then 1 end) as late_phase_studies,
    avg(duration_days) as avg_duration_days,
    max(extraction_date) as last_updated
from {{ ref('fact_trial_metrics') }}
group by location_country_clean
order by total_studies desc