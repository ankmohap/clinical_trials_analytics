{{ config(
    materialized='table',
    tags=['marts', 'analysis']
) }}

select
    sponsor_name_clean,
    sponsor_category,
    count(*) as total_studies,
    count(case when is_active then 1 end) as active_studies,
    count(case when is_completed then 1 end) as completed_studies,
    count(distinct therapeutic_area) as unique_therapeutic_areas,
    count(case when is_international then 1 end) as international_studies,
    avg(duration_days) as avg_duration_days,
    min(start_year) as first_study_year,
    max(start_year) as latest_study_year,
    max(extraction_date) as last_updated
from {{ ref('fact_trial_metrics') }}
group by sponsor_name_clean, sponsor_category
having count(*) >= 2  -- Only sponsors with 2+ studies
order by total_studies desc