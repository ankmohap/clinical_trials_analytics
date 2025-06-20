{{ config(
    materialized='table',
    tags=['marts', 'analysis']
) }}

select
    therapeutic_area,
    count(*) as total_studies,
    count(case when is_active then 1 end) as active_studies,
    count(case when is_completed then 1 end) as completed_studies,
    count(case when phase_category = 'Early Phase' then 1 end) as early_phase_studies,
    count(case when phase_category = 'Mid Phase' then 1 end) as mid_phase_studies,
    count(case when phase_category = 'Late Phase' then 1 end) as late_phase_studies,
    count(case when phase_category = 'Post Market' then 1 end) as post_market_studies,
    count(distinct sponsor_category) as unique_sponsor_types,
    avg(duration_days) as avg_duration_days,
    min(start_year) as earliest_study_year,
    max(start_year) as latest_study_year,
    max(extraction_date) as last_updated
from {{ ref('fact_trial_metrics') }}
group by therapeutic_area
order by total_studies desc