{{ config(
    materialized='table',
    tags=['marts', 'summary'],
    post_hook="grant select on {{ this }} to role analyst_role"
) }}

with monthly_metrics as (
    select
        concat(start_year, '-', lpad(start_month, 2, '0')) as year_month,
        count(*) as total_studies,
        count(case when is_active then 1 end) as active_studies,
        count(case when is_completed then 1 end) as completed_studies,
        count(case when study_type = 'Interventional' then 1 end) as interventional_studies,
        count(case when study_type = 'Observational' then 1 end) as observational_studies,
        count(case when phase_category = 'Early Phase' then 1 end) as early_phase_studies,
        count(case when phase_category = 'Mid Phase' then 1 end) as mid_phase_studies,
        count(case when phase_category = 'Late Phase' then 1 end) as late_phase_studies,
        count(case when phase_category = 'Post Market' then 1 end) as post_market_studies,
        count(case when is_international then 1 end) as international_studies,
        count(distinct therapeutic_area) as unique_therapeutic_areas,
        count(distinct sponsor_category) as unique_sponsor_categories,
        avg(duration_days) as avg_duration_days,
        max(extraction_date) as last_updated
    from {{ ref('fact_trial_metrics') }}
    where start_year is not null and start_month is not null
    group by start_year, start_month
)

select * from monthly_metrics
order by year_month desc