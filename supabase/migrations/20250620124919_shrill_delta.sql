{{ config(
    materialized='view',
    tags=['intermediate', 'clinical_trials']
) }}

with trial_dates as (
    select
        trial_key,
        nct_id,
        study_start_date,
        completion_date,
        last_update_submit_date,
        is_completed,
        
        -- Calculate duration in days
        case 
            when study_start_date is not null and completion_date is not null
            then datediff('day', study_start_date, completion_date)
            when study_start_date is not null and completion_date is null and is_completed = false
            then datediff('day', study_start_date, current_date())
            else null
        end as duration_days,
        
        -- Calculate years and months for grouping
        case 
            when study_start_date is not null 
            then extract(year from study_start_date)
            else null
        end as start_year,
        
        case 
            when study_start_date is not null 
            then extract(month from study_start_date)
            else null
        end as start_month,
        
        case 
            when completion_date is not null 
            then extract(year from completion_date)
            else null
        end as completion_year,
        
        case 
            when completion_date is not null 
            then extract(month from completion_date)
            else null
        end as completion_month
        
    from {{ ref('stg_clinical_trials') }}
)

select * from trial_dates