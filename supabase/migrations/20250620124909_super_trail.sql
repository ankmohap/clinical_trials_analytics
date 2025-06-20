{{ config(
    materialized='view',
    tags=['staging', 'clinical_trials']
) }}

with source_data as (
    select * from {{ source('raw_data', 'staging_clinical_trials') }}
),

cleaned_data as (
    select
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['nct_id']) }} as trial_key,
        
        -- Core identifiers
        nct_id,
        brief_title,
        official_title,
        
        -- Status and type
        overall_status,
        study_type,
        phase,
        
        -- Clean and standardize condition
        case 
            when condition is null or trim(condition) = '' then 'Unknown'
            else trim(upper(condition))
        end as condition_clean,
        
        -- Clean sponsor name
        case 
            when sponsor_name is null or trim(sponsor_name) = '' then 'Unknown'
            else trim(sponsor_name)
        end as sponsor_name_clean,
        
        -- Clean location data
        case 
            when location_country is null or trim(location_country) = '' then 'Unknown'
            else trim(upper(location_country))
        end as location_country_clean,
        
        location_state,
        location_city,
        
        -- Intervention details
        intervention_name,
        primary_outcome_measure,
        
        -- Dates
        study_first_submit_date as study_start_date,
        last_update_submit_date,
        completion_date,
        
        -- Derived flags
        case 
            when overall_status in ('Recruiting', 'Enrolling by invitation', 'Active, not recruiting') 
            then true 
            else false 
        end as is_active,
        
        case 
            when overall_status in ('Completed', 'Terminated', 'Withdrawn') 
            then true 
            else false 
        end as is_completed,
        
        case 
            when location_country != 'UNITED STATES' or location_country is null 
            then true 
            else false 
        end as is_international,
        
        -- Metadata
        extraction_timestamp,
        date(extraction_timestamp) as extraction_date,
        data_source,
        batch_id,
        created_at,
        updated_at
        
    from source_data
    where nct_id is not null
)

select * from cleaned_data