{{ config(
    materialized='view',
    tags=['intermediate', 'clinical_trials']
) }}

with trial_categories as (
    select
        trial_key,
        nct_id,
        condition_clean,
        phase,
        study_type,
        sponsor_name_clean,
        
        -- Categorize conditions into therapeutic areas
        case 
            when condition_clean like '%CANCER%' or condition_clean like '%TUMOR%' 
                or condition_clean like '%ONCOLOGY%' or condition_clean like '%CARCINOMA%'
            then 'Oncology'
            when condition_clean like '%DIABETES%' or condition_clean like '%GLUCOSE%'
            then 'Endocrinology'
            when condition_clean like '%HEART%' or condition_clean like '%CARDIAC%' 
                or condition_clean like '%CARDIOVASCULAR%'
            then 'Cardiology'
            when condition_clean like '%BRAIN%' or condition_clean like '%NEUROLOG%' 
                or condition_clean like '%ALZHEIMER%' or condition_clean like '%PARKINSON%'
            then 'Neurology'
            when condition_clean like '%INFECTION%' or condition_clean like '%VIRUS%' 
                or condition_clean like '%BACTERIA%'
            then 'Infectious Disease'
            when condition_clean like '%MENTAL%' or condition_clean like '%DEPRESSION%' 
                or condition_clean like '%ANXIETY%' or condition_clean like '%PSYCHIATRIC%'
            then 'Psychiatry'
            else 'Other'
        end as therapeutic_area,
        
        -- Categorize sponsors
        case 
            when sponsor_name_clean like '%UNIVERSITY%' or sponsor_name_clean like '%COLLEGE%'
            then 'Academic'
            when sponsor_name_clean like '%PHARMACEUTICAL%' or sponsor_name_clean like '%PHARMA%'
                or sponsor_name_clean like '%BIOTECH%' or sponsor_name_clean like '%THERAPEUTICS%'
            then 'Industry'
            when sponsor_name_clean like '%HOSPITAL%' or sponsor_name_clean like '%MEDICAL CENTER%'
            then 'Healthcare'
            when sponsor_name_clean like '%GOVERNMENT%' or sponsor_name_clean like '%NIH%' 
                or sponsor_name_clean like '%FDA%'
            then 'Government'
            else 'Other'
        end as sponsor_category,
        
        -- Phase categorization
        case 
            when phase in ('Phase 1', 'Early Phase 1') then 'Early Phase'
            when phase in ('Phase 2', 'Phase 1|Phase 2') then 'Mid Phase'
            when phase in ('Phase 3', 'Phase 2|Phase 3') then 'Late Phase'
            when phase = 'Phase 4' then 'Post Market'
            else 'Not Applicable'
        end as phase_category
        
    from {{ ref('stg_clinical_trials') }}
)

select * from trial_categories