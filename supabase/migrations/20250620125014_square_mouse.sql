-- Test to ensure all NCT IDs follow the expected format
select *
from {{ ref('stg_clinical_trials') }}
where not regexp_like(nct_id, '^NCT[0-9]{8}$')