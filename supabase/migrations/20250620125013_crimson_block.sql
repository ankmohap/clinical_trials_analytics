-- Test to ensure we have positive trial counts in our summary tables
select *
from {{ ref('mart_monthly_summary') }}
where total_studies <= 0