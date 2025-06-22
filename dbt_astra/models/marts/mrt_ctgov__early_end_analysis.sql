with trials as (
    select
        sponsor_class,
        lower(status) as status,
        case 
            when lower(status) in ('terminated', 'withdrawn') then 1 
            else 0 
        end as is_ended_early
    from {{ ref('stg_ctgov__studies') }}
    where sponsor_class is not null

),

summary as (

    select
        sponsor_class,
        count(*) as total_trials,
        sum(is_ended_early) as early_ended_trials,
        round(100.0 * sum(is_ended_early)::float / count(*), 2) as early_end_pct
    from trials
    group by sponsor_class

),

max_early_end as (

    select 
        sponsor_class as highest_early_end_class,
        early_end_pct as highest_early_end_pct
    from summary
    order by early_end_pct desc
    limit 1

),

industry_comparison as (

    select 
        early_end_pct as industry_early_end_pct
    from summary
    where sponsor_class = 'INDUSTRY'

)

select 
    m.highest_early_end_class,
    m.highest_early_end_pct,
    i.industry_early_end_pct
from max_early_end m
left join industry_comparison i on true
