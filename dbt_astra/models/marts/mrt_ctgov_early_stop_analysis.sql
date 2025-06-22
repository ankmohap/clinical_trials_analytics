with trials as (

    select
        sponsor_class,
        lower(status) as status,
        case 
            when lower(status) in ('terminated', 'withdrawn', 'suspended') then 1 
            else 0 
        end as is_early_stop,
        case 
            when lower(status) in ('terminated', 'withdrawn', 'suspended', 'completed') then 1
            else 0
        end as is_closed
    from {{ ref('stg_ctgov__studies') }}
    where sponsor_class is not null

),

summary as (

    select
        sponsor_class,
        count_if(is_early_stop = 1) as early_stopped_trials,
        count_if(is_closed = 1) as closed_trials,
        round(
            100.0 * count_if(is_early_stop = 1)::float / nullif(count_if(is_closed = 1), 0),
            2
        ) as early_stop_pct,
        round(
            count_if(is_early_stop = 1)::float / nullif(count_if(is_closed = 1), 0),
            4
        ) as early_stop_rate
    from trials
    group by sponsor_class

)

select * from summary
