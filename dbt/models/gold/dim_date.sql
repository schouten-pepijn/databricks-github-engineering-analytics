{{
    config(
        materialized="table",
        tags=["gold", "dimension"],
    )
}}

-- Grain: exactly one row per calendar date.
-- date_key is a deterministic YYYYMMDD surrogate key for fact-table joins.
-- The configured boundaries make every rebuild reproducible.
-- Type 0 dimension
with calendar_dates as (
    select
        explode(
            sequence(
                date '{{ var("date_dimension_start") }}',
                date '{{ var("date_dimension_end") }}',
                interval 1 day
            )
        ) as calendar_date
)

select
    cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_key,
    calendar_date,
    year(calendar_date) as calendar_year,
    quarter(calendar_date) as calendar_quarter,
    month(calendar_date) as month_number,
    date_format(calendar_date, 'MMMM') as month_name,
    dayofmonth(calendar_date) as day_of_month,
    case
        when dayofweek(calendar_date) = 1 then 7
        else dayofweek(calendar_date) - 1
    end as iso_day_of_week_number,
    date_format(calendar_date, 'EEEE') as day_name,
    case
        when dayofweek(calendar_date) in (1, 7) then true
        else false
    end as is_weekend
from calendar_dates
