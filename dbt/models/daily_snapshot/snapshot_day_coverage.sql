-- One row per calendar date between the snapshot bounds. A date is "observed"
-- when a successful run of the manifest finished on it, otherwise "missing".
-- A missing date is a visible gap: it must never read as an empty backlog.
{{ config(materialized='table') }}

with dates as (
    -- sequence() lists every date from start to end, explode() makes them rows.
    select explode(
        sequence(
            to_date('{{ var("snapshot_start_date") }}'),
            to_date('{{ var("snapshot_end_date") }}'),
            interval 1 day
        )
    ) as snapshot_date
),

run_days as (
    -- to_date() uses the session timezone; SQL warehouses run in UTC by default.
    select distinct to_date(finished_at) as run_date
    from {{ ref('stg_manifest_runs') }}
)

select
    dates.snapshot_date,
    case
        when run_days.run_date is null then 'missing'
        else 'observed'
    end as coverage
from dates
left join run_days
    on run_days.run_date = dates.snapshot_date
