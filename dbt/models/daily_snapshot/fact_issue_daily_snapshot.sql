-- Grain: one Issue per observed UTC date. For each day that has coverage, the
-- row is the newest Bronze observation of that Issue ingested before the end
-- of the day. A day without coverage gets no rows: a gap is not an empty
-- backlog. See snapshot_day_coverage for how a day becomes observed.
{{ config(materialized='table') }}

with observed_days as (
    select snapshot_date
    from {{ ref('snapshot_day_coverage') }}
    where coverage = 'observed'
),

-- Pair each observed day with every observation that already existed by the
-- end of that day. One Issue therefore appears once per earlier observation.
-- An observation made on a day with no coverage still counts from the next
-- observed day on, because it was ingested before that day ended.
--
-- Cost note: this pairs every observed day with every earlier observation, so
-- the work grows with days times observations. That is fine at today's size;
-- Sprint 8 measures it.
candidates as (
    select
        observed_days.snapshot_date,
        observations.manifest_id,
        observations.repository_owner,
        observations.repository_name,
        observations.issue_id,
        observations.run_id,
        observations.ingested_at,
        observations.source_updated_at,
        observations.state,
        observations.created_at
    from observed_days
    join {{ ref('stg_bronze_issue_observations') }} as observations
        -- date_add(day, 1) is midnight at the end of the day.
        on observations.ingested_at < date_add(observed_days.snapshot_date, 1)
),

-- Number the observations of one Issue on one day, newest first, so that
-- rank 1 is the state of the Issue at the end of that day.
ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date, repository_owner, repository_name, issue_id
            order by ingested_at desc, source_updated_at desc, run_id desc
        ) as observation_rank
    from candidates
)

select
    -- Keep both hashes identical to fact_issue.issue_key and
    -- dim_repository.repository_key, so a snapshot row joins to those tables.
    sha2(
        concat(
            repository_owner,
            '||',
            repository_name,
            '||',
            cast(issue_id as string)
        ),
        256
    ) as issue_key,
    sha2(
        concat(
            repository_owner,
            '||',
            repository_name
        ),
        256
    ) as repository_key,
    snapshot_date,
    repository_owner,
    repository_name,
    issue_id,
    state,
    state = 'open' as is_open_on_date,
    -- Whole days between creation and the snapshot date. Both dates use the
    -- time zone of the SQL warehouse session, which is UTC by default.
    datediff(snapshot_date, to_date(created_at)) as age_days,
    run_id as observation_run_id,
    -- When the pipeline read this observation, not when the Issue changed.
    ingested_at as observed_at,
    manifest_id
from ranked
where observation_rank = 1
