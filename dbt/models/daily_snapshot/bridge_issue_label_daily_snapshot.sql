-- Grain: one Issue-to-Label relation per observed UTC date. The labels come
-- from the same Bronze observation that fact_issue_daily_snapshot picked for
-- that Issue and date, so a label removed later stays on the earlier date and
-- never appears on the later one. A day without coverage has no fact rows and
-- therefore no relations either.
{{ config(materialized='table') }}

select
    snapshot.snapshot_date,
    snapshot.repository_owner,
    snapshot.repository_name,
    snapshot.issue_id,
    labels.label_id,
    snapshot.observation_run_id,
    snapshot.manifest_id
from {{ ref('fact_issue_daily_snapshot') }} as snapshot
-- An inner join: an observation without label rows (the label was removed, or
-- there never was one) gives no relation, which is exactly the meaning we want.
join {{ ref('stg_bronze_issue_label_observations') }} as labels
    on labels.repository_owner = snapshot.repository_owner
    and labels.repository_name = snapshot.repository_name
    and labels.issue_id = snapshot.issue_id
    -- Same observation as the Issue snapshot picked.
    and labels.run_id = snapshot.observation_run_id
