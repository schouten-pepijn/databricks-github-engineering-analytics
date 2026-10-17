-- One row per successful run that a replay manifest locked, with the time the
-- run finished. The daily snapshot models use finished_at to decide whether a
-- day was observed.
--
-- The manifest comes from a var with no default, so forgetting it fails the
-- compile instead of silently reading the wrong manifest.
-- Ephemeral: dbt writes nothing for this model; it is inlined where it is used.
{{ config(materialized='ephemeral') }}

with manifest as (
    select
        manifest_id,
        -- One row per run ID. explode() turns the array into rows.
        explode(successful_run_ids) as run_id
    from {{ source("control", "replay_manifests") }}
    where manifest_id = '{{ var("snapshot_manifest_id") }}'
)

select
    manifest.manifest_id,
    manifest.run_id,
    runs.finished_at
from manifest
join {{ source("control", "pipeline_runs") }} as runs
    on runs.run_id = manifest.run_id
-- Only runs that ended successfully count as an observation.
where runs.status = 'succeeded'
