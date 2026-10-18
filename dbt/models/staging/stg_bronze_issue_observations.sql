-- One row per Bronze observation of an Issue that belongs to a replay
-- manifest: the manifest's repository, its successful runs, and the Bronze
-- version it locked. This is the history that Silver does not keep.
{{ config(materialized='ephemeral') }}

-- Read the manifest ID from the var once, and reuse it below.
{% set manifest_id = var('snapshot_manifest_id') %}

{#
  Time travel needs a literal version number, so look it up in the manifest.
  Doing it here, and not in a second var, means the version can never disagree
  with the manifest. run_query only works at execution time, so the first
  parsing pass gets a placeholder.
#}
-- "execute" is false during dbt's first pass, which only reads the project
-- structure. On that pass no database is available, so we skip the lookup.
{% if execute %}
    -- Ask the control table which Bronze version this manifest locked.
    {% set version_query %}
        select bronze_version
        from {{ source('control', 'replay_manifests') }}
        where manifest_id = '{{ manifest_id }}'
    {% endset %}
    {% set version_result = run_query(version_query) %}
    {% if version_result.rows | length == 0 %}
        {{ exceptions.raise_compiler_error("Unknown manifest: " ~ manifest_id) }}
    {% endif %}
    {% set bronze_version = version_result.columns[0].values()[0] %}
{% else %}
    {% set bronze_version = 0 %}
{% endif %}

with manifest as (
    select
        manifest_id,
        repository_owner,
        repository_name
    from {{ source('control', 'replay_manifests') }}
    where manifest_id = '{{ manifest_id }}'
),

bronze as (
    -- Delta time travel: read the table as it was at the locked version.
    -- Later appends to Bronze cannot change what this model sees.
    select *
    from {{ source('bronze', 'github_issues_raw') }}
        version as of {{ bronze_version }}
),

-- Only observations from runs the manifest locked are kept. The join
-- narrows the rows to this manifest's repository.
parsed as (
    select
        manifest.manifest_id,
        bronze.repository_owner,
        bronze.repository_name,
        bronze.issue_id,
        bronze._run_id as run_id,
        bronze._ingested_at as ingested_at,
        bronze.source_updated_at,
        -- The payload schema is declared here again because the Python
        -- parsing in silver/issues.py cannot be called from dbt. Keep the two
        -- in step when GitHub fields change.
        from_json(
            bronze.raw_json,
            'struct<number: bigint,
                state: string,
                created_at: string,
                closed_at: string,
                labels: array<struct<id: bigint, name: string>>>'
        ) as payload
    from bronze
    join manifest
        on bronze.repository_owner = manifest.repository_owner
        and bronze.repository_name = manifest.repository_name
    where bronze._run_id in (select run_id from {{ ref('stg_manifest_runs') }})
)

select
    manifest_id,
    repository_owner,
    repository_name,
    issue_id,
    run_id,
    ingested_at,
    source_updated_at,
    payload.number as issue_number,
    payload.state as state,
    -- try_to_timestamp gives null for a malformed value instead of failing.
    try_to_timestamp(payload.created_at) as created_at,
    try_to_timestamp(payload.closed_at) as closed_at,
    -- Kept for the label staging model; the Issue snapshot does not use it.
    payload.labels as labels
from parsed
