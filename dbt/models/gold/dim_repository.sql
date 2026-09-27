{{
    config(
        materialized="table",
        tags=["gold", "dimension"],
    )
}}

-- Grain: exactly one row per repository business key.
-- repository_key is a deterministic SHA-256 surrogate key used by Gold facts.
-- The delimiter is safe because GitHub owner and repository names cannot
-- contain a pipe character.
-- ``distinct`` deliberately changes the source grain from one current issue
-- to one repository, so every issue in the same repository shares one key.
with repositories as (

    select distinct
        repository_owner,
        repository_name
    -- Silver Issues is the authoritative current-state source for repositories
    -- observed by this pipeline; dbt must not recreate that extraction logic.
    from {{ source("silver", "github_issues") }}

)

select
    sha2(
        concat(repository_owner, '||', repository_name),
        256
    ) as repository_key,
    repository_owner,
    repository_name
from repositories
