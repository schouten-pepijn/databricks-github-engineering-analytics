{{
    config(
        materialized="table",
        tags=["gold", "dimension"],
    )
}}

-- Grain: exactly one row per repository-scoped GitHub label ID.
-- A label ID is not globally unique, so the repository business key is part of
-- both the natural key and the deterministic Gold surrogate key.
-- This is a current Type 1 dimension; Silver owns source lineage and freshness.
select
    sha2(
        concat(
            repository_owner,
            '||',
            repository_name,
            '||',
            cast(label_id as string)
        ),
        256
    ) as label_key,
    repository_owner,
    repository_name,
    label_id,
    name,
    color,
    description,
    is_default
from {{ source("silver", "github_labels") }}
