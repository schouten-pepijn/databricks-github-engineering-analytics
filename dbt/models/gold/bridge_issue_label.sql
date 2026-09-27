{{
    config(
        materialized="table",
        tags=["gold", "bridge"],
    )
}}

-- Grain: exactly one current Issue-to-Label relationship per repository-scoped
-- issue ID and repository-scoped label ID.
-- This factless bridge resolves the many-to-many relationship between the
-- future fact_issue table and the current Type-1 dim_label dimension.
-- Both hashes must remain aligned with the future fact_issue and dim_label
-- surrogate-key formulas.
select
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
            repository_name,
            '||',
            cast(label_id as string)
        ),
        256
    ) as label_key,
    repository_owner,
    repository_name,
    issue_id,
    label_id
from {{ source("silver", "github_issue_labels") }}
