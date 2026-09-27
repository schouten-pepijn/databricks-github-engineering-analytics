{{
    config(
        materialized="table",
        tags=["gold", "fact"],
    )
}}

-- Grain: exactly one current GitHub Issue per repository owner, repository
-- name, and GitHub Issue ID. Silver has already selected the latest source
-- state at this grain; this full-build Gold model does not apply another
-- potentially ambiguous deduplication rule.
--
-- This is a current-state fact, not an event history. The descriptive Issue
-- fields remain as degenerate attributes for audit drill-through. The date
-- keys, repository key, and author key make it the central Gold relation for
-- BI analysis and for bridge_issue_label's many-to-many Label relationship.
with issues as (
    select
        repository_owner,
        repository_name,
        issue_id,
        issue_number,
        title,
        state,
        is_pull_request,
        author_user_id,
        created_at,
        updated_at,
        closed_at,
        source_run_id
    from {{ source("silver", "github_issues") }}
)

select
    -- Keep this formula identical to bridge_issue_label.issue_key.
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
    -- Keep this formula identical to dim_repository.repository_key.
    sha2(
        concat(repository_owner, '||', repository_name),
        256
    ) as repository_key,
    -- Keep this formula identical to dim_user.user_key.
    sha2(cast(author_user_id as string), 256) as author_key,
    cast(date_format(created_at, 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(updated_at, 'yyyyMMdd') as int) as updated_date_key,
    case
        when closed_at is not null then cast(date_format(closed_at, 'yyyyMMdd') as int)
    end as closed_date_key,
    -- Preserve source business keys and current descriptive values for
    -- transparent drill-through without forcing BI consumers back to Silver.
    repository_owner,
    repository_name,
    issue_id,
    issue_number,
    title,
    state,
    is_pull_request,
    author_user_id,
    created_at,
    updated_at,
    closed_at,
    source_run_id
from issues
