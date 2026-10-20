{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="issue_key",
        tags=["candidate", "fact"],
    )
}}

-- Incremental CANDIDATE of gold/fact_issue. It is not served to BI: a proof
-- compares it with a full build of fact_issue over the same frozen Silver input.
--
-- First run (or --full-refresh): builds from all of Silver, like the full model.
-- Later runs: only the Issues in the workset are read from Silver and merged on
-- issue_key. The workset is the list of Issues whose Silver rows really changed
-- (see silver/changed_issues.py). Every other Issue keeps its existing row.
--
-- The column logic below must stay identical to gold/fact_issue.sql, otherwise
-- the comparison proves nothing.
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
    from {{ source("silver", "github_issues") }} as silver_issues
    {% if is_incremental() %}
    -- EXISTS on the full business key. Unlike a join it cannot multiply rows,
    -- even if the workset had a duplicate.
    where exists (
        select 1
        from {{ source("workset", "changed_issues") }} as workset
        where workset.repository_owner = silver_issues.repository_owner
          and workset.repository_name = silver_issues.repository_name
          and workset.issue_id = silver_issues.issue_id
    )
    {% endif %}
)

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
        concat(repository_owner, '||', repository_name),
        256
    ) as repository_key,
    sha2(cast(author_user_id as string), 256) as author_key,
    cast(date_format(created_at, 'yyyyMMdd') as int) as created_date_key,
    cast(date_format(updated_at, 'yyyyMMdd') as int) as updated_date_key,
    case
        when closed_at is not null then cast(date_format(closed_at, 'yyyyMMdd') as int)
    end as closed_date_key,
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
