{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        tags=["candidate", "bridge"],
        pre_hook=[
            "{{ delete_workset_rows() }}"
        ],
    )
}}

-- Incremental CANDIDATE of gold/bridge_issue_label. See fact_issue_incremental
-- for the idea. A bridge needs one extra step: when an Issue loses its last label
-- there is no new row to merge on, so a merge would never remove the old one.
-- The delete therefore comes from the WORKSET, not from the new rows:
--   1. pre_hook: delete every bridge row of the Issues in the workset
--   2. this select: append the current Silver rows of those same Issues
-- An Issue with no labels left gets step 1 but no rows in step 2. Running the
-- same batch twice gives the same table, because step 1 undoes the earlier step 2.
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
from {{ source("silver", "github_issue_labels") }} as silver_issue_labels
{% if is_incremental() %}
where exists (
    select 1
    from {{ source("workset", "changed_issues") }} as workset
    where workset.repository_owner = silver_issue_labels.repository_owner
      and workset.repository_name = silver_issue_labels.repository_name
      and workset.issue_id = silver_issue_labels.issue_id
)
{% endif %}
