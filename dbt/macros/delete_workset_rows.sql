-- Pre-hook of the incremental bridge: delete the bridge rows of every Issue in
-- the workset. It renders to nothing on the first run, because the table does
-- not exist yet and there is nothing to delete.
{% macro delete_workset_rows() -%}
    {%- if is_incremental() -%}
        delete from {{ this }} as bridge
        where exists (
            select 1
            from {{ source("workset", "changed_issues") }} as workset
            where workset.repository_owner = bridge.repository_owner
              and workset.repository_name = bridge.repository_name
              and workset.issue_id = bridge.issue_id
        )
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
