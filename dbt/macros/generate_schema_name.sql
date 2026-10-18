-- dbt normally builds "<target schema>_<custom schema>", which would give
-- github_analytics_gold_snapshot. This macro uses the custom schema name as
-- it is, so +schema: snapshot becomes exactly github_analytics_snapshot.
-- Models without a custom schema still go to the target schema, as before.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
