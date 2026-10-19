-- dbt normally builds "<target schema>_<custom schema>", which would give
-- github_analytics_gold_snapshot. This macro uses the custom schema name as
-- it is, so +schema: snapshot becomes exactly github_analytics_snapshot.
-- Models without a custom schema still go to the target schema, as before.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Safety net: a build that reads a non-default Silver schema must also say
        where to write, otherwise it would overwrite the real Gold tables with
        results built from a frozen copy. -#}
    {%- if var('silver_schema', none) is not none and var('gold_output_schema', none) is none -%}
        {{ exceptions.raise_compiler_error(
            "silver_schema is set, so gold_output_schema must be set too. "
            "Otherwise the real Gold tables would be built from a frozen copy."
        ) }}
    {%- endif -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
