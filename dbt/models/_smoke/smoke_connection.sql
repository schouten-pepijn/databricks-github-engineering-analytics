{{
    config(
        enabled=var("enable_smoke_model", false),
        materialized="table",
        tags=["smoke"],
    )
}}

-- Grain: exactly one deterministic row.
-- This operational fixture is disabled by default and enabled explicitly by
-- ``task dbt:run-smoke``. It proves dbt can create and read a Gold table in the
-- test catalog without becoming a normal analytics relation.
select
    cast(1 as bigint) as smoke_test_id
