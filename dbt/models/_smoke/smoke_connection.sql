{{
    config(
        enabled=var("enable_smoke_model", false),
        materialized="table",
        tags=["smoke"],
    )
}}

-- Grain: exactly one deterministic row.
-- This model exists only to validate dbt's create-and-read access to the
-- dedicated Gold schema in the test catalog.
select
    cast(1 as bigint) as smoke_test_id
