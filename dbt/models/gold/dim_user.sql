{{
    config(
        materialized="table",
        tags=["gold", "dimension"],
    )
}}

-- Grain: exactly one row per global GitHub user ID.
-- GitHub user IDs are global, so a user can appear in many repositories and
-- issues while remaining one dimension member.
-- user_key is a deterministic surrogate key for future fact_issue joins.
select
    sha2(cast(user_id as string), 256) as user_key,
    user_id,
    login,
    user_type
from {{ source("silver", "github_users") }}
