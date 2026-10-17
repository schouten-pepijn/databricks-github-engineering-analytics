-- One row per label on each Bronze observation of an Issue. An observation
-- with an empty labels array produces no rows: that is how "the label was
-- removed" shows up in the history.
{{ config(materialized='ephemeral') }}

select
    observations.repository_owner,
    observations.repository_name,
    observations.issue_id,
    observations.run_id,
    labels.id as label_id,
    labels.name as label_name
from {{ ref('stg_bronze_issue_observations') }} as observations,
    lateral inline(observations.labels) as labels
