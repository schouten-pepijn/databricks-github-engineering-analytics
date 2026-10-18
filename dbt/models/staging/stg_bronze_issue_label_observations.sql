-- One row per label on each Bronze observation of an Issue. An observation
-- with an empty labels array produces no rows: that is how "the label was
-- removed" shows up in the history.
{{ config(materialized='ephemeral') }}

-- "lateral inline" turns each struct in the labels array into its own row,
-- with the struct fields as columns (id and name). A "from a, lateral b"
-- comma is how Spark SQL attaches that per-row expansion to the outer table.
select
    observations.repository_owner,
    observations.repository_name,
    observations.issue_id,
    observations.run_id,
    labels.id as label_id,
    labels.name as label_name
from {{ ref('stg_bronze_issue_observations') }} as observations,
    lateral inline(observations.labels) as labels
