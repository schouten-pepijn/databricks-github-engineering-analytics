-- Read-only control-plane health snapshot.
--
-- Grain:
-- - `pipeline_run` rows: one row per recent pipeline lifecycle.
-- - `committed_watermark` rows: one row per source entity.
--
-- The caller supplies `:catalog` as a parameter. IDENTIFIER keeps the
-- environment-specific object name parameterized rather than concatenating it
-- into SQL in the task runner.
WITH recent_pipeline_runs AS (
    SELECT
        run_id,
        status,
        started_at,
        finished_at,
        watermark_before_value,
        candidate_watermark_value,
        error_message
    FROM IDENTIFIER(:catalog || '.github_analytics_control.pipeline_runs')
    ORDER BY
        started_at DESC,
        run_id DESC
    LIMIT 10
)

SELECT
    'pipeline_run' AS record_type,
    1 AS record_type_order,
    recent_pipeline_runs.run_id,
    recent_pipeline_runs.status,
    recent_pipeline_runs.started_at,
    recent_pipeline_runs.finished_at,
    recent_pipeline_runs.watermark_before_value,
    recent_pipeline_runs.candidate_watermark_value,
    recent_pipeline_runs.error_message,
    CAST(NULL AS STRING) AS source_name,
    CAST(NULL AS STRING) AS entity_name,
    CAST(NULL AS STRING) AS watermark_column,
    CAST(NULL AS TIMESTAMP) AS committed_watermark_value,
    CAST(NULL AS INT) AS overlap_seconds,
    CAST(NULL AS STRING) AS last_successful_run_id,
    CAST(NULL AS TIMESTAMP) AS watermark_updated_at
FROM recent_pipeline_runs

UNION ALL

SELECT
    'committed_watermark' AS record_type,
    2 AS record_type_order,
    CAST(NULL AS STRING) AS run_id,
    CAST(NULL AS STRING) AS status,
    CAST(NULL AS TIMESTAMP) AS started_at,
    CAST(NULL AS TIMESTAMP) AS finished_at,
    CAST(NULL AS TIMESTAMP) AS watermark_before_value,
    CAST(NULL AS TIMESTAMP) AS candidate_watermark_value,
    CAST(NULL AS STRING) AS error_message,
    ingestion_watermark.source_name,
    ingestion_watermark.entity_name,
    ingestion_watermark.watermark_column,
    ingestion_watermark.watermark_value AS committed_watermark_value,
    ingestion_watermark.overlap_seconds,
    ingestion_watermark.last_successful_run_id,
    ingestion_watermark.updated_at AS watermark_updated_at
FROM IDENTIFIER(:catalog || '.github_analytics_control.ingestion_watermark')
    AS ingestion_watermark

ORDER BY
    record_type_order,
    started_at DESC NULLS LAST,
    run_id DESC NULLS LAST,
    source_name ASC NULLS LAST,
    entity_name ASC NULLS LAST;
