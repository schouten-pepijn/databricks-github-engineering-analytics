"""Finalize the control-state lifecycle after the native dbt Gold gate."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from loguru import logger
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import (
    PipelineRun,
    PipelineRunStatus,
)
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.watermark_repository import (
    DeltaWatermarkRepository,
)

_SOURCE_NAME = "github"
_ENTITY_NAME = "issues"
_WATERMARK_COLUMN = "updated_at"


def _current_utc_time() -> datetime:
    """Return the current UTC timestamp; tests inject a deterministic clock."""
    return datetime.now(UTC)


def finalize_successful_run(
    *,
    spark: SparkSession,
    catalog: str,
    run_id: str,
    clock: Callable[[], datetime] = _current_utc_time,
) -> PipelineRun:
    """Commit a pending candidate and mark its run successful after Gold.

    The native dbt task is the Gold quality gate. This function runs only after
    that task succeeds. It first advances the monotonic watermark and then
    writes the terminal run status. If the second write is interrupted, a retry
    sees the still-running row, replays the idempotent watermark merge, and can
    complete the lifecycle without losing the source position.
    """
    pipeline_runs, watermarks = _create_repositories(spark=spark, catalog=catalog)
    run = pipeline_runs.get(run_id)

    if run.status is PipelineRunStatus.SUCCEEDED:
        logger.bind(run_id=run_id).info("Pipeline run is already finalized.")
        return run

    if run.status is PipelineRunStatus.FAILED:
        raise ValueError(f"Cannot finalize failed pipeline run: run_id={run_id!r}")

    # One explicit timestamp represents the successful terminal transition in
    # both control tables, making the audit trail easier to correlate.
    finalized_at = clock()
    if run.candidate_watermark is not None:
        watermarks.commit_success(
            source_name=_SOURCE_NAME,
            entity_name=_ENTITY_NAME,
            watermark_column=_WATERMARK_COLUMN,
            watermark=run.candidate_watermark,
            run_id=run.run_id,
            committed_at=finalized_at,
        )

    succeeded_run = run.succeed(
        candidate_watermark=run.candidate_watermark,
        finished_at=finalized_at,
    )
    pipeline_runs.record_finished(succeeded_run)
    logger.bind(run_id=run_id).info("Finalized successful pipeline run.")

    return succeeded_run


def finalize_failed_run(
    *,
    spark: SparkSession,
    catalog: str,
    run_id: str,
    failure_reason: str,
    clock: Callable[[], datetime] = _current_utc_time,
) -> PipelineRun:
    """Mark a pending run failed without changing its committed watermark.

    A Bronze/Silver failure may already have recorded the terminal state. That
    is a safe no-op here, allowing the job's failure-cleanup branch to depend on
    every upstream stage without trying to regress an immutable terminal row.
    """
    if not failure_reason.strip():
        raise ValueError("failure_reason must not be empty")

    pipeline_runs, _ = _create_repositories(spark=spark, catalog=catalog)
    run = pipeline_runs.get(run_id)

    if run.status is PipelineRunStatus.FAILED:
        logger.bind(run_id=run_id).info("Pipeline run is already marked failed.")
        return run

    if run.status is PipelineRunStatus.SUCCEEDED:
        raise ValueError(f"Cannot fail successful pipeline run: run_id={run_id!r}")

    failed_run = run.fail(
        error_message=failure_reason,
        finished_at=clock(),
    )
    pipeline_runs.record_finished(failed_run)
    logger.bind(run_id=run_id).error("Finalized failed pipeline run.")

    return failed_run


def _create_repositories(
    *,
    spark: SparkSession,
    catalog: str,
) -> tuple[DeltaPipelineRunRepository, DeltaWatermarkRepository]:
    """Create the paired control repositories required by finalization."""
    config = PipelineConfig(catalog=catalog)
    pipeline_runs = DeltaPipelineRunRepository(spark=spark, config=config)
    watermarks = DeltaWatermarkRepository(spark=spark, config=config)
    pipeline_runs.ensure_table()
    watermarks.ensure_table()

    return pipeline_runs, watermarks
