"""Coordinate one tracked GitHub Issues Bronze-to-Silver lifecycle."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from loguru import logger
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.full_load import (
    run_full_load,
    run_incremental_load,
)
from github_engineering_analytics.bronze.ingestion import BronzeIngestionResult
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.watermark_repository import (
    DeltaWatermarkRepository,
)
from github_engineering_analytics.silver.issue_labels_full_load import (
    run_bronze_to_silver_issue_labels,
)
from github_engineering_analytics.silver.issues_full_load import run_bronze_to_silver
from github_engineering_analytics.silver.labels_full_load import (
    run_bronze_to_silver_labels,
)
from github_engineering_analytics.silver.users_full_load import (
    run_bronze_to_silver_users,
)


def _current_utc_time() -> datetime:
    """Return the current timezone-aware UTC timestamp for a pipeline run."""
    return datetime.now(UTC)


def run_tracked_full_load(
    *,
    spark: SparkSession,
    catalog: str,
    owner: str,
    repository: str,
    github_token: str | None = None,
    run_id_factory: Callable[[], UUID] = uuid4,
    clock: Callable[[], datetime] = _current_utc_time,
) -> BronzeIngestionResult:
    """Run and record one full-or-incremental Bronze-to-Silver lifecycle.

    A missing committed watermark selects a full extraction; an existing one
    selects the overlap-aware incremental route. The run succeeds only after
    Bronze, Issues Silver, Users Silver, Labels Silver, and Issue-Label Silver
    complete. This boundary records the candidate watermark but deliberately
    does not commit it while required Gold processing is still absent. A stage
    failure is recorded as FAILED before the original exception is re-raised.
    """
    config = PipelineConfig(catalog=catalog)
    # Control tables must exist before creating the RUNNING record; otherwise a
    # later Bronze or Silver failure would have no durable lifecycle audit row.
    pipeline_runs = DeltaPipelineRunRepository(spark=spark, config=config)
    pipeline_runs.ensure_table()
    watermarks = DeltaWatermarkRepository(spark=spark, config=config)
    watermarks.ensure_table()
    watermark_before = watermarks.get("github", "issues")

    # This ID is shared by Bronze and every Silver stage. It is the boundary
    # that prevents one pipeline attempt from consuming another attempt's rows.
    started_run = PipelineRun(
        run_id=run_id_factory().hex,
        source_name="github",
        entity_name="issues",
        started_at=clock(),
        watermark_before=watermark_before,
    )
    pipeline_runs.record_started(started_run)

    try:
        # The durable watermark is the sole mode switch: no stored position
        # means bootstrap; otherwise replay its deliberate overlap window.
        if watermark_before is None:
            result = run_full_load(
                spark=spark,
                catalog=catalog,
                owner=owner,
                repository=repository,
                github_token=github_token,
                run_id=started_run.run_id,
                ingested_at=started_run.started_at,
            )
        else:
            result = run_incremental_load(
                spark=spark,
                catalog=catalog,
                owner=owner,
                repository=repository,
                github_token=github_token,
                run_id=started_run.run_id,
                ingested_at=started_run.started_at,
                watermark=watermark_before,
            )

        # Every Silver stage reads only the Bronze evidence appended by this
        # run. Entity tables are reconciled before the Issue-to-Label bridge.
        run_bronze_to_silver(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
        run_bronze_to_silver_users(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
        run_bronze_to_silver_labels(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
        # The bridge runs after Labels so the dimension-to-bridge ordering is
        # explicit for the future Gold model.
        run_bronze_to_silver_issue_labels(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
    except Exception as error:
        failed_run = started_run.fail(
            error_message=str(error).strip() or type(error).__name__,
            finished_at=clock(),
        )

        try:
            pipeline_runs.record_finished(failed_run)
        except Exception:
            # Lifecycle persistence must not hide the original stage failure
            # that determines the job result and its retry behaviour.
            logger.bind(run_id=started_run.run_id).exception(
                "Failed to persist pipeline run failure."
            )
        raise

    # A candidate watermark is recorded for observability, but Gold remains a
    # required downstream stage before a later lifecycle boundary may commit it.
    pipeline_runs.record_finished(
        started_run.succeed(
            candidate_watermark=result.candidate_watermark,
            finished_at=clock(),
        )
    )

    return result
