"""Live Delta integration coverage for pipeline-run lifecycle persistence."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.watermark import Watermark

pytestmark = pytest.mark.integration


def test_pipeline_run_repository_persists_successful_lifecycle(
    integration_spark: SparkSession,
) -> None:
    """Persist and verify one RUNNING-to-SUCCEEDED lifecycle in Delta."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    repository = DeltaPipelineRunRepository(integration_spark, config)
    run_id = f"pytest-{uuid4().hex}"

    repository.ensure_table()

    started_run = PipelineRun(
        run_id=run_id,
        source_name="pytest",
        entity_name="pipeline_runs",
        started_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
        watermark_before=Watermark(
            value=datetime(2026, 9, 20, 11, 55, tzinfo=UTC),
        ),
    )
    succeeded_run = started_run.succeed(
        candidate_watermark=Watermark(
            value=datetime(2026, 9, 20, 12, 3, tzinfo=UTC),
            overlap_seconds=600,
        ),
        finished_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
    )

    try:
        repository.record_started(started_run)
        repository.record_finished(succeeded_run)

        rows = (
            integration_spark.table(config.pipeline_runs_table)
            .where(F.col("run_id") == run_id)
            # Format in Spark's UTC session to avoid Python datetime conversion
            # differences between Databricks Connect environments.
            .select(
                "status",
                F.date_format("finished_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                    "finished_at_utc"
                ),
                F.date_format(
                    "candidate_watermark_value",
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ).alias("candidate_watermark_value_utc"),
                "candidate_watermark_overlap_seconds",
                "error_message",
            )
            .limit(2)
            .collect()
        )

        assert len(rows) == 1
        assert rows[0]["status"] == "succeeded"
        assert rows[0]["finished_at_utc"] == "2026-09-20T12:05:00Z"
        assert rows[0]["candidate_watermark_value_utc"] == "2026-09-20T12:03:00Z"
        assert rows[0]["candidate_watermark_overlap_seconds"] == 600
        assert rows[0]["error_message"] is None
    finally:
        # The random run ID scopes cleanup to this test's only row.
        DeltaTable.forName(integration_spark, config.pipeline_runs_table).delete(
            condition=f"run_id = '{run_id}'"
        )
