"""Live Delta integration coverage for the tracked Bronze full-load boundary."""

import os
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID, uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.full_load import run_tracked_full_load
from github_engineering_analytics.bronze.ingestion import BronzeIngestionResult
from github_engineering_analytics.common.config import PipelineConfig

pytestmark = pytest.mark.integration


def test_tracked_full_load_persists_bronze_data_and_successful_run(
    integration_spark: SparkSession,
    mocker,
) -> None:
    """Persist one mocked issue and its RUNNING-to-SUCCEEDED lifecycle."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    client = Mock()
    client.iter_issues.return_value = iter(
        [
            {
                "id": 123,
                "number": 42,
                "title": "Tracked full-load integration test",
                "updated_at": "2026-09-20T12:03:00Z",
            }
        ]
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.GitHubClient",
        return_value=client,
    )

    run_uuid = uuid4()
    run_id = run_uuid.hex
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    try:
        result = run_tracked_full_load(
            spark=integration_spark,
            catalog=catalog,
            owner="pytest",
            repository="tracked-full-load",
            run_id_factory=lambda: UUID(hex=run_id),
            clock=Mock(side_effect=[started_at, finished_at]),
        )

        assert result == BronzeIngestionResult(
            records_extracted=1,
            batches_written=1,
        )
        client.iter_issues.assert_called_once_with(
            owner="pytest",
            repository="tracked-full-load",
            since=None,
        )

        bronze_rows = (
            integration_spark.table(config.bronze_issues_table)
            .where(F.col("_run_id") == run_id)
            .select("issue_id", "repository_owner", "repository_name")
            .limit(2)
            .collect()
        )
        pipeline_run_rows = (
            integration_spark.table(config.pipeline_runs_table)
            .where(F.col("run_id") == run_id)
            .select(
                "source_name",
                "entity_name",
                "status",
                "error_message",
                F.date_format("started_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                    "started_at_utc"
                ),
                F.date_format("finished_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                    "finished_at_utc"
                ),
            )
            .limit(2)
            .collect()
        )

        assert len(bronze_rows) == 1
        assert bronze_rows[0].asDict() == {
            "issue_id": 123,
            "repository_owner": "pytest",
            "repository_name": "tracked-full-load",
        }
        assert len(pipeline_run_rows) == 1
        assert pipeline_run_rows[0].asDict() == {
            "source_name": "github",
            "entity_name": "issues",
            "status": "succeeded",
            "error_message": None,
            "started_at_utc": "2026-09-20T12:00:00Z",
            "finished_at_utc": "2026-09-20T12:05:00Z",
        }
    finally:
        # The generated run ID isolates cleanup from job-produced records.
        DeltaTable.forName(integration_spark, config.bronze_issues_table).delete(
            condition=f"_run_id = '{run_id}'"
        )
        DeltaTable.forName(integration_spark, config.pipeline_runs_table).delete(
            condition=f"run_id = '{run_id}'"
        )


def test_tracked_full_load_persists_failure_without_bronze_rows(
    integration_spark: SparkSession,
    mocker,
) -> None:
    """Persist a failed lifecycle when mocked GitHub extraction raises."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    client = Mock()
    client.iter_issues.side_effect = RuntimeError("GitHub request timed out")
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.GitHubClient",
        return_value=client,
    )

    run_uuid = uuid4()
    run_id = run_uuid.hex
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    try:
        with pytest.raises(RuntimeError, match="GitHub request timed out"):
            run_tracked_full_load(
                spark=integration_spark,
                catalog=catalog,
                owner="pytest",
                repository="tracked-full-load",
                run_id_factory=lambda: UUID(hex=run_id),
                clock=Mock(side_effect=[started_at, finished_at]),
            )

        client.iter_issues.assert_called_once_with(
            owner="pytest",
            repository="tracked-full-load",
            since=None,
        )
        bronze_rows = (
            integration_spark.table(config.bronze_issues_table)
            .where(F.col("_run_id") == run_id)
            .limit(2)
            .collect()
        )
        pipeline_run_rows = (
            integration_spark.table(config.pipeline_runs_table)
            .where(F.col("run_id") == run_id)
            .select(
                "status",
                "error_message",
                F.date_format("finished_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                    "finished_at_utc"
                ),
            )
            .limit(2)
            .collect()
        )

        assert bronze_rows == []
        assert len(pipeline_run_rows) == 1
        assert pipeline_run_rows[0].asDict() == {
            "status": "failed",
            "error_message": "GitHub request timed out",
            "finished_at_utc": "2026-09-20T12:05:00Z",
        }
    finally:
        # The failed run never writes Bronze records; only its control row exists.
        DeltaTable.forName(integration_spark, config.pipeline_runs_table).delete(
            condition=f"run_id = '{run_id}'"
        )
