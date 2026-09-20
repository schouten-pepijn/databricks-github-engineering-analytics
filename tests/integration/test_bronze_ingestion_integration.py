"""Live Delta integration coverage for full-load Bronze orchestration."""

import os
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.ingestion import (
    BronzeIngestionResult,
    GitHubIssueBronzeIngestion,
)
from github_engineering_analytics.bronze.issues import DeltaBronzeIssueWriter
from github_engineering_analytics.common.config import PipelineConfig

pytestmark = pytest.mark.integration


def test_full_load_persists_one_mocked_github_issue(
    integration_spark: SparkSession,
) -> None:
    """Compose mocked extraction with real Delta Bronze persistence and cleanup."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    writer = DeltaBronzeIssueWriter(integration_spark, config)
    client = Mock()
    client.iter_issues.return_value = iter(
        [
            {
                "id": 123,
                "number": 42,
                "title": "Full-load integration test",
                "updated_at": "2026-09-20T12:03:00Z",
            }
        ]
    )
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)
    run_id = f"pytest-{uuid4().hex}"

    # Create the table before try/finally so cleanup can always target it.
    writer.ensure_table()

    try:
        result = ingestion.full_load(
            owner="delta-io",
            repository="delta",
            run_id=run_id,
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        )

        assert result == BronzeIngestionResult(
            records_extracted=1,
            batches_written=1,
        )
        client.iter_issues.assert_called_once_with(
            owner="delta-io",
            repository="delta",
            since=None,
        )

        rows = (
            integration_spark.table(config.bronze_issues_table)
            .where(F.col("_run_id") == run_id)
            # Format in Spark's UTC session to avoid Python datetime conversion
            # differences between Databricks Connect environments.
            .select(
                "issue_id",
                "raw_json",
                "_page_or_batch_reference",
                F.date_format(
                    "source_updated_at",
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ).alias("source_updated_at_utc"),
            )
            .limit(2)
            .collect()
        )

        assert len(rows) == 1
        assert rows[0]["issue_id"] == 123
        assert rows[0]["raw_json"] == (
            '{"id":123,"number":42,"title":"Full-load integration test",'
            '"updated_at":"2026-09-20T12:03:00Z"}'
        )
        assert rows[0]["_page_or_batch_reference"] == "batch-1"
        assert rows[0]["source_updated_at_utc"] == "2026-09-20T12:03:00Z"
    finally:
        # The generated run ID isolates cleanup from every other Bronze row.
        DeltaTable.forName(integration_spark, config.bronze_issues_table).delete(
            condition=f"_run_id = '{run_id}'"
        )
