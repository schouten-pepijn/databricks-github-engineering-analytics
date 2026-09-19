"""Live Delta integration coverage for append-only Bronze issue storage."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.issues import (
    BronzeIssueRecord,
    DeltaBronzeIssueWriter,
)
from github_engineering_analytics.common.config import PipelineConfig

pytestmark = pytest.mark.integration


def test_delta_bronze_issue_writer_appends_one_raw_issue(
    integration_spark: SparkSession,
) -> None:
    """Append and verify one raw issue record, then clean up its run scope."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    writer = DeltaBronzeIssueWriter(integration_spark, config)
    run_id = f"pytest-{uuid4().hex}"

    record = BronzeIssueRecord.from_github_payload(
        repository_owner="delta-io",
        repository_name="delta",
        payload={
            "id": 123,
            "number": 42,
            "title": "Bronze integration test",
            "updated_at": "2026-09-20T12:03:00Z",
        },
        run_id=run_id,
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        request_watermark=datetime(2026, 9, 20, 11, 55, tzinfo=UTC),
        page_or_batch_reference="page-1",
    )

    writer.ensure_table()

    try:
        writer.append([record])

        rows = (
            integration_spark.table(config.bronze_issues_table)
            .where(F.col("_run_id") == run_id)
            # Format in Spark's UTC session to avoid Python datetime conversion
            # differences between Databricks Connect environments.
            .select(
                "issue_id",
                "raw_json",
                "_run_id",
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
        assert rows[0]["raw_json"] == record.raw_json
        assert rows[0]["_run_id"] == run_id
        assert rows[0]["_page_or_batch_reference"] == "page-1"
        assert rows[0]["source_updated_at_utc"] == "2026-09-20T12:03:00Z"
    finally:
        # The random run ID scopes cleanup to this test's appended row only.
        DeltaTable.forName(integration_spark, config.bronze_issues_table).delete(
            condition=f"_run_id = '{run_id}'"
        )
