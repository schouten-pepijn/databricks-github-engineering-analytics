"""Live Delta integration coverage for Bronze-to-Silver orchestration."""

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
from github_engineering_analytics.silver.full_load import run_bronze_to_silver

pytestmark = pytest.mark.integration


def test_run_bronze_to_silver_keeps_the_newest_issue_version(
    integration_spark: SparkSession,
) -> None:
    """Merge two Bronze versions into one current-state Silver issue."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    bronze_writer = DeltaBronzeIssueWriter(integration_spark, config)

    repository_owner = "pytest"
    repository_name = f"silver-full-load-{uuid4().hex}"
    issue_id = 1001
    old_run_id = f"pytest-{uuid4().hex}"
    new_run_id = f"pytest-{uuid4().hex}"

    bronze_writer.ensure_table()

    try:
        bronze_writer.append(
            [
                BronzeIssueRecord.from_github_payload(
                    repository_owner=repository_owner,
                    repository_name=repository_name,
                    payload={
                        "id": issue_id,
                        "number": 1,
                        "title": "Old title",
                        "state": "open",
                        "created_at": "2026-09-20T09:00:00Z",
                        "updated_at": "2026-09-20T10:00:00Z",
                        "closed_at": None,
                    },
                    run_id=old_run_id,
                    ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                    request_watermark=None,
                    page_or_batch_reference="batch-1",
                )
            ]
        )

        run_bronze_to_silver(
            spark=integration_spark,
            catalog=catalog,
            bronze_run_id=old_run_id,
        )

        bronze_writer.append(
            [
                BronzeIssueRecord.from_github_payload(
                    repository_owner=repository_owner,
                    repository_name=repository_name,
                    payload={
                        "id": issue_id,
                        "number": 1,
                        "title": "New title",
                        "state": "closed",
                        "created_at": "2026-09-20T09:00:00Z",
                        "updated_at": "2026-09-20T11:00:00Z",
                        "closed_at": "2026-09-20T11:00:00Z",
                    },
                    run_id=new_run_id,
                    ingested_at=datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                    request_watermark=None,
                    page_or_batch_reference="batch-1",
                )
            ]
        )

        run_bronze_to_silver(
            spark=integration_spark,
            catalog=catalog,
            bronze_run_id=new_run_id,
        )

        rows = (
            integration_spark.table(config.silver_issues_table)
            .where(
                (F.col("repository_owner") == repository_owner)
                & (F.col("repository_name") == repository_name)
                & (F.col("issue_id") == issue_id)
            )
            .select(
                "title",
                "state",
                "is_pull_request",
                "source_run_id",
                F.date_format(
                    "updated_at",
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ).alias("updated_at_utc"),
            )
            .limit(2)
            .collect()
        )

        assert len(rows) == 1
        assert rows[0]["title"] == "New title"
        assert rows[0]["state"] == "closed"
        assert rows[0]["is_pull_request"] is False
        assert rows[0]["source_run_id"] == new_run_id
        assert rows[0]["updated_at_utc"] == "2026-09-20T11:00:00Z"
    finally:
        DeltaTable.forName(
            integration_spark,
            config.bronze_issues_table,
        ).delete(condition=(f"_run_id = '{old_run_id}' OR _run_id = '{new_run_id}'"))
        DeltaTable.forName(
            integration_spark,
            config.silver_issues_table,
        ).delete(
            condition=(
                f"repository_owner = '{repository_owner}' "
                f"AND repository_name = '{repository_name}' "
                f"AND issue_id = {issue_id}"
            )
        )
