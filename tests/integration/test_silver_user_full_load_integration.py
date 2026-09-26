"""Live Delta integration coverage for Bronze-to-Silver user orchestration."""

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
from github_engineering_analytics.silver.users_full_load import (
    run_bronze_to_silver_users,
)

pytestmark = pytest.mark.integration


def test_run_bronze_to_silver_users_persists_one_scoped_user(
    integration_spark: SparkSession,
) -> None:
    """Append one Bronze issue and merge its nested user into Silver."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    bronze_writer = DeltaBronzeIssueWriter(integration_spark, config)
    run_id = f"pytest-{uuid4().hex}"
    user_id = 9_000_000_000_000 + (uuid4().int % 1_000_000_000)
    issue_id = 1_000_000 + (uuid4().int % 1_000_000_000)

    bronze_writer.ensure_table()

    try:
        bronze_writer.append(
            [
                BronzeIssueRecord.from_github_payload(
                    repository_owner="pytest",
                    repository_name=f"silver-users-{run_id}",
                    payload={
                        "id": issue_id,
                        "updated_at": "2026-09-20T12:03:00Z",
                        "user": {
                            "id": user_id,
                            "login": "integration-user",
                            "type": "User",
                        },
                    },
                    run_id=run_id,
                    ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
                    request_watermark=None,
                    page_or_batch_reference="batch-1",
                )
            ]
        )

        run_bronze_to_silver_users(
            spark=integration_spark,
            catalog=catalog,
            bronze_run_id=run_id,
        )

        rows = (
            integration_spark.table(config.silver_users_table)
            .where(F.col("user_id") == user_id)
            .select(
                "user_id",
                "login",
                "user_type",
                "source_issue_id",
                "source_run_id",
                F.date_format(
                    "observed_at",
                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                ).alias("observed_at_utc"),
            )
            .limit(2)
            .collect()
        )

        assert len(rows) == 1
        assert rows[0]["user_id"] == user_id
        assert rows[0]["login"] == "integration-user"
        assert rows[0]["user_type"] == "User"
        assert rows[0]["source_issue_id"] == issue_id
        assert rows[0]["source_run_id"] == run_id
        assert rows[0]["observed_at_utc"] == "2026-09-20T12:03:00Z"
    finally:
        DeltaTable.forName(integration_spark, config.bronze_issues_table).delete(
            condition=f"_run_id = '{run_id}'"
        )
        DeltaTable.forName(integration_spark, config.silver_users_table).delete(
            condition=f"user_id = {user_id}"
        )
