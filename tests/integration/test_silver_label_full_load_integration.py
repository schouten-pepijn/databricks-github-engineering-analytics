"""Live Delta integration coverage for Bronze-to-Silver label orchestration."""

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
from github_engineering_analytics.silver.labels_full_load import (
    run_bronze_to_silver_labels,
)

pytestmark = pytest.mark.integration


def test_run_bronze_to_silver_labels_persists_one_scoped_label(
    integration_spark: SparkSession,
) -> None:
    """Append one Bronze issue and merge its nested label into Silver."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    bronze_writer = DeltaBronzeIssueWriter(integration_spark, config)
    run_id = f"pytest-{uuid4().hex}"
    repository_name = f"silver-labels-{run_id}"
    label_id = 9_000_000_000_000 + (uuid4().int % 1_000_000_000)
    issue_id = 1_000_000 + (uuid4().int % 1_000_000_000)

    bronze_writer.ensure_table()

    try:
        bronze_writer.append(
            [
                BronzeIssueRecord.from_github_payload(
                    repository_owner="pytest",
                    repository_name=repository_name,
                    payload={
                        "id": issue_id,
                        "updated_at": "2026-09-20T12:03:00Z",
                        "labels": [
                            {
                                "id": label_id,
                                "name": "integration-label",
                                "color": "A1B2C3",
                                "description": "Created by an integration test.",
                                "default": False,
                            }
                        ],
                    },
                    run_id=run_id,
                    ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
                    request_watermark=None,
                    page_or_batch_reference="batch-1",
                )
            ]
        )

        run_bronze_to_silver_labels(
            spark=integration_spark,
            catalog=catalog,
            bronze_run_id=run_id,
        )

        rows = (
            integration_spark.table(config.silver_labels_table)
            .where(
                (F.col("repository_owner") == "pytest")
                & (F.col("repository_name") == repository_name)
                & (F.col("label_id") == label_id)
            )
            .select(
                "label_id",
                "name",
                "color",
                "description",
                "is_default",
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
        assert rows[0]["label_id"] == label_id
        assert rows[0]["name"] == "integration-label"
        assert rows[0]["color"] == "a1b2c3"
        assert rows[0]["description"] == "Created by an integration test."
        assert rows[0]["is_default"] is False
        assert rows[0]["source_issue_id"] == issue_id
        assert rows[0]["source_run_id"] == run_id
        assert rows[0]["observed_at_utc"] == "2026-09-20T12:03:00Z"
    finally:
        DeltaTable.forName(integration_spark, config.bronze_issues_table).delete(
            condition=f"_run_id = '{run_id}'"
        )
        DeltaTable.forName(integration_spark, config.silver_labels_table).delete(
            condition=(
                "repository_owner = 'pytest' "
                f"AND repository_name = '{repository_name}' "
                f"AND label_id = {label_id}"
            )
        )
