"""Live Delta coverage for Silver Issue-to-Label reconciliation."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.issue_labels import (
    DeltaSilverIssueLabelWriter,
)

pytestmark = pytest.mark.integration

_OBSERVED_ISSUE_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), nullable=False),
        StructField("repository_name", StringType(), nullable=False),
        StructField("issue_id", LongType(), nullable=False),
        StructField("observed_at", TimestampType(), nullable=False),
        StructField("source_run_id", StringType(), nullable=False),
    ]
)

_RELATIONSHIP_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), nullable=False),
        StructField("repository_name", StringType(), nullable=False),
        StructField("issue_id", LongType(), nullable=False),
        StructField("label_id", LongType(), nullable=False),
        StructField("observed_at", TimestampType(), nullable=False),
        StructField("source_run_id", StringType(), nullable=False),
    ]
)


def make_observed_issue(
    spark: SparkSession,
    *,
    repository_name: str,
    observed_at: datetime,
    source_run_id: str,
) -> DataFrame:
    """Build one Issue snapshot that defines the reconciliation scope."""
    return spark.createDataFrame(
        [
            (
                "pytest",
                repository_name,
                1001,
                observed_at,
                source_run_id,
            )
        ],
        schema=_OBSERVED_ISSUE_SCHEMA,
    )


def make_relationships(
    spark: SparkSession,
    *,
    repository_name: str,
    label_ids: tuple[int, ...],
    observed_at: datetime,
    source_run_id: str,
) -> DataFrame:
    """Build current label relationships for the one observed test Issue."""
    return spark.createDataFrame(
        [
            (
                "pytest",
                repository_name,
                1001,
                label_id,
                observed_at,
                source_run_id,
            )
            for label_id in label_ids
        ],
        schema=_RELATIONSHIP_SCHEMA,
    )


def test_reconcile_dataframe_removes_stale_relationships_for_an_unlabeled_issue(
    integration_spark: SparkSession,
) -> None:
    """Remove old relationships when a newer Issue snapshot has no labels."""
    config = PipelineConfig(catalog=os.environ["DATABRICKS_TEST_CATALOG"])
    writer = DeltaSilverIssueLabelWriter(
        spark=integration_spark,
        config=config,
    )
    writer.ensure_table()

    run_id = uuid4().hex
    repository_name = f"pytest-issue-labels-{run_id}"
    first_observed_at = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)
    latest_observed_at = datetime(2026, 9, 27, 11, 0, tzinfo=UTC)

    relationship_table = DeltaTable.forName(
        integration_spark,
        config.silver_issue_labels_table,
    )

    try:
        first_observed_issue = make_observed_issue(
            integration_spark,
            repository_name=repository_name,
            observed_at=first_observed_at,
            source_run_id="run-first",
        )
        first_relationships = make_relationships(
            integration_spark,
            repository_name=repository_name,
            label_ids=(2001,),
            observed_at=first_observed_at,
            source_run_id="run-first",
        )

        writer.reconcile_dataframe(
            observed_issues=first_observed_issue,
            relationships=first_relationships,
        )

        first_rows = (
            integration_spark.table(config.silver_issue_labels_table)
            .where(
                (F.col("repository_owner") == "pytest")
                & (F.col("repository_name") == repository_name)
                & (F.col("issue_id") == 1001)
            )
            .select(
                "issue_id",
                "label_id",
                "source_run_id",
            )
            .collect()
        )

        assert [tuple(row) for row in first_rows] == [
            (1001, 2001, "run-first"),
        ]

        latest_observed_issue = make_observed_issue(
            integration_spark,
            repository_name=repository_name,
            observed_at=latest_observed_at,
            source_run_id="run-latest",
        )
        latest_relationships = make_relationships(
            integration_spark,
            repository_name=repository_name,
            label_ids=(),
            observed_at=latest_observed_at,
            source_run_id="run-latest",
        )

        writer.reconcile_dataframe(
            observed_issues=latest_observed_issue,
            relationships=latest_relationships,
        )

        remaining_rows = (
            integration_spark.table(config.silver_issue_labels_table)
            .where(
                (F.col("repository_owner") == "pytest")
                & (F.col("repository_name") == repository_name)
                & (F.col("issue_id") == 1001)
            )
            .collect()
        )

        assert remaining_rows == []
    finally:
        relationship_table.delete(
            condition=(
                f"repository_owner = 'pytest' AND repository_name = '{repository_name}'"
            )
        )
