"""Live Spark integration coverage for Issue-label Silver transformation."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer,
)

pytestmark = pytest.mark.integration

_BRONZE_ISSUE_LABEL_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), nullable=False),
        StructField("repository_name", StringType(), nullable=False),
        StructField("issue_id", LongType(), nullable=False),
        StructField("source_updated_at", TimestampType(), nullable=False),
        StructField("raw_json", StringType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_page_or_batch_reference", StringType(), nullable=False),
    ]
)


def test_transform_retains_empty_latest_snapshot_and_removes_relationships(
    integration_spark: SparkSession,
) -> None:
    """Keep the latest unlabeled Issue in deletion scope without a relation."""
    bronze = integration_spark.createDataFrame(
        [
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                (
                    '{"id":1001,"labels":[{"id":2001,"name":"bug",'
                    '"color":"d73a4a","description":null,"default":false}]}'
                ),
                "run-old",
                datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                "batch-1",
            ),
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                '{"id":1001,"labels":[]}',
                "run-new",
                datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                "batch-1",
            ),
        ],
        schema=_BRONZE_ISSUE_LABEL_SCHEMA,
    )

    transformed = BronzeIssueToSilverLabelTransformer().transform(bronze)

    assert transformed.observed_issues.columns == [
        "repository_owner",
        "repository_name",
        "issue_id",
        "observed_at",
        "source_run_id",
    ]
    assert transformed.relationships.columns == [
        "repository_owner",
        "repository_name",
        "issue_id",
        "label_id",
        "observed_at",
        "source_run_id",
    ]
    assert transformed.observed_issues.collect() == [
        (
            "psf",
            "requests",
            1001,
            datetime(2026, 9, 20, 11, 0),
            "run-new",
        )
    ]
    assert transformed.relationships.count() == 0


def test_transform_rejects_invalid_labels_before_latest_snapshot_selection(
    integration_spark: SparkSession,
) -> None:
    """Fail rather than silently dropping a malformed older Issue relation."""
    bronze = integration_spark.createDataFrame(
        [
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                (
                    '{"id":1001,"labels":[{"id":2001,"name":"bug",'
                    '"color":"not-a-color","description":null,"default":false}]}'
                ),
                "run-old",
                datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                "batch-1",
            ),
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                '{"id":1001,"labels":[]}',
                "run-new",
                datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                "batch-1",
            ),
        ],
        schema=_BRONZE_ISSUE_LABEL_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver issue-label relationships",
    ):
        BronzeIssueToSilverLabelTransformer().transform(bronze)
