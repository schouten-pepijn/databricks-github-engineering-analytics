"""Live Spark integration coverage for Bronze-to-Silver label transformation."""

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

from github_engineering_analytics.silver.labels import (
    BronzeIssueToSilverLabelTransformer,
)

pytestmark = pytest.mark.integration

_BRONZE_LABEL_TEST_SCHEMA = StructType(
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


def test_transform_selects_latest_observation_per_repository_label(
    integration_spark: SparkSession,
) -> None:
    """Keep the newest current-state observation for each repository label."""
    bronze = integration_spark.createDataFrame(
        [
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                (
                    '{"id":1001,"labels":[{"id":2001,"name":"old-name",'
                    '"color":"D73A4A","description":"old","default":false}]}'
                ),
                "run-old",
                datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                "batch-1",
            ),
            (
                "psf",
                "requests",
                1002,
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                (
                    '{"id":1002,"labels":[{"id":2001,"name":"new-name",'
                    '"color":"0E8A16","description":"new","default":true}]}'
                ),
                "run-new",
                datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                "batch-1",
            ),
            (
                "psf",
                "requests",
                1003,
                datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                (
                    '{"id":1003,"labels":[{"id":3001,"name":"documentation",'
                    '"color":"0075CA","description":null,"default":false}]}'
                ),
                "run-other",
                datetime(2026, 9, 20, 12, 1, tzinfo=UTC),
                "batch-1",
            ),
        ],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    transformed = BronzeIssueToSilverLabelTransformer().transform(bronze)
    rows = transformed.orderBy("label_id").collect()

    assert transformed.columns == [
        "repository_owner",
        "repository_name",
        "label_id",
        "name",
        "color",
        "description",
        "is_default",
        "source_issue_id",
        "observed_at",
        "source_run_id",
    ]
    assert len(rows) == 2

    assert rows[0]["label_id"] == 2001
    assert rows[0]["name"] == "new-name"
    assert rows[0]["color"] == "0e8a16"
    assert rows[0]["source_issue_id"] == 1002
    assert rows[0]["source_run_id"] == "run-new"

    assert rows[1]["label_id"] == 3001
    assert rows[1]["name"] == "documentation"
    assert rows[1]["description"] is None
