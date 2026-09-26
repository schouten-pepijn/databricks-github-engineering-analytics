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


def _bronze_label_row(
    *,
    raw_json: str,
    source_issue_id: int = 1001,
) -> tuple[str, str, int, datetime, str, str, datetime, str]:
    """Build one valid Bronze envelope around a supplied Issue JSON payload."""
    return (
        "psf",
        "requests",
        source_issue_id,
        datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
        raw_json,
        "run-invalid",
        datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
        "batch-1",
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


def test_transform_rejects_an_invalid_nested_label(
    integration_spark: SparkSession,
) -> None:
    """Fail when a Bronze label cannot satisfy the Silver contract."""
    bronze = integration_spark.createDataFrame(
        [
            (
                "psf",
                "requests",
                1001,
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                (
                    '{"id":1001,"labels":[{"id":2001,"name":"bug",'
                    '"color":"not-a-color","description":null,"default":false}]}'
                ),
                "run-invalid",
                datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                "batch-1",
            )
        ],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver labels",
    ):
        BronzeIssueToSilverLabelTransformer().transform(bronze)


@pytest.mark.parametrize(
    "raw_json",
    [
        "not-json",
        '{"id":1001}',
        '{"id":1001,"labels":null}',
    ],
)
def test_transform_rejects_malformed_or_missing_label_arrays(
    integration_spark: SparkSession,
    raw_json: str,
) -> None:
    """Reject input that explode would otherwise silently turn into no rows."""
    bronze = integration_spark.createDataFrame(
        [_bronze_label_row(raw_json=raw_json)],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver labels",
    ):
        BronzeIssueToSilverLabelTransformer().transform(bronze)


def test_transform_rejects_a_mismatched_parent_issue_id(
    integration_spark: SparkSession,
) -> None:
    """Reject a payload whose Issue ID does not match the Bronze metadata."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_label_row(
                raw_json=(
                    '{"id":1002,"labels":[{"id":2001,"name":"bug",'
                    '"color":"d73a4a","description":null,"default":false}]}'
                )
            )
        ],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver labels",
    ):
        BronzeIssueToSilverLabelTransformer().transform(bronze)


def test_transform_rejects_duplicate_label_ids_in_one_issue(
    integration_spark: SparkSession,
) -> None:
    """Reject ambiguous duplicate Label objects before latest-row reduction."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_label_row(
                raw_json=(
                    '{"id":1001,"labels":['
                    '{"id":2001,"name":"bug","color":"d73a4a",'
                    '"description":null,"default":false},'
                    '{"id":2001,"name":"different","color":"0e8a16",'
                    '"description":null,"default":true}]}'
                )
            )
        ],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver labels",
    ):
        BronzeIssueToSilverLabelTransformer().transform(bronze)


def test_transform_returns_no_rows_for_an_empty_label_array(
    integration_spark: SparkSession,
) -> None:
    """Keep an Issue without labels as a valid zero-row Silver contribution."""
    bronze = integration_spark.createDataFrame(
        [_bronze_label_row(raw_json='{"id":1001,"labels":[]}')],
        schema=_BRONZE_LABEL_TEST_SCHEMA,
    )

    assert BronzeIssueToSilverLabelTransformer().transform(bronze).count() == 0
