"""Live Spark integration coverage for Bronze-to-Silver user transformation."""

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

from github_engineering_analytics.silver.users import (
    BronzeIssueToSilverUserTransformer,
)

pytestmark = pytest.mark.integration


_BRONZE_USER_TEST_SCHEMA = StructType(
    [
        StructField("issue_id", LongType(), nullable=False),
        StructField("source_updated_at", TimestampType(), nullable=False),
        StructField("raw_json", StringType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_page_or_batch_reference", StringType(), nullable=False),
    ]
)


def _bronze_row(
    *,
    issue_id: int,
    source_updated_at: datetime,
    raw_json: str,
    run_id: str,
    ingested_at: datetime,
    batch_reference: str,
) -> tuple[int, datetime, str, str, datetime, str]:
    """Build one explicit Bronze issue row for a user transformer test."""
    return (
        issue_id,
        source_updated_at,
        raw_json,
        run_id,
        ingested_at,
        batch_reference,
    )


def test_transform_selects_the_latest_observation_per_global_user(
    integration_spark: SparkSession,
) -> None:
    """Use real Spark JSON parsing and windowing to select current user state."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1001,"user":{"id":2001,"login":"old-login","type":"User"}}'
                ),
                run_id="run-old",
                ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
            _bronze_row(
                issue_id=1002,
                source_updated_at=datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1002,"user":{"id":2001,"login":"new-login","type":"User"}}'
                ),
                run_id="run-new",
                ingested_at=datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
            _bronze_row(
                issue_id=1003,
                source_updated_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1003,"user":{"id":3001,"login":"other-user","type":"User"}}'
                ),
                run_id="run-other",
                ingested_at=datetime(2026, 9, 20, 12, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
        ],
        schema=_BRONZE_USER_TEST_SCHEMA,
    )

    transformed = BronzeIssueToSilverUserTransformer().transform(bronze)
    rows = transformed.orderBy("user_id").collect()

    assert transformed.columns == [
        "user_id",
        "login",
        "user_type",
        "source_issue_id",
        "observed_at",
        "source_run_id",
    ]
    assert len(rows) == 2

    assert rows[0]["user_id"] == 2001
    assert rows[0]["login"] == "new-login"
    assert rows[0]["source_issue_id"] == 1002
    assert rows[0]["source_run_id"] == "run-new"

    assert rows[1]["user_id"] == 3001
    assert rows[1]["login"] == "other-user"


def test_transform_rejects_an_invalid_nested_user(
    integration_spark: SparkSession,
) -> None:
    """Fail when Bronze JSON cannot form a valid Silver user."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                raw_json=('{"id":1001,"user":{"id":2001,"login":"","type":"User"}}'),
                run_id="run-invalid",
                ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                batch_reference="batch-1",
            )
        ],
        schema=_BRONZE_USER_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver users",
    ):
        BronzeIssueToSilverUserTransformer().transform(bronze)
