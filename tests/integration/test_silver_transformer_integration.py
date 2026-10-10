"""Live Spark integration coverage for Bronze-to-Silver issue transformation."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.bronze.manifest_input import read_manifest_input
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.silver.issues import (
    BronzeIssueToSilverTransformer,
)

pytestmark = pytest.mark.integration


_BRONZE_TEST_SCHEMA = StructType(
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


def _bronze_row(
    *,
    issue_id: int,
    source_updated_at: datetime,
    raw_json: str,
    run_id: str,
    ingested_at: datetime,
    batch_reference: str,
) -> tuple[str, str, int, datetime, str, str, datetime, str]:
    """Build one explicit Bronze row for a Spark transformer test."""
    return (
        "pytest",
        "silver-transformer",
        issue_id,
        source_updated_at,
        raw_json,
        run_id,
        ingested_at,
        batch_reference,
    )


def test_manifest_input_flows_into_issue_silver_contract(
    integration_spark: SparkSession,
) -> None:
    """Read one frozen Bronze row and normalize it to the Silver contract."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    bronze_table = f"{catalog}.github_analytics_bronze.pytest_replay_{uuid4().hex}"
    bronze_schema = quote_multipart_identifier(f"{catalog}.github_analytics_bronze")
    integration_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")

    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1001,"number":42,"title":"Replay lesson",'
                    '"state":"open","user":{"id":2001},'
                    '"created_at":"2026-09-20T10:00:00Z",'
                    '"updated_at":"2026-09-20T11:00:00Z","closed_at":null}'
                ),
                run_id="fixture-run-1",
                ingested_at=datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                batch_reference="fixture-page-1",
            )
        ],
        schema=_BRONZE_TEST_SCHEMA,
    )

    try:
        bronze.write.format("delta").mode("error").saveAsTable(bronze_table)
        version = (
            DeltaTable.forName(integration_spark, bronze_table)
            .history(1)
            .collect()[0]["version"]
        )
        manifest = ReplayManifest(
            manifest_id="fixture-manifest-1",
            repository_owner="pytest",
            repository_name="silver-transformer",
            bronze_table=bronze_table,
            bronze_version=version,
            successful_run_ids=("fixture-run-1",),
            cutoff_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
            code_commit="fixture",
            transformation_version="v1",
        )

        frozen_input = read_manifest_input(integration_spark, manifest)
        silver_rows = BronzeIssueToSilverTransformer().transform(frozen_input).collect()

        assert len(silver_rows) == 1
        row = silver_rows[0]
        assert (row.issue_id, row.issue_number, row.title, row.state) == (
            1001,
            42,
            "Replay lesson",
            "open",
        )
        assert row.source_run_id == "fixture-run-1"
    finally:
        integration_spark.sql(
            f"DROP TABLE IF EXISTS {quote_multipart_identifier(bronze_table)}"
        )


def test_transform_selects_the_latest_issue_and_classifies_pull_requests(
    integration_spark: SparkSession,
) -> None:
    """Parse real Spark JSON and choose one deterministic latest row per issue."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1001,"number":1,"title":"Old title","state":"open",'
                    '"user":{"id":2001},'
                    '"created_at":"2026-09-20T09:00:00Z",'
                    '"updated_at":"2026-09-20T10:00:00Z","closed_at":null}'
                ),
                run_id="run-old",
                ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1001,"number":1,"title":"New title","state":"closed",'
                    '"user":{"id":2001},'
                    '"created_at":"2026-09-20T09:00:00Z",'
                    '"updated_at":"2026-09-20T11:00:00Z",'
                    '"closed_at":"2026-09-20T11:00:00Z"}'
                ),
                run_id="run-new",
                ingested_at=datetime(2026, 9, 20, 11, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
            _bronze_row(
                issue_id=1002,
                source_updated_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1002,"number":2,"title":"Pull request","state":"open",'
                    '"user":{"id":2002},'
                    '"created_at":"2026-09-20T12:00:00Z",'
                    '"updated_at":"2026-09-20T12:00:00Z",'
                    '"closed_at":null,"pull_request":{"url":"https://example.test/pr"}}'
                ),
                run_id="run-pr",
                ingested_at=datetime(2026, 9, 20, 12, 1, tzinfo=UTC),
                batch_reference="batch-1",
            ),
        ],
        schema=_BRONZE_TEST_SCHEMA,
    )

    transformed = BronzeIssueToSilverTransformer().transform(bronze)
    rows = transformed.orderBy("issue_id").collect()

    assert transformed.columns == [
        "repository_owner",
        "repository_name",
        "issue_id",
        "issue_number",
        "title",
        "state",
        "is_pull_request",
        "author_user_id",
        "created_at",
        "updated_at",
        "closed_at",
        "source_run_id",
    ]
    assert len(rows) == 2
    assert rows[0]["issue_id"] == 1001
    assert rows[0]["title"] == "New title"
    assert rows[0]["state"] == "closed"
    assert rows[0]["is_pull_request"] is False
    assert rows[0]["author_user_id"] == 2001
    assert rows[0]["source_run_id"] == "run-new"
    assert rows[1]["issue_id"] == 1002
    assert rows[1]["is_pull_request"] is True
    assert rows[1]["author_user_id"] == 2002


@pytest.mark.parametrize(
    "raw_json",
    [
        (
            '{"id":1001,"number":1,"title":"Naive timestamp","state":"open",'
            '"user":{"id":2001},'
            '"created_at":"2026-09-20T10:00:00",'
            '"updated_at":"2026-09-20T10:00:00Z","closed_at":null}'
        ),
        (
            '{"id":1001,"number":1,"title":"Invalid closed timestamp",'
            '"state":"open","user":{"id":2001},'
            '"created_at":"2026-09-20T10:00:00Z",'
            '"updated_at":"2026-09-20T10:00:00Z","closed_at":""}'
        ),
    ],
)
def test_transform_rejects_invalid_source_timestamps(
    integration_spark: SparkSession,
    raw_json: str,
) -> None:
    """Reject malformed timestamps instead of silently converting them to null."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                raw_json=raw_json,
                run_id="run-invalid",
                ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                batch_reference="batch-1",
            )
        ],
        schema=_BRONZE_TEST_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="cannot form valid Silver issues",
    ):
        BronzeIssueToSilverTransformer().transform(bronze)


def test_transform_rejects_an_issue_without_a_valid_author(
    integration_spark: SparkSession,
) -> None:
    """Reject rows that cannot join an Issue to the global user dimension."""
    bronze = integration_spark.createDataFrame(
        [
            _bronze_row(
                issue_id=1001,
                source_updated_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                raw_json=(
                    '{"id":1001,"number":1,"title":"Missing author","state":"open",'
                    '"created_at":"2026-09-20T10:00:00Z",'
                    '"updated_at":"2026-09-20T10:00:00Z","closed_at":null}'
                ),
                run_id="run-invalid-author",
                ingested_at=datetime(2026, 9, 20, 10, 1, tzinfo=UTC),
                batch_reference="batch-1",
            )
        ],
        schema=_BRONZE_TEST_SCHEMA,
    )

    with pytest.raises(ValueError, match="cannot form valid Silver issues"):
        BronzeIssueToSilverTransformer().transform(bronze)
