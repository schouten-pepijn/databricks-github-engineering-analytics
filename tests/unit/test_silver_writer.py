from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.issues import (
    DeltaSilverIssueWriter,
    SilverIssue,
)


def make_writer(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaSilverIssueWriter]:
    """Build a Silver writer with a mocked Spark session."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    writer = DeltaSilverIssueWriter(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, writer


def make_issue(
    *,
    issue_id: int = 1001,
    updated_at: datetime = datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
) -> SilverIssue:
    """Build one valid Silver issue for writer tests."""
    return SilverIssue(
        repository_owner="psf",
        repository_name="requests",
        issue_id=issue_id,
        issue_number=42,
        title="Improve retry handling",
        state="open",
        is_pull_request=False,
        created_at=datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
        updated_at=updated_at,
        closed_at=None,
        source_run_id="run-123",
    )


def test_ensure_table_creates_silver_schema_and_issue_table() -> None:
    spark, writer = make_writer()

    writer.ensure_table()

    assert spark.sql.call_count == 2

    schema_statement = spark.sql.call_args_list[0].args[0]
    table_statement = spark.sql.call_args_list[1].args[0]

    assert schema_statement == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`github_analytics_silver`"
    )
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "`test_catalog`.`github_analytics_silver`.`github_issues`"
    ) in table_statement
    assert "issue_id BIGINT NOT NULL" in table_statement
    assert "is_pull_request BOOLEAN NOT NULL" in table_statement
    assert "source_run_id STRING NOT NULL" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_upsert_does_not_write_empty_input() -> None:
    spark, writer = make_writer()

    writer.upsert([])

    spark.conf.get.assert_not_called()
    spark.createDataFrame.assert_not_called()


@patch("github_engineering_analytics.silver.issues.DeltaTable")
def test_upsert_merges_one_issue_using_the_silver_business_key(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()
    issue = make_issue()

    source = Mock()
    source_alias = Mock()
    source.alias.return_value = source_alias
    spark.createDataFrame.return_value = source

    target = Mock()
    target_alias = Mock()
    target.alias.return_value = target_alias
    delta_table.forName.return_value = target

    merge_builder = Mock()
    target_alias.merge.return_value = merge_builder

    update_builder = Mock()
    merge_builder.whenMatchedUpdate.return_value = update_builder

    insert_builder = Mock()
    update_builder.whenNotMatchedInsert.return_value = insert_builder

    writer.upsert([issue])

    spark.createDataFrame.assert_called_once_with(
        [
            (
                "psf",
                "requests",
                1001,
                42,
                "Improve retry handling",
                "open",
                False,
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
                None,
                "run-123",
            )
        ],
        schema=DeltaSilverIssueWriter._ROW_SCHEMA,
    )
    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_silver.github_issues",
    )
    target_alias.merge.assert_called_once_with(
        source_alias,
        (
            "target.repository_owner = source.repository_owner "
            "AND target.repository_name = source.repository_name "
            "AND target.issue_id = source.issue_id"
        ),
    )
    merge_builder.whenMatchedUpdate.assert_called_once()
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["condition"] == (
        "source.updated_at >= target.updated_at"
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["set"] == {
        "issue_number": "source.issue_number",
        "title": "source.title",
        "state": "source.state",
        "is_pull_request": "source.is_pull_request",
        "created_at": "source.created_at",
        "updated_at": "source.updated_at",
        "closed_at": "source.closed_at",
        "source_run_id": "source.source_run_id",
    }
    insert_builder.execute.assert_called_once()


@patch("github_engineering_analytics.silver.issues.DeltaTable")
def test_upsert_rejects_duplicate_business_keys_before_writing(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()

    with pytest.raises(ValueError, match="unique Silver business keys"):
        writer.upsert(
            [
                make_issue(),
                make_issue(updated_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC)),
            ]
        )

    spark.createDataFrame.assert_not_called()
    delta_table.forName.assert_not_called()


def test_upsert_rejects_non_utc_spark_session() -> None:
    spark, writer = make_writer(session_timezone="Europe/Amsterdam")

    with pytest.raises(
        RuntimeError,
        match="Spark session timezone must be UTC",
    ):
        writer.upsert([make_issue()])

    spark.createDataFrame.assert_not_called()
