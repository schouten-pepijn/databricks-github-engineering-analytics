from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.labels import (
    DeltaSilverLabelWriter,
    SilverLabel,
)


def make_writer(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaSilverLabelWriter]:
    """Build a label writer with a mocked Spark session."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    writer = DeltaSilverLabelWriter(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, writer


def make_label(
    *,
    label_id: int = 2001,
    observed_at: datetime = datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
) -> SilverLabel:
    """Build one valid Silver label for writer tests."""
    return SilverLabel(
        repository_owner="psf",
        repository_name="requests",
        label_id=label_id,
        name="bug",
        color="d73a4a",
        description="Something is not working",
        is_default=True,
        source_issue_id=1001,
        observed_at=observed_at,
        source_run_id="run-123",
    )


def make_dataframe_source() -> Mock:
    """Build a mocked DataFrame satisfying the Silver label writer contract."""
    source = Mock()
    source.columns = [field.name for field in DeltaSilverLabelWriter._ROW_SCHEMA]
    (
        source.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = []

    return source


def test_ensure_table_creates_silver_schema_and_label_table() -> None:
    """Create the repository-scoped current-state Labels table when absent."""
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
        "`test_catalog`.`github_analytics_silver`.`github_labels`"
    ) in table_statement
    assert "label_id BIGINT NOT NULL" in table_statement
    assert "description STRING" in table_statement
    assert "is_default BOOLEAN NOT NULL" in table_statement
    assert "observed_at TIMESTAMP NOT NULL" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_ensure_table_rejects_non_utc_spark_session() -> None:
    """Fail before DDL when Spark would interpret timestamps ambiguously."""
    spark, writer = make_writer(session_timezone="Europe/Amsterdam")

    with pytest.raises(
        RuntimeError,
        match="Spark session timezone must be UTC before writing Silver labels.",
    ):
        writer.ensure_table()

    spark.sql.assert_not_called()


def test_upsert_does_not_write_empty_input() -> None:
    """Treat an empty batch as a no-op without touching Spark."""
    spark, writer = make_writer()

    writer.upsert([])

    spark.conf.get.assert_not_called()
    spark.createDataFrame.assert_not_called()


def test_upsert_rejects_duplicate_business_keys_before_writing(mocker) -> None:
    """Reject ambiguous label observations before creating a Delta source."""
    delta_table = mocker.patch(
        "github_engineering_analytics.silver.labels.DeltaTable",
        create=True,
    )
    spark, writer = make_writer()

    with pytest.raises(ValueError, match="unique Silver label business keys"):
        writer.upsert(
            [
                make_label(),
                make_label(
                    observed_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                ),
            ]
        )

    spark.createDataFrame.assert_not_called()
    delta_table.forName.assert_not_called()


def test_upsert_merges_one_label_using_the_silver_business_key(mocker) -> None:
    """Merge a label only when its observed version is at least as recent."""
    delta_table = mocker.patch(
        "github_engineering_analytics.silver.labels.DeltaTable",
        create=True,
    )
    spark, writer = make_writer()
    label = make_label()

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

    writer.upsert([label])

    spark.createDataFrame.assert_called_once_with(
        [
            (
                "psf",
                "requests",
                2001,
                "bug",
                "d73a4a",
                "Something is not working",
                True,
                1001,
                datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
                "run-123",
            )
        ],
        schema=DeltaSilverLabelWriter._ROW_SCHEMA,
    )
    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_silver.github_labels",
    )
    target_alias.merge.assert_called_once_with(
        source_alias,
        "target.repository_owner = source.repository_owner "
        "AND target.repository_name = source.repository_name "
        "AND target.label_id = source.label_id",
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["condition"] == (
        "source.observed_at >= target.observed_at"
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["set"] == {
        "name": "source.name",
        "color": "source.color",
        "description": "source.description",
        "is_default": "source.is_default",
        "source_issue_id": "source.source_issue_id",
        "observed_at": "source.observed_at",
        "source_run_id": "source.source_run_id",
    }
    assert update_builder.whenNotMatchedInsert.call_args.kwargs["values"] == {
        "repository_owner": "source.repository_owner",
        "repository_name": "source.repository_name",
        "label_id": "source.label_id",
        "name": "source.name",
        "color": "source.color",
        "description": "source.description",
        "is_default": "source.is_default",
        "source_issue_id": "source.source_issue_id",
        "observed_at": "source.observed_at",
        "source_run_id": "source.source_run_id",
    }
    insert_builder.execute.assert_called_once()


def test_upsert_dataframe_rejects_missing_required_columns_before_writing(
    mocker,
) -> None:
    """Reject an incomplete normalized Label DataFrame before Delta work."""
    delta_table = mocker.patch(
        "github_engineering_analytics.silver.labels.DeltaTable",
    )
    spark, writer = make_writer()
    source = make_dataframe_source()
    source.columns.remove("observed_at")

    with pytest.raises(
        ValueError,
        match=r"missing required columns: \['observed_at'\]",
    ):
        writer.upsert_dataframe(source)

    source.groupBy.assert_not_called()
    delta_table.forName.assert_not_called()


def test_upsert_dataframe_rejects_duplicate_business_keys_before_merging(
    mocker,
) -> None:
    """Reject an ambiguous DataFrame source before its Delta MERGE."""
    delta_table = mocker.patch(
        "github_engineering_analytics.silver.labels.DeltaTable",
    )
    spark, writer = make_writer()
    source = make_dataframe_source()
    (
        source.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = [Mock()]

    with pytest.raises(ValueError, match="duplicate Label business keys"):
        writer.upsert_dataframe(source)

    source.groupBy.assert_called_once_with(
        "repository_owner",
        "repository_name",
        "label_id",
    )
    delta_table.forName.assert_not_called()
