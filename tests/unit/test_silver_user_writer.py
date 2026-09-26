from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.users import (
    DeltaSilverUserWriter,
    SilverUser,
)


def make_writer(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaSilverUserWriter]:
    """Build a user writer with a mocked UTC Spark session."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    writer = DeltaSilverUserWriter(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, writer


def make_user(
    *,
    user_id: int = 2001,
    observed_at: datetime = datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
) -> SilverUser:
    """Build one valid Silver user for writer tests."""
    return SilverUser(
        user_id=user_id,
        login="octocat",
        user_type="User",
        source_issue_id=1001,
        observed_at=observed_at,
        source_run_id="run-123",
    )


def make_dataframe_source() -> Mock:
    """Build a mocked DataFrame satisfying the Silver user writer contract."""
    source = Mock()
    source.columns = [field.name for field in DeltaSilverUserWriter._ROW_SCHEMA]
    (
        source.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = []

    return source


def test_ensure_table_creates_silver_schema_and_user_table() -> None:
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
        "`test_catalog`.`github_analytics_silver`.`github_users`"
    ) in table_statement
    assert "user_id BIGINT NOT NULL" in table_statement
    assert "login STRING NOT NULL" in table_statement
    assert "observed_at TIMESTAMP NOT NULL" in table_statement
    assert "source_run_id STRING NOT NULL" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_upsert_does_not_write_empty_input() -> None:
    spark, writer = make_writer()

    writer.upsert([])

    spark.conf.get.assert_not_called()
    spark.createDataFrame.assert_not_called()


@patch("github_engineering_analytics.silver.users.DeltaTable")
def test_upsert_merges_one_user_on_global_user_id(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()
    user = make_user()

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

    writer.upsert([user])

    spark.createDataFrame.assert_called_once_with(
        [
            (
                2001,
                "octocat",
                "User",
                1001,
                datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
                "run-123",
            )
        ],
        schema=DeltaSilverUserWriter._ROW_SCHEMA,
    )
    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_silver.github_users",
    )
    target_alias.merge.assert_called_once_with(
        source_alias,
        "target.user_id = source.user_id",
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["condition"] == (
        "source.observed_at >= target.observed_at"
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["set"] == {
        "login": "source.login",
        "user_type": "source.user_type",
        "source_issue_id": "source.source_issue_id",
        "observed_at": "source.observed_at",
        "source_run_id": "source.source_run_id",
    }
    insert_builder.execute.assert_called_once()


@patch("github_engineering_analytics.silver.users.DeltaTable")
def test_upsert_rejects_duplicate_user_ids_before_writing(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()

    with pytest.raises(ValueError, match="unique Silver user IDs"):
        writer.upsert(
            [
                make_user(),
                make_user(
                    observed_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                ),
            ]
        )

    spark.createDataFrame.assert_not_called()
    delta_table.forName.assert_not_called()


def test_upsert_rejects_non_utc_spark_session() -> None:
    spark, writer = make_writer(session_timezone="Europe/Amsterdam")

    with pytest.raises(RuntimeError, match="Spark session timezone must be UTC"):
        writer.upsert([make_user()])

    spark.createDataFrame.assert_not_called()


@patch("github_engineering_analytics.silver.users.DeltaTable")
def test_upsert_dataframe_rejects_missing_required_columns_before_writing(
    delta_table: Mock,
) -> None:
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


@patch("github_engineering_analytics.silver.users.DeltaTable")
def test_upsert_dataframe_rejects_duplicate_user_ids_before_merging(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()
    source = make_dataframe_source()
    (
        source.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = [Mock()]

    with pytest.raises(ValueError, match="duplicate user IDs"):
        writer.upsert_dataframe(source)

    source.groupBy.assert_called_once_with("user_id")
    delta_table.forName.assert_not_called()


@patch("github_engineering_analytics.silver.users.DeltaTable")
def test_upsert_dataframe_merges_a_validated_source(
    delta_table: Mock,
) -> None:
    spark, writer = make_writer()
    source = make_dataframe_source()
    source_alias = Mock()
    source.alias.return_value = source_alias

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

    writer.upsert_dataframe(source)

    target_alias.merge.assert_called_once_with(
        source_alias,
        "target.user_id = source.user_id",
    )
    assert merge_builder.whenMatchedUpdate.call_args.kwargs["condition"] == (
        "source.observed_at >= target.observed_at"
    )
    insert_builder.execute.assert_called_once()
