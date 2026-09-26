"""Unit tests for the Bronze-to-Silver GitHub users orchestration boundary."""

from unittest.mock import MagicMock, Mock

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.users_full_load import (
    run_bronze_to_silver_users,
)


def test_run_bronze_to_silver_users_rejects_blank_run_id_before_reading_bronze() -> (
    None
):
    """Reject an ambiguous Bronze scope before opening the source table."""
    spark = Mock()

    with pytest.raises(ValueError, match="bronze_run_id must not be empty"):
        run_bronze_to_silver_users(
            spark=spark,
            catalog="test_catalog",
            bronze_run_id="   ",
        )

    spark.table.assert_not_called()


def test_run_bronze_to_silver_users_processes_all_bronze_history_when_unscoped(
    mocker,
) -> None:
    """Compose the unscoped Bronze reader, transformer, and Silver writer."""
    spark = Mock()
    bronze = Mock()
    transformed = Mock()
    transformer = Mock()
    writer = Mock()
    spark.table.return_value = bronze
    transformer.transform.return_value = transformed

    transformer_constructor = mocker.patch(
        "github_engineering_analytics.silver.users_full_load."
        "BronzeIssueToSilverUserTransformer",
        return_value=transformer,
    )
    writer_constructor = mocker.patch(
        "github_engineering_analytics.silver.users_full_load.DeltaSilverUserWriter",
        return_value=writer,
    )

    run_bronze_to_silver_users(
        spark=spark,
        catalog="test_catalog",
    )

    spark.table.assert_called_once_with(
        "test_catalog.github_analytics_bronze.github_issues_raw"
    )
    bronze.where.assert_not_called()
    transformer_constructor.assert_called_once_with()
    transformer.transform.assert_called_once_with(bronze)
    writer_constructor.assert_called_once_with(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )
    assert writer.method_calls == [
        mocker.call.ensure_table(),
        mocker.call.upsert_dataframe(transformed),
    ]


def test_run_bronze_to_silver_users_filters_on_the_bronze_run_id(
    mocker,
) -> None:
    """Process exactly one append-only Bronze run when a scope is supplied."""
    spark = Mock()
    bronze = Mock()
    scoped_bronze = Mock()
    transformed = Mock()
    transformer = Mock()
    writer = Mock()
    functions = mocker.patch("github_engineering_analytics.silver.users_full_load.F")
    run_id_column = MagicMock()
    filter_condition = Mock()
    run_id_column.__eq__.return_value = filter_condition
    functions.col.return_value = run_id_column
    spark.table.return_value = bronze
    bronze.where.return_value = scoped_bronze
    transformer.transform.return_value = transformed

    mocker.patch(
        "github_engineering_analytics.silver.users_full_load."
        "BronzeIssueToSilverUserTransformer",
        return_value=transformer,
    )
    mocker.patch(
        "github_engineering_analytics.silver.users_full_load.DeltaSilverUserWriter",
        return_value=writer,
    )

    run_bronze_to_silver_users(
        spark=spark,
        catalog="test_catalog",
        bronze_run_id="run-123",
    )

    functions.col.assert_called_once_with("_run_id")
    bronze.where.assert_called_once_with(filter_condition)
    transformer.transform.assert_called_once_with(scoped_bronze)
