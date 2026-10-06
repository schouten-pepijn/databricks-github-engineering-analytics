from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.errors import PySparkException

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
    read_manifest_input,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest


def make_manifest() -> ReplayManifest:
    return ReplayManifest(
        manifest_id="manifest-001",
        repository_owner="psf",
        repository_name="requests",
        bronze_table="test_catalog.bronze.github_issues_raw",
        bronze_version=42,
        successful_run_ids=("run-001", "run-002"),
        cutoff_at=datetime(2026, 10, 6, 10, 0, tzinfo=UTC),
        code_commit="787ea43",
        transformation_version="v1",
    )


def configure_found_runs(spark: Mock, run_ids: list[str]) -> Mock:
    """Mock the Spark chain and return the filtered 'selected' DataFrame."""
    selected = spark.sql.return_value.where.return_value
    selected.select.return_value.distinct.return_value.collect.return_value = [
        {"_run_id": run_id} for run_id in run_ids
    ]
    return selected


@patch("github_engineering_analytics.bronze.manifest_input.F")
def test_reads_pinned_version_and_returns_selected_rows(functions: Mock) -> None:
    spark = Mock()
    selected = configure_found_runs(spark, ["run-001", "run-002"])

    result = read_manifest_input(spark, make_manifest())

    assert result is selected
    query = spark.sql.call_args.args[0]
    assert "VERSION AS OF 42" in query
    assert "`test_catalog`.`bronze`.`github_issues_raw`" in query


@patch("github_engineering_analytics.bronze.manifest_input.F")
def test_filters_run_ids_with_a_list(functions: Mock) -> None:
    spark = Mock()
    configure_found_runs(spark, ["run-001", "run-002"])

    read_manifest_input(spark, make_manifest())

    # Column.isin() unpacks a list or set only; a tuple would be one value.
    functions.col.return_value.isin.assert_called_once_with(["run-001", "run-002"])


@patch("github_engineering_analytics.bronze.manifest_input.F")
def test_fails_when_a_selected_run_has_no_rows(functions: Mock) -> None:
    spark = Mock()
    configure_found_runs(spark, ["run-001"])

    with pytest.raises(ManifestInputUnavailableError, match="run-002"):
        read_manifest_input(spark, make_manifest())


@patch("github_engineering_analytics.bronze.manifest_input.F")
def test_wraps_spark_errors_for_an_unavailable_version(functions: Mock) -> None:
    spark = Mock()
    selected = configure_found_runs(spark, [])
    selected.select.return_value.distinct.return_value.collect.side_effect = (
        PySparkException("version no longer available")
    )

    with pytest.raises(ManifestInputUnavailableError, match="version 42") as info:
        read_manifest_input(spark, make_manifest())

    assert isinstance(info.value.__cause__, PySparkException)
