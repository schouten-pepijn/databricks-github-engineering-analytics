from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)


def make_repository(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaPipelineRunRepository]:
    """Build a repository with a mocked Spark session for local unit tests."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    repository = DeltaPipelineRunRepository(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, repository


def test_ensure_table_creates_control_schema_and_run_table() -> None:
    spark, repository = make_repository()

    repository.ensure_table()

    assert spark.sql.call_count == 2

    schema_statement = spark.sql.call_args_list[0].args[0]
    table_statement = spark.sql.call_args_list[1].args[0]

    assert schema_statement == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`github_analytics_control`"
    )
    expected_table_name = (
        "CREATE TABLE IF NOT EXISTS "
        "`test_catalog`.`github_analytics_control`.`pipeline_runs`"
    )
    assert expected_table_name in table_statement
    assert "run_id STRING NOT NULL" in table_statement
    assert "status STRING NOT NULL" in table_statement
    assert "watermark_before_overlap_seconds BIGINT" in table_statement
    assert "candidate_watermark_overlap_seconds BIGINT" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_ensure_table_rejects_non_utc_spark_session() -> None:
    spark, repository = make_repository(session_timezone="Europe/Amsterdam")

    with pytest.raises(
        RuntimeError,
        match="Spark session timezone must be UTC",
    ):
        repository.ensure_table()

    spark.sql.assert_not_called()
