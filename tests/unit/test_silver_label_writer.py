from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.labels import DeltaSilverLabelWriter


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
