from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
    require_utc_spark_session,
)


@pytest.mark.parametrize("timezone", ["UTC", "Etc/UTC"])
def test_require_utc_spark_session_accepts_supported_timezones(timezone: str) -> None:
    """Allow the UTC identifiers accepted by Spark and Databricks Connect."""
    spark = Mock()
    spark.conf.get.return_value = timezone

    require_utc_spark_session(spark, operation="writing test records")

    spark.conf.get.assert_called_once_with("spark.sql.session.timeZone")


def test_require_utc_spark_session_preserves_caller_operation_context() -> None:
    """Report the domain operation supplied by the Delta caller."""
    spark = Mock()
    spark.conf.get.return_value = "Europe/Amsterdam"

    with pytest.raises(
        RuntimeError,
        match="UTC before writing Silver issues",
    ):
        require_utc_spark_session(spark, operation="writing Silver issues")


def test_require_utc_spark_session_rejects_an_empty_operation() -> None:
    """Avoid a context-free infrastructure error at the call site."""
    spark = Mock()

    with pytest.raises(ValueError, match="operation must not be empty"):
        require_utc_spark_session(spark, operation=" ")

    spark.conf.get.assert_not_called()


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("catalog.schema.table", "`catalog`.`schema`.`table`"),
        ("catalog.part`name", "`catalog`.`part``name`"),
    ],
)
def test_quote_multipart_identifier_quotes_each_part(
    identifier: str,
    expected: str,
) -> None:
    """Produce a Spark SQL-safe Unity Catalog identifier."""
    assert quote_multipart_identifier(identifier) == expected


@pytest.mark.parametrize("identifier", ["", ".schema", "catalog..table", "table."])
def test_quote_multipart_identifier_rejects_empty_parts(identifier: str) -> None:
    """Reject an ambiguous multipart identifier before composing SQL."""
    with pytest.raises(ValueError, match="Invalid multipart identifier"):
        quote_multipart_identifier(identifier)
