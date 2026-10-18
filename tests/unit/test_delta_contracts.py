from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.delta_contracts import (
    ensure_change_data_feed,
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


def make_spark_with_property(value: str) -> Mock:
    """A Spark mock whose SHOW TBLPROPERTIES returns one (key, value) row."""
    spark = Mock()
    spark.sql.return_value.collect.return_value = [{"value": value}]
    return spark


def test_ensure_change_data_feed_does_nothing_when_it_is_already_on() -> None:
    spark = make_spark_with_property("true")

    ensure_change_data_feed(spark, "test_catalog.silver.github_issues")

    # Only the read happened. A repeated ALTER would add an empty Delta commit
    # on every run, so it must be skipped when the property is already set.
    assert spark.sql.call_count == 1
    assert spark.sql.call_args.args[0].startswith("SHOW TBLPROPERTIES")


def test_ensure_change_data_feed_switches_it_on_when_the_property_is_missing() -> None:
    # A missing property does not read back as empty: Delta returns a sentence.
    spark = make_spark_with_property(
        "Table test_catalog.silver.github_issues does not have property: "
        "delta.enableChangeDataFeed"
    )

    ensure_change_data_feed(spark, "test_catalog.silver.github_issues")

    assert spark.sql.call_args.args[0] == (
        "ALTER TABLE `test_catalog`.`silver`.`github_issues` "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def test_ensure_change_data_feed_switches_it_on_when_it_is_explicitly_off() -> None:
    spark = make_spark_with_property("false")

    ensure_change_data_feed(spark, "test_catalog.silver.github_issues")

    assert "SET TBLPROPERTIES" in spark.sql.call_args.args[0]


def test_ensure_change_data_feed_quotes_the_table_name() -> None:
    spark = make_spark_with_property("true")

    ensure_change_data_feed(spark, "test_catalog.silver.github_issues")

    # The name is built into SQL text, so every part must be quoted.
    assert "`test_catalog`.`silver`.`github_issues`" in spark.sql.call_args.args[0]
