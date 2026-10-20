from unittest.mock import Mock

import pytest

from github_engineering_analytics.orchestration.workset_table import (
    WORKSET_TABLE,
    write_workset,
)
from github_engineering_analytics.silver.changed_issues import KEY_COLUMNS

CATALOG = "demo_test"
SCHEMA = "github_analytics_s5_proof"
TABLE = f"{CATALOG}.{SCHEMA}.{WORKSET_TABLE}"


def make_keys(rows: int = 4) -> Mock:
    """A mock DataFrame of Issue keys: select(keys) -> distinct() is the workset."""
    keys = Mock()
    keys.columns = list(KEY_COLUMNS)
    workset = keys.select.return_value.distinct.return_value
    workset.count.return_value = rows
    return keys


def make_spark(stored_rows: int = 4) -> Mock:
    spark = Mock()
    spark.table.return_value.count.return_value = stored_rows
    return spark


def write(spark: Mock, keys: Mock, **overrides) -> int:
    arguments = {"catalog": CATALOG, "schema": SCHEMA}
    arguments.update(overrides)
    return write_workset(spark, keys, **arguments)


@pytest.mark.parametrize("catalog", ["demo_prod", "demo_dev", "demo"])
def test_a_catalog_that_is_not_test_is_refused_before_anything_is_touched(
    catalog: str,
) -> None:
    spark, keys = Mock(), make_keys()

    with pytest.raises(ValueError, match="only run in a"):
        write(spark, keys, catalog=catalog)

    assert spark.mock_calls == []
    keys.select.assert_not_called()


@pytest.mark.parametrize(
    "schema",
    [
        "github_analytics_silver",
        "github_analytics_gold",
        "github_analytics_s5_",
        "github_analytics_s5_UPPER",
        "github_analytics_s5_a;b",
        "github_analytics_s5_" + "x" * 41,
        "other",
        "",
    ],
)
def test_only_scratch_schemas_with_the_fixed_prefix_are_accepted(
    schema: str,
) -> None:
    spark, keys = Mock(), make_keys()

    with pytest.raises(ValueError, match="github_analytics_s5_"):
        write(spark, keys, schema=schema)

    assert spark.mock_calls == []


def test_missing_key_columns_are_named_and_nothing_is_written() -> None:
    spark, keys = Mock(), make_keys()
    keys.columns = ["repository_owner", "issue_id"]

    with pytest.raises(ValueError, match="repository_name"):
        write(spark, keys)

    assert spark.mock_calls == []


def test_only_the_key_columns_are_kept_and_each_key_once() -> None:
    keys = make_keys()

    write(make_spark(), keys)

    keys.select.assert_called_once_with(*KEY_COLUMNS)
    keys.select.return_value.distinct.assert_called_once_with()


def test_the_table_is_replaced_not_appended() -> None:
    keys = make_keys()
    writer = keys.select.return_value.distinct.return_value.write

    write(make_spark(), keys)

    writer.format.assert_called_once_with("delta")
    writer.format.return_value.mode.assert_called_once_with("overwrite")
    save = writer.format.return_value.mode.return_value.saveAsTable
    save.assert_called_once_with(
        "`demo_test`.`github_analytics_s5_proof`.`changed_issues`"
    )


def test_the_schema_is_created_before_the_table_is_written() -> None:
    spark, keys = make_spark(), make_keys()
    order: list[str] = []
    spark.sql.side_effect = lambda statement: order.append("schema")
    save = keys.select.return_value.distinct.return_value.write.format
    save.return_value.mode.return_value.saveAsTable.side_effect = lambda name: (
        order.append("table")
    )

    write(spark, keys)

    assert order == ["schema", "table"]
    assert spark.sql.call_args.args[0] == (
        "CREATE SCHEMA IF NOT EXISTS `demo_test`.`github_analytics_s5_proof`"
    )


def test_the_row_count_is_returned() -> None:
    assert write(make_spark(stored_rows=7), make_keys(rows=7)) == 7


def test_a_row_count_mismatch_is_an_error_naming_the_table() -> None:
    with pytest.raises(RuntimeError, match="expected 4 rows, found 3") as info:
        write(make_spark(stored_rows=3), make_keys(rows=4))

    assert TABLE in str(info.value)
