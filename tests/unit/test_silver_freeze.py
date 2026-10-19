import re
from unittest.mock import Mock

import pytest
from pyspark.errors import PySparkException

from github_engineering_analytics.orchestration.silver_freeze import (
    SILVER_TABLES,
    FrozenInputUnavailableError,
    freeze_silver,
    frozen_schema_name,
)

CATALOG = "demo_test"
VERSIONS = {name: index for index, name in enumerate(SILVER_TABLES, start=1)}
ROWS = {
    "github_issues": 5,
    "github_users": 3,
    "github_labels": 2,
    "github_issue_labels": 4,
}


def make_spark(
    existing: set[str] | None = None,
    stored: dict[str, int] | None = None,
) -> Mock:
    """A Spark mock where only the listed tables exist and the counts are known."""
    tables = (
        set(existing)
        if existing is not None
        else {f"{CATALOG}.src.{name}" for name in SILVER_TABLES}
    )
    counts = stored if stored is not None else dict(ROWS)
    spark = Mock()
    spark.catalog.tableExists.side_effect = lambda name: name in tables

    def sql(statement: str) -> Mock:
        result = Mock()
        if statement.startswith("SELECT count(*)"):
            name = next(n for n in SILVER_TABLES if f"`{n}`" in statement)
            result.collect.return_value = [{"c": ROWS[name]}]
        elif statement.startswith("CREATE TABLE"):
            # A created table exists from now on, like in Delta. re.match returns
            # None when the pattern does not fit, so check it before using it.
            match = re.match(r"CREATE TABLE `(.+?)`.`(.+?)`.`(.+?)`", statement)
            assert match is not None, f"unexpected CREATE statement: {statement}"
            catalog, schema, table = match.groups()
            tables.add(f"{catalog}.{schema}.{table}")
        return result

    spark.sql.side_effect = sql
    spark.table.side_effect = lambda name: Mock(
        count=Mock(return_value=counts[name.rsplit(".", 1)[1]])
    )
    return spark


def freeze(
    spark: Mock,
    *,
    catalog: str = CATALOG,
    label: str = "x",
    source_schema: str = "src",
    versions: dict[str, object] | None = None,
) -> dict[str, dict[str, int | str]]:
    """Call freeze_silver with the default test arguments, overridden per test."""
    # The versions may be deliberately wrong in a test, so they are typed loosely.
    pinned: dict[str, int] = dict(VERSIONS) if versions is None else versions  # pyright: ignore[reportAssignmentType]
    return freeze_silver(
        spark,
        catalog=catalog,
        label=label,
        source_schema=source_schema,
        versions=pinned,
    )


@pytest.mark.parametrize(
    "label", ["", "Upper", "has space", "dash-id", "x" * 31, "a;b"]
)
def test_frozen_schema_name_rejects_unsafe_labels(label: str) -> None:
    with pytest.raises(ValueError, match="label must contain"):
        frozen_schema_name(label)


def test_frozen_schema_name_always_uses_the_frozen_prefix() -> None:
    assert frozen_schema_name("base_v3") == "github_analytics_s5_frozen_base_v3"


@pytest.mark.parametrize("catalog", ["demo_prod", "demo_dev", "demo"])
def test_a_catalog_that_is_not_test_is_refused_before_spark_is_touched(
    catalog: str,
) -> None:
    spark = Mock()

    with pytest.raises(ValueError, match="only run in a"):
        freeze(spark, catalog=catalog)

    assert spark.mock_calls == []


def test_versions_must_name_exactly_the_four_tables() -> None:
    spark = Mock()

    with pytest.raises(ValueError, match="exactly these tables"):
        freeze(spark, versions={"github_issues": 1})

    assert spark.mock_calls == []


@pytest.mark.parametrize("bad", [-1, True, 1.5, "2"])
def test_each_version_must_be_a_non_negative_integer(bad: object) -> None:
    spark = Mock()
    versions = {**VERSIONS, "github_users": bad}

    with pytest.raises(ValueError, match="github_users"):
        freeze(spark, versions=versions)

    assert spark.mock_calls == []


def test_a_source_schema_with_a_dot_is_refused() -> None:
    with pytest.raises(ValueError, match="without dots"):
        freeze(Mock(), source_schema="a.b")


def test_missing_source_tables_stop_the_freeze_before_any_write() -> None:
    spark = make_spark(existing=set())

    with pytest.raises(ValueError, match="source tables not found"):
        freeze(spark)

    assert not any(c.args[0].startswith("CREATE") for c in spark.sql.call_args_list)


def test_an_existing_target_stops_the_freeze_before_any_write() -> None:
    existing = {f"{CATALOG}.src.{n}" for n in SILVER_TABLES}
    existing.add(f"{CATALOG}.github_analytics_s5_frozen_x.github_users")
    spark = make_spark(existing=existing)

    with pytest.raises(ValueError, match="already exist"):
        freeze(spark)

    assert not any(c.args[0].startswith("CREATE") for c in spark.sql.call_args_list)


def test_an_unreadable_version_names_the_table_and_writes_nothing() -> None:
    spark = make_spark()
    real_sql = spark.sql.side_effect

    def sql(statement: str) -> Mock:
        if statement.startswith("SELECT count(*)") and "`github_labels`" in statement:
            raise PySparkException("DELTA_VERSION_NOT_FOUND")
        return real_sql(statement)

    spark.sql.side_effect = sql

    with pytest.raises(FrozenInputUnavailableError, match="github_labels") as info:
        freeze(spark)

    assert "version 3" in str(info.value)
    assert isinstance(info.value.__cause__, PySparkException)
    assert not any(c.args[0].startswith("CREATE") for c in spark.sql.call_args_list)


def test_a_valid_freeze_copies_each_table_at_its_own_version() -> None:
    spark = make_spark()

    written = freeze(spark)

    statements = [c.args[0] for c in spark.sql.call_args_list]
    creates = [s for s in statements if s.startswith("CREATE TABLE")]
    assert len(creates) == 4
    for name, version in VERSIONS.items():
        assert any(
            f"`github_analytics_s5_frozen_x`.`{name}` AS SELECT * FROM "
            f"`demo_test`.`src`.`{name}` VERSION AS OF {version}" in s
            for s in creates
        )
    assert {n: info["rows"] for n, info in written.items()} == ROWS
    assert {n: info["version"] for n, info in written.items()} == VERSIONS


def test_a_row_count_mismatch_names_the_tables_to_remove() -> None:
    spark = make_spark(stored={**ROWS, "github_users": 99})

    with pytest.raises(RuntimeError, match="after writing 1 of 4") as info:
        freeze(spark)

    assert "github_analytics_s5_frozen_x.github_issues" in str(info.value)
    assert isinstance(info.value.__cause__, RuntimeError)
