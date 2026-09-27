"""Unit tests for the Issue-to-Label Silver Delta writer boundary."""

from unittest.mock import Mock

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.issue_labels import (
    DeltaSilverIssueLabelWriter,
)


def make_writer(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaSilverIssueLabelWriter]:
    """Build an Issue-to-Label writer with a mocked Spark session."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    writer = DeltaSilverIssueLabelWriter(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, writer


def make_dataframe_source(columns: tuple[str, ...]) -> Mock:
    """Build a mocked DataFrame that passes the shared uniqueness check."""
    source = Mock()
    source.columns = list(columns)
    (
        source.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = []
    return source


def test_ensure_table_creates_silver_schema_and_relationship_table() -> None:
    """Create the repository-scoped current Issue-to-Label table when absent."""
    spark, writer = make_writer()

    writer.ensure_table()

    assert spark.sql.call_count == 2
    assert spark.sql.call_args_list[0].args[0] == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`github_analytics_silver`"
    )

    table_statement = spark.sql.call_args_list[1].args[0]
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "`test_catalog`.`github_analytics_silver`.`github_issue_labels`"
    ) in table_statement
    assert "issue_id BIGINT NOT NULL" in table_statement
    assert "label_id BIGINT NOT NULL" in table_statement
    assert "observed_at TIMESTAMP NOT NULL" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_ensure_table_rejects_non_utc_spark_session() -> None:
    """Fail before DDL when timestamps would be interpreted ambiguously."""
    spark, writer = make_writer(session_timezone="Europe/Amsterdam")

    with pytest.raises(
        RuntimeError,
        match=(
            "Spark session timezone must be UTC before writing "
            "Silver Issue-to-Label relationships."
        ),
    ):
        writer.ensure_table()

    spark.sql.assert_not_called()


def test_reconcile_dataframe_rejects_missing_observed_issue_column() -> None:
    """Reject an incomplete reconciliation scope before any Delta work."""
    spark, writer = make_writer()
    observed_issues = make_dataframe_source(writer._OBSERVED_ISSUE_COLUMNS)
    observed_issues.columns.remove("observed_at")
    relationships = make_dataframe_source(writer._RELATIONSHIP_COLUMNS)

    with pytest.raises(
        ValueError,
        match=r"Observed Issue source is missing required columns: \['observed_at'\]",
    ):
        writer.reconcile_dataframe(
            observed_issues=observed_issues,
            relationships=relationships,
        )

    observed_issues.groupBy.assert_not_called()
    relationships.groupBy.assert_not_called()
    spark.table.assert_not_called()


def test_reconcile_dataframe_rejects_duplicate_observed_issue_keys() -> None:
    """Reject ambiguous deletion scope before reading the target table."""
    spark, writer = make_writer()
    observed_issues = make_dataframe_source(writer._OBSERVED_ISSUE_COLUMNS)
    relationships = make_dataframe_source(writer._RELATIONSHIP_COLUMNS)
    (
        observed_issues.groupBy.return_value.count.return_value.where.return_value.limit.return_value.collect.return_value
    ) = [Mock()]

    with pytest.raises(
        ValueError,
        match="Observed Issue source contains duplicate business keys",
    ):
        writer.reconcile_dataframe(
            observed_issues=observed_issues,
            relationships=relationships,
        )

    observed_issues.groupBy.assert_called_once_with(
        "repository_owner",
        "repository_name",
        "issue_id",
    )
    relationships.groupBy.assert_not_called()
    spark.table.assert_not_called()


def test_reconcile_dataframe_combines_upserts_and_scoped_deletes_atomically(
    mocker,
) -> None:
    """Build one merge source so additions and scoped removals commit together."""
    _, writer = make_writer()
    observed_issues = make_dataframe_source(writer._OBSERVED_ISSUE_COLUMNS)
    relationships = make_dataframe_source(writer._RELATIONSHIP_COLUMNS)
    operation = Mock()
    upserts = Mock()
    deletes = Mock()
    merge_source = Mock()
    relationships.select.return_value = upserts
    upserts.unionByName.return_value = merge_source

    functions = mocker.patch("github_engineering_analytics.silver.issue_labels.F")
    functions.lit.return_value.alias.return_value = operation
    relationships_match = mocker.patch.object(
        writer,
        "_require_relationships_match_observed_issues",
    )
    build_delete_source = mocker.patch.object(
        writer,
        "_build_delete_source",
        return_value=deletes,
    )
    merge_reconciliation_source = mocker.patch.object(
        writer,
        "_merge_reconciliation_source",
    )

    writer.reconcile_dataframe(
        observed_issues=observed_issues,
        relationships=relationships,
    )

    relationships_match.assert_called_once_with(
        observed_issues=observed_issues,
        relationships=relationships,
    )
    relationships.select.assert_called_once_with(
        *writer._RELATIONSHIP_COLUMNS,
        operation,
    )
    build_delete_source.assert_called_once_with(
        observed_issues=observed_issues,
        relationships=relationships,
    )
    upserts.unionByName.assert_called_once_with(deletes)
    merge_reconciliation_source.assert_called_once_with(merge_source)


def test_merge_reconciliation_source_uses_the_relationship_business_key(mocker) -> None:
    """Merge upserts and deletes by the Issue-to-Label business key."""
    delta_table = mocker.patch(
        "github_engineering_analytics.silver.issue_labels.DeltaTable",
    )
    spark, writer = make_writer()
    source = Mock()
    source_alias = Mock()
    source.alias.return_value = source_alias
    target = Mock()
    target_alias = Mock()
    target.alias.return_value = target_alias
    delta_table.forName.return_value = target
    merge_builder = Mock()
    target_alias.merge.return_value = merge_builder
    delete_builder = Mock()
    merge_builder.whenMatchedDelete.return_value = delete_builder
    update_builder = Mock()
    delete_builder.whenMatchedUpdate.return_value = update_builder
    insert_builder = Mock()
    update_builder.whenNotMatchedInsert.return_value = insert_builder

    writer._merge_reconciliation_source(source)

    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_silver.github_issue_labels",
    )
    target_alias.merge.assert_called_once_with(
        source_alias,
        "target.repository_owner = source.repository_owner "
        "AND target.repository_name = source.repository_name "
        "AND target.issue_id = source.issue_id "
        "AND target.label_id = source.label_id",
    )
    assert merge_builder.whenMatchedDelete.call_args.kwargs["condition"] == (
        "source._operation = 'delete'"
    )
    assert delete_builder.whenMatchedUpdate.call_args.kwargs["condition"] == (
        "source._operation = 'upsert' AND source.observed_at >= target.observed_at"
    )
    assert update_builder.whenNotMatchedInsert.call_args.kwargs["condition"] == (
        "source._operation = 'upsert'"
    )
    insert_builder.execute.assert_called_once()
