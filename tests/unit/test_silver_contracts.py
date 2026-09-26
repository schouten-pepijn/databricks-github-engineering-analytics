from unittest.mock import Mock

import pytest

from github_engineering_analytics.silver.contracts import (
    require_required_columns,
    require_unique_dataframe_keys,
)


def configure_duplicate_key_lookup(source: Mock, duplicate_keys: list[Mock]) -> None:
    """Configure the mocked Spark aggregation used by the uniqueness guard."""
    grouped_source = source.groupBy.return_value
    counted_source = grouped_source.count.return_value
    filtered_source = counted_source.where.return_value
    limited_source = filtered_source.limit.return_value
    limited_source.collect.return_value = duplicate_keys


def test_require_required_columns_accepts_a_complete_silver_source() -> None:
    """Allow a DataFrame exposing every column required by its target table."""
    source = Mock()
    source.columns = ["issue_id", "updated_at"]
    required_columns = (column for column in ("issue_id", "updated_at"))

    require_required_columns(
        source,
        required_columns=required_columns,
        source_name="Silver source",
    )


def test_require_required_columns_reports_sorted_missing_columns() -> None:
    """Make an incomplete MERGE source actionable before Spark work begins."""
    source = Mock()
    source.columns = ["issue_id"]

    with pytest.raises(
        ValueError,
        match=(
            r"Silver source is missing required columns: "
            r"\['observed_at', 'updated_at'\]"
        ),
    ):
        require_required_columns(
            source,
            required_columns=("issue_id", "updated_at", "observed_at"),
            source_name="Silver source",
        )


def test_require_unique_dataframe_keys_allows_a_unique_merge_source(mocker) -> None:
    """Check exactly the explicit business key before a Silver MERGE."""
    source = Mock()
    configure_duplicate_key_lookup(source, [])
    functions = mocker.patch("github_engineering_analytics.silver.contracts.F")
    functions.col.return_value.__gt__.return_value = Mock()

    require_unique_dataframe_keys(
        source,
        key_columns=("repository_owner", "repository_name", "issue_id"),
        error_message="duplicate issue keys",
    )

    source.groupBy.assert_called_once_with(
        "repository_owner",
        "repository_name",
        "issue_id",
    )
    functions.col.assert_called_once_with("count")


def test_require_unique_dataframe_keys_rejects_duplicate_merge_keys(mocker) -> None:
    """Reject an ambiguous source before Delta can match it to a target row."""
    source = Mock()
    configure_duplicate_key_lookup(source, [Mock()])
    functions = mocker.patch("github_engineering_analytics.silver.contracts.F")
    functions.col.return_value.__gt__.return_value = Mock()

    with pytest.raises(ValueError, match="duplicate issue keys"):
        require_unique_dataframe_keys(
            source,
            key_columns=("issue_id",),
            error_message="duplicate issue keys",
        )


def test_require_unique_dataframe_keys_rejects_an_empty_business_key() -> None:
    """Fail fast rather than issuing an undefined ungrouped Spark operation."""
    source = Mock()

    with pytest.raises(ValueError, match="key_columns must not be empty"):
        require_unique_dataframe_keys(
            source,
            key_columns=(),
            error_message="duplicate keys",
        )

    source.groupBy.assert_not_called()
