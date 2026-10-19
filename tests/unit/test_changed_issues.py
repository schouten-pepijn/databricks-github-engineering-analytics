"""Unit tests for the changed-Issue workset builder."""

from dataclasses import fields
from unittest.mock import Mock

import pytest
from pyspark.errors import PySparkException

from github_engineering_analytics.silver.changed_issues import (
    ISSUE_LABEL_ROW_KEY,
    ISSUE_LABELS_LINEAGE_COLUMNS,
    ISSUES_LINEAGE_COLUMNS,
    KEY_COLUMNS,
    ChangeRange,
    ChangeRangeUnavailableError,
    business_columns,
    changed_issue_keys,
)
from github_engineering_analytics.silver.issue_labels import SilverIssueLabel
from github_engineering_analytics.silver.issues import DeltaSilverIssueWriter

MODULE = "github_engineering_analytics.silver.changed_issues"

ISSUES = "test_catalog.silver.github_issues"
LABELS = "test_catalog.silver.github_issue_labels"


def make_spark(feeds: dict[str, Mock]) -> Mock:
    """A Spark mock whose change-feed reader returns one mock feed per table.

    These unit tests check the wiring only: which options are set, which
    columns are selected, and how errors are handled. They do not prove that
    Delta returns the right rows. The live check in scripts/ does that.
    """
    reader = Mock()
    # Every .option(...) call returns the same reader, like the real chain.
    reader.option.return_value = reader

    def lookup(name: str) -> Mock:
        """Return the mock feed for one table name."""
        return feeds[name]

    reader.table.side_effect = lookup
    spark = Mock()
    spark.read = reader
    return spark


def make_feed() -> Mock:
    """A mock feed: select(keys) -> distinct() is the key frame we expect back."""
    return Mock()


@pytest.fixture(autouse=True)
def passthrough_filter(mocker) -> Mock:
    """Treat the no-op filter as identity in the wiring tests.

    The filter is Spark logic. It is proven live with build_cdf_projection.py,
    not with mocks, so these unit tests only check how it is called.
    """

    def keep_all_rows(feed: Mock, row_key: tuple[str, ...], lineage_columns) -> Mock:
        return feed

    return mocker.patch(f"{MODULE}._real_changes", side_effect=keep_all_rows)


def test_a_change_range_must_not_be_negative_or_reversed() -> None:
    with pytest.raises(ValueError, match="start_version"):
        _ = ChangeRange(start_version=-1, end_version=3)

    with pytest.raises(ValueError, match="end_version"):
        _ = ChangeRange(start_version=5, end_version=4)


def test_a_single_version_range_is_allowed() -> None:
    assert ChangeRange(start_version=3, end_version=3).end_version == 3


def test_each_table_is_read_with_its_own_pinned_range() -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)

    _ = changed_issue_keys(
        spark,
        issues_table=ISSUES,
        issues_range=ChangeRange(2, 3),
        issue_labels_table=LABELS,
        issue_labels_range=ChangeRange(7, 9),
    )

    options = [c.args for c in spark.read.option.call_args_list]
    # Two reads, each with the change feed switched on and its own bounds.
    assert options.count(("readChangeFeed", "true")) == 2
    assert ("startingVersion", 2) in options and ("endingVersion", 3) in options
    assert ("startingVersion", 7) in options and ("endingVersion", 9) in options


def test_only_the_business_key_columns_are_selected() -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)

    _ = changed_issue_keys(
        spark,
        issues_table=ISSUES,
        issues_range=ChangeRange(2, 3),
        issue_labels_table=LABELS,
        issue_labels_range=ChangeRange(2, 3),
    )

    assert KEY_COLUMNS == ("repository_owner", "repository_name", "issue_id")
    feeds[ISSUES].select.assert_called_once_with(*KEY_COLUMNS)
    feeds[LABELS].select.assert_called_once_with(*KEY_COLUMNS)


def test_the_result_is_the_distinct_union_of_both_feeds() -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)
    issue_keys = feeds[ISSUES].select.return_value.distinct.return_value
    label_keys = feeds[LABELS].select.return_value.distinct.return_value

    result = changed_issue_keys(
        spark,
        issues_table=ISSUES,
        issues_range=ChangeRange(2, 3),
        issue_labels_table=LABELS,
        issue_labels_range=ChangeRange(2, 3),
    )

    # An Issue that changed in both feeds must still appear once, so the union
    # is made distinct.
    issue_keys.union.assert_called_once_with(label_keys)
    assert result is issue_keys.union.return_value.distinct.return_value


@pytest.mark.parametrize("failing", [ISSUES, LABELS])
def test_an_unavailable_range_raises_a_clear_error_naming_the_table(
    failing: str,
) -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)
    # limit(1).collect() is the cheap action that makes Spark check the range.
    feeds[failing].limit.return_value.collect.side_effect = PySparkException(
        "DELTA_MISSING_CHANGE_DATA"
    )

    with pytest.raises(ChangeRangeUnavailableError, match=failing) as info:
        _ = changed_issue_keys(
            spark,
            issues_table=ISSUES,
            issues_range=ChangeRange(2, 3),
            issue_labels_table=LABELS,
            issue_labels_range=ChangeRange(2, 3),
        )

    assert "2" in str(info.value) and "3" in str(info.value)
    assert isinstance(info.value.__cause__, PySparkException)


def test_a_failing_range_stops_the_union_from_being_built() -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)
    feeds[ISSUES].limit.return_value.collect.side_effect = PySparkException("x")

    with pytest.raises(ChangeRangeUnavailableError):
        _ = changed_issue_keys(
            spark,
            issues_table=ISSUES,
            issues_range=ChangeRange(2, 3),
            issue_labels_table=LABELS,
            issue_labels_range=ChangeRange(2, 3),
        )

    feeds[ISSUES].select.return_value.distinct.return_value.union.assert_not_called()


FEED_COLUMNS = [
    "repository_owner",
    "repository_name",
    "issue_id",
    "state",
    "source_run_id",
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
]


def test_business_columns_exclude_cdf_metadata_and_lineage() -> None:
    result = business_columns(FEED_COLUMNS, lineage_columns=("source_run_id",))

    # The three CDF bookkeeping columns and the lineage column are not business
    # values, so a change in them alone is not a real change.
    assert result == ("repository_owner", "repository_name", "issue_id", "state")


def test_without_lineage_columns_every_value_column_counts() -> None:
    result = business_columns(FEED_COLUMNS, lineage_columns=())

    assert "source_run_id" in result
    assert "_change_type" not in result


def test_an_unknown_lineage_column_is_rejected_by_name() -> None:
    with pytest.raises(ValueError, match="no_such_column"):
        _ = business_columns(FEED_COLUMNS, lineage_columns=("no_such_column",))


def test_lineage_constants_name_the_lineage_columns() -> None:
    assert ISSUES_LINEAGE_COLUMNS == ("source_run_id",)
    assert ISSUE_LABELS_LINEAGE_COLUMNS == ("source_run_id", "observed_at")


def test_lineage_constants_exist_in_the_silver_tables() -> None:
    # If a Silver column is renamed, this fails, so the filter cannot silently
    # stop filtering.
    issue_columns = {field.name for field in DeltaSilverIssueWriter._ROW_SCHEMA}
    label_columns = {field.name for field in fields(SilverIssueLabel)}

    assert set(ISSUES_LINEAGE_COLUMNS) <= issue_columns
    assert set(ISSUE_LABELS_LINEAGE_COLUMNS) <= label_columns


def test_changed_issue_keys_uses_the_constants_without_arguments(
    passthrough_filter: Mock,
) -> None:
    feeds = {ISSUES: make_feed(), LABELS: make_feed()}
    spark = make_spark(feeds)

    _ = changed_issue_keys(
        spark,
        issues_table=ISSUES,
        issues_range=ChangeRange(2, 3),
        issue_labels_table=LABELS,
        issue_labels_range=ChangeRange(2, 3),
    )

    calls = {c.args[0]: c.args[1:] for c in passthrough_filter.call_args_list}
    assert calls[feeds[ISSUES]] == (KEY_COLUMNS, ISSUES_LINEAGE_COLUMNS)
    assert calls[feeds[LABELS]] == (ISSUE_LABEL_ROW_KEY, ISSUE_LABELS_LINEAGE_COLUMNS)
