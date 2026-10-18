"""Live Spark coverage for the no-op update filter of the changed-Issue workset.

The unit tests patch the filter out, so they cannot see mistakes inside it. These
tests feed it small hand-made change feeds (no Delta table is needed) and check
which Issues it keeps.
"""

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.silver.changed_issues import (
    ISSUE_LABEL_ROW_KEY,
    KEY_COLUMNS,
    _real_changes,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.integration

_FEED_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), nullable=False),
        StructField("repository_name", StringType(), nullable=False),
        StructField("issue_id", LongType(), nullable=False),
        StructField("label_id", LongType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("source_run_id", StringType(), nullable=False),
        StructField("_change_type", StringType(), nullable=False),
        StructField("_commit_version", LongType(), nullable=False),
        StructField("_commit_timestamp", TimestampType(), nullable=False),
    ]
)

_COMMITTED_AT = datetime(2026, 10, 12, 10, 0, tzinfo=UTC)


def _row(
    issue_id: int,
    change_type: str,
    *,
    state: str | None = "open",
    run: str = "run_1",
    label_id: int | None = None,
    version: int = 3,
) -> tuple[object, ...]:
    """One change-feed row for psf/requests."""
    return (
        "psf",
        "requests",
        issue_id,
        label_id,
        state,
        run,
        change_type,
        version,
        _COMMITTED_AT,
    )


def _kept_issue_ids(
    spark: SparkSession,
    rows: list[tuple[object, ...]],
    row_key: tuple[str, ...],
    lineage_columns: tuple[str, ...],
) -> list[int]:
    """Run the filter on a hand-made feed and return the Issue ids it keeps."""
    feed = spark.createDataFrame(rows, schema=_FEED_SCHEMA)
    kept = _real_changes(feed, row_key, lineage_columns).select(*KEY_COLUMNS)
    return sorted(row["issue_id"] for row in kept.distinct().collect())


def test_inserts_deletes_and_real_updates_are_kept(
    integration_spark: SparkSession,
) -> None:
    rows = [
        _row(1, "insert"),
        _row(2, "delete"),
        # A real change: the state differs between the two images.
        _row(3, "update_preimage", state="open"),
        _row(3, "update_postimage", state="closed", run="run_2"),
    ]

    result = _kept_issue_ids(integration_spark, rows, KEY_COLUMNS, ("source_run_id",))

    assert result == [1, 2, 3]


def test_an_update_that_only_changes_lineage_is_dropped(
    integration_spark: SparkSession,
) -> None:
    rows = [
        # Only source_run_id differs: the row was rewritten, not changed.
        _row(4, "update_preimage", run="run_1"),
        _row(4, "update_postimage", run="run_2"),
    ]

    result = _kept_issue_ids(integration_spark, rows, KEY_COLUMNS, ("source_run_id",))

    assert result == []


def test_two_nulls_count_as_equal(integration_spark: SparkSession) -> None:
    rows = [
        # state is NULL on both sides. A plain = would call them different.
        _row(5, "update_preimage", state=None, run="run_1"),
        _row(5, "update_postimage", state=None, run="run_2"),
    ]

    result = _kept_issue_ids(integration_spark, rows, KEY_COLUMNS, ("source_run_id",))

    assert result == []


def test_without_lineage_columns_a_rewritten_row_still_counts(
    integration_spark: SparkSession,
) -> None:
    rows = [
        _row(4, "update_preimage", run="run_1"),
        _row(4, "update_postimage", run="run_2"),
    ]

    result = _kept_issue_ids(integration_spark, rows, KEY_COLUMNS, ())

    # With the default (no lineage columns) source_run_id is a value column.
    assert result == [4]


def test_label_rows_are_paired_per_row_and_not_per_issue(
    integration_spark: SparkSession,
) -> None:
    rows = [
        # Issue 7 has two labels in the same commit; only label 101 really changed.
        _row(7, "update_preimage", state="bug", label_id=100),
        _row(7, "update_postimage", state="bug", label_id=100, run="run_2"),
        _row(7, "update_preimage", state="docs", label_id=101),
        _row(7, "update_postimage", state="documentation", label_id=101, run="run_2"),
        # Issue 8 has two labels in the same commit and none really changed.
        _row(8, "update_preimage", state="bug", label_id=100),
        _row(8, "update_postimage", state="bug", label_id=100, run="run_2"),
        _row(8, "update_preimage", state="docs", label_id=101),
        _row(8, "update_postimage", state="docs", label_id=101, run="run_2"),
    ]

    result = _kept_issue_ids(
        integration_spark, rows, ISSUE_LABEL_ROW_KEY, ("source_run_id",)
    )

    # If the halves were paired per Issue, label 100 would be matched against
    # label 101 and Issue 8 would wrongly count as changed.
    assert result == [7]
