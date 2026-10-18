"""Find the Issues that changed between two pinned Delta versions.

Delta's Change Data Feed (CDF) lists, per commit, which rows were inserted,
updated or deleted. Two tables describe an Issue: the Issue row itself and its
label relations. A change in either one means the Issue is affected, so the
workset is the union of the keys found in both feeds.

Each table has its own version numbers, so each is read with its own range.
CDF is not a permanent archive: a range that has expired, or that starts before
CDF was switched on, cannot be read. That is reported as one explicit error.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

# The business key of an Issue. Both Silver tables carry these three columns.
KEY_COLUMNS = ("repository_owner", "repository_name", "issue_id")


class ChangeRangeUnavailableError(RuntimeError):
    """Raised when a pinned CDF range cannot be read for a table."""


@dataclass(frozen=True)
class ChangeRange:
    """An inclusive range of Delta versions of ONE table."""

    start_version: int
    end_version: int

    def __post_init__(self) -> None:
        """Reject ranges that cannot exist."""
        if self.start_version < 0:
            raise ValueError("start_version must not be negative")
        if self.end_version < self.start_version:
            raise ValueError("end_version must not be before start_version")


def _read_change_keys(
    spark: SparkSession,
    table: str,
    version_range: ChangeRange,
) -> DataFrame:
    """Return the distinct Issue keys that changed in one table's range.

    The option names are the Spark reader options for the change feed:
    "readChangeFeed" switches the feed on for this read, and the two version
    options pick the commits to include. Both ends are inclusive.
    """
    feed = (
        spark.read.option("readChangeFeed", "true")
        .option("startingVersion", version_range.start_version)
        .option("endingVersion", version_range.end_version)
        .table(table)
    )

    try:
        # Spark is lazy: nothing is read until an action such as collect() runs.
        # Reading one row is the cheapest action, and it forces Delta to check
        # the range right now. Without this, a bad range would fail later, in a
        # step that has nothing to do with the range.
        feed.limit(1).collect()
    except PySparkException as exc:
        raise ChangeRangeUnavailableError(
            f"Change feed of {table} is unavailable for versions "
            f"{version_range.start_version} to {version_range.end_version}; "
            "decide whether to bootstrap or replay."
        ) from exc

    # Every change type (insert, update, delete) carries the key. An update is
    # stored as two rows: update_preimage (the old version) and update_postimage
    # (the new version). Both carry the same key, so keep each key once.
    #
    # Caution: a MERGE that matches a row rewrites it, even when no business
    # value changed. That shows up here as an update pair. Filtering those out
    # is a separate step, so this function still returns them for now.
    return feed.select(*KEY_COLUMNS).distinct()


def changed_issue_keys(
    spark: SparkSession,
    *,
    issues_table: str,
    issues_range: ChangeRange,
    issue_labels_table: str,
    issue_labels_range: ChangeRange,
) -> DataFrame:
    """Return the keys of every Issue affected in either feed, once each.

    This is a union, not an intersection: an Issue counts when it changed in
    either table. An Issue whose labels changed but whose own row did not (for
    example its last label was removed) only shows up in the label feed, which
    is why both feeds are read.
    """
    issue_keys = _read_change_keys(spark, issues_table, issues_range)
    label_keys = _read_change_keys(spark, issue_labels_table, issue_labels_range)

    # An issue that changed in both feeds appears once, not twice.
    return issue_keys.union(label_keys).distinct()
