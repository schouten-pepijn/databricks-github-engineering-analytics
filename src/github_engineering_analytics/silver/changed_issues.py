"""Find the Issues that changed between two pinned Delta versions.

Delta's Change Data Feed (CDF) lists, per commit, which rows were inserted,
updated or deleted. Two tables describe an Issue: the Issue row itself and its
label relations. A change in either one means the Issue is affected, so the
workset is the union of the keys found in both feeds.

Only real changes count. A MERGE rewrites every row it matches, so Delta also
records updates where no business value changed. Those update pairs are dropped
by comparing the row before and after, ignoring the lineage columns.

Each table has its own version numbers, so each is read with its own range.
CDF is not a permanent archive: a range that has expired, or that starts before
CDF was switched on, cannot be read. That is reported as one explicit error.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

# The business key of an Issue. Both Silver tables carry these three columns.
KEY_COLUMNS = ("repository_owner", "repository_name", "issue_id")

# A label row is identified by the Issue key plus its label id. Comparing the
# before and after image of an update must happen per row, not per Issue,
# otherwise a real label change could hide behind an unchanged label.
ISSUE_LABEL_ROW_KEY = (*KEY_COLUMNS, "label_id")

# Columns that Delta adds to every change feed row. They describe the change,
# not the data, so they never count as a business value.
CDF_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")


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


def business_columns(
    columns: Sequence[str],
    lineage_columns: Sequence[str],
) -> tuple[str, ...]:
    """Return the columns whose change counts as a real change.

    Lineage columns (for example source_run_id) say where a row came from. A
    MERGE rewrites them even when the Issue itself did not change, so they must
    not make an update look like a change.
    """
    unknown = [name for name in lineage_columns if name not in columns]
    if unknown:
        # A misspelled name would silently filter nothing, so fail loudly.
        raise ValueError(f"lineage columns not in the change feed: {unknown}")

    return tuple(
        name
        for name in columns
        if name not in CDF_METADATA_COLUMNS and name not in lineage_columns
    )


def _real_changes(
    feed: DataFrame,
    row_key: Sequence[str],
    lineage_columns: Sequence[str],
) -> DataFrame:
    """Keep inserts, deletes, and only those updates that changed a business value.

    An update is stored as two rows, update_preimage (before) and
    update_postimage (after). A MERGE can rewrite a row without changing any
    business value, which still produces such a pair. Those pairs are dropped.
    """
    columns = business_columns(feed.columns, lineage_columns)

    # A new row or a removed row is always a real change.
    always = feed.where(F.col("_change_type").isin("insert", "delete"))

    # Alias the two halves, so that the join can tell their columns apart.
    before = feed.where(F.col("_change_type") == "update_preimage").alias("pre")
    after = feed.where(F.col("_change_type") == "update_postimage").alias("post")

    # Pair the halves: same row (by its key) in the same commit. eqNullSafe is a
    # comparison where two NULLs count as equal; a plain = would drop them.
    paired = [
        F.col(f"post.{name}").eqNullSafe(F.col(f"pre.{name}")) for name in row_key
    ]
    paired.append(F.col("post._commit_version") == F.col("pre._commit_version"))

    # Compare the whole row after the update with the whole row before it. A
    # struct compares field by field, and eqNullSafe treats two NULLs as equal.
    # With no business columns both structs are empty and equal, so no update
    # counts as a change.
    #
    # The key columns are in this list too. After the join they always match,
    # so they never add a difference. They are kept so the struct describes the
    # whole row, and the function does not need to know which columns are keys.
    before_values = F.struct(*[F.col(f"pre.{name}") for name in columns])
    after_values = F.struct(*[F.col(f"post.{name}") for name in columns])
    differs = ~after_values.eqNullSafe(before_values)

    # Keep only the pairs that really changed, then drop the pre-image side.
    # After the join there is one row per update pair. "post.*" selects its
    # after-image columns, so the result has the same shape as the inserts and
    # deletes in "always". union() matches columns by position, which is why
    # both sides must come from the same feed.
    updated = after.join(before, paired, "inner").where(differs)
    return always.union(updated.select("post.*"))


def _read_change_keys(
    spark: SparkSession,
    table: str,
    version_range: ChangeRange,
    row_key: Sequence[str],
    lineage_columns: Sequence[str],
) -> DataFrame:
    """Return the distinct Issue keys that really changed in one table's range.

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
        _ = feed.limit(1).collect()
    except PySparkException as exc:
        raise ChangeRangeUnavailableError(
            f"Change feed of {table} is unavailable for versions "
            + f"{version_range.start_version} to {version_range.end_version}; "
            + "decide whether to bootstrap or replay."
        ) from exc

    # Drop updates that changed no business value, then keep each Issue key once.
    changes = _real_changes(feed, row_key, lineage_columns)
    return changes.select(*KEY_COLUMNS).distinct()


def changed_issue_keys(
    spark: SparkSession,
    *,
    issues_table: str,
    issues_range: ChangeRange,
    issue_labels_table: str,
    issue_labels_range: ChangeRange,
    issues_lineage_columns: Sequence[str] = (),
    issue_labels_lineage_columns: Sequence[str] = (),
) -> DataFrame:
    """Return the keys of every Issue really affected in either feed, once each.

    This is a union, not an intersection: an Issue counts when it changed in
    either table. An Issue whose labels changed but whose own row did not (for
    example its last label was removed) only shows up in the label feed, which
    is why both feeds are read.

    Lineage columns are ignored when deciding whether an update is a change.
    With the default (none), every value column counts.
    """
    issue_keys = _read_change_keys(
        spark, issues_table, issues_range, KEY_COLUMNS, issues_lineage_columns
    )
    label_keys = _read_change_keys(
        spark,
        issue_labels_table,
        issue_labels_range,
        ISSUE_LABEL_ROW_KEY,
        issue_labels_lineage_columns,
    )

    # An Issue that changed in both feeds appears once, not twice.
    return issue_keys.union(label_keys).distinct()
