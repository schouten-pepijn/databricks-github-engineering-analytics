"""Freeze Silver tables at pinned Delta versions into an isolated schema.

TEST only. A proof that compares a full build with an incremental one needs both
to see exactly the same input. The live Silver moves on with every pipeline run,
so the proof first copies the Silver tables at chosen Delta versions into a
schema of its own. The copies keep the Silver table names, so dbt only has to
be pointed at the other schema (see the silver_schema var in dbt).
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.orchestration.silver_replay import (
    require_test_catalog,
)

# The four Silver tables a Gold build reads, under the names Silver gives them.
SILVER_TABLES = (
    "github_issues",
    "github_users",
    "github_labels",
    "github_issue_labels",
)

# Frozen copies always live under this prefix. A label cannot reach a live
# schema, because the name is built here and never passed in whole.
FROZEN_SCHEMA_PREFIX = "github_analytics_s5_frozen_"

_LABEL = re.compile(r"[a-z0-9_]{1,30}")


class FrozenInputUnavailableError(RuntimeError):
    """Raised when a Silver table cannot be read at the pinned version."""


def frozen_schema_name(label: str) -> str:
    """Return the schema name for one frozen input, or raise for a bad label."""
    # fullmatch (not match) so that "abc; DROP" is rejected, not just a prefix.
    if not _LABEL.fullmatch(label):
        raise ValueError("label must contain 1-30 lowercase letters, digits or _")
    return f"{FROZEN_SCHEMA_PREFIX}{label}"


def _require_pinned_versions(versions: Mapping[str, int]) -> None:
    """Require exactly one non-negative version for each Silver table."""
    if set(versions) != set(SILVER_TABLES):
        raise ValueError(f"versions must name exactly these tables: {SILVER_TABLES}")

    for name, version in versions.items():
        # "type(x) is not int" instead of isinstance(): bool is a subclass of int,
        # so True would otherwise pass as version 1.
        if type(version) is not int or version < 0:
            raise ValueError(f"version of {name} must be a non-negative integer")


def _count_at_version(spark: SparkSession, table: str, version: int) -> int:
    """Count the rows of a table at one Delta version, or say it is unreadable."""
    try:
        rows = spark.sql(
            f"SELECT count(*) AS c FROM {quote_multipart_identifier(table)} "
            f"VERSION AS OF {version}"
        ).collect()
    except PySparkException as exc:
        raise FrozenInputUnavailableError(
            f"{table} cannot be read at version {version}"
        ) from exc

    return int(rows[0]["c"])


def freeze_silver(
    spark: SparkSession,
    *,
    catalog: str,
    label: str,
    source_schema: str,
    versions: Mapping[str, int],
) -> dict[str, dict[str, int | str]]:
    """Copy the four Silver tables, each at its pinned version, into a new schema.

    Delta cannot commit four tables atomically. Every check that can fail
    therefore runs before the first write, and a failure during the writes names
    exactly which tables to remove.
    """
    require_test_catalog(catalog)
    schema = frozen_schema_name(label)
    _require_pinned_versions(versions)
    if "." in source_schema:
        raise ValueError("source_schema must be one schema name, without dots")

    sources = {name: f"{catalog}.{source_schema}.{name}" for name in SILVER_TABLES}
    targets = {name: f"{catalog}.{schema}.{name}" for name in SILVER_TABLES}

    # 1. Checks, with nothing written yet.
    missing = [
        table for table in sources.values() if not spark.catalog.tableExists(table)
    ]
    if missing:
        raise ValueError(f"source tables not found: {missing}")

    existing = [table for table in targets.values() if spark.catalog.tableExists(table)]
    if existing:
        raise ValueError(f"target tables already exist: {existing}")

    expected_rows = {
        name: _count_at_version(spark, sources[name], versions[name])
        for name in SILVER_TABLES
    }

    # 2. Write. CREATE TABLE AS SELECT fails if the table exists, so an existing
    #    copy is never overwritten.
    quoted_schema = quote_multipart_identifier(f"{catalog}.{schema}")
    _ = spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
    written: dict[str, dict[str, int | str]] = {}
    try:
        for name in SILVER_TABLES:
            _ = spark.sql(
                f"CREATE TABLE {quote_multipart_identifier(targets[name])} AS "
                f"SELECT * FROM {quote_multipart_identifier(sources[name])} "
                f"VERSION AS OF {versions[name]}"
            )
            stored_rows = spark.table(targets[name]).count()
            if stored_rows != expected_rows[name]:
                raise RuntimeError(
                    f"{targets[name]}: expected {expected_rows[name]} rows, "
                    f"found {stored_rows}"
                )
            written[name] = {
                "table": targets[name],
                "rows": stored_rows,
                "version": versions[name],
            }
    except Exception as exc:
        leftover = [t for t in targets.values() if spark.catalog.tableExists(t)]
        raise RuntimeError(
            f"freeze failed after writing {len(written)} of {len(SILVER_TABLES)} "
            f"tables; remove these by exact name: {leftover}"
        ) from exc

    return written
