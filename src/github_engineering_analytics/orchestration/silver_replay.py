"""Replay a stored manifest into isolated Silver candidate tables.

A replay rebuilds the four Silver entity sets from one frozen Bronze input and
writes them to NEW tables in an isolated schema. It never writes live Silver,
the control tables or the extraction watermark, and it never calls GitHub.
"""

from __future__ import annotations

import re

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from github_engineering_analytics.bronze.manifest_input import read_manifest_input
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.control.replay_manifest_repository import (
    DeltaReplayManifestRepository,
)
from github_engineering_analytics.silver.replay import replay_silver

# The one schema where replay output may be written. It is fixed in code, not a
# parameter, so a replay can never be pointed at a live schema.
REPLAY_SCHEMA = "github_analytics_v2min"
ENTITIES = ("issues", "users", "labels", "issue_labels")

_EXECUTION_ID = re.compile(r"[a-z0-9_]{1,40}")


def validate_execution_id(execution_id: str) -> str:
    """Return the ID if it is safe to use in a table name, otherwise raise"""
    # fullmatch (not match) so that "abc; DROP" is rejected, not just a prefix.
    if not _EXECUTION_ID.fullmatch(execution_id):
        raise ValueError(
            "execution_id must contain 1-40 lowercase letters, digits or _"
        )
    return execution_id


def require_test_catalog(catalog: str) -> str:
    """Return the catalog if it is a TEST catalog, otherwise raise."""
    # Replays are a learning tool. This guard keeps them out of DEV or PROD.
    if not catalog.endswith("_test"):
        raise ValueError(f"Replays may only run in a *_test catalog, got {catalog!r}")
    return catalog


def candidate_tables(catalog: str, execution_id: str) -> dict[str, str]:
    """Return the four candidate table names of one replay execution."""
    require_test_catalog(catalog)
    validate_execution_id(execution_id)
    return {
        entity: f"{catalog}.{REPLAY_SCHEMA}.s2_replay_{entity}_{execution_id}"
        for entity in ENTITIES
    }


def existing_tables(spark: SparkSession, targets: dict[str, str]) -> list[str]:
    """Return the target tables that already exist."""
    return [table for table in targets.values() if spark.catalog.tableExists(table)]


def replay_to_tables(
    spark: SparkSession,
    manifest: ReplayManifest,
    catalog: str,
    execution_id: str,
) -> dict[str, dict[str, int | str]]:
    """Validate everything first, then write one table per Silver entity.

    Delta cannot commit four tables atomically. The order below therefore makes
    every check that can fail run before the first write, and a failure during
    the writes names exactly which tables to remove.
    """
    targets = candidate_tables(catalog, execution_id)

    # 1. Resolve the pinned input and run every transformer's validation. Spark is
    #    lazy, so the count() calls are what actually force that work. No schema or
    #    table exists yet, so a failure here leaves nothing behind.
    result = replay_silver(read_manifest_input(spark, manifest))
    silver: dict[str, DataFrame] = {
        "issues": result.issues,
        "users": result.users,
        "labels": result.labels,
        "issue_labels": result.issue_labels,
    }
    # Tag every row with where it came from. These two columns describe the
    # execution, not the Issue, so comparisons leave them out.
    candidates = {
        entity: frame.withColumn("manifest_id", F.lit(manifest.manifest_id)).withColumn(
            "replay_execution_id", F.lit(execution_id)
        )
        for entity, frame in silver.items()
    }
    expected_rows = {entity: frame.count() for entity, frame in candidates.items()}

    # 2. Refuse to start when any target exists, so one execution ID never ends
    #    up with a mix of old and new tables.
    existing = existing_tables(spark, targets)
    if existing:
        raise ValueError(f"target tables already exist: {existing}")

    # 3. Write. mode("error") refuses to overwrite, and Databricks Connect only
    #    knows the mode names append, overwrite, error and ignore.
    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS "
        f"{quote_multipart_identifier(f'{catalog}.{REPLAY_SCHEMA}')}"
    )
    written: dict[str, dict[str, int | str]] = {}
    try:
        for entity, frame in candidates.items():
            target = targets[entity]
            frame.write.format("delta").mode("error").saveAsTable(
                quote_multipart_identifier(target)
            )
            stored_rows = spark.table(target).count()
            if stored_rows != expected_rows[entity]:
                raise RuntimeError(
                    f"{target}: expected {expected_rows[entity]} rows, "
                    f"found {stored_rows}"
                )
            version = DeltaTable.forName(spark, target).history(1).collect()[0]
            written[entity] = {
                "table": target,
                "rows": stored_rows,
                "version": int(version["version"]),
            }
    except Exception as e:
        leftover = existing_tables(spark, targets)
        raise RuntimeError(
            f"replay failed after writing {len(written)} of {len(ENTITIES)} "
            f"tables; remove these by exact name: {leftover}"
        ) from e
    return written


def run_silver_replay(
    *,
    spark: SparkSession,
    catalog: str,
    manifest_id: str,
    execution_id: str,
) -> dict[str, object]:
    """Replay one stored manifest and return what was written.

    The cheap checks on the two caller-supplied values run before Spark is
    touched at all, so a typo costs nothing.
    """
    require_test_catalog(catalog)
    validate_execution_id(execution_id)

    config = PipelineConfig(catalog=catalog)
    manifest = DeltaReplayManifestRepository(spark, config).get(manifest_id)

    # The manifest names a Bronze table. It has to be this catalog's table,
    # otherwise the replay would read one catalog and write into another.
    if manifest.bronze_table != config.bronze_issues_table:
        raise ValueError(
            f"manifest {manifest_id!r} belongs to {manifest.bronze_table}, "
            f"not to {config.bronze_issues_table}"
        )

    log = logger.bind(manifest_id=manifest_id, execution_id=execution_id)
    log.info("Starting Silver replay.")
    written = replay_to_tables(spark, manifest, catalog, execution_id)
    log.info("Finished Silver replay.")

    return {
        "manifest_id": manifest.manifest_id,
        "execution_id": execution_id,
        "tables": written,
    }
