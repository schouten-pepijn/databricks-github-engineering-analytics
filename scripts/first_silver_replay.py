"""Replay one manifest into four isolated Silver candidate tables."""

import argparse
import re

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
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

TEST_CATALOG = "github_engineering_analytics_test"
TARGET_SCHEMA = "github_analytics_v2min"
ENTITIES = ("issues", "users", "labels", "issue_labels")


def candidate_tables(execution_id: str) -> dict[str, str]:
    """Return the four candidate table names of one replay execution."""
    if not re.fullmatch(r"[a-z0-9_]{1,40}", execution_id):
        raise SystemExit(
            "execution_id must contain 1-40 lowercase letters, digits or _"
        )
    return {
        entity: f"{TEST_CATALOG}.{TARGET_SCHEMA}.s2_replay_{entity}_{execution_id}"
        for entity in ENTITIES
    }


def existing_tables(spark: SparkSession, targets: dict[str, str]) -> list[str]:
    """Return the target tables that already exist."""
    return [table for table in targets.values() if spark.catalog.tableExists(table)]


def replay_to_tables(
    spark: SparkSession,
    manifest: ReplayManifest,
    execution_id: str,
) -> dict[str, dict[str, int | str]]:
    """Validate everything first, then write one table per Silver entity.

    Delta cannot commit four tables atomically. The order below therefore makes
    every check that can fail run before the first write, and a failure during
    the write names which tables to remove.
    """
    targets = candidate_tables(execution_id)

    # 1. Resolve the pinned input and run every transformer's validation. No
    #    schema or table exists yet, so a failure here leaves nothing behind.
    result = replay_silver(read_manifest_input(spark, manifest))
    silver: dict[str, DataFrame] = {
        "issues": result.issues,
        "users": result.users,
        "labels": result.labels,
        "issue_labels": result.issue_labels,
    }
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
        raise SystemExit(f"target tables already exist: {existing}")

    # 3. Write. mode("error") refuses to overwrite, and Databricks Connect only
    #    knows the mode names append, overwrite, error and ignore.
    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS "
        f"{quote_multipart_identifier(f'{TEST_CATALOG}.{TARGET_SCHEMA}')}"
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
                    f"{target}: {expected_rows[entity]} transformed, "
                    f"{stored_rows} stored"
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


def main(manifest_id: str, execution_id: str) -> None:
    """Replay one stored manifest into the isolated V2-min schema."""
    candidate_tables(execution_id)  # validate the ID before connecting

    spark: SparkSession = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog=TEST_CATALOG)
    manifest = DeltaReplayManifestRepository(spark, config).get(manifest_id)
    if (
        manifest.bronze_table != config.bronze_issues_table
        or manifest.repository_owner != "psf"
        or manifest.repository_name != "requests"
    ):
        raise SystemExit("manifest does not target the configured TEST repository")

    written = replay_to_tables(spark, manifest, execution_id)
    print(
        {
            "manifest_id": manifest.manifest_id,
            "execution_id": execution_id,
            "tables": written,
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--execution-id", required=True)
    arguments = parser.parse_args()
    main(arguments.manifest_id, arguments.execution_id)
