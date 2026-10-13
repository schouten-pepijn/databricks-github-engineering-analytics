"""Compare two Silver replay executions with each other and with live Silver.

Read-only: this script never writes. For each of the four Silver entities it
proves that replaying one manifest twice gives the same rows, and that those
rows equal live Silver as long as no live Silver table changed Delta version
during the comparison.

Business columns and lineage are compared separately. ``source_run_id`` says
which run an observation came from. A failed run can still have updated live
Silver before it failed, so live Silver may name a run that a replay of the
succeeded runs never uses. Such a lineage difference is reported and accepted
only when the live run is a failed run.
"""

import argparse
import re

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRunStatus

TEST_CATALOG = "github_engineering_analytics_test"
TARGET_SCHEMA = "github_analytics_v2min"
ENTITIES = ("issues", "users", "labels", "issue_labels")
LINEAGE_COLUMN = "source_run_id"

# The business key of each entity: the columns that identify one row.
BUSINESS_KEYS = {
    "issues": ("repository_owner", "repository_name", "issue_id"),
    "users": ("user_id",),
    "labels": ("repository_owner", "repository_name", "label_id"),
    "issue_labels": ("repository_owner", "repository_name", "issue_id", "label_id"),
}


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this check remains active under python -O."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def candidate_table(entity: str, execution_id: str) -> str:
    """Return the table name first_silver_replay wrote for one entity."""
    if not re.fullmatch(r"[a-z0-9_]{1,40}", execution_id):
        raise SystemExit(
            "execution_id must contain 1-40 lowercase letters, digits or _"
        )
    return f"{TEST_CATALOG}.{TARGET_SCHEMA}.s2_replay_{entity}_{execution_id}"


def live_tables(config: PipelineConfig) -> dict[str, str]:
    """Return the live Silver table of each entity."""
    return {
        "issues": config.silver_issues_table,
        "users": config.silver_users_table,
        "labels": config.silver_labels_table,
        "issue_labels": config.silver_issue_labels_table,
    }


def differences(left: DataFrame, right: DataFrame) -> tuple[int, int]:
    """Count rows only in ``left`` and rows only in ``right`` (duplicates count)."""
    return left.exceptAll(right).count(), right.exceptAll(left).count()


def lineage_differences(
    left: DataFrame, right: DataFrame, keys: tuple[str, ...]
) -> list[tuple[str, str, int]]:
    """List (left run, right run, rows) for rows whose source_run_id differs.

    Only rows present on both sides are compared; a row that exists on one side
    only is a business difference and is counted elsewhere.
    """
    left_lineage = left.select(*keys, F.col(LINEAGE_COLUMN).alias("left_run"))
    right_lineage = right.select(*keys, F.col(LINEAGE_COLUMN).alias("right_run"))
    rows = (
        left_lineage.join(right_lineage, list(keys), "inner")
        .where(F.col("left_run") != F.col("right_run"))
        .groupBy("left_run", "right_run")
        .count()
        .collect()
    )
    return sorted((row["left_run"], row["right_run"], row["count"]) for row in rows)


def failed_run_ids(spark: SparkSession, config: PipelineConfig) -> set[str]:
    """Return the IDs of pipeline runs that ended as failed."""
    rows = (
        spark.table(config.pipeline_runs_table)
        .where(F.col("status") == PipelineRunStatus.FAILED.value)
        .select("run_id")
        .collect()
    )
    return {row["run_id"] for row in rows}


def table_version(spark: SparkSession, table: str) -> int:
    """Return the newest Delta version of a table."""
    return int(DeltaTable.forName(spark, table).history(1).collect()[0]["version"])


def main(first_id: str, second_id: str) -> None:
    """Compare two replay executions and live Silver without writing anything."""
    spark: SparkSession = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog=TEST_CATALOG)
    live = live_tables(config)
    failed_runs = failed_run_ids(spark, config)
    versions_before = {
        entity: table_version(spark, table) for entity, table in live.items()
    }

    manifest_ids: set[str] = set()
    report: dict[str, dict[str, object]] = {}
    failures: list[str] = []
    for entity in ENTITIES:
        live_frame = spark.table(live[entity])
        # The candidates also carry manifest_id and replay_execution_id, which
        # describe the execution and are therefore left out. What remains is
        # the Silver contract: business columns plus the lineage column.
        columns = live_frame.columns
        business = [column for column in columns if column != LINEAGE_COLUMN]
        keys = BUSINESS_KEYS[entity]

        first_raw = spark.table(candidate_table(entity, first_id))
        second_raw = spark.table(candidate_table(entity, second_id))
        for frame in (first_raw, second_raw):
            manifest_ids.update(
                row["manifest_id"]
                for row in frame.select("manifest_id").distinct().collect()
            )
        first = first_raw.select(*columns)
        second = second_raw.select(*columns)

        # 1. Business columns must match everywhere, without exception.
        business_results = {
            "first_vs_second": differences(
                first.select(*business), second.select(*business)
            ),
            "first_vs_live": differences(
                first.select(*business), live_frame.select(*business)
            ),
            "second_vs_live": differences(
                second.select(*business), live_frame.select(*business)
            ),
        }
        failures += [
            f"{entity} business columns {name} differ: {only_left}/{only_right}"
            for name, (only_left, only_right) in business_results.items()
            if only_left or only_right
        ]

        # 2. Two replays of one manifest must have identical lineage.
        replay_lineage = lineage_differences(first, second, keys)
        if replay_lineage:
            failures.append(f"{entity} replays disagree on lineage: {replay_lineage}")

        # 3. Lineage against live Silver may differ, but only where live Silver
        #    names a failed run, which a replay of succeeded runs never uses.
        live_lineage = lineage_differences(first, live_frame, keys)
        unexplained = [diff for diff in live_lineage if diff[1] not in failed_runs]
        if unexplained:
            failures.append(
                f"{entity} lineage differs from live Silver without a failed run: "
                f"{unexplained}"
            )

        report[entity] = {
            "rows (first/second/live)": (
                first.count(),
                second.count(),
                live_frame.count(),
            ),
            "business differences": business_results,
            "lineage vs live (replay run, live run, rows)": live_lineage,
            "explained by failed runs": not unexplained,
        }

    # Live Silver is mutable. If a pipeline run landed during the comparison,
    # the live result is not a valid reference.
    versions_after = {
        entity: table_version(spark, table) for entity, table in live.items()
    }
    check(
        versions_before == versions_after,
        f"live Silver changed: {versions_before} -> {versions_after}",
    )
    check(len(manifest_ids) == 1, f"executions use different manifests: {manifest_ids}")

    print(
        {
            "manifest_id": manifest_ids.pop(),
            "failed_runs": sorted(failed_runs),
            "live_silver_versions": versions_before,
            "per_entity": report,
        }
    )
    check(not failures, "; ".join(failures))
    print("OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--first", required=True, help="first replay execution_id")
    parser.add_argument("--second", required=True, help="second replay execution_id")
    arguments = parser.parse_args()
    main(arguments.first, arguments.second)
