"""Compare two Issues replay candidates with each other and with live Silver.

Read-only: this script never writes. It proves that replaying one manifest
twice gives the same Issues, and that those Issues equal live Silver as long as
live Silver did not change during the comparison.
"""

import argparse
import re

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from github_engineering_analytics.common.config import PipelineConfig

TEST_CATALOG = "github_engineering_analytics_test"
TARGET_SCHEMA = "github_analytics_v2min"


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this check remains active under python -O."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def candidate_table(execution_id: str) -> str:
    """Return the table name written by first_issue_replay for one execution."""
    if not re.fullmatch(r"[a-z0-9_]{1,40}", execution_id):
        raise SystemExit(
            "execution_id must contain 1-40 lowercase letters, digits or _"
        )
    return f"{TEST_CATALOG}.{TARGET_SCHEMA}.s2_replay_issues_{execution_id}"


def differences(left: DataFrame, right: DataFrame) -> tuple[int, int]:
    """Count rows only in ``left`` and rows only in ``right`` (duplicates count)."""
    return left.exceptAll(right).count(), right.exceptAll(left).count()


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
    live_table = config.silver_issues_table
    live_version_before = table_version(spark, live_table)

    live = spark.table(live_table)
    # Compare exactly the Silver contract columns. The candidates also carry
    # manifest_id and replay_execution_id, which describe the execution, so they
    # are left out. source_run_id is source lineage, so it stays in.
    columns = live.columns
    first_raw = spark.table(candidate_table(first_id))
    second_raw = spark.table(candidate_table(second_id))

    manifest_ids = {
        row["manifest_id"]
        for frame in (first_raw, second_raw)
        for row in frame.select("manifest_id").distinct().collect()
    }
    check(len(manifest_ids) == 1, f"executions use different manifests: {manifest_ids}")

    first = first_raw.select(*columns)
    second = second_raw.select(*columns)

    results = {
        "first_vs_second": differences(first, second),
        "first_vs_live": differences(first, live),
        "second_vs_live": differences(second, live),
    }

    # Live Silver is mutable. If a pipeline run landed during the comparison,
    # the live result is not a valid reference.
    live_version_after = table_version(spark, live_table)
    check(
        live_version_before == live_version_after,
        f"live Silver changed from v{live_version_before} to v{live_version_after}",
    )

    print(
        {
            "manifest_id": manifest_ids.pop(),
            "rows": {
                first_id: first.count(),
                second_id: second.count(),
                "live": live.count(),
            },
            "live_silver_version": live_version_before,
            "differences (only_left, only_right)": results,
        }
    )
    for name, (only_left, only_right) in results.items():
        check(
            only_left == 0 and only_right == 0,
            f"{name} differ: {only_left}/{only_right}",
        )
    print("OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--first", required=True, help="first replay execution_id")
    parser.add_argument("--second", required=True, help="second replay execution_id")
    arguments = parser.parse_args()
    main(arguments.first, arguments.second)
