"""Live check of the changed-Issue workset against the Sprint 4 fixture tables.

Part 1 is read-only: it reads the two fixture tables and checks the workset.
Part 2 (--failure-proof) creates a scratch table where the change feed was
switched on late, asks for a range that predates it, and expects the explicit
error. The scratch table is always dropped afterwards.
"""

import argparse

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from github_engineering_analytics.silver.changed_issues import (
    ChangeRange,
    ChangeRangeUnavailableError,
    changed_issue_keys,
)

SCHEMA = "github_engineering_analytics_test.github_analytics_v2min"
ISSUES = f"{SCHEMA}.s4_fx_issues"
LABELS = f"{SCHEMA}.s4_fx_issue_labels"
LATE_CDF = f"{SCHEMA}.s4_fx_late_cdf"


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this check remains active under python -O.

    A plain assert is removed when Python runs with -O, so a check written with
    assert could silently pass. A SystemExit always stops the script.
    """
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def prove_the_workset(spark: SparkSession) -> None:
    """The fixture changed Issues 1, 2, 3 and 4; Issue 5 was never touched."""
    keys = changed_issue_keys(
        spark,
        issues_table=ISSUES,
        issues_range=ChangeRange(2, 3),
        issue_labels_table=LABELS,
        issue_labels_range=ChangeRange(2, 3),
    )
    issue_ids = sorted(row["issue_id"] for row in keys.collect())

    check(issue_ids == [1, 2, 3, 4], f"unexpected workset: {issue_ids}")
    print({"workset": issue_ids, "issue_5_excluded": 5 not in issue_ids})


def prove_the_failure(spark: SparkSession) -> None:
    """A range that predates the moment CDF was switched on must fail clearly."""
    # Drop any leftover from an earlier run that crashed before its cleanup, so
    # this proof always starts from a fresh table.
    _ = spark.sql(f"DROP TABLE IF EXISTS {LATE_CDF}")
    try:
        _ = spark.sql(
            f"CREATE TABLE {LATE_CDF} "
            + "(repository_owner STRING, repository_name STRING, issue_id BIGINT)"
        )
        _ = spark.sql(f"INSERT INTO {LATE_CDF} VALUES ('psf', 'requests', 1)")
        _ = spark.sql(
            f"ALTER TABLE {LATE_CDF} "
            + "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        _ = spark.sql(f"UPDATE {LATE_CDF} SET issue_id = 2 WHERE issue_id = 1")

        try:
            _ = changed_issue_keys(
                spark,
                issues_table=LATE_CDF,
                issues_range=ChangeRange(0, 3),  # predates the SET TBLPROPERTIES
                issue_labels_table=LABELS,
                issue_labels_range=ChangeRange(2, 3),
            )
        except ChangeRangeUnavailableError as error:
            print({"failure_proof": "explicit error", "message": str(error)})
        else:
            raise SystemExit("CHECK FAILED: the late-CDF range did not fail")
    finally:
        # Always clean up the scratch table, even when a check fails.
        _ = spark.sql(f"DROP TABLE IF EXISTS {LATE_CDF}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--failure-proof", action="store_true")
    arguments = parser.parse_args()

    session = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    prove_the_workset(session)
    if arguments.failure_proof:
        prove_the_failure(session)
    print("OK")
