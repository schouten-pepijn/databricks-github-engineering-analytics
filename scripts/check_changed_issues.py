"""Live proof that an unreadable change-feed range fails with one clear error.

TEST only. The script creates one scratch table, switches the change feed on
AFTER the first data commit, and asks for a range that starts before that. The
feed is not retroactive, so Delta cannot serve that range, and the workset
builder must report it as ChangeRangeUnavailableError.

The scratch table is created and dropped by the script itself, so nothing needs
to exist beforehand. The positive case (which Issues changed) is proven by
build_cdf_projection.py, which uses the real Silver writers.
"""

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from github_engineering_analytics.orchestration.silver_replay import (
    require_test_catalog,
)
from github_engineering_analytics.silver.changed_issues import (
    ChangeRange,
    ChangeRangeUnavailableError,
    changed_issue_keys,
)

CATALOG = "github_engineering_analytics_test"
LATE_CDF = f"{CATALOG}.github_analytics_v2min.s4_late_cdf"


def prove_the_failure(spark: SparkSession) -> None:
    """A range that predates the moment CDF was switched on must fail clearly."""
    # Drop any leftover from an earlier run that crashed before its cleanup, so
    # this proof always starts from a fresh table.
    _ = spark.sql(f"DROP TABLE IF EXISTS {LATE_CDF}")
    try:
        # Version 0: the empty table, created WITHOUT the change feed.
        _ = spark.sql(
            f"CREATE TABLE {LATE_CDF} "
            + "(repository_owner STRING, repository_name STRING, issue_id BIGINT)"
        )
        # Version 1: a data commit that the feed will not have recorded.
        _ = spark.sql(f"INSERT INTO {LATE_CDF} VALUES ('psf', 'requests', 1)")
        # Version 2: only now is the change feed switched on.
        _ = spark.sql(
            f"ALTER TABLE {LATE_CDF} "
            + "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        # Version 3: the first commit the feed does record.
        _ = spark.sql(f"UPDATE {LATE_CDF} SET issue_id = 2 WHERE issue_id = 1")

        try:
            # Both tables point at the scratch table. The first read fails, so the
            # second is never reached; passing the same table keeps the proof
            # self-contained, with no second fixture table.
            _ = changed_issue_keys(
                spark,
                issues_table=LATE_CDF,
                issues_range=ChangeRange(0, 3),  # starts before the feed existed
                issue_labels_table=LATE_CDF,
                issue_labels_range=ChangeRange(3, 3),
            )
        except ChangeRangeUnavailableError as error:
            print({"failure_proof": "explicit error", "message": str(error)})
        else:
            raise SystemExit("CHECK FAILED: the late-CDF range did not fail")
    finally:
        # Always clean up the scratch table, even when the check fails.
        _ = spark.sql(f"DROP TABLE IF EXISTS {LATE_CDF}")


if __name__ == "__main__":
    require_test_catalog(CATALOG)
    session = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    prove_the_failure(session)
    print("OK")
