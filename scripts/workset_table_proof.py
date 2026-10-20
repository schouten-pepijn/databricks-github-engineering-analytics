"""Live proof of write_workset: store the changed-Issue keys as a Delta table.

TEST only. Reads the scratch projection from build_cdf_projection.py, derives the
workset from the run-2 change range, writes it into a github_analytics_s5_*
scratch schema and checks the table. A second call must replace the table, not
append to it. Nothing in the real Silver or Gold tables is touched.
"""

from databricks.connect import DatabricksSession

from github_engineering_analytics.orchestration.workset_table import (
    WORKSET_TABLE,
    write_workset,
)
from github_engineering_analytics.silver.changed_issues import (
    ChangeRange,
    changed_issue_keys,
)

CATALOG = "github_engineering_analytics_test"
SOURCE_SCHEMA = "github_analytics_s4_projection"
TARGET_SCHEMA = "github_analytics_s5_workset_proof"

# Run 2 as one range per table (baseline version + 1 .. end version).
ISSUES_RANGE = ChangeRange(4, 4)
LABELS_RANGE = ChangeRange(4, 4)


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this stays active under python -O."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def main() -> None:
    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    table = f"{CATALOG}.{TARGET_SCHEMA}.{WORKSET_TABLE}"
    check(
        not spark.catalog.tableExists(table),
        f"{table} exists; drop the scratch schema first",
    )

    keys = changed_issue_keys(
        spark,
        issues_table=f"{CATALOG}.{SOURCE_SCHEMA}.github_issues",
        issues_range=ISSUES_RANGE,
        issue_labels_table=f"{CATALOG}.{SOURCE_SCHEMA}.github_issue_labels",
        issue_labels_range=LABELS_RANGE,
    )

    # 1. First write: exactly the changed Issues [1, 2, 3, 4].
    rows = write_workset(spark, keys, catalog=CATALOG, schema=TARGET_SCHEMA)
    stored = sorted(r["issue_id"] for r in spark.table(table).collect())
    print({"rows": rows, "issue_ids": stored})
    check(rows == 4 and stored == [1, 2, 3, 4], f"unexpected workset: {stored}")

    # 2. Second write with a smaller workset must replace, not append.
    smaller = keys.where("issue_id = 1")
    rows = write_workset(spark, smaller, catalog=CATALOG, schema=TARGET_SCHEMA)
    stored = sorted(r["issue_id"] for r in spark.table(table).collect())
    print({"rows": rows, "issue_ids": stored})
    check(rows == 1 and stored == [1], f"table was not replaced: {stored}")

    # 3. Guards refuse before anything is written.
    for bad in ("github_analytics_silver", "github_analytics_s5_x; DROP"):
        try:
            write_workset(spark, keys, catalog=CATALOG, schema=bad)
        except ValueError as exc:
            print("REFUSED", repr(bad), "->", exc)
        else:
            raise SystemExit(f"CHECK FAILED: {bad!r} was accepted")
    print("OK")


if __name__ == "__main__":
    main()
