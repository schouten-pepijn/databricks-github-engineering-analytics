"""Compare the incremental Gold candidate with a full build (TEST only).

Both builds must come from the same frozen Silver input. Rows are compared as
sets in both directions (EXCEPT ALL, so duplicate rows count too), plus a row
count. Read-only: nothing is written.
"""

from databricks.connect import DatabricksSession

CATALOG = "github_engineering_analytics_test"
CANDIDATE_SCHEMA = "github_analytics_s5_cand"
FULL_SCHEMA = "github_analytics_s5_full_end"
# Lineage columns are ignored by the change filter on purpose (see
# silver/changed_issues.py): an Issue that was only re-observed keeps its old
# source_run_id in the candidate, while a full build shows the newest run. They
# are compared separately and reported as drift, not as a business difference.
LINEAGE_COLUMNS = ("source_run_id",)
PAIRS = {
    "fact_issue": ("fact_issue_incremental", "fact_issue"),
    "bridge_issue_label": ("bridge_issue_label_incremental", "bridge_issue_label"),
}


def main() -> None:
    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    failed = False
    for name, (candidate_table, full_table) in PAIRS.items():
        candidate_all = spark.table(f"{CATALOG}.{CANDIDATE_SCHEMA}.{candidate_table}")
        full_all = spark.table(f"{CATALOG}.{FULL_SCHEMA}.{full_table}")
        candidate = candidate_all.drop(*LINEAGE_COLUMNS)
        full = full_all.drop(*LINEAGE_COLUMNS)
        # Same columns in the same order, otherwise EXCEPT would compare wrongly.
        columns_equal = candidate.columns == full.columns
        only_candidate = candidate.exceptAll(full).count() if columns_equal else -1
        only_full = full.exceptAll(candidate).count() if columns_equal else -1
        counts = (candidate.count(), full.count())
        # Rows whose lineage columns differ (only meaningful when the rest is equal).
        drift = candidate_all.exceptAll(full_all).count()
        ok = columns_equal and only_candidate == 0 and only_full == 0
        failed |= not ok
        print(
            "COMPARE",
            name,
            "rows(candidate, full)=",
            counts,
            "only_candidate=",
            only_candidate,
            "only_full=",
            only_full,
            "lineage_drift_rows=",
            drift,
            "OK" if ok else "DIFFERENT",
        )
    if failed:
        raise SystemExit("CHECK FAILED: candidate differs from the full build")
    print("OK")


if __name__ == "__main__":
    main()
