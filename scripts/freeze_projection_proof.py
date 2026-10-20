"""Live proof of freeze_silver: freeze the scratch projection at two moments.

TEST only. Reads the scratch projection from build_cdf_projection.py and copies
its four Silver tables at two pinned Delta versions into two frozen schemas.
Nothing in the real Silver or Gold tables is touched.

Run the discovery step first, then fill in the two version dictionaries.
"""

from databricks.connect import DatabricksSession

from github_engineering_analytics.orchestration.silver_freeze import freeze_silver

CATALOG = "github_engineering_analytics_test"
SOURCE_SCHEMA = "github_analytics_s4_projection"
TABLES = ("github_issues", "github_users", "github_labels", "github_issue_labels")

# Fill these in from the history printed by discover(). Each value is the Delta
# version of the last MERGE after the run named in the comment.
BASE_VERSIONS = {  # the last MERGE of run 1 (baseline), per table
    "github_issues": 3,
    "github_users": 2,
    "github_labels": 2,
    "github_issue_labels": 3,
}
END_VERSIONS = {  # the last MERGE of run 2, per table
    "github_issues": 4,
    "github_users": 3,
    "github_labels": 3,
    "github_issue_labels": 4,
}


def discover(spark) -> None:
    """Print the Delta history of each source table, so the versions can be read."""
    for table in TABLES:
        rows = (
            spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SOURCE_SCHEMA}.{table}")
            .orderBy("version")
            .select("version", "operation")
            .collect()
        )
        print("HISTORY", table, [(r.version, r.operation) for r in rows])


def freeze_both(spark) -> None:
    """Freeze the two moments into two isolated schemas."""
    for label, versions in (("base_v3", BASE_VERSIONS), ("end_v4", END_VERSIONS)):
        result = freeze_silver(
            spark,
            catalog=CATALOG,
            label=label,
            source_schema=SOURCE_SCHEMA,
            versions=versions,
        )
        for table, info in result.items():
            print(
                "FROZEN", label, table, "rows", info["rows"], "version", info["version"]
            )


if __name__ == "__main__":
    session = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")

    discover(session)
    # Run freeze_both only after the version dictionaries above are filled in.
    freeze_both(session)
