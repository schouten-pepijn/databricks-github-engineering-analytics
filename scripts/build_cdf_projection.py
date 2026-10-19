"""Build a small accepted-Silver projection with the Change Data Feed switched on.

TEST only. Two invented pipeline runs go through the real Bronze writer, the real
Silver transformers and the real MERGE writers, into a scratch schema. After each
run the Delta version of each table is noted, so the second run can be read as a
bounded change range. The changed-Issue workset is then compared with the answer
that was decided in advance.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.issues import (
    BronzeIssueRecord,
    DeltaBronzeIssueWriter,
)
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.orchestration.silver_replay import (
    require_test_catalog,
)
from github_engineering_analytics.silver.changed_issues import (
    ChangeRange,
    changed_issue_keys,
)
from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer as IssueLabelTransformer,
    DeltaSilverIssueLabelWriter,
)
from github_engineering_analytics.silver.issues import (
    BronzeIssueToSilverTransformer,
    DeltaSilverIssueWriter,
)
from github_engineering_analytics.silver.labels import (
    BronzeIssueToSilverLabelTransformer as LabelTransformer,
    DeltaSilverLabelWriter,
)
from github_engineering_analytics.silver.users import (
    BronzeIssueToSilverUserTransformer,
    DeltaSilverUserWriter,
)

CATALOG = "github_engineering_analytics_test"
SCRATCH_SCHEMA = "github_analytics_s4_projection"
OWNER, REPOSITORY = "psf", "requests"

BUG = {
    "id": 100,
    "name": "bug",
    "color": "d73a4a",
    "description": None,
    "default": True,
}
DOCS = {
    "id": 101,
    "name": "docs",
    "color": "0075ca",
    "description": None,
    "default": False,
}
FEATURE = {
    "id": 102,
    "name": "feature",
    "color": "a2eeef",
    "description": None,
    "default": False,
}
USER = {
    "id": 9001,
    "login": "alice",
    "type": "User",
}


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this check remains active under python -O.

    A plain assert is removed when Python runs with -O, so a check written with
    assert could silently pass. A SystemExit always stops the script.
    """
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def payload(
    issue_id: int,
    *,
    state: str,
    updated_at: str,
    # Sequence and Mapping are covariant: a list of label dicts with mixed value
    # types (str, int, bool, None) is accepted. list and dict are invariant, so
    # the type checker would reject the same list typed as list[dict[str, object]].
    labels: Sequence[Mapping[str, object]],
    created_at: str = "2026-10-09T08:00:00Z",
    closed_at: str | None = None,
) -> dict[str, object]:
    """Build the part of a Github issue payload that the transformers require."""
    return {
        "id": issue_id,
        "number": issue_id,
        "title": f"issue {issue_id}",
        "state": state,
        "user": USER,
        "labels": labels,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at,
    }


# Run 1 is the baseline: Issues 1, 2, 3 and 5.
RUN_1 = [
    payload(1, state="open", updated_at="2026-10-09T08:00:00Z", labels=[BUG, DOCS]),
    payload(2, state="open", updated_at="2026-10-09T09:00:00Z", labels=[BUG]),
    payload(3, state="open", updated_at="2026-10-09T10:00:00Z", labels=[]),
    payload(5, state="open", updated_at="2026-10-09T11:00:00Z", labels=[BUG]),
]

# Run 2 changes four Issues, and re-observes Issue 5 exactly as it was.
#
# Issue 5 keeps its updated_at. The Silver MERGE uses updated_at >= target, so
# the row still matches and is rewritten. Business values stay the same, but
# Delta records an update pair anyway. That is the "no-op update" that the next
# step filters out. It is the reason this scenario includes Issue 5.
RUN_2 = [
    payload(
        1,
        state="closed",
        updated_at="2026-10-12T09:00:00Z",
        labels=[BUG, DOCS],
        closed_at="2026-10-12T09:00:00Z",
    ),
    payload(2, state="open", updated_at="2026-10-12T09:30:00Z", labels=[]),
    payload(3, state="open", updated_at="2026-10-12T09:40:00Z", labels=[FEATURE]),
    payload(
        4,
        state="open",
        updated_at="2026-10-12T08:00:00Z",
        labels=[],
        created_at="2026-10-12T08:00:00Z",
    ),
    payload(5, state="open", updated_at="2026-10-09T11:00:00Z", labels=[BUG]),
]


def append_bronze_run(
    spark: SparkSession,
    config: PipelineConfig,
    run_id: str,
    ingested_at: datetime,
    payloads: list[dict[str, object]],
) -> None:
    """Append one invented run to the scratch Bronze table."""
    records = [
        BronzeIssueRecord.from_github_payload(
            repository_owner=OWNER,
            repository_name=REPOSITORY,
            payload=item,
            run_id=run_id,
            ingested_at=ingested_at,
            request_watermark=None,
            page_or_batch_reference="page-1",
        )
        for item in payloads
    ]
    DeltaBronzeIssueWriter(spark=spark, config=config).append(records)


def run_silver(spark: SparkSession, config: PipelineConfig, run_id: str) -> None:
    """Run the four Silver stages of the pipeline for one Bronze run.

    The filter on _run_id reads only this run's rows, as the real pipeline does.
    The order matters: Issues, users and labels first, then the Issue-label
    reconciliation, which needs the observed Issues from its transformer.
    """
    bronze = spark.table(config.bronze_issues_table).where(F.col("_run_id") == run_id)

    issues = DeltaSilverIssueWriter(spark=spark, config=config)
    users = DeltaSilverUserWriter(spark=spark, config=config)
    labels = DeltaSilverLabelWriter(spark=spark, config=config)
    issue_labels = DeltaSilverIssueLabelWriter(spark=spark, config=config)

    issues.upsert_dataframe(BronzeIssueToSilverTransformer().transform(bronze))
    users.upsert_dataframe(BronzeIssueToSilverUserTransformer().transform(bronze))
    labels.upsert_dataframe(LabelTransformer().transform(bronze))
    transformation = IssueLabelTransformer().transform(bronze)
    issue_labels.reconcile_dataframe(
        observed_issues=transformation.observed_issues,
        relationships=transformation.relationships,
    )


def latest_version(spark: SparkSession, table: str) -> int:
    """Return the newest Delta version of a table.

    DESCRIBE HISTORY lists every commit, newest first, with a version number
    per commit. The maximum version is the last commit written so far.
    """
    rows = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).collect()
    return int(rows[0][0])


def main() -> None:
    """Build the projection and compare the workset with the expected answer."""
    _ = require_test_catalog(CATALOG)
    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(
        catalog=CATALOG, bronze_schema=SCRATCH_SCHEMA, silver_schema=SCRATCH_SCHEMA
    )
    # Start from empty scratch tables. If they already exist, the commit
    # versions would not match the numbers this script expects, so refuse to run.
    check(
        not spark.catalog.tableExists(config.bronze_issues_table),
        f"{config.bronze_issues_table} exists; drop the scratch schema first",
    )

    # Create every table first. The Issues and Issue-label writers switch the
    # change feed on inside ensure_table, before any data is written: the feed is
    # not retroactive, so a later switch would miss the first commits. The users
    # and labels tables do not need the feed, and their writers leave it off.
    DeltaBronzeIssueWriter(spark=spark, config=config).ensure_table()
    for writer in (
        DeltaSilverIssueWriter(spark=spark, config=config),
        DeltaSilverUserWriter(spark=spark, config=config),
        DeltaSilverLabelWriter(spark=spark, config=config),
        DeltaSilverIssueLabelWriter(spark=spark, config=config),
    ):
        writer.ensure_table()

    append_bronze_run(
        spark, config, "s4_run_1", datetime(2026, 10, 10, 10, 0, tzinfo=UTC), RUN_1
    )
    run_silver(spark, config, "s4_run_1")
    baseline = {
        "issues": latest_version(spark, config.silver_issues_table),
        "labels": latest_version(spark, config.silver_issue_labels_table),
    }

    append_bronze_run(
        spark, config, "s4_run_2", datetime(2026, 10, 12, 10, 0, tzinfo=UTC), RUN_2
    )
    run_silver(spark, config, "s4_run_2")
    end = {
        "issues": latest_version(spark, config.silver_issues_table),
        "labels": latest_version(spark, config.silver_issue_labels_table),
    }
    print({"baseline_versions": baseline, "end_versions": end})

    # Each table has its own version numbers, so each gets its own range.
    # The range starts one version after the baseline: the baseline commits
    # belong to run 1, and reading them again would count them as changes.
    keys = changed_issue_keys(
        spark,
        issues_table=config.silver_issues_table,
        issues_range=ChangeRange(baseline["issues"] + 1, end["issues"]),
        issue_labels_table=config.silver_issue_labels_table,
        issue_labels_range=ChangeRange(baseline["labels"] + 1, end["labels"]),
    )
    issue_ids = sorted(row["issue_id"] for row in keys.collect())

    # Exactly the Issues that really changed. Issue 5 was only re-observed.
    check(issue_ids == [1, 2, 3, 4], f"unexpected workset: {issue_ids}")
    print({"workset": issue_ids, "issue_5_excluded": 5 not in issue_ids})
    print("OK")


if __name__ == "__main__":
    main()
