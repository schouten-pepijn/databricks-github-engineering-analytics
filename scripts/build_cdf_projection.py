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
    """Fail loudly; unlike assert, this check remains active under python -0."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def payload(
    issue_id: int,
    *,
    state: str,
    updated_at: str,
    # Sequence and Mapping are covariant, so a list of label dicts with mixed
    # value types is accepted. list and dict are invariant and would not be.
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


def enable_change_feed(spark: SparkSession, table: str) -> None:
    """Switch the change feed on for an empty table, before any delta commit."""
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def run_silver(spark: SparkSession, config: PipelineConfig, run_id: str) -> None:
    """Run the four Silver stages of the pipeline for one Bronze run."""
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
    """Return the newest Delta version of a table."""
    rows = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).collect()
    return int(rows[0][0])


def main() -> None:
    """Build the projection and compare the workset with the expected answer."""
    require_test_catalog(CATALOG)
    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(
        catalog=CATALOG, bronze_schema=SCRATCH_SCHEMA, silver_schema=SCRATCH_SCHEMA
    )
    check(
        not spark.catalog.tableExists(config.bronze_issues_table),
        f"{config.bronze_issues_table} exists; drop the scratch schema first",
    )

    # The tables must exist, with the change feed on, before the first data
    # commit: the feed is not retroactive.
    DeltaBronzeIssueWriter(spark=spark, config=config).ensure_table()
    for writer in (
        DeltaSilverIssueWriter(spark=spark, config=config),
        DeltaSilverUserWriter(spark=spark, config=config),
        DeltaSilverLabelWriter(spark=spark, config=config),
        DeltaSilverIssueLabelWriter(spark=spark, config=config),
    ):
        writer.ensure_table()
    for table in (
        config.silver_issues_table,
        config.silver_users_table,
        config.silver_labels_table,
        config.silver_issue_labels_table,
    ):
        enable_change_feed(spark, table)

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
    keys = changed_issue_keys(
        spark,
        issues_table=config.silver_issues_table,
        issues_range=ChangeRange(baseline["issues"] + 1, end["issues"]),
        issue_labels_table=config.silver_issue_labels_table,
        issue_labels_range=ChangeRange(baseline["labels"] + 1, end["labels"]),
    )
    issue_ids = sorted(row["issue_id"] for row in keys.collect())

    expected = {1, 2, 3, 4}
    check(expected <= set(issue_ids), f"missing changed Issues: {issue_ids}")
    check(set(issue_ids) - expected <= {5}, f"unexpected Issues: {issue_ids}")
    print(
        {
            "workset": issue_ids,
            "issue_5_in_workset_without_a_real_change": 5 in issue_ids,
        }
    )
    print("OK")


if __name__ == "__main__":
    main()
