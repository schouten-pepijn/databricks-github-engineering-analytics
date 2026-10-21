"""Run one incremental Gold batch on the scratch projection (TEST only).

Wires run_batch to the real pieces: the CDF workset, a dbt run of the candidate
models, the comparison script and the position table. Positions live in a
scratch control schema, never in the real control schema.

First run:  --baseline-issues 3 --baseline-labels 3
Later runs: no baseline; the stored positions are used.
Recovery proof: add --fail-after-write, then run again without it.
"""

import argparse
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime

from databricks.connect import DatabricksSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.cdf_consumer_position_repository import (
    DeltaCdfConsumerPositionRepository,
)
from github_engineering_analytics.orchestration.incremental_batch import run_batch
from github_engineering_analytics.orchestration.silver_replay import (
    require_test_catalog,
)
from github_engineering_analytics.orchestration.workset_table import write_workset
from github_engineering_analytics.silver.changed_issues import (
    ChangeRange,
    changed_issue_keys,
)

CATALOG = "github_engineering_analytics_test"
SOURCE_SCHEMA = "github_analytics_s4_projection"
FROZEN_SCHEMA = "github_analytics_s5_frozen_end_v4"
WORKSET_SCHEMA = "github_analytics_s5_workset_proof"
CANDIDATE_SCHEMA = "github_analytics_s5_cand"
# Scratch control schema: the real positions table is never touched here.
CONTROL_SCHEMA = "github_analytics_s5_control"

ISSUES = f"{CATALOG}.{SOURCE_SCHEMA}.github_issues"
ISSUE_LABELS = f"{CATALOG}.{SOURCE_SCHEMA}.github_issue_labels"


def latest_version(spark, table: str) -> int:
    """Return the newest Delta version of a table."""
    row = spark.sql(f"DESCRIBE HISTORY {table}").agg({"version": "max"}).collect()[0]
    return int(row[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-issues", type=int)
    parser.add_argument("--baseline-labels", type=int)
    parser.add_argument(
        "--run-id", default=datetime.now(UTC).strftime("b_%Y%m%d%H%M%S")
    )
    parser.add_argument("--fail-after-write", action="store_true")
    args = parser.parse_args()

    require_test_catalog(CATALOG)
    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog=CATALOG, control_schema=CONTROL_SCHEMA)
    positions = DeltaCdfConsumerPositionRepository(spark, config)
    positions.ensure_table()

    baseline = None
    if args.baseline_issues is not None or args.baseline_labels is not None:
        if args.baseline_issues is None or args.baseline_labels is None:
            raise SystemExit("pass both --baseline-issues and --baseline-labels")
        baseline = {ISSUES: args.baseline_issues, ISSUE_LABELS: args.baseline_labels}

    # Read BEFORE the batch: exactly these versions are committed afterwards.
    current = {
        ISSUES: latest_version(spark, ISSUES),
        ISSUE_LABELS: latest_version(spark, ISSUE_LABELS),
    }

    def write(ranges: Mapping[str, ChangeRange]) -> int:
        keys = changed_issue_keys(
            spark,
            issues_table=ISSUES,
            issues_range=ranges[ISSUES],
            issue_labels_table=ISSUE_LABELS,
            issue_labels_range=ranges[ISSUE_LABELS],
        )
        return write_workset(spark, keys, catalog=CATALOG, schema=WORKSET_SCHEMA)

    def build_candidate() -> None:
        variables = (
            f"{{silver_schema: {FROZEN_SCHEMA}, "
            f"gold_output_schema: {CANDIDATE_SCHEMA}, "
            f"workset_schema: {WORKSET_SCHEMA}}}"
        )
        subprocess.run(
            [
                "uv",
                "run",
                "dbt",
                "run",
                "--project-dir",
                "dbt",
                "--profiles-dir",
                "dbt",
                "--select",
                "tag:candidate",
                "--vars",
                variables,
            ],
            check=True,
        )

    def compare() -> None:
        subprocess.run(
            [sys.executable, "scripts/compare_gold_candidate.py"], check=True
        )

    result = run_batch(
        positions=positions,
        current_versions=current,
        baseline=baseline,
        write_workset=write,
        build_candidate=build_candidate,
        compare=compare,
        run_id=args.run_id,
        now=datetime.now(UTC),
        fail_after_write=args.fail_after_write,
    )
    print(
        "BATCH",
        {
            "workset_rows": result.workset_rows,
            "positions": dict(result.committed_positions),
        },
    )


if __name__ == "__main__":
    main()
