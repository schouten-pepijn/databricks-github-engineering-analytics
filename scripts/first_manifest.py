"""One-off Sprint 1 proof against the TEST catalog (writes one manifest row)."""

import subprocess
from datetime import UTC, datetime

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
    read_manifest_input,
)
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.control.replay_manifest_repository import (
    DeltaReplayManifestRepository,
)
from github_engineering_analytics.orchestration.manifest_builder import (
    build_replay_manifest,
)

RUN_IDS = ["937907479861126"]


def check(condition: bool, message: str) -> None:
    """Fail loudly. Unlike assert, this is not removed by ``python -O``."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def main() -> None:
    spark: SparkSession = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    # The repositories refuse to run unless the session time zone is UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog="github_engineering_analytics_test")
    runs = DeltaPipelineRunRepository(spark, config)
    manifests = DeltaReplayManifestRepository(spark, config)
    manifests.ensure_table()
    commit = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], text=True
    ).strip()

    def build(cutoff_at: datetime) -> ReplayManifest:
        return build_replay_manifest(
            spark=spark,
            runs=runs,
            manifests=manifests,
            repository_owner="psf",
            repository_name="requests",
            bronze_table=config.bronze_issues_table,
            successful_run_ids=RUN_IDS,
            cutoff_at=cutoff_at,
            code_commit=commit,
            transformation_version="v1",
        )

    def count_manifests() -> int:
        return spark.table(config.replay_manifest_table).count()

    # Case 1: success
    before = count_manifests()
    now = datetime.now(UTC)  # one value, so both builds describe the same input
    manifest = build(now)
    print("created:", manifest)
    rows_frozen = read_manifest_input(spark, manifest).count()
    print("rows frozen:", rows_frozen)
    check(rows_frozen == 2, f"expected 2 frozen rows, got {rows_frozen}")
    check(manifests.get(manifest.manifest_id) == manifest, "stored manifest differs")
    check(count_manifests() == before + 1, "first build should add one row")
    build(now)  # same inputs again -> same ID -> idempotent no-op
    check(count_manifests() == before + 1, "repeat build must not add a row")

    # Case 2: expired input must fail before any write
    try:
        build(datetime(2026, 9, 28, 21, 0, tzinfo=UTC))
    except ManifestInputUnavailableError as error:
        print("expected failure:", error.__cause__ or error)
    else:
        raise SystemExit("CHECK FAILED: expected ManifestInputUnavailableError")
    check(count_manifests() == before + 1, "failed build must not store a manifest")
    print("OK")


if __name__ == "__main__":
    main()
