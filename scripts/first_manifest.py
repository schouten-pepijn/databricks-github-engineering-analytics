"""Prove manifest creation and replay-input reads against the TEST catalog."""

import argparse
import subprocess
from dataclasses import replace

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
from github_engineering_analytics.control.watermark_repository import (
    DeltaWatermarkRepository,
)
from github_engineering_analytics.orchestration.manifest_builder import (
    build_replay_manifest,
)


def check(condition: bool, message: str) -> None:
    """Fail loudly; unlike assert, this check remains active under python -O."""
    if not condition:
        raise SystemExit(f"CHECK FAILED: {message}")


def main(run_id: str) -> None:
    """Create and verify one manifest for a successful TEST pipeline run."""
    spark: SparkSession = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog="github_engineering_analytics_test")
    runs = DeltaPipelineRunRepository(spark, config)
    manifests = DeltaReplayManifestRepository(spark, config)
    watermarks = DeltaWatermarkRepository(spark, config)

    run = runs.get(run_id)
    if run.finished_at is None:
        raise SystemExit(f"CHECK FAILED: run {run_id} has no finish time")
    cutoff_at = run.finished_at
    watermark_before = watermarks.get("github", "issues")
    manifest_count_before = spark.table(config.replay_manifest_table).count()
    code_commit = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], text=True
    ).strip()

    def build() -> ReplayManifest:
        return build_replay_manifest(
            spark=spark,
            runs=runs,
            manifests=manifests,
            repository_owner="psf",
            repository_name="requests",
            bronze_table=config.bronze_issues_table,
            successful_run_ids=[run_id],
            cutoff_at=cutoff_at,
            code_commit=code_commit,
            transformation_version="v1",
        )

    manifest = build()
    stored_manifest = manifests.get(manifest.manifest_id)
    frozen_rows = read_manifest_input(spark, manifest).count()
    check(frozen_rows > 0, "manifest input must contain Bronze rows")

    count_after_first_build = spark.table(config.replay_manifest_table).count()
    check(
        count_after_first_build in {manifest_count_before, manifest_count_before + 1},
        "manifest create must insert at most one row",
    )

    # Same run finish time and inputs produce the same manifest ID on retry.
    build()
    check(
        spark.table(config.replay_manifest_table).count() == count_after_first_build,
        "repeating the same manifest build must not add a row",
    )

    # An unavailable Delta version must fail explicitly without another manifest.
    unavailable = replace(
        manifest,
        bronze_version=manifest.bronze_version + 1_000_000,
    )
    try:
        read_manifest_input(spark, unavailable).count()
    except ManifestInputUnavailableError as error:
        print("expected failure:", error)
    else:
        raise SystemExit("CHECK FAILED: expected ManifestInputUnavailableError")

    check(
        manifests.get(manifest.manifest_id) == stored_manifest == manifest,
        "stored manifest differs from the manifest that was built",
    )
    check(
        spark.table(config.replay_manifest_table).count() == count_after_first_build,
        "failed input read must not add a manifest",
    )
    check(
        watermarks.get("github", "issues") == watermark_before,
        "manifest creation must not change the extraction watermark",
    )

    print(
        {
            "manifest_id": manifest.manifest_id,
            "run_id": run_id,
            "bronze_version": manifest.bronze_version,
            "frozen_rows": frozen_rows,
            "watermark_unchanged": True,
            "idempotent_repeat": True,
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    main(parser.parse_args().run_id)
