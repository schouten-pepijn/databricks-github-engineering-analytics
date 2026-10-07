"""Compose a ReplayManifest from succeeded runs and Bronze Delta history.

This is called by hand (or by a separate replay job), never by the normal
pipeline run. It looks at the evidence the pipeline already left behind:

- ``pipeline_runs``: which runs fully succeeded, and when they finished
- Bronze Delta history: which table version existed at the cutoff

and turns it into one stored manifest that locks the input of a manual replay.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
    read_manifest_input,
)
from github_engineering_analytics.control.pipeline_run import PipelineRunStatus
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.control.replay_manifest_repository import (
    DeltaReplayManifestRepository,
)


def resolve_bronze_version(
    spark: SparkSession,
    bronze_table: str,
    cutoff_at: datetime,
) -> int:
    """Return the newest Delta version committed at or before ``cutoff_at``."""
    # Every write to a Delta table is a numbered commit with a timestamp.
    # Take the newest commit that is not later than the cutoff.
    rows = (
        DeltaTable.forName(spark, bronze_table)
        .history()
        .where(F.col("timestamp") <= F.lit(cutoff_at))
        .orderBy(F.col("version").desc())
        .select("version")
        .limit(1)
        .collect()
    )

    if not rows:
        # Old commits drop out of the history after delta.logRetentionDuration,
        # so a very old cutoff can fall before the oldest commit we can see.
        raise ManifestInputUnavailableError(
            f"{bronze_table} has no Delta commit at or before {cutoff_at.isoformat()}"
        )

    return int(rows[0]["version"])


def require_succeeded_runs(
    runs: DeltaPipelineRunRepository,
    run_ids: Sequence[str],
    cutoff_at: datetime,
) -> None:
    """Accept only runs that fully succeeded before the cutoff."""
    for run_id in run_ids:
        run = runs.get(run_id)  # raises ValueError for an unknown run

        # FAILED and RUNNING runs only wrote part of their data to Bronze,
        # so they are never a valid replay input.
        if run.status is not PipelineRunStatus.SUCCEEDED:
            raise ValueError(f"Run {run_id!r} is {run.status.value}, not succeeded")

        # A run writes Bronze in several commits. If it finished after the
        # cutoff, some of its commits come after the version we will pick,
        # and we would read only part of that run.
        if run.finished_at is None or run.finished_at > cutoff_at:
            raise ValueError(f"Run {run_id!r} did not finish before the cutoff")


def _manifest_id(**fields: object) -> str:
    """Derive the ID from the content, so the same input gives the same ID.

    A repeated build then hits the idempotent no-op in the repository's
    ``create``, and a changed field gives a new ID instead of a conflict.
    """
    # sort_keys makes the JSON (and so the hash) independent of argument order.
    payload = json.dumps(fields, sort_keys=True, default=str)
    return "manifest-" + hashlib.sha256(payload.encode()).hexdigest()[:16]


def build_replay_manifest(
    *,
    spark: SparkSession,
    runs: DeltaPipelineRunRepository,
    manifests: DeltaReplayManifestRepository,
    repository_owner: str,
    repository_name: str,
    bronze_table: str,
    successful_run_ids: Sequence[str],
    cutoff_at: datetime,
    code_commit: str,
    transformation_version: str,
) -> ReplayManifest:
    """Freeze, verify and store one replay input. Nothing is stored on failure.

    The order is deliberate: every check that can fail runs before the single
    write at the end, so a rejected build leaves no manifest behind.
    """
    # 0. Normalize the inputs: the cutoff in UTC, and run IDs sorted so the
    #    same set of runs always gives the same manifest.
    if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
        raise ValueError("cutoff_at must be timezone-aware")
    cutoff_utc = cutoff_at.astimezone(UTC)
    run_ids = tuple(sorted(successful_run_ids))

    # 1. Which rows? Only runs that fully succeeded before the cutoff.
    require_succeeded_runs(runs, run_ids, cutoff_utc)

    # 2. As of when? The newest Bronze version at or before the cutoff.
    version = resolve_bronze_version(spark, bronze_table, cutoff_utc)

    # 3. Describe the input. Nothing is written yet.
    manifest = ReplayManifest(
        manifest_id=_manifest_id(
            repository_owner=repository_owner,
            repository_name=repository_name,
            bronze_table=bronze_table,
            bronze_version=version,
            successful_run_ids=run_ids,
            cutoff_at=cutoff_utc,
            code_commit=code_commit,
            transformation_version=transformation_version,
        ),
        repository_owner=repository_owner,
        repository_name=repository_name,
        bronze_table=bronze_table,
        bronze_version=version,
        successful_run_ids=run_ids,
        cutoff_at=cutoff_utc,
        code_commit=code_commit,
        transformation_version=transformation_version,
    )

    # 4. Prove the input can still be read (the version may be vacuumed, or a
    #    run may have no rows), then 5. store the manifest. This is the only
    #    write in the whole function.
    read_manifest_input(spark, manifest)
    manifests.create(manifest)

    return manifest
