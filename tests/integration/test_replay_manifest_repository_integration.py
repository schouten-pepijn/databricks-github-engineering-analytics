"""Live Delta integration coverage for replay-manifest persistence."""

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.control.replay_manifest_repository import (
    DeltaReplayManifestRepository,
)

pytestmark = pytest.mark.integration


@pytest.fixture()
def manifest_id(integration_spark: SparkSession) -> Iterator[str]:
    """Yield a random manifest ID and delete only its rows afterwards."""
    config = PipelineConfig(catalog=os.environ["DATABRICKS_TEST_CATALOG"])
    repository = DeltaReplayManifestRepository(integration_spark, config)
    repository.ensure_table()

    value = f"pytest-{uuid4().hex}"

    try:
        yield value
    finally:
        # The random ID scopes cleanup to rows created by this test.
        DeltaTable.forName(integration_spark, config.replay_manifest_table).delete(
            condition=f"manifest_id = '{value}'"
        )


def _config() -> PipelineConfig:
    return PipelineConfig(catalog=os.environ["DATABRICKS_TEST_CATALOG"])


def _make_manifest(manifest_id: str) -> ReplayManifest:
    return ReplayManifest(
        manifest_id=manifest_id,
        repository_owner="psf",
        repository_name="requests",
        bronze_table=_config().bronze_issues_table,
        bronze_version=42,
        successful_run_ids=("run-001", "run-002"),
        # Non-UTC input: the manifest normalizes it to 10:00 UTC.
        cutoff_at=datetime(2026, 10, 6, 12, 0, tzinfo=timezone(timedelta(hours=2))),
        code_commit="787ea43",
        transformation_version="v1",
    )


def _count_rows(spark: SparkSession, manifest_id: str) -> int:
    return (
        spark.table(_config().replay_manifest_table)
        .where(F.col("manifest_id") == manifest_id)
        .count()
    )


def test_create_then_get_round_trips_manifest(
    integration_spark: SparkSession,
    manifest_id: str,
) -> None:
    """Tuple order and the UTC cutoff survive the trip through Delta."""
    repository = DeltaReplayManifestRepository(integration_spark, _config())
    manifest = _make_manifest(manifest_id)

    repository.create(manifest)

    stored = repository.get(manifest_id)
    assert stored == manifest
    assert stored.successful_run_ids == ("run-001", "run-002")
    assert stored.cutoff_at == datetime(2026, 10, 6, 10, 0, tzinfo=UTC)


def test_create_same_manifest_twice_keeps_one_row(
    integration_spark: SparkSession,
    manifest_id: str,
) -> None:
    """An identical retry is a no-op and does not duplicate the row."""
    repository = DeltaReplayManifestRepository(integration_spark, _config())
    manifest = _make_manifest(manifest_id)

    repository.create(manifest)
    repository.create(manifest)

    assert _count_rows(integration_spark, manifest_id) == 1


def test_create_rejects_same_id_with_different_content(
    integration_spark: SparkSession,
    manifest_id: str,
) -> None:
    """Reusing an ID for another Bronze version fails and keeps the original."""
    repository = DeltaReplayManifestRepository(integration_spark, _config())
    original = _make_manifest(manifest_id)
    conflicting = replace(original, bronze_version=43)

    repository.create(original)

    with pytest.raises(ValueError, match="already exists with different content"):
        repository.create(conflicting)

    assert repository.get(manifest_id) == original
    assert _count_rows(integration_spark, manifest_id) == 1


def test_get_unknown_manifest_raises(
    integration_spark: SparkSession,
    manifest_id: str,
) -> None:
    """A manifest that was never created is reported as not found."""
    repository = DeltaReplayManifestRepository(integration_spark, _config())

    with pytest.raises(ValueError, match="Replay manifest not found"):
        repository.get(manifest_id)
