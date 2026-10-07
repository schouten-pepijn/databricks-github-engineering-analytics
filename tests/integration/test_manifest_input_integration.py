"""Live Delta time-travel coverage for manifest input reads."""

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
    read_manifest_input,
)
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.orchestration.manifest_builder import (
    resolve_bronze_version,
)

pytestmark = pytest.mark.integration


@pytest.fixture()
def bronze_table(integration_spark: SparkSession) -> Iterator[str]:
    """Create a scratch table with the columns the reader needs, then drop it."""
    config = PipelineConfig(catalog=os.environ["DATABRICKS_TEST_CATALOG"])
    name = f"{config.catalog}.{config.bronze_schema}.pytest_manifest_{uuid4().hex}"
    schema = quote_multipart_identifier(f"{config.catalog}.{config.bronze_schema}")

    integration_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    integration_spark.sql(
        f"CREATE TABLE {quote_multipart_identifier(name)} ("
        "repository_owner STRING, repository_name STRING, "
        "_run_id STRING, issue_id BIGINT) USING DELTA"
    )

    try:
        yield name
    finally:
        integration_spark.sql(
            f"DROP TABLE IF EXISTS {quote_multipart_identifier(name)}"
        )


def append_run(
    spark: SparkSession, table: str, run_id: str, issue_ids: list[int]
) -> None:
    values = ", ".join(f"('psf', 'requests', '{run_id}', {i})" for i in issue_ids)
    spark.sql(f"INSERT INTO {quote_multipart_identifier(table)} VALUES {values}")


def latest_version(spark: SparkSession, table: str) -> int:
    return DeltaTable.forName(spark, table).history(1).collect()[0]["version"]


def make_manifest(table: str, version: int, run_ids: tuple[str, ...]) -> ReplayManifest:
    return ReplayManifest(
        manifest_id=f"pytest-{uuid4().hex}",
        repository_owner="psf",
        repository_name="requests",
        bronze_table=table,
        bronze_version=version,
        successful_run_ids=run_ids,
        cutoff_at=datetime(2026, 10, 6, 10, 0, tzinfo=UTC),
        code_commit="787ea43",
        transformation_version="v1",
    )


def test_later_append_does_not_change_manifest_input(
    integration_spark: SparkSession, bronze_table: str
) -> None:
    """The sprint's core proof: same manifest, same rows, even after appends."""
    append_run(integration_spark, bronze_table, "run-001", [1, 2])
    append_run(integration_spark, bronze_table, "run-002", [3])
    manifest = make_manifest(
        bronze_table,
        latest_version(integration_spark, bronze_table),
        ("run-001", "run-002"),
    )

    before = read_manifest_input(integration_spark, manifest).count()

    # A new run AND a late extra row for a selected run.
    append_run(integration_spark, bronze_table, "run-003", [4])
    append_run(integration_spark, bronze_table, "run-001", [5])

    after = read_manifest_input(integration_spark, manifest)

    assert before == 3
    assert after.count() == 3
    assert {r["_run_id"] for r in after.collect()} == {"run-001", "run-002"}


def test_missing_bronze_version_fails_explicitly(
    integration_spark: SparkSession, bronze_table: str
) -> None:
    append_run(integration_spark, bronze_table, "run-001", [1])
    valid = make_manifest(
        bronze_table,
        latest_version(integration_spark, bronze_table),
        ("run-001",),
    )
    # Control: the same manifest reads fine, so only the version differs below.
    assert read_manifest_input(integration_spark, valid).count() == 1

    missing_version = replace(valid, bronze_version=999_999)

    with pytest.raises(ManifestInputUnavailableError, match="version 999999"):
        read_manifest_input(integration_spark, missing_version)


def test_run_without_rows_at_version_fails_explicitly(
    integration_spark: SparkSession, bronze_table: str
) -> None:
    append_run(integration_spark, bronze_table, "run-001", [1])
    manifest = make_manifest(
        bronze_table,
        latest_version(integration_spark, bronze_table),
        ("run-001", "run-404"),
    )

    with pytest.raises(ManifestInputUnavailableError, match="run-404"):
        read_manifest_input(integration_spark, manifest)


def commit_time(spark: SparkSession, table: str, version: int) -> datetime:
    """Commit time of one Delta version as an aware UTC datetime.

    Read as epoch microseconds, not as a Python datetime, because Databricks
    Connect can return a datetime in the client's local time zone.
    """
    row = (
        DeltaTable.forName(spark, table)
        .history()
        .where(F.col("version") == version)
        .select(F.unix_micros("timestamp").alias("micros"))
        .collect()[0]
    )
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=row["micros"])


def test_resolve_bronze_version_follows_commit_times(
    integration_spark: SparkSession, bronze_table: str
) -> None:
    append_run(integration_spark, bronze_table, "run-001", [1])
    v1 = latest_version(integration_spark, bronze_table)
    append_run(integration_spark, bronze_table, "run-002", [2])
    v2 = latest_version(integration_spark, bronze_table)

    at_v1 = commit_time(integration_spark, bronze_table, v1)
    at_v2 = commit_time(integration_spark, bronze_table, v2)

    # The comparison is inclusive: a cutoff exactly at a commit picks that commit.
    assert resolve_bronze_version(integration_spark, bronze_table, at_v1) == v1
    assert resolve_bronze_version(integration_spark, bronze_table, at_v2) == v2
    # Just before the second commit, the first one is still the newest.
    just_before_v2 = at_v2 - timedelta(milliseconds=1)
    assert resolve_bronze_version(integration_spark, bronze_table, just_before_v2) == v1

    # Before the table existed, no commit qualifies.
    before_everything = commit_time(integration_spark, bronze_table, 0) - timedelta(
        seconds=1
    )
    with pytest.raises(ManifestInputUnavailableError, match="no Delta commit"):
        resolve_bronze_version(integration_spark, bronze_table, before_everything)
