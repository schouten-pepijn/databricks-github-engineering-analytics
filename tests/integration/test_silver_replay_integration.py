"""Live Spark coverage for replaying frozen Bronze input into all Silver entities."""

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.bronze.manifest_input import read_manifest_input
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.silver.replay import ReplaySilver, replay_silver

pytestmark = pytest.mark.integration

_BRONZE_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), nullable=False),
        StructField("repository_name", StringType(), nullable=False),
        StructField("issue_id", LongType(), nullable=False),
        StructField("source_updated_at", TimestampType(), nullable=False),
        StructField("raw_json", StringType(), nullable=False),
        StructField("_run_id", StringType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_page_or_batch_reference", StringType(), nullable=False),
    ]
)

_BUG = {
    "id": 301,
    "name": "bug",
    "color": "d73a4a",
    "description": "b",
    "default": True,
}
_DOCS = {
    "id": 302,
    "name": "docs",
    "color": "0075ca",
    "description": "d",
    "default": False,
}


def _issue_json(
    *,
    issue_id: int,
    number: int,
    title: str,
    state: str,
    user: tuple[int, str],
    labels: list[dict[str, Any]],
    updated_at: str,
    closed_at: str | None = None,
) -> str:
    """Build one GitHub-shaped Issue payload for the Bronze raw_json column."""
    return json.dumps(
        {
            "id": issue_id,
            "number": number,
            "title": title,
            "state": state,
            "user": {"id": user[0], "login": user[1], "type": "User"},
            "labels": labels,
            "created_at": "2026-09-20T10:00:00Z",
            "updated_at": updated_at,
            "closed_at": closed_at,
        }
    )


def _bronze_row(
    issue_id: int, raw_json: str, run_id: str, day: int, hour: int
) -> tuple[Any, ...]:
    """One Bronze row, observed on the given day and ingested a minute later."""
    observed = datetime(2026, 9, day, hour, 0, tzinfo=UTC)
    ingested = datetime(2026, 9, day, hour, 1, tzinfo=UTC)
    return (
        "pytest",
        "replay",
        issue_id,
        observed,
        raw_json,
        run_id,
        ingested,
        "page-1",
    )


# Baseline run: Issue 1001 has two labels, Issue 1002 has none.
_BASELINE_ROWS = [
    _bronze_row(
        1001,
        _issue_json(
            issue_id=1001,
            number=42,
            title="Replay lesson",
            state="open",
            user=(2001, "alice"),
            labels=[_BUG, _DOCS],
            updated_at="2026-09-20T11:00:00Z",
        ),
        "fixture-run-1",
        20,
        11,
    ),
    _bronze_row(
        1002,
        _issue_json(
            issue_id=1002,
            number=43,
            title="No labels yet",
            state="open",
            user=(2002, "bob"),
            labels=[],
            updated_at="2026-09-20T11:00:00Z",
        ),
        "fixture-run-1",
        20,
        11,
    ),
]

# Later run: Issue 1001 is edited, closed, and loses its "docs" label.
_LATER_ROWS = [
    _bronze_row(
        1001,
        _issue_json(
            issue_id=1001,
            number=42,
            title="Replay lesson (edited)",
            state="closed",
            user=(2001, "alice"),
            labels=[_BUG],
            updated_at="2026-09-21T09:00:00Z",
            closed_at="2026-09-21T09:00:00Z",
        ),
        "fixture-run-2",
        21,
        9,
    ),
]


@dataclass(frozen=True)
class FixtureBronze:
    """A scratch Bronze table and the Delta versions after each fixture run."""

    table: str
    baseline_version: int
    later_version: int


def _latest_version(spark: SparkSession, table: str) -> int:
    return int(DeltaTable.forName(spark, table).history(1).collect()[0]["version"])


@pytest.fixture()
def fixture_bronze(integration_spark: SparkSession) -> Iterator[FixtureBronze]:
    """Create a scratch Bronze table with two runs, and drop it afterwards."""
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    table = f"{catalog}.github_analytics_bronze.pytest_replay_silver_{uuid4().hex[:10]}"
    quoted = quote_multipart_identifier(table)
    integration_spark.sql(
        "CREATE SCHEMA IF NOT EXISTS "
        + quote_multipart_identifier(f"{catalog}.github_analytics_bronze")
    )

    try:
        baseline = integration_spark.createDataFrame(_BASELINE_ROWS, _BRONZE_SCHEMA)
        baseline.write.format("delta").mode("error").saveAsTable(quoted)
        baseline_version = _latest_version(integration_spark, table)

        later = integration_spark.createDataFrame(_LATER_ROWS, _BRONZE_SCHEMA)
        later.write.format("delta").mode("append").saveAsTable(quoted)

        yield FixtureBronze(
            table, baseline_version, _latest_version(integration_spark, table)
        )
    finally:
        integration_spark.sql(f"DROP TABLE IF EXISTS {quoted}")


def _replay(
    spark: SparkSession,
    fixture: FixtureBronze,
    version: int,
    run_ids: tuple[str, ...],
) -> ReplaySilver:
    """Freeze the fixture at one version and replay it through every Silver entity."""
    manifest = ReplayManifest(
        manifest_id=f"fixture-{version}",
        repository_owner="pytest",
        repository_name="replay",
        bronze_table=fixture.table,
        bronze_version=version,
        successful_run_ids=run_ids,
        cutoff_at=datetime(2026, 9, 22, tzinfo=UTC),
        code_commit="fixture",
        transformation_version="v1",
    )
    return replay_silver(read_manifest_input(spark, manifest))


def _rows(frame: DataFrame, *columns: str) -> set[tuple[Any, ...]]:
    """Collect selected non-timestamp columns as comparable tuples."""
    selected = frame.select(*columns).collect()
    return {tuple(row[column] for column in columns) for row in selected}


def test_baseline_replay_builds_all_entities(
    integration_spark: SparkSession, fixture_bronze: FixtureBronze
) -> None:
    result = _replay(
        integration_spark,
        fixture_bronze,
        fixture_bronze.baseline_version,
        ("fixture-run-1",),
    )

    assert _rows(result.issues, "issue_id", "title", "state", "source_run_id") == {
        (1001, "Replay lesson", "open", "fixture-run-1"),
        (1002, "No labels yet", "open", "fixture-run-1"),
    }
    assert _rows(result.users, "user_id", "login") == {(2001, "alice"), (2002, "bob")}
    assert _rows(result.labels, "label_id", "name") == {(301, "bug"), (302, "docs")}
    assert _rows(result.issue_labels, "issue_id", "label_id") == {
        (1001, 301),
        (1001, 302),
    }


def test_later_observation_updates_issue_and_removes_label_relation(
    integration_spark: SparkSession, fixture_bronze: FixtureBronze
) -> None:
    result = _replay(
        integration_spark,
        fixture_bronze,
        fixture_bronze.later_version,
        ("fixture-run-1", "fixture-run-2"),
    )

    # The edited Issue replaces the baseline; the untouched one keeps its lineage.
    assert _rows(result.issues, "issue_id", "title", "state", "source_run_id") == {
        (1001, "Replay lesson (edited)", "closed", "fixture-run-2"),
        (1002, "No labels yet", "open", "fixture-run-1"),
    }
    assert _rows(result.users, "user_id", "source_run_id") == {
        (2001, "fixture-run-2"),
        (2002, "fixture-run-1"),
    }
    # The "docs" label definition persists, but Issue 1001 no longer uses it.
    assert _rows(result.labels, "label_id", "name") == {(301, "bug"), (302, "docs")}
    # The bridge keeps only bug. Issue 1002 has an empty label set, so no rows.
    assert _rows(result.issue_labels, "issue_id", "label_id") == {(1001, 301)}


def test_replaying_the_same_manifest_twice_gives_the_same_rows(
    integration_spark: SparkSession, fixture_bronze: FixtureBronze
) -> None:
    runs = ("fixture-run-1", "fixture-run-2")
    version = fixture_bronze.later_version
    first = _replay(integration_spark, fixture_bronze, version, runs)
    second = _replay(integration_spark, fixture_bronze, version, runs)

    for name in ("issues", "users", "labels", "issue_labels"):
        left, right = getattr(first, name), getattr(second, name)
        assert left.exceptAll(right).count() == 0, name
        assert right.exceptAll(left).count() == 0, name
