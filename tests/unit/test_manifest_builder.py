from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
)
from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.orchestration.manifest_builder import (
    build_replay_manifest,
    resolve_bronze_version,
)

MODULE = "github_engineering_analytics.orchestration.manifest_builder"
CUTOFF = datetime(2026, 10, 6, 10, 9, tzinfo=UTC)


def configure_history(
    delta_table: Mock, functions: Mock, rows: list[dict[str, Any]]
) -> None:
    """Mock DeltaTable(...).history().where().orderBy().select().limit().collect()."""
    # A patched F returns MagicMocks; `<=` needs an explicit result or it raises.
    functions.col.return_value.__le__.return_value = Mock()
    history = delta_table.forName.return_value.history.return_value
    query = history.where.return_value.orderBy.return_value.select.return_value.limit
    query.return_value.collect.return_value = rows


def make_run(
    run_id: str,
    *,
    succeeded: bool = True,
    finished_at=None,
) -> PipelineRun:
    running = PipelineRun(
        run_id=run_id,
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 10, 6, 8, 0, tzinfo=UTC),
        watermark_before=None,
    )
    if not succeeded:
        return running

    return running.succeed(
        candidate_watermark=None,
        finished_at=finished_at or datetime(2026, 10, 6, 9, 0, tzinfo=UTC),
    )


def make_runs(*runs: PipelineRun) -> Mock:
    repository = Mock()
    # __getitem__ makes get("run-001") look the run up by ID. Passing the dict
    # itself would make Mock iterate over its keys and return strings.
    repository.get.side_effect = {run.run_id: run for run in runs}.__getitem__
    return repository


def build(runs: Mock, manifests: Mock, **overrides: Any) -> ReplayManifest:
    # Any: the values have different types, so a plain dict(...) would be
    # inferred as one big union that no single parameter accepts.
    arguments: dict[str, Any] = {
        "spark": Mock(),
        "runs": runs,
        "manifests": manifests,
        "repository_owner": "psf",
        "repository_name": "requests",
        "bronze_table": "test_catalog.bronze.github_issues_raw",
        "successful_run_ids": ["run-002", "run-001"],
        "cutoff_at": CUTOFF,
        "code_commit": "787ea43",
        "transformation_version": "v1",
    }
    arguments.update(overrides)
    return build_replay_manifest(**arguments)


@patch(f"{MODULE}.F")
@patch(f"{MODULE}.DeltaTable")
def test_resolve_returns_latest_version_at_or_before_cutoff(delta_table, functions):
    configure_history(delta_table, functions, [{"version": 7}])

    assert resolve_bronze_version(Mock(), "t.b.raw", CUTOFF) == 7


@patch(f"{MODULE}.F")
@patch(f"{MODULE}.DeltaTable")
def test_resolve_fails_when_history_does_not_reach_cutoff(delta_table, functions):
    configure_history(delta_table, functions, [])

    with pytest.raises(ManifestInputUnavailableError, match="no Delta commit"):
        resolve_bronze_version(Mock(), "t.b.raw", CUTOFF)


@patch(f"{MODULE}.resolve_bronze_version")
def test_rejects_run_that_did_not_succeed(resolve: Mock) -> None:
    runs = make_runs(make_run("run-001"), make_run("run-002", succeeded=False))
    manifests = Mock()

    with pytest.raises(ValueError, match="run-002.*not succeeded"):
        build(runs, manifests)

    resolve.assert_not_called()
    manifests.create.assert_not_called()


@patch(f"{MODULE}.resolve_bronze_version")
def test_rejects_run_finished_after_cutoff(resolve: Mock) -> None:
    late = datetime(2026, 10, 6, 11, 0, tzinfo=UTC)
    runs = make_runs(make_run("run-001"), make_run("run-002", finished_at=late))
    manifests = Mock()

    with pytest.raises(ValueError, match="did not finish before the cutoff"):
        build(runs, manifests)

    resolve.assert_not_called()
    manifests.create.assert_not_called()


def test_rejects_naive_cutoff() -> None:
    manifests = Mock()

    with pytest.raises(ValueError, match="cutoff_at must be timezone-aware"):
        build(Mock(), manifests, cutoff_at=datetime(2026, 10, 6, 10, 0))

    manifests.create.assert_not_called()


@patch(f"{MODULE}.read_manifest_input")
@patch(f"{MODULE}.resolve_bronze_version", return_value=7)
def test_creates_manifest_after_input_is_proven_readable(resolve, read) -> None:
    runs = make_runs(make_run("run-001"), make_run("run-002"))
    manifests = Mock()

    manifest = build(runs, manifests)

    assert manifest.bronze_version == 7
    assert manifest.successful_run_ids == ("run-001", "run-002")  # stored sorted
    read.assert_called_once()
    manifests.create.assert_called_once_with(manifest)


@patch(
    f"{MODULE}.read_manifest_input",
    side_effect=ManifestInputUnavailableError("gone"),
)
@patch(f"{MODULE}.resolve_bronze_version", return_value=7)
def test_does_not_create_manifest_when_input_is_unreadable(resolve, read) -> None:
    runs = make_runs(make_run("run-001"), make_run("run-002"))
    manifests = Mock()

    with pytest.raises(ManifestInputUnavailableError):
        build(runs, manifests)

    manifests.create.assert_not_called()


@patch(f"{MODULE}.read_manifest_input")
@patch(f"{MODULE}.resolve_bronze_version", return_value=7)
def test_same_input_produces_the_same_manifest_id(resolve, read) -> None:
    runs = make_runs(make_run("run-001"), make_run("run-002"))

    first = build(runs, Mock())
    reordered = build(runs, Mock(), successful_run_ids=["run-001", "run-002"])
    other_code = build(runs, Mock(), code_commit="abc1234")

    assert first.manifest_id == reordered.manifest_id
    assert first.manifest_id != other_code.manifest_id
