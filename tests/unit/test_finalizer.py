from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.control.pipeline_run import (
    PipelineRun,
    PipelineRunStatus,
)
from github_engineering_analytics.control.watermark import Watermark
from github_engineering_analytics.orchestration.finalizer import (
    finalize_failed_run,
    finalize_successful_run,
)


def make_running_run(
    candidate_watermark: Watermark | None = None,
) -> PipelineRun:
    """Build a pending GitHub Issues lifecycle row for finalizer unit tests."""
    run = PipelineRun(
        run_id="job-run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )
    return run.with_candidate_watermark(candidate_watermark)


def configure_repositories(mocker, run: PipelineRun) -> tuple[Mock, Mock]:
    """Patch finalizer control repositories around one persisted lifecycle row."""
    pipeline_runs = Mock()
    pipeline_runs.get.return_value = run
    watermarks = Mock()
    mocker.patch(
        "github_engineering_analytics.orchestration.finalizer.DeltaPipelineRunRepository",
        return_value=pipeline_runs,
    )
    mocker.patch(
        "github_engineering_analytics.orchestration.finalizer.DeltaWatermarkRepository",
        return_value=watermarks,
    )
    return pipeline_runs, watermarks


def test_finalize_successful_run_commits_candidate_then_completes_lifecycle(
    mocker,
) -> None:
    spark = Mock()
    candidate = Watermark(
        value=datetime(2026, 9, 20, 12, 3, tzinfo=UTC),
        overlap_seconds=300,
    )
    run = make_running_run(candidate)
    pipeline_runs, watermarks = configure_repositories(mocker, run)
    finalized_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    result = finalize_successful_run(
        spark=spark,
        catalog="test_catalog",
        run_id=run.run_id,
        clock=Mock(return_value=finalized_at),
    )

    pipeline_runs.ensure_table.assert_called_once_with()
    watermarks.ensure_table.assert_called_once_with()
    pipeline_runs.get.assert_called_once_with(run.run_id)
    watermarks.commit_success.assert_called_once_with(
        source_name="github",
        entity_name="issues",
        watermark_column="updated_at",
        watermark=candidate,
        run_id=run.run_id,
        committed_at=finalized_at,
    )
    finished_run = pipeline_runs.record_finished.call_args.args[0]
    assert result is finished_run
    assert result.status is PipelineRunStatus.SUCCEEDED
    assert result.finished_at == finalized_at


def test_finalize_successful_run_completes_empty_extraction_without_committing(
    mocker,
) -> None:
    spark = Mock()
    run = make_running_run()
    pipeline_runs, watermarks = configure_repositories(mocker, run)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    result = finalize_successful_run(
        spark=spark,
        catalog="test_catalog",
        run_id=run.run_id,
        clock=Mock(return_value=finished_at),
    )

    assert result.status is PipelineRunStatus.SUCCEEDED
    watermarks.commit_success.assert_not_called()
    assert pipeline_runs.record_finished.call_args.args[0].finished_at == finished_at


def test_finalize_successful_run_is_idempotent_for_completed_run(mocker) -> None:
    spark = Mock()
    succeeded_run = make_running_run().succeed(
        candidate_watermark=None,
        finished_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
    )
    pipeline_runs, watermarks = configure_repositories(mocker, succeeded_run)

    result = finalize_successful_run(
        spark=spark,
        catalog="test_catalog",
        run_id=succeeded_run.run_id,
    )

    assert result is succeeded_run
    watermarks.commit_success.assert_not_called()
    pipeline_runs.record_finished.assert_not_called()


def test_finalize_failed_run_marks_running_run_without_committing(mocker) -> None:
    spark = Mock()
    run = make_running_run()
    pipeline_runs, watermarks = configure_repositories(mocker, run)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    result = finalize_failed_run(
        spark=spark,
        catalog="test_catalog",
        run_id=run.run_id,
        failure_reason="Gold dbt build failed",
        clock=Mock(return_value=finished_at),
    )

    assert result.status is PipelineRunStatus.FAILED
    assert result.error_message == "Gold dbt build failed"
    assert result.finished_at == finished_at
    assert pipeline_runs.record_finished.call_args.args[0] is result
    watermarks.commit_success.assert_not_called()


def test_finalize_failed_run_is_idempotent_after_bronze_silver_failure(mocker) -> None:
    spark = Mock()
    failed_run = make_running_run().fail(
        error_message="Silver merge failed",
        finished_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
    )
    pipeline_runs, _ = configure_repositories(mocker, failed_run)

    result = finalize_failed_run(
        spark=spark,
        catalog="test_catalog",
        run_id=failed_run.run_id,
        failure_reason="Gold dbt build failed",
    )

    assert result is failed_run
    pipeline_runs.record_finished.assert_not_called()


def test_finalize_failed_run_rejects_blank_failure_reason(mocker) -> None:
    pipeline_runs, _ = configure_repositories(mocker, make_running_run())

    with pytest.raises(ValueError, match="failure_reason must not be empty"):
        finalize_failed_run(
            spark=Mock(),
            catalog="test_catalog",
            run_id="job-run-123",
            failure_reason=" ",
        )

    pipeline_runs.get.assert_not_called()
