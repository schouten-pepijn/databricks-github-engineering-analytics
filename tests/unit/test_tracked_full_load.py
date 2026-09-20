from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID

from github_engineering_analytics.bronze.full_load import run_tracked_full_load
from github_engineering_analytics.bronze.ingestion import BronzeIngestionResult
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRunStatus


def test_run_tracked_full_load_records_a_successful_lifecycle(mocker) -> None:
    spark = Mock()
    repository = Mock()
    result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
    )
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    repository_constructor = mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=repository,
    )
    full_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
        return_value=result,
    )

    actual_result = run_tracked_full_load(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        github_token="test-token",
        run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
        clock=Mock(side_effect=[started_at, finished_at]),
    )

    assert actual_result is result
    repository_constructor.assert_called_once_with(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )
    repository.ensure_table.assert_called_once_with()

    started_run = repository.record_started.call_args.args[0]
    finished_run = repository.record_finished.call_args.args[0]

    assert started_run.run_id == "12345678123456781234567812345678"
    assert started_run.source_name == "github"
    assert started_run.entity_name == "issues"
    assert started_run.status is PipelineRunStatus.RUNNING
    assert started_run.started_at == started_at
    assert started_run.watermark_before is None

    assert finished_run.run_id == started_run.run_id
    assert finished_run.status is PipelineRunStatus.SUCCEEDED
    assert finished_run.finished_at == finished_at
    assert finished_run.candidate_watermark is None

    full_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        github_token="test-token",
        run_id="12345678123456781234567812345678",
        ingested_at=started_at,
    )
