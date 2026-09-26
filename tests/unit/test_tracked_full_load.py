from datetime import UTC, datetime
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from github_engineering_analytics.bronze.full_load import run_tracked_full_load
from github_engineering_analytics.bronze.ingestion import BronzeIngestionResult
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRunStatus
from github_engineering_analytics.control.watermark import Watermark


def test_run_tracked_full_load_records_silver_failure_and_reraises(
    mocker,
) -> None:
    spark = Mock()
    repository = Mock()
    result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
    )
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)
    failure = RuntimeError("Silver merge failed")
    expected_run_id = "12345678123456781234567812345678"

    mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=repository,
    )
    bronze_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
        return_value=result,
    )
    silver_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_bronze_to_silver",
        side_effect=failure,
    )

    with pytest.raises(RuntimeError, match="Silver merge failed") as exc_info:
        run_tracked_full_load(
            spark=spark,
            catalog="test_catalog",
            owner="psf",
            repository="requests",
            run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
            clock=Mock(side_effect=[started_at, finished_at]),
        )

    assert exc_info.value is failure

    bronze_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        github_token=None,
        run_id=expected_run_id,
        ingested_at=started_at,
    )
    silver_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        bronze_run_id=expected_run_id,
    )

    started_run = repository.record_started.call_args.args[0]
    failed_run = repository.record_finished.call_args.args[0]

    assert started_run.status is PipelineRunStatus.RUNNING
    assert failed_run.run_id == started_run.run_id
    assert failed_run.status is PipelineRunStatus.FAILED
    assert failed_run.finished_at == finished_at
    assert failed_run.error_message == "Silver merge failed"


def test_run_tracked_full_load_records_a_successful_lifecycle(mocker) -> None:
    spark = Mock()
    repository = Mock()
    result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
        candidate_watermark=Watermark(
            value=datetime(2026, 9, 20, 12, 3, tzinfo=UTC),
            overlap_seconds=300,
        ),
    )
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)
    expected_run_id = "12345678123456781234567812345678"

    repository_constructor = mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=repository,
    )
    bronze_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
        return_value=result,
    )
    silver_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_bronze_to_silver",
        create=True,
    )

    # Verzamel beide aanroepen zodat de test ook hun volgorde controleert.
    pipeline_calls = Mock()
    pipeline_calls.attach_mock(bronze_load, "bronze")
    pipeline_calls.attach_mock(silver_load, "silver")

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

    assert started_run.run_id == expected_run_id
    assert started_run.source_name == "github"
    assert started_run.entity_name == "issues"
    assert started_run.status is PipelineRunStatus.RUNNING
    assert started_run.started_at == started_at
    assert started_run.watermark_before is None

    assert finished_run.run_id == started_run.run_id
    assert finished_run.status is PipelineRunStatus.SUCCEEDED
    assert finished_run.finished_at == finished_at
    assert finished_run.candidate_watermark == Watermark(
        value=datetime(2026, 9, 20, 12, 3, tzinfo=UTC),
        overlap_seconds=300,
    )

    assert pipeline_calls.mock_calls == [
        call.bronze(
            spark=spark,
            catalog="test_catalog",
            owner="psf",
            repository="requests",
            github_token="test-token",
            run_id=expected_run_id,
            ingested_at=started_at,
        ),
        call.silver(
            spark=spark,
            catalog="test_catalog",
            bronze_run_id=expected_run_id,
        ),
    ]


def test_run_tracked_full_load_records_failure_and_reraises(mocker) -> None:
    spark = Mock()
    repository = Mock()
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)
    failure = RuntimeError("GitHub request timed out")

    mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=repository,
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
        side_effect=failure,
    )

    with pytest.raises(RuntimeError, match="GitHub request timed out"):
        run_tracked_full_load(
            spark=spark,
            catalog="test_catalog",
            owner="psf",
            repository="requests",
            run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
            clock=Mock(side_effect=[started_at, finished_at]),
        )

    failed_run = repository.record_finished.call_args.args[0]
    assert failed_run.status is PipelineRunStatus.FAILED
    assert failed_run.finished_at == finished_at
    assert failed_run.error_message == "GitHub request timed out"


def test_run_tracked_full_load_records_no_candidate_for_empty_extraction(
    mocker,
) -> None:
    spark = Mock()
    repository = Mock()
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)

    mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=repository,
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
        return_value=BronzeIngestionResult(
            records_extracted=0,
            batches_written=0,
            candidate_watermark=None,
        ),
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_bronze_to_silver",
    )

    run_tracked_full_load(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
        clock=Mock(side_effect=[started_at, finished_at]),
    )

    finished_run = repository.record_finished.call_args.args[0]

    assert finished_run.status is PipelineRunStatus.SUCCEEDED
    assert finished_run.candidate_watermark is None


def test_run_tracked_full_load_uses_incremental_load_when_watermark_exists(
    mocker,
) -> None:
    spark = Mock()
    pipeline_run_repository = Mock()
    watermark_repository = Mock()
    started_at = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    finished_at = datetime(2026, 9, 20, 12, 5, tzinfo=UTC)
    expected_run_id = "12345678123456781234567812345678"

    stored_watermark = Watermark(
        value=datetime(2026, 9, 20, 11, 55, tzinfo=UTC),
        overlap_seconds=300,
    )
    result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
        candidate_watermark=Watermark(
            value=datetime(2026, 9, 20, 12, 3, tzinfo=UTC),
            overlap_seconds=300,
        ),
    )

    mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaPipelineRunRepository",
        return_value=pipeline_run_repository,
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaWatermarkRepository",
        return_value=watermark_repository,
    )
    watermark_repository.get.return_value = stored_watermark

    full_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_full_load",
    )
    incremental_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_incremental_load",
        return_value=result,
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_bronze_to_silver",
    )

    run_tracked_full_load(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        github_token="test-token",
        run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
        clock=Mock(side_effect=[started_at, finished_at]),
    )

    watermark_repository.get.assert_called_once_with("github", "issues")
    full_load.assert_not_called()
    incremental_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        github_token="test-token",
        run_id=expected_run_id,
        ingested_at=started_at,
        watermark=stored_watermark,
    )

    started_run = pipeline_run_repository.record_started.call_args.args[0]
    assert started_run.watermark_before == stored_watermark
