from datetime import UTC, datetime, timedelta, timezone

import pytest

from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.watermark import Watermark


def test_pipeline_run_uses_watermark_overlap() -> None:
    watermark_before = Watermark(
        value=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        overlap_seconds=300,
    )

    pipeline_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 30, tzinfo=UTC),
        watermark_before=watermark_before,
    )

    assert pipeline_run.extraction_start == datetime(
        2026,
        9,
        19,
        11,
        55,
        tzinfo=UTC,
    )


def test_initial_pipeline_run_has_no_extraction_start() -> None:
    pipeline_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 30, tzinfo=UTC),
        watermark_before=None,
    )

    assert pipeline_run.extraction_start is None


def test_pipeline_run_normalizes_started_at_to_utc() -> None:
    pipeline_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(
            2026,
            9,
            19,
            14,
            30,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        watermark_before=None,
    )

    assert pipeline_run.started_at == datetime(
        2026,
        9,
        19,
        12,
        30,
        tzinfo=UTC,
    )


def test_pipeline_run_rejects_naive_started_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        PipelineRun(
            run_id="run-123",
            source_name="github",
            entity_name="issues",
            started_at=datetime(2026, 9, 19, 12, 30),
            watermark_before=None,
        )


def test_pipeline_run_rejects_empty_identifiers() -> None:
    with pytest.raises(ValueError, match="run_id"):
        PipelineRun(
            run_id="",
            source_name="github",
            entity_name="issues",
            started_at=datetime(2026, 9, 19, 12, 30, tzinfo=UTC),
            watermark_before=None,
        )


def test_pipeline_run_rejects_candidate_before_previous_watermark() -> None:
    watermark_before = Watermark(
        value=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
    )
    candidate_watermark = Watermark(
        value=datetime(2026, 9, 19, 11, 59, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="must not be earlier"):
        PipelineRun(
            run_id="run-123",
            source_name="github",
            entity_name="issues",
            started_at=datetime(2026, 9, 19, 12, 30, tzinfo=UTC),
            watermark_before=watermark_before,
            candidate_watermark=candidate_watermark,
        )
