from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.bronze.issues import (
    BronzeIssueRecord,
    DeltaBronzeIssueWriter,
)
from github_engineering_analytics.common.config import PipelineConfig


def test_bronze_issue_record_preserves_github_payload() -> None:
    payload = {
        "id": 123,
        "number": 42,
        "title": "Fix pagination",
        "updated_at": "2026-09-20T12:03:00Z",
        "pull_request": {"url": "https://api.github.com/example"},
    }

    record = BronzeIssueRecord.from_github_payload(
        repository_owner="delta-io",
        repository_name="delta",
        payload=payload,
        run_id="run-123",
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        request_watermark=datetime(2026, 9, 20, 11, 55, tzinfo=UTC),
        page_or_batch_reference="page-1",
    )

    assert record.repository_owner == "delta-io"
    assert record.repository_name == "delta"
    assert record.issue_id == 123
    assert record.source_updated_at == datetime(2026, 9, 20, 12, 3, tzinfo=UTC)
    assert record.run_id == "run-123"
    assert record.raw_json == (
        '{"id":123,"number":42,"pull_request":'
        '{"url":"https://api.github.com/example"},'
        '"title":"Fix pagination","updated_at":"2026-09-20T12:03:00Z"}'
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"updated_at": "2026-09-20T12:03:00Z"},
        {"id": 0, "updated_at": "2026-09-20T12:03:00Z"},
        {"id": True, "updated_at": "2026-09-20T12:03:00Z"},
        {"id": 123, "updated_at": "not-a-timestamp"},
    ],
)
def test_bronze_issue_record_rejects_invalid_required_payload_fields(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        BronzeIssueRecord.from_github_payload(
            repository_owner="delta-io",
            repository_name="delta",
            payload=payload,
            run_id="run-123",
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
            request_watermark=None,
            page_or_batch_reference="page-1",
        )


@pytest.mark.parametrize("timestamp_field", ["ingested_at", "request_watermark"])
def test_bronze_issue_record_rejects_naive_pipeline_timestamps(
    timestamp_field: str,
) -> None:
    arguments: dict[str, object] = {
        "repository_owner": "delta-io",
        "repository_name": "delta",
        "payload": {"id": 123, "updated_at": "2026-09-20T12:03:00Z"},
        "run_id": "run-123",
        "ingested_at": datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        "request_watermark": datetime(2026, 9, 20, 11, 55, tzinfo=UTC),
        "page_or_batch_reference": "page-1",
    }
    arguments[timestamp_field] = datetime(2026, 9, 20, 12, 0)

    with pytest.raises(ValueError, match=f"{timestamp_field} must be timezone-aware"):
        BronzeIssueRecord.from_github_payload(**arguments)


def test_bronze_issue_record_serializes_equivalent_payloads_deterministically() -> None:
    first_payload = {
        "id": 123,
        "title": "Fix pagination",
        "updated_at": "2026-09-20T12:03:00Z",
    }
    reordered_payload = {
        "updated_at": "2026-09-20T12:03:00Z",
        "title": "Fix pagination",
        "id": 123,
    }

    common_arguments = {
        "repository_owner": "delta-io",
        "repository_name": "delta",
        "run_id": "run-123",
        "ingested_at": datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        "request_watermark": None,
        "page_or_batch_reference": "page-1",
    }

    first_record = BronzeIssueRecord.from_github_payload(
        payload=first_payload,
        **common_arguments,
    )
    reordered_record = BronzeIssueRecord.from_github_payload(
        payload=reordered_payload,
        **common_arguments,
    )

    assert first_record.raw_json == reordered_record.raw_json


def test_bronze_issue_record_rejects_non_json_serializable_payload() -> None:
    payload = {
        "id": 123,
        "updated_at": "2026-09-20T12:03:00Z",
        "unsupported": object(),
    }

    with pytest.raises(ValueError, match="payload must be JSON-serializable"):
        BronzeIssueRecord.from_github_payload(
            repository_owner="delta-io",
            repository_name="delta",
            payload=payload,
            run_id="run-123",
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
            request_watermark=None,
            page_or_batch_reference="page-1",
        )


def test_delta_bronze_issue_writer_does_not_write_empty_input() -> None:
    spark = Mock()
    writer = DeltaBronzeIssueWriter(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    writer.append([])

    spark.conf.get.assert_not_called()
    spark.createDataFrame.assert_not_called()
