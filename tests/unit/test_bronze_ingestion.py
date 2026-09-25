from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.bronze.ingestion import GitHubIssueBronzeIngestion
from github_engineering_analytics.control.watermark import Watermark


def test_incremental_load_uses_overlap_adjusted_watermark() -> None:
    client = Mock()
    client.iter_issues.return_value = iter(
        [{"id": 123, "updated_at": "2026-09-20T12:03:00Z"}]
    )
    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    watermark = Watermark(
        value=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
        overlap_seconds=300,
    )
    expected_since = datetime(2026, 9, 20, 11, 55, tzinfo=UTC)

    result = ingestion.incremental_load(
        owner="delta-io",
        repository="delta",
        run_id="run-456",
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        watermark=watermark,
    )

    assert result.records_extracted == 1
    client.iter_issues.assert_called_once_with(
        owner="delta-io",
        repository="delta",
        since=expected_since,
    )

    written_records = writer.append.call_args.args[0]
    assert written_records[0].request_watermark == expected_since


def test_full_load_returns_zero_counts_without_writing_records() -> None:
    client = Mock()
    client.iter_issues.return_value = iter(())

    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(
        client=client,
        writer=writer,
    )

    result = ingestion.full_load(
        owner="delta-io",
        repository="delta",
        run_id="run-123",
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
    )

    assert result.records_extracted == 0
    assert result.batches_written == 0
    client.iter_issues.assert_called_once_with(
        owner="delta-io",
        repository="delta",
        since=None,
    )
    writer.ensure_table.assert_called_once()
    writer.append.assert_not_called()


def test_full_load_maps_and_appends_one_batch() -> None:
    client = Mock()
    client.iter_issues.return_value = iter(
        [
            {
                "id": 123,
                "title": "Fix pagination",
                "updated_at": "2026-09-20T12:03:00Z",
            }
        ]
    )

    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(
        client=client,
        writer=writer,
    )

    result = ingestion.full_load(
        owner="delta-io",
        repository="delta",
        run_id="run-123",
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
    )

    assert result.records_extracted == 1
    assert result.batches_written == 1
    writer.ensure_table.assert_called_once()

    written_records = writer.append.call_args.args[0]
    assert len(written_records) == 1
    assert written_records[0].issue_id == 123
    assert written_records[0].repository_owner == "delta-io"
    assert written_records[0].repository_name == "delta"
    assert written_records[0].run_id == "run-123"
    assert written_records[0].page_or_batch_reference == "batch-1"


def test_full_load_writes_multiple_bounded_batches() -> None:
    client = Mock()
    client.iter_issues.return_value = iter(
        [
            {"id": 1, "updated_at": "2026-09-20T12:01:00Z"},
            {"id": 2, "updated_at": "2026-09-20T12:02:00Z"},
            {"id": 3, "updated_at": "2026-09-20T12:03:00Z"},
        ]
    )
    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    result = ingestion.full_load(
        owner="delta-io",
        repository="delta",
        run_id="run-123",
        ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        batch_size=2,
    )

    assert result.records_extracted == 3
    assert result.batches_written == 2
    assert writer.append.call_count == 2

    first_batch = writer.append.call_args_list[0].args[0]
    second_batch = writer.append.call_args_list[1].args[0]

    assert [record.issue_id for record in first_batch] == [1, 2]
    assert [record.issue_id for record in second_batch] == [3]
    assert {record.page_or_batch_reference for record in first_batch} == {"batch-1"}
    assert {record.page_or_batch_reference for record in second_batch} == {"batch-2"}


def test_full_load_rejects_non_positive_batch_size_before_side_effects() -> None:
    client = Mock()
    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    with pytest.raises(ValueError, match="batch_size must be positive"):
        ingestion.full_load(
            owner="delta-io",
            repository="delta",
            run_id="run-123",
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
            batch_size=0,
        )

    client.iter_issues.assert_not_called()
    writer.ensure_table.assert_not_called()


def test_full_load_propagates_client_failure() -> None:
    client = Mock()
    client.iter_issues.side_effect = RuntimeError("GitHub is unavailable")
    writer = Mock()
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    with pytest.raises(RuntimeError, match="GitHub is unavailable"):
        ingestion.full_load(
            owner="delta-io",
            repository="delta",
            run_id="run-123",
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        )

    writer.ensure_table.assert_called_once()
    writer.append.assert_not_called()


def test_full_load_propagates_writer_failure() -> None:
    client = Mock()
    client.iter_issues.return_value = iter(
        [{"id": 123, "updated_at": "2026-09-20T12:03:00Z"}]
    )
    writer = Mock()
    writer.append.side_effect = RuntimeError("Delta write failed")
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    with pytest.raises(RuntimeError, match="Delta write failed"):
        ingestion.full_load(
            owner="delta-io",
            repository="delta",
            run_id="run-123",
            ingested_at=datetime(2026, 9, 20, 12, 5, tzinfo=UTC),
        )

    writer.ensure_table.assert_called_once()
