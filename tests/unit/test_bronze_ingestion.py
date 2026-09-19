from datetime import UTC, datetime
from unittest.mock import Mock

from github_engineering_analytics.bronze.ingestion import GitHubIssueBronzeIngestion


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
