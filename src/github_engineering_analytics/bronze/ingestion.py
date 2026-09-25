"""Orchestrate a full GitHub Issues load into append-only Bronze storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import batched

from github_engineering_analytics.api.client import GitHubClient
from github_engineering_analytics.bronze.issues import (
    BronzeIssueRecord,
    DeltaBronzeIssueWriter,
)
from github_engineering_analytics.control.watermark import Watermark


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Operational counts from one Bronze ingestion attempt."""

    records_extracted: int
    batches_written: int
    candidate_watermark: Watermark | None = None


class GitHubIssueBronzeIngestion:
    """Compose GitHub extraction, Bronze mapping, and append-only storage."""

    def __init__(
        self,
        client: GitHubClient,
        writer: DeltaBronzeIssueWriter,
    ) -> None:
        self._client = client
        self._writer = writer

    def full_load(
        self,
        *,
        owner: str,
        repository: str,
        run_id: str,
        ingested_at: datetime,
        batch_size: int = 100,
    ) -> BronzeIngestionResult:
        """Extract all issues and append them to Bronze in bounded batches."""
        return self._ingest(
            owner=owner,
            repository=repository,
            run_id=run_id,
            ingested_at=ingested_at,
            watermark=None,
            batch_size=batch_size,
        )

    def incremental_load(
        self,
        *,
        owner: str,
        repository: str,
        run_id: str,
        ingested_at: datetime,
        watermark: Watermark,
        batch_size: int = 100,
    ) -> BronzeIngestionResult:
        """Extract issues since the overlap-adjusted committed watermark."""
        return self._ingest(
            owner=owner,
            repository=repository,
            run_id=run_id,
            ingested_at=ingested_at,
            watermark=watermark,
            batch_size=batch_size,
        )

    def _ingest(
        self,
        *,
        owner: str,
        repository: str,
        run_id: str,
        ingested_at: datetime,
        watermark: Watermark | None,
        batch_size: int,
    ) -> BronzeIngestionResult:
        """Share batching and Bronze mapping across full and incremental loads."""
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        request_watermark = (
            watermark.extraction_start if watermark is not None else None
        )
        latest_source_updated_at: datetime | None = None

        self._writer.ensure_table()
        issues = self._client.iter_issues(
            owner=owner,
            repository=repository,
            since=request_watermark,
        )

        records_extracted = 0
        batches_written = 0

        for batch_number, payload_batch in enumerate(
            batched(issues, batch_size),
            start=1,
        ):
            records = [
                BronzeIssueRecord.from_github_payload(
                    repository_owner=owner,
                    repository_name=repository,
                    payload=payload,
                    run_id=run_id,
                    ingested_at=ingested_at,
                    request_watermark=request_watermark,
                    page_or_batch_reference=f"batch-{batch_number}",
                )
                for payload in payload_batch
            ]

            self._writer.append(records)
            records_extracted += len(records)
            batches_written += 1

        return BronzeIngestionResult(
            records_extracted=records_extracted,
            batches_written=batches_written,
        )
