"""Orchestrate a full GitHub Issues load into append-only Bronze storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from github_engineering_analytics.api.client import GitHubClient
from github_engineering_analytics.bronze.issues import (
    DeltaBronzeIssueWriter,
)


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Operational counts from one Bronze ingestion attempt."""

    records_extracted: int
    batches_written: int


class GitHubIssueBronzeIngestion:
    """Compose GitHub extraction, Bronze mapping, and append-only storage."""

    def __init__(
        self,
        client: GitHubClient,
        writer: DeltaBronzeIssueWriter,
    ) -> None:
        self._client = client
        self._writer = writer
