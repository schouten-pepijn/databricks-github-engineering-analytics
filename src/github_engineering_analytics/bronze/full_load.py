"""Application boundary for a GitHub Issues full load into Bronze."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pyspark.sql import SparkSession

from github_engineering_analytics.api.client import GitHubClient
from github_engineering_analytics.bronze.ingestion import (
    BronzeIngestionResult,
    GitHubIssueBronzeIngestion,
)
from github_engineering_analytics.bronze.issues import DeltaBronzeIssueWriter
from github_engineering_analytics.common.config import PipelineConfig


def _current_utc_time() -> datetime:
    """Return the current timezone-aware UTC timestamp for a pipeline run."""
    return datetime.now(UTC)


def run_full_load(
    *,
    spark: SparkSession,
    catalog: str,
    owner: str,
    repository: str,
    github_token: str | None = None,
    run_id_factory: Callable[[], UUID] = uuid4,
    clock: Callable[[], datetime] = _current_utc_time,
) -> BronzeIngestionResult:
    """Run one complete GitHub Issues extraction into the Bronze table.

    The orchestration boundary creates the run ID and ingestion timestamp.
    It does not manage watermarks, pipeline-run state, Silver processing,
    Databricks secrets, or job configuration.
    """
    config = PipelineConfig(catalog=catalog)
    client = GitHubClient(token=github_token)
    writer = DeltaBronzeIssueWriter(spark=spark, config=config)
    ingestion = GitHubIssueBronzeIngestion(client=client, writer=writer)

    return ingestion.full_load(
        owner=owner,
        repository=repository,
        run_id=run_id_factory().hex,
        ingested_at=clock(),
    )
