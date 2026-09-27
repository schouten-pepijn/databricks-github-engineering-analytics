"""Extract GitHub Issues into the append-only Bronze table."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from github_engineering_analytics.api.client import GitHubClient
from github_engineering_analytics.bronze.ingestion import (
    BronzeIngestionResult,
    GitHubIssueBronzeIngestion,
)
from github_engineering_analytics.bronze.issues import DeltaBronzeIssueWriter
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.watermark import Watermark


def _create_bronze_ingestion(
    *,
    spark: SparkSession,
    catalog: str,
    github_token: str | None,
) -> GitHubIssueBronzeIngestion:
    """Create the GitHub and Delta adapters shared by Bronze load modes."""
    config = PipelineConfig(catalog=catalog)
    client = GitHubClient(token=github_token)
    writer = DeltaBronzeIssueWriter(spark=spark, config=config)

    return GitHubIssueBronzeIngestion(client=client, writer=writer)


def run_full_load(
    *,
    spark: SparkSession,
    catalog: str,
    owner: str,
    repository: str,
    github_token: str | None = None,
    run_id: str,
    ingested_at: datetime,
) -> BronzeIngestionResult:
    """Run one complete GitHub Issues extraction into the Bronze table.

    The caller supplies the run ID and ingestion timestamp so a higher-level
    orchestrator can use the same metadata for the durable pipeline lifecycle.
    This function does not manage watermarks, pipeline-run state, Silver
    processing, Databricks secrets, or job configuration.
    """
    ingestion = _create_bronze_ingestion(
        spark=spark,
        catalog=catalog,
        github_token=github_token,
    )

    return ingestion.full_load(
        owner=owner,
        repository=repository,
        run_id=run_id,
        ingested_at=ingested_at,
    )


def run_incremental_load(
    *,
    spark: SparkSession,
    catalog: str,
    owner: str,
    repository: str,
    github_token: str | None = None,
    run_id: str,
    ingested_at: datetime,
    watermark: Watermark,
) -> BronzeIngestionResult:
    """Run an extraction from the committed watermark's overlap boundary."""
    ingestion = _create_bronze_ingestion(
        spark=spark,
        catalog=catalog,
        github_token=github_token,
    )

    return ingestion.incremental_load(
        owner=owner,
        repository=repository,
        run_id=run_id,
        ingested_at=ingested_at,
        watermark=watermark,
    )
