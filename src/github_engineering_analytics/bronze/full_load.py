"""Application boundary for a GitHub Issues full load into Bronze."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self
from uuid import UUID, uuid4

from loguru import logger
from pyspark.sql import SparkSession

from github_engineering_analytics.api.client import GitHubClient
from github_engineering_analytics.bronze.ingestion import (
    BronzeIngestionResult,
    GitHubIssueBronzeIngestion,
)
from github_engineering_analytics.bronze.issues import DeltaBronzeIssueWriter
from github_engineering_analytics.common.config import PipelineConfig


@dataclass(frozen=True)
class FullLoadSettings:
    """Validated runtime configuration for one GitHub Issues full load run."""

    catalog: str
    owner: str
    repository: str
    github_token: str | None = None

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
    ) -> Self:
        """Create settings from explicitly named environment variables."""
        required_variables = (
            "GITHUB_ANALYTICS_CATALOG",
            "GITHUB_ANALYTICS_OWNER",
            "GITHUB_ANALYTICS_REPOSITORY",
        )

        if missing_variables := [
            variable
            for variable in required_variables
            if not environment.get(variable, "").strip()
        ]:
            missing = ", ".join(missing_variables)
            raise ValueError(f"Missing required environment variables: {missing}")

        token = environment.get("GITHUB_TOKEN")

        return cls(
            catalog=environment["GITHUB_ANALYTICS_CATALOG"].strip(),
            owner=environment["GITHUB_ANALYTICS_OWNER"].strip(),
            repository=environment["GITHUB_ANALYTICS_REPOSITORY"].strip(),
            github_token=token.strip() if token and token.strip() else None,
        )


def _current_utc_time() -> datetime:
    """Return the current timezone-aware UTC timestamp for a pipeline run."""
    return datetime.now(UTC)


def _get_or_create_spark() -> SparkSession:
    """Return the active Spark session or create one in the runtime."""
    return SparkSession.builder.getOrCreate()


def _get_databricks_secret(
    *,
    spark: SparkSession,
    scope: str,
    key: str,
) -> str:
    """Read one secret through Databricks Utilities at runtime."""
    from pyspark.dbutils import DBUtils

    return DBUtils(spark).secrets.get(scope=scope, key=key)


def resolve_github_token(
    *,
    settings: FullLoadSettings,
    spark: SparkSession,
    secret_getter: DatabricksSecretGetter = _get_databricks_secret,
) -> str | None:
    """Resolve a direct GitHub token or a configured Databricks secret."""

    if settings.github_token is not None:
        return settings.github_token

    scope = settings.github_token_secret_scope
    key = settings.github_token_secret_key

    if scope is None and key is None:
        return None

    if scope is None or key is None:
        raise ValueError(
            "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE and "
            "GITHUB_ANALYTICS_TOKEN_SECRET_KEY must be provided together"
        )

    token = secret_getter(
        spark=spark,
        scope=scope,
        key=key,
    )

    if not token.strip():
        raise ValueError(f"Databricks secret {scope}/{key} must not be empty")

    return token


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


def main() -> None:
    """Run a full load of GitHub Issues into the Bronze table."""
    settings = FullLoadSettings.from_environment(os.environ)
    spark = _get_or_create_spark()

    result = run_full_load(
        spark=spark,
        catalog=settings.catalog,
        owner=settings.owner,
        repository=settings.repository,
        github_token=resolve_github_token(
            settings=settings,
            spark=spark,
        ),
    )

    logger.bind(
        catalog=settings.catalog,
        repository_owner=settings.owner,
        repository_name=settings.repository,
        records_extracted=result.records_extracted,
        batches_written=result.batches_written,
    ).info("Completed full Bronze load.")


if __name__ == "__main__":
    main()
