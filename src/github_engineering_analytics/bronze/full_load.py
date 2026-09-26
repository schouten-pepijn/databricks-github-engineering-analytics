"""Tracked GitHub Issues ingestion and Bronze-to-Silver job entry point."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, Self
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
from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.watermark import Watermark
from github_engineering_analytics.control.watermark_repository import (
    DeltaWatermarkRepository,
)
from github_engineering_analytics.silver.full_load import run_bronze_to_silver
from github_engineering_analytics.silver.users_full_load import (
    run_bronze_to_silver_users,
)


@dataclass(frozen=True)
class FullLoadSettings:
    """Validated runtime configuration for one tracked GitHub Issues run."""

    catalog: str
    owner: str
    repository: str
    github_token: str | None = None
    github_token_secret_scope: str | None = None
    github_token_secret_key: str | None = None

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
        secret_scope = environment.get("GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE")
        secret_key = environment.get("GITHUB_ANALYTICS_TOKEN_SECRET_KEY")

        token = token.strip() if token and token.strip() else None
        secret_scope = (
            secret_scope.strip() if secret_scope and secret_scope.strip() else None
        )
        secret_key = secret_key.strip() if secret_key and secret_key.strip() else None

        if bool(secret_scope) != bool(secret_key):
            raise ValueError(
                "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE and "
                "GITHUB_ANALYTICS_TOKEN_SECRET_KEY must be provided together"
            )

        if token is not None and secret_scope is not None:
            raise ValueError(
                "GITHUB_TOKEN cannot be used with a Databricks secret reference"
            )

        return cls(
            catalog=environment["GITHUB_ANALYTICS_CATALOG"].strip(),
            owner=environment["GITHUB_ANALYTICS_OWNER"].strip(),
            repository=environment["GITHUB_ANALYTICS_REPOSITORY"].strip(),
            github_token=token,
            github_token_secret_scope=secret_scope,
            github_token_secret_key=secret_key,
        )


class DatabricksSecretGetter(Protocol):
    """Read one value from a classic Databricks secret scope/key pair."""

    def __call__(
        self,
        *,
        spark: SparkSession,
        scope: str,
        key: str,
    ) -> str:
        """Return one secret value without exposing it to job parameters."""
        ...


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

    try:
        token = secret_getter(
            spark=spark,
            scope=scope,
            key=key,
        )
    except Exception as error:
        raise RuntimeError(
            f"Unable to read GitHub token secret {scope}/{key}"
        ) from error

    if not token.strip():
        raise ValueError(f"Databricks secret {scope}/{key} must not be empty")

    return token


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


def run_tracked_full_load(
    *,
    spark: SparkSession,
    catalog: str,
    owner: str,
    repository: str,
    github_token: str | None = None,
    run_id_factory: Callable[[], UUID] = uuid4,
    clock: Callable[[], datetime] = _current_utc_time,
) -> BronzeIngestionResult:
    """Run and record one full-or-incremental Bronze-to-Silver lifecycle.

    A missing committed watermark selects a full extraction; an existing one
    selects the overlap-aware incremental route. The run succeeds only after
    Bronze, Issues Silver, and Users Silver complete. This boundary records the
    candidate watermark but deliberately does not commit it while required Gold
    processing is still absent. A stage failure is recorded as FAILED before
    the original exception is re-raised.
    """
    config = PipelineConfig(catalog=catalog)
    pipeline_runs = DeltaPipelineRunRepository(
        spark=spark,
        config=config,
    )
    pipeline_runs.ensure_table()
    watermarks = DeltaWatermarkRepository(
        spark=spark,
        config=config,
    )
    watermarks.ensure_table()
    watermark_before = watermarks.get("github", "issues")

    started_run = PipelineRun(
        run_id=run_id_factory().hex,
        source_name="github",
        entity_name="issues",
        started_at=clock(),
        watermark_before=watermark_before,
    )
    pipeline_runs.record_started(started_run)

    try:
        # The durable watermark is the sole mode switch: no stored position
        # means bootstrap; otherwise replay its deliberate overlap window.
        if watermark_before is None:
            result = run_full_load(
                spark=spark,
                catalog=catalog,
                owner=owner,
                repository=repository,
                github_token=github_token,
                run_id=started_run.run_id,
                ingested_at=started_run.started_at,
            )
        else:
            result = run_incremental_load(
                spark=spark,
                catalog=catalog,
                owner=owner,
                repository=repository,
                github_token=github_token,
                run_id=started_run.run_id,
                ingested_at=started_run.started_at,
                watermark=watermark_before,
            )

        run_bronze_to_silver(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
        run_bronze_to_silver_users(
            spark=spark,
            catalog=catalog,
            bronze_run_id=started_run.run_id,
        )
    except Exception as error:
        failed_run = started_run.fail(
            error_message=str(error).strip() or type(error).__name__,
            finished_at=clock(),
        )

        try:
            pipeline_runs.record_finished(failed_run)
        except Exception:
            # Lifecycle persistence must not hide the original stage failure
            # that determines the job result and its retry behaviour.
            logger.bind(run_id=started_run.run_id).exception(
                "Failed to persist pipeline run failure."
            )
        raise

    pipeline_runs.record_finished(
        started_run.succeed(
            candidate_watermark=result.candidate_watermark,
            finished_at=clock(),
        )
    )

    return result


def main(
    catalog: str | None = None,
    owner: str | None = None,
    repository: str | None = None,
    token_secret_scope: str | None = None,
    token_secret_key: str | None = None,
) -> None:
    """Run tracked GitHub Issues ingestion from local or supplied configuration.

    When called without arguments, configuration comes from local environment
    variables. The ``cli`` adapter supplies the five task arguments for a
    Databricks Python wheel run. The token itself is deliberately never a task
    parameter; Databricks resolves it from the supplied secret reference.
    """
    job_parameters = (
        catalog,
        owner,
        repository,
        token_secret_scope,
        token_secret_key,
    )

    if all(parameter is None for parameter in job_parameters):
        settings = FullLoadSettings.from_environment(os.environ)
    else:
        settings = FullLoadSettings.from_environment(
            {
                "GITHUB_ANALYTICS_CATALOG": catalog or "",
                "GITHUB_ANALYTICS_OWNER": owner or "",
                "GITHUB_ANALYTICS_REPOSITORY": repository or "",
                "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE": token_secret_scope or "",
                "GITHUB_ANALYTICS_TOKEN_SECRET_KEY": token_secret_key or "",
            }
        )
    spark = _get_or_create_spark()

    result = run_tracked_full_load(
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
    ).info("Completed tracked GitHub Issues load.")


def cli() -> None:
    """Parse wheel-task arguments and delegate to the tracked-load entry point."""
    parser = argparse.ArgumentParser(
        description="Run tracked GitHub Issues ingestion through Bronze and Silver."
    )
    parser.add_argument("--catalog")
    parser.add_argument("--owner")
    parser.add_argument("--repository")
    parser.add_argument("--token-secret-scope")
    parser.add_argument("--token-secret-key")
    arguments = parser.parse_args()

    main(
        catalog=arguments.catalog,
        owner=arguments.owner,
        repository=arguments.repository,
        token_secret_scope=arguments.token_secret_scope,
        token_secret_key=arguments.token_secret_key,
    )


if __name__ == "__main__":
    cli()
