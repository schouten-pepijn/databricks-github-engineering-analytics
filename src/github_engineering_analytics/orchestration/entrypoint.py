"""Configure and launch the tracked GitHub Issues Bronze-to-Silver stage."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Protocol, Self

import typer
from loguru import logger
from pyspark.sql import SparkSession

from github_engineering_analytics.orchestration.tracked_load import (
    run_tracked_full_load,
)

# A Databricks wheel task supplies named options directly, rather than a Typer
# subcommand. Completion is unnecessary because this is an automation boundary,
# not an interactive shell command.
app = typer.Typer(
    add_completion=False,
    help="Run tracked GitHub Issues ingestion through Bronze and Silver.",
)


@dataclass(frozen=True)
class FullLoadSettings:
    """Validated configuration for one tracked GitHub Issues Bronze-to-Silver run.

    At most one token mechanism may be configured: ``GITHUB_TOKEN`` for local
    development, or a Databricks secret scope/key pair for a deployed wheel.
    A token may be omitted for a public repository. The deployed option keeps
    a private-repository token out of bundle configuration and job parameters.
    """

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
        """Validate named configuration values without resolving a token.

        Secret lookup is intentionally deferred to :func:`resolve_github_token`.
        Keeping validation separate from runtime I/O makes this configuration
        boundary deterministic and straightforward to unit test.
        """
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

        # Normalize blank optional values to ``None`` before validating the two
        # mutually exclusive token mechanisms.
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


def _get_or_create_spark() -> SparkSession:
    """Return the Spark session supplied or created by the active PySpark runtime.

    A Databricks job supplies its active session. Outside Databricks, the
    process must configure a compatible Spark runtime separately; GitHub CLI
    environment variables do not configure Spark or Databricks Connect.
    """
    return SparkSession.builder.getOrCreate()


def _get_databricks_secret(
    *,
    spark: SparkSession,
    scope: str,
    key: str,
) -> str:
    """Resolve one classic Databricks secret at runtime without logging it."""
    from pyspark.dbutils import DBUtils

    return DBUtils(spark).secrets.get(scope=scope, key=key)


def resolve_github_token(
    *,
    settings: FullLoadSettings,
    spark: SparkSession,
    secret_getter: DatabricksSecretGetter = _get_databricks_secret,
) -> str | None:
    """Resolve the configured GitHub token without exposing it as a CLI value.

    A direct token may be used for local development. Deployed jobs use the
    secret scope/key reference and retrieve the value only inside Databricks.
    """
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


def main(
    catalog: str | None = None,
    owner: str | None = None,
    repository: str | None = None,
    token_secret_scope: str | None = None,
    token_secret_key: str | None = None,
) -> None:
    """Run the tracked GitHub Issues Bronze-to-Silver stage from configuration.

    When called without arguments, configuration comes from local environment
    variables. The Typer adapter supplies the five task arguments for a
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

    # Treat an invocation as local only when every wheel option is absent. This
    # prevents a partial job invocation from silently mixing job and developer
    # environment configuration.
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
    # Start Spark and secret I/O only after the complete configuration is valid.
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

    # Record operational counts, never raw issue payloads or secret values.
    logger.bind(
        catalog=settings.catalog,
        repository_owner=settings.owner,
        repository_name=settings.repository,
        records_extracted=result.records_extracted,
        batches_written=result.batches_written,
    ).info("Completed tracked GitHub Issues load.")


# ``invoke_without_command`` preserves the wheel's one-command invocation
# shape: ``github-engineering-analytics-full-load --catalog ...``. The explicit
# hyphenated secret options must match the DAB ``named_parameters`` keys.
@app.callback(invoke_without_command=True)
def _run_cli(
    catalog: Annotated[str | None, typer.Option()] = None,
    owner: Annotated[str | None, typer.Option()] = None,
    repository: Annotated[str | None, typer.Option()] = None,
    token_secret_scope: Annotated[
        str | None,
        typer.Option("--token-secret-scope"),
    ] = None,
    token_secret_key: Annotated[
        str | None,
        typer.Option("--token-secret-key"),
    ] = None,
) -> None:
    """Map Databricks wheel options onto the typed runtime entry point."""
    main(
        catalog=catalog,
        owner=owner,
        repository=repository,
        token_secret_scope=token_secret_scope,
        token_secret_key=token_secret_key,
    )


def cli() -> None:
    """Run the Typer adapter used by the published Python-wheel command."""
    app()


if __name__ == "__main__":
    cli()
