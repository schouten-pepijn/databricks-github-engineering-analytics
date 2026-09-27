"""Typer adapters for successful and failed tracked-run finalization."""

from __future__ import annotations

from typing import Annotated

import typer
from pyspark.sql import SparkSession

from github_engineering_analytics.orchestration.finalizer import (
    finalize_failed_run,
    finalize_successful_run,
)

success_app = typer.Typer(
    add_completion=False,
    help="Finalize a successful GitHub Analytics pipeline run.",
)
failure_app = typer.Typer(
    add_completion=False,
    help="Finalize a failed GitHub Analytics pipeline run.",
)


def _get_or_create_spark() -> SparkSession:
    """Return the active Databricks Spark session for a wheel task."""
    return SparkSession.builder.getOrCreate()


def finalize_success_main(
    *,
    catalog: str,
    run_id: str,
) -> None:
    """Run the successful finalizer after all dbt Gold work has passed."""
    finalize_successful_run(
        spark=_get_or_create_spark(),
        catalog=catalog,
        run_id=run_id,
    )


def finalize_failure_main(
    *,
    catalog: str,
    run_id: str,
    failure_reason: str,
) -> None:
    """Run the failure finalizer after an upstream job task fails."""
    finalize_failed_run(
        spark=_get_or_create_spark(),
        catalog=catalog,
        run_id=run_id,
        failure_reason=failure_reason,
    )


@success_app.callback(invoke_without_command=True)
def _run_success_cli(
    catalog: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option("--run-id")],
) -> None:
    """Map the successful finalizer wheel parameters onto its runtime call."""
    finalize_success_main(catalog=catalog, run_id=run_id)


@failure_app.callback(invoke_without_command=True)
def _run_failure_cli(
    catalog: Annotated[str, typer.Option()],
    run_id: Annotated[str, typer.Option("--run-id")],
    failure_reason: Annotated[str, typer.Option("--failure-reason")],
) -> None:
    """Map the failed finalizer wheel parameters onto its runtime call."""
    finalize_failure_main(
        catalog=catalog,
        run_id=run_id,
        failure_reason=failure_reason,
    )


def success_cli() -> None:
    """Run the successful-finalizer Typer adapter for the published wheel."""
    success_app(standalone_mode=False)


def failure_cli() -> None:
    """Run the failed-finalizer Typer adapter for the published wheel."""
    failure_app(standalone_mode=False)
