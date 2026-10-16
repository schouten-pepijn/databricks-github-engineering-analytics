"""Typer adapter that lets a Databricks wheel task start a Silver replay."""

from __future__ import annotations

from typing import Annotated

import typer
from pyspark.sql import SparkSession

from github_engineering_analytics.orchestration.silver_replay import (
    run_silver_replay,
)

app = typer.Typer(
    add_completion=False,
    help="Replay a stored manifest into isolated Silver candidate tables.",
)


def _get_or_create_spark() -> SparkSession:
    """Return the active Databricks Spark session for a wheel task."""
    return SparkSession.builder.getOrCreate()


def replay_silver_main(
    *,
    catalog: str,
    manifest_id: str,
    execution_id: str,
) -> None:
    """Run one replay inside the Databricks job."""
    result = run_silver_replay(
        spark=_get_or_create_spark(),
        catalog=catalog,
        manifest_id=manifest_id,
        execution_id=execution_id,
    )
    # The job log is where an operator reads what was written.
    print(result)


@app.callback(invoke_without_command=True)
def _run_cli(
    catalog: Annotated[str, typer.Option()],
    manifest_id: Annotated[str, typer.Option("--manifest-id")],
    execution_id: Annotated[str, typer.Option("--execution-id")],
) -> None:
    """Map the wheel parameters onto the replay call."""
    replay_silver_main(
        catalog=catalog,
        manifest_id=manifest_id,
        execution_id=execution_id,
    )


def cli() -> None:
    """Run the Typer adapter for the published wheel.

    standalone_mode=False stops Click from calling sys.exit(0) after a normal
    run, which Databricks would otherwise report as a failed wheel task.
    """
    app(standalone_mode=False)
