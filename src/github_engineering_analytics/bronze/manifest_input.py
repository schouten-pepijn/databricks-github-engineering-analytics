"""Read the exact Bronze input that a replay manifest locked."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest


class ManifestInputUnavailableError(RuntimeError):
    """Raised when the manifest's Bronze input is not available in the Delta table."""


def read_manifest_input(
    spark: SparkSession,
    manifest: ReplayManifest,
) -> DataFrame:
    """Return the Bronze rows frozen by a manifest, or fail before any write.

    This is how a manual replay turns a manifest into data; the normal pipeline
    run never calls it. The result is pinned to ``manifest.bronze_version``
    with Delta time travel, so appends made after that version cannot change
    it. Rows of runs that are not listed in the manifest stay out.
    """
    try:
        # Delta time travel: read the table as it was at one commit version.
        bronze = spark.sql(f"""
            SELECT * FROM {quote_multipart_identifier(manifest.bronze_table)}
            VERSION AS OF {manifest.bronze_version}
        """)

        selected = bronze.where(
            (F.col("repository_owner") == manifest.repository_owner)
            & (F.col("repository_name") == manifest.repository_name)
            # isin() unpacks a list or set only; a tuple is read as one value.
            & (F.col("_run_id").isin(list(manifest.successful_run_ids)))
        )
        # Resolve version and the data files, so a vacuum or missed version fails.
        found_run_ids = set(
            row["_run_id"] for row in selected.select("_run_id").distinct().collect()
        )

    except PySparkException as exc:
        raise ManifestInputUnavailableError(
            f"Bronze input for manifest {manifest.manifest_id!r} is unavailable: "
            f"{manifest.bronze_table} version {manifest.bronze_version}"
        ) from exc

    # Every run must contribute at least one row to the manifest's input.
    missing = sorted(set(manifest.successful_run_ids) - found_run_ids)
    if missing:
        raise ManifestInputUnavailableError(
            f"Bronze input for manifest {manifest.manifest_id!r} is missing "
            f"rows from runs {missing}: {manifest.bronze_table} version "
            f"{manifest.bronze_version}"
        )

    return selected
