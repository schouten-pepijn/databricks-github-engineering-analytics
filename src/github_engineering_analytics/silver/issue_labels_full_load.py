"""Application boundary for reconciling Bronze Issue labels into Silver."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer,
    DeltaSilverIssueLabelWriter,
)


def run_bronze_to_silver_issue_labels(
    *,
    spark: SparkSession,
    catalog: str,
    bronze_run_id: str | None = None,
) -> None:
    """Reconcile Bronze Issue-label snapshots into current Silver relationships.

    When ``bronze_run_id`` is supplied, only that append-only Bronze run is
    processed, so a tracked pipeline attempt cannot consume another run's rows.
    """
    if bronze_run_id is not None and not bronze_run_id.strip():
        raise ValueError("Bronze run_id must not be empty when supplied")

    config = PipelineConfig(catalog=catalog)
    # Bronze remains append-only. Read its full history unless the caller
    # explicitly isolates this orchestration attempt to one ingestion run.
    bronze = spark.table(config.bronze_issues_table)

    if bronze_run_id is not None:
        bronze = bronze.where(F.col("run_id") == bronze_run_id)

    # The transformer keeps empty label arrays in ``observed_issues`` while
    # producing relationship rows only for labels that currently exist.
    transformation = BronzeIssueToSilverLabelTransformer().transform(bronze)
    writer = DeltaSilverIssueLabelWriter(spark=spark, config=config)

    writer.ensure_table()
    # Reconciliation needs both outputs: the observation scope authorizes
    # stale-delete work, while the relationships define the desired state.
    writer.reconcile_dataframe(
        observed_issues=transformation.observed_issues,
        relationships=transformation.relationships,
    )
