"""Application boundary for transforming Bronze GitHub issues into Silver."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.issues import (
    BronzeIssueToSilverTransformer,
    DeltaSilverIssueWriter,
)


def run_bronze_to_silver(
    *,
    spark: SparkSession,
    catalog: str,
    bronze_run_id: str | None = None,
) -> None:
    """Transform Bronze issue history into the current-state Silver table.

    When ``bronze_run_id`` is supplied, process only that append-only Bronze
    run. This keeps integration tests isolated and later lets the job process
    exactly the Bronze data produced by one pipeline attempt.
    """
    if bronze_run_id is not None and not bronze_run_id.strip():
        raise ValueError("bronze_run_id must not be empty when supplied")

    config = PipelineConfig(catalog=catalog)
    bronze = spark.table(config.bronze_issues_table)

    if bronze_run_id is not None:
        bronze = bronze.where(F.col("_run_id") == bronze_run_id)

    transformer = BronzeIssueToSilverTransformer()
    writer = DeltaSilverIssueWriter(spark=spark, config=config)

    writer.ensure_table()
    writer.upsert_dataframe(transformer.transform(bronze))
