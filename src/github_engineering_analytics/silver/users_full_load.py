"""Application boundary for transforming Bronze GitHub issue users into Silver."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.users import (
    BronzeIssueToSilverUserTransformer,
    DeltaSilverUserWriter,
)


def run_bronze_to_silver_users(
    *,
    spark: SparkSession,
    catalog: str,
    bronze_run_id: str | None = None,
) -> None:
    """Transform Bronze issue-user observations into current Silver users.

    When ``bronze_run_id`` is supplied, only that append-only Bronze run is
    processed so a tracked job cannot mix observations from another attempt.
    """
    if bronze_run_id is not None and not bronze_run_id.strip():
        raise ValueError("bronze_run_id must not be empty when supplied")

    config = PipelineConfig(catalog=catalog)
    bronze = spark.table(config.bronze_issues_table)

    # Optionally scope to one run. The tracked pipeline passes its own run ID so
    # it only processes what it just ingested; without one, the whole
    # append-only Bronze history is reprocessed.
    if bronze_run_id is not None:
        bronze = bronze.where(F.col("_run_id") == bronze_run_id)

    transformer = BronzeIssueToSilverUserTransformer()
    writer = DeltaSilverUserWriter(spark=spark, config=config)

    # ensure_table() creates the table on the first run and does nothing after
    # that. The transformer keeps the newest observation per user, and the writer
    # merges it in without ever replacing newer data with older data.
    writer.ensure_table()
    writer.upsert_dataframe(transformer.transform(bronze))
