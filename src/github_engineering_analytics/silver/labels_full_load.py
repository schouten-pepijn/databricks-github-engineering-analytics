"""Application boundary for transforming Bronze GitHub issue labels into Silver."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.silver.labels import (
    BronzeIssueToSilverLabelTransformer,
    DeltaSilverLabelWriter,
)


def run_bronze_to_silver_labels(
    *,
    spark: SparkSession,
    catalog: str,
    bronze_run_id: str | None = None,
) -> None:
    """Transform Bronze issue-label observations into current Silver labels.

    When ``bronze_run_id`` is supplied, only that append-only Bronze run is
    processed. This keeps one tracked pipeline attempt isolated from other runs.
    """
    if bronze_run_id is not None and not bronze_run_id.strip():
        raise ValueError("bronze_run_id must not be empty when supplied")

    config = PipelineConfig(catalog=catalog)
    bronze = spark.table(config.bronze_issues_table)

    if bronze_run_id is not None:
        bronze = bronze.where(F.col("_run_id") == bronze_run_id)

    transformer = BronzeIssueToSilverLabelTransformer()
    writer = DeltaSilverLabelWriter(spark=spark, config=config)

    writer.ensure_table()
    writer.upsert_dataframe(transformer.transform(bronze))
