"""Replay one manifest into a new, isolated Issues candidate table."""

import argparse
import re

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from github_engineering_analytics.bronze.manifest_input import read_manifest_input
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.control.replay_manifest_repository import (
    DeltaReplayManifestRepository,
)
from github_engineering_analytics.silver.issues import (
    BronzeIssueToSilverTransformer,
)

TEST_CATALOG = "github_engineering_analytics_test"
TARGET_SCHEMA = "github_analytics_v2min"


def main(manifest_id: str, execution_id: str) -> None:
    """Validate one frozen input, then write a new execution-specific table."""
    if not re.fullmatch(r"[a-z0-9_]{1,40}", execution_id):
        raise SystemExit(
            "execution_id must contain 1-40 lowercase letters, digits or _"
        )

    spark: SparkSession = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    config = PipelineConfig(catalog=TEST_CATALOG)
    manifest = DeltaReplayManifestRepository(spark, config).get(manifest_id)
    if (
        manifest.bronze_table != config.bronze_issues_table
        or manifest.repository_owner != "psf"
        or manifest.repository_name != "requests"
    ):
        raise SystemExit("manifest does not target the configured TEST repository")

    # Resolve the pinned Bronze input and validate all rows before any DDL or write.
    bronze = read_manifest_input(spark, manifest)
    bronze_rows = bronze.count()
    if bronze_rows == 0:
        raise SystemExit("manifest input contains no Bronze rows")

    silver = BronzeIssueToSilverTransformer().transform(bronze)
    candidate = silver.withColumn(
        "manifest_id", F.lit(manifest.manifest_id)
    ).withColumn("replay_execution_id", F.lit(execution_id))
    candidate_rows = candidate.count()

    table_name = f"s2_replay_issues_{execution_id}"
    target_table = f"{TEST_CATALOG}.{TARGET_SCHEMA}.{table_name}"
    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS "
        f"{quote_multipart_identifier(f'{TEST_CATALOG}.{TARGET_SCHEMA}')}"
    )

    # Error if this execution ID already has an output; never overwrite a result.
    candidate.write.format("delta").mode("errorifexists").saveAsTable(
        quote_multipart_identifier(target_table)
    )
    stored_rows = spark.table(target_table).count()
    if stored_rows != candidate_rows:
        raise RuntimeError(
            f"candidate row count changed on write: "
            f"{candidate_rows} transformed, {stored_rows} stored"
        )

    version = DeltaTable.forName(spark, target_table).history(1).collect()[0]["version"]
    print(
        {
            "manifest_id": manifest.manifest_id,
            "execution_id": execution_id,
            "bronze_rows": bronze_rows,
            "silver_rows": candidate_rows,
            "target_table": target_table,
            "target_version": version,
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--execution-id", required=True)
    arguments = parser.parse_args()
    main(arguments.manifest_id, arguments.execution_id)
