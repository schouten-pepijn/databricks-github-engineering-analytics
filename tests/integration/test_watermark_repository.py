import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.watermark import Watermark
from github_engineering_analytics.control.watermark_repository import (
    DeltaWatermarkRepository,
)

pytestmark = pytest.mark.integration


@pytest.fixture()
def integration_spark() -> SparkSession:
    if os.getenv("RUN_DATABRICKS_INTEGRATION_TESTS") != "1":
        pytest.skip("Set RUN_DATABRICKS_INTEGRATION_TESTS=1 to run integration tests.")

    if not os.getenv("DATABRICKS_TEST_CATALOG"):
        pytest.skip("Set DATABRICKS_TEST_CATALOG to a dedicated DEV catalog.")

    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "databricks-dev")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    builder = DatabricksSession.builder.profile(profile)

    if cluster_id:
        builder = builder.clusterId(cluster_id)
    else:
        builder = builder.serverless()

    spark = builder.getOrCreate()
    original_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        yield spark
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_timezone)


def test_watermark_repository_preserves_latest_value(
    integration_spark: SparkSession,
) -> None:
    catalog = os.environ["DATABRICKS_TEST_CATALOG"]
    config = PipelineConfig(catalog=catalog)
    repository = DeltaWatermarkRepository(integration_spark, config)
    entity_name = f"issues_{uuid4().hex}"

    repository.ensure_table()

    try:
        assert repository.get("pytest", entity_name) is None

        first = Watermark(
            value=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        )
        repository.commit_success(
            source_name="pytest",
            entity_name=entity_name,
            watermark_column="updated_at",
            watermark=first,
            run_id="first-run",
            committed_at=datetime(2026, 9, 19, 12, 1, tzinfo=UTC),
        )

        assert repository.get("pytest", entity_name) == first

        latest = Watermark(
            value=datetime(2026, 9, 19, 12, 10, tzinfo=UTC),
        )
        repository.commit_success(
            source_name="pytest",
            entity_name=entity_name,
            watermark_column="updated_at",
            watermark=latest,
            run_id="latest-run",
            committed_at=datetime(2026, 9, 19, 12, 11, tzinfo=UTC),
        )

        repository.commit_success(
            source_name="pytest",
            entity_name=entity_name,
            watermark_column="updated_at",
            watermark=first,
            run_id="stale-run",
            committed_at=datetime(2026, 9, 19, 12, 12, tzinfo=UTC),
        )

        assert repository.get("pytest", entity_name) == latest
    finally:
        DeltaTable.forName(
            integration_spark,
            config.watermark_table,
        ).delete(
            condition=(f"source_name = 'pytest' AND entity_name = '{entity_name}'")
        )
