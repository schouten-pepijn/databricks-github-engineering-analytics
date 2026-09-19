"""Shared opt-in Databricks Connect fixtures for integration tests."""

import os
from collections.abc import Iterator

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture()
def integration_spark() -> Iterator[SparkSession]:
    """Yield a UTC Spark session only when a dedicated DEV catalog is explicit."""
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
