"""Destructively empty the known GitHub analytics tables in a confirmed catalog."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from dataclasses import dataclass

from databricks.connect import DatabricksSession
from loguru import logger
from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig

_TEST_CATALOG_ENVIRONMENT_VARIABLE = "DATABRICKS_TEST_CATALOG"
_TEST_CONFIRMATION_ENVIRONMENT_VARIABLE = "CONFIRM_TEST_CATALOG_RESET"
_DEV_CATALOG_ENVIRONMENT_VARIABLE = "DATABRICKS_DEV_CATALOG"
_DEV_CONFIRMATION_ENVIRONMENT_VARIABLE = "CONFIRM_DEV_CATALOG_RESET"
_SUPPORTED_ENVIRONMENT_VARIABLE_PAIRS = frozenset(
    {
        (
            _TEST_CATALOG_ENVIRONMENT_VARIABLE,
            _TEST_CONFIRMATION_ENVIRONMENT_VARIABLE,
        ),
        (
            _DEV_CATALOG_ENVIRONMENT_VARIABLE,
            _DEV_CONFIRMATION_ENVIRONMENT_VARIABLE,
        ),
    }
)


@dataclass(frozen=True)
class CatalogResetResult:
    """Report the known tables that were truncated or absent during a reset."""

    truncated_tables: tuple[str, ...]
    skipped_tables: tuple[str, ...]


def reset_catalog(
    *,
    spark: SparkSession,
    catalog: str,
) -> CatalogResetResult:
    """Truncate only the known project tables in one explicitly supplied catalog."""
    config = PipelineConfig(
        catalog=_require_single_identifier(catalog, environment_variable="catalog")
    )
    truncated_tables: list[str] = []
    skipped_tables: list[str] = []

    for table_name in _known_table_names(config):
        if not spark.catalog.tableExists(table_name):
            skipped_tables.append(table_name)
            logger.bind(catalog=catalog, table_name=table_name).info(
                "Skipped absent table during catalog reset."
            )
            continue

        spark.sql(f"TRUNCATE TABLE {_quote_identifier(table_name)}")
        truncated_tables.append(table_name)
        logger.bind(catalog=catalog, table_name=table_name).info(
            "Truncated table during catalog reset."
        )

    return CatalogResetResult(
        truncated_tables=tuple(truncated_tables),
        skipped_tables=tuple(skipped_tables),
    )


def main(
    environment: Mapping[str, str] | None = None,
    *,
    catalog_environment_variable: str = _TEST_CATALOG_ENVIRONMENT_VARIABLE,
    confirmation_environment_variable: str = _TEST_CONFIRMATION_ENVIRONMENT_VARIABLE,
) -> CatalogResetResult:
    """Reset one confirmed DEV or test catalog through Databricks Connect."""
    settings = os.environ if environment is None else environment
    catalog = _require_confirmed_catalog(
        settings,
        catalog_environment_variable=catalog_environment_variable,
        confirmation_environment_variable=confirmation_environment_variable,
    )
    spark = _get_or_create_spark(settings)
    result = reset_catalog(spark=spark, catalog=catalog)

    logger.bind(
        catalog=catalog,
        truncated_tables=result.truncated_tables,
        skipped_tables=result.skipped_tables,
    ).info("Completed destructive catalog reset.")

    return result


def cli() -> None:
    """Run the catalog reset with one supported environment-variable pair."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog-environment-variable",
        default=_TEST_CATALOG_ENVIRONMENT_VARIABLE,
    )
    parser.add_argument(
        "--confirmation-environment-variable",
        default=_TEST_CONFIRMATION_ENVIRONMENT_VARIABLE,
    )
    arguments = parser.parse_args()

    main(
        catalog_environment_variable=arguments.catalog_environment_variable,
        confirmation_environment_variable=arguments.confirmation_environment_variable,
    )


def _require_confirmed_catalog(
    environment: Mapping[str, str],
    *,
    catalog_environment_variable: str,
    confirmation_environment_variable: str,
) -> str:
    """Read one supported catalog only after a matching explicit confirmation."""
    environment_variable_pair = (
        catalog_environment_variable,
        confirmation_environment_variable,
    )
    if environment_variable_pair not in _SUPPORTED_ENVIRONMENT_VARIABLE_PAIRS:
        raise ValueError("Unsupported catalog reset environment-variable pair.")

    catalog = environment.get(catalog_environment_variable, "").strip()
    confirmation = environment.get(confirmation_environment_variable)

    if not catalog:
        raise ValueError(f"{catalog_environment_variable} is required.")

    _require_single_identifier(catalog, catalog_environment_variable)

    if confirmation != catalog:
        raise ValueError(
            f"Set {confirmation_environment_variable} exactly to "
            f"{catalog_environment_variable} before resetting catalog data."
        )

    return catalog


def _get_or_create_spark(environment: Mapping[str, str]) -> SparkSession:
    """Create a remote Databricks Connect session using local test settings."""
    profile = environment.get("DATABRICKS_CONFIG_PROFILE", "databricks-dev")
    cluster_id = environment.get("DATABRICKS_CLUSTER_ID", "").strip()
    builder = DatabricksSession.builder.profile(profile)

    if cluster_id:
        builder = builder.clusterId(cluster_id)
    else:
        builder = builder.serverless()

    return builder.getOrCreate()


def _known_table_names(config: PipelineConfig) -> tuple[str, ...]:
    """Return only tables whose live reset behavior is currently supported.

    Entities still under construction are added only after their persistence
    and test-cleanup semantics are complete.
    """
    return (
        config.bronze_issues_table,
        config.silver_issues_table,
        config.silver_users_table,
        config.watermark_table,
        config.pipeline_runs_table,
    )


def _require_single_identifier(identifier: str, environment_variable: str) -> str:
    """Reject multipart or blank catalog names before generating SQL."""
    if not identifier or "." in identifier:
        raise ValueError(f"{environment_variable} must be one non-empty catalog name.")

    return identifier


def _quote_identifier(identifier: str) -> str:
    """Quote all multipart identifier parts for Spark SQL."""
    parts = identifier.split(".")

    if not all(parts):
        raise ValueError(f"Invalid multipart identifier: {identifier!r}")

    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


if __name__ == "__main__":
    cli()
