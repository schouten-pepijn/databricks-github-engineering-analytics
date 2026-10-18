from unittest.mock import Mock, call

import pytest
from typer.testing import CliRunner

from github_engineering_analytics.common.catalog_reset import (
    CatalogResetResult,
    _require_confirmed_catalog,
    app,
    main,
    reset_catalog,
)


def test_reset_catalog_truncates_only_existing_known_tables() -> None:
    spark = Mock()
    spark.catalog.tableExists.side_effect = [True, False, True, True, True, True, False]

    result = reset_catalog(spark=spark, catalog="test_catalog")

    assert result == CatalogResetResult(
        truncated_tables=(
            "test_catalog.github_analytics_bronze.github_issues_raw",
            "test_catalog.github_analytics_silver.github_users",
            "test_catalog.github_analytics_silver.github_labels",
            "test_catalog.github_analytics_silver.github_issue_labels",
            "test_catalog.github_analytics_control.ingestion_watermark",
        ),
        skipped_tables=(
            "test_catalog.github_analytics_silver.github_issues",
            "test_catalog.github_analytics_control.pipeline_runs",
        ),
    )
    assert spark.catalog.tableExists.call_args_list == [
        call("test_catalog.github_analytics_bronze.github_issues_raw"),
        call("test_catalog.github_analytics_silver.github_issues"),
        call("test_catalog.github_analytics_silver.github_users"),
        call("test_catalog.github_analytics_silver.github_labels"),
        call("test_catalog.github_analytics_silver.github_issue_labels"),
        call("test_catalog.github_analytics_control.ingestion_watermark"),
        call("test_catalog.github_analytics_control.pipeline_runs"),
    ]
    assert spark.sql.call_args_list == [
        call(
            "TRUNCATE TABLE "
            "`test_catalog`.`github_analytics_bronze`.`github_issues_raw`"
        ),
        call("TRUNCATE TABLE `test_catalog`.`github_analytics_silver`.`github_users`"),
        call("TRUNCATE TABLE `test_catalog`.`github_analytics_silver`.`github_labels`"),
        call(
            "TRUNCATE TABLE "
            "`test_catalog`.`github_analytics_silver`.`github_issue_labels`"
        ),
        call(
            "TRUNCATE TABLE "
            "`test_catalog`.`github_analytics_control`.`ingestion_watermark`"
        ),
    ]


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({}, "DATABRICKS_TEST_CATALOG is required"),
        (
            {"DATABRICKS_TEST_CATALOG": "test_catalog"},
            "CONFIRM_TEST_CATALOG_RESET exactly",
        ),
        (
            {
                "DATABRICKS_TEST_CATALOG": "test_catalog",
                "CONFIRM_TEST_CATALOG_RESET": "other_catalog",
            },
            "CONFIRM_TEST_CATALOG_RESET exactly",
        ),
        (
            {
                "DATABRICKS_TEST_CATALOG": "test.catalog",
                "CONFIRM_TEST_CATALOG_RESET": "test.catalog",
            },
            "one non-empty catalog name",
        ),
    ],
)
def test_require_confirmed_catalog_rejects_unsafe_environment(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _require_confirmed_catalog(
            environment,
            catalog_environment_variable="DATABRICKS_TEST_CATALOG",
            confirmation_environment_variable="CONFIRM_TEST_CATALOG_RESET",
        )


def test_main_uses_confirmed_catalog_and_returns_reset_result(mocker) -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    spark_factory = mocker.patch(
        "github_engineering_analytics.common.catalog_reset._get_or_create_spark",
        return_value=spark,
    )

    result = main(
        {
            "DATABRICKS_TEST_CATALOG": "test_catalog",
            "CONFIRM_TEST_CATALOG_RESET": "test_catalog",
            "DATABRICKS_CONFIG_PROFILE": "databricks-test",
        }
    )

    spark_factory.assert_called_once_with(
        {
            "DATABRICKS_TEST_CATALOG": "test_catalog",
            "CONFIRM_TEST_CATALOG_RESET": "test_catalog",
            "DATABRICKS_CONFIG_PROFILE": "databricks-test",
        }
    )
    assert result == CatalogResetResult(
        truncated_tables=(),
        skipped_tables=(
            "test_catalog.github_analytics_bronze.github_issues_raw",
            "test_catalog.github_analytics_silver.github_issues",
            "test_catalog.github_analytics_silver.github_users",
            "test_catalog.github_analytics_silver.github_labels",
            "test_catalog.github_analytics_silver.github_issue_labels",
            "test_catalog.github_analytics_control.ingestion_watermark",
            "test_catalog.github_analytics_control.pipeline_runs",
        ),
    )


def test_main_supports_the_confirmed_dev_catalog(mocker) -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    mocker.patch(
        "github_engineering_analytics.common.catalog_reset._get_or_create_spark",
        return_value=spark,
    )

    result = main(
        {
            "DATABRICKS_DEV_CATALOG": "dev_catalog",
            "CONFIRM_DEV_CATALOG_RESET": "dev_catalog",
        },
        catalog_environment_variable="DATABRICKS_DEV_CATALOG",
        confirmation_environment_variable="CONFIRM_DEV_CATALOG_RESET",
    )

    assert result.truncated_tables == ()
    assert result.skipped_tables == (
        "dev_catalog.github_analytics_bronze.github_issues_raw",
        "dev_catalog.github_analytics_silver.github_issues",
        "dev_catalog.github_analytics_silver.github_users",
        "dev_catalog.github_analytics_silver.github_labels",
        "dev_catalog.github_analytics_silver.github_issue_labels",
        "dev_catalog.github_analytics_control.ingestion_watermark",
        "dev_catalog.github_analytics_control.pipeline_runs",
    )


def test_cli_passes_the_dev_environment_variable_pair_to_main(mocker) -> None:
    reset_main = mocker.patch(
        "github_engineering_analytics.common.catalog_reset.main",
    )
    result = CliRunner().invoke(
        app,
        [
            "--catalog-environment-variable",
            "DATABRICKS_DEV_CATALOG",
            "--confirmation-environment-variable",
            "CONFIRM_DEV_CATALOG_RESET",
        ],
    )

    assert result.exit_code == 0
    reset_main.assert_called_once_with(
        catalog_environment_variable="DATABRICKS_DEV_CATALOG",
        confirmation_environment_variable="CONFIRM_DEV_CATALOG_RESET",
        include_all=False,
    )


def test_cli_include_all_flag_is_passed_to_main(mocker) -> None:
    reset_main = mocker.patch(
        "github_engineering_analytics.common.catalog_reset.main",
    )
    result = CliRunner().invoke(app, ["--include-all"])

    assert result.exit_code == 0
    reset_main.assert_called_once_with(
        catalog_environment_variable="DATABRICKS_TEST_CATALOG",
        confirmation_environment_variable="CONFIRM_TEST_CATALOG_RESET",
        include_all=True,
    )


_PYTHON_OWNED_TABLES = (
    "test_catalog.github_analytics_bronze.github_issues_raw",
    "test_catalog.github_analytics_silver.github_issues",
    "test_catalog.github_analytics_silver.github_users",
    "test_catalog.github_analytics_silver.github_labels",
    "test_catalog.github_analytics_silver.github_issue_labels",
    "test_catalog.github_analytics_control.ingestion_watermark",
    "test_catalog.github_analytics_control.pipeline_runs",
)
_GOLD_AND_REPLAY_TABLES = (
    "test_catalog.github_analytics_gold.dim_date",
    "test_catalog.github_analytics_gold.dim_repository",
    "test_catalog.github_analytics_gold.dim_user",
    "test_catalog.github_analytics_gold.dim_label",
    "test_catalog.github_analytics_gold.bridge_issue_label",
    "test_catalog.github_analytics_gold.fact_issue",
    "test_catalog.github_analytics_gold.smoke_connection",
)
_SNAPSHOT_TABLES = (
    "test_catalog.github_analytics_snapshot.snapshot_day_coverage",
    "test_catalog.github_analytics_snapshot.fact_issue_daily_snapshot",
    "test_catalog.github_analytics_snapshot.bridge_issue_label_daily_snapshot",
)
_REPLAY_MANIFEST_TABLE = "test_catalog.github_analytics_control.replay_manifests"
_INCLUDE_ALL_TABLES = (
    *_GOLD_AND_REPLAY_TABLES,
    *_SNAPSHOT_TABLES,
    _REPLAY_MANIFEST_TABLE,
)


def test_default_reset_leaves_gold_and_replay_manifests_alone() -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = True

    result = reset_catalog(spark=spark, catalog="test_catalog")

    assert result.truncated_tables == _PYTHON_OWNED_TABLES
    assert not any(
        "gold" in name or "snapshot" in name or "replay_manifests" in name
        for name in result.truncated_tables
    )


def test_include_all_also_truncates_gold_and_replay_manifests() -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = True

    result = reset_catalog(spark=spark, catalog="test_catalog", include_all=True)

    assert result.truncated_tables == _PYTHON_OWNED_TABLES + _INCLUDE_ALL_TABLES
    assert result.skipped_tables == ()
    assert call(
        "TRUNCATE TABLE `test_catalog`.`github_analytics_gold`.`fact_issue`"
    ) in (spark.sql.call_args_list)
    assert call(
        "TRUNCATE TABLE `test_catalog`.`github_analytics_control`.`replay_manifests`"
    ) in (spark.sql.call_args_list)
    assert call(
        "TRUNCATE TABLE `test_catalog`.`github_analytics_snapshot`"
        ".`fact_issue_daily_snapshot`"
    ) in (spark.sql.call_args_list)


def test_include_all_skips_relations_that_do_not_exist() -> None:
    spark = Mock()
    # DEV has no smoke model and no replay manifests table.
    spark.catalog.tableExists.side_effect = lambda name: (
        not name.endswith(("smoke_connection", "replay_manifests"))
    )

    result = reset_catalog(spark=spark, catalog="test_catalog", include_all=True)

    assert result.skipped_tables == (
        "test_catalog.github_analytics_gold.smoke_connection",
        "test_catalog.github_analytics_control.replay_manifests",
    )
    assert len(result.truncated_tables) == 16


def test_main_forwards_include_all_to_the_reset(mocker) -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    mocker.patch(
        "github_engineering_analytics.common.catalog_reset._get_or_create_spark",
        return_value=spark,
    )

    result = main(
        {
            "DATABRICKS_TEST_CATALOG": "test_catalog",
            "CONFIRM_TEST_CATALOG_RESET": "test_catalog",
        },
        include_all=True,
    )

    assert result.skipped_tables == _PYTHON_OWNED_TABLES + _INCLUDE_ALL_TABLES
