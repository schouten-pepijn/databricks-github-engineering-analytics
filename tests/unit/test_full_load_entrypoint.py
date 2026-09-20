import os
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.bronze.full_load import (
    FullLoadSettings,
    cli,
    main,
    resolve_github_token,
    run_full_load,
)
from github_engineering_analytics.bronze.ingestion import BronzeIngestionResult
from github_engineering_analytics.common.config import PipelineConfig


@pytest.mark.parametrize("github_token", ["test-token", None])
def test_run_full_load_builds_and_invokes_the_ingestion_service(
    mocker,
    github_token: str | None,
) -> None:
    spark = Mock()
    client = Mock()
    writer = Mock()
    ingestion = Mock()
    expected_result = Mock()

    github_client_constructor = mocker.patch(
        "github_engineering_analytics.bronze.full_load.GitHubClient",
        return_value=client,
    )
    writer_constructor = mocker.patch(
        "github_engineering_analytics.bronze.full_load.DeltaBronzeIssueWriter",
        return_value=writer,
    )
    ingestion_constructor = mocker.patch(
        "github_engineering_analytics.bronze.full_load.GitHubIssueBronzeIngestion",
        return_value=ingestion,
    )

    ingestion.full_load.return_value = expected_result

    result = run_full_load(
        spark=spark,
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token=github_token,
        run_id="12345678123456781234567812345678",
        ingested_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
    )

    assert result is expected_result
    github_client_constructor.assert_called_once_with(token=github_token)
    writer_constructor.assert_called_once_with(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )
    ingestion_constructor.assert_called_once_with(
        client=client,
        writer=writer,
    )
    ingestion.full_load.assert_called_once_with(
        owner="octo-org",
        repository="engineering-analytics",
        run_id="12345678123456781234567812345678",
        ingested_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
    )


def test_full_load_settings_reads_required_values_and_optional_token() -> None:
    settings = FullLoadSettings.from_environment(
        {
            "GITHUB_ANALYTICS_CATALOG": "test_catalog",
            "GITHUB_ANALYTICS_OWNER": "octo-org",
            "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
            "GITHUB_TOKEN": "test-token",
        }
    )

    assert settings.catalog == "test_catalog"
    assert settings.owner == "octo-org"
    assert settings.repository == "engineering-analytics"
    assert settings.github_token == "test-token"


def test_full_load_settings_reads_github_token_secret_reference() -> None:
    settings = FullLoadSettings.from_environment(
        {
            "GITHUB_ANALYTICS_CATALOG": "test_catalog",
            "GITHUB_ANALYTICS_OWNER": "octo-org",
            "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
            "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE": "github-secrets",
            "GITHUB_ANALYTICS_TOKEN_SECRET_KEY": "api-token",
        }
    )

    assert settings.github_token is None
    assert settings.github_token_secret_scope == "github-secrets"
    assert settings.github_token_secret_key == "api-token"


@pytest.mark.parametrize(
    "environment",
    [
        {
            "GITHUB_ANALYTICS_CATALOG": "test_catalog",
            "GITHUB_ANALYTICS_OWNER": "octo-org",
            "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
            "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE": "github-secrets",
        },
        {
            "GITHUB_ANALYTICS_CATALOG": "test_catalog",
            "GITHUB_ANALYTICS_OWNER": "octo-org",
            "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
            "GITHUB_ANALYTICS_TOKEN_SECRET_KEY": "api-token",
        },
    ],
)
def test_full_load_settings_rejects_incomplete_secret_reference(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="TOKEN_SECRET"):
        FullLoadSettings.from_environment(environment)


def test_full_load_settings_rejects_direct_token_and_secret_reference() -> None:
    environment = {
        "GITHUB_ANALYTICS_CATALOG": "test_catalog",
        "GITHUB_ANALYTICS_OWNER": "octo-org",
        "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
        "GITHUB_TOKEN": "direct-token",
        "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE": "github-secrets",
        "GITHUB_ANALYTICS_TOKEN_SECRET_KEY": "api-token",
    }

    with pytest.raises(ValueError, match="GITHUB_TOKEN"):
        FullLoadSettings.from_environment(environment)


@pytest.mark.parametrize(
    "missing_variable",
    [
        "GITHUB_ANALYTICS_CATALOG",
        "GITHUB_ANALYTICS_OWNER",
        "GITHUB_ANALYTICS_REPOSITORY",
    ],
)
def test_full_load_settings_rejects_missing_required_values(
    missing_variable: str,
) -> None:
    environment = {
        "GITHUB_ANALYTICS_CATALOG": "test_catalog",
        "GITHUB_ANALYTICS_OWNER": "octo-org",
        "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
    }
    del environment[missing_variable]

    with pytest.raises(ValueError, match=missing_variable):
        FullLoadSettings.from_environment(environment)


def test_main_builds_spark_and_runs_full_load_from_environment(
    mocker,
) -> None:
    spark = Mock()
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token=None,
    )
    expected_result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
    )

    get_or_create = mocker.patch(
        "github_engineering_analytics.bronze.full_load._get_or_create_spark",
        return_value=spark,
    )
    settings_from_environment = mocker.patch(
        "github_engineering_analytics.bronze.full_load.FullLoadSettings.from_environment",
        return_value=settings,
    )
    full_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_tracked_full_load",
        return_value=expected_result,
    )

    main()

    get_or_create.assert_called_once_with()
    settings_from_environment.assert_called_once_with(os.environ)
    full_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token=None,
    )


def test_main_accepts_named_job_parameters(
    mocker,
) -> None:
    spark = Mock()
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token_secret_scope="github-secrets",
        github_token_secret_key="api-token",
    )
    expected_result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
    )

    mocker.patch(
        "github_engineering_analytics.bronze.full_load._get_or_create_spark",
        return_value=spark,
    )
    settings_from_environment = mocker.patch(
        "github_engineering_analytics.bronze.full_load.FullLoadSettings.from_environment",
        return_value=settings,
    )
    resolve_token = mocker.patch(
        "github_engineering_analytics.bronze.full_load.resolve_github_token",
        return_value="resolved-token",
    )
    full_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_tracked_full_load",
        return_value=expected_result,
    )

    main(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        token_secret_scope="github-secrets",
        token_secret_key="api-token",
    )

    settings_from_environment.assert_called_once_with(
        {
            "GITHUB_ANALYTICS_CATALOG": "test_catalog",
            "GITHUB_ANALYTICS_OWNER": "octo-org",
            "GITHUB_ANALYTICS_REPOSITORY": "engineering-analytics",
            "GITHUB_ANALYTICS_TOKEN_SECRET_SCOPE": "github-secrets",
            "GITHUB_ANALYTICS_TOKEN_SECRET_KEY": "api-token",
        }
    )
    resolve_token.assert_called_once_with(
        settings=settings,
        spark=spark,
    )
    full_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token="resolved-token",
    )


def test_cli_passes_databricks_named_parameters_to_main(mocker) -> None:
    mocker.patch(
        "sys.argv",
        [
            "github-engineering-analytics-full-load",
            "--catalog=test_catalog",
            "--owner=psf",
            "--repository=requests",
            "--token-secret-scope=github-engineering-analytics",
            "--token-secret-key=github-token",
        ],
    )
    run_main = mocker.patch("github_engineering_analytics.bronze.full_load.main")

    cli()

    run_main.assert_called_once_with(
        catalog="test_catalog",
        owner="psf",
        repository="requests",
        token_secret_scope="github-engineering-analytics",
        token_secret_key="github-token",
    )


def test_resolve_github_token_returns_direct_token_without_reading_secret() -> None:
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token="direct-token",
    )
    spark = Mock()
    secret_getter = Mock()

    token = resolve_github_token(
        settings=settings,
        spark=spark,
        secret_getter=secret_getter,
    )

    assert token == "direct-token"
    secret_getter.assert_not_called()


def test_resolve_github_token_reads_configured_databricks_secret() -> None:
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token_secret_scope="github-secrets",
        github_token_secret_key="api-token",
    )
    spark = Mock()
    secret_getter = Mock(return_value="resolved-token")

    token = resolve_github_token(
        settings=settings,
        spark=spark,
        secret_getter=secret_getter,
    )

    assert token == "resolved-token"
    secret_getter.assert_called_once_with(
        spark=spark,
        scope="github-secrets",
        key="api-token",
    )


def test_resolve_github_token_returns_none_without_token_configuration() -> None:
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
    )

    assert (
        resolve_github_token(
            settings=settings,
            spark=Mock(),
            secret_getter=Mock(),
        )
        is None
    )


def test_resolve_github_token_rejects_blank_databricks_secret() -> None:
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token_secret_scope="github-secrets",
        github_token_secret_key="api-token",
    )

    with pytest.raises(ValueError, match="github-secrets/api-token"):
        resolve_github_token(
            settings=settings,
            spark=Mock(),
            secret_getter=Mock(return_value="   "),
        )


def test_main_resolves_secret_reference_before_running_full_load(
    mocker,
) -> None:
    spark = Mock()
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token_secret_scope="github-secrets",
        github_token_secret_key="api-token",
    )
    expected_result = BronzeIngestionResult(
        records_extracted=3,
        batches_written=1,
    )

    mocker.patch(
        "github_engineering_analytics.bronze.full_load._get_or_create_spark",
        return_value=spark,
    )
    mocker.patch(
        "github_engineering_analytics.bronze.full_load.FullLoadSettings.from_environment",
        return_value=settings,
    )
    resolve_token = mocker.patch(
        "github_engineering_analytics.bronze.full_load.resolve_github_token",
        return_value="resolved-token",
    )
    full_load = mocker.patch(
        "github_engineering_analytics.bronze.full_load.run_tracked_full_load",
        return_value=expected_result,
    )

    main()

    resolve_token.assert_called_once_with(
        settings=settings,
        spark=spark,
    )
    full_load.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token="resolved-token",
    )


def test_resolve_github_token_adds_context_when_secret_read_fails() -> None:
    settings = FullLoadSettings(
        catalog="test_catalog",
        owner="octo-org",
        repository="engineering-analytics",
        github_token_secret_scope="github-secrets",
        github_token_secret_key="api-token",
    )

    with pytest.raises(
        RuntimeError,
        match="github-secrets/api-token",
    ):
        resolve_github_token(
            settings=settings,
            spark=Mock(),
            secret_getter=Mock(side_effect=RuntimeError("permission denied")),
        )
