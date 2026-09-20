from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID

import pytest

from github_engineering_analytics.bronze.full_load import (
    FullLoadSettings,
    run_full_load,
)
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
        run_id_factory=lambda: UUID("12345678-1234-5678-1234-567812345678"),
        clock=lambda: datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
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
