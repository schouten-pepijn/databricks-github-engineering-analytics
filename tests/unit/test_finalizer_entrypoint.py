from unittest.mock import Mock

from typer.testing import CliRunner

from github_engineering_analytics.orchestration.finalizer_entrypoint import (
    failure_app,
    success_app,
)


def test_success_cli_passes_wheel_parameters_to_finalizer(mocker) -> None:
    finalize = mocker.patch(
        "github_engineering_analytics.orchestration.finalizer_entrypoint.finalize_success_main"
    )

    result = CliRunner().invoke(
        success_app,
        ["--catalog=test_catalog", "--run-id=job-run-123"],
    )

    assert result.exit_code == 0
    finalize.assert_called_once_with(catalog="test_catalog", run_id="job-run-123")


def test_failure_cli_passes_wheel_parameters_to_finalizer(mocker) -> None:
    finalize = mocker.patch(
        "github_engineering_analytics.orchestration.finalizer_entrypoint.finalize_failure_main"
    )

    result = CliRunner().invoke(
        failure_app,
        [
            "--catalog=test_catalog",
            "--run-id=job-run-123",
            "--failure-reason=Gold failed",
        ],
    )

    assert result.exit_code == 0
    finalize.assert_called_once_with(
        catalog="test_catalog",
        run_id="job-run-123",
        failure_reason="Gold failed",
    )


def test_finalizer_entrypoint_uses_active_spark_session(mocker) -> None:
    spark = Mock()
    get_or_create = mocker.patch(
        "github_engineering_analytics.orchestration.finalizer_entrypoint._get_or_create_spark",
        return_value=spark,
    )
    finalize = mocker.patch(
        "github_engineering_analytics.orchestration.finalizer_entrypoint.finalize_successful_run"
    )

    from github_engineering_analytics.orchestration.finalizer_entrypoint import (
        finalize_success_main,
    )

    finalize_success_main(catalog="test_catalog", run_id="job-run-123")

    get_or_create.assert_called_once_with()
    finalize.assert_called_once_with(
        spark=spark,
        catalog="test_catalog",
        run_id="job-run-123",
    )
