from unittest.mock import Mock

from typer.testing import CliRunner

from github_engineering_analytics.orchestration.silver_replay_entrypoint import (
    app,
    replay_silver_main,
)

MODULE = "github_engineering_analytics.orchestration.silver_replay_entrypoint"


def test_cli_passes_the_wheel_parameters_to_the_replay(mocker) -> None:
    replay = mocker.patch(f"{MODULE}.replay_silver_main")

    result = CliRunner().invoke(
        app,
        [
            "--catalog=demo_test",
            "--manifest-id=manifest-001",
            "--execution-id=run_1",
        ],
    )

    assert result.exit_code == 0
    replay.assert_called_once_with(
        catalog="demo_test", manifest_id="manifest-001", execution_id="run_1"
    )


def test_cli_requires_all_three_parameters(mocker) -> None:
    replay = mocker.patch(f"{MODULE}.replay_silver_main")

    result = CliRunner().invoke(app, ["--catalog=demo_test"])

    assert result.exit_code != 0
    replay.assert_not_called()


def test_main_uses_the_active_spark_session(mocker) -> None:
    spark = Mock()
    get_or_create = mocker.patch(f"{MODULE}._get_or_create_spark", return_value=spark)
    run = mocker.patch(f"{MODULE}.run_silver_replay", return_value={"tables": {}})

    replay_silver_main(
        catalog="demo_test", manifest_id="manifest-001", execution_id="run_1"
    )

    get_or_create.assert_called_once_with()
    run.assert_called_once_with(
        spark=spark,
        catalog="demo_test",
        manifest_id="manifest-001",
        execution_id="run_1",
    )
