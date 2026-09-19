from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.pipeline_run import PipelineRun
from github_engineering_analytics.control.pipeline_run_repository import (
    DeltaPipelineRunRepository,
)
from github_engineering_analytics.control.watermark import Watermark


def make_repository(
    session_timezone: str = "UTC",
) -> tuple[Mock, DeltaPipelineRunRepository]:
    """Build a repository with a mocked Spark session for local unit tests."""
    spark = Mock()
    spark.conf.get.return_value = session_timezone

    repository = DeltaPipelineRunRepository(
        spark=spark,
        config=PipelineConfig(catalog="test_catalog"),
    )

    return spark, repository


def configure_existing_run_statuses(spark: Mock, statuses: list[str]) -> None:
    """Configure the chained mocked Spark lookup used before a finish update."""
    status_lookup = spark.table.return_value
    limited_status_lookup = (
        status_lookup.where.return_value.select.return_value.limit.return_value
    )
    limited_status_lookup.collect.return_value = [
        {"status": status} for status in statuses
    ]


def configure_delta_update(delta_table: Mock, spark: Mock) -> Mock:
    """Configure mocked source and Delta merge objects for a terminal update."""
    source = Mock()
    source.alias.return_value = Mock()
    spark.createDataFrame.return_value = source

    target = Mock()
    target.alias.return_value = Mock()
    delta_table.forName.return_value = target

    merge_builder = Mock()
    target.alias.return_value.merge.return_value = merge_builder

    update_builder = Mock()
    merge_builder.whenMatchedUpdate.return_value = update_builder

    return update_builder


def test_ensure_table_creates_control_schema_and_run_table() -> None:
    spark, repository = make_repository()

    repository.ensure_table()

    assert spark.sql.call_count == 2

    schema_statement = spark.sql.call_args_list[0].args[0]
    table_statement = spark.sql.call_args_list[1].args[0]

    assert schema_statement == (
        "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`github_analytics_control`"
    )
    expected_table_name = (
        "CREATE TABLE IF NOT EXISTS "
        "`test_catalog`.`github_analytics_control`.`pipeline_runs`"
    )
    assert expected_table_name in table_statement
    assert "run_id STRING NOT NULL" in table_statement
    assert "status STRING NOT NULL" in table_statement
    assert "watermark_before_overlap_seconds BIGINT" in table_statement
    assert "candidate_watermark_overlap_seconds BIGINT" in table_statement
    assert table_statement.endswith(") USING DELTA")


def test_ensure_table_rejects_non_utc_spark_session() -> None:
    spark, repository = make_repository(session_timezone="Europe/Amsterdam")

    with pytest.raises(
        RuntimeError,
        match="Spark session timezone must be UTC",
    ):
        repository.ensure_table()

    spark.sql.assert_not_called()


@patch(
    "github_engineering_analytics.control.pipeline_run_repository.DeltaTable",
    create=True,
)
def test_record_started_inserts_running_run_once(
    delta_table: Mock,
) -> None:
    spark, repository = make_repository()

    run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=Watermark(
            value=datetime(2026, 9, 19, 11, 55, tzinfo=UTC),
        ),
    )

    source = Mock()
    source_alias = Mock()
    source.alias.return_value = source_alias
    spark.createDataFrame.return_value = source

    target = Mock()
    target_alias = Mock()
    target.alias.return_value = target_alias
    delta_table.forName.return_value = target

    merge_builder = Mock()
    target_alias.merge.return_value = merge_builder

    insert_builder = Mock()
    merge_builder.whenNotMatchedInsert.return_value = insert_builder

    repository.record_started(run)

    spark.createDataFrame.assert_called_once_with(
        [
            (
                "run-123",
                "github",
                "issues",
                "running",
                datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
                None,
                datetime(2026, 9, 19, 11, 55, tzinfo=UTC),
                300,
                None,
                None,
                None,
            )
        ],
        schema=DeltaPipelineRunRepository._ROW_SCHEMA,
    )
    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_control.pipeline_runs",
    )
    target_alias.merge.assert_called_once_with(
        source_alias,
        "target.run_id = source.run_id",
    )
    merge_builder.whenNotMatchedInsert.assert_called_once()
    insert_values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert insert_values["watermark_before_overlap_seconds"] == (
        "source.watermark_before_overlap_seconds"
    )
    assert insert_values["candidate_watermark_value"] == (
        "source.candidate_watermark_value"
    )
    assert insert_values["candidate_watermark_overlap_seconds"] == (
        "source.candidate_watermark_overlap_seconds"
    )
    insert_builder.execute.assert_called_once()


@patch(
    "github_engineering_analytics.control.pipeline_run_repository.DeltaTable",
    create=True,
)
def test_record_started_rejects_terminal_run_before_writing(
    delta_table: Mock,
) -> None:
    spark, repository = make_repository()

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )
    succeeded_run = running_run.succeed(
        candidate_watermark=None,
        finished_at=datetime(2026, 9, 19, 12, 5, tzinfo=UTC),
    )

    with pytest.raises(
        ValueError,
        match="record_started requires a running pipeline run",
    ):
        repository.record_started(succeeded_run)

    spark.createDataFrame.assert_not_called()
    delta_table.forName.assert_not_called()


def test_record_finished_rejects_running_run_before_writing() -> None:
    spark, repository = make_repository()

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )

    with pytest.raises(
        ValueError,
        match="record_finished requires a succeeded or failed pipeline run",
    ):
        repository.record_finished(running_run)

    spark.createDataFrame.assert_not_called()
    spark.table.assert_not_called()


@patch("github_engineering_analytics.control.pipeline_run_repository.F")
@patch("github_engineering_analytics.control.pipeline_run_repository.DeltaTable")
def test_record_finished_updates_running_row_to_succeeded(
    delta_table: Mock,
    functions: Mock,
) -> None:
    spark, repository = make_repository()
    functions.col.return_value = Mock()
    configure_existing_run_statuses(spark, ["running"])
    update_builder = configure_delta_update(delta_table, spark)

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=Watermark(
            value=datetime(2026, 9, 19, 11, 55, tzinfo=UTC),
        ),
    )
    succeeded_run = running_run.succeed(
        candidate_watermark=Watermark(
            value=datetime(2026, 9, 19, 12, 3, tzinfo=UTC),
            overlap_seconds=600,
        ),
        finished_at=datetime(2026, 9, 19, 12, 5, tzinfo=UTC),
    )

    repository.record_finished(succeeded_run)

    spark.table.assert_called_once_with(
        "test_catalog.github_analytics_control.pipeline_runs"
    )
    delta_table.forName.assert_called_once_with(
        spark,
        "test_catalog.github_analytics_control.pipeline_runs",
    )
    merge_builder = (
        delta_table.forName.return_value.alias.return_value.merge.return_value
    )
    merge_builder.whenMatchedUpdate.assert_called_once_with(
        condition="target.status = 'running'",
        set={
            "status": "source.status",
            "finished_at": "source.finished_at",
            "candidate_watermark_value": "source.candidate_watermark_value",
            "candidate_watermark_overlap_seconds": (
                "source.candidate_watermark_overlap_seconds"
            ),
            "error_message": "source.error_message",
        },
    )
    update_builder.execute.assert_called_once()


@patch("github_engineering_analytics.control.pipeline_run_repository.F")
@patch("github_engineering_analytics.control.pipeline_run_repository.DeltaTable")
def test_record_finished_updates_running_row_to_failed(
    delta_table: Mock,
    functions: Mock,
) -> None:
    spark, repository = make_repository()
    functions.col.return_value = Mock()
    configure_existing_run_statuses(spark, ["running"])
    update_builder = configure_delta_update(delta_table, spark)

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )
    failed_run = running_run.fail(
        error_message="GitHub request timed out",
        finished_at=datetime(2026, 9, 19, 12, 5, tzinfo=UTC),
    )

    repository.record_finished(failed_run)

    created_row = spark.createDataFrame.call_args.args[0][0]
    assert created_row[3] == "failed"
    assert created_row[5] == datetime(2026, 9, 19, 12, 5, tzinfo=UTC)
    assert created_row[10] == "GitHub request timed out"
    update_builder.execute.assert_called_once()


@patch("github_engineering_analytics.control.pipeline_run_repository.F")
def test_record_finished_rejects_unknown_run_id(functions: Mock) -> None:
    spark, repository = make_repository()
    functions.col.return_value = Mock()
    configure_existing_run_statuses(spark, [])

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )
    succeeded_run = running_run.succeed(
        candidate_watermark=None,
        finished_at=datetime(2026, 9, 19, 12, 5, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="Pipeline run not found"):
        repository.record_finished(succeeded_run)

    spark.createDataFrame.assert_not_called()


@pytest.mark.parametrize("statuses", [["succeeded"], ["running", "running"]])
@patch("github_engineering_analytics.control.pipeline_run_repository.F")
def test_record_finished_rejects_invalid_stored_run_state(
    functions: Mock,
    statuses: list[str],
) -> None:
    spark, repository = make_repository()
    functions.col.return_value = Mock()
    configure_existing_run_statuses(spark, statuses)

    running_run = PipelineRun(
        run_id="run-123",
        source_name="github",
        entity_name="issues",
        started_at=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        watermark_before=None,
    )
    succeeded_run = running_run.succeed(
        candidate_watermark=None,
        finished_at=datetime(2026, 9, 19, 12, 5, tzinfo=UTC),
    )

    expected_error = (
        "Pipeline run must be running before it can finish"
        if statuses == ["succeeded"]
        else "duplicate run_id"
    )

    with pytest.raises(
        RuntimeError if len(statuses) > 1 else ValueError, match=expected_error
    ):
        repository.record_finished(succeeded_run)

    spark.createDataFrame.assert_not_called()
