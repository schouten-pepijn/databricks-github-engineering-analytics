from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

import github_engineering_analytics.control.cdf_consumer_position_repository as module
from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.cdf_consumer_position_repository import (
    DeltaCdfConsumerPositionRepository,
)

CONFIG = PipelineConfig(catalog="demo_test")
NOW = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
POSITIONS = {"t.issues": 4, "t.issue_labels": 4}


def make_spark(timezone: str = "UTC") -> Mock:
    spark = Mock()
    spark.conf.get.return_value = timezone
    return spark


def make_repository(spark: Mock | None = None) -> DeltaCdfConsumerPositionRepository:
    return DeltaCdfConsumerPositionRepository(spark or make_spark(), CONFIG)


@pytest.fixture
def merge(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Replace DeltaTable so the MERGE chain can be inspected."""
    delta_table = Mock()
    monkeypatch.setattr(module, "DeltaTable", delta_table)
    return delta_table.forName.return_value.alias.return_value.merge


def test_ensure_table_creates_the_schema_and_the_table_idempotently() -> None:
    spark = make_spark()

    make_repository(spark).ensure_table()

    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert statements[0] == (
        "CREATE SCHEMA IF NOT EXISTS `demo_test`.`github_analytics_control`"
    )
    create = statements[1]
    assert create.startswith("CREATE TABLE IF NOT EXISTS ")
    for column in (
        "consumer_name STRING NOT NULL",
        "source_table STRING NOT NULL",
        "last_version BIGINT NOT NULL",
        "last_run_id STRING NOT NULL",
        "updated_at TIMESTAMP NOT NULL",
    ):
        assert column in create


def collected(spark: Mock) -> Mock:
    return spark.table.return_value.where.return_value.select.return_value.collect


def test_no_rows_means_no_positions() -> None:
    spark = make_spark()
    collected(spark).return_value = []

    assert make_repository(spark).get_positions("gold_candidate") == {}


def test_positions_are_returned_per_source_table() -> None:
    spark = make_spark()
    collected(spark).return_value = [
        {"source_table": "t.issues", "last_version": 4},
        {"source_table": "t.issue_labels", "last_version": 7},
    ]

    result = make_repository(spark).get_positions("gold_candidate")

    assert result == {"t.issues": 4, "t.issue_labels": 7}


def test_a_duplicate_key_is_an_error() -> None:
    spark = make_spark()
    collected(spark).return_value = [
        {"source_table": "t.issues", "last_version": 4},
        {"source_table": "t.issues", "last_version": 5},
    ]

    with pytest.raises(RuntimeError, match="logical key"):
        make_repository(spark).get_positions("gold_candidate")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"positions": {}}, "positions must not be empty"),
        ({"consumer_name": " "}, "consumer_name"),
        ({"run_id": ""}, "run_id"),
        ({"positions": {"t.issues": -1}}, "t.issues"),
        ({"positions": {"t.issues": True}}, "t.issues"),
        ({"positions": {"t.issues": 1.5}}, "t.issues"),
        ({"positions": {" ": 1}}, "source table"),
        ({"committed_at": datetime(2026, 10, 21, 9, 0)}, "timezone-aware"),
    ],
)
def test_bad_input_is_refused_before_anything_is_written(
    overrides: dict[str, object], message: str, merge: Mock
) -> None:
    spark = make_spark()
    arguments: dict[str, object] = {
        "consumer_name": "gold_candidate",
        "positions": POSITIONS,
        "run_id": "run_1",
        "committed_at": NOW,
    }
    arguments.update(overrides)

    with pytest.raises(ValueError, match=message):
        make_repository(spark).commit_positions(**arguments)  # pyright: ignore[reportArgumentType]

    spark.createDataFrame.assert_not_called()
    merge.assert_not_called()


def test_a_non_utc_session_is_refused() -> None:
    repository = make_repository(make_spark(timezone="Europe/Amsterdam"))

    with pytest.raises(RuntimeError, match="UTC"):
        repository.commit_positions("gold_candidate", POSITIONS, "run_1", NOW)


def test_all_positions_go_into_one_merge(merge: Mock) -> None:
    spark = make_spark()

    make_repository(spark).commit_positions("gold_candidate", POSITIONS, "run_1", NOW)

    rows = spark.createDataFrame.call_args.args[0]
    assert rows == [
        ("gold_candidate", "t.issues", 4, "run_1", NOW),
        ("gold_candidate", "t.issue_labels", 4, "run_1", NOW),
    ]
    merge.assert_called_once()
    chain = merge.return_value.whenMatchedUpdate.return_value
    chain.whenNotMatchedInsert.return_value.execute.assert_called_once()


def test_the_merge_is_keyed_on_both_columns_and_never_moves_back(
    merge: Mock,
) -> None:
    make_repository().commit_positions("gold_candidate", POSITIONS, "run_1", NOW)

    condition = merge.call_args.args[1]
    assert "target.consumer_name = source.consumer_name" in condition
    assert "target.source_table = source.source_table" in condition
    update = merge.return_value.whenMatchedUpdate.call_args.kwargs
    assert update["condition"] == "source.last_version >= target.last_version"
    assert set(update["set"]) == {"last_version", "last_run_id", "updated_at"}
