from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from github_engineering_analytics.bronze.manifest_input import (
    ManifestInputUnavailableError,
)
from github_engineering_analytics.control.replay_manifest import ReplayManifest
from github_engineering_analytics.orchestration.silver_replay import (
    candidate_tables,
    replay_to_tables,
    run_silver_replay,
)

MODULE = "github_engineering_analytics.orchestration.silver_replay"
CATALOG = "demo_test"
BRONZE = f"{CATALOG}.github_analytics_bronze.github_issues_raw"
ROWS = {"issues": 3, "users": 2, "labels": 1, "issue_labels": 4}


def make_manifest(bronze_table: str = BRONZE) -> ReplayManifest:
    return ReplayManifest(
        manifest_id="manifest-001",
        repository_owner="psf",
        repository_name="requests",
        bronze_table=bronze_table,
        bronze_version=7,
        successful_run_ids=("run-1",),
        cutoff_at=datetime(2026, 10, 13, 12, 0, tzinfo=UTC),
        code_commit="abc1234",
        transformation_version="v1",
    )


def fake_frame(rows: int) -> Mock:
    """A stand-in for a Spark DataFrame: two withColumn calls, then count/write."""
    frame = Mock()
    frame.withColumn.return_value.withColumn.return_value.count.return_value = rows
    return frame


def fake_replay() -> SimpleNamespace:
    return SimpleNamespace(**{entity: fake_frame(n) for entity, n in ROWS.items()})


def candidate_of(replay: SimpleNamespace, entity: str) -> Mock:
    return getattr(replay, entity).withColumn.return_value.withColumn.return_value


def make_spark(tables: dict[str, str]) -> Mock:
    """A Spark mock whose table(...).count() matches the rows each table should get."""
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    by_name = {name: ROWS[entity] for entity, name in tables.items()}
    spark.table.side_effect = lambda name: Mock(count=Mock(return_value=by_name[name]))
    return spark


# --- candidate_tables ---------------------------------------------------------


def test_candidate_tables_names_one_table_per_entity() -> None:
    assert candidate_tables(CATALOG, "run_1") == {
        "issues": "demo_test.github_analytics_v2min.s2_replay_issues_run_1",
        "users": "demo_test.github_analytics_v2min.s2_replay_users_run_1",
        "labels": "demo_test.github_analytics_v2min.s2_replay_labels_run_1",
        "issue_labels": "demo_test.github_analytics_v2min.s2_replay_issue_labels_run_1",
    }


@pytest.mark.parametrize(
    "execution_id", ["", "Upper", "has space", "semi;colon", "dash-id", "x" * 41]
)
def test_candidate_tables_rejects_unsafe_execution_ids(execution_id: str) -> None:
    with pytest.raises(ValueError, match="execution_id must contain"):
        candidate_tables(CATALOG, execution_id)


@pytest.mark.parametrize(
    "catalog",
    ["github_engineering_analytics_dev", "github_engineering_analytics_prod", "test"],
)
def test_candidate_tables_rejects_catalogs_that_are_not_test(catalog: str) -> None:
    with pytest.raises(ValueError, match="only run in a \\*_test catalog"):
        candidate_tables(catalog, "run_1")


# --- replay_to_tables ---------------------------------------------------------


@patch(f"{MODULE}.DeltaTable")
@patch(f"{MODULE}.F")
@patch(f"{MODULE}.replay_silver")
@patch(f"{MODULE}.read_manifest_input")
def test_all_checks_run_before_the_first_write(read, replay, functions, delta) -> None:
    targets = candidate_tables(CATALOG, "run_1")
    spark = make_spark(targets)
    replay.return_value = fake_replay()
    delta.forName.return_value.history.return_value.collect.return_value = [
        {"version": 0}
    ]
    order: list[str] = []
    read.side_effect = lambda *args: order.append("read-input") or Mock()
    spark.sql.side_effect = lambda *args: order.append("create-schema")
    spark.catalog.tableExists.side_effect = lambda name: (
        order.append("exists?") or False
    )

    written = replay_to_tables(spark, make_manifest(), CATALOG, "run_1")

    # The input is read and the targets are checked before the schema is created.
    assert order[0] == "read-input"
    assert order.index("create-schema") > max(
        i for i, step in enumerate(order) if step == "exists?"
    )
    assert {entity: info["rows"] for entity, info in written.items()} == ROWS
    assert {info["version"] for info in written.values()} == {0}


@patch(f"{MODULE}.DeltaTable")
@patch(f"{MODULE}.F")
@patch(f"{MODULE}.replay_silver")
@patch(f"{MODULE}.read_manifest_input")
def test_each_entity_is_written_with_mode_error_to_a_quoted_name(
    read, replay, functions, delta
) -> None:
    targets = candidate_tables(CATALOG, "run_1")
    spark = make_spark(targets)
    replayed = fake_replay()
    replay.return_value = replayed
    delta.forName.return_value.history.return_value.collect.return_value = [
        {"version": 0}
    ]

    replay_to_tables(spark, make_manifest(), CATALOG, "run_1")

    for entity, table in targets.items():
        write = candidate_of(replayed, entity).write.format.return_value
        write.mode.assert_called_once_with("error")
        quoted = ".".join(f"`{part}`" for part in table.split("."))
        write.mode.return_value.saveAsTable.assert_called_once_with(quoted)


@patch(f"{MODULE}.F")
@patch(f"{MODULE}.replay_silver")
@patch(f"{MODULE}.read_manifest_input")
def test_an_existing_target_stops_the_replay_before_any_write(
    read, replay, functions
) -> None:
    targets = candidate_tables(CATALOG, "run_1")
    spark = make_spark(targets)
    spark.catalog.tableExists.side_effect = lambda name: name == targets["labels"]
    replayed = fake_replay()
    replay.return_value = replayed

    with pytest.raises(ValueError, match="already exist"):
        replay_to_tables(spark, make_manifest(), CATALOG, "run_1")

    spark.sql.assert_not_called()
    for entity in ROWS:
        candidate_of(replayed, entity).write.format.assert_not_called()


@patch(f"{MODULE}.read_manifest_input")
def test_an_unreadable_input_leaves_nothing_behind(read) -> None:
    spark = Mock()
    read.side_effect = ManifestInputUnavailableError("version expired")

    with pytest.raises(ManifestInputUnavailableError):
        replay_to_tables(spark, make_manifest(), CATALOG, "run_1")

    spark.sql.assert_not_called()


@patch(f"{MODULE}.DeltaTable")
@patch(f"{MODULE}.F")
@patch(f"{MODULE}.replay_silver")
@patch(f"{MODULE}.read_manifest_input")
def test_a_failure_halfway_names_the_tables_to_remove(
    read, replay, functions, delta
) -> None:
    targets = candidate_tables(CATALOG, "run_1")
    spark = make_spark(targets)
    replayed = fake_replay()
    replay.return_value = replayed
    delta.forName.return_value.history.return_value.collect.return_value = [
        {"version": 0}
    ]
    created: set[str] = set()
    spark.catalog.tableExists.side_effect = lambda name: name in created

    def write_issues(quoted: str) -> None:
        created.add(quoted.replace("`", ""))

    def fail_on_users(quoted: str) -> None:
        raise RuntimeError("disk full")

    def saver(entity: str) -> Mock:
        """The mock behind df.write.format(...).mode(...) for one entity."""
        return candidate_of(
            replayed, entity
        ).write.format.return_value.mode.return_value

    saver("issues").saveAsTable.side_effect = write_issues
    saver("users").saveAsTable.side_effect = fail_on_users

    with pytest.raises(RuntimeError, match="after writing 1 of 4") as info:
        replay_to_tables(spark, make_manifest(), CATALOG, "run_1")

    assert targets["issues"] in str(info.value)  # written, so listed for cleanup
    assert targets["users"] not in str(info.value)  # never created
    assert isinstance(info.value.__cause__, RuntimeError)


# --- run_silver_replay --------------------------------------------------------


@pytest.mark.parametrize(
    ("catalog", "execution_id", "message"),
    [
        ("demo_prod", "run_1", "only run in a"),
        (CATALOG, "Bad id", "execution_id must contain"),
    ],
)
def test_bad_inputs_are_rejected_before_spark_is_touched(
    catalog: str, execution_id: str, message: str
) -> None:
    spark = Mock()

    with pytest.raises(ValueError, match=message):
        run_silver_replay(
            spark=spark,
            catalog=catalog,
            manifest_id="manifest-001",
            execution_id=execution_id,
        )

    assert spark.mock_calls == []


@patch(f"{MODULE}.replay_to_tables")
@patch(f"{MODULE}.DeltaReplayManifestRepository")
def test_a_manifest_from_another_catalog_is_refused(repository, replay) -> None:
    other = make_manifest("other_test.github_analytics_bronze.github_issues_raw")
    repository.return_value.get.return_value = other

    with pytest.raises(ValueError, match="belongs to other_test"):
        run_silver_replay(
            spark=Mock(),
            catalog=CATALOG,
            manifest_id="manifest-001",
            execution_id="run_1",
        )

    replay.assert_not_called()


@patch(f"{MODULE}.replay_to_tables")
@patch(f"{MODULE}.DeltaReplayManifestRepository")
def test_a_valid_replay_returns_what_was_written(repository, replay) -> None:
    spark = Mock()
    manifest = make_manifest()
    repository.return_value.get.return_value = manifest
    replay.return_value = {"issues": {"table": "t", "rows": 3, "version": 0}}

    result = run_silver_replay(
        spark=spark, catalog=CATALOG, manifest_id="manifest-001", execution_id="run_1"
    )

    replay.assert_called_once_with(spark, manifest, CATALOG, "run_1")
    assert result == {
        "manifest_id": "manifest-001",
        "execution_id": "run_1",
        "tables": {"issues": {"table": "t", "rows": 3, "version": 0}},
    }
