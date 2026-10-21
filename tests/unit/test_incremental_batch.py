from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.orchestration.incremental_batch import (
    CONSUMER_NAME,
    NothingToProcessError,
    SimulatedFailureError,
    plan_ranges,
    run_batch,
)
from github_engineering_analytics.silver.changed_issues import ChangeRange

NOW = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
CURRENT = {"t.issues": 4, "t.labels": 6}
BASELINE = {"t.issues": 3, "t.labels": 5}


# ---- plan_ranges -----------------------------------------------------------


def test_the_first_run_starts_after_the_baseline() -> None:
    ranges = plan_ranges(stored={}, baseline=BASELINE, current=CURRENT)

    assert ranges == {"t.issues": ChangeRange(4, 4), "t.labels": ChangeRange(6, 6)}


def test_later_runs_start_after_the_stored_position() -> None:
    stored = {"t.issues": 2, "t.labels": 3}

    ranges = plan_ranges(stored=stored, baseline=None, current=CURRENT)

    assert ranges == {"t.issues": ChangeRange(3, 4), "t.labels": ChangeRange(4, 6)}


def test_the_first_run_without_a_baseline_is_refused() -> None:
    with pytest.raises(ValueError, match="pass a baseline"):
        plan_ranges(stored={}, baseline=None, current=CURRENT)


def test_a_baseline_next_to_stored_positions_is_refused() -> None:
    with pytest.raises(ValueError, match="only allowed when no position"):
        plan_ranges(stored=BASELINE, baseline=BASELINE, current=CURRENT)


def test_stored_positions_must_cover_exactly_the_tables() -> None:
    with pytest.raises(ValueError, match="do not match"):
        plan_ranges(stored={"t.issues": 3}, baseline=None, current=CURRENT)


def test_a_table_without_a_new_version_is_named() -> None:
    stored = {"t.issues": 4, "t.labels": 3}

    with pytest.raises(NothingToProcessError, match="t.issues"):
        plan_ranges(stored=stored, baseline=None, current=CURRENT)


# ---- run_batch -------------------------------------------------------------


class Harness:
    """Collects the order of the steps, so a test can assert on the sequence."""

    def __init__(self, stored: dict[str, int] | None = None) -> None:
        self.calls: list[str] = []
        self.positions = Mock()
        self.positions.get_positions.return_value = stored or {}
        self.positions.commit_positions.side_effect = lambda *a: self.calls.append(
            "commit"
        )

    def run(
        self,
        *,
        compare_error: Exception | None = None,
        fail_after_write: bool = False,
        baseline: dict[str, int] | None = BASELINE,
    ):
        def write_workset(ranges: object) -> int:
            self.calls.append("workset")
            return 4

        def build_candidate() -> None:
            self.calls.append("candidate")

        def compare() -> None:
            self.calls.append("compare")
            if compare_error is not None:
                raise compare_error

        return run_batch(
            positions=self.positions,
            current_versions=CURRENT,
            baseline=baseline,
            write_workset=write_workset,
            build_candidate=build_candidate,
            compare=compare,
            run_id="run_1",
            now=NOW,
            fail_after_write=fail_after_write,
        )


def test_a_successful_batch_moves_the_positions_last() -> None:
    harness = Harness()

    result = harness.run()

    assert harness.calls == ["workset", "candidate", "compare", "commit"]
    harness.positions.commit_positions.assert_called_once_with(
        CONSUMER_NAME, CURRENT, "run_1", NOW
    )
    assert result.workset_rows == 4
    assert result.committed_positions == CURRENT


def test_a_failed_comparison_leaves_the_positions_alone() -> None:
    harness = Harness()

    with pytest.raises(RuntimeError, match="differs"):
        harness.run(compare_error=RuntimeError("candidate differs"))

    assert harness.calls == ["workset", "candidate", "compare"]
    harness.positions.commit_positions.assert_not_called()


def test_fail_after_write_stops_after_the_compare_and_before_the_commit() -> None:
    harness = Harness()

    with pytest.raises(SimulatedFailureError):
        harness.run(fail_after_write=True)

    assert harness.calls == ["workset", "candidate", "compare"]
    harness.positions.commit_positions.assert_not_called()


def test_a_bad_plan_stops_before_anything_is_built() -> None:
    harness = Harness()

    with pytest.raises(ValueError, match="pass a baseline"):
        harness.run(baseline=None)

    assert harness.calls == []


def test_the_second_run_reads_from_the_stored_position() -> None:
    harness = Harness(stored={"t.issues": 4, "t.labels": 6})
    current_moved = {"t.issues": 5, "t.labels": 7}
    seen: list[object] = []

    run_batch(
        positions=harness.positions,
        current_versions=current_moved,
        baseline=None,
        write_workset=lambda ranges: seen.append(ranges) or 1,
        build_candidate=lambda: None,
        compare=lambda: None,
        run_id="run_2",
        now=NOW,
    )

    assert seen == [{"t.issues": ChangeRange(5, 5), "t.labels": ChangeRange(7, 7)}]
