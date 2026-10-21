"""Run one incremental Gold batch and move the consumer positions only on success.

The order is the whole point of this module:

1. read the stored positions and the current Delta versions of the Silver tables
2. plan one change range per table (from the position, or from a baseline)
3. write the workset, build the candidate, compare it with a full build
4. only then commit the positions, in one MERGE

Anything that fails before step 4 leaves the positions where they were, so the
next run reads the same range again. Nothing is skipped.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from github_engineering_analytics.control.cdf_consumer_position_repository import (
    DeltaCdfConsumerPositionRepository,
)
from github_engineering_analytics.silver.changed_issues import ChangeRange

CONSUMER_NAME = "gold_candidate"


class NothingToProcessError(RuntimeError):
    """Raised when a Silver table has no new Delta version since the position."""


class SimulatedFailureError(RuntimeError):
    """Raised on purpose by --fail-after-write to prove the recovery path."""


@dataclass(frozen=True)
class BatchResult:
    """What one successful batch did."""

    ranges: Mapping[str, ChangeRange]
    workset_rows: int
    committed_positions: Mapping[str, int]


def plan_ranges(
    *,
    stored: Mapping[str, int],
    baseline: Mapping[str, int] | None,
    current: Mapping[str, int],
) -> dict[str, ChangeRange]:
    """Return the change range to read per table: position + 1 up to current.

    The first run has no stored position, so it needs an explicit baseline: the
    version the Gold candidate was last built from. Starting silently at version
    0 would replay history that is not in the feed (or not in the candidate).
    """
    if stored:
        if baseline is not None:
            # A baseline next to stored positions would silently reset them.
            raise ValueError("baseline is only allowed when no position is stored")
        if set(stored) != set(current):
            raise ValueError(
                f"stored positions {sorted(stored)} do not match tables "
                f"{sorted(current)}"
            )
        starts = dict(stored)
    elif baseline is not None:
        if set(baseline) != set(current):
            raise ValueError("baseline must name exactly the same tables")
        starts = dict(baseline)
    else:
        raise ValueError("no stored position: pass a baseline for the first run")

    ranges: dict[str, ChangeRange] = {}
    for table, end in current.items():
        start = starts[table] + 1
        if end < start:
            raise NothingToProcessError(
                f"{table} has no new version after {starts[table]} (current {end})"
            )
        ranges[table] = ChangeRange(start, end)
    return ranges


def run_batch(
    *,
    positions: DeltaCdfConsumerPositionRepository,
    current_versions: Mapping[str, int],
    baseline: Mapping[str, int] | None,
    write_workset: Callable[[Mapping[str, ChangeRange]], int],
    build_candidate: Callable[[], None],
    compare: Callable[[], None],
    run_id: str,
    now: datetime,
    fail_after_write: bool = False,
) -> BatchResult:
    """Process one batch. The positions move last, and only if all else passed.

    ``current_versions`` is read by the caller BEFORE the batch starts and is
    exactly what gets committed. A commit that lands in Silver while the batch
    runs is therefore not covered by the new position, and is read next time.
    """
    ranges = plan_ranges(
        stored=positions.get_positions(CONSUMER_NAME),
        baseline=baseline,
        current=current_versions,
    )

    workset_rows = write_workset(ranges)
    build_candidate()
    compare()  # raises when the candidate differs from the full build

    if fail_after_write:
        # The candidate is written and compared, but the positions are not moved.
        raise SimulatedFailureError("stopped after the candidate write, as asked")

    positions.commit_positions(CONSUMER_NAME, current_versions, run_id, now)
    return BatchResult(ranges, workset_rows, dict(current_versions))
