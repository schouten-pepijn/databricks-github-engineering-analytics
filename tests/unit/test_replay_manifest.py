from datetime import UTC, datetime, timedelta, timezone

import pytest

from github_engineering_analytics.control.replay_manifest import ReplayManifest


def test_manifest_normalizes_cutoff_to_utc() -> None:
    manifest = ReplayManifest(
        manifest_id="manifest-20261006-001",
        repository_owner="psf",
        repository_name="requests",
        bronze_table="test_catalog.github_analytics_bronze.github_issues_raw",
        bronze_version=42,
        successful_run_ids=("run-001", "run-002"),
        cutoff_at=datetime(
            2026,
            10,
            6,
            12,
            0,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        code_commit="787ea43",
        transformation_version="v1",
    )

    assert manifest.cutoff_at == datetime(2026, 10, 6, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("run_ids", "error"),
    [
        ((), "successful_run_ids must not be empty"),
        (("run-001", "run-001"), "successful_run_ids must be unique"),
        (("run-001", ""), "successful_run_ids must not contain empty values"),
    ],
)
def test_manifest_rejects_invalid_run_ids(
    run_ids: tuple[str, ...],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        ReplayManifest(
            manifest_id="manifest-20261006-001",
            repository_owner="psf",
            repository_name="requests",
            bronze_table="test_catalog.bronze.github_issues",
            bronze_version=42,
            successful_run_ids=run_ids,
            cutoff_at=datetime(2026, 10, 6, 10, 0, tzinfo=UTC),
            code_commit="787ea43",
            transformation_version="v1",
        )


def test_manifest_rejects_naive_cutoff() -> None:
    with pytest.raises(ValueError, match="cutoff_at must be timezone-aware"):
        ReplayManifest(
            manifest_id="manifest-20261006-001",
            repository_owner="psf",
            repository_name="requests",
            bronze_table="test_catalog.github_analytics_bronze.github_issues_raw",
            bronze_version=42,
            successful_run_ids=("run-001",),
            cutoff_at=datetime(2026, 10, 6, 10, 0),
            code_commit="787ea43",
            transformation_version="v1",
        )
