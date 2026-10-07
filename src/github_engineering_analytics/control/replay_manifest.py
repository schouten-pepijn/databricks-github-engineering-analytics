"""Immutable input boundary for one manual, reproducible Bronze replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class ReplayManifest:
    """Lock one past Bronze input so a manual replay can repeat it exactly.

    A manifest is a recipe card, not part of the live pipeline. The normal
    pipeline run never reads or writes it. It is created on demand, after the
    fact, and later handed (by ``manifest_id``) to a manually started replay
    job. Bronze keeps growing; the manifest is what lets the replay read the
    same rows today, next week, or after more appends.

    What it locks (the input, not the output):
        - Which rows: ``bronze_table`` at Delta ``bronze_version`` (time
          travel), limited to this repository's ``successful_run_ids``.
        - Up to when: ``cutoff_at`` is the upper bound for accepted Bronze
          commits.
        - With which code: ``code_commit`` and ``transformation_version``.

    What it does not do:
        - It does not change what the live pipeline extracts or writes.
        - It is deliberately separate from the source watermark: creating or
          using a manifest must never advance source extraction progress.
        - It never calls GitHub. A replay rebuilds state from stored Bronze.
    """

    manifest_id: str
    repository_owner: str
    repository_name: str
    bronze_table: str
    bronze_version: int
    successful_run_ids: tuple[str, ...]
    cutoff_at: datetime
    code_commit: str
    transformation_version: str

    def __post_init__(self) -> None:
        """Validate the immutable replay boundary at the domain edge."""
        for value, field_name in (
            (self.manifest_id, "manifest_id"),
            (self.repository_owner, "repository_owner"),
            (self.repository_name, "repository_name"),
            (self.bronze_table, "bronze_table"),
            (self.code_commit, "code_commit"),
            (self.transformation_version, "transformation_version"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

        if self.bronze_version < 0:
            raise ValueError("bronze_version must not be negative")

        if not self.successful_run_ids:
            raise ValueError("successful_run_ids must not be empty")

        if any(not run_id.strip() for run_id in self.successful_run_ids):
            raise ValueError("successful_run_ids must not contain empty values")

        if len(set(self.successful_run_ids)) != len(self.successful_run_ids):
            raise ValueError("successful_run_ids must be unique")

        if self.cutoff_at.tzinfo is None or self.cutoff_at.utcoffset() is None:
            raise ValueError("cutoff_at must be timezone-aware")

        object.__setattr__(self, "cutoff_at", self.cutoff_at.astimezone(UTC))
