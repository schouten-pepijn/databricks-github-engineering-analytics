"""Immutable input boundary for one reproducible Bronze replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class ReplayManifest:
    """Identify the exact Bronze input accepted for one replay.

    The manifest freezes both the selected extraction attempts and the Delta
    table version that contains them. It is deliberately separate from the
    source watermark: creating or using a manifest must never advance source
    extraction progress.
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
