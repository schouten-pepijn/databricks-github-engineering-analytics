"""Pure domain model for a single incremental pipeline run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from github_engineering_analytics.control.watermark import Watermark


@dataclass(frozen=True)
class PipelineRun:
    """
    Describe one pipeline attempt before it is persisted.

    candidate_watermark is the maximum source position observed during
    this run. It is not automatically committed.
    """

    run_id: str
    source_name: str
    entity_name: str
    started_at: datetime
    watermark_before: Watermark | None
    candidate_watermark: Watermark | None = None

    def __post_init__(self) -> None:
        self._require_non_empty(self.run_id, "run_id")
        self._require_non_empty(self.source_name, "source_name")
        self._require_non_empty(self.entity_name, "entity_name")

        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")

        object.__setattr__(
            self,
            "started_at",
            self.started_at.astimezone(UTC),
        )

        if (
            self.watermark_before is not None
            and self.candidate_watermark is not None
            and self.candidate_watermark.value < self.watermark_before.value
        ):
            raise ValueError(
                "candidate_watermark must not be earlier than watermark_before"
            )

    @property
    def extraction_start(self) -> datetime | None:
        """Return the overlap-adjusted extraction position."""
        if self.watermark_before is None:
            return None

        return self.watermark_before.extraction_start

    @staticmethod
    def _require_non_empty(
        value: str,
        field_name: str,
    ) -> None:
        if not value.strip():
            raise ValueError(f"{field_name} must not be empty")
