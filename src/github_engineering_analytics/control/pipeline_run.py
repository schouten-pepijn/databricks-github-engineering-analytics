"""Pure domain model for a single incremental pipeline run."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum

from github_engineering_analytics.control.watermark import Watermark


class PipelineRunStatus(StrEnum):
    """Possible lifecycle states of a pipeline run."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class PipelineRun:
    """
    Describe one pipeline attempt before it is persisted.

    A candidate watermark is observed during extraction. It may only be
    committed after the run has succeeded and all required downstream
    processing has completed.
    """

    run_id: str
    source_name: str
    entity_name: str
    started_at: datetime
    watermark_before: Watermark | None
    candidate_watermark: Watermark | None = None
    status: PipelineRunStatus = PipelineRunStatus.RUNNING
    finished_at: datetime | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        self._require_non_empty(self.run_id, "run_id")
        self._require_non_empty(self.source_name, "source_name")
        self._require_non_empty(self.entity_name, "entity_name")

        object.__setattr__(
            self,
            "started_at",
            self._normalize_to_utc(
                self.started_at,
                "started_at",
            ),
        )

        if self.finished_at is not None:
            object.__setattr__(
                self,
                "finished_at",
                self._normalize_to_utc(
                    self.finished_at,
                    "finished_at",
                ),
            )

        if (
            self.watermark_before is not None
            and self.candidate_watermark is not None
            and self.candidate_watermark.value < self.watermark_before.value
        ):
            raise ValueError(
                "candidate_watermark must not be earlier than watermark_before"
            )

        if self.status is PipelineRunStatus.RUNNING:
            if self.finished_at is not None:
                raise ValueError("running pipeline runs must not have finished_at")

            if self.error_message is not None:
                raise ValueError("running pipeline runs must not have error_message")

        if self.status is PipelineRunStatus.SUCCEEDED:
            if self.finished_at is None:
                raise ValueError("succeeded pipeline runs require finished_at")

            if self.error_message is not None:
                raise ValueError("succeeded pipeline runs must not have error_message")

        if self.status is PipelineRunStatus.FAILED:
            if self.finished_at is None:
                raise ValueError("failed pipeline runs require finished_at")

            if self.error_message is None or not self.error_message.strip():
                raise ValueError("failed pipeline runs require error_message")

    @property
    def extraction_start(self) -> datetime | None:
        """Return the overlap-adjusted extraction position."""
        if self.watermark_before is None:
            return None

        return self.watermark_before.extraction_start

    def succeed(
        self,
        candidate_watermark: Watermark | None,
        finished_at: datetime,
    ) -> PipelineRun:
        """Return a successful copy of a running pipeline run."""
        self._require_running()

        return replace(
            self,
            candidate_watermark=candidate_watermark,
            status=PipelineRunStatus.SUCCEEDED,
            finished_at=finished_at,
        )

    def fail(
        self,
        error_message: str,
        finished_at: datetime,
    ) -> PipelineRun:
        """Return a failed copy of a running pipeline run."""
        self._require_running()

        return replace(
            self,
            status=PipelineRunStatus.FAILED,
            finished_at=finished_at,
            error_message=error_message,
        )

    def _require_running(self) -> None:
        if self.status is not PipelineRunStatus.RUNNING:
            raise ValueError("only running pipeline runs can change lifecycle state")

    @staticmethod
    def _normalize_to_utc(
        value: datetime,
        field_name: str,
    ) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field_name} must be timezone-aware")

        return value.astimezone(UTC)

    @staticmethod
    def _require_non_empty(
        value: str,
        field_name: str,
    ) -> None:
        if not value.strip():
            raise ValueError(f"{field_name} must not be empty")
