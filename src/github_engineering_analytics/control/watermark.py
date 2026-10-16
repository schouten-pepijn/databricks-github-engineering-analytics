"""Pure domain model for incremental source watermarks."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class Watermark:
    """A committed source position and its deliberate reprocessing overlap.

    ``value`` is always normalized to UTC. The next extraction begins before
    it, allowing records updated near an earlier run's boundary to be read
    again and handled idempotently downstream.
    """

    value: datetime
    # GitHub can change an Issue shortly after an earlier run recorded its
    # position. Starting five minutes earlier re-reads those late changes. Bronze
    # keeps the duplicate rows (it is append-only); Silver keeps only the newest
    # version of each Issue, so the duplicates do not reach the analytics.
    overlap_seconds: int = 300

    def __post_init__(self) -> None:
        """Validate the overlap and normalize the committed position to UTC."""
        if self.value.tzinfo is None or self.value.utcoffset() is None:
            raise ValueError("value must be timezone-aware")

        if self.overlap_seconds < 0:
            raise ValueError("overlap_seconds must not be negative")

        # The dataclass is frozen, so object.__setattr__ is how a field is
        # normalized (here to UTC) while the object is being constructed.
        object.__setattr__(self, "value", self.value.astimezone(UTC))

    @property
    def extraction_start(self) -> datetime:
        """Return the inclusive source position for the next extraction."""
        return self.value - timedelta(seconds=self.overlap_seconds)
