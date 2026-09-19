"""Pure domain model for incremental source watermarks."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class Watermark:
    """A successfully processed source position with a reprocessing overlap."""

    value: datetime
    overlap_seconds: int = 300

    def __post_init__(self) -> None:
        if self.value.tzinfo is None or self.value.utcoffset() is None:
            raise ValueError("value must be timezone-aware")

        if self.overlap_seconds < 0:
            raise ValueError("overlap_seconds must not be negative")

        object.__setattr__(self, "value", self.value.astimezone(UTC))

    @property
    def extraction_start(self) -> datetime:
        """Return the inclusive source position for the next extraction."""
        return self.value - timedelta(seconds=self.overlap_seconds)
