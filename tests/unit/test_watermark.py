from datetime import UTC, datetime, timedelta, timezone

import pytest

from github_engineering_analytics.control.watermark import Watermark


def test_watermark_returns_extraction_start() -> None:
    watermark = Watermark(
        value=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
        overlap_seconds=300,
    )

    assert watermark.extraction_start == datetime(2026, 9, 19, 11, 55, tzinfo=UTC)


def test_watermark_zero_overlap() -> None:
    value = datetime(2026, 9, 19, 12, 0, tzinfo=UTC)

    watermark = Watermark(value=value, overlap_seconds=0)

    assert watermark.extraction_start == value


def test_watermark_rejects_negative_overlap() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        Watermark(
            value=datetime(2026, 9, 19, 12, 0, tzinfo=UTC),
            overlap_seconds=-1,
        )


def test_watermark_requires_timezone_aware_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Watermark(value=datetime(2026, 9, 19, 12, 0))


def test_watermark_normalizes_timezone_aware_value_to_utc() -> None:
    watermark = Watermark(
        value=datetime(
            2026,
            9,
            19,
            14,
            0,
            tzinfo=timezone(timedelta(hours=2)),
        )
    )

    assert watermark.value == datetime(2026, 9, 19, 12, 0, tzinfo=UTC)
