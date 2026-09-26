from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from github_engineering_analytics.silver.labels import SilverLabel


def _valid_issue_payload(**overrides: object) -> dict[str, object]:
    """Build a Bronze issue payload containing one valid GitHub label."""
    payload: dict[str, object] = {
        "id": 1001,
        "labels": [
            {
                "id": 2001,
                "name": "bug",
                "color": "D73A4A",
                "description": "Something is not working",
                "default": True,
            }
        ],
    }
    payload.update(overrides)

    return payload


def _from_payload(
    payload: dict[str, object],
    **overrides: object,
) -> tuple[SilverLabel, ...]:
    """Normalize one test Bronze payload with valid source metadata by default."""
    arguments: dict[str, object] = {
        "repository_owner": "psf",
        "repository_name": "requests",
        "source_issue_id": 1001,
        "raw_json": json.dumps(payload),
        "source_updated_at": datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
        "source_run_id": "run-123",
    }
    arguments.update(overrides)

    return SilverLabel.from_bronze_row(**arguments)  # type: ignore[arg-type]


def test_from_bronze_row_normalizes_labels_and_observation_time_to_utc() -> None:
    """Produce one Silver record per label with the documented repository grain."""
    labels = _from_payload(
        _valid_issue_payload(
            labels=[
                {
                    "id": 2001,
                    "name": "bug",
                    "color": "D73A4A",
                    "description": "Something is not working",
                    "default": True,
                },
                {
                    "id": 2002,
                    "name": "documentation",
                    "color": "0075CA",
                    "description": None,
                    "default": False,
                },
            ]
        ),
        source_updated_at=datetime.fromisoformat("2026-09-20T11:30:00+02:00"),
    )

    assert labels == (
        SilverLabel(
            repository_owner="psf",
            repository_name="requests",
            label_id=2001,
            name="bug",
            color="d73a4a",
            description="Something is not working",
            is_default=True,
            source_issue_id=1001,
            observed_at=datetime(2026, 9, 20, 9, 30, tzinfo=UTC),
            source_run_id="run-123",
        ),
        SilverLabel(
            repository_owner="psf",
            repository_name="requests",
            label_id=2002,
            name="documentation",
            color="0075ca",
            description=None,
            is_default=False,
            source_issue_id=1001,
            observed_at=datetime(2026, 9, 20, 9, 30, tzinfo=UTC),
            source_run_id="run-123",
        ),
    )


def test_from_bronze_row_returns_no_records_for_an_issue_without_labels() -> None:
    """Keep unlabeled issues out of the label-grain Silver result."""
    labels = _from_payload(_valid_issue_payload(labels=[]))

    assert labels == ()


def test_from_bronze_row_rejects_mismatched_issue_ids() -> None:
    """Prevent a Bronze metadata row from being paired with another issue payload."""
    with pytest.raises(
        ValueError,
        match="Bronze source_issue_id must match payload\\['id'\\]",
    ):
        _from_payload(_valid_issue_payload(id=1002))


def test_from_bronze_row_rejects_duplicate_label_ids() -> None:
    """Require one label definition per repository and GitHub label ID."""
    with pytest.raises(ValueError, match="must not contain duplicate label IDs"):
        _from_payload(
            _valid_issue_payload(
                labels=[
                    {
                        "id": 2001,
                        "name": "bug",
                        "color": "d73a4a",
                        "description": None,
                        "default": True,
                    },
                    {
                        "id": 2001,
                        "name": "bug duplicate",
                        "color": "d73a4a",
                        "description": None,
                        "default": False,
                    },
                ]
            )
        )


@pytest.mark.parametrize(
    ("labels", "message"),
    [
        (None, "payload\\['labels'\\] must be a JSON array"),
        (
            [
                {
                    "id": 2001,
                    "name": "bug",
                    "color": "red",
                    "description": None,
                    "default": True,
                }
            ],
            "must be a six-character hex color",
        ),
        (
            [
                {
                    "id": 2001,
                    "name": "bug",
                    "color": "d73a4a",
                    "description": None,
                    "default": "true",
                }
            ],
            "payload\\['labels'\\]\\[\\*\\]\\['default'\\] must be a boolean",
        ),
    ],
)
def test_from_bronze_row_rejects_invalid_label_contract(
    labels: object,
    message: str,
) -> None:
    """Fail before Silver writes when nested label fields violate the contract."""
    with pytest.raises(ValueError, match=message):
        _from_payload(_valid_issue_payload(labels=labels))


def test_from_bronze_row_rejects_naive_observation_timestamp() -> None:
    """Require the Bronze source timestamp to be comparable across runs."""
    with pytest.raises(ValueError, match="source_updated_at must be timezone-aware"):
        _from_payload(
            _valid_issue_payload(),
            source_updated_at=datetime(2026, 9, 20, 11, 30),
        )
