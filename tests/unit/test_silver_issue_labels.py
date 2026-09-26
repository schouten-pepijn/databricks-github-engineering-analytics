from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from github_engineering_analytics.silver.issue_labels import SilverIssueLabel


def _valid_issue_payload(**overrides: object) -> dict[str, object]:
    """Build a Bronze issue payload containing one valid GitHub label."""
    payload: dict[str, object] = {
        "id": 1001,
        "labels": [
            {
                "id": 2001,
                "name": "bug",
                "color": "d73a4a",
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
) -> tuple[SilverIssueLabel, ...]:
    """Build relation records using valid Bronze metadata by default."""
    arguments: dict[str, object] = {
        "repository_owner": "psf",
        "repository_name": "requests",
        "issue_id": 1001,
        "raw_json": json.dumps(payload),
        "source_updated_at": datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
        "source_run_id": "run-123",
    }
    arguments.update(overrides)

    return SilverIssueLabel.from_bronze_row(**arguments)  # type: ignore[arg-type]


def test_from_bronze_row_maps_each_label_to_one_issue_relationship() -> None:
    """Produce one relation record for each validated nested issue label."""
    relationships = _from_payload(
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
                    "id": 2002,
                    "name": "documentation",
                    "color": "0075ca",
                    "description": None,
                    "default": False,
                },
            ]
        ),
        source_updated_at=datetime.fromisoformat("2026-09-20T11:30:00+02:00"),
    )

    assert relationships == (
        SilverIssueLabel(
            repository_owner="psf",
            repository_name="requests",
            issue_id=1001,
            label_id=2001,
            observed_at=datetime(2026, 9, 20, 9, 30, tzinfo=UTC),
            source_run_id="run-123",
        ),
        SilverIssueLabel(
            repository_owner="psf",
            repository_name="requests",
            issue_id=1001,
            label_id=2002,
            observed_at=datetime(2026, 9, 20, 9, 30, tzinfo=UTC),
            source_run_id="run-123",
        ),
    )


def test_from_bronze_row_returns_no_relationships_for_an_unlabeled_issue() -> None:
    """Represent an issue with no labels as an empty relation result."""
    assert _from_payload(_valid_issue_payload(labels=[])) == ()


def test_from_bronze_row_rejects_a_payload_for_another_issue() -> None:
    """Prevent a Bronze issue row from being paired with another issue payload."""
    with pytest.raises(
        ValueError,
        match=r"Bronze source_issue_id must match payload\['id'\]",
    ):
        _from_payload(_valid_issue_payload(id=1002))


def test_from_bronze_row_rejects_duplicate_label_ids() -> None:
    """Keep the relation grain unambiguous within one issue payload."""
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
                        "name": "duplicate bug",
                        "color": "d73a4a",
                        "description": None,
                        "default": False,
                    },
                ]
            )
        )
