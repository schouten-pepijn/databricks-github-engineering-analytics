from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from github_engineering_analytics.silver.issues import SilverIssue


def _valid_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": 1001,
        "number": 42,
        "title": "Improve retry handling",
        "state": "open",
        "created_at": "2026-09-20T10:00:00+02:00",
        "updated_at": "2026-09-20T11:30:00Z",
        "closed_at": None,
    }
    payload.update(overrides)
    return payload


def _from_payload(
    payload: dict[str, object],
    **overrides: object,
) -> SilverIssue:
    arguments: dict[str, object] = {
        "repository_owner": "psf",
        "repository_name": "requests",
        "issue_id": 1001,
        "raw_json": json.dumps(payload),
        "source_run_id": "run-123",
    }
    arguments.update(overrides)

    return SilverIssue.from_bronze_row(**arguments)  # type: ignore[arg-type]


def test_from_bronze_row_normalizes_a_valid_issue() -> None:
    issue = _from_payload(_valid_payload())

    assert issue == SilverIssue(
        repository_owner="psf",
        repository_name="requests",
        issue_id=1001,
        issue_number=42,
        title="Improve retry handling",
        state="open",
        is_pull_request=False,
        created_at=datetime(2026, 9, 20, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
        closed_at=None,
        source_run_id="run-123",
    )


def test_from_bronze_row_identifies_pull_requests() -> None:
    issue = _from_payload(_valid_payload(pull_request={}))

    assert issue.is_pull_request is True


def test_from_bronze_row_rejects_invalid_json() -> None:
    with pytest.raises(ValueError, match="raw_json must contain valid JSON"):
        SilverIssue.from_bronze_row(
            repository_owner="psf",
            repository_name="requests",
            issue_id=1001,
            raw_json="not-json",
            source_run_id="run-123",
        )


def test_from_bronze_row_rejects_naive_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match="payload\\['created_at'\\] must include timezone information",
    ):
        _from_payload(_valid_payload(created_at="2026-09-20T10:00:00"))


def test_from_bronze_row_rejects_mismatched_issue_ids() -> None:
    with pytest.raises(
        ValueError, match="Bronze issue_id must match payload\\['id'\\]"
    ):
        _from_payload(_valid_payload(id=2002))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("repository_owner", ""),
        ("repository_owner", 123),
        ("repository_name", ""),
        ("source_run_id", ""),
    ],
)
def test_from_bronze_row_rejects_invalid_bronze_metadata(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=f"{field_name} must be a non-empty string"):
        _from_payload(_valid_payload(), **{field_name: value})
