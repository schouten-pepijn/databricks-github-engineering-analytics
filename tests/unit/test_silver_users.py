from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from github_engineering_analytics.silver.users import SilverUser


def _validate_issue_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": 1001,
        "updated_at": "2026-09-20T11:30:00Z",
        "user": {
            "id": 2001,
            "login": "octocat",
            "type": "User",
        },
    }
    payload.update(overrides)

    return payload


def _from_payload(
    payload: dict[str, object],
    **overrides: object,
) -> SilverUser:
    arguments: dict[str, object] = {
        "raw_json": json.dumps(payload),
        "source_issue_id": 1001,
        "source_updated_at": datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
        "source_run_id": "run-123",
    }
    arguments.update(overrides)

    return SilverUser.from_bronze_row(**arguments)  # type: ignore[arg-type]


def test_from_bronze_row_normalizes_a_valid_user() -> None:
    user = _from_payload(_validate_issue_payload())

    assert user == SilverUser(
        user_id=2001,
        login="octocat",
        user_type="User",
        source_issue_id=1001,
        observed_at=datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
        source_run_id="run-123",
    )


def test_from_bronze_row_rejects_a_missing_user() -> None:
    with pytest.raises(ValueError, match="payload\\['user'\\] must be a JSON object"):
        _from_payload(_validate_issue_payload(user=None))


def test_from_bronze_row_rejects_an_invalid_user_id() -> None:
    with pytest.raises(
        ValueError,
        match="payload\\['user'\\]\\['id'\\] must be a positive integer",
    ):
        _from_payload(
            _validate_issue_payload(user={"id": 0, "login": "octocat", "type": "User"})
        )


def test_from_bronze_row_rejects_a_mismatched_issue_id() -> None:
    with pytest.raises(
        ValueError,
        match="Bronze source_issue_id must match payload\\['id'\\]",
    ):
        _from_payload(_validate_issue_payload(id=1002))
