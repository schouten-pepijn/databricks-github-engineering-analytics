"""Normalize nested GitHub issue-user payloads into Silver domain records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self


@dataclass(frozen=True)
class SilverUser:
    """One GitHub user observed in a Bronze issue payload."""

    user_id: int
    login: str
    user_type: str
    source_issue_id: int
    observed_at: datetime
    source_run_id: str

    @classmethod
    def from_bronze_row(
        cls,
        *,
        raw_json: str,
        source_issue_id: int,
        source_updated_at: datetime,
        source_run_id: str,
    ) -> Self:
        """Create a normalized user from one preserved Bronze issue payload."""
        if type(source_issue_id) is not int or source_issue_id <= 0:
            raise ValueError("source_issue_id must be a positive integer")

        if not isinstance(source_run_id, str) or not source_run_id.strip():
            raise ValueError("source_run_id must be a non-empty string")

        if source_updated_at.tzinfo is None or source_updated_at.utcoffset() is None:
            raise ValueError("source_updated_at must be timezone-aware")

        payload = cls._parse_payload(raw_json)
        payload_issue_id = cls._require_positive_int(
            payload,
            "id",
            "payload['id']",
        )

        if source_issue_id != payload_issue_id:
            raise ValueError(
                "Bronze source_issue_id must match payload['id']: "
                f"{source_issue_id!r} != {payload_issue_id!r}"
            )

        user_payload = payload.get("user")
        if not isinstance(user_payload, dict):
            raise ValueError("payload['user'] must be a JSON object")

        return cls(
            user_id=cls._require_positive_int(
                user_payload,
                "id",
                "payload['user']['id']",
            ),
            login=cls._require_non_empty_string(
                user_payload,
                "login",
                "payload['user']['login']",
            ),
            user_type=cls._require_non_empty_string(
                user_payload,
                "type",
                "payload['user']['type']",
            ),
            source_issue_id=source_issue_id,
            observed_at=source_updated_at.astimezone(UTC),
            source_run_id=source_run_id,
        )

    @staticmethod
    def _parse_payload(raw_json: str) -> dict[str, object]:
        """Parse a preserved GitHub issue JSON object."""
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError as error:
            raise ValueError("raw_json must contain valid JSON") from error

        if not isinstance(payload, dict):
            raise ValueError("raw_json must contain a JSON object")

        return payload

    @staticmethod
    def _require_positive_int(
        payload: dict[str, object],
        field_name: str,
        field_path: str,
    ) -> int:
        """Read one required positive integer from a JSON object."""
        value = payload.get(field_name)

        if type(value) is not int or value <= 0:
            raise ValueError(f"{field_path} must be a positive integer")

        return value

    @staticmethod
    def _require_non_empty_string(
        payload: dict[str, object],
        field_name: str,
        field_path: str,
    ) -> str:
        """Read one required non-empty string from a JSON object."""
        value = payload.get(field_name)

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_path} must be a non-empty string")

        return value
