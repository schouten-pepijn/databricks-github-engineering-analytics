"""Normalize Bronze GitHub issue records into Silver domain records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self


@dataclass(frozen=True)
class SilverIssue:
    """One normalized GitHub issue at the grain of one GitHub issue ID."""

    repository_owner: str
    repository_name: str
    issue_id: int
    issue_number: int
    title: str
    state: str
    is_pull_request: bool
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    source_run_id: str

    @classmethod
    def from_bronze_row(
        cls,
        *,
        repository_owner: str,
        repository_name: str,
        issue_id: int,
        raw_json: str,
        source_run_id: str,
    ) -> Self:
        """Create a Silver issue from the preserved GitHub source payload."""
        if type(issue_id) is not int or issue_id <= 0:
            raise ValueError("issue_id must be a positive integer")

        for value, field_name in (
            (repository_owner, "repository_owner"),
            (repository_name, "repository_name"),
            (source_run_id, "source_run_id"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

        payload = cls._parse_payload(raw_json)
        payload_issue_id = cls._require_positive_int(payload, "id")

        if issue_id != payload_issue_id:
            raise ValueError(
                "Bronze issue_id must match payload['id']: "
                f"{issue_id!r} != {payload_issue_id!r}"
            )

        return cls(
            repository_owner=repository_owner,
            repository_name=repository_name,
            issue_id=issue_id,
            issue_number=cls._require_positive_int(payload, "number"),
            title=cls._require_non_empty_string(payload, "title"),
            state=cls._require_non_empty_string(payload, "state"),
            is_pull_request="pull_request" in payload,
            created_at=cls._parse_timestamp(payload, "created_at"),
            updated_at=cls._parse_timestamp(payload, "updated_at"),
            closed_at=cls._parse_optional_timestamp(payload, "closed_at"),
            source_run_id=source_run_id,
        )

    @staticmethod
    def _parse_payload(raw_json: str) -> dict[str, object]:
        """Parse and validate the raw GitHub JSON object."""
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError as e:
            raise ValueError("raw_json must contain valid JSON") from e

        if not isinstance(payload, dict):
            raise ValueError("raw_json must contain a JSON object")

        return payload

    @staticmethod
    def _require_positive_int(payload: dict[str, object], field_name: str) -> int:
        """Read a required positive integer field."""
        value = payload.get(field_name)

        if type(value) is not int or value <= 0:
            raise ValueError(f"payload[{field_name!r}] must be a positive integer")

        return value

    @staticmethod
    def _require_non_empty_string(
        payload: dict[str, object],
        field_name: str,
    ) -> str:
        """Read a required non-empty string field."""
        value = payload.get(field_name)

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"payload[{field_name!r}] must be a non-empty string")

        return value

    @classmethod
    def _parse_timestamp(
        cls,
        payload: dict[str, object],
        field_name: str,
    ) -> datetime:
        """Read a required GitHub ISO-8601 timestamp in UTC."""
        value = cls._require_non_empty_string(payload, field_name)
        return cls._parse_timestamp_value(value, field_name)

    @classmethod
    def _parse_optional_timestamp(
        cls,
        payload: dict[str, object],
        field_name: str,
    ) -> datetime | None:
        """Read an optional GitHub ISO-8601 timestamp in UTC."""
        value = payload.get(field_name)

        if value is None:
            return None

        if not isinstance(value, str):
            raise ValueError(f"payload[{field_name!r}] must be a string or null")

        return cls._parse_timestamp_value(value, field_name)

    @staticmethod
    def _parse_timestamp_value(value: str, field_name: str) -> datetime:
        """Parse a timezone-aware ISO-8601 timestamp and normalize it to UTC."""
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(
                f"payload[{field_name!r}] must be an ISO-8601 timestamp"
            ) from e

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(
                f"payload[{field_name!r}] must include timezone information"
            )

        return parsed.astimezone(UTC)
