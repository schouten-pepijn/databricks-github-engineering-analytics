"""Normalize nested GitHub issue-label payloads into Silver domain records."""

from __future__ import annotations

import json
import string
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self


@dataclass(frozen=True)
class SilverLabel:
    """One GitHub label observed in a Bronze issue payload.

    Grain: one label definition per repository and GitHub label ID.
    """

    repository_owner: str
    repository_name: str
    label_id: int
    name: str
    color: str
    description: str | None
    is_default: bool
    source_issue_id: int
    observed_at: datetime
    source_run_id: str

    @classmethod
    def from_bronze_row(
        cls,
        *,
        repository_owner: str,
        repository_name: str,
        source_issue_id: int,
        raw_json: str,
        source_updated_at: datetime,
        source_run_id: str,
    ) -> tuple[Self, ...]:
        """Create zero or more normalized labels from one Bronze issue."""

        for value, field_name in (
            (repository_owner, "repository_owner"),
            (repository_name, "repository_name"),
            (source_run_id, "source_run_id"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

        if type(source_issue_id) is not int or source_issue_id <= 0:
            raise ValueError("source_issue_id must be a positive integer")

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

        label_payloads = payload.get("labels")
        if not isinstance(label_payloads, list):
            raise ValueError("payload['labels'] must be a JSON array")

        observed_at = source_updated_at.astimezone(UTC)
        labels: list[SilverLabel] = []
        label_ids: set[int] = set()

        for label_payload in label_payloads:
            if not isinstance(label_payload, dict):
                raise ValueError("each payload['labels'] item must be a JSON object")

            label = cls._from_label_payload(
                label_payload=label_payload,
                repository_owner=repository_owner,
                repository_name=repository_name,
                source_issue_id=source_issue_id,
                observed_at=observed_at,
                source_run_id=source_run_id,
            )

            if label.label_id in label_ids:
                raise ValueError(
                    "payload['labels'] must not contain duplicate label IDs"
                )

            label_ids.add(label.label_id)
            labels.append(label)

        return tuple(labels)

    @classmethod
    def _from_label_payload(
        cls,
        *,
        label_payload: dict[str, object],
        repository_owner: str,
        repository_name: str,
        source_issue_id: int,
        observed_at: datetime,
        source_run_id: str,
    ) -> Self:
        """Normalize and validate one nested GitHub label object."""

        return cls(
            repository_owner=repository_owner,
            repository_name=repository_name,
            label_id=cls._require_positive_int(
                label_payload,
                "id",
                "payload['labels'][*]['id']",
            ),
            name=cls._require_non_empty_string(
                label_payload,
                "name",
                "payload['labels'][*]['name']",
            ),
            color=cls._require_hex_color(label_payload),
            description=cls._require_optional_string(
                label_payload,
                "description",
                "payload['labels'][*]['description']",
            ),
            is_default=cls._require_boolean(
                label_payload,
                "default",
                "payload['labels'][*]['default']",
            ),
            source_issue_id=source_issue_id,
            observed_at=observed_at,
            source_run_id=source_run_id,
        )

    @staticmethod
    def _parse_payload(raw_json: str) -> dict[str, object]:
        """Parse one preserved GitHub issue JSON object."""
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
        """Read one required positive integer."""
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
        """Read one required non-empty string."""
        value = payload.get(field_name)

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_path} must be a non-empty string")

        return value

    @staticmethod
    def _require_optional_string(
        payload: dict[str, object],
        field_name: str,
        field_path: str,
    ) -> str | None:
        """Read one optional string."""
        value = payload.get(field_name)

        if value is None:
            return None

        if not isinstance(value, str):
            raise ValueError(f"{field_path} must be a string or null")

        return value

    @classmethod
    def _require_hex_color(cls, payload: dict[str, object]) -> str:
        """Read a six-character GitHub hexadecimal label color."""
        color = cls._require_non_empty_string(
            payload,
            "color",
            "payload['labels'][*]['color']",
        )

        if len(color) != 6 or any(
            character not in string.hexdigits for character in color
        ):
            raise ValueError(
                "payload['labels'][*]['color'] must be a six-character hex color"
            )

        return color.lower()

    @staticmethod
    def _require_boolean(
        payload: dict[str, object],
        field_name: str,
        field_path: str,
    ) -> bool:
        """Read one required boolean."""
        value = payload.get(field_name)

        if type(value) is not bool:
            raise ValueError(f"{field_path} must be a boolean")

        return value
