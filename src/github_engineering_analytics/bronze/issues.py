"""Pure mapping from GitHub issue responses to append-only Bronze records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self


@dataclass(frozen=True)
class BronzeIssueRecord:
    """One source-oriented, append-only Bronze record for a GitHub issue."""

    repository_owner: str
    repository_name: str
    issue_id: int
    source_updated_at: datetime
    raw_json: str
    run_id: str
    ingested_at: datetime
    request_watermark: datetime | None
    page_or_batch_reference: str

    @classmethod
    def from_github_payload(
        cls,
        *,
        repository_owner: str,
        repository_name: str,
        payload: dict[str, object],
        run_id: str,
        ingested_at: datetime,
        request_watermark: datetime | None,
        page_or_batch_reference: str,
    ) -> Self:
        """Create a Bronze record while preserving the complete source payload."""
        cls._require_non_empty(repository_owner, "repository_owner")
        cls._require_non_empty(repository_name, "repository_name")
        cls._require_non_empty(run_id, "run_id")
        cls._require_non_empty(page_or_batch_reference, "page_or_batch_reference")

        issue_id = payload.get("id")
        if type(issue_id) is not int or issue_id <= 0:
            raise ValueError("payload['id'] must be a positive integer")

        updated_at = payload.get("updated_at")
        if not isinstance(updated_at, str):
            raise ValueError("payload['updated_at'] must be an ISO 8601 string")

        return cls(
            repository_owner=repository_owner,
            repository_name=repository_name,
            issue_id=issue_id,
            source_updated_at=cls._parse_source_timestamp(updated_at),
            raw_json=cls._serialize_payload(payload),
            run_id=run_id,
            ingested_at=cls._normalize_to_utc(ingested_at, "ingested_at"),
            request_watermark=(
                cls._normalize_to_utc(request_watermark, "request_watermark")
                if request_watermark is not None
                else None
            ),
            page_or_batch_reference=page_or_batch_reference,
        )

    @staticmethod
    def _serialize_payload(payload: dict[str, object]) -> str:
        """Return stable JSON without dropping or classifying source fields."""
        try:
            return json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        except (TypeError, ValueError) as error:
            raise ValueError("payload must be JSON-serializable") from error

    @staticmethod
    def _parse_source_timestamp(value: str) -> datetime:
        """Parse GitHub's ISO 8601 timestamp and normalize it to UTC."""
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(
                "payload['updated_at'] must be an ISO 8601 timestamp"
            ) from error

        return BronzeIssueRecord._normalize_to_utc(
            parsed,
            "payload['updated_at']",
        )

    @staticmethod
    def _normalize_to_utc(value: datetime, field_name: str) -> datetime:
        """Reject naive timestamps and normalize aware values to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field_name} must be timezone-aware")

        return value.astimezone(UTC)

    @staticmethod
    def _require_non_empty(value: str, field_name: str) -> None:
        """Reject empty identifiers at the Bronze ingestion boundary."""
        if not value.strip():
            raise ValueError(f"{field_name} must not be empty")
