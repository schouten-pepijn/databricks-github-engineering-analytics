"""Pure mapping from GitHub issue responses to append-only Bronze records."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.common.config import PipelineConfig


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


class DeltaBronzeIssueWriter:
    """Append source-oriented Bronze issue records to a Delta table."""

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("repository_owner", StringType(), nullable=False),
            StructField("repository_name", StringType(), nullable=False),
            StructField("issue_id", LongType(), nullable=False),
            StructField("source_updated_at", TimestampType(), nullable=False),
            StructField("raw_json", StringType(), nullable=False),
            StructField("_run_id", StringType(), nullable=False),
            StructField("_ingested_at", TimestampType(), nullable=False),
            StructField("_request_watermark", TimestampType(), nullable=True),
            StructField("_page_or_batch_reference", StringType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.bronze_schema}"
        self._table_name = config.bronze_issues_table

    def append(self, records: Sequence[BronzeIssueRecord]) -> None:
        """Append records without deduplicating or modifying existing Bronze rows."""
        if not records:
            return

        self._require_utc_session()

        rows = [
            (
                record.repository_owner,
                record.repository_name,
                record.issue_id,
                record.source_updated_at,
                record.raw_json,
                record.run_id,
                record.ingested_at,
                record.request_watermark,
                record.page_or_batch_reference,
            )
            for record in records
        ]

        dataframe = self._spark.createDataFrame(
            rows,
            schema=self._ROW_SCHEMA,
        )

        dataframe.write.format("delta").mode("append").saveAsTable(self._table_name)

    def ensure_table(self) -> None:
        """Create the Bronze schema and append-only issue table when absent."""
        self._require_utc_session()

        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{self._quote_identifier(self._table_name)} ("
            "repository_owner STRING NOT NULL, "
            "repository_name STRING NOT NULL, "
            "issue_id BIGINT NOT NULL, "
            "source_updated_at TIMESTAMP NOT NULL, "
            "raw_json STRING NOT NULL, "
            "_run_id STRING NOT NULL, "
            "_ingested_at TIMESTAMP NOT NULL, "
            "_request_watermark TIMESTAMP, "
            "_page_or_batch_reference STRING NOT NULL"
            ") USING DELTA"
        )

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret Delta timestamps differently."""
        session_timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if session_timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before reading "
                "or writing Bronze issue records."
            )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote every Unity Catalog identifier part and escape embedded backticks."""
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
