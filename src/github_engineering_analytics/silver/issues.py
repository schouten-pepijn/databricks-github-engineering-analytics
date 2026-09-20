"""Normalize Bronze GitHub issue records into Silver domain records."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.common.config import PipelineConfig


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


class DeltaSilverIssueWriter:
    """Persist the latest valid GitHub issue state at Silver grain.

    Table grain: one row per repository owner, repository name, and GitHub
    issue ID. Repeated or older source versions cannot create duplicate rows
    or replace a newer issue state.
    """

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("repository_owner", StringType(), nullable=False),
            StructField("repository_name", StringType(), nullable=False),
            StructField("issue_id", LongType(), nullable=False),
            StructField("issue_number", LongType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("state", StringType(), nullable=False),
            StructField("is_pull_request", BooleanType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=False),
            StructField("closed_at", TimestampType(), nullable=True),
            StructField("source_run_id", StringType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.silver_schema}"
        self._table_name = config.silver_issues_table

    def ensure_table(self) -> None:
        """Create the Silver schema and latest-issue table when absent."""
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
            "issue_number BIGINT NOT NULL, "
            "title STRING NOT NULL, "
            "state STRING NOT NULL, "
            "is_pull_request BOOLEAN NOT NULL, "
            "created_at TIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP NOT NULL, "
            "closed_at TIMESTAMP, "
            "source_run_id STRING NOT NULL"
            ") USING DELTA"
        )

    def upsert(self, records: Sequence[SilverIssue]) -> None:
        """Merge valid records without allowing one batch to duplicate a key."""
        if not records:
            return

        self._require_utc_session()
        self._require_unique_business_keys(records)

        source = self._spark.createDataFrame(
            [
                (
                    record.repository_owner,
                    record.repository_name,
                    record.issue_id,
                    record.issue_number,
                    record.title,
                    record.state,
                    record.is_pull_request,
                    record.created_at,
                    record.updated_at,
                    record.closed_at,
                    record.source_run_id,
                )
                for record in records
            ],
            schema=self._ROW_SCHEMA,
        )

        self._merge_source(source)

    def upsert_dataframe(
        self,
        source: DataFrame,
    ) -> None:
        """Merge one already-normalized, unique Silver source DataFrame."""
        self._require_utc_session()
        self._require_required_columns(source)
        self._require_unique_dataframe_business_keys(source)
        self._merge_source(source)

    def _merge_source(
        self,
        source: DataFrame,
    ) -> None:
        """Merge a validated source DataFrame into the current-state Silver table."""
        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                (
                    "target.repository_owner = source.repository_owner "
                    "AND target.repository_name = source.repository_name "
                    "AND target.issue_id = source.issue_id"
                ),
            )
            .whenMatchedUpdate(
                condition="source.updated_at >= target.updated_at",
                set={
                    "issue_number": "source.issue_number",
                    "title": "source.title",
                    "state": "source.state",
                    "is_pull_request": "source.is_pull_request",
                    "created_at": "source.created_at",
                    "updated_at": "source.updated_at",
                    "closed_at": "source.closed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .whenNotMatchedInsert(
                values={
                    "repository_owner": "source.repository_owner",
                    "repository_name": "source.repository_name",
                    "issue_id": "source.issue_id",
                    "issue_number": "source.issue_number",
                    "title": "source.title",
                    "state": "source.state",
                    "is_pull_request": "source.is_pull_request",
                    "created_at": "source.created_at",
                    "updated_at": "source.updated_at",
                    "closed_at": "source.closed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .execute()
        )

    def _require_required_columns(self, source: DataFrame) -> None:
        """Reject DataFrames that cannot satisfy the Silver table contract."""
        required_columns = {field.name for field in self._ROW_SCHEMA}
        missing_columns = required_columns - set(source.columns)

        if missing_columns:
            raise ValueError(
                f"Silver source is missing required columns: {sorted(missing_columns)}"
            )

    def _require_unique_dataframe_business_keys(self, source: DataFrame) -> None:
        """Reject source DataFrames with more than one row per merge key."""
        duplicate_keys = (
            source.groupBy(
                "repository_owner",
                "repository_name",
                "issue_id",
            )
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .collect()
        )

        if duplicate_keys:
            raise ValueError(
                "Silver source contains duplicate business keys before MERGE"
            )

    @staticmethod
    def _require_unique_business_keys(records: Sequence[SilverIssue]) -> None:
        """Reject batches that would match more than one source row per key."""
        keys = {
            (record.repository_owner, record.repository_name, record.issue_id)
            for record in records
        }

        if len(keys) != len(records):
            raise ValueError("records must have unique Silver business keys")

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret Delta timestamps differently."""
        timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before writing Silver issues."
            )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote every Unity Catalog identifier part and escape embedded backticks."""
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
