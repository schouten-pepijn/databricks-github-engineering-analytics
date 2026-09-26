"""Normalize nested GitHub issue-user payloads into Silver domain records."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self

from delta.tables import DeltaTable
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


class DeltaSilverUserWriter:
    """Persist the latest observed GitHub user state at Silver grain."""

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("user_id", LongType(), nullable=False),
            StructField("login", StringType(), nullable=False),
            StructField("user_type", StringType(), nullable=False),
            StructField("source_issue_id", LongType(), nullable=False),
            StructField("observed_at", TimestampType(), nullable=False),
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
        self._table_name = config.silver_users_table

    def ensure_table(self) -> None:
        """Create the Silver schema and global GitHub users table when absent."""
        self._require_utc_session()

        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self._schema_name)}"
        )
        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{self._quote_identifier(self._table_name)} ("
            "user_id BIGINT NOT NULL, "
            "login STRING NOT NULL, "
            "user_type STRING NOT NULL, "
            "source_issue_id BIGINT NOT NULL, "
            "observed_at TIMESTAMP NOT NULL, "
            "source_run_id STRING NOT NULL"
            ") USING DELTA"
        )

    def upsert(
        self,
        records: Sequence[SilverUser],
    ) -> None:
        """Merge one unique, validated observation per global GitHub user ID."""
        if not records:
            return

        self._require_utc_session()
        self._require_unique_user_ids(records)

        source = self._spark.createDataFrame(
            [
                (
                    record.user_id,
                    record.login,
                    record.user_type,
                    record.source_issue_id,
                    record.observed_at,
                    record.source_run_id,
                )
                for record in records
            ],
            schema=self._ROW_SCHEMA,
        )

        target = DeltaTable.forName(self._spark, self._table_name)
        (
            target.alias("target")
            .merge(
                source.alias("source"),
                "target.user_id = source.user_id",
            )
            .whenMatchedUpdate(
                condition="source.observed_at >= target.observed_at",
                set={
                    "login": "source.login",
                    "user_type": "source.user_type",
                    "source_issue_id": "source.source_issue_id",
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .whenNotMatchedInsert(
                values={
                    "user_id": "source.user_id",
                    "login": "source.login",
                    "user_type": "source.user_type",
                    "source_issue_id": "source.source_issue_id",
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .execute()
        )

    @staticmethod
    def _require_unique_user_ids(records: Sequence[SilverUser]) -> None:
        """Reject a batch that would supply multiple rows for one MERGE key."""
        user_ids = {record.user_id for record in records}

        if len(user_ids) != len(records):
            raise ValueError("records must have unique Silver user IDs")

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret user observations ambiguously."""
        timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before writing Silver users."
            )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote each Unity Catalog identifier part."""
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
