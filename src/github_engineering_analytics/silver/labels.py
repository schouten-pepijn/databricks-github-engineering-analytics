"""Normalize nested GitHub issue-label payloads into Silver domain records."""

from __future__ import annotations

import json
import string
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.common.config import PipelineConfig


@dataclass(frozen=True)
class SilverLabel:
    """One GitHub label observed in a Bronze issue payload.

    Grain: one label definition per repository and GitHub label ID.
    GitHub labels have no label-level update timestamp in an issue response,
    so ``observed_at`` uses the parent issue's source update time.
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
        labels: list[Self] = []
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


class DeltaSilverLabelWriter:
    """Create the repository-scoped Silver label table contract.

    Table grain: one row per repository owner, repository name, and GitHub
    label ID. ``description`` is the only nullable domain attribute.
    """

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("repository_owner", StringType(), nullable=False),
            StructField("repository_name", StringType(), nullable=False),
            StructField("label_id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("color", StringType(), nullable=False),
            StructField("description", StringType(), nullable=True),
            StructField("is_default", BooleanType(), nullable=False),
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
        """Bind the writer to one Spark session and Silver labels table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.silver_schema}"
        self._table_name = config.silver_labels_table

    def ensure_table(self) -> None:
        """Create the Silver schema and labels current-state table when absent."""
        self._require_utc_session()

        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{self._quote_identifier(self._table_name)} ("
            "repository_owner STRING NOT NULL, "
            "repository_name STRING NOT NULL, "
            "label_id BIGINT NOT NULL, "
            "name STRING NOT NULL, "
            "color STRING NOT NULL, "
            "description STRING, "
            "is_default BOOLEAN NOT NULL, "
            "source_issue_id BIGINT NOT NULL, "
            "observed_at TIMESTAMP NOT NULL, "
            "source_run_id STRING NOT NULL"
            ") USING DELTA"
        )

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret label timestamps ambiguously."""
        timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before writing Silver labels."
            )

    def upsert(self, records: Sequence[SilverLabel]) -> None:
        """Merge validated label observations into the Silver current-state table."""
        if not records:
            return

        self._require_utc_session()
        self._require_unique_business_keys(records)

        source = self._spark.createDataFrame(
            [
                (
                    record.repository_owner,
                    record.repository_name,
                    record.label_id,
                    record.name,
                    record.color,
                    record.description,
                    record.is_default,
                    record.source_issue_id,
                    record.observed_at,
                    record.source_run_id,
                )
                for record in records
            ],
            schema=self._ROW_SCHEMA,
        )

        self._merge_source(source)

    def _merge_source(self, source: DataFrame) -> None:
        """Merge one validated Label DataFrame into the current-state table."""
        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                (
                    "target.repository_owner = source.repository_owner "
                    "AND target.repository_name = source.repository_name "
                    "AND target.label_id = source.label_id"
                ),
            )
            .whenMatchedUpdate(
                condition="source.observed_at >= target.observed_at",
                set={
                    "name": "source.name",
                    "color": "source.color",
                    "description": "source.description",
                    "is_default": "source.is_default",
                    "source_issue_id": "source.source_issue_id",
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .whenNotMatchedInsert(
                values={
                    "repository_owner": "source.repository_owner",
                    "repository_name": "source.repository_name",
                    "label_id": "source.label_id",
                    "name": "source.name",
                    "color": "source.color",
                    "description": "source.description",
                    "is_default": "source.is_default",
                    "source_issue_id": "source.source_issue_id",
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .execute()
        )

    @staticmethod
    def _require_unique_business_keys(records: Sequence[SilverLabel]) -> None:
        """Reject multiple source rows for the same repository-scoped label."""
        business_keys = {
            (record.repository_owner, record.repository_name, record.label_id)
            for record in records
        }

        if len(business_keys) != len(records):
            raise ValueError("records must have unique Silver label business keys")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote each Unity Catalog identifier part."""
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


class BronzeIssueToSilverLabelTransformer:
    """Extract normalized label observations from Bronze issue payloads."""

    _REQUIRED_BRONZE_COLUMNS: ClassVar[frozenset[str]] = frozenset(
        {
            "repository_owner",
            "repository_name",
            "issue_id",
            "source_updated_at",
            "raw_json",
            "_run_id",
            "_ingested_at",
            "_page_or_batch_reference",
        }
    )

    _GITHUB_ISSUE_LABEL_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("id", LongType(), nullable=True),
            StructField(
                "labels",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=True),
                            StructField("name", StringType(), nullable=True),
                            StructField("color", StringType(), nullable=True),
                            StructField("description", StringType(), nullable=True),
                            StructField("default", BooleanType(), nullable=True),
                        ]
                    ),
                    containsNull=True,
                ),
                nullable=True,
            ),
        ]
    )

    def transform(
        self,
        bronze: DataFrame,
    ) -> DataFrame:
        """Validate the Bronze contract before parsing nested label JSON."""
        self._require_required_columns(bronze)

        raise NotImplementedError()

    def _require_required_columns(self, bronze: DataFrame) -> None:
        """Reject incomplete Bronze input before any Spark JSON operation."""
        missing_columns = self._REQUIRED_BRONZE_COLUMNS - set(bronze.columns)

        if missing_columns:
            raise ValueError(
                f"Bronze source is missing required columns: {sorted(missing_columns)}"
            )
