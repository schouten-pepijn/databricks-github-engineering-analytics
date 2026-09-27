"""Normalize Bronze GitHub issue records into Silver domain records."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar, Self

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
    require_utc_spark_session,
)
from github_engineering_analytics.silver.contracts import (
    require_required_columns,
    require_unique_dataframe_keys,
)


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

        author_payload = payload.get("user")
        if not isinstance(author_payload, dict):
            raise ValueError("payload['user'] must be a JSON object")

        return cls(
            repository_owner=repository_owner,
            repository_name=repository_name,
            issue_id=issue_id,
            issue_number=cls._require_positive_int(payload, "number"),
            title=cls._require_non_empty_string(payload, "title"),
            state=cls._require_non_empty_string(payload, "state"),
            is_pull_request="pull_request" in payload,
            author_user_id=cls._require_positive_int(
                author_payload,
                "id",
                field_path="payload['user']['id']",
            ),
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
    def _require_positive_int(
        payload: dict[str, object],
        field_name: str,
        *,
        field_path: str | None = None,
    ) -> int:
        """Read a required positive integer field."""
        value = payload.get(field_name)

        if type(value) is not int or value <= 0:
            raise ValueError(
                f"{field_path or f'payload[{field_name!r}]'} must be a positive integer"
            )

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
    or replace a newer issue state. This entity is the future source of the
    Gold ``fact_issue`` table; nested Labels remain outside this table because
    an Issue can have many Labels.
    """

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("repository_owner", StringType(), nullable=False),
            StructField("repository_name", StringType(), nullable=False),
            StructField("issue_id", LongType(), nullable=False),
            StructField("issue_number", LongType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("state", StringType(), nullable=False),
            StructField("is_pull_request", BooleanType(), nullable=False),
            StructField("author_user_id", LongType(), nullable=False),
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
        """Bind the writer to one Spark session and Silver issues table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.silver_schema}"
        self._table_name = config.silver_issues_table

    def ensure_table(self) -> None:
        """Create the Silver table and add columns required by newer contracts.

        Delta adds a column as nullable for an existing table because historic
        rows cannot be populated atomically during DDL. The next Bronze-to-
        Silver refresh writes the required author ID for each observed Issue.
        """
        require_utc_spark_session(
            self._spark,
            operation="writing Silver issues",
        )

        self._spark.sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{quote_multipart_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{quote_multipart_identifier(self._table_name)} ("
            "repository_owner STRING NOT NULL, "
            "repository_name STRING NOT NULL, "
            "issue_id BIGINT NOT NULL, "
            "issue_number BIGINT NOT NULL, "
            "title STRING NOT NULL, "
            "state STRING NOT NULL, "
            "is_pull_request BOOLEAN NOT NULL, "
            "author_user_id BIGINT NOT NULL, "
            "created_at TIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP NOT NULL, "
            "closed_at TIMESTAMP, "
            "source_run_id STRING NOT NULL"
            ") USING DELTA"
        )

        existing_columns = set(self._spark.table(self._table_name).columns)
        if "author_user_id" not in existing_columns:
            self._spark.sql(
                "ALTER TABLE "
                f"{quote_multipart_identifier(self._table_name)} "
                "ADD COLUMNS (author_user_id BIGINT)"
            )

    def upsert(self, records: Sequence[SilverIssue]) -> None:
        """Merge valid records without allowing one batch to duplicate a key."""
        if not records:
            return

        require_utc_spark_session(
            self._spark,
            operation="writing Silver issues",
        )
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
                    record.author_user_id,
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
        require_utc_spark_session(
            self._spark,
            operation="writing Silver issues",
        )
        require_required_columns(
            source,
            required_columns=(field.name for field in self._ROW_SCHEMA),
            source_name="Silver source",
        )
        require_unique_dataframe_keys(
            source,
            key_columns=("repository_owner", "repository_name", "issue_id"),
            error_message="Silver source contains duplicate business keys before MERGE",
        )
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
                    "author_user_id": "source.author_user_id",
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
                    "author_user_id": "source.author_user_id",
                    "created_at": "source.created_at",
                    "updated_at": "source.updated_at",
                    "closed_at": "source.closed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .execute()
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


class BronzeIssueToSilverTransformer:
    """Transform append-only Bronze issue history into current Silver issue rows."""

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

    _GITHUB_ISSUE_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("id", LongType(), nullable=True),
            StructField("number", LongType(), nullable=True),
            StructField("title", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField(
                "user",
                StructType(
                    [
                        StructField("id", LongType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
            StructField("created_at", StringType(), nullable=True),
            StructField("updated_at", StringType(), nullable=True),
            StructField("closed_at", StringType(), nullable=True),
        ]
    )

    _TIMESTAMP_WITH_TIMEZONE_PATTERN: ClassVar[str] = r".*(?:Z|[+-][0-9]{2}:[0-9]{2})$"

    def transform(
        self,
        bronze: DataFrame,
    ) -> DataFrame:
        """Return one validated, latest Silver row per issue business key."""
        self._require_required_columns(bronze)

        parsed_rows = bronze.withColumn(
            "_payload",
            F.from_json(
                F.col("raw_json"),
                self._GITHUB_ISSUE_SCHEMA,
            ),
        )

        normalized_rows = parsed_rows.select(
            F.col("repository_owner"),
            F.col("repository_name"),
            F.col("issue_id"),
            F.col("_payload.number").alias("issue_number"),
            F.col("_payload.title").alias("title"),
            F.col("_payload.state").alias("state"),
            F.get_json_object(
                F.col("raw_json"),
                "$.pull_request",
            )
            .isNotNull()
            .alias("is_pull_request"),
            # The ID, rather than the mutable login, is the durable author
            # relationship consumed by Gold's dim_user and fact_issue models.
            F.col("_payload.user.id").alias("author_user_id"),
            # Tolerant parsing lets the validation below raise one domain error
            # for malformed source timestamps, even when Spark ANSI mode is on.
            F.try_to_timestamp(F.col("_payload.created_at")).alias("created_at"),
            F.try_to_timestamp(F.col("_payload.updated_at")).alias("updated_at"),
            F.try_to_timestamp(F.col("_payload.closed_at")).alias("closed_at"),
            F.col("_run_id").alias("source_run_id"),
            # Retain ingestion metadata only as deterministic tie-breakers;
            # the final projection removes it from the Silver table contract.
            F.col("source_updated_at").alias("_source_updated_at"),
            F.col("_ingested_at"),
            F.col("_page_or_batch_reference"),
            F.col("raw_json"),
            F.col("_payload.id").alias("_payload_issue_id"),
            F.col("_payload.created_at").alias("_created_at_raw"),
            F.col("_payload.updated_at").alias("_updated_at_raw"),
            F.col("_payload.closed_at").alias("_closed_at_raw"),
        )

        self._require_valid_normalized_rows(normalized_rows)

        latest_window = Window.partitionBy(
            "repository_owner",
            "repository_name",
            "issue_id",
        ).orderBy(
            F.col("_source_updated_at").desc(),
            F.col("_ingested_at").desc(),
            F.col("source_run_id").desc(),
            F.col("_page_or_batch_reference").desc(),
            F.col("raw_json").desc(),
        )

        return (
            normalized_rows.withColumn(
                "_row_number",
                F.row_number().over(latest_window),
            )
            .where(F.col("_row_number") == 1)
            .select(
                "repository_owner",
                "repository_name",
                "issue_id",
                "issue_number",
                "title",
                "state",
                "is_pull_request",
                "author_user_id",
                "created_at",
                "updated_at",
                "closed_at",
                "source_run_id",
            )
        )

    def _require_required_columns(
        self,
        bronze: DataFrame,
    ) -> None:
        """Reject Bronze DataFrames that cannot satisfy the Silver contract."""
        if missing_columns := self._REQUIRED_BRONZE_COLUMNS - set(bronze.columns):
            raise ValueError(
                f"Bronze source is missing required columns: {sorted(missing_columns)}"
            )

    @classmethod
    def _require_valid_normalized_rows(cls, rows: DataFrame) -> None:
        """Fail before MERGE when Bronze JSON cannot form valid Silver records."""
        invalid_rows = rows.where(
            F.col("_payload_issue_id").isNull()
            | (F.col("_payload_issue_id") != F.col("issue_id"))
            | (F.col("issue_id") <= 0)
            | (F.col("_payload_issue_id") <= 0)
            | F.col("issue_number").isNull()
            | (F.col("issue_number") <= 0)
            | F.col("title").isNull()
            | (F.length(F.trim(F.col("title"))) == 0)
            | F.col("state").isNull()
            | (F.length(F.trim(F.col("state"))) == 0)
            | F.col("author_user_id").isNull()
            | (F.col("author_user_id") <= 0)
            | F.col("repository_owner").isNull()
            | (F.length(F.trim(F.col("repository_owner"))) == 0)
            | F.col("repository_name").isNull()
            | (F.length(F.trim(F.col("repository_name"))) == 0)
            | F.col("source_run_id").isNull()
            | (F.length(F.trim(F.col("source_run_id"))) == 0)
            | F.col("_source_updated_at").isNull()
            | F.col("_ingested_at").isNull()
            | F.col("_created_at_raw").isNull()
            | ~F.col("_created_at_raw").rlike(cls._TIMESTAMP_WITH_TIMEZONE_PATTERN)
            | F.col("_updated_at_raw").isNull()
            | ~F.col("_updated_at_raw").rlike(cls._TIMESTAMP_WITH_TIMEZONE_PATTERN)
            | F.col("created_at").isNull()
            | F.col("updated_at").isNull()
            | (F.col("updated_at") != F.col("_source_updated_at"))
            | (
                F.col("_closed_at_raw").isNotNull()
                & (
                    F.col("closed_at").isNull()
                    | ~F.col("_closed_at_raw").rlike(
                        cls._TIMESTAMP_WITH_TIMEZONE_PATTERN
                    )
                )
            )
        )

        if invalid_rows.limit(1).count():
            raise ValueError(
                "Bronze source contains rows that cannot form valid Silver issues"
            )
