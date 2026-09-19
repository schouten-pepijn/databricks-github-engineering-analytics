from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

import pyspark.sql.functions as F
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
from github_engineering_analytics.control.watermark import Watermark


class DeltaWatermarkRepository:
    """Persist the latest successful incremental position in a Delta table.

    The table grain is one row per ``(source_name, entity_name)``. A merge is
    monotonic: a stale retry cannot move a committed watermark backwards.
    """

    _UTC_TIMEZONES: ClassVar[set[str]] = {"UTC", "Etc/UTC"}

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("source_name", StringType(), nullable=False),
            StructField("entity_name", StringType(), nullable=False),
            StructField("watermark_column", StringType(), nullable=False),
            StructField("watermark_value", TimestampType(), nullable=False),
            StructField("overlap_seconds", LongType(), nullable=False),
            StructField("last_successful_run_id", StringType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.watermark_table

    def ensure_table(self) -> None:
        """Create the control schema and Delta table when they are absent."""

        self._require_utc_session()

        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{self._quote_identifier(self._table_name)} ("
            "source_name STRING NOT NULL, "
            "entity_name STRING NOT NULL, "
            "watermark_column STRING NOT NULL, "
            "watermark_value TIMESTAMP NOT NULL, "
            "overlap_seconds BIGINT NOT NULL, "
            "last_successful_run_id STRING NOT NULL, "
            "updated_at TIMESTAMP NOT NULL"
            ") USING DELTA"
        )

    def get(
        self,
        source_name: str,
        entity_name: str,
    ) -> Watermark | None:
        """
        Return the last successfully committed watermark for an entity.

        ``None`` means this source/entity has not been processed before.
        """

        self._validate_key(source_name, entity_name)
        self._require_utc_session()

        rows = (
            self._spark.table(self._table_name)
            .where(
                (F.col("source_name") == source_name)
                & (F.col("entity_name") == entity_name)
            )
            .select(
                "watermark_value",
                "overlap_seconds",
            )
            .limit(2)
            .collect()
        )

        if not rows:
            return None

        if len(rows) > 1:
            raise RuntimeError(
                "Watermark table violates its logical key for "
                f"source_name={source_name!r}, "
                f"entity_name={entity_name!r}."
            )

        watermark_value = rows[0]["watermark_value"]
        overlap_seconds = rows[0]["overlap_seconds"]

        if not isinstance(watermark_value, datetime):
            raise RuntimeError(
                f"Watermark value is not a datetime: {watermark_value!r}"
            )

        return Watermark(
            # PySpark returns a TimestampType as a local, naive Python value.
            # astimezone() preserves its instant; replace(tzinfo=UTC) would
            # incorrectly relabel a local clock time as UTC.
            value=watermark_value.astimezone(UTC),
            overlap_seconds=int(overlap_seconds),
        )

    def commit_success(
        self,
        source_name: str,
        entity_name: str,
        watermark_column: str,
        watermark: Watermark,
        run_id: str,
        committed_at: datetime,
    ) -> None:
        """
        Commit a candidate watermark after all downstream processing succeeds.

        The merge permits only equal-or-later values, making retries and
        out-of-order completions unable to regress the stored position.
        """
        self._validate_key(source_name, entity_name)
        self._require_utc_session()

        if not watermark_column.strip():
            raise ValueError("watermark_column must not be empty")

        if not run_id.strip():
            raise ValueError("run_id must not be empty")

        if committed_at.tzinfo is None or committed_at.utcoffset() is None:
            raise ValueError("committed_at must be timezone-aware")

        source = self._spark.createDataFrame(
            [
                (
                    source_name,
                    entity_name,
                    watermark_column,
                    watermark.value,
                    watermark.overlap_seconds,
                    run_id,
                    committed_at.astimezone(UTC),
                )
            ],
            schema=self._ROW_SCHEMA,
        )

        target = DeltaTable.forName(
            self._spark,
            self._table_name,
        )

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                (
                    "target.source_name = source.source_name "
                    "AND target.entity_name = source.entity_name"
                ),
            )
            .whenMatchedUpdate(
                condition=("source.watermark_value >= target.watermark_value"),
                set={
                    "watermark_column": "source.watermark_column",
                    "watermark_value": "source.watermark_value",
                    "overlap_seconds": "source.overlap_seconds",
                    "last_successful_run_id": ("source.last_successful_run_id"),
                    "updated_at": "source.updated_at",
                },
            )
            .whenNotMatchedInsert(
                values={
                    "source_name": "source.source_name",
                    "entity_name": "source.entity_name",
                    "watermark_column": "source.watermark_column",
                    "watermark_value": "source.watermark_value",
                    "overlap_seconds": "source.overlap_seconds",
                    "last_successful_run_id": ("source.last_successful_run_id"),
                    "updated_at": "source.updated_at",
                },
            )
            .execute()
        )

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret Delta TIMESTAMP values differently."""
        session_timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if session_timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before reading "
                "or writing watermarks."
            )

    @staticmethod
    def _validate_key(
        source_name: str,
        entity_name: str,
    ) -> None:
        if not source_name.strip():
            raise ValueError("source_name must not be empty")

        if not entity_name.strip():
            raise ValueError("entity_name must not be empty")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
