"""Delta persistence for immutable pipeline run lifecycle state."""

from __future__ import annotations

from typing import ClassVar

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
from github_engineering_analytics.control.pipeline_run import (
    PipelineRun,
    PipelineRunStatus,
)


class DeltaPipelineRunRepository:
    """Create and later persist one Delta row per pipeline run."""

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    # Keep this aligned with ensure_table(). An explicit schema prevents Spark
    # from inferring nullable fields or timestamp types from a single row.
    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("source_name", StringType(), nullable=False),
            StructField("entity_name", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("started_at", TimestampType(), nullable=False),
            StructField("finished_at", TimestampType(), nullable=True),
            StructField("watermark_before_value", TimestampType(), nullable=True),
            StructField(
                "watermark_before_overlap_seconds",
                LongType(),
                nullable=True,
            ),
            StructField("candidate_watermark_value", TimestampType(), nullable=True),
            StructField(
                "candidate_watermark_overlap_seconds",
                LongType(),
                nullable=True,
            ),
            StructField("error_message", StringType(), nullable=True),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.pipeline_runs_table

    def ensure_table(self) -> None:
        """Create the control schema and one-row-per-run Delta table when absent."""
        self._require_utc_session()

        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{self._quote_identifier(self._table_name)} ("
            "run_id STRING NOT NULL, "
            "source_name STRING NOT NULL, "
            "entity_name STRING NOT NULL, "
            "status STRING NOT NULL, "
            "started_at TIMESTAMP NOT NULL, "
            "finished_at TIMESTAMP, "
            "watermark_before_value TIMESTAMP, "
            "watermark_before_overlap_seconds BIGINT, "
            "candidate_watermark_value TIMESTAMP, "
            "candidate_watermark_overlap_seconds BIGINT, "
            "error_message STRING"
            ") USING DELTA"
        )

    def record_started(self, run: PipelineRun) -> None:
        """Persist a RUNNING run without duplicating its ``run_id``.

        A repeated job-start attempt is an insert-only no-op. It must not
        overwrite the existing lifecycle row, because the original run may
        already have progressed to a terminal state.
        """
        if run.status is not PipelineRunStatus.RUNNING:
            raise ValueError("record_started requires a running pipeline run")

        self._require_utc_session()

        watermark_before = run.watermark_before
        candidate_watermark = run.candidate_watermark

        source = self._spark.createDataFrame(
            [
                (
                    run.run_id,
                    run.source_name,
                    run.entity_name,
                    run.status.value,
                    run.started_at,
                    run.finished_at,
                    watermark_before.value if watermark_before is not None else None,
                    (
                        watermark_before.overlap_seconds
                        if watermark_before is not None
                        else None
                    ),
                    (
                        candidate_watermark.value
                        if candidate_watermark is not None
                        else None
                    ),
                    (
                        candidate_watermark.overlap_seconds
                        if candidate_watermark is not None
                        else None
                    ),
                    run.error_message,
                )
            ],
            schema=self._ROW_SCHEMA,
        )

        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                "target.run_id = source.run_id",
            )
            # No matched clause: a retry of the same start does not mutate
            # the row that was first created for this run ID.
            .whenNotMatchedInsert(
                values={
                    "run_id": "source.run_id",
                    "source_name": "source.source_name",
                    "entity_name": "source.entity_name",
                    "status": "source.status",
                    "started_at": "source.started_at",
                    "finished_at": "source.finished_at",
                    "watermark_before_value": "source.watermark_before_value",
                    "watermark_before_overlap_seconds": (
                        "source.watermark_before_overlap_seconds"
                    ),
                    "candidate_watermark_value": "source.candidate_watermark_value",
                    "candidate_watermark_overlap_seconds": (
                        "source.candidate_watermark_overlap_seconds"
                    ),
                    "error_message": "source.error_message",
                }
            )
            .execute()
        )

    def record_finished(self, run: PipelineRun) -> None:
        """Validate a terminal run before its guarded persistence is implemented.

        This deliberately accepts only SUCCEEDED and FAILED states. The next
        implementation step will update an existing RUNNING row, rather than
        inserting a new row or overwriting a terminal state.
        """
        if run.status not in {
            PipelineRunStatus.SUCCEEDED,
            PipelineRunStatus.FAILED,
        }:
            raise ValueError(
                "record_finished requires a succeeded or failed pipeline run"
            )

        self._require_utc_session()

    def _require_utc_session(self) -> None:
        """Reject sessions that would interpret Delta timestamps differently."""
        session_timezone = self._spark.conf.get("spark.sql.session.timeZone")

        if session_timezone not in self._UTC_TIMEZONES:
            raise RuntimeError(
                "Spark session timezone must be UTC before reading "
                "or writing pipeline runs."
            )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote each part of a Unity Catalog multipart identifier safely."""
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
