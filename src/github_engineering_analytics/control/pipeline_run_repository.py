"""Delta persistence for immutable pipeline run lifecycle state."""

from __future__ import annotations

from typing import ClassVar

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
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
from github_engineering_analytics.control.pipeline_run import (
    PipelineRun,
    PipelineRunStatus,
)


class DeltaPipelineRunRepository:
    """Create and later persist one Delta row per pipeline run.

    Pipeline runs are operational audit records for replay, diagnosis and
    watermark safety. They describe processing, not GitHub business entities,
    and are not intended as a Gold analytical source.
    """

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
        """Bind the repository to one Spark session and lifecycle table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.pipeline_runs_table

    def ensure_table(self) -> None:
        """Create the control schema and one-row-per-run Delta table when absent."""
        require_utc_spark_session(
            self._spark,
            operation="reading or writing pipeline runs",
        )

        self._spark.sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{quote_multipart_identifier(self._schema_name)}"
        )

        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{quote_multipart_identifier(self._table_name)} ("
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

        require_utc_spark_session(
            self._spark,
            operation="reading or writing pipeline runs",
        )

        source = self._create_source_dataframe(run)

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
        """Transition an existing RUNNING row to a terminal lifecycle state.

        A preflight lookup makes missing and already terminal run IDs explicit
        errors. The Delta condition repeats the RUNNING check so a concurrent
        completion cannot overwrite a terminal row between that lookup and the
        merge.
        """
        if run.status not in {
            PipelineRunStatus.SUCCEEDED,
            PipelineRunStatus.FAILED,
        }:
            raise ValueError(
                "record_finished requires a succeeded or failed pipeline run"
            )

        require_utc_spark_session(
            self._spark,
            operation="reading or writing pipeline runs",
        )
        self._require_existing_running_run(run.run_id)

        source = self._create_source_dataframe(run)
        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                "target.run_id = source.run_id",
            )
            .whenMatchedUpdate(
                # Do not regress a row if another attempt finished it after
                # the preflight lookup but before this merge was executed.
                condition="target.status = 'running'",
                set={
                    "status": "source.status",
                    "finished_at": "source.finished_at",
                    "candidate_watermark_value": ("source.candidate_watermark_value"),
                    "candidate_watermark_overlap_seconds": (
                        "source.candidate_watermark_overlap_seconds"
                    ),
                    "error_message": "source.error_message",
                },
            )
            .execute()
        )

    def record_candidate(self, run: PipelineRun) -> None:
        """Persist a candidate watermark while the run remains ``RUNNING``.

        This is intentionally narrower than :meth:`record_finished`: the
        Bronze-to-Silver stage may expose its candidate to downstream tasks,
        but it must not set a terminal status before the Gold gate succeeds.
        """
        if run.status is not PipelineRunStatus.RUNNING:
            raise ValueError("record_candidate requires a running pipeline run")

        require_utc_spark_session(
            self._spark,
            operation="reading or writing pipeline runs",
        )
        self._require_existing_running_run(run.run_id)

        source = self._create_source_dataframe(run)
        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                "target.run_id = source.run_id",
            )
            .whenMatchedUpdate(
                # Preserve a terminal outcome if another task finalized the
                # run after the preflight lookup but before this merge.
                condition="target.status = 'running'",
                set={
                    "candidate_watermark_value": "source.candidate_watermark_value",
                    "candidate_watermark_overlap_seconds": (
                        "source.candidate_watermark_overlap_seconds"
                    ),
                },
            )
            .execute()
        )

    def _create_source_dataframe(self, run: PipelineRun) -> DataFrame:
        """Serialize one immutable PipelineRun using the table's explicit schema."""
        watermark_before = run.watermark_before
        candidate_watermark = run.candidate_watermark

        return self._spark.createDataFrame(
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

    def _require_existing_running_run(self, run_id: str) -> None:
        """Reject missing, duplicated, or already terminal lifecycle rows.

        Fetching at most two rows detects a broken logical ``run_id`` key
        without collecting the complete control table.
        """
        rows = (
            self._spark.table(self._table_name)
            .where(F.col("run_id") == run_id)
            .select("status")
            .limit(2)
            .collect()
        )

        if not rows:
            raise ValueError(f"Pipeline run not found: run_id={run_id!r}")

        if len(rows) > 1:
            raise RuntimeError(
                f"Pipeline run table has duplicate run_id values: {run_id!r}"
            )

        status = rows[0]["status"]

        if status != PipelineRunStatus.RUNNING.value:
            raise ValueError(
                "Pipeline run must be running before it can finish: "
                f"run_id={run_id!r}, status={status!r}"
            )
