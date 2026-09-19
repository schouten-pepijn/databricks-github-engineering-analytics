"""Delta persistence for immutable pipeline run lifecycle state."""

from __future__ import annotations

from typing import ClassVar

from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig


class DeltaPipelineRunRepository:
    """Create and later persist one Delta row per pipeline run."""

    _UTC_TIMEZONES: ClassVar[frozenset[str]] = frozenset({"UTC", "Etc/UTC"})

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.pipeline_runs_table

    def ensure_table(self) -> None:
        """Create the control schema and pipeline-run Delta table when absent."""
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
        """Persist a running pipeline run without duplicating its run ID."""
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
        parts = identifier.split(".")

        if not all(parts):
            raise ValueError(f"Invalid multipart identifier: {identifier!r}")

        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
