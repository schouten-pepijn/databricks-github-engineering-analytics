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
