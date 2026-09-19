from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession

from github_engineering_analytics.common.config import PipelineConfig
from github_engineering_analytics.control.watermark import Watermark


class DeltaWatermarkRepository:
    """Repository for managing watermark state in Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        self._spark = spark
        self._table_name = config.watermark_table

    def ensure_table(self) -> None:
        """Create the watermark table if it does not exist."""


    def get(
        self,
        source_name: str,
        entity_name: str,
    ) -> Watermark | None:
        """
        Return the last successfully committed watermark.

        None means this source/entity has not been processed before.
        """

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
        Return the last successfully committed watermark.

        None means this source/entity has not been processed before.
        """
