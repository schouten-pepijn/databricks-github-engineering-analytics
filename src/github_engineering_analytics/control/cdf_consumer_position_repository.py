"""Delta persistence for the Change Data Feed positions of one consumer."""

from __future__ import annotations

from collections.abc import Mapping
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
from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
    require_utc_spark_session,
)


class DeltaCdfConsumerPositionRepository:
    """Remember up to which Delta version each consumer has processed a table.

    The grain is one row per ``(consumer_name, source_table)``. All positions of
    one batch are written in a single MERGE, so they move together or not at all.
    A merge is monotonic: a stale retry cannot move a position backwards.
    This is pipeline control state, not an analytical source.
    """

    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("consumer_name", StringType(), nullable=False),
            StructField("source_table", StringType(), nullable=False),
            StructField("last_version", LongType(), nullable=False),
            StructField("last_run_id", StringType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        """Bind the repository to one Spark session and control table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.cdf_consumer_positions_table

    def ensure_table(self) -> None:
        """Create the control schema and Delta table when they are absent."""
        self._spark.sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{quote_multipart_identifier(self._schema_name)}"
        )
        self._spark.sql(
            "CREATE TABLE IF NOT EXISTS "
            f"{quote_multipart_identifier(self._table_name)} ("
            "consumer_name STRING NOT NULL, "
            "source_table STRING NOT NULL, "
            "last_version BIGINT NOT NULL, "
            "last_run_id STRING NOT NULL, "
            "updated_at TIMESTAMP NOT NULL"
            ") USING DELTA"
        )

    def get_positions(self, consumer_name: str) -> dict[str, int]:
        """Return the last processed version per source table of this consumer.

        An empty dict means this consumer has never committed a position.
        """
        self._require_name("consumer_name", consumer_name)
        rows = (
            self._spark.table(self._table_name)
            .where(F.col("consumer_name") == consumer_name)
            .select("source_table", "last_version")
            .collect()
        )
        positions = {str(row["source_table"]): int(row["last_version"]) for row in rows}
        # A duplicate key would collapse silently in the dict above, so compare.
        if len(positions) != len(rows):
            raise RuntimeError(
                "Position table violates its logical key for "
                f"consumer_name={consumer_name!r}"
            )
        return positions

    def commit_positions(
        self,
        consumer_name: str,
        positions: Mapping[str, int],
        run_id: str,
        committed_at: datetime,
    ) -> None:
        """Move all positions of one consumer in a single MERGE.

        Call this only after the candidate was built and compared successfully.
        """
        self._require_name("consumer_name", consumer_name)
        self._require_name("run_id", run_id)
        if not positions:
            raise ValueError("positions must not be empty")

        for table, version in positions.items():
            self._require_name("source table", table)
            # "type(x) is not int": bool is a subclass of int, so True would
            # otherwise pass as version 1.
            if type(version) is not int or version < 0:
                raise ValueError(f"version of {table} must be a non-negative integer")

        if committed_at.tzinfo is None or committed_at.utcoffset() is None:
            raise ValueError("committed_at must be timezone-aware")

        require_utc_spark_session(
            self._spark, operation="reading or writing CDF positions"
        )

        source = self._spark.createDataFrame(
            [
                (consumer_name, table, version, run_id, committed_at.astimezone(UTC))
                for table, version in positions.items()
            ],
            schema=self._ROW_SCHEMA,
        )
        columns = ("last_version", "last_run_id", "updated_at")
        (
            DeltaTable.forName(self._spark, self._table_name)
            .alias("target")
            .merge(
                source.alias("source"),
                "target.consumer_name = source.consumer_name "
                "AND target.source_table = source.source_table",
            )
            # ">=": committing the same version again is a harmless retry, while
            # an older version matches no rule, so a position never goes back.
            .whenMatchedUpdate(
                condition="source.last_version >= target.last_version",
                set={name: f"source.{name}" for name in columns},
            )
            .whenNotMatchedInsert(
                values={
                    "consumer_name": "source.consumer_name",
                    "source_table": "source.source_table",
                    **{name: f"source.{name}" for name in columns},
                }
            )
            .execute()
        )

    @staticmethod
    def _require_name(label: str, value: str) -> None:
        if not value.strip():
            raise ValueError(f"{label} must not be empty")
