"""Delta persistence for immutable replay-input manifests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
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
from github_engineering_analytics.control.replay_manifest import ReplayManifest


class DeltaReplayManifestRepository:
    """Insert-only storage: a manifest_id once written, never changes."""

    # Explicit schema so Spark does not guess types. Keep in sync with
    # the CREATE TABLE statement in ensure_table().
    _ROW_SCHEMA: ClassVar[StructType] = StructType(
        [
            StructField("manifest_id", StringType(), nullable=False),
            StructField("repository_owner", StringType(), nullable=False),
            StructField("repository_name", StringType(), nullable=False),
            StructField("bronze_table", StringType(), nullable=False),
            StructField("bronze_version", LongType(), nullable=False),
            StructField(
                "successful_run_ids", ArrayType(StringType(), False), nullable=False
            ),
            StructField("cutoff_at", TimestampType(), nullable=False),
            StructField("code_commit", StringType(), nullable=False),
            StructField("transformation_version", StringType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        """Bind the repository to one Spark session and the manifest table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.control_schema}"
        self._table_name = config.replay_manifest_table

    def ensure_table(self) -> None:
        """Create the control schema and manifest table when they are absent."""
        require_utc_spark_session(
            self._spark, operation="reading or writing replay manifest table"
        )
        self._spark.sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{quote_multipart_identifier(self._schema_name)}"
        )

        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS
            {quote_multipart_identifier(self._table_name)}
            (
                manifest_id STRING NOT NULL,
                repository_owner STRING NOT NULL,
                repository_name STRING NOT NULL,
                bronze_table STRING NOT NULL,
                bronze_version BIGINT NOT NULL,
                successful_run_ids ARRAY<STRING> NOT NULL,
                cutoff_at TIMESTAMP NOT NULL,
                code_commit STRING NOT NULL,
                transformation_version STRING NOT NULL
            ) USING DELTA""")

    def create(self, manifest: ReplayManifest) -> None:
        """Insert a manifest once; an existing manifest_id is never modified.

        Repeating an identical create is a no-op. Reusing a manifest_id for
        different content fails, because the caller would otherwise believe
        it froze an input that was never stored.
        """
        require_utc_spark_session(
            self._spark,
            operation="writing replay manifest table",
        )

        # Value order must match the field order in _ROW_SCHEMA.
        source = self._spark.createDataFrame(
            [
                (
                    manifest.manifest_id,
                    manifest.repository_owner,
                    manifest.repository_name,
                    manifest.bronze_table,
                    manifest.bronze_version,
                    list(manifest.successful_run_ids),
                    manifest.cutoff_at,
                    manifest.code_commit,
                    manifest.transformation_version,
                )
            ],
            schema=self._ROW_SCHEMA,
        )

        # Insert-only MERGE: no "matched" clause, so an existing manifest_id
        # is left untouched. A plain append would write a duplicate row.
        (
            DeltaTable.forName(self._spark, self._table_name)
            .alias("target")
            .merge(source.alias("source"), "target.manifest_id = source.manifest_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Read back and compare: equal for a new insert or an identical
        # retry, different if the id was already taken by other content.
        stored = self.get(manifest.manifest_id)
        if stored != manifest:
            raise ValueError(
                f"Manifest {manifest.manifest_id} "
                "already exists with different content: "
                f"{stored} vs {manifest}"
            )

    def get(self, manifest_id: str) -> ReplayManifest:
        """Return exactly one stored manifest, rebuilt as a validated object."""
        if not manifest_id.strip():
            raise ValueError("manifest_id must be non-empty")

        require_utc_spark_session(
            self._spark,
            operation="reading replay manifest table",
        )

        rows = (
            self._spark.table(self._table_name)
            .where(F.col("manifest_id") == manifest_id)
            # Read the instant as epoch microseconds: a Python datetime from
            # Databricks Connect can arrive in the client's local time zone.
            .withColumn("cutoff_at_micros", F.unix_micros(F.col("cutoff_at")))
            .limit(2)  # Two rows -> error, because manifest_id is unique
            .collect()
        )

        if not rows:
            raise ValueError(f"Replay manifest not found: manifest_id={manifest_id!r}")

        if len(rows) > 1:
            raise RuntimeError(
                f"Replay manifest has duplicate manifest_id values: {manifest_id!r}"
            )

        row = rows[0]

        return ReplayManifest(
            manifest_id=row["manifest_id"],
            repository_owner=row["repository_owner"],
            repository_name=row["repository_name"],
            bronze_table=row["bronze_table"],
            bronze_version=row["bronze_version"],
            successful_run_ids=tuple(row["successful_run_ids"]),
            cutoff_at=datetime(1970, 1, 1, tzinfo=UTC)
            + timedelta(microseconds=row["cutoff_at_micros"]),
            code_commit=row["code_commit"],
            transformation_version=row["transformation_version"],
        )
