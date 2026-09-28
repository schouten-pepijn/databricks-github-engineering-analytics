"""Represent GitHub issue-to-label associations at Silver grain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Self

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
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
from github_engineering_analytics.silver.labels import SilverLabel


@dataclass(frozen=True)
class SilverIssueLabel:
    """One label currently associated with one GitHub issue.

    Grain: one row per repository owner, repository name, issue ID, and label ID.
    Label attributes such as name and color remain owned by ``SilverLabel``.
    The persisted relation is the source of Gold ``bridge_issue_label``:
    it connects Issue facts to Label dimensions without repeating label fields
    on every Issue.
    """

    repository_owner: str
    repository_name: str
    issue_id: int
    label_id: int
    observed_at: datetime
    source_run_id: str

    @classmethod
    def from_bronze_row(
        cls,
        *,
        repository_owner: str,
        repository_name: str,
        issue_id: int,
        raw_json: str,
        source_updated_at: datetime,
        source_run_id: str,
    ) -> tuple[Self, ...]:
        """Return the validated label associations contained in one Bronze issue."""

        labels = SilverLabel.from_bronze_row(
            repository_owner=repository_owner,
            repository_name=repository_name,
            source_issue_id=issue_id,
            raw_json=raw_json,
            source_updated_at=source_updated_at,
            source_run_id=source_run_id,
        )

        relationships: list[Self] = []

        for label in labels:
            relationships.append(
                cls(
                    repository_owner=repository_owner,
                    repository_name=repository_name,
                    issue_id=issue_id,
                    label_id=label.label_id,
                    observed_at=label.observed_at,
                    source_run_id=source_run_id,
                )
            )

        return tuple(relationships)


@dataclass(frozen=True)
class SilverIssueLabelTransformation:
    """Hold one latest issue snapshot and its current label relationships.

    ``observed_issues`` retains a valid Issue even when its labels array is
    empty. The Delta writer uses that scope to remove stale issue-to-label
    relationships. It is technical reconciliation input, not a separate
    Silver or Gold table. ``relationships`` is the persisted Silver relation
    and the source for Gold ``bridge_issue_label``.
    """

    observed_issues: DataFrame
    relationships: DataFrame


class DeltaSilverIssueLabelWriter:
    """Persist current Issue-to-Label relationships at Silver grain.

    One row means one Label currently belongs to one Issue. This relation is
    the source for Gold ``bridge_issue_label``.
    """

    _RELATIONSHIP_COLUMNS: ClassVar[tuple[str, ...]] = (
        "repository_owner",
        "repository_name",
        "issue_id",
        "label_id",
        "observed_at",
        "source_run_id",
    )

    _OBSERVED_ISSUE_COLUMNS: ClassVar[tuple[str, ...]] = (
        "repository_owner",
        "repository_name",
        "issue_id",
        "observed_at",
        "source_run_id",
    )

    _OBSERVED_ISSUE_KEY: ClassVar[tuple[str, ...]] = (
        "repository_owner",
        "repository_name",
        "issue_id",
    )

    _RELATIONSHIP_KEY: ClassVar[tuple[str, ...]] = (
        "repository_owner",
        "repository_name",
        "issue_id",
        "label_id",
    )

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
    ) -> None:
        """Bind the writer to one Spark session and Silver relation table."""
        self._spark = spark
        self._schema_name = f"{config.catalog}.{config.silver_schema}"
        self._table_name = config.silver_issue_labels_table

    def ensure_table(self) -> None:
        """Create the current-state Issue-to-Label Delta table when absent."""
        require_utc_spark_session(
            self._spark,
            operation="writing Silver Issue-to-Label relationships",
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
            "label_id BIGINT NOT NULL, "
            "observed_at TIMESTAMP NOT NULL, "
            "source_run_id STRING NOT NULL"
            ") USING DELTA"
        )

    def reconcile_dataframe(
        self,
        *,
        observed_issues: DataFrame,
        relationships: DataFrame,
    ) -> None:
        """Atomically align stored relationships with one observed Issue scope.

        ``observed_issues`` may contain an Issue with no relationship rows. That
        explicit empty-label snapshot authorizes removal of its stale target
        relationships; it must therefore be reconciled alongside the current
        relationship rows rather than inferred from them.
        """
        require_utc_spark_session(
            self._spark,
            operation="writing Silver Issue-to-Label relationships",
        )
        require_required_columns(
            observed_issues,
            required_columns=self._OBSERVED_ISSUE_COLUMNS,
            source_name="Observed Issue source",
        )
        require_required_columns(
            relationships,
            required_columns=self._RELATIONSHIP_COLUMNS,
            source_name="Issue-to-Label relationship source",
        )
        require_unique_dataframe_keys(
            observed_issues,
            key_columns=self._OBSERVED_ISSUE_KEY,
            error_message="Observed Issue source contains duplicate business keys",
        )
        require_unique_dataframe_keys(
            relationships,
            key_columns=self._RELATIONSHIP_KEY,
            error_message=(
                "Issue-to-Label relationship source contains duplicate business keys"
            ),
        )
        self._require_relationships_match_observed_issues(
            observed_issues=observed_issues,
            relationships=relationships,
        )

        # Represent the new current state as upserts. Deletes are added below
        # and committed in the same Delta MERGE transaction.
        upserts = relationships.select(
            *self._RELATIONSHIP_COLUMNS,
            F.lit("upsert").alias("_operation"),
        )
        deletes = self._build_delete_source(
            observed_issues=observed_issues,
            relationships=relationships,
        )
        # The two sources are mutually exclusive per relationship business key:
        # a current relationship is never also eligible for deletion.
        self._merge_reconciliation_source(upserts.unionByName(deletes))

    def _require_relationships_match_observed_issues(
        self,
        *,
        observed_issues: DataFrame,
        relationships: DataFrame,
    ) -> None:
        """Reject relationships outside their declared Issue reconciliation scope.

        A relationship must inherit both the observation timestamp and source
        run ID of its Issue snapshot. Otherwise an unrelated observation could
        update or delete the wrong current-state relationships.
        """
        invalid_relationships = (
            relationships.alias("relationship")
            .join(
                observed_issues.alias("observed"),
                self._issue_key_join_condition(
                    left_alias="relationship",
                    right_alias="observed",
                ),
                "left",
            )
            .where(
                F.col("observed.issue_id").isNull()
                | (F.col("relationship.observed_at") != F.col("observed.observed_at"))
                | (
                    F.col("relationship.source_run_id")
                    != F.col("observed.source_run_id")
                )
            )
            .limit(1)
            .collect()
        )

        if invalid_relationships:
            raise ValueError(
                "Issue-to-Label relationships must match one observed Issue snapshot"
            )

    def _build_delete_source(
        self,
        *,
        observed_issues: DataFrame,
        relationships: DataFrame,
    ) -> DataFrame:
        """Return stale target rows that this observed Issue scope may delete.

        Only target rows for observed Issues are considered. An old Bronze
        replay may update neither a newer relationship nor delete it.
        """
        scoped_target_rows = (
            self._spark.table(self._table_name)
            .alias("target")
            .join(
                observed_issues.alias("observed"),
                self._issue_key_join_condition(
                    left_alias="target",
                    right_alias="observed",
                ),
                "inner",
            )
            # A replay may delete only rows no newer than its observation.
            .where(F.col("target.observed_at") <= F.col("observed.observed_at"))
            .select(
                F.col("target.repository_owner").alias("repository_owner"),
                F.col("target.repository_name").alias("repository_name"),
                F.col("target.issue_id").alias("issue_id"),
                F.col("target.label_id").alias("label_id"),
                F.col("observed.observed_at").alias("observed_at"),
                F.col("observed.source_run_id").alias("source_run_id"),
            )
        )

        # An Issue can deliberately have no labels. In that case this empty
        # source leaves every scoped target relationship eligible for deletion.
        current_relationship_keys = relationships.select(
            *self._RELATIONSHIP_KEY,
        )

        stale_relationships = scoped_target_rows.alias("target").join(
            current_relationship_keys.alias("relationship"),
            (
                (
                    F.col("target.repository_owner")
                    == F.col("relationship.repository_owner")
                )
                & (
                    F.col("target.repository_name")
                    == F.col("relationship.repository_name")
                )
                & (F.col("target.issue_id") == F.col("relationship.issue_id"))
                & (F.col("target.label_id") == F.col("relationship.label_id"))
            ),
            "left_anti",
        )

        return stale_relationships.select(
            *self._RELATIONSHIP_COLUMNS,
            F.lit("delete").alias("_operation"),
        )

    def _merge_reconciliation_source(
        self,
        source: DataFrame,
    ) -> None:
        """Apply relationship additions, updates and scoped removals atomically.

        Delta's one transaction ensures a failed reconciliation cannot leave
        an Issue with its old relationships deleted but its new ones absent.
        """
        target = DeltaTable.forName(self._spark, self._table_name)

        (
            target.alias("target")
            .merge(
                source.alias("source"),
                (
                    "target.repository_owner = source.repository_owner "
                    "AND target.repository_name = source.repository_name "
                    "AND target.issue_id = source.issue_id "
                    "AND target.label_id = source.label_id"
                ),
            )
            .whenMatchedDelete(
                condition="source._operation = 'delete'",
            )
            .whenMatchedUpdate(
                condition=(
                    "source._operation = 'upsert' "
                    "AND source.observed_at >= target.observed_at"
                ),
                set={
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .whenNotMatchedInsert(
                condition="source._operation = 'upsert'",
                values={
                    "repository_owner": "source.repository_owner",
                    "repository_name": "source.repository_name",
                    "issue_id": "source.issue_id",
                    "label_id": "source.label_id",
                    "observed_at": "source.observed_at",
                    "source_run_id": "source.source_run_id",
                },
            )
            .execute()
        )

    @staticmethod
    def _issue_key_join_condition(
        *,
        left_alias: str,
        right_alias: str,
    ) -> Column:
        """Build one repository-scoped Issue key join condition."""
        return (
            (
                F.col(f"{left_alias}.repository_owner")
                == F.col(f"{right_alias}.repository_owner")
            )
            & (
                F.col(f"{left_alias}.repository_name")
                == F.col(f"{right_alias}.repository_name")
            )
            & (F.col(f"{left_alias}.issue_id") == F.col(f"{right_alias}.issue_id"))
        )


class BronzeIssueToSilverLabelTransformer:
    """Reduce Bronze Issue snapshots into current Issue-to-Label relationships.

    The transformer intentionally returns both reconciliation scope and
    relationship rows. Exploding first would make a current ``labels=[]``
    snapshot disappear, leaving a writer unable to remove an old association.
    """

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
                            StructField(
                                "description",
                                StringType(),
                                nullable=True,
                            ),
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
    ) -> SilverIssueLabelTransformation:
        """Return latest observed issues and their current label relationships."""
        self._require_required_columns(bronze)

        parsed_rows = bronze.withColumn(
            "_payload",
            F.from_json(
                F.col("raw_json"),
                self._GITHUB_ISSUE_LABEL_SCHEMA,
            ),
        )

        issue_snapshots = parsed_rows.select(
            "repository_owner",
            "repository_name",
            "issue_id",
            F.col("source_updated_at").alias("observed_at"),
            F.col("_run_id").alias("source_run_id"),
            "_ingested_at",
            "_page_or_batch_reference",
            "raw_json",
            F.col("_payload.id").alias("_payload_issue_id"),
            F.col("_payload.labels").alias("_labels"),
        )

        self._require_valid_issue_rows(issue_snapshots)
        self._require_valid_relationship_rows(
            self._normalize_relationship_rows(issue_snapshots)
        )

        # Bronze is append-only, so choose one deterministic latest snapshot
        # before deciding which label relationships are currently true.
        latest_issue_window = Window.partitionBy(
            "repository_owner",
            "repository_name",
            "issue_id",
        ).orderBy(
            F.col("observed_at").desc(),
            F.col("_ingested_at").desc(),
            F.col("source_run_id").desc(),
            F.col("_page_or_batch_reference").desc(),
            F.col("raw_json").desc(),
        )

        latest_issues = (
            issue_snapshots.withColumn(
                "_row_number",
                F.row_number().over(latest_issue_window),
            )
            .where(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        # Keep the latest Issue even when its labels array is empty. The writer
        # needs this scope to remove any previously stored label associations.
        observed_issues = latest_issues.select(
            "repository_owner",
            "repository_name",
            "issue_id",
            "observed_at",
            "source_run_id",
        )

        # Only non-empty label arrays produce persisted bridge-source rows.
        relationships = self._normalize_relationship_rows(latest_issues).select(
            "repository_owner",
            "repository_name",
            "issue_id",
            "label_id",
            "observed_at",
            "source_run_id",
        )

        return SilverIssueLabelTransformation(
            observed_issues=observed_issues,
            relationships=relationships,
        )

    @staticmethod
    def _normalize_relationship_rows(issue_snapshots: DataFrame) -> DataFrame:
        """Extract label fields while retaining Issue metadata for validation."""
        return issue_snapshots.select(
            "repository_owner",
            "repository_name",
            "issue_id",
            "observed_at",
            "source_run_id",
            F.explode(F.col("_labels")).alias("_label"),
        ).select(
            "repository_owner",
            "repository_name",
            "issue_id",
            F.col("_label.id").alias("label_id"),
            F.col("_label.name").alias("_label_name"),
            F.lower(F.col("_label.color")).alias("_label_color"),
            F.col("_label").getField("default").alias("_label_is_default"),
            "observed_at",
            "source_run_id",
        )

    def _require_required_columns(self, bronze: DataFrame) -> None:
        """Reject Bronze DataFrames that cannot satisfy the relation contract."""
        if missing_columns := self._REQUIRED_BRONZE_COLUMNS - set(bronze.columns):
            raise ValueError(
                f"Bronze source is missing required columns: {sorted(missing_columns)}"
            )

    @staticmethod
    def _require_valid_issue_rows(rows: DataFrame) -> None:
        """Reject invalid Issue envelopes before empty arrays can hide them."""
        label_ids = F.transform(
            F.col("_labels"),
            lambda label: label.getField("id"),
        )
        invalid_rows = rows.where(
            F.col("repository_owner").isNull()
            | (F.length(F.trim(F.col("repository_owner"))) == 0)
            | F.col("repository_name").isNull()
            | (F.length(F.trim(F.col("repository_name"))) == 0)
            | F.col("issue_id").isNull()
            | (F.col("issue_id") <= 0)
            | F.col("_payload_issue_id").isNull()
            | (F.col("_payload_issue_id") <= 0)
            | (F.col("_payload_issue_id") != F.col("issue_id"))
            | F.col("_labels").isNull()
            | (F.size(F.col("_labels")) != F.size(F.array_distinct(label_ids)))
            | F.col("observed_at").isNull()
            | F.col("source_run_id").isNull()
            | (F.length(F.trim(F.col("source_run_id"))) == 0)
            | F.col("_ingested_at").isNull()
        )

        if invalid_rows.limit(1).count():
            raise ValueError(
                "Bronze source contains rows that cannot form valid "
                "Silver issue-label relationships"
            )

    @staticmethod
    def _require_valid_relationship_rows(rows: DataFrame) -> None:
        """Reject malformed nested labels before relationship reduction."""
        invalid_rows = rows.where(
            F.col("label_id").isNull()
            | (F.col("label_id") <= 0)
            | F.col("_label_name").isNull()
            | (F.length(F.trim(F.col("_label_name"))) == 0)
            | F.col("_label_color").isNull()
            | ~F.col("_label_color").rlike(r"^[0-9a-f]{6}$")
            | F.col("_label_is_default").isNull()
            | F.col("observed_at").isNull()
            | F.col("source_run_id").isNull()
            | (F.length(F.trim(F.col("source_run_id"))) == 0)
        )

        if invalid_rows.limit(1).count():
            raise ValueError(
                "Bronze source contains rows that cannot form valid "
                "Silver issue-label relationships"
            )
