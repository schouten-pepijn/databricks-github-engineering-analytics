"""Represent GitHub issue-to-label associations at Silver grain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Self

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from github_engineering_analytics.silver.labels import SilverLabel


@dataclass(frozen=True)
class SilverIssueLabel:
    """One label currently associated with one GitHub issue.

    Grain: one row per repository owner, repository name, issue ID, and label ID.
    Label attributes such as name and color remain owned by ``SilverLabel``.
    The future persisted relation is the source of Gold ``bridge_issue_label``:
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
    empty. The future Delta writer uses that scope to remove stale
    issue-to-label relationships. It is technical reconciliation input, not a
    separate Silver or Gold table. ``relationships`` is the future persisted
    Silver relation and the source for Gold ``bridge_issue_label``.
    """

    observed_issues: DataFrame
    relationships: DataFrame


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

        observed_issues = latest_issues.select(
            "repository_owner",
            "repository_name",
            "issue_id",
            "observed_at",
            "source_run_id",
        )

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
