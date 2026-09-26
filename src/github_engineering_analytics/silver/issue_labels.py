"""Represent GitHub issue-to-label associations at Silver grain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Self

from github_engineering_analytics.silver.labels import SilverLabel


@dataclass(frozen=True)
class SilverIssueLabel:
    """One label currently associated with one GitHub issue.

    Grain: one row per repository owner, repository name, issue ID, and label ID.
    Label attributes such as name and color remain owned by ``SilverLabel``.
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
