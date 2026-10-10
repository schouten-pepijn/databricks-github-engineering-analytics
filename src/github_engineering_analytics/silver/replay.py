"""Rebuild the Silver entity sets from frozen Bronze input, without writing."""

from dataclasses import dataclass

from pyspark.sql import DataFrame

from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer as IssueLabelTransformer,
)
from github_engineering_analytics.silver.issues import BronzeIssueToSilverTransformer
from github_engineering_analytics.silver.labels import (
    BronzeIssueToSilverLabelTransformer as LabelTransformer,
)
from github_engineering_analytics.silver.users import BronzeIssueToSilverUserTransformer


@dataclass(frozen=True)
class ReplaySilver:
    """The four Silver entity sets a replay produces"""

    issues: DataFrame
    users: DataFrame
    labels: DataFrame
    issue_labels: DataFrame


def replay_silver(bronze: DataFrame) -> ReplaySilver:
    """Apply the existing Silver normalizations to one frozen Bronze input.

    Every transformer keeps the latest observation per key, so the result is the
    state a sequence of per-run MERGEs would reach on an empty target.
    """
    return ReplaySilver(
        issues=BronzeIssueToSilverTransformer().transform(bronze),
        users=BronzeIssueToSilverUserTransformer().transform(bronze),
        labels=LabelTransformer().transform(bronze),
        # Only the relationships feed the bridge; observed_issues is writer scope.
        issue_labels=IssueLabelTransformer().transform(bronze).relationships,
    )
