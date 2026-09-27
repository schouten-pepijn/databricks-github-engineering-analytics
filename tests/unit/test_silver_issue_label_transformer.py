"""Unit tests for the Issue-label Spark transformation boundary."""

from unittest.mock import Mock

import pytest

from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer,
)


def test_transform_rejects_missing_required_bronze_columns() -> None:
    """Fail before Spark parsing when the Bronze input contract is incomplete."""
    bronze = Mock()
    bronze.columns = ["issue_id", "raw_json"]

    with pytest.raises(ValueError, match="Bronze source is missing required columns"):
        BronzeIssueToSilverLabelTransformer().transform(bronze)

    bronze.withColumn.assert_not_called()
