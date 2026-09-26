from unittest.mock import Mock

import pytest

from github_engineering_analytics.silver.labels import (
    BronzeIssueToSilverLabelTransformer,
)


def test_transform_rejects_missing_required_bronze_columns() -> None:
    """Fail before Spark parsing when the Bronze input contract is incomplete."""
    transformer = BronzeIssueToSilverLabelTransformer()
    bronze = Mock()
    bronze.columns = [
        "issue_id",
        "raw_json",
    ]

    with pytest.raises(ValueError, match="Bronze source is missing required columns"):
        transformer.transform(bronze)

    bronze.withColumn.assert_not_called()
