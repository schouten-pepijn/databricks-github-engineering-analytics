from unittest.mock import Mock

import pytest

from github_engineering_analytics.silver.issues import (
    BronzeIssueToSilverTransformer,
)


def test_transform_rejects_missing_bronze_columns() -> None:
    """Fail before Spark parsing when the Bronze input contract is incomplete."""
    transformer = BronzeIssueToSilverTransformer()
    bronze = Mock()
    bronze.columns = [
        "repository_owner",
        "repository_name",
        "issue_id",
    ]

    with pytest.raises(ValueError, match="Bronze source is missing required columns"):
        transformer.transform(bronze)

    bronze.withColumn.assert_not_called()
