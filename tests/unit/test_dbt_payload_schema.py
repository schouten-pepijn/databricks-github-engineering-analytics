"""Guard the payload schema that dbt re-declares for the daily snapshots.

stg_bronze_issue_observations.sql parses raw_json with its own from_json
schema, because the Python parsing cannot be called from dbt. If a field is
renamed in Python and not in dbt, dbt would silently return nulls. This test
fails instead.
"""

import re
from pathlib import Path

from pyspark.sql.types import ArrayType, StructType

from github_engineering_analytics.silver.issue_labels import (
    BronzeIssueToSilverLabelTransformer,
)
from github_engineering_analytics.silver.issues import BronzeIssueToSilverTransformer

STAGING_MODEL = (
    Path(__file__).parents[2] / "dbt/models/staging/stg_bronze_issue_observations.sql"
)


def dbt_payload_fields() -> tuple[set[str], set[str]]:
    """Return the top-level and label field names declared in the dbt SQL."""
    sql = STAGING_MODEL.read_text(encoding="utf-8")
    # The schema is the single-quoted string that starts with "struct<"
    match = re.search(r"'(struct<.*?>)'", sql, flags=re.DOTALL)
    assert match, "no struct<...> schema found in the dbt staging model"
    schema = match.group(1)

    # Label fields sit inside labels: array<struct<...>>.
    label_part = re.search(r"labels:\s*array<struct<(.*?)>>", schema, flags=re.DOTALL)
    assert label_part, "no labels array found in the dbt schema"
    label_fields = set(re.findall(r"(\w+):", label_part.group(1)))

    # Top level: remove the labels block first, then read the field names.
    top_level = schema.replace(label_part.group(0), "labels:")
    top_fields = set(re.findall(r"(\w+):", top_level))
    return top_fields, label_fields


def test_dbt_issue_fields_exist_in_the_python_issue_schema() -> None:
    top_fields, _ = dbt_payload_fields()
    python_fields = {
        field.name for field in BronzeIssueToSilverTransformer._GITHUB_ISSUE_SCHEMA
    }

    # labels is parsed by the label transformer, so it is checked separately.
    assert top_fields - {"labels"} <= python_fields


def test_dbt_label_fields_exist_in_the_python_label_schema() -> None:
    top_fields, label_fields = dbt_payload_fields()
    label_schema = BronzeIssueToSilverLabelTransformer._GITHUB_ISSUE_LABEL_SCHEMA
    labels_field = label_schema["labels"]

    # dataType is the broad DataType class. Narrow it step by step: labels is an
    # array, and each array element is a struct that has the label fields.
    labels_type = labels_field.dataType
    assert isinstance(labels_type, ArrayType)
    element_type = labels_type.elementType
    assert isinstance(element_type, StructType)
    python_label_fields = {field.name for field in element_type}

    assert "labels" in top_fields
    assert label_fields <= python_label_fields
