"""Write the changed-Issue workset to a Delta table that dbt can read.

The workset (see silver/changed_issues.py) is a DataFrame of Issue keys. dbt only
reads tables, so the incremental Gold candidate needs the workset as a table.

TEST only. The schema must start with a fixed prefix, so this function can never
write into a live schema, and the table is replaced on every call because a
workset belongs to exactly one batch.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession

from github_engineering_analytics.common.delta_contracts import (
    quote_multipart_identifier,
)
from github_engineering_analytics.orchestration.silver_replay import (
    require_test_catalog,
)
from github_engineering_analytics.silver.changed_issues import KEY_COLUMNS

# The table dbt reads. The name is fixed, so a dbt source can point at it.
WORKSET_TABLE = "changed_issues"

# Scratch schemas of the incremental Gold proof always start with this prefix.
WORKSET_SCHEMA_PREFIX = "github_analytics_s5_"

_SCHEMA = re.compile(r"github_analytics_s5_[a-z0-9_]{1,40}")


def write_workset(
    spark: SparkSession,
    keys: DataFrame,
    *,
    catalog: str,
    schema: str,
) -> int:
    """Replace the workset table with these Issue keys and return the row count.

    One row per Issue, with only the three key columns. The table is overwritten,
    not appended to, so a stale batch can never leak into the next one.
    """
    require_test_catalog(catalog)
    # fullmatch (not match) so that "github_analytics_s5_x; DROP" is rejected.
    if not _SCHEMA.fullmatch(schema):
        raise ValueError(
            f"schema must be {WORKSET_SCHEMA_PREFIX} followed by 1-40 lowercase "
            "letters, digits, or _"
        )
    missing = [name for name in KEY_COLUMNS if name not in keys.columns]
    if missing:
        raise ValueError(f"workset is missing key columns: {missing}")

    table = f"{catalog}.{schema}.{WORKSET_TABLE}"

    # An Issue can come from both feeds. Keep each key once, and nothing else.
    workset = keys.select(*KEY_COLUMNS).distinct()
    # Spark is lazy: count() is what forces the change feed to be read, so a bad
    # range fails here.
    expected_rows = workset.count()

    quoted_schema = quote_multipart_identifier(f"{catalog}.{schema}")
    _ = spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
    (
        workset.write.format("delta")
        .mode("overwrite")
        .saveAsTable(quote_multipart_identifier(table))
    )

    if (stored_rows := spark.table(table).count()) != expected_rows:
        raise RuntimeError(
            f"{table}: expected {expected_rows} rows, found {stored_rows}"
        )
    return stored_rows
