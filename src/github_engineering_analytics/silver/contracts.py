"""Reusable validation for normalized Silver DataFrames before Delta MERGE."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def require_required_columns(
    source: DataFrame,
    *,
    required_columns: Iterable[str],
    source_name: str,
) -> None:
    """Reject a MERGE source that cannot satisfy its Silver table contract.

    The iterable is consumed once, allowing callers to derive required columns
    directly from a Spark schema without materializing an intermediate tuple.
    """
    missing_columns = set(required_columns) - set(source.columns)

    if missing_columns:
        raise ValueError(
            f"{source_name} is missing required columns: {sorted(missing_columns)}"
        )


def require_unique_dataframe_keys(
    source: DataFrame,
    *,
    key_columns: Sequence[str],
    error_message: str,
) -> None:
    """Reject a MERGE source containing more than one row per business key."""
    if not key_columns:
        raise ValueError("key_columns must not be empty")

    duplicate_keys = (
        source.groupBy(*key_columns)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .collect()
    )

    if duplicate_keys:
        raise ValueError(error_message)
