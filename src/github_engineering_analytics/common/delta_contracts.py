"""Technical Delta and Unity Catalog invariants shared across pipeline layers."""

from __future__ import annotations

from pyspark.sql import SparkSession

_UTC_TIMEZONES = frozenset({"UTC", "Etc/UTC"})


def require_utc_spark_session(
    spark: SparkSession,
    *,
    operation: str,
) -> None:
    """Reject a Spark session that could interpret Delta timestamps ambiguously.

    ``operation`` remains at the caller so domain-specific failure context stays
    visible where the Delta read or write is performed.
    """
    if not operation.strip():
        raise ValueError("operation must not be empty")

    session_timezone = spark.conf.get("spark.sql.session.timeZone")

    if session_timezone not in _UTC_TIMEZONES:
        raise RuntimeError(f"Spark session timezone must be UTC before {operation}.")


def quote_multipart_identifier(identifier: str) -> str:
    """Quote each Unity Catalog identifier part and escape embedded backticks."""
    parts = identifier.split(".")

    if not all(parts):
        raise ValueError(f"Invalid multipart identifier: {identifier!r}")

    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
