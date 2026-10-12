"""Technical Delta and Unity Catalog invariants shared across pipeline layers."""

from __future__ import annotations

from pyspark.sql import SparkSession

# Both spellings name the same zone; Spark reports whichever one was configured.
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

    # Spark converts timestamps using this session setting when it reads and
    # writes. In any other zone the stored instants could shift by hours.
    session_timezone = spark.conf.get("spark.sql.session.timeZone")

    if session_timezone not in _UTC_TIMEZONES:
        raise RuntimeError(f"Spark session timezone must be UTC before {operation}.")


def quote_multipart_identifier(identifier: str) -> str:
    """Quote each Unity Catalog identifier part and escape embedded backticks."""
    parts = identifier.split(".")

    # An empty part (for example "catalog..table") would produce invalid SQL.
    if not all(parts):
        raise ValueError(f"Invalid multipart identifier: {identifier!r}")

    # Backticks stop an unusual name from breaking out of the SQL statement; a
    # literal backtick inside a name is escaped by doubling it.
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
