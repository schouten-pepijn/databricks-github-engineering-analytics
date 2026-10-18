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


# The Delta table property that switches the Change Data Feed on.
CHANGE_DATA_FEED_PROPERTY = "delta.enableChangeDataFeed"


def ensure_change_data_feed(spark: SparkSession, table_name: str) -> None:
    """Switch the Change Data Feed on for a table, only when it is not on yet.

    The feed is not retroactive: it records only the writes made after it was
    switched on. Call this before the first data write of a new table, and on
    every later run, so an older table is brought up to date as well.
    """
    quoted = quote_multipart_identifier(table_name)

    # Ask Delta for this one property. Reading it is a cheap metadata call, and
    # it writes nothing, so it is safe to run on every pipeline run.
    rows = spark.sql(
        f"SHOW TBLPROPERTIES {quoted} ('{CHANGE_DATA_FEED_PROPERTY}')"
    ).collect()

    # A missing property does not come back empty: Delta returns a sentence such
    # as "Table ... does not have property". Only the exact value "true" means
    # the feed is already on.
    # "rows and" guards against an empty result, so a missing row is treated as
    # "not on" and the ALTER below runs instead of raising an error.
    if rows and rows[0]["value"] == "true":
        return

    # ALTER TABLE writes a commit even when the value does not change. Without
    # the check above, every pipeline run would add an empty version.
    _ = spark.sql(
        f"ALTER TABLE {quoted} "
        f"SET TBLPROPERTIES ('{CHANGE_DATA_FEED_PROPERTY}' = 'true')"
    )
