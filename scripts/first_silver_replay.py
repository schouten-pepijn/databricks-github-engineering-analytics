"""Replay one stored manifest into four isolated Silver candidate tables.

A thin wrapper for running the replay from a laptop through Databricks Connect.
The logic itself lives in github_engineering_analytics.orchestration.silver_replay,
which the Databricks job runs as well, so both always behave the same.
"""

import argparse

from databricks.connect import DatabricksSession

from github_engineering_analytics.orchestration.silver_replay import (
    run_silver_replay,
    validate_execution_id,
)

TEST_CATALOG = "github_engineering_analytics_test"


def main(manifest_id: str, execution_id: str) -> None:
    """Replay one stored manifest into the isolated V2-min schema."""
    # Validate the ID before connecting, so a typo costs nothing.
    try:
        validate_execution_id(execution_id)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    spark = (
        DatabricksSession.builder.profile("databricks-dev").serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    result = run_silver_replay(
        spark=spark,
        catalog=TEST_CATALOG,
        manifest_id=manifest_id,
        execution_id=execution_id,
    )
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--execution-id", required=True)
    arguments = parser.parse_args()
    main(arguments.manifest_id, arguments.execution_id)
