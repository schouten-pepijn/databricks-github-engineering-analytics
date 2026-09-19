"""Manually print a few public GitHub issue records as a client smoke test."""

from itertools import islice

from github_engineering_analytics.api.client import GitHubClient


def main() -> None:
    """Fetch and display five issue records from the Delta Lake repository."""
    client = GitHubClient()

    issues = client.iter_issues(
        owner="delta-io",
        repository="delta",
    )

    for issue in islice(issues, 5):
        print(
            issue["number"],
            issue["title"],
            issue["updated_at"],
        )


if __name__ == "__main__":
    main()
