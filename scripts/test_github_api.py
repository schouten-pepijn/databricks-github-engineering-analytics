from itertools import islice

from github_engineering_analytics.api.client import GitHubClient


def main() -> None:
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
