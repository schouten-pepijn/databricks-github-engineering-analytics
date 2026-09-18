from itertools import islice

from github_engineering_analytics.api.client import GitHubApiClient


def main():
    client = GitHubApiClient()

    issues = client.iter_issues(
        owner="delta-io",
        repo="delta",
    )

    for issue in islice(issues, 5):
        print(
            issue["number"],
            issue["title"],
            issue["updated_at"],
        )


if __name__ == "__main__":
    main()
