from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import requests


class GitHubApiError(RuntimeError):
    pass


class GitHubApiClient:
    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.timeout_seconds = timeout_seconds

        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "github-engineering-analytics",
        }

        if token:
            headers["Authorization"] = f"Bearer {token}"

        self.session = requests.Session()
        self.session.headers.update(headers)

    def iter_issues(
        self,
        owner: str,
        repo: str,
        since: datetime | None = None,
        per_page: int = 100,
        max_pages: int | None = None,
    ) -> Iterator[dict]:
        url = f"{self.BASE_URL}/repos/{owner}/{repo}/issues"

        params: dict[str, str | int] = {
            "state": "all",
            "sort": "updated",
            "direction": "asc",
            "per_page": per_page,
            "page": 1,
        }

        if since is not None:
            params["since"] = since.isoformat().replace("+00:00", "Z")

        while True:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout_seconds,
            )

            if not response.ok:
                raise GitHubApiError(f"GitHub API request failed: {response.status_code} {response.text}")

            records = response.json()

            if not records:
                break

            yield from records

            if len(records) < per_page:
                break

            params["page"] = int(params["page"]) + 1
