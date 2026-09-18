from __future__ import annotations

from collections.abc import Iterator
from typing import ClassVar
from datetime import datetime

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class GitHubApiError(RuntimeError):
    """Base exception for GitHub API errors."""


class GitHubRetryableError(GitHubApiError):
    """Base exception for retryable GitHub API errors."""


class GitHubRateLimitError(GitHubRetryableError):
    """Raised when GitHub rate limiting is encountered."""


class GitHubServerError(GitHubRetryableError):
    """Raised for retryable GitHub 5xx responses."""


class GitHubClient:
    BASE_URL = "https://api.github.com"

    RETRYABLE_STATUS_CODES: ClassVar[set[int]] = {500, 502, 503, 504}

    def __init__(
        self,
        token: str | None = None,
        timeout_seconds: int = 30,
        max_attempts: int = 5,
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

        self.retrying = Retrying(
            retry=retry_if_exception_type(
                (
                    GitHubRetryableError,
                    requests.Timeout,
                    requests.ConnectionError,
                )
            ),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=1,
                max=30,
            ),
            reraise=True,
        )

    def _request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> requests.Response:
        return self.retrying(
            self._request_once,
            method,
            url,
            **kwargs,
        )

    def _request_once(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> requests.Response:
        response = self.session.request(
            method=method,
            url=url,
            timeout=self.timeout_seconds,
            **kwargs,
        )

        if self._is_rate_limited(response):
            raise GitHubRateLimitError(f"GitHub API rate limit exceeded: {response.status_code} {response.text}")

        if response.status_code in self.RETRYABLE_STATUS_CODES:
            raise GitHubServerError(f"GitHub server error: {response.status_code} {response.text}")

        if not response.ok:
            raise GitHubApiError(f"GitHub API request failed: {response.status_code} {response.text}")

        return response

    @staticmethod
    def _is_rate_limited(
        response: requests.Response,
    ) -> bool:
        if response.status_code == 429:
            return True

        if response.status_code == 403:
            remaining = response.headers.get("X-RateLimit-Remaining")

            if remaining == "0":
                return True

        return False

    def iter_issues(
        self,
        owner: str,
        repository: str,
        since: datetime | None = None,
        per_page: int = 100,
    ) -> Iterator[dict]:
        if not 1 <= per_page <= 100:
            raise ValueError("per_page must be between 1 and 100")

        url = f"{self.BASE_URL}/repos/{owner}/{repository}/issues"

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
            response = self._request(
                method="GET",
                url=url,
                params=params.copy(),
            )

            records = response.json()

            if not isinstance(records, list):
                raise GitHubApiError("Expected GitHub API response to contain a list of issues.")

            if not records:
                break

            yield from records

            if len(records) < per_page:
                break

            params["page"] = int(params["page"]) + 1
