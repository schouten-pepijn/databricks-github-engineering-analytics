from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import ClassVar

import requests
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity.wait import wait_base


class GitHubApiError(RuntimeError):
    """Base exception for GitHub API errors."""


class GitHubRetryableError(GitHubApiError):
    """Base exception for retryable GitHub API errors."""


class GitHubRateLimitError(GitHubRetryableError):
    """Raised when GitHub rate limiting is encountered."""

    def __init__(
        self,
        message: str,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class GitHubServerError(GitHubRetryableError):
    """Raised for retryable GitHub 5xx responses."""


class WaitGitHubRateLimit(wait_base):
    """
    Tenacity wait strategy.

    If GitHub explicitly provides a retry delay through Retry-After
    or X-RateLimit-Reset, that delay is used. Otherwise the fallback
    Tenacity wait strategy is used.
    """

    def __init__(self, fallback: wait_base) -> None:
        self.fallback = fallback

    def __call__(self, retry_state: RetryCallState) -> float:
        if retry_state.outcome is not None:
            exception = retry_state.outcome.exception()

            if isinstance(exception, GitHubRateLimitError):
                if exception.retry_after_seconds is not None:
                    return exception.retry_after_seconds

        return self.fallback(retry_state)


class GitHubClient:
    BASE_URL = "https://api.github.com"

    RETRYABLE_STATUS_CODES: ClassVar[set[int]] = {
        500,
        502,
        503,
        504,
    }

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
            wait=WaitGitHubRateLimit(
                fallback=wait_exponential(
                    multiplier=1,
                    min=1,
                    max=30,
                )
            ),
            reraise=True,
        )

    def _request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> requests.Response:
        """
        Execute a request with retry handling.

        Retries:
        - GitHub rate-limit errors
        - HTTP 500/502/503/504
        - requests.Timeout
        - requests.ConnectionError

        Does not retry regular client errors such as 400, 401 or 404.
        """
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
        """Execute exactly one HTTP request."""
        response = self.session.request(
            method=method,
            url=url,
            timeout=self.timeout_seconds,
            **kwargs,
        )

        if self._is_rate_limited(response):
            retry_after_seconds = self._get_rate_limit_delay(response)
            message = (
                "GitHub API rate limit exceeded: "
                f"{response.status_code} {response.text}"
            )

            raise GitHubRateLimitError(
                message,
                retry_after_seconds=retry_after_seconds,
            )

        if response.status_code in self.RETRYABLE_STATUS_CODES:
            raise GitHubServerError(
                f"GitHub server error: {response.status_code} {response.text}"
            )

        if not response.ok:
            raise GitHubApiError(
                f"GitHub API request failed: {response.status_code} {response.text}"
            )

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

    @staticmethod
    def _get_rate_limit_delay(
        response: requests.Response,
    ) -> float | None:
        retry_after = response.headers.get("Retry-After")

        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass

        reset_at = response.headers.get("X-RateLimit-Reset")

        if reset_at is not None:
            try:
                reset_timestamp = float(reset_at)

                return max(
                    reset_timestamp - time.time(),
                    0.0,
                )
            except ValueError:
                pass

        return None

    def iter_issues(
        self,
        owner: str,
        repository: str,
        since: datetime | None = None,
        per_page: int = 100,
    ) -> Iterator[dict]:
        """
        Iterate over GitHub issues for a repository.

        GitHub's issues endpoint also returns pull requests. Those are
        deliberately retained here and can be classified later in Silver.
        """
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
            if since.tzinfo is None or since.utcoffset() is None:
                raise ValueError("since must be timezone-aware")

            params["since"] = since.astimezone(UTC).isoformat().replace("+00:00", "Z")

        while True:
            response = self._request(
                method="GET",
                url=url,
                params=params.copy(),
            )

            records = response.json()

            if not isinstance(records, list):
                raise GitHubApiError(
                    "Expected GitHub API response to contain a list of issues."
                )

            if not records:
                break

            yield from records

            if len(records) < per_page:
                break

            params["page"] = int(params["page"]) + 1
