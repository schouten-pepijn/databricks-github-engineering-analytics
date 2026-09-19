from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
import requests
from tenacity import wait_none

from github_engineering_analytics.api.client import (
    GitHubApiError,
    GitHubClient,
    GitHubRateLimitError,
)


@pytest.fixture
def client() -> GitHubClient:
    client = GitHubClient(max_attempts=5)

    # Disable real waiting during unit tests.
    client.retrying.wait = wait_none()

    return client


def make_response(
    status_code: int,
    *,
    json_data=None,
    text: str = "",
    headers: dict[str, str] | None = None,
) -> Mock:
    response = Mock(spec=requests.Response)

    response.status_code = status_code
    response.ok = 200 <= status_code < 400
    response.text = text
    response.headers = headers or {}

    response.json.return_value = json_data

    return response


def test_iter_issues_returns_records(
    client: GitHubClient,
) -> None:
    response = make_response(
        200,
        json_data=[
            {
                "id": 1,
                "number": 10,
                "title": "First issue",
            }
        ],
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    issues = list(
        client.iter_issues(
            owner="delta-io",
            repository="delta",
        )
    )

    assert len(issues) == 1
    assert issues[0]["id"] == 1

    request_mock.assert_called_once()


def test_iter_issues_passes_watermark_as_since(
    client: GitHubClient,
) -> None:
    response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    watermark = datetime(
        2026,
        9,
        19,
        12,
        0,
        tzinfo=UTC,
    )

    list(
        client.iter_issues(
            owner="delta-io",
            repository="delta",
            since=watermark,
        )
    )

    _, kwargs = request_mock.call_args

    assert kwargs["params"]["since"] == ("2026-09-19T12:00:00Z")


def test_iter_issues_normalizes_since_to_utc(
    client: GitHubClient,
) -> None:
    response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    since = datetime(
        2026,
        9,
        19,
        12,
        0,
        tzinfo=timezone(timedelta(hours=2)),
    )

    list(
        client.iter_issues(
            owner="delta-io",
            repository="delta",
            since=since,
        )
    )

    _, kwargs = request_mock.call_args

    assert kwargs["params"]["since"] == "2026-09-19T10:00:00Z"


def test_iter_issues_rejects_naive_since(
    client: GitHubClient,
) -> None:
    request_mock = Mock()
    client.session.request = request_mock

    with pytest.raises(ValueError, match="timezone-aware"):
        list(
            client.iter_issues(
                owner="delta-io",
                repository="delta",
                since=datetime(2026, 9, 19, 12, 0),
            )
        )

    request_mock.assert_not_called()


def test_iter_issues_handles_pagination(
    client: GitHubClient,
) -> None:
    page_1 = make_response(
        200,
        json_data=[{"id": index} for index in range(100)],
    )

    page_2 = make_response(
        200,
        json_data=[{"id": 100}],
    )

    request_mock = Mock(
        side_effect=[
            page_1,
            page_2,
        ]
    )

    client.session.request = request_mock

    issues = list(
        client.iter_issues(
            owner="delta-io",
            repository="delta",
            per_page=100,
        )
    )

    assert len(issues) == 101
    assert issues[-1]["id"] == 100
    assert request_mock.call_count == 2

    first_call = request_mock.call_args_list[0]
    second_call = request_mock.call_args_list[1]

    assert first_call.kwargs["params"]["page"] == 1
    assert second_call.kwargs["params"]["page"] == 2


def test_request_retries_server_error_then_succeeds(
    client: GitHubClient,
) -> None:
    failed_response = make_response(
        503,
        text="Service Unavailable",
    )

    success_response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(
        side_effect=[
            failed_response,
            success_response,
        ]
    )

    client.session.request = request_mock

    response = client._request(
        "GET",
        "https://api.github.com/test",
    )

    assert response is success_response
    assert request_mock.call_count == 2


def test_request_retries_timeout_then_succeeds(
    client: GitHubClient,
) -> None:
    success_response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(
        side_effect=[
            requests.Timeout("Request timed out"),
            success_response,
        ]
    )

    client.session.request = request_mock

    response = client._request(
        "GET",
        "https://api.github.com/test",
    )

    assert response is success_response
    assert request_mock.call_count == 2


def test_request_retries_connection_error_then_succeeds(
    client: GitHubClient,
) -> None:
    success_response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(
        side_effect=[
            requests.ConnectionError("Connection failed"),
            success_response,
        ]
    )

    client.session.request = request_mock

    response = client._request(
        "GET",
        "https://api.github.com/test",
    )

    assert response is success_response
    assert request_mock.call_count == 2


def test_request_does_not_retry_non_retryable_error(
    client: GitHubClient,
) -> None:
    response = make_response(
        404,
        text="Not Found",
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with pytest.raises(
        GitHubApiError,
        match="404",
    ):
        client._request(
            "GET",
            "https://api.github.com/test",
        )

    assert request_mock.call_count == 1


def test_request_retries_429_rate_limit_then_succeeds(
    client: GitHubClient,
) -> None:
    rate_limited = make_response(
        429,
        text="Too Many Requests",
    )

    success_response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(
        side_effect=[
            rate_limited,
            success_response,
        ]
    )

    client.session.request = request_mock

    response = client._request(
        "GET",
        "https://api.github.com/test",
    )

    assert response is success_response
    assert request_mock.call_count == 2


def test_request_retries_primary_rate_limit_403_then_succeeds(
    client: GitHubClient,
) -> None:
    rate_limited = make_response(
        403,
        text="API rate limit exceeded",
        headers={
            "X-RateLimit-Remaining": "0",
        },
    )

    success_response = make_response(
        200,
        json_data=[],
    )

    request_mock = Mock(
        side_effect=[
            rate_limited,
            success_response,
        ]
    )

    client.session.request = request_mock

    response = client._request(
        "GET",
        "https://api.github.com/test",
    )

    assert response is success_response
    assert request_mock.call_count == 2


def test_request_exhausts_retries_on_rate_limit(
    client: GitHubClient,
) -> None:
    rate_limited = make_response(
        429,
        text="Too Many Requests",
    )

    request_mock = Mock(return_value=rate_limited)
    client.session.request = request_mock

    with pytest.raises(GitHubRateLimitError):
        client._request(
            "GET",
            "https://api.github.com/test",
        )

    assert request_mock.call_count == 5


def test_rate_limit_uses_retry_after_header(
    client: GitHubClient,
) -> None:
    response = make_response(
        429,
        text="Too Many Requests",
        headers={
            "Retry-After": "12",
        },
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with pytest.raises(GitHubRateLimitError) as exc_info:
        client._request_once(
            "GET",
            "https://api.github.com/test",
        )

    assert exc_info.value.retry_after_seconds == 12


def test_rate_limit_uses_reset_timestamp(
    client: GitHubClient,
) -> None:
    response = make_response(
        403,
        text="Rate limit exceeded",
        headers={
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": "1015",
        },
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with (
        patch(
            "github_engineering_analytics.api.client.time.time",
            return_value=1000,
        ),
        pytest.raises(GitHubRateLimitError) as exc_info,
    ):
        client._request_once(
            "GET",
            "https://api.github.com/test",
        )

    assert exc_info.value.retry_after_seconds == 15


def test_retry_after_takes_precedence_over_reset_timestamp(
    client: GitHubClient,
) -> None:
    response = make_response(
        429,
        text="Too Many Requests",
        headers={
            "Retry-After": "8",
            "X-RateLimit-Reset": "9999999999",
        },
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with pytest.raises(GitHubRateLimitError) as exc_info:
        client._request_once(
            "GET",
            "https://api.github.com/test",
        )

    assert exc_info.value.retry_after_seconds == 8


def test_invalid_retry_after_falls_back_to_reset_timestamp(
    client: GitHubClient,
) -> None:
    response = make_response(
        429,
        text="Too Many Requests",
        headers={
            "Retry-After": "invalid",
            "X-RateLimit-Reset": "1010",
        },
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with (
        patch(
            "github_engineering_analytics.api.client.time.time",
            return_value=1000,
        ),
        pytest.raises(GitHubRateLimitError) as exc_info,
    ):
        client._request_once(
            "GET",
            "https://api.github.com/test",
        )

    assert exc_info.value.retry_after_seconds == 10


def test_iter_issues_rejects_invalid_per_page(
    client: GitHubClient,
) -> None:
    request_mock = Mock()
    client.session.request = request_mock

    with pytest.raises(
        ValueError,
        match="per_page must be between 1 and 100",
    ):
        list(
            client.iter_issues(
                owner="delta-io",
                repository="delta",
                per_page=101,
            )
        )

    request_mock.assert_not_called()


def test_iter_issues_rejects_unexpected_response_shape(
    client: GitHubClient,
) -> None:
    response = make_response(
        200,
        json_data={"message": "unexpected response"},
    )

    request_mock = Mock(return_value=response)
    client.session.request = request_mock

    with pytest.raises(
        GitHubApiError,
        match="Expected GitHub API response",
    ):
        list(
            client.iter_issues(
                owner="delta-io",
                repository="delta",
            )
        )
