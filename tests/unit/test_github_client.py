from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from github_engineering_analytics.api.client import GitHubApiClient, GitHubApiError


def test_iter_issues_returns_records():
    client = GitHubApiClient()

    response = Mock()
    response.ok = True
    response.json.return_value = [
        {
            "id": 1,
            "number": 10,
            "title": "First issue",
        }
    ]

    client.session.get = Mock(return_value=response)

    issues = list(
        client.iter_issues(
            owner="delta-io",
            repo="delta",
        )
    )

    assert len(issues) == 1
    assert issues[0]["id"] == 1


def test_iter_issues_passes_watermark_as_since():
    client = GitHubApiClient()

    response = Mock()
    response.ok = True
    response.json.return_value = []

    client.session.get = Mock(return_value=response)

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
            repo="delta",
            since=watermark,
        )
    )

    _, kwargs = client.session.get.call_args

    assert kwargs["params"]["since"] == "2026-09-19T12:00:00Z"


def test_iter_issues_handles_pagination():
    client = GitHubApiClient()

    page_1 = Mock()
    page_1.ok = True
    page_1.json.return_value = [{"id": index} for index in range(100)]

    page_2 = Mock()
    page_2.ok = True
    page_2.json.return_value = [{"id": 100}]

    client.session.get = Mock(
        side_effect=[
            page_1,
            page_2,
        ]
    )

    issues = list(
        client.iter_issues(
            owner="delta-io",
            repo="delta",
            per_page=100,
        )
    )

    assert len(issues) == 101
    assert client.session.get.call_count == 2


def test_iter_issues_raises_on_api_error():
    client = GitHubApiClient()

    response = Mock()
    response.ok = False
    response.status_code = 500
    response.text = "Internal Server Error"

    client.session.get = Mock(return_value=response)

    with pytest.raises(GitHubApiError):
        list(
            client.iter_issues(
                owner="delta-io",
                repo="delta",
            )
        )
