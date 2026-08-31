from __future__ import annotations

import base64
from unittest.mock import Mock

import pytest

from connector_gateway_service import live_github


def _state() -> dict:
    return {"settings": {"github": {"base_url": "https://api.github.test"}}}


def test_recent_issues_filters_pull_requests_and_bounds_bodies(monkeypatch):
    monkeypatch.setattr(live_github, "installation_token", lambda _config: "short-lived")
    response = Mock(ok=True, status_code=200, reason="OK")
    response.json.return_value = [
        {
            "id": 1, "number": 1, "title": "Pull request", "body": "not an issue",
            "state": "open", "pull_request": {}, "user": {"login": "alice"},
            "labels": [], "comments": 0, "created_at": "", "updated_at": "", "html_url": "",
        },
        {
            "id": 2, "number": 2, "title": "Issue", "body": "x" * 1000,
            "state": "open", "user": {"login": "bob", "avatar_url": "avatar"},
            "labels": [{"name": "bug"}], "comments": 3, "created_at": "a",
            "updated_at": "b", "html_url": "https://github.test/acme/repo/issues/2",
        },
    ]
    get = Mock(return_value=response)
    monkeypatch.setattr(live_github.requests, "get", get)

    rows = live_github.query_live_github(
        _state(),
        "github.issues.recent",
        {"repository": "acme/repo", "state": "open", "limit": 20, "max_characters": 500},
    )

    assert [row["number"] for row in rows] == [2]
    assert len(rows[0]["body"]) == 500
    assert rows[0]["labels"] == ["bug"]
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer short-lived"


def test_repository_file_is_decoded_in_memory_and_bounded(monkeypatch):
    monkeypatch.setattr(live_github, "installation_token", lambda _config: "short-lived")
    response = Mock(ok=True, status_code=200, reason="OK")
    response.json.return_value = {
        "type": "file", "sha": "abc", "content": base64.b64encode(b"hello world").decode(),
        "html_url": "https://github.test/acme/repo/blob/main/README.md",
    }
    monkeypatch.setattr(live_github.requests, "get", Mock(return_value=response))

    result = live_github.query_live_github(
        _state(),
        "github.repository.file",
        {"repository": "acme/repo", "path": "README.md", "ref": "", "max_characters": 5},
    )

    assert result["text"] == "hello"
    assert result["truncated"] is True


@pytest.mark.parametrize("repository", ["repo", "owner/repo/extra", "../repo", "owner/../repo"])
def test_repository_must_be_owner_name(monkeypatch, repository):
    monkeypatch.setattr(live_github, "installation_token", lambda _config: "short-lived")
    with pytest.raises(live_github.LiveGitHubError, match="owner/name"):
        live_github.query_live_github(
            _state(),
            "github.commits.recent",
            {"repository": repository, "ref": "", "limit": 1, "max_characters": 100},
        )
