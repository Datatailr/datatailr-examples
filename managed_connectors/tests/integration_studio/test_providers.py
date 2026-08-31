from __future__ import annotations

from unittest.mock import Mock

from integration_studio_app import providers


def test_github_connection_test_uses_installation_token(monkeypatch):
    settings = {
        "github": {
            "app_id": "123",
            "installation_id": "456",
            "private_key": "pem",
            "base_url": "https://api.github.test",
        }
    }
    token_response = Mock(ok=True, status_code=201, reason="Created")
    token_response.json.return_value = {"token": "short-lived"}
    repositories_response = Mock(ok=True, status_code=200, reason="OK")
    repositories_response.json.return_value = {"total_count": 3, "repositories": []}
    post = Mock(return_value=token_response)
    get = Mock(return_value=repositories_response)
    monkeypatch.setattr(providers, "_github_app_jwt", lambda _cfg: "signed-app-jwt")
    monkeypatch.setattr(providers.requests, "post", post)
    monkeypatch.setattr(providers.requests, "get", get)

    result = providers.test_connector("github", settings)

    assert result == {"ok": True, "label": "GitHub App connected · 3 selected repositories"}
    assert post.call_args.args[0].endswith("/app/installations/456/access_tokens")
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer signed-app-jwt"
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer short-lived"
