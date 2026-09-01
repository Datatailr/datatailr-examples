from __future__ import annotations

import base64
import json
import re
import time
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

_API_VERSION = "2022-11-28"
_REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class LiveGitHubError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _app_jwt(config: dict[str, Any]) -> str:
    app_id = str(config.get("app_id") or "").strip()
    private_key = str(config.get("private_key") or "").strip().replace("\\n", "\n")
    if not app_id or not private_key:
        raise LiveGitHubError("GitHub App ID and private key are not configured", 409)
    now = int(time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url(
        json.dumps({"iat": now - 60, "exp": now + 540, "iss": app_id}, separators=(",", ":")).encode()
    )
    signing_input = f"{header}.{payload}".encode()
    try:
        key = serialization.load_pem_private_key(private_key.encode(), password=None)
        signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except (TypeError, ValueError) as exc:
        raise LiveGitHubError("GitHub App private key is invalid", 409) from exc
    return f"{header}.{payload}.{_b64url(signature)}"


def _json(response: requests.Response, operation: str) -> Any:
    try:
        data = response.json()
    except ValueError as exc:
        raise LiveGitHubError(
            f"GitHub returned invalid JSON for {operation} (HTTP {response.status_code})", 502
        ) from exc
    if response.ok:
        return data
    message = data.get("message") if isinstance(data, dict) else response.reason
    status = 409 if response.status_code in {401, 403, 404} else 502
    raise LiveGitHubError(f"GitHub {operation} failed: {message or response.reason}", status)


def installation_token(config: dict[str, Any]) -> str:
    installation_id = str(config.get("installation_id") or "").strip()
    if not installation_id:
        raise LiveGitHubError("GitHub App installation ID is not configured", 409)
    base_url = str(config.get("base_url") or "https://api.github.com").rstrip("/")
    response = requests.post(
        f"{base_url}/app/installations/{installation_id}/access_tokens",
        headers={
            "Authorization": f"Bearer {_app_jwt(config)}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": _API_VERSION,
        },
        timeout=25,
    )
    data = _json(response, "installation authentication")
    token = str(data.get("token") or "") if isinstance(data, dict) else ""
    if not token:
        raise LiveGitHubError("GitHub did not return an installation token", 502)
    return token


def _get(config: dict[str, Any], token: str, path: str, params: dict[str, Any] | None = None) -> Any:
    base_url = str(config.get("base_url") or "https://api.github.com").rstrip("/")
    query = {
        key: value
        for key, value in (params or {}).items()
        if value is not None and value != ""
    }
    return _json(
        requests.get(
            f"{base_url}{path}",
            params=query,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": _API_VERSION,
            },
            timeout=25,
        ),
        path,
    )


def _repository(value: str) -> str:
    repository = str(value or "").strip()
    if not _REPOSITORY.fullmatch(repository) or ".." in repository:
        raise LiveGitHubError("repository must be in owner/name form")
    return repository


def _actor(value: Any) -> dict[str, str]:
    item = value if isinstance(value, dict) else {}
    return {"login": str(item.get("login") or ""), "avatar_url": str(item.get("avatar_url") or "")}


def _query_with_token(
    config: dict[str, Any], token: str, capability: str, params: dict[str, Any]
) -> list[dict[str, Any]] | dict[str, Any]:
    if capability == "github.repositories.list":
        data = _get(config, token, "/installation/repositories", {"per_page": params["limit"]})
        repositories = (data.get("repositories") or []) if isinstance(data, dict) else []
        return [
            {
                "id": str(item.get("id") or ""),
                "name": str(item.get("name") or ""),
                "full_name": str(item.get("full_name") or ""),
                "private": bool(item.get("private")),
                "description": str(item.get("description") or ""),
                "default_branch": str(item.get("default_branch") or ""),
                "updated_at": str(item.get("updated_at") or ""),
                "ref": str(item.get("html_url") or ""),
            }
            for item in repositories[: params["limit"]]
            if isinstance(item, dict)
        ]

    repository = _repository(params.get("repository", ""))
    if capability == "github.issues.recent":
        values = _get(
            config,
            token,
            f"/repos/{repository}/issues",
            {"state": params["state"], "sort": "updated", "direction": "desc", "per_page": params["limit"]},
        )
        return [
            {
                "id": str(item.get("id") or ""),
                "number": int(item.get("number") or 0),
                "repository": repository,
                "title": str(item.get("title") or ""),
                "body": str(item.get("body") or "")[: params["max_characters"]],
                "state": str(item.get("state") or ""),
                "author": _actor(item.get("user")),
                "labels": [str(label.get("name") or "") for label in item.get("labels") or [] if isinstance(label, dict)],
                "comments": int(item.get("comments") or 0),
                "created_at": str(item.get("created_at") or ""),
                "updated_at": str(item.get("updated_at") or ""),
                "ref": str(item.get("html_url") or ""),
            }
            for item in values
            if isinstance(item, dict) and "pull_request" not in item
        ][: params["limit"]]

    if capability == "github.pull_requests.recent":
        values = _get(
            config,
            token,
            f"/repos/{repository}/pulls",
            {"state": params["state"], "sort": "updated", "direction": "desc", "per_page": params["limit"]},
        )
        return [
            {
                "id": str(item.get("id") or ""),
                "number": int(item.get("number") or 0),
                "repository": repository,
                "title": str(item.get("title") or ""),
                "body": str(item.get("body") or "")[: params["max_characters"]],
                "state": str(item.get("state") or ""),
                "draft": bool(item.get("draft")),
                "author": _actor(item.get("user")),
                "head": str((item.get("head") or {}).get("ref") or ""),
                "base": str((item.get("base") or {}).get("ref") or ""),
                "created_at": str(item.get("created_at") or ""),
                "updated_at": str(item.get("updated_at") or ""),
                "ref": str(item.get("html_url") or ""),
            }
            for item in values
            if isinstance(item, dict)
        ][: params["limit"]]

    if capability == "github.commits.recent":
        values = _get(
            config,
            token,
            f"/repos/{repository}/commits",
            {"sha": params.get("ref") or "", "per_page": params["limit"]},
        )
        return [
            {
                "sha": str(item.get("sha") or ""),
                "repository": repository,
                "message": str(((item.get("commit") or {}).get("message")) or "")[: params["max_characters"]],
                "author": _actor(item.get("author")),
                "authored_at": str((((item.get("commit") or {}).get("author") or {}).get("date")) or ""),
                "ref": str(item.get("html_url") or ""),
            }
            for item in values
            if isinstance(item, dict)
        ][: params["limit"]]

    if capability == "github.repository.file":
        path = str(params["path"]).strip().lstrip("/")
        if not path or any(part in {"", ".", ".."} for part in path.split("/")):
            raise LiveGitHubError("path must identify one repository file")
        data = _get(
            config,
            token,
            f"/repos/{repository}/contents/{path}",
            {"ref": params.get("ref") or ""},
        )
        if not isinstance(data, dict) or data.get("type") != "file":
            raise LiveGitHubError("The requested GitHub path is not a file", 400)
        try:
            decoded = base64.b64decode(str(data.get("content") or "").replace("\n", ""), validate=True)
            text = decoded.decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise LiveGitHubError("The requested GitHub file is not UTF-8 text", 400) from exc
        truncated = len(text) > params["max_characters"]
        return {
            "repository": repository,
            "path": path,
            "sha": str(data.get("sha") or ""),
            "text": text[: params["max_characters"]],
            "truncated": truncated,
            "ref": str(data.get("html_url") or ""),
        }

    raise LiveGitHubError("Unknown GitHub capability", 400)


def query_live_github(
    state: dict[str, Any], capability: str, params: dict[str, Any]
) -> list[dict[str, Any]] | dict[str, Any]:
    config = (state.get("settings") or {}).get("github") or {}
    return _query_with_token(config, installation_token(config), capability, params)
