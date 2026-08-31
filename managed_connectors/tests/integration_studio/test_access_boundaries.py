from __future__ import annotations

from types import SimpleNamespace

import integration_studio_app.app as studio
from integration_studio_app import connector_audit
from integration_studio_app import storage
from unittest.mock import Mock


def user(name: str, groups: list[str]):
    return SimpleNamespace(name=name, groups=groups, email=f"{name}@example.test")


def test_platform_origin_uses_the_runtime_domain(monkeypatch) -> None:
    monkeypatch.delenv("INTEGRATION_STUDIO_PLATFORM_URL", raising=False)
    monkeypatch.setenv("DATATAILR_DOMAIN", "sonic.example.test")

    assert studio._platform_origin() == "https://sonic.example.test"


def test_gateway_schema_link_uses_the_runtime_environment(monkeypatch) -> None:
    monkeypatch.setattr(studio, "DEPLOY_ENVIRONMENT", "prod")

    response = studio.app.test_client().get("/")

    assert response.status_code == 200
    assert b"/job/prod/connector-gateway/openapi.json" in response.data


def test_platform_admin_group_can_configure_workspace() -> None:
    access = studio._connector_access(user("platform-admin", ["dtusers", "admin"]))
    assert set(access["workspace_configurable"]) == {
        "slack", "hubspot", "github", "outlook", "zoom"
    }
    assert access["personal_configurable"] == ["gmail"]
    assert access["personal_connectable"] == ["gmail", "outlook", "zoom"]


def test_non_admin_can_only_connect_personal_mail() -> None:
    access = studio._connector_access(user("member", ["dtusers"]))
    assert access == {
        "workspace_configurable": [],
        "personal_configurable": ["gmail"],
        "personal_connectable": ["gmail", "outlook", "zoom"],
    }


def test_github_is_shared_live_and_admin_configured() -> None:
    state = storage._empty_state()
    state["settings"]["github"].update(
        {"app_id": "123", "installation_id": "456", "private_key": "pem"}
    )

    admin = {item["id"]: item for item in studio._source_status(
        state, user("admin", ["dtusers", "admin"])
    )}
    member = {item["id"]: item for item in studio._source_status(
        state, user("member", ["dtusers"])
    )}

    assert admin["github"]["usable"] is True
    assert member["github"]["usable"] is True
    assert member["github"]["trust"] == "shared"
    assert member["github"]["live_only"] is True


def test_user_can_save_only_their_personal_mail_settings(monkeypatch) -> None:
    state = storage._empty_state()
    monkeypatch.setattr(studio, "_current_user", lambda: user("trialuser", ["dtusers"]))
    monkeypatch.setattr(studio, "read_state", lambda: state)
    monkeypatch.setattr(studio, "update_state", lambda mutate: mutate(state))

    response = studio.app.test_client().post(
        "/api/personal-settings/gmail",
        json={"settings": {"username": "trial@example.test", "app_password": "abcd efgh ijkl mnop"}},
        headers={"X-Requested-With": "IntegrationStudio"},
    )

    assert response.status_code == 200
    assert response.json["settings"]["username"] == "trial@example.test"
    assert response.json["settings"]["has_app_password"] is True
    assert "app_password" not in response.json["settings"]
    assert state["users"]["trialuser"]["connector_settings"]["gmail"]["app_password"] == "abcdefghijklmnop"
    assert state["settings"]["gmail"]["username"] == ""


def test_outlook_application_is_not_exposed_as_personal_settings() -> None:
    state = storage._empty_state()
    state["settings"]["outlook"].update(
        {"tenant": "tenant-id", "client_id": "workspace-client", "client_secret": "secret"}
    )
    state["users"] = {
        "alice": {"tokens": {"outlook": {"access_token": "alice-token"}}},
        "bob": {},
    }

    alice = storage.public_personal_settings(state, "alice")
    bob = storage.public_personal_settings(state, "bob")

    assert alice["outlook"] == {"managed_by_admin": True}
    assert bob["outlook"] == {"managed_by_admin": True}
    assert storage.settings_for_user(state, "alice")["outlook"]["client_id"] == "workspace-client"


def test_outlook_calendar_authorization_is_scoped_to_each_user() -> None:
    state = storage._empty_state()
    state["settings"]["outlook"].update(
        {"tenant": "tenant-id", "client_id": "workspace-client", "client_secret": "secret"}
    )
    state["users"] = {
        "alice": {
            "tokens": {
                "outlook": {
                    "refresh_token": "alice-refresh",
                    "scope": "Mail.Read Calendars.ReadBasic",
                }
            }
        },
        "bob": {"tokens": {"outlook": {"refresh_token": "bob-refresh", "scope": "Mail.Read"}}},
    }

    alice = {item["id"]: item for item in studio._source_status(state, user("alice", ["dtusers"]))}
    bob = {item["id"]: item for item in studio._source_status(state, user("bob", ["dtusers"]))}

    assert alice["outlook"]["connected"] is True
    assert alice["outlook"]["calendar_authorized"] is True
    assert bob["outlook"]["connected"] is True
    assert bob["outlook"]["calendar_authorized"] is False


def test_zoom_application_is_workspace_managed_but_tokens_are_per_user() -> None:
    state = storage._empty_state()
    state["settings"]["zoom"].update(
        {"client_id": "workspace-client", "client_secret": "secret"}
    )
    state["users"] = {
        "alice": {"tokens": {"zoom": {
            "access_token": "alice-token",
            "scope": "meeting:read:list_meetings meeting:read:summary cloud_recording:read:meeting_transcript",
        }}},
        "bob": {},
    }

    alice = {item["id"]: item for item in studio._source_status(state, user("alice", ["dtusers"]))}
    bob = {item["id"]: item for item in studio._source_status(state, user("bob", ["dtusers"]))}

    assert storage.public_personal_settings(state, "alice")["zoom"] == {"managed_by_admin": True}
    assert alice["zoom"]["connected"] is True
    assert alice["zoom"]["ai_companion_authorized"] is True
    assert bob["zoom"]["connected"] is False
    assert bob["zoom"]["ai_companion_authorized"] is False
    assert bob["zoom"]["reason"] == "Not connected yet"


def test_zoom_disconnect_revokes_upstream_and_deletes_only_current_users_token(monkeypatch) -> None:
    state = storage._empty_state()
    state["settings"]["zoom"].update({"client_id": "client", "client_secret": "secret"})
    state["users"] = {
        "alice": {"tokens": {"zoom": {"access_token": "alice-access"}}},
        "bob": {"tokens": {"zoom": {"access_token": "bob-access"}}},
    }
    revoked = []
    index = Mock()
    index.delete_scope.return_value = 0
    monkeypatch.setattr(studio, "_current_user", lambda: user("alice", ["dtusers"]))
    monkeypatch.setattr(studio, "read_state", lambda: state)
    monkeypatch.setattr(studio, "update_state", lambda mutate: mutate(state))
    monkeypatch.setattr(studio, "revoke_oauth_token", lambda provider, cfg, token: revoked.append((provider, token["access_token"])))
    monkeypatch.setattr(studio, "get_index", lambda: index)

    response = studio.app.test_client().post(
        "/api/disconnect/zoom",
        json={},
        headers={"X-Requested-With": "IntegrationStudio"},
    )

    assert response.status_code == 200
    assert response.json["upstream_revoked"] is True
    assert revoked == [("zoom", "alice-access")]
    assert "zoom" not in state["users"]["alice"]["tokens"]
    assert state["users"]["bob"]["tokens"]["zoom"]["access_token"] == "bob-access"


def test_personal_source_status_uses_only_the_current_users_configuration() -> None:
    state = storage._empty_state()
    state["users"] = {
        "alice": {
            "connector_settings": {
                "gmail": {"username": "alice@gmail.test", "app_password": "abcdefghijklmnop"}
            }
        },
        "bob": {},
    }

    alice = {item["id"]: item for item in studio._source_status(state, user("alice", ["dtusers"]))}
    bob = {item["id"]: item for item in studio._source_status(state, user("bob", ["dtusers"]))}

    assert alice["gmail"]["configured"] is True
    assert bob["gmail"]["configured"] is False
    assert bob["gmail"]["reason"] == "Configure in Connections"


def test_admin_cannot_put_personal_mail_credentials_in_workspace_settings(monkeypatch) -> None:
    monkeypatch.setattr(studio, "_current_user", lambda: user("admin", ["dtusers", "admin"]))

    response = studio.app.test_client().post(
        "/api/settings",
        json={"settings": {"gmail": {"client_id": "shared", "client_secret": "shared"}}},
        headers={"X-Requested-With": "IntegrationStudio"},
    )

    assert response.status_code == 400
    assert response.json["error"] == "Personal Gmail settings must be saved from Connections"


def test_admin_can_configure_one_workspace_outlook_application(monkeypatch) -> None:
    state = storage._empty_state()
    monkeypatch.setattr(studio, "_current_user", lambda: user("admin", ["dtusers", "admin"]))
    monkeypatch.setattr(studio, "read_state", lambda: state)
    monkeypatch.setattr(studio, "update_state", lambda mutate: mutate(state))
    monkeypatch.setattr(studio, "get_sync_manager", Mock)

    response = studio.app.test_client().post(
        "/api/settings",
        json={"settings": {"outlook": {
            "tenant": "tenant-id",
            "client_id": "workspace-client",
            "client_secret": "workspace-secret",
            "redirect_uri": "https://example.test/oauth/outlook/callback",
        }}},
        headers={"X-Requested-With": "IntegrationStudio"},
    )

    assert response.status_code == 200
    assert state["settings"]["outlook"]["client_id"] == "workspace-client"
    assert "outlook" not in state["users"]


def test_ask_and_build_endpoints_are_not_part_of_the_connector_app() -> None:
    client = studio.app.test_client()
    page = client.get("/")

    assert b'data-view="ask"' not in page.data
    assert b'data-view="build"' not in page.data
    assert b'data-view="skills"' in page.data
    assert client.post("/api/chat", json={}).status_code == 404
    assert client.post("/api/generate-app", json={}).status_code == 404


def test_non_admin_settings_write_is_denied(monkeypatch) -> None:
    monkeypatch.setattr(studio, "_current_user", lambda: user("member", ["dtusers"]))
    response = studio.app.test_client().post(
        "/api/settings",
        json={"settings": {"slack": {"bot_token": "must-not-save"}}},
        headers={"X-Requested-With": "IntegrationStudio"},
    )
    assert response.status_code == 403
    assert response.json["error"] == "Only the Integration Studio administrator can change settings"


def test_connector_audit_api_is_platform_admin_only(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(connector_audit, "AUDIT_DB", tmp_path / "audit.sqlite3")
    monkeypatch.setattr(studio, "list_connector_events", connector_audit.list_connector_events)
    connector_audit.record_connector_event(
        user="alice", connector="gmail", capability="gmail.messages.recent",
        operation="query", status="succeeded", result_count=3,
        metadata={"surface": "integration-studio", "query": "private search", "limit": 20},
    )

    monkeypatch.setattr(studio, "_current_user", lambda: user("member", ["dtusers"]))
    denied = studio.app.test_client().get("/api/admin/connector-audit")
    assert denied.status_code == 403
    assert b"alice" not in denied.data

    monkeypatch.setattr(studio, "_current_user", lambda: user("platform-admin", ["dtusers", "admin"]))
    allowed = studio.app.test_client().get("/api/admin/connector-audit")
    assert allowed.status_code == 200
    assert allowed.json["events"][0]["metadata_only"] is True
    assert allowed.json["events"][0]["metadata"] == {"limit": 20, "surface": "integration-studio"}
    assert "private search" not in allowed.get_data(as_text=True)
