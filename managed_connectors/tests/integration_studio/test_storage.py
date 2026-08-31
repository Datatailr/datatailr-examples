from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from integration_studio_app import storage


class StateOwnershipTest(unittest.TestCase):
    def test_github_private_key_is_never_exposed_by_settings_api_shape(self) -> None:
        settings = storage._empty_state()["settings"]
        settings["github"].update(
            {"app_id": "123", "installation_id": "456", "private_key": "private-pem"}
        )

        visible = storage.public_workspace_settings(settings)["github"]

        self.assertNotIn("private_key", visible)
        self.assertTrue(visible["has_private_key"])
        self.assertEqual(visible["app_id"], "123")

    def test_retired_github_personal_credentials_and_tokens_are_discarded(self) -> None:
        state = storage._merge_defaults(
            {
                "settings": {
                    "github": {
                        "app_id": "org-app",
                        "private_key": "org-pem",
                        "client_id": "retired-client",
                        "client_secret": "retired-secret",
                    },
                    "github_personal": {
                        "client_id": "personal-client",
                        "client_secret": "personal-secret",
                    },
                },
                "users": {
                    "alice": {"tokens": {"github_personal": {"access_token": "token"}}}
                },
                "oauth_states": {"github-state": {"provider": "github_personal"}},
            }
        )

        self.assertNotIn("github_personal", state["settings"])
        self.assertNotIn("client_id", state["settings"]["github"])
        self.assertNotIn("client_secret", state["settings"]["github"])
        self.assertNotIn("github_personal", state["users"]["alice"]["tokens"])
        self.assertNotIn("github-state", state["oauth_states"])

    def test_gmail_is_personal_and_oauth_applications_are_workspace_managed(self) -> None:
        state = storage._merge_defaults(
            {
                "settings": {
                    "gmail": {"client_id": "shared", "client_secret": "must-not-fallback"},
                    "outlook": {"client_id": "shared", "client_secret": "must-not-fallback"},
                    "zoom": {"client_id": "zoom-shared", "client_secret": "zoom-secret"},
                }
            }
        )

        self.assertEqual(state["settings"]["gmail"], storage.DEFAULT_SETTINGS["gmail"])
        self.assertEqual(state["settings"]["outlook"]["client_id"], "shared")
        self.assertEqual(state["settings"]["outlook"]["client_secret"], "must-not-fallback")
        self.assertEqual(state["settings"]["zoom"]["client_id"], "zoom-shared")

    def test_legacy_per_user_outlook_application_is_discarded_but_token_is_kept(self) -> None:
        state = storage._merge_defaults(
            {
                "users": {
                    "alice": {
                        "connector_settings": {
                            "outlook": {"client_id": "alice-app", "client_secret": "secret"}
                        },
                        "tokens": {"outlook": {"refresh_token": "delegated"}},
                    }
                }
            }
        )

        self.assertNotIn("outlook", state["users"]["alice"]["connector_settings"])
        self.assertEqual(
            state["users"]["alice"]["tokens"]["outlook"]["refresh_token"], "delegated"
        )

    def test_legacy_shared_connector_groups_are_discarded(self) -> None:
        state = storage._merge_defaults(
            {
                "settings": {
                    "slack": {"bot_token": "secret", "allowed_groups": ["engineering"]},
                    "hubspot": {"access_token": "secret", "allowed_groups": ["sales"]},
                    "github": {"private_key": "pem", "allowed_groups": ["engineering"]},
                }
            }
        )

        self.assertNotIn("allowed_groups", state["settings"]["slack"])
        self.assertNotIn("allowed_groups", state["settings"]["hubspot"])
        self.assertNotIn("allowed_groups", state["settings"]["github"])

    def test_legacy_user_gmail_oauth_credentials_and_token_are_discarded(self) -> None:
        state = storage._merge_defaults(
            {
                "users": {
                    "alice": {
                        "connector_settings": {
                            "gmail": {"client_id": "old-id", "client_secret": "old-secret"}
                        },
                        "tokens": {"gmail": {"refresh_token": "old-refresh"}},
                    }
                },
                "oauth_states": {"gmail-state": {"provider": "gmail"}},
            }
        )

        self.assertEqual(
            state["users"]["alice"]["connector_settings"]["gmail"],
            storage.DEFAULT_SETTINGS["gmail"],
        )
        self.assertNotIn("gmail", state["users"]["alice"]["tokens"])
        self.assertNotIn("gmail-state", state["oauth_states"])

    def test_privileged_write_preserves_existing_runtime_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.enc"
            state_path.write_bytes(b"previous")
            previous = state_path.stat()
            cipher = Mock()
            cipher.encrypt.return_value = b"encrypted"

            with (
                patch.object(storage, "STATE_PATH", state_path),
                patch.object(storage, "_cipher", return_value=cipher),
                patch.object(storage.os, "geteuid", return_value=0),
                patch.object(storage.os, "chown") as chown,
            ):
                storage.write_state({"settings": {}})

            chown.assert_called_once_with(
                state_path.with_suffix(".tmp"), previous.st_uid, previous.st_gid
            )
            self.assertEqual(state_path.read_bytes(), b"encrypted")


if __name__ == "__main__":
    unittest.main()
