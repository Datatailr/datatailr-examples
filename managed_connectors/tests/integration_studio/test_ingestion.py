from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from integration_studio_app.ingestion import (
    HUBSPOT_OBJECTS,
    HubSpotConnector,
    IngestionError,
    SlackConnector,
    SyncManager,
)
from integration_studio_app.knowledge import KnowledgeIndex


class ConnectorIngestionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.index = KnowledgeIndex(root / "knowledge.sqlite3")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_slack_reads_every_joined_channel_and_thread_replies(self) -> None:
        connector = SlackConnector("secret", self.index)
        connector._paginate = Mock(
            side_effect=[
                [{"id": "C1", "name": "pull-requests"}, {"id": "C2", "name": "sonic-bug-report"}],
                [{"ts": "10.0", "user": "U1", "text": "PR opened", "reply_count": 1}],
                [
                    {"ts": "10.0", "user": "U1", "text": "PR opened"},
                    {"ts": "11.0", "user": "U2", "text": "Approved"},
                ],
                [{"ts": "20.0", "user": "U3", "text": "Digest job is stuck"}],
            ]
        )

        result = connector.load(full=True)

        self.assertEqual(len(result.documents), 2)
        self.assertTrue(result.replace_scope)
        self.assertEqual(result.cursors, {"channel:C1": "10.0", "channel:C2": "20.0"})
        self.assertIn("Approved", result.documents[0].content)
        self.assertEqual(result.documents[1].metadata["channel"], "sonic-bug-report")
        self.assertEqual(result.documents[1].acl_groups, [])
        self.assertEqual(result.documents[1].acl_users, [])

    def test_hubspot_uses_per_object_incremental_checkpoints(self) -> None:
        connector = HubSpotConnector({"access_token": "secret"}, self.index)
        for object_type in HUBSPOT_OBJECTS:
            self.index.set_cursor("hubspot", "portal", f"modified:{object_type}", "1000")
        connector._search = Mock(
            side_effect=lambda object_type, _after: [
                {
                    "id": f"{object_type}-1",
                    "updatedAt": "2026-08-02T10:00:00Z",
                    "properties": {"name": "Acme", "dealname": "Renewal", "subject": "Case"},
                }
            ]
        )
        connector._activity_associations = Mock(
            side_effect=lambda object_type, ids: (
                {record_id: {"contacts": [], "companies": [], "deals": [], "tickets": []} for record_id in ids},
                [],
            )
        )

        result = connector.load(full=False)

        self.assertEqual(connector._search.call_count, len(HUBSPOT_OBJECTS))
        self.assertEqual(len(result.documents), len(HUBSPOT_OBJECTS))
        self.assertFalse(result.replace_scope)
        self.assertTrue(all(not doc.acl_groups and not doc.acl_users for doc in result.documents))
        self.assertTrue(all(int(value) > 1000 for value in result.cursors.values()))

    def test_hubspot_empty_success_is_not_an_error_when_another_scope_is_denied(self) -> None:
        connector = HubSpotConnector({"access_token": "secret"}, self.index)
        for object_type in HUBSPOT_OBJECTS:
            self.index.set_cursor("hubspot", "portal", f"modified:{object_type}", "1000")

        def search(object_type: str, _after: str) -> list[dict]:
            if object_type in {"companies", "tickets"}:
                raise IngestionError("HTTP 403: missing scope")
            return []

        connector._search = Mock(side_effect=search)

        result = connector.load(full=False)

        self.assertEqual(result.documents, [])
        self.assertIn("companies: HTTP 403", result.detail)
        self.assertIn("tickets: HTTP 403", result.detail)

    def test_hubspot_ingests_activity_details_associations_and_next_activity_fields(self) -> None:
        connector = HubSpotConnector({"access_token": "secret"}, self.index)

        def records(object_type: str) -> list[dict]:
            if object_type == "contacts":
                return [{
                    "id": "contact-1",
                    "updatedAt": "2026-08-08T08:00:00Z",
                    "properties": {
                        "firstname": "Ada",
                        "notes_next_activity_date": "2026-08-12T10:00:00Z",
                        "hs_notes_next_activity_type": "MEETING",
                    },
                }]
            if object_type == "meetings":
                return [{
                    "id": "meeting-1",
                    "updatedAt": "2026-08-08T09:00:00Z",
                    "properties": {
                        "hs_timestamp": "2026-08-12T10:00:00Z",
                        "hs_meeting_start_time": "2026-08-12T10:00:00Z",
                        "hs_meeting_end_time": "2026-08-12T10:30:00Z",
                        "hs_meeting_title": "Discovery call",
                        "hs_meeting_body": "<p>Discuss the rollout</p>",
                        "hs_meeting_outcome": "SCHEDULED",
                        "hubspot_owner_id": "owner-1",
                    },
                }]
            return []

        connector._list = Mock(side_effect=records)
        connector._activity_associations = Mock(return_value=(
            {
                "meeting-1": {
                    "contacts": ["contact-1"], "companies": ["company-1"],
                    "deals": ["deal-1"], "tickets": [],
                }
            },
            [],
        ))

        result = connector.load(full=True)

        contact = next(doc for doc in result.documents if doc.external_id == "contacts:contact-1")
        meeting = next(doc for doc in result.documents if doc.external_id == "meetings:meeting-1")
        self.assertEqual(contact.metadata["properties"]["notes_next_activity_date"], "2026-08-12T10:00:00Z")
        self.assertEqual(meeting.metadata["activity_type"], "meetings")
        self.assertEqual(meeting.metadata["associations"]["deals"], ["deal-1"])
        self.assertEqual(meeting.metadata["owner_id"], "owner-1")
        self.assertIn("Discuss the rollout", meeting.content)
        self.assertNotIn("<p>", meeting.content)
        self.assertTrue(result.replace_scope)

    def test_live_personal_connectors_cannot_enter_the_persistent_sync_manager(self) -> None:
        manager = SyncManager(self.index)

        with self.assertRaisesRegex(IngestionError, "live-only.*never be ingested"):
            manager._connector("gmail", "alice", {})
        with self.assertRaisesRegex(IngestionError, "live-only.*never be ingested"):
            manager._connector("outlook", "alice", {})
        with self.assertRaisesRegex(IngestionError, "live-only.*never be ingested"):
            manager._connector("zoom", "alice", {})


if __name__ == "__main__":
    unittest.main()
