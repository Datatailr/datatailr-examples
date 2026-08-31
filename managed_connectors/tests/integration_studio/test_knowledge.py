from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from integration_studio_app.knowledge import KnowledgeDocument, KnowledgeIndex


class KnowledgeIndexTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.index = KnowledgeIndex(root / "knowledge.sqlite3")
        self.settings = {}

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def doc(external_id: str, content: str, *, source="slack", scope="workspace", users=None, groups=None) -> KnowledgeDocument:
        return KnowledgeDocument(
            source=source,
            scope=scope,
            external_id=external_id,
            title=f"Slack #{external_id}",
            content=content,
            ref=f"slack://channel/{external_id}",
            source_updated_at="2026-08-02T10:00:00+00:00",
            acl_users=list(users or []),
            acl_groups=list(groups or []),
        )

    def ingest(self, docs: list[KnowledgeDocument], replace: bool = False) -> tuple[int, int, str]:
        run = self.index.start_run("slack", "workspace", "full")
        return self.index.upsert_documents(
            docs, self.settings, run_id=run, replace_scope=replace
        )

    def test_shared_sources_ignore_document_acl(self) -> None:
        self.ingest(
            [
                self.doc("public", "Project Apollo launch decision", groups=["engineering"]),
                self.doc("private", "Project Apollo confidential acquisition", users=["alice"]),
            ]
        )

        bob = self.index.search(
            "Project Apollo",
            user="bob",
            groups=["engineering"],
            sources=["slack"],
            settings=self.settings,
        )
        alice = self.index.search(
            "Project Apollo",
            user="alice",
            groups=[],
            sources=["slack"],
            settings=self.settings,
        )

        expected = {self.doc("public", "x").id, self.doc("private", "x").id}
        self.assertEqual({hit.id for hit in bob.hits}, expected)
        self.assertEqual({hit.id for hit in alice.hits}, expected)

    def test_personal_source_acl_is_applied_before_ranking(self) -> None:
        documents = [
            self.doc("alice", "Project Apollo inbox", source="gmail", users=["alice"]),
            self.doc("bob", "Project Apollo inbox", source="gmail", users=["bob"]),
        ]
        run = self.index.start_run("gmail", "mailboxes", "full")
        self.index.upsert_documents(documents, self.settings, run_id=run)

        result = self.index.search(
            "Project Apollo", user="alice", groups=[], sources=["gmail"], settings=self.settings
        )

        self.assertEqual([hit.id for hit in result.hits], [documents[0].id])

    def test_live_only_personal_sources_are_purged_from_every_index_table(self) -> None:
        documents = [
            self.doc("mail", "Sensitive mailbox record", source="gmail", scope="alice", users=["alice"]),
            self.doc("zoom", "Sensitive meeting transcript", source="zoom", scope="alice", users=["alice"]),
            self.doc("shared", "Shared Slack record", source="slack", scope="workspace"),
        ]
        for document in documents:
            run = self.index.start_run(document.source, document.scope, "full")
            self.index.upsert_documents([document], self.settings, run_id=run)
            self.index.set_cursor(document.source, document.scope, "last_success", "now")
            self.index.finish_run(run, "ok", fetched=1, changed=1)

        removed = self.index.purge_sources({"gmail", "outlook", "zoom"})

        self.assertEqual(removed, 2)
        with self.index._db() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM documents WHERE source='gmail'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM documents WHERE source='zoom'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM documents_fts WHERE content LIKE '%Sensitive%'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM sync_cursors WHERE source='gmail'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM ingestion_runs WHERE source='gmail'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM documents WHERE source='slack'").fetchone()[0], 1)

    def test_personal_ingestion_metadata_is_hidden_from_other_users(self) -> None:
        documents = [
            self.doc("alice-mail", "Alice inbox", source="gmail", scope="alice", users=["alice"]),
            self.doc("bob-mail", "Bob inbox", source="gmail", scope="bob", users=["bob"]),
            self.doc("shared", "Shared update", source="slack", scope="workspace"),
        ]
        for document in documents:
            run = self.index.start_run(document.source, document.scope, "full")
            self.index.upsert_documents([document], self.settings, run_id=run)
            self.index.finish_run(run, "ok", fetched=1, changed=1)

        stats = self.index.stats("alice", [])

        self.assertEqual(
            {(row["source"], row["scope"]) for row in stats["sources"]},
            {("gmail", "alice"), ("slack", "workspace")},
        )
        self.assertNotIn("bob", {row["scope"] for row in stats["runs"]})

    def test_reingest_is_idempotent_and_full_scan_reconciles_deletions(self) -> None:
        first = self.doc("one", "first version", groups=["dtusers"])
        second = self.doc("two", "second document", groups=["dtusers"])
        self.assertEqual(self.ingest([first, second], replace=True)[:2], (2, 0))
        self.assertEqual(self.ingest([first, second], replace=True)[:2], (0, 0))
        self.assertEqual(self.ingest([first], replace=True)[:2], (0, 1))

        result = self.index.search(
            "second document",
            user="alice",
            groups=["dtusers"],
            sources=["slack"],
            settings=self.settings,
        )
        self.assertFalse(any("second document" in hit.content for hit in result.hits))

    def test_recent_query_adds_latest_authorized_documents(self) -> None:
        self.ingest([self.doc("latest", "A standup update with no query keywords", groups=["dtusers"])])
        result = self.index.search(
            "Summarize the latest discussions",
            user="alice",
            groups=["dtusers"],
            sources=["slack"],
            settings=self.settings,
        )
        self.assertEqual(len(result.hits), 1)
        self.assertIn("standup update", result.hits[0].content)


if __name__ == "__main__":
    unittest.main()
