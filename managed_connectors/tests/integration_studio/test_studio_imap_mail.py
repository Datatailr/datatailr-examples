from __future__ import annotations

from integration_studio_app import imap_mail


def _message(subject: str, body: str) -> bytes:
    return (
        f"From: sender@example.test\r\n"
        f"To: reader@example.test\r\n"
        f"Subject: {subject}\r\n"
        "Date: Tue, 5 Aug 2026 10:00:00 +0000\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n\r\n"
        f"{body}"
    ).encode()


class FakeImap:
    def __init__(self):
        self.login_args = None
        self.readonly = None
        self.fetch_query = None
        self.logged_out = False

    def login(self, username, password):
        self.login_args = (username, password)

    def select(self, folder, readonly=False):
        self.readonly = readonly
        return "OK", [b"2"]

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"1 2"]
        self.fetch_query = args[-1]
        return "OK", [
            (b"1 (UID 1 BODY[] {1}", _message("Older", "Routine note")),
            b")",
            (b"2 (UID 2 BODY[] {1}", _message("Incident", "Cluster alarm")),
            b")",
        ]

    def logout(self):
        self.logged_out = True


def test_gmail_imap_fetch_is_read_only_bounded_and_not_cached(monkeypatch) -> None:
    fake = FakeImap()
    monkeypatch.setattr(imap_mail.imaplib, "IMAP4_SSL", lambda *args, **kwargs: fake)

    rows = imap_mail.fetch_gmail_messages(
        {"username": "reader@example.test", "app_password": "abcd efgh ijkl mnop"},
        query="cluster",
        limit=1,
    )

    assert fake.login_args == ("reader@example.test", "abcdefghijklmnop")
    assert fake.readonly is True
    assert fake.fetch_query == "(UID BODY.PEEK[])"
    assert fake.logged_out is True
    assert rows == [
        {
            "id": "2",
            "title": "Incident",
            "text": "From: sender@example.test\nTo: reader@example.test\n\nCluster alarm",
            "updated_at": "2026-08-05T10:00:00+00:00",
            "ref": "gmail://imap/INBOX/2",
        }
    ]
