from __future__ import annotations

import contextlib
import html
import imaplib
import re
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from typing import Any


IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
TIMEOUT = 30
MAX_BODY_CHARS = 16_000
_UID = re.compile(rb"\bUID\s+(\d+)\b")
_HTML_TAG = re.compile(r"<[^>]+>")


class GmailImapError(RuntimeError):
    pass


def _credentials(config: dict[str, Any]) -> tuple[str, str]:
    username = str(config.get("username") or "").strip()
    app_password = "".join(str(config.get("app_password") or "").split())
    if not username or not app_password:
        raise GmailImapError("Gmail address and app password are required")
    if len(app_password) != 16:
        raise GmailImapError("Google app password must contain exactly 16 characters")
    return username, app_password


def _header(value: str | None) -> str:
    try:
        return str(make_header(decode_header(value or "")))
    except (LookupError, UnicodeError):
        return str(value or "")


def _body(message: Any) -> str:
    try:
        part = message.get_body(preferencelist=("plain", "html")) if message.is_multipart() else message
        if part is None or str(part.get_content_disposition() or "") == "attachment":
            return ""
        value = part.get_content()
        text = value if isinstance(value, str) else value.decode(errors="replace")
        if part.get_content_type() == "text/html":
            text = html.unescape(_HTML_TAG.sub(" ", text))
        return " ".join(text.split())[:MAX_BODY_CHARS]
    except Exception:  # malformed MIME must not fail the entire live request
        return ""


def _timestamp(value: str) -> str:
    try:
        parsed = parsedate_to_datetime(value)
        return parsed.isoformat() if parsed else value
    except (TypeError, ValueError, OverflowError):
        return value


def _connect(config: dict[str, Any]) -> imaplib.IMAP4_SSL:
    username, app_password = _credentials(config)
    try:
        client = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=TIMEOUT)
        client.login(username, app_password)
        return client
    except Exception as exc:  # do not echo provider responses or credentials
        raise GmailImapError(
            "Gmail IMAP login failed. Check the Gmail address, 2-Step Verification, and app password."
        ) from exc


def test_gmail_configuration(config: dict[str, Any]) -> None:
    client = _connect(config)
    try:
        status, _ = client.select("INBOX", readonly=True)
        if status != "OK":
            raise GmailImapError("Gmail connected but the inbox could not be opened read-only")
    finally:
        with contextlib.suppress(Exception):
            client.logout()


def fetch_gmail_messages(
    config: dict[str, Any], *, query: str = "", limit: int = 20
) -> list[dict[str, str]]:
    """Fetch bounded Gmail rows into memory without changing mailbox state."""
    limit = min(max(int(limit), 1), 100)
    query_folded = str(query or "").casefold()
    client = _connect(config)
    try:
        status, _ = client.select("INBOX", readonly=True)
        if status != "OK":
            raise GmailImapError("Gmail inbox could not be opened read-only")
        status, data = client.uid("search", None, "ALL")
        if status != "OK" or not data or not data[0]:
            return []
        # Pull a bounded candidate window for local substring filtering. No
        # cursor, cache, or result row is written anywhere.
        candidates = min(max(limit * 3, 20), 100)
        uids = data[0].split()[-candidates:]
        status, fetched = client.uid("fetch", b",".join(uids).decode(), "(UID BODY.PEEK[])")
        if status != "OK" or not fetched:
            raise GmailImapError("Gmail did not return the requested messages")

        rows: list[dict[str, str]] = []
        for item in reversed(fetched):
            if not isinstance(item, tuple) or len(item) < 2 or not isinstance(item[1], bytes):
                continue
            metadata = item[0] if isinstance(item[0], bytes) else b""
            uid_match = _UID.search(metadata)
            uid = uid_match.group(1).decode() if uid_match else ""
            message = BytesParser(policy=policy.default).parsebytes(item[1])
            sender = _header(message.get("From"))
            recipients = _header(message.get("To"))
            subject = _header(message.get("Subject")) or "(no subject)"
            date = _header(message.get("Date"))
            body = _body(message)
            searchable = f"{sender}\n{recipients}\n{subject}\n{body}".casefold()
            if query_folded and query_folded not in searchable:
                continue
            rows.append(
                {
                    "id": uid,
                    "title": subject,
                    "text": f"From: {sender}\nTo: {recipients}\n\n{body}"[:MAX_BODY_CHARS],
                    "updated_at": _timestamp(date),
                    "ref": f"gmail://imap/INBOX/{uid}",
                }
            )
            if len(rows) >= limit:
                break
        return rows
    except GmailImapError:
        raise
    except Exception as exc:
        raise GmailImapError("Gmail IMAP retrieval failed") from exc
    finally:
        with contextlib.suppress(Exception):
            client.logout()
