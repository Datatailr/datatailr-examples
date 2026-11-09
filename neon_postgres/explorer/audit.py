"""The query audit trail: who ran which statement, read from the service log.

The Neon Postgres service logs every statement with the database user, the
database, the client's application name and the client host (see
``LOG_LINE_PREFIX`` in ``service/neon_stack.py``). Postgres writes those lines
to stderr, ``compute_ctl`` prefixes them with ``PG:``, and the platform keeps
them as the service's stderr log. This module reads that log and turns it into
rows the explorer shows.

The platform user behind an explorer connection travels in the application
name, as ``neon-postgres-explorer:<user>``. Other clients choose their own
application name, so for them the trail shows the database user and the host.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Mapping

log = logging.getLogger("explorer.audit")

USER_HEADER = "x-datatailr-user"
APPLICATION_PREFIX = "neon-postgres-explorer:"
DEFAULT_LINES = 2000

# One Postgres log line as the service writes it, for example:
#   PG:2026-11-09 11:42:46.710 GMT [665] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 LOG:  statement: select 1
LINE = re.compile(
    r"^PG:(?P<time>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+ \S+) \[(?P<pid>\d+)\] "
    r"user=(?P<user>\S*) db=(?P<database>\S*) app=(?P<application>.*?) host=(?P<host>\S*) "
    r"(?P<level>[A-Z]+):\s+(?P<message>.*)$"
)
# Postgres writes the second and later lines of a statement with a tab and no
# prefix. compute_ctl joins them onto the first line with a zero-width space
# before the tab; a raw Postgres log still has them as separate lines.
JOINED_NEWLINE = "\u200b\t"
CONTINUATION = re.compile(r"^(?:PG:)?\t(?P<text>.*)$")
STATEMENT = re.compile(r"^statement: (?P<sql>.*)$", re.DOTALL)
EXECUTE = re.compile(r"^execute (?P<name>\S+): (?P<sql>.*)$", re.DOTALL)


def platform_user(headers: Mapping[str, str]) -> str | None:
    """The platform user name from the request, or None off-platform.

    The platform puts a JSON object with a ``name`` field into the
    ``X-Datatailr-User`` header of every request it forwards to an app.
    """
    raw = headers.get(USER_HEADER)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    name = payload.get("name") if isinstance(payload, dict) else None
    return name if isinstance(name, str) and name.strip() else None


def _entry(match: re.Match) -> dict | None:
    """An audit row for a statement or an error line, None for any other line."""
    level, message = match["level"], match["message"]
    if level == "LOG":
        if (m := STATEMENT.match(message)) is not None:
            kind, sql = "statement", m["sql"]
        elif (m := EXECUTE.match(message)) is not None:
            kind, sql = "execute", m["sql"]
        else:
            return None
    elif level in ("ERROR", "FATAL"):
        kind, sql = level.lower(), message
    else:
        return None
    application = match["application"]
    explorer_user = application[len(APPLICATION_PREFIX):] if application.startswith(APPLICATION_PREFIX) else None
    return {
        "time": match["time"],
        "pid": int(match["pid"]),
        "user": match["user"],
        "database": match["database"],
        "application": application,
        "platform_user": explorer_user,
        "host": match["host"],
        "kind": kind,
        "sql": sql.replace(JOINED_NEWLINE, "\n"),
    }


def parse_log(text: str) -> list[dict]:
    """Audit rows from the service's stderr log, oldest first."""
    rows: list[dict] = []
    current: dict | None = None
    for line in text.splitlines():
        match = LINE.match(line)
        if match is not None:
            current = _entry(match)
            if current is not None:
                rows.append(current)
            continue
        continuation = CONTINUATION.match(line)
        if continuation is not None and current is not None:
            current["sql"] += "\n" + continuation["text"]
        else:
            current = None
    return rows


def read_service_log(service: str, lines: int = DEFAULT_LINES) -> str:
    """The last ``lines`` lines of the service's stderr log, through the platform CLI."""
    from datatailr.wrapper import dt__Log  # type: ignore

    text = dt__Log().read(service, lines=lines, stderr=True)
    if isinstance(text, dict):
        text = text.get("log") or text.get("output") or ""
    return str(text or "")


def recent_statements(service: str, lines: int = DEFAULT_LINES, reader=read_service_log) -> list[dict]:
    """The statements in the last ``lines`` log lines of ``service``, newest first."""
    rows = parse_log(reader(service, lines))
    rows.reverse()
    return rows
