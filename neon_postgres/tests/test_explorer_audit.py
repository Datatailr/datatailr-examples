"""The audit trail: the platform user in application_name, and the service log parsed into rows."""

from __future__ import annotations

import json

import httpx
import psycopg
import pytest

from neon_postgres.explorer import app as explorer_app
from neon_postgres.explorer import audit, db
from neon_postgres.explorer.app import app

SAMPLE_LOG = """\
2026-11-09T11:39:34.067697Z  INFO start_compute: compute start finished
PG:2026-11-09 11:42:46.710 GMT [665] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 LOG:  connection authorized: user=cloud_admin database=postgres application_name=neon-postgres-explorer:dgeidman
PG:2026-11-09 11:42:46.715 GMT [665] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 LOG:  statement: select count(*)
PG:\tfrom public.health_check
PG:2026-11-09 11:42:50.781 GMT [783] user=cloud_admin db=postgres app=psql host=172.18.0.9 LOG:  execute <unnamed>: insert into t\u200b\tvalues ($1)
PG:2026-11-09 11:42:50.782 GMT [783] user=cloud_admin db=postgres app=psql host=172.18.0.9 DETAIL:  Parameters: $1 = '1'
PG:2026-11-09 11:42:56.776 GMT [784] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 LOG:  statement: select * from no_such_table
PG:2026-11-09 11:42:56.777 GMT [784] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 ERROR:  relation "no_such_table" does not exist at character 15
PG:2026-11-09 11:42:56.777 GMT [784] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 STATEMENT:  select * from no_such_table
PG:2026-11-09 11:44:33.147 GMT [359] user= db= app= host= LOG:  checkpoint starting: time
"""


def test_parse_log_keeps_statements_and_errors_with_their_actor():
    rows = audit.parse_log(SAMPLE_LOG)
    assert [(r["kind"], r["pid"]) for r in rows] == [("statement", 665), ("execute", 783), ("statement", 784), ("error", 784)]

    first = rows[0]
    assert first["time"] == "2026-11-09 11:42:46.715 GMT"
    assert first["user"] == "cloud_admin"
    assert first["database"] == "postgres"
    assert first["host"] == "172.18.0.4"
    assert first["application"] == "neon-postgres-explorer:dgeidman"
    assert first["platform_user"] == "dgeidman"
    assert first["sql"] == "select count(*)\nfrom public.health_check"

    prepared = rows[1]
    assert prepared["platform_user"] is None
    assert prepared["application"] == "psql"
    assert prepared["sql"] == "insert into t\nvalues ($1)"  # compute_ctl joins continuation lines with U+200B

    assert rows[3]["sql"].startswith('relation "no_such_table" does not exist')


def test_parse_log_ignores_lines_without_the_audit_prefix():
    plain = "PG:2026-11-09 11:42:46.715 GMT [665] LOG:  statement: select 1\n"
    assert audit.parse_log(plain) == []
    assert audit.parse_log("") == []
    # A continuation line with nothing to continue is dropped, not attached to the next entry.
    assert audit.parse_log("PG:\tselect 2\n" + SAMPLE_LOG.splitlines()[2] + "\n")[0]["sql"] == "select count(*)"


def test_recent_statements_are_newest_first():
    rows = audit.recent_statements("neon-postgres", 500, reader=lambda service, lines: SAMPLE_LOG)
    assert [r["pid"] for r in rows] == [784, 784, 783, 665]


def test_platform_user_comes_from_the_platform_header():
    assert audit.platform_user({"x-datatailr-user": json.dumps({"name": "dgeidman", "id": 7})}) == "dgeidman"
    assert audit.platform_user({}) is None
    assert audit.platform_user({"x-datatailr-user": "not json"}) is None
    assert audit.platform_user({"x-datatailr-user": json.dumps(["dgeidman"])}) is None
    assert audit.platform_user({"x-datatailr-user": json.dumps({"name": "  "})}) is None


def test_application_name_carries_the_platform_user():
    assert db.application_name(None) == "neon-postgres-explorer"
    assert db.application_name("dgeidman") == "neon-postgres-explorer:dgeidman"
    assert db.application_name("ann smithé") == "neon-postgres-explorer:annsmith"
    assert len(db.application_name("x" * 100)) == 63

    info = db.ConnectionInfo.from_dict({"host": "db", "application_name": "forged"}, platform_user="dgeidman")
    assert info.application_name == "neon-postgres-explorer:dgeidman"
    assert db.ConnectionInfo.from_dict({"host": "db"}).application_name == "neon-postgres-explorer"


class FakeConnection:
    def __init__(self, privileged: bool):
        self.privileged = privileged
        self.statements: list[str] = []
        self.rolled_back = False

    def execute(self, sql):
        self.statements.append(sql)
        if not self.privileged:
            raise psycopg.errors.InsufficientPrivilege("permission denied to set parameter")

    def rollback(self):
        self.rolled_back = True


@pytest.mark.parametrize("privileged", [True, False])
def test_explorer_sessions_log_every_statement_when_they_may(monkeypatch, privileged):
    made = []

    def fake_connect(**kwargs):
        made.append(kwargs)
        return FakeConnection(privileged)

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    conn = db.ConnectionInfo.from_dict({"host": "db"}, platform_user="dgeidman").connect()
    assert made[0]["application_name"] == "neon-postgres-explorer:dgeidman"
    assert conn.statements == ["set log_statement = 'all'"]
    assert conn.rolled_back is (not privileged)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_query_endpoint_tags_the_connection_with_the_platform_user(monkeypatch):
    seen = []

    def fake_run_query(info, sql, limit):
        seen.append(info)
        return {"columns": [], "rows": [], "truncated": False, "rowcount": 0, "status": "SELECT 0", "duration_ms": 0}

    monkeypatch.setattr(db, "run_query", fake_run_query)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://explorer") as client:
        body = {"connection": {"host": "neon-postgres"}, "sql": "select 1"}
        header = {"x-datatailr-user": json.dumps({"name": "dgeidman"})}
        assert (await client.post("/api/query", json=body, headers=header)).status_code == 200
        assert (await client.post("/api/query", json=body)).status_code == 200
    assert [info.application_name for info in seen] == ["neon-postgres-explorer:dgeidman", "neon-postgres-explorer"]


@pytest.mark.anyio
async def test_audit_endpoint_reads_the_service_log(monkeypatch):
    calls = []

    def fake_recent(service, lines):
        calls.append((service, lines))
        if service == "missing":
            raise RuntimeError("no such job")
        return audit.parse_log(SAMPLE_LOG)[::-1]

    monkeypatch.setattr(explorer_app.audit, "recent_statements", fake_recent)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://explorer") as client:
        response = await client.post("/api/audit", json={"service": "neon-postgres", "lines": 10})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["service"] == "neon-postgres"
        assert body["lines"] == 100  # the floor
        assert [r["platform_user"] for r in body["statements"]] == ["dgeidman", "dgeidman", None, "dgeidman"]

        assert (await client.post("/api/audit", json={"service": ""})).status_code == 400
        missing = await client.post("/api/audit", json={"service": "missing"})
        assert missing.status_code == 502
        assert "no such job" in missing.json()["detail"]
    assert calls[0] == ("neon-postgres", 100)
