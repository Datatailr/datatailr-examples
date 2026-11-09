"""The explorer's connect-as-me path without a database."""

from __future__ import annotations

import httpx
import pytest

from neon_postgres import access
from neon_postgres.explorer.app import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://explorer") as client:
        yield client


@pytest.mark.anyio
async def test_whoami_reads_the_signed_user_header(client, monkeypatch):
    monkeypatch.delenv(access.ROLE_SECRET_VAR, raising=False)
    assert (await client.get("/api/whoami")).json() == {"user": None, "role_secret": False}
    me = await client.get("/api/whoami", headers={"X-Datatailr-User": '{"name": "alice", "expiry": 1, "signature": "x"}'})
    assert me.json() == {"user": "alice", "role_secret": False}
    assert (await client.get("/api/whoami", headers={"X-Datatailr-User": "{not json"})).json()["user"] is None


@pytest.mark.anyio
async def test_as_user_needs_a_platform_user_and_the_secret(client, monkeypatch):
    connection = {"host": "db", "as_user": True}
    monkeypatch.delenv(access.ROLE_SECRET_VAR, raising=False)
    no_user = await client.post("/api/connect", json={"connection": connection})
    assert no_user.status_code == 401
    no_secret = await client.post("/api/connect", json={"connection": connection}, headers={"X-Datatailr-User": '{"name": "alice"}'})
    assert no_secret.status_code == 400
    assert access.ROLE_SECRET_VAR in no_secret.json()["detail"]
