"""The explorer API against a live Postgres (set NEON_TEST_DSN to run these)."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import httpx
import pytest

from neon_postgres.explorer.app import app

DSN = os.environ.get("NEON_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="NEON_TEST_DSN is not set")


def _connection() -> dict:
    url = urlparse(DSN)
    return {
        "host": url.hostname,
        "port": url.port or 5432,
        "dbname": (url.path or "/postgres").lstrip("/"),
        "user": url.username,
        "password": url.password or "",
    }


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://explorer") as client:
        yield client


async def _query(client, sql: str, **extra):
    response = await client.post("/api/query", json={"connection": _connection(), "sql": sql, **extra})
    return response


@pytest.mark.anyio
async def test_connect_and_query(client):
    response = await client.post("/api/connect", json={"connection": _connection()})
    assert response.status_code == 200, response.text
    assert response.json()["version"].startswith("PostgreSQL")

    response = await _query(client, "select 1 as one, 'a' as letter, null as nothing, 2.5 as num")
    body = response.json()
    assert response.status_code == 200, response.text
    assert body["columns"] == ["one", "letter", "nothing", "num"]
    assert body["rows"] == [[1, "a", None, 2.5]]
    assert body["duration_ms"] >= 0


@pytest.mark.anyio
async def test_metadata_roundtrip(client):
    await _query(client, "drop schema if exists explorer_test cascade")
    await _query(client, "create schema explorer_test")
    create = await client.post(
        "/api/action",
        json={
            "action": "create_table",
            "params": {
                "schema": "explorer_test",
                "name": "items",
                "columns": [
                    {"name": "id", "type": "serial", "primary_key": True},
                    {"name": "label", "type": "text", "nullable": False},
                ],
            },
        },
    )
    assert create.status_code == 200
    assert (await _query(client, create.json()["sql"])).status_code == 200
    assert (await _query(client, "insert into explorer_test.items(label) select 'x' || g from generate_series(1, 7) g")).json()["rowcount"] == 7

    schemas = (await client.post("/api/schemas", json={"connection": _connection()})).json()["schemas"]
    assert any(s["name"] == "explorer_test" and s["relations"] == 1 for s in schemas)

    relations = (await client.post("/api/relations", json={"connection": _connection(), "schema": "explorer_test"})).json()
    assert [(r["name"], r["kind"]) for r in relations["relations"]] == [("items", "table")]

    detail = (
        await client.post("/api/relation", json={"connection": _connection(), "schema": "explorer_test", "name": "items", "sample": 3})
    ).json()
    assert [c["name"] for c in detail["columns"]] == ["id", "label"]
    assert detail["columns"][0]["primary_key"] is True
    assert detail["columns"][1]["nullable"] is False
    assert any(i["primary_key"] for i in detail["indexes"])
    assert detail["sample"]["columns"] == ["id", "label"]
    assert len(detail["sample"]["rows"]) == 3

    truncated = await _query(client, "select * from explorer_test.items", limit=2)
    assert truncated.json()["truncated"] is True
    assert len(truncated.json()["rows"]) == 2

    await _query(client, "drop schema explorer_test cascade")


@pytest.mark.anyio
async def test_data_grid_and_ddl(client):
    await _query(client, "drop schema if exists explorer_grid cascade")
    await _query(
        client,
        """
        create schema explorer_grid;
        create table explorer_grid.items(id serial primary key, label text not null default 'none', qty numeric, tags jsonb);
        comment on table explorer_grid.items is 'grid test';
        create index items_qty_idx on explorer_grid.items(qty);
        create view explorer_grid.labels as select id, label from explorer_grid.items;
        """,
    )
    target = {"connection": _connection(), "schema": "explorer_grid", "name": "items"}

    inserted = await client.post("/api/rows/insert", json={**target, "values": {"label": "a", "qty": "1.5", "tags": '{"k": 1}'}})
    assert inserted.status_code == 200, inserted.text
    assert inserted.json()["row"] == [1, "a", 1.5, {"k": 1}]
    defaults = await client.post("/api/rows/insert", json={**target, "values": {}})
    assert defaults.json()["row"] == [2, "none", None, None]
    await client.post("/api/rows/insert", json={**target, "values": {"label": "c", "qty": "3"}})

    page = (
        await client.post(
            "/api/rows",
            json={**target, "sort": [{"column": "id", "desc": True}], "filters": [{"column": "qty", "op": "is not null"}], "limit": 1},
        )
    ).json()
    assert page["columns"] == ["id", "label", "qty", "tags"]
    assert page["total"] == 2 and page["editable"] is True
    assert page["rows"] == [[3, "c", 3, None]]
    assert len(page["ids"]) == 1

    row_id = page["ids"][0]
    updated = await client.post("/api/rows/update", json={**target, "id": row_id, "values": {"label": "changed", "qty": None}})
    assert updated.status_code == 200, updated.text
    assert updated.json()["row"] == [3, "changed", None, None]
    stale = await client.post("/api/rows/update", json={**target, "id": row_id, "values": {"label": "again"}})
    assert stale.status_code == 404
    bad_value = await client.post("/api/rows/update", json={**target, "id": updated.json()["id"], "values": {"qty": "not a number"}})
    assert bad_value.status_code == 400
    bad_filter = await client.post("/api/rows", json={**target, "filters": [{"column": "qty", "op": "; drop"}]})
    assert bad_filter.status_code == 400

    view = (await client.post("/api/rows", json={**target, "name": "labels"})).json()
    assert view["editable"] is False and view["ids"] == [] and view["total"] == 3

    deleted = await client.post("/api/rows/delete", json={**target, "ids": [updated.json()["id"], defaults.json()["id"]]})
    assert deleted.json() == {"deleted": 2}
    assert (await client.post("/api/rows", json=target)).json()["total"] == 1

    ddl = (await client.post("/api/ddl", json=target)).json()["sql"]
    assert ddl.startswith('create table "explorer_grid"."items" (\n    "id" integer default nextval(')
    assert '"label" text default \'none\'::text not null' in ddl
    assert 'constraint "items_pkey" PRIMARY KEY (id)' in ddl
    assert "CREATE INDEX items_qty_idx ON explorer_grid.items USING btree (qty);" in ddl
    assert "comment on table \"explorer_grid\".\"items\" is 'grid test';" in ddl
    view_ddl = (await client.post("/api/ddl", json={**target, "name": "labels"})).json()["sql"]
    assert view_ddl.startswith('create or replace view "explorer_grid"."labels" as')
    assert (await client.post("/api/ddl", json={**target, "name": "nope"})).status_code == 404

    await _query(client, "drop schema explorer_grid cascade")


@pytest.mark.anyio
async def test_errors_are_reported(client):
    bad_sql = await _query(client, "select * from no_such_table_anywhere")
    assert bad_sql.status_code == 400
    assert "no_such_table_anywhere" in bad_sql.json()["detail"]

    bad_host = await client.post("/api/connect", json={"connection": {**_connection(), "host": "127.0.0.1", "port": 1}})
    assert bad_host.status_code == 502

    missing = await client.post("/api/relation", json={"connection": _connection(), "schema": "public", "name": "nope"})
    assert missing.status_code == 404

    no_host = await client.post("/api/connect", json={"connection": {"host": ""}})
    assert no_host.status_code == 400
