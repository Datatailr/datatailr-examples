"""The SQL the explorer generates for metadata changes."""

from __future__ import annotations

import httpx
import pytest

from neon_postgres.explorer import actions
from neon_postgres.explorer.app import app


def test_identifiers_are_quoted():
    assert actions.create_schema("Sales Data") == 'create schema "Sales Data";'
    assert actions.drop_relation("sales", 'odd"name', "table") == 'drop table "sales"."odd""name";'
    assert actions.rename_column("s", "t", "a", "select") == 'alter table "s"."t" rename column "a" to "select";'


def test_create_table_renders_columns():
    text = actions.create_table(
        "sales",
        "orders",
        [
            {"name": "id", "type": "serial", "primary_key": True},
            {"name": "customer", "type": "text", "nullable": False},
            {"name": "amount", "type": "numeric", "default": "0"},
        ],
    )
    assert text == (
        'create table "sales"."orders" (\n'
        '    "id" serial primary key,\n'
        '    "customer" text not null,\n'
        '    "amount" numeric default 0\n'
        ");"
    )


def test_drop_kinds_and_cascade():
    assert actions.drop_relation("s", "v", "view", cascade=True) == 'drop view "s"."v" cascade;'
    assert actions.drop_relation("s", "m", "materialized view") == 'drop materialized view "s"."m";'
    with pytest.raises(ValueError):
        actions.drop_relation("s", "x", "sequence")


def test_index_and_comment():
    assert actions.create_index("s", "t", ["a", "b"]) == 'create index "t_a_b_idx" on "s"."t" ("a", "b");'
    assert actions.create_index("s", "t", ["a"], "my_idx", unique=True) == 'create unique index "my_idx" on "s"."t" ("a");'
    assert actions.set_comment("s", "t", "it's a table") == '''comment on table "s"."t" is 'it''s a table';'''
    assert actions.set_comment("s", "t", "", column="c") == 'comment on column "s"."t"."c" is null;'


def test_build_validates():
    assert actions.build("truncate_table", {"schema": "s", "name": "t"}) == 'truncate table "s"."t";'
    with pytest.raises(ValueError):
        actions.build("nope", {})
    with pytest.raises(TypeError):
        actions.build("drop_schema", {"bogus": 1})
    with pytest.raises(ValueError):
        actions.build("create_schema", {"name": "  "})


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_page_carries_the_platform_prefix_as_base_href():
    transport = httpx.ASGITransport(app=app, root_path="/job/dev/neon-postgres-explorer")
    async with httpx.AsyncClient(transport=transport, base_url="http://explorer") as client:
        page = (await client.get("/")).text
    assert '<base href="/job/dev/neon-postgres-explorer/">' in page
    assert 'href="static/style.css"' in page

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://explorer") as client:
        assert '<base href="/">' in (await client.get("/")).text
