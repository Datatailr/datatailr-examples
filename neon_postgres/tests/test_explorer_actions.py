"""The SQL the explorer generates for metadata changes."""

from __future__ import annotations

import pytest

from neon_postgres.explorer import actions


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
