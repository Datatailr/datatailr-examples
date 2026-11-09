"""The parameterised SQL the data grid builds. No database needed."""

from __future__ import annotations

import pytest

from neon_postgres.explorer import rows


def render(stmt) -> str:
    return stmt.as_string(None)


def test_page_quotes_identifiers_and_binds_values():
    stmt, params = rows.select_page(
        "s", 'odd"name',
        filters=[{"column": "a", "op": ">=", "value": "5"}, {"column": "b", "op": "is null"}],
        sort=[{"column": "a", "desc": True}, {"column": "b"}],
        limit=50, offset=100, with_ctid=True,
    )
    assert render(stmt) == (
        'select ctid::text as __ctid, * from "s"."odd""name" where "a" >= %s and "b" is null '
        'order by "a" desc, "b" asc limit %s offset %s'
    )
    assert params == ["5", 50, 100]


def test_page_without_filters_or_sort():
    stmt, params = rows.select_page("s", "t", [], [], 10, 0, with_ctid=False)
    assert render(stmt) == 'select * from "s"."t" limit %s offset %s'
    assert params == [10, 0]
    stmt, params = rows.count_rows("s", "t", [{"column": "c", "op": "ilike", "value": "%x%"}])
    assert render(stmt) == 'select count(*) from "s"."t" where "c" ilike %s'
    assert params == ["%x%"]


def test_filter_rejects_unknown_operators_and_empty_columns():
    with pytest.raises(ValueError):
        rows.where_clause([{"column": "a", "op": "; drop table t", "value": 1}])
    with pytest.raises(ValueError):
        rows.where_clause([{"column": "", "op": "=", "value": 1}])
    with pytest.raises(ValueError):
        rows.order_clause([{"column": " "}])


def test_insert_update_delete():
    stmt, params = rows.insert_row("s", "t", {"a": "1", "b": None})
    assert render(stmt) == 'insert into "s"."t" ("a", "b") values (%s, %s) returning ctid::text as __ctid, *'
    assert params == ["1", None]
    stmt, params = rows.insert_row("s", "t", {})
    assert render(stmt) == 'insert into "s"."t" default values returning ctid::text as __ctid, *'

    stmt, params = rows.update_row("s", "t", "(0,7)", {"a": "x", "select": None})
    assert render(stmt) == 'update "s"."t" set "a" = %s, "select" = %s where ctid = %s returning ctid::text as __ctid, *'
    assert params == ["x", None, "(0,7)"]
    with pytest.raises(ValueError):
        rows.update_row("s", "t", "(0,7)", {})

    stmt, params = rows.delete_rows("s", "t", ["(0,1)", "(0,2)"])
    assert render(stmt) == 'delete from "s"."t" where ctid = any(%s::tid[])'
    assert params == [["(0,1)", "(0,2)"]]
    with pytest.raises(ValueError):
        rows.delete_rows("s", "t", [])


def test_row_ids_must_look_like_tids():
    for bad in ("", "1", "(0,1) or true", "(0,1,2)", "x"):
        with pytest.raises(ValueError):
            rows.update_row("s", "t", bad, {"a": 1})
