"""The data grid: browse, filter, sort and edit the rows of one table.

Every statement is built with ``psycopg.sql`` so identifiers are quoted and
values travel as parameters, never as text inside the SQL. A row is addressed
by its ``ctid``, the physical position Postgres gives it; an update or delete
that matches no row means the row changed since the page was loaded.
"""

from __future__ import annotations

from typing import Any

from psycopg import sql
from psycopg.rows import dict_row

from neon_postgres.explorer.db import ConnectionInfo, jsonable

OPERATORS = {
    "=": "=",
    "<>": "<>",
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
    "like": "like",
    "ilike": "ilike",
    "is null": "is null",
    "is not null": "is not null",
}
UNARY = ("is null", "is not null")
MAX_PAGE = 1000
EDITABLE_KINDS = ("r", "p")  # plain and partitioned tables; views and foreign tables stay read only


def _ident(*parts: str) -> sql.Identifier:
    for part in parts:
        if not isinstance(part, str) or not part.strip():
            raise ValueError("identifiers must not be empty")
    return sql.Identifier(*parts)


def where_clause(filters: list[dict]) -> tuple[sql.Composable, list[Any]]:
    """``filters``: dicts with column, op and value. Joined with AND."""
    parts: list[sql.Composable] = []
    params: list[Any] = []
    for item in filters or []:
        op = str(item.get("op", "=")).strip().lower()
        if op not in OPERATORS:
            raise ValueError(f"unknown operator {op!r}")
        column = _ident(str(item.get("column", "")))
        if op in UNARY:
            parts.append(sql.SQL("{} {}").format(column, sql.SQL(OPERATORS[op])))
        else:
            parts.append(sql.SQL("{} {} {}").format(column, sql.SQL(OPERATORS[op]), sql.Placeholder()))
            params.append(item.get("value"))
    if not parts:
        return sql.SQL(""), []
    return sql.SQL(" where ") + sql.SQL(" and ").join(parts), params


def order_clause(sort: list[dict]) -> sql.Composable:
    """``sort``: dicts with column and an optional desc flag."""
    if not sort:
        return sql.SQL("")
    parts = [
        sql.SQL("{} {}").format(_ident(str(s.get("column", ""))), sql.SQL("desc" if s.get("desc") else "asc"))
        for s in sort
    ]
    return sql.SQL(" order by ") + sql.SQL(", ").join(parts)


def select_page(schema: str, name: str, filters: list[dict], sort: list[dict], limit: int, offset: int, with_ctid: bool) -> tuple[sql.Composable, list[Any]]:
    where, params = where_clause(filters)
    head = sql.SQL("select ctid::text as __ctid, * from {}") if with_ctid else sql.SQL("select * from {}")
    stmt = head.format(_ident(schema, name)) + where + order_clause(sort) + sql.SQL(" limit {} offset {}").format(sql.Placeholder(), sql.Placeholder())
    return stmt, [*params, limit, offset]


def count_rows(schema: str, name: str, filters: list[dict]) -> tuple[sql.Composable, list[Any]]:
    where, params = where_clause(filters)
    return sql.SQL("select count(*) from {}").format(_ident(schema, name)) + where, params


def insert_row(schema: str, name: str, values: dict[str, Any]) -> tuple[sql.Composable, list[Any]]:
    """``values``: column -> value; a value of None inserts NULL. Columns left out take their default."""
    table = _ident(schema, name)
    if not values:
        return sql.SQL("insert into {} default values returning ctid::text as __ctid, *").format(table), []
    columns = [_ident(c) for c in values]
    stmt = sql.SQL("insert into {} ({}) values ({}) returning ctid::text as __ctid, *").format(
        table, sql.SQL(", ").join(columns), sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    )
    return stmt, list(values.values())


def update_row(schema: str, name: str, ctid: str, values: dict[str, Any]) -> tuple[sql.Composable, list[Any]]:
    if not values:
        raise ValueError("nothing to update")
    assignments = sql.SQL(", ").join(sql.SQL("{} = {}").format(_ident(c), sql.Placeholder()) for c in values)
    stmt = sql.SQL("update {} set {} where ctid = {} returning ctid::text as __ctid, *").format(
        _ident(schema, name), assignments, sql.Placeholder()
    )
    return stmt, [*values.values(), _tid(ctid)]


def delete_rows(schema: str, name: str, ctids: list[str]) -> tuple[sql.Composable, list[Any]]:
    if not ctids:
        raise ValueError("no rows selected")
    stmt = sql.SQL("delete from {} where ctid = any({}::tid[])").format(_ident(schema, name), sql.Placeholder())
    return stmt, [[_tid(c) for c in ctids]]


def _tid(value: Any) -> str:
    text = str(value).strip()
    if not (text.startswith("(") and text.endswith(")") and text.count(",") == 1):
        raise ValueError(f"not a row id: {text!r}")
    return text


def _relkind(cur, schema: str, name: str) -> str:
    row = cur.execute(
        "select c.relkind from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = %s and c.relname = %s",
        (schema, name),
    ).fetchone()
    if row is None:
        raise LookupError(f"{schema}.{name} does not exist")
    return row["relkind"]


def fetch_page(info: ConnectionInfo, schema: str, name: str, filters: list[dict], sort: list[dict], limit: int, offset: int) -> dict:
    limit = max(1, min(int(limit), MAX_PAGE))
    offset = max(0, int(offset))
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        editable = _relkind(cur, schema, name) in EDITABLE_KINDS
        count_stmt, count_params = count_rows(schema, name, filters)
        total = cur.execute(count_stmt, count_params).fetchone()["count"]
        stmt, params = select_page(schema, name, filters, sort, limit, offset, with_ctid=editable)
        cur.execute(stmt, params)
        columns = [d.name for d in cur.description]
        rows = [[jsonable(v) for v in row.values()] for row in cur.fetchall()]
    ids: list[str | None] = []
    if editable:
        columns = columns[1:]
        ids = [row[0] for row in rows]
        rows = [row[1:] for row in rows]
    return {"columns": columns, "rows": rows, "ids": ids, "total": total, "offset": offset, "limit": limit, "editable": editable}


def _edit_result(cur) -> dict:
    row = cur.fetchone()
    if row is None:
        raise LookupError("the row changed or was deleted since the page was loaded; reload the page")
    values = [jsonable(v) for v in row.values()]
    return {"id": values[0], "row": values[1:]}


def insert(info: ConnectionInfo, schema: str, name: str, values: dict[str, Any]) -> dict:
    stmt, params = insert_row(schema, name, values)
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(stmt, params)
        return _edit_result(cur)


def update(info: ConnectionInfo, schema: str, name: str, ctid: str, values: dict[str, Any]) -> dict:
    stmt, params = update_row(schema, name, ctid, values)
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(stmt, params)
        return _edit_result(cur)


def delete(info: ConnectionInfo, schema: str, name: str, ctids: list[str]) -> dict:
    stmt, params = delete_rows(schema, name, ctids)
    with info.connect() as conn, conn.cursor() as cur:
        cur.execute(stmt, params)
        return {"deleted": cur.rowcount}
