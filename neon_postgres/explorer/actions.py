"""Metadata management as generated SQL.

The explorer never runs a management action directly. Each action here returns
the SQL text, the UI puts it in the query editor, and the user runs it after
reading it. Identifiers are quoted with psycopg's ``sql`` module.
"""

from __future__ import annotations

from psycopg import sql

TYPES = [
    "integer", "bigint", "smallint", "serial", "bigserial", "numeric", "real", "double precision",
    "text", "varchar(255)", "char(1)", "boolean", "date", "timestamp", "timestamptz", "time",
    "interval", "uuid", "json", "jsonb", "bytea", "inet",
]


def _ident(*parts: str) -> sql.Composable:
    for part in parts:
        if not part or not part.strip():
            raise ValueError("identifiers must not be empty")
    return sql.Identifier(*parts)


def _render(stmt: sql.Composable) -> str:
    return stmt.as_string(None) + ";"


def create_schema(name: str) -> str:
    return _render(sql.SQL("create schema {}").format(_ident(name)))


def drop_schema(name: str, cascade: bool = False) -> str:
    return _render(sql.SQL("drop schema {}{}").format(_ident(name), sql.SQL(" cascade" if cascade else "")))


def create_table(schema: str, name: str, columns: list[dict]) -> str:
    """``columns``: dicts with name, type, and optional nullable (default True), primary_key, default."""
    if not columns:
        raise ValueError("a table needs at least one column")
    parts = []
    for col in columns:
        col_type = str(col.get("type", "")).strip()
        if not col_type:
            raise ValueError(f"column {col.get('name')!r} needs a type")
        piece = sql.SQL("{} {}").format(_ident(str(col["name"])), sql.SQL(col_type))
        if col.get("primary_key"):
            piece += sql.SQL(" primary key")
        elif col.get("nullable", True) is False:
            piece += sql.SQL(" not null")
        if col.get("default") not in (None, ""):
            piece += sql.SQL(" default ") + sql.SQL(str(col["default"]))
        parts.append(piece)
    body = sql.SQL(",\n    ").join(parts)
    return _render(sql.SQL("create table {} (\n    {}\n)").format(_ident(schema, name), body))


def drop_relation(schema: str, name: str, kind: str = "table", cascade: bool = False) -> str:
    verbs = {"table": "drop table", "view": "drop view", "materialized view": "drop materialized view",
             "partitioned table": "drop table", "foreign table": "drop foreign table"}
    verb = verbs.get(kind)
    if verb is None:
        raise ValueError(f"cannot drop a {kind}")
    return _render(sql.SQL("{} {}{}").format(sql.SQL(verb), _ident(schema, name), sql.SQL(" cascade" if cascade else "")))


def truncate_table(schema: str, name: str) -> str:
    return _render(sql.SQL("truncate table {}").format(_ident(schema, name)))


def rename_relation(schema: str, name: str, new_name: str, kind: str = "table") -> str:
    verb = "alter view" if kind == "view" else "alter materialized view" if kind == "materialized view" else "alter table"
    return _render(sql.SQL("{} {} rename to {}").format(sql.SQL(verb), _ident(schema, name), _ident(new_name)))


def add_column(schema: str, name: str, column: str, col_type: str, nullable: bool = True, default: str = "") -> str:
    if not col_type.strip():
        raise ValueError("the column needs a type")
    stmt = sql.SQL("alter table {} add column {} {}").format(_ident(schema, name), _ident(column), sql.SQL(col_type))
    if not nullable:
        stmt += sql.SQL(" not null")
    if default:
        stmt += sql.SQL(" default ") + sql.SQL(default)
    return _render(stmt)


def drop_column(schema: str, name: str, column: str) -> str:
    return _render(sql.SQL("alter table {} drop column {}").format(_ident(schema, name), _ident(column)))


def rename_column(schema: str, name: str, column: str, new_name: str) -> str:
    return _render(
        sql.SQL("alter table {} rename column {} to {}").format(_ident(schema, name), _ident(column), _ident(new_name))
    )


def create_index(schema: str, name: str, columns: list[str], index_name: str = "", unique: bool = False) -> str:
    if not columns:
        raise ValueError("an index needs at least one column")
    index_name = index_name or f"{name}_{'_'.join(columns)}_idx"
    return _render(
        sql.SQL("create {}index {} on {} ({})").format(
            sql.SQL("unique " if unique else ""),
            _ident(index_name),
            _ident(schema, name),
            sql.SQL(", ").join(_ident(c) for c in columns),
        )
    )


def drop_index(schema: str, index_name: str) -> str:
    return _render(sql.SQL("drop index {}").format(_ident(schema, index_name)))


def set_comment(schema: str, name: str, comment: str, column: str = "") -> str:
    target = sql.SQL("column {}").format(_ident(schema, name, column)) if column else sql.SQL("table {}").format(_ident(schema, name))
    value = sql.Literal(comment) if comment else sql.SQL("null")
    return _render(sql.SQL("comment on {} is {}").format(target, value))


ACTIONS = {
    "create_schema": create_schema,
    "drop_schema": drop_schema,
    "create_table": create_table,
    "drop_relation": drop_relation,
    "truncate_table": truncate_table,
    "rename_relation": rename_relation,
    "add_column": add_column,
    "drop_column": drop_column,
    "rename_column": rename_column,
    "create_index": create_index,
    "drop_index": drop_index,
    "set_comment": set_comment,
}


def build(action: str, params: dict) -> str:
    """Render the SQL for ``action`` with keyword ``params``."""
    try:
        func = ACTIONS[action]
    except KeyError:
        raise ValueError(f"unknown action {action!r}") from None
    return func(**params)
