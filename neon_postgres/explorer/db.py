"""Database access for the explorer: connections, queries and catalog metadata.

Every function takes a ``ConnectionInfo`` and opens a short-lived connection,
so the app holds no state between requests.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")
DEFAULT_ROW_LIMIT = 1000


@dataclass(frozen=True)
class ConnectionInfo:
    host: str
    port: int = 5432
    dbname: str = "postgres"
    user: str = "cloud_admin"
    password: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionInfo":
        host = str(data.get("host", "")).strip()
        if not host:
            raise ValueError("host is required")
        return cls(
            host=host,
            port=int(data.get("port") or 5432),
            dbname=str(data.get("dbname") or "postgres"),
            user=str(data.get("user") or "cloud_admin"),
            password=str(data.get("password") or ""),
        )

    def connect(self, autocommit: bool = True) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=10,
            autocommit=autocommit,
            application_name="neon-postgres-explorer",
        )


def jsonable(value: Any) -> Any:
    """Turn a cell value into something ``json.dumps`` accepts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    return str(value)


def server_info(info: ConnectionInfo) -> dict:
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            """
            select version() as version,
                   current_database() as database,
                   current_user as "user",
                   pg_size_pretty(pg_database_size(current_database())) as size,
                   now() as server_time
            """
        ).fetchone()
    return {k: jsonable(v) for k, v in row.items()}


def run_query(info: ConnectionInfo, query: str, limit: int = DEFAULT_ROW_LIMIT) -> dict:
    """Run one or more statements with autocommit; return the last result set, if any."""
    started = time.perf_counter()
    with info.connect() as conn, conn.cursor() as cur:
        cur.execute(query)
        columns = [d.name for d in cur.description] if cur.description else []
        rows: list[list[Any]] = []
        truncated = False
        if cur.description:
            for row in cur:
                if len(rows) >= limit:
                    truncated = True
                    break
                rows.append([jsonable(v) for v in row])
        status = cur.statusmessage
        rowcount = cur.rowcount
    return {
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
        "rowcount": rowcount,
        "status": status,
        "duration_ms": round((time.perf_counter() - started) * 1000, 1),
    }


def list_databases(info: ConnectionInfo) -> list[dict]:
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            select datname as name, pg_size_pretty(pg_database_size(datname)) as size,
                   pg_get_userbyid(datdba) as owner
            from pg_database where not datistemplate order by datname
            """
        ).fetchall()
    return [dict(r) for r in rows]


def list_schemas(info: ConnectionInfo) -> list[dict]:
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            select n.nspname as name, pg_get_userbyid(n.nspowner) as owner,
                   count(c.oid) filter (where c.relkind in ('r','p','v','m','f')) as relations
            from pg_namespace n
            left join pg_class c on c.relnamespace = n.oid
            where n.nspname not in ('pg_catalog','information_schema','pg_toast')
              and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
            group by n.nspname, n.nspowner
            order by n.nspname = 'public' desc, n.nspname
            """
        ).fetchall()
    return [dict(r) for r in rows]


def list_relations(info: ConnectionInfo, schema: str) -> list[dict]:
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            select c.relname as name,
                   case c.relkind when 'r' then 'table' when 'p' then 'partitioned table'
                                  when 'v' then 'view' when 'm' then 'materialized view'
                                  when 'f' then 'foreign table' end as kind,
                   c.reltuples::bigint as estimated_rows,
                   pg_size_pretty(pg_total_relation_size(c.oid)) as size,
                   pg_total_relation_size(c.oid) as size_bytes,
                   obj_description(c.oid, 'pg_class') as comment
            from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = %s and c.relkind in ('r','p','v','m','f')
            order by c.relname
            """,
            (schema,),
        ).fetchall()
    return [dict(r) for r in rows]


def describe_relation(info: ConnectionInfo, schema: str, name: str, sample: int = 50) -> dict:
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        rel = cur.execute(
            """
            select c.oid, c.relkind, pg_get_userbyid(c.relowner) as owner,
                   obj_description(c.oid, 'pg_class') as comment,
                   c.reltuples::bigint as estimated_rows,
                   pg_size_pretty(pg_total_relation_size(c.oid)) as size
            from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = %s and c.relname = %s
            """,
            (schema, name),
        ).fetchone()
        if rel is None:
            raise LookupError(f"{schema}.{name} does not exist")
        oid = rel["oid"]
        columns = cur.execute(
            """
            select a.attname as name, format_type(a.atttypid, a.atttypmod) as type,
                   not a.attnotnull as nullable,
                   pg_get_expr(d.adbin, d.adrelid) as "default",
                   exists (select 1 from pg_constraint p
                           where p.conrelid = a.attrelid and p.contype = 'p' and a.attnum = any(p.conkey)) as primary_key,
                   col_description(a.attrelid, a.attnum) as comment
            from pg_attribute a
            left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
            where a.attrelid = %s and a.attnum > 0 and not a.attisdropped
            order by a.attnum
            """,
            (oid,),
        ).fetchall()
        indexes = cur.execute(
            """
            select i.relname as name, pg_get_indexdef(i.oid) as definition, x.indisunique as "unique",
                   x.indisprimary as primary_key, pg_size_pretty(pg_relation_size(i.oid)) as size
            from pg_index x join pg_class i on i.oid = x.indexrelid
            where x.indrelid = %s order by i.relname
            """,
            (oid,),
        ).fetchall()
        constraints = cur.execute(
            """
            select conname as name,
                   case contype when 'p' then 'primary key' when 'f' then 'foreign key' when 'u' then 'unique'
                                when 'c' then 'check' when 'x' then 'exclusion' else contype::text end as type,
                   pg_get_constraintdef(oid) as definition
            from pg_constraint where conrelid = %s order by contype, conname
            """,
            (oid,),
        ).fetchall()
        definition = None
        if rel["relkind"] in ("v", "m"):
            definition = cur.execute("select pg_get_viewdef(%s, true)", (oid,)).fetchone()["pg_get_viewdef"]
        sample_rows: dict = {"columns": [], "rows": []}
        if sample > 0:
            query = sql.SQL("select * from {} limit {}").format(sql.Identifier(schema, name), sql.Literal(sample))
            cur.execute(query)
            sample_rows = {
                "columns": [d.name for d in cur.description],
                "rows": [[jsonable(v) for v in row.values()] for row in cur.fetchall()],
            }
    kinds = {"r": "table", "p": "partitioned table", "v": "view", "m": "materialized view", "f": "foreign table"}
    return {
        "schema": schema,
        "name": name,
        "kind": kinds.get(rel["relkind"], rel["relkind"]),
        "owner": rel["owner"],
        "comment": rel["comment"],
        "estimated_rows": rel["estimated_rows"],
        "size": rel["size"],
        "columns": [dict(c) for c in columns],
        "indexes": [dict(i) for i in indexes],
        "constraints": [dict(c) for c in constraints],
        "definition": definition,
        "sample": sample_rows,
    }


def _ddl_columns(cur, oid: int) -> list[dict]:
    return [
        dict(r)
        for r in cur.execute(
            """
            select a.attname as name, format_type(a.atttypid, a.atttypmod) as type, a.attnotnull as not_null,
                   pg_get_expr(d.adbin, d.adrelid) as "default", a.attidentity as identity,
                   a.attgenerated as generated, col_description(a.attrelid, a.attnum) as comment
            from pg_attribute a
            left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
            where a.attrelid = %s and a.attnum > 0 and not a.attisdropped
            order by a.attnum
            """,
            (oid,),
        ).fetchall()
    ]


def relation_ddl(info: ConnectionInfo, schema: str, name: str) -> str:
    """The CREATE statements that rebuild a table or view, built from the catalog.

    Postgres has no function that prints a table definition, so this assembles
    one: columns, constraints, the indexes that do not back a constraint, and
    comments. The output is for reading and copying, not a byte-exact pg_dump.
    """
    with info.connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        rel = cur.execute(
            """
            select c.oid, c.relkind, obj_description(c.oid, 'pg_class') as comment
            from pg_class c join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = %s and c.relname = %s
            """,
            (schema, name),
        ).fetchone()
        if rel is None:
            raise LookupError(f"{schema}.{name} does not exist")
        oid, kind = rel["oid"], rel["relkind"]
        table = sql.Identifier(schema, name)
        statements: list[sql.Composable] = []
        if kind in ("v", "m"):
            body = cur.execute("select pg_get_viewdef(%s, true) as def", (oid,)).fetchone()["def"].rstrip().rstrip(";")
            verb = "create or replace view" if kind == "v" else "create materialized view"
            statements.append(sql.SQL("{} {} as\n{}").format(sql.SQL(verb), table, sql.SQL(body)))
        elif kind in ("r", "p", "f"):
            lines: list[sql.Composable] = []
            for col in _ddl_columns(cur, oid):
                piece = sql.SQL("{} {}").format(sql.Identifier(col["name"]), sql.SQL(col["type"]))
                if col["generated"] == "s":
                    piece += sql.SQL(" generated always as ({}) stored").format(sql.SQL(col["default"] or ""))
                elif col["identity"] in ("a", "d"):
                    piece += sql.SQL(" generated {} as identity").format(sql.SQL("always" if col["identity"] == "a" else "by default"))
                elif col["default"] is not None:
                    piece += sql.SQL(" default {}").format(sql.SQL(col["default"]))
                if col["not_null"]:
                    piece += sql.SQL(" not null")
                lines.append(piece)
            constraints = cur.execute(
                "select conname, pg_get_constraintdef(oid) as def from pg_constraint where conrelid = %s order by contype, conname",
                (oid,),
            ).fetchall()
            for con in constraints:
                lines.append(sql.SQL("constraint {} {}").format(sql.Identifier(con["conname"]), sql.SQL(con["def"])))
            verb = "create table" if kind != "f" else "create foreign table"
            body = sql.SQL(",\n    ").join(lines)
            statements.append(sql.SQL("{} {} (\n    {}\n)").format(sql.SQL(verb), table, body))
            indexes = cur.execute(
                """
                select pg_get_indexdef(x.indexrelid) as def
                from pg_index x
                where x.indrelid = %s
                  and not exists (select 1 from pg_constraint p where p.conindid = x.indexrelid)
                order by x.indexrelid
                """,
                (oid,),
            ).fetchall()
            statements.extend(sql.SQL(i["def"]) for i in indexes)
        else:
            raise LookupError(f"{schema}.{name} is not a table or view")
        if rel["comment"]:
            statements.append(sql.SQL("comment on {} {} is {}").format(sql.SQL("view" if kind == "v" else "table"), table, sql.Literal(rel["comment"])))
        if kind in ("r", "p", "f"):
            for col in _ddl_columns(cur, oid):
                if col["comment"]:
                    statements.append(
                        sql.SQL("comment on column {} is {}").format(sql.Identifier(schema, name, col["name"]), sql.Literal(col["comment"]))
                    )
        return "\n\n".join(s.as_string(conn) + ";" for s in statements) + "\n"
