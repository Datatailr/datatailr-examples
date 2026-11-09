"""Neon Postgres Explorer: a FastAPI app to query a database and manage its metadata.

The browser keeps the connection details and sends them with every request,
so the app itself holds no state. Management actions are rendered as SQL and
run through the same query endpoint, after the user has seen the statement.
The data grid (``rows``) edits rows with parameterised statements instead.

When the browser asks to connect "as me", the app connects as the Postgres
role that the Neon Postgres service created for the calling platform user.
The platform puts the signed user in the ``X-Datatailr-User`` header, and the
container's auth-server has verified it before the request reaches the app.
The role's password is derived from ``NEON_ROLE_SECRET``, which the deploy
gives to both jobs.
"""

from __future__ import annotations

import json
import logging
from importlib.resources import files
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from neon_postgres import access
from neon_postgres.explorer import actions, db, rows, services

log = logging.getLogger("explorer")

STATIC_DIR = Path(str(files(__package__).joinpath("static")))

app = FastAPI(
    title="Neon Postgres Explorer",
    description="Query a Neon Postgres service on Datatailr and manage its schemas, tables and indexes.",
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def platform_user(request: Request) -> str | None:
    """The platform user behind this request, from the X-Datatailr-User header.

    The header carries the signed user as JSON (``{"name": ..., "expiry": ...,
    "signature": ...}``). Off the platform there is no header and no user. The
    datatailr SDK offers ``User.from_request`` for the same purpose; it also
    looks the user up on the platform, which this app does not need.
    """
    raw = request.headers.get("x-datatailr-user", "").strip()
    if not raw:
        return None
    if raw.startswith("{"):
        try:
            name = json.loads(raw).get("name")
        except (ValueError, AttributeError):
            return None
        return str(name) if name else None
    return raw


def _connection(body: dict, request: Request) -> db.ConnectionInfo:
    try:
        info = db.ConnectionInfo.from_dict(body.get("connection") or {})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not info.as_user:
        return info
    username = platform_user(request)
    if username is None:
        raise HTTPException(status_code=401, detail="no platform user: the request carries no X-Datatailr-User header")
    secret = access.role_secret()
    if secret is None:
        raise HTTPException(
            status_code=400,
            detail=f"the explorer has no {access.ROLE_SECRET_VAR}; deploy it with the same secret as the Neon Postgres service",
        )
    return info.for_user(username, access.role_password(secret, username))


def _guard(func, *args, **kwargs) -> Any:
    """Run a database call and turn its failures into HTTP errors the UI can show."""
    try:
        return func(*args, **kwargs)
    except psycopg.OperationalError as exc:
        raise HTTPException(status_code=502, detail=f"connection failed: {exc}".strip()) from exc
    except psycopg.Error as exc:
        detail = exc.diag.message_primary if exc.diag and exc.diag.message_primary else str(exc)
        if exc.diag and exc.diag.message_detail:
            detail += f"\n{exc.diag.message_detail}"
        if exc.diag and exc.diag.message_hint:
            detail += f"\nhint: {exc.diag.message_hint}"
        raise HTTPException(status_code=400, detail=detail) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _target(body: dict) -> tuple[str, str]:
    schema, name = str(body.get("schema") or ""), str(body.get("name") or "")
    if not schema or not name:
        raise HTTPException(status_code=400, detail="schema and name are required")
    return schema, name


@app.get("/", include_in_schema=False)
def index(request: Request):
    """The page, with a <base href> for the prefix the platform serves the app under.

    On Datatailr the app runs with uvicorn's root path set to /job/<env>/<name>;
    locally the root path is empty. All links and API calls in the page are
    relative to that base.
    """
    base = request.scope.get("root_path", "").rstrip("/") + "/"
    html = (STATIC_DIR / "index.html").read_text().replace("__BASE__", base)
    return HTMLResponse(html)


@app.get("/health", response_class=PlainTextResponse, summary="Health check")
def health():
    return "OK\n"


@app.get("/api/whoami", summary="The platform user behind the request and whether it can connect as that user")
def whoami(request: Request):
    return {"user": platform_user(request), "role_secret": access.role_secret() is not None}


@app.get("/api/services", summary="Neon Postgres services found on the platform")
def list_services():
    return {"services": services.database_services()}


@app.post("/api/connect", summary="Test a connection and describe the server")
async def connect(request: Request):
    body = await request.json()
    return _guard(db.server_info, _connection(body, request))


@app.post("/api/query", summary="Run SQL and return the last result set")
async def query(request: Request):
    body = await request.json()
    sql_text = str(body.get("sql") or "").strip()
    if not sql_text:
        raise HTTPException(status_code=400, detail="sql is required")
    limit = max(1, min(int(body.get("limit") or db.DEFAULT_ROW_LIMIT), 10_000))
    return _guard(db.run_query, _connection(body, request), sql_text, limit)


@app.post("/api/databases", summary="Databases on the server")
async def databases(request: Request):
    body = await request.json()
    return {"databases": _guard(db.list_databases, _connection(body, request))}


@app.post("/api/schemas", summary="Schemas in the current database")
async def schemas(request: Request):
    body = await request.json()
    return {"schemas": _guard(db.list_schemas, _connection(body, request))}


@app.post("/api/relations", summary="Tables and views in a schema")
async def relations(request: Request):
    body = await request.json()
    schema = str(body.get("schema") or "")
    if not schema:
        raise HTTPException(status_code=400, detail="schema is required")
    return {"relations": _guard(db.list_relations, _connection(body, request), schema)}


@app.post("/api/relation", summary="Columns, indexes, constraints and sample rows of a relation")
async def relation(request: Request):
    body = await request.json()
    schema, name = _target(body)
    sample = max(0, min(int(body.get("sample") or 0), 500))
    return _guard(db.describe_relation, _connection(body, request), schema, name, sample)


@app.post("/api/ddl", summary="The CREATE statements for a table or view")
async def ddl(request: Request):
    body = await request.json()
    schema, name = _target(body)
    return {"sql": _guard(db.relation_ddl, _connection(body, request), schema, name)}


@app.post("/api/rows", summary="One page of a table, filtered and sorted")
async def rows_page(request: Request):
    body = await request.json()
    schema, name = _target(body)
    return _guard(
        rows.fetch_page,
        _connection(body, request),
        schema,
        name,
        list(body.get("filters") or []),
        list(body.get("sort") or []),
        int(body.get("limit") or 100),
        int(body.get("offset") or 0),
    )


@app.post("/api/rows/insert", summary="Insert one row; values are parameters, columns left out take their default")
async def rows_insert(request: Request):
    body = await request.json()
    schema, name = _target(body)
    return _guard(rows.insert, _connection(body, request), schema, name, dict(body.get("values") or {}))


@app.post("/api/rows/update", summary="Change the given columns of the row with this id")
async def rows_update(request: Request):
    body = await request.json()
    schema, name = _target(body)
    return _guard(rows.update, _connection(body, request), schema, name, str(body.get("id") or ""), dict(body.get("values") or {}))


@app.post("/api/rows/delete", summary="Delete the rows with these ids")
async def rows_delete(request: Request):
    body = await request.json()
    schema, name = _target(body)
    return _guard(rows.delete, _connection(body, request), schema, name, [str(i) for i in body.get("ids") or []])


@app.post("/api/action", summary="Render the SQL for a management action")
async def action(request: Request):
    body = await request.json()
    try:
        return {"sql": actions.build(str(body.get("action") or ""), dict(body.get("params") or {}))}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/types", summary="Column types offered by the UI")
def types():
    return {"types": actions.TYPES}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
