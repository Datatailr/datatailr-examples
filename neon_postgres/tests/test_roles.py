"""Platform users and groups become Postgres roles: the plan, and the live result."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import httpx
import psycopg
import pytest

from neon_postgres import access
from neon_postgres.explorer.app import app
from neon_postgres.service.roles import (
    GROUP_MARK,
    USER_MARK,
    Directory,
    ExistingRole,
    build_directory,
    fetch_existing_roles,
    plan_sync,
    sync_roles,
)

SECRET = "test-secret"


def render(statements) -> list[str]:
    return [s.as_string() for s in statements]


def test_role_passwords_are_derived_per_user_and_secret():
    alice = access.role_password(SECRET, "alice")
    assert alice == access.role_password(SECRET, "alice")
    assert alice != access.role_password(SECRET, "bob")
    assert alice != access.role_password("other", "alice")
    assert len(alice) >= 32 and " " not in alice
    assert access.is_reserved_role("cloud_admin") and access.is_reserved_role("pg_read_all_data")
    assert not access.is_reserved_role("developers")


def test_role_secret_reads_the_platform_prefixed_variable(monkeypatch):
    monkeypatch.delenv(access.ROLE_SECRET_VAR, raising=False)
    monkeypatch.setenv(access.JOB_ENV_PREFIX + access.ROLE_SECRET_VAR, "s3cret")
    assert access.role_secret() == "s3cret"
    monkeypatch.delenv(access.JOB_ENV_PREFIX + access.ROLE_SECRET_VAR)
    assert access.role_secret() is None


def test_directory_merges_user_groups_with_group_members():
    users = [
        {"name": "root", "user_id": 0, "groups": [""], "is_system": True},
        {"name": "alice", "user_id": 1001, "groups": ["admin"], "is_system": False},
        {"name": "bob", "user_id": 1002, "groups": [], "is_system": False},
        {"name": "bot", "user_id": 1003, "groups": ["developers"], "is_system": True},
    ]
    groups = [
        {"name": "admin", "members": [1001]},
        {"name": "developers", "members": [1002, 1003]},
        {"name": "dtusers", "members": [0, 1001, 1002, 1003]},
    ]
    directory = build_directory(users, groups)
    assert directory.groups == {"admin", "developers", "dtusers"}
    assert directory.users == {"alice": {"admin", "dtusers"}, "bob": {"developers", "dtusers"}}


def test_plan_creates_group_and_user_roles_with_memberships():
    directory = Directory(users={"alice": {"analysts", "dtusers"}, "bob": {"dtusers"}}, groups={"analysts", "dtusers"})
    statements = render(plan_sync(directory, {}, SECRET))
    assert statements[:4] == [
        'create role "analysts" nologin',
        f"comment on role \"analysts\" is '{GROUP_MARK}'",
        'create role "dtusers" nologin',
        f"comment on role \"dtusers\" is '{GROUP_MARK}'",
    ]
    assert f"create role \"alice\" login password '{access.role_password(SECRET, 'alice')}'" in statements
    assert f"comment on role \"alice\" is '{USER_MARK}'" in statements
    assert 'grant "analysts" to "alice"' in statements
    assert 'grant "dtusers" to "alice"' in statements
    assert 'grant "dtusers" to "bob"' in statements
    assert 'grant "analysts" to "bob"' not in statements


def _md5(user: str) -> str:
    import hashlib

    return "md5" + hashlib.md5((access.role_password(SECRET, user) + user).encode()).hexdigest()


def test_plan_follows_membership_changes_and_departures():
    existing = {
        "analysts": ExistingRole("analysts", False, GROUP_MARK),
        "ops": ExistingRole("ops", False, GROUP_MARK),
        "alice": ExistingRole("alice", True, USER_MARK, {"ops"}, _md5("alice")),
        "gone": ExistingRole("gone", True, USER_MARK, {"analysts"}, _md5("gone")),
    }
    directory = Directory(users={"alice": {"analysts"}}, groups={"analysts", "ops"})
    statements = render(plan_sync(directory, existing, SECRET))
    assert statements == [
        'grant "analysts" to "alice"',
        'revoke "ops" from "alice"',
        'alter role "gone" nologin',
        'revoke "analysts" from "gone"',
    ]


def test_plan_is_empty_when_nothing_changed_and_resets_a_changed_password():
    existing = {
        "analysts": ExistingRole("analysts", False, GROUP_MARK),
        "alice": ExistingRole("alice", True, USER_MARK, {"analysts"}, _md5("alice")),
    }
    directory = Directory(users={"alice": {"analysts"}}, groups={"analysts"})
    assert plan_sync(directory, existing, SECRET) == []
    assert render(plan_sync(directory, existing, "rotated")) == [
        f"alter role \"alice\" login password '{access.role_password('rotated', 'alice')}'"
    ]
    existing["alice"].login = False  # a user who came back
    assert render(plan_sync(directory, existing, SECRET))[0].startswith('alter role "alice" login')


def test_plan_leaves_foreign_and_reserved_roles_alone():
    existing = {"analysts": ExistingRole("analysts", False, None), "carol": ExistingRole("carol", True, None)}
    directory = Directory(
        users={"carol": {"analysts"}, "cloud_admin": set(), "pg_x": set(), "admin": set()}, groups={"analysts", "cloud_admin", "admin"}
    )
    skipped: dict[str, str] = {}
    statements = render(plan_sync(directory, existing, SECRET, skipped))
    assert statements == ['create role "admin" nologin', f"comment on role \"admin\" is '{GROUP_MARK}'"]
    assert set(skipped) == {"analysts", "carol", "cloud_admin", "pg_x", "admin"}
    assert skipped["admin"] == "user has the same name as a group"


# --------------------------------------------------------------------------- live, with NEON_TEST_DSN

DSN = os.environ.get("NEON_TEST_DSN")
live = pytest.mark.skipif(not DSN, reason="NEON_TEST_DSN is not set")
ROLES = ("acl_alice", "acl_bob", "acl_analysts", "acl_ops")


def _connection(**overrides) -> dict:
    url = urlparse(DSN)
    return {
        "host": url.hostname,
        "port": url.port or 5432,
        "dbname": (url.path or "/postgres").lstrip("/"),
        "user": url.username,
        "password": url.password or "",
        **overrides,
    }


def _signed(name: str) -> dict:
    return {"x-datatailr-user": f'{{"name": "{name}", "expiry": 0, "signature": "test"}}'}


def _drop_roles(conn: psycopg.Connection) -> None:
    for role in ROLES:
        if conn.execute("select 1 from pg_roles where rolname = %s", (role,)).fetchone():
            conn.execute(f"drop owned by {role}")
            conn.execute(f"drop role {role}")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def synced_db():
    directory = Directory(users={"acl_alice": {"acl_analysts"}, "acl_bob": {"acl_ops"}}, groups={"acl_analysts", "acl_ops"})
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("drop schema if exists acl_test cascade")
        _drop_roles(conn)
        sync_roles(conn, directory, SECRET)
        conn.execute("create schema acl_test")
        conn.execute("create table acl_test.sales(id int)")
        conn.execute("create table acl_test.servers(id int)")
        conn.execute("grant usage on schema acl_test to acl_analysts, acl_ops")
        conn.execute("grant select on acl_test.sales to acl_analysts")
        conn.execute("grant select on acl_test.servers to acl_ops")
        yield conn, directory
        conn.execute("drop schema acl_test cascade")
        _drop_roles(conn)


@live
def test_sync_is_idempotent_and_marks_its_roles(synced_db):
    conn, directory = synced_db
    roles = fetch_existing_roles(conn)
    assert roles["acl_alice"].login and roles["acl_alice"].mark == USER_MARK and roles["acl_alice"].member_of == {"acl_analysts"}
    assert not roles["acl_analysts"].login and roles["acl_analysts"].mark == GROUP_MARK
    again = render(plan_sync(directory, roles, SECRET))
    assert again and all(s.startswith("alter role") for s in again)

    directory.users["acl_alice"] = set()
    sync_roles(conn, directory, SECRET)
    assert fetch_existing_roles(conn)["acl_alice"].member_of == set()


@live
def test_each_user_sees_only_the_tables_of_their_groups(synced_db):
    for user, visible, hidden in (("acl_alice", "sales", "servers"), ("acl_bob", "servers", "sales")):
        params = _connection(user=user, password=access.role_password(SECRET, user))
        with psycopg.connect(**params, autocommit=True) as conn:
            assert conn.execute(f"select count(*) from acl_test.{visible}").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(f"select count(*) from acl_test.{hidden}")


@live
@pytest.mark.anyio
async def test_explorer_connects_as_the_platform_user(synced_db, monkeypatch):
    monkeypatch.setenv(access.ROLE_SECRET_VAR, SECRET)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://explorer") as client:
        me = (await client.get("/api/whoami", headers=_signed("acl_alice"))).json()
        assert me == {"user": "acl_alice", "role_secret": True}

        connection = _connection(user="", password="", as_user=True)
        info = await client.post("/api/connect", json={"connection": connection}, headers=_signed("acl_alice"))
        assert info.status_code == 200, info.text
        assert info.json()["user"] == "acl_alice"

        ok = await client.post("/api/query", json={"connection": connection, "sql": "select * from acl_test.sales"}, headers=_signed("acl_alice"))
        assert ok.status_code == 200, ok.text
        denied = await client.post("/api/query", json={"connection": connection, "sql": "select * from acl_test.servers"}, headers=_signed("acl_alice"))
        assert denied.status_code == 400 and "permission denied" in denied.json()["detail"]

        bob = await client.post("/api/query", json={"connection": connection, "sql": "select * from acl_test.servers"}, headers=_signed("acl_bob"))
        assert bob.status_code == 200, bob.text

        # Catalog rows stay visible to everyone; reading the rows of a table does not.
        relations = await client.post("/api/relations", json={"connection": connection, "schema": "acl_test"}, headers=_signed("acl_alice"))
        assert {r["name"] for r in relations.json()["relations"]} == {"sales", "servers"}
        sample = await client.post(
            "/api/relation", json={"connection": connection, "schema": "acl_test", "name": "servers"}, headers=_signed("acl_alice")
        )
        assert sample.status_code == 400 and "permission denied" in sample.json()["detail"]


@live
def test_syncer_runs_the_loader_and_reports(synced_db):
    from neon_postgres.service.roles import RoleSyncer

    conn, directory = synced_db
    syncer = RoleSyncer(DSN, SECRET, interval=3600, load=lambda: directory)
    syncer.sync_once()
    assert syncer.last_error is None and syncer.last_sync is not None
    assert syncer.status()["users"] == 2 and syncer.status()["groups"] == 2

    def broken():
        raise RuntimeError("platform unreachable")

    syncer.load = broken
    syncer.sync_once()
    assert syncer.last_error == "platform unreachable"
    assert fetch_existing_roles(conn)["acl_alice"].login  # the previous roles stay


def test_access_main_prints_the_role_password(monkeypatch, capsys):
    monkeypatch.setenv(access.ROLE_SECRET_VAR, SECRET)
    assert access.main(["alice"]) == 0
    assert capsys.readouterr().out.strip() == access.role_password(SECRET, "alice")
    monkeypatch.delenv(access.ROLE_SECRET_VAR)
    assert access.main(["alice"]) == 2
