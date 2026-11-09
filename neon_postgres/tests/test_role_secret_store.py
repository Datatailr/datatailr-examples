"""The role secret lives in the platform's secret store: the deploy creates it, the jobs read it."""

from __future__ import annotations

import re

import pytest

from neon_postgres import access
from neon_postgres.service.roles import RoleSyncer


class FakeStore:
    """Stands in for ``dt kv``: the laptop can list and write secrets but not read them."""

    def __init__(self, keys=()):
        self.keys = set(keys)
        self.puts: list[tuple] = []

    def ls(self, prefix="", **options):
        assert options.get("secure") is True
        return {"items": [{"key": k} for k in sorted(self.keys)], "total": len(self.keys)}

    def put(self, key, value, **options):
        self.puts.append((key, value, options))
        self.keys.add(key.lower().lstrip("/"))


def test_keys_are_normalised_like_the_kv_server_does():
    # The server lowercases every key and strips leading slashes, so a key with
    # capitals is stored under its lowercase name.
    assert access.normalize_key("Neon-Postgres/Role-Secret") == "neon-postgres/role-secret"
    assert access.normalize_key("//A/B ") == "a/b"
    assert access.normalize_key("") == ""


def test_role_secret_key_comes_from_the_environment_normalised(monkeypatch):
    monkeypatch.delenv(access.ROLE_SECRET_KEY_VAR, raising=False)
    assert access.role_secret_key() is None
    monkeypatch.setenv(access.JOB_ENV_PREFIX + access.ROLE_SECRET_KEY_VAR, "Team/Neon-Secret")
    assert access.role_secret_key() == "team/neon-secret"


def test_ensure_creates_a_random_secret_when_the_store_has_none():
    store = FakeStore()
    key, action = access.ensure_role_secret("dev", "Neon-Postgres/Role-Secret", client=store)
    assert (key, action) == ("neon-postgres/role-secret", "created")
    (put_key, value, options), = store.puts
    assert put_key == "neon-postgres/role-secret"
    assert re.fullmatch(r"[0-9a-f]{64}", value)
    assert options == {"secure": True, "environment": "dev"}


def test_ensure_keeps_an_existing_secret_without_reading_it():
    store = FakeStore(["neon-postgres/role-secret"])
    assert access.ensure_role_secret("dev", "NEON-POSTGRES/ROLE-SECRET", client=store) == ("neon-postgres/role-secret", "kept")
    assert store.puts == []


def test_ensure_stores_an_explicit_value_and_overwrites():
    store = FakeStore(["neon-postgres/role-secret"])
    assert access.ensure_role_secret("prod", "neon-postgres/role-secret", value="chosen", client=store) == (
        "neon-postgres/role-secret",
        "stored",
    )
    assert store.puts == [("neon-postgres/role-secret", "chosen", {"secure": True, "environment": "prod"})]


def test_role_secret_prefers_the_variable_then_reads_the_store(monkeypatch):
    access.forget_role_secret()
    monkeypatch.setenv(access.ROLE_SECRET_VAR, "from-env")
    monkeypatch.setenv(access.ROLE_SECRET_KEY_VAR, "neon-postgres/role-secret")
    monkeypatch.setattr(access, "read_stored_secret", lambda key: pytest.fail("the store must not be read"))
    assert access.role_secret() == "from-env"

    monkeypatch.delenv(access.ROLE_SECRET_VAR)
    reads = []

    def reader(key):
        reads.append(key)
        return "from-store"

    monkeypatch.setattr(access, "read_stored_secret", reader)
    assert access.role_secret() == "from-store"
    assert access.role_secret() == "from-store"  # cached: one read, not one per call
    assert reads == ["neon-postgres/role-secret"]


def test_role_secret_is_none_when_nothing_is_configured_or_the_store_fails(monkeypatch):
    access.forget_role_secret()
    monkeypatch.delenv(access.ROLE_SECRET_VAR, raising=False)
    monkeypatch.delenv(access.ROLE_SECRET_KEY_VAR, raising=False)
    assert access.role_secret() is None
    assert access.role_sync_enabled() is False

    monkeypatch.setenv(access.ROLE_SECRET_KEY_VAR, "neon-postgres/role-secret")
    assert access.role_sync_enabled() is True

    def failing(key):
        raise RuntimeError("Cannot read secrets")

    monkeypatch.setattr(access, "read_stored_secret", failing)
    assert access.role_secret() is None


def test_syncer_takes_the_secret_from_a_provider_each_time(monkeypatch):
    secrets = iter([None, "one"])
    loads = []
    syncer = RoleSyncer("postgresql://nobody@127.0.0.1:1/x", lambda: next(secrets), load=lambda: loads.append(1))
    # No secret: the sync reports and stops before loading the directory.
    syncer.sync_once()
    assert syncer.last_error == "no role secret: set NEON_ROLE_SECRET or NEON_ROLE_SECRET_KEY"
    assert loads == []
    # A secret: the sync loads the directory and goes on to the database.
    syncer.sync_once()
    assert loads == [1]
    assert syncer.last_error and "no role secret" not in syncer.last_error


def test_access_main_reads_the_store_when_the_variable_is_unset(monkeypatch, capsys):
    access.forget_role_secret()
    monkeypatch.delenv(access.ROLE_SECRET_VAR, raising=False)
    monkeypatch.delenv(access.ROLE_SECRET_KEY_VAR, raising=False)
    monkeypatch.setattr(access, "read_stored_secret", lambda key: {"neon-postgres/role-secret": "stored"}[key])
    assert access.main(["alice"]) == 0
    assert capsys.readouterr().out.strip() == access.role_password("stored", "alice")
    assert access.main(["alice", "--key", "Other/Key"]) == 2
    assert "other/key" in capsys.readouterr().err
