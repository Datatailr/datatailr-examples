"""Configuration rendering and state persistence for the Neon stack."""

from __future__ import annotations

import hashlib
import json

import pytest
from pathlib import Path

from neon_postgres.s3_gateway.backends import MemoryBackend
from neon_postgres.service.neon_stack import (
    STATE_KEY,
    NeonConfig,
    TenantState,
    compute_ctl_args,
    compute_ctl_preload,
    compute_spec,
    pageserver_toml,
    safekeeper_args,
)


def test_pageserver_config_points_at_the_gateway():
    cfg = NeonConfig(gateway_port=9100, bucket="db", checkpoint_timeout="30s")
    toml = pageserver_toml(cfg)
    assert "endpoint='http://127.0.0.1:9100'" in toml
    assert "bucket_name='db'" in toml
    assert "prefix_in_bucket='/pageserver'" in toml
    assert "control_plane_emergency_mode=true" in toml
    assert "control_plane_api='http://127.0.0.1:9100/'" in toml
    assert "checkpoint_timeout='30s'" in toml


def test_safekeeper_uploads_wal_through_the_gateway():
    args = safekeeper_args(NeonConfig(partial_backup_timeout="45s"), Path("/data/sk"))
    joined = " ".join(args)
    assert "prefix_in_bucket='/safekeeper'" in joined
    assert "--partial-backup-timeout=45s" in joined
    assert "-D=/data/sk" in joined


def test_compute_spec_wires_tenant_timeline_and_password():
    cfg = NeonConfig(pg_user="app", pg_password="secret", pg_port=5433)
    state = TenantState("t" * 32, "l" * 32, 3)
    spec = compute_spec(cfg, state)

    settings = {s["name"]: s["value"] for s in spec["spec"]["cluster"]["settings"]}
    assert settings["neon.tenant_id"] == "t" * 32
    assert settings["neon.timeline_id"] == "l" * 32
    assert settings["port"] == "5433"
    assert settings["listen_addresses"] == "0.0.0.0"
    assert settings["shared_preload_libraries"] == "neon"
    assert settings["neon.safekeepers"] == "127.0.0.1:5454"

    role = spec["spec"]["cluster"]["roles"][0]
    assert role["name"] == "app"
    assert role["encrypted_password"] == hashlib.md5(b"secretapp").hexdigest()
    assert "jwks" in spec["compute_ctl_config"]
    json.dumps(spec)  # must be serializable


def test_compute_ctl_uses_the_bundled_postgres():
    cfg = NeonConfig(bin_dir="/opt/neon", pg_version=17)
    args = compute_ctl_args(cfg, Path("/data/compute"), Path("/data/compute/spec.json"))
    assert args[0] == "/opt/neon/bin/compute_ctl"
    assert "--pgbin=/opt/neon/v17/bin/postgres" in args
    assert "--pgdata=/data/compute/pgdata" in args
    assert "--dev" in args


def test_state_roundtrip_and_missing_state():
    backend = MemoryBackend()
    assert TenantState.load(backend, "neon") is None

    TenantState("t" * 32, None, 1).save(backend, "neon")
    loaded = TenantState.load(backend, "neon")
    assert loaded == TenantState("t" * 32, None, 1)
    assert backend.stat("neon", STATE_KEY) is not None

    loaded.timeline_id = "l" * 32
    loaded.generation = 2
    loaded.save(backend, "neon")
    assert TenantState.load(backend, "neon") == TenantState("t" * 32, "l" * 32, 2)


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("NEON_PG_PASSWORD", "pw")
    monkeypatch.setenv("NEON_BUCKET", "mydb")
    monkeypatch.setenv("NEON_CHECKPOINT_DISTANCE", "1024")
    cfg = NeonConfig.from_env()
    assert cfg.pg_password == "pw"
    assert cfg.bucket == "mydb"
    assert cfg.checkpoint_distance == 1024
    assert cfg.pg_dir == Path("/opt/neon/v17")



def test_unreadable_state_file_is_an_error_not_a_new_tenant(tmp_path):
    backend = MemoryBackend()
    bad = tmp_path / "bad"
    bad.write_text("")
    backend.upload("neon", STATE_KEY, str(bad))
    with pytest.raises(RuntimeError, match="not valid JSON"):
        TenantState.load(backend, "neon")


def test_core_limit_shim_is_used_only_when_needed(tmp_path):
    import resource

    cfg = NeonConfig(bin_dir=str(tmp_path))
    assert compute_ctl_preload(cfg, resource.RLIM_INFINITY) is None
    assert compute_ctl_preload(cfg, 0) is None  # shim not built
    cfg.setrlimit_shim.parent.mkdir()
    cfg.setrlimit_shim.write_bytes(b"")
    assert compute_ctl_preload(cfg, 0) == str(cfg.setrlimit_shim)
    assert compute_ctl_preload(cfg, resource.RLIM_INFINITY) is None


def test_config_accepts_the_platform_env_prefix(monkeypatch):
    monkeypatch.delenv("NEON_PG_PASSWORD", raising=False)
    monkeypatch.setenv("DATATAILR_JOB_ENV_NEON_PG_PASSWORD", "from-platform")
    monkeypatch.setenv("DATATAILR_JOB_ENV_NEON_BUCKET", "db2")
    cfg = NeonConfig.from_env()
    assert cfg.pg_password == "from-platform"
    assert cfg.bucket == "db2"
