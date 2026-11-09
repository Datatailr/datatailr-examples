"""Run a complete single-node Neon stack inside one container.

Processes, all bound to localhost except Postgres itself:

    storage_broker  (50051)  message bus between safekeeper and pageserver
    safekeeper      (5454)   receives WAL from Postgres, uploads it to the S3 gateway
    pageserver      (6400)   serves pages to Postgres, stores layer files via the S3 gateway
    compute_ctl              starts Postgres (5432) against the pageserver and safekeeper

Durable state lives only in object storage, through the S3 gateway. Local disk
is a cache: when the container restarts, the tenant is re-attached from the
bucket with a new generation number. The tenant id, timeline id and the last
generation are kept in a small JSON object in the same bucket.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import resource
import secrets
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from neon_postgres.s3_gateway.backends import Backend, NotFound

log = logging.getLogger("neon_stack")

STATE_KEY = "neon-postgres/state.json"

# Every Postgres log line starts with the time, the backend pid, the database
# user, the database, the client's application name and the client host.
LOG_LINE_PREFIX = "%m [%p] user=%u db=%d app=%a host=%h "
LOG_STATEMENT_VALUES = ("none", "ddl", "mod", "all")

# Public key from Neon's own docker-compose example. compute_ctl requires a JWKS to
# authorize calls to its HTTP API; nobody holds the matching private key, so the
# API is effectively locked.
COMPUTE_CTL_JWKS = {
    "keys": [
        {
            "use": "sig",
            "key_ops": ["verify"],
            "alg": "EdDSA",
            "kid": "ZGIxMzAzOGY0YWQwODk2ODU1MTk1NzMxMDFkYmUyOWU2NzZkOWNjNjMyMGRkZGJjOWY0MjdjYWVmNzE1MjUyOAo=",
            "kty": "OKP",
            "crv": "Ed25519",
            "x": "MGQ4ZDFhOTdmNTM0NmUwMDc3ZmJkN2Q0MWE0ZmI3M2NhNWE3YjFjOTNkM2IyYzRkZTQzOGM3MjBkZTk3N2E5ZAo=",
        }
    ]
}


@dataclass
class NeonConfig:
    bin_dir: str = "/opt/neon"
    pg_version: int = 17
    data_dir: str = "/tmp/neon_postgres"
    bucket: str = "neon"
    gateway_port: int = 9000
    pg_port: int = 5432
    pg_user: str = "cloud_admin"
    pg_password: str = "cloud_admin"
    shared_buffers: str = "256MB"
    max_connections: int = 100
    # How long WAL and layers may sit on local disk before they are uploaded.
    # Lower values shrink the data-loss window after a container crash.
    partial_backup_timeout: str = "1m"
    checkpoint_timeout: str = "1m"
    checkpoint_distance: int = 64 * 1024 * 1024
    # Which statements Postgres logs: none, ddl, mod (DDL plus writes) or all. The
    # log line carries the user, database, application name and client host, so
    # the service log is the query audit trail (see "Audit logging" in the README).
    # "all" also logs the four queries compute_ctl's monitor runs every half
    # second; the explorer logs all of its own statements whatever this says.
    log_statement: str = "mod"
    log_line_prefix: str = LOG_LINE_PREFIX
    extra_settings: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "NeonConfig":
        # The platform stores a job's env_vars with a DATATAILR_JOB_ENV_ prefix; accept both spellings.
        env = {
            key.removeprefix("DATATAILR_JOB_ENV_"): value
            for key, value in sorted(os.environ.items(), key=lambda item: item[0].startswith("DATATAILR_JOB_ENV_"))
        }
        log_statement = env.get("NEON_LOG_STATEMENT", cls.log_statement).strip().lower()
        if log_statement not in LOG_STATEMENT_VALUES:
            raise ValueError(f"NEON_LOG_STATEMENT must be one of {', '.join(LOG_STATEMENT_VALUES)}, not {log_statement!r}")
        return cls(
            bin_dir=env.get("NEON_BIN_DIR", cls.bin_dir),
            pg_version=int(env.get("NEON_PG_VERSION", cls.pg_version)),
            data_dir=env.get("NEON_DATA_DIR", cls.data_dir),
            bucket=env.get("NEON_BUCKET", cls.bucket),
            gateway_port=int(env.get("S3_GATEWAY_PORT", cls.gateway_port)),
            pg_port=int(env.get("NEON_PG_PORT", cls.pg_port)),
            pg_user=env.get("NEON_PG_USER", cls.pg_user),
            pg_password=env.get("NEON_PG_PASSWORD", cls.pg_password),
            shared_buffers=env.get("NEON_SHARED_BUFFERS", cls.shared_buffers),
            max_connections=int(env.get("NEON_MAX_CONNECTIONS", cls.max_connections)),
            partial_backup_timeout=env.get("NEON_PARTIAL_BACKUP_TIMEOUT", cls.partial_backup_timeout),
            checkpoint_timeout=env.get("NEON_CHECKPOINT_TIMEOUT", cls.checkpoint_timeout),
            checkpoint_distance=int(env.get("NEON_CHECKPOINT_DISTANCE", cls.checkpoint_distance)),
            log_statement=log_statement,
        )

    @property
    def pg_dir(self) -> Path:
        return Path(self.bin_dir) / f"v{self.pg_version}"

    def binary(self, name: str) -> str:
        return str(Path(self.bin_dir) / "bin" / name)

    @property
    def setrlimit_shim(self) -> Path:
        return Path(self.bin_dir) / "lib" / "setrlimit_shim.so"

    @property
    def gateway_url(self) -> str:
        return f"http://127.0.0.1:{self.gateway_port}"

    def remote_storage(self, prefix: str) -> str:
        return (
            f"{{endpoint='{self.gateway_url}', bucket_name='{self.bucket}', "
            f"bucket_region='local', prefix_in_bucket='{prefix}'}}"
        )


@dataclass
class TenantState:
    tenant_id: str
    timeline_id: str | None
    generation: int

    @classmethod
    def load(cls, backend: Backend, bucket: str) -> "TenantState | None":
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            path = handle.name
        try:
            backend.download(bucket, STATE_KEY, path)
            with open(path) as handle:
                content = handle.read()
        except NotFound:
            return None
        finally:
            os.unlink(path)
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            # Never start a new tenant over an unreadable state file: that would orphan the data.
            raise RuntimeError(
                f"{bucket}/{STATE_KEY} exists but is not valid JSON ({len(content)} bytes: {content[:80]!r}); "
                "fix or remove it before starting the service"
            ) from exc
        return cls(data["tenant_id"], data.get("timeline_id"), int(data["generation"]))

    def save(self, backend: Backend, bucket: str) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            json.dump({"tenant_id": self.tenant_id, "timeline_id": self.timeline_id, "generation": self.generation}, handle)
            path = handle.name
        try:
            backend.upload(bucket, STATE_KEY, path)
        finally:
            os.unlink(path)


def pageserver_toml(cfg: NeonConfig) -> str:
    return "\n".join(
        [
            "broker_endpoint='http://127.0.0.1:50051'",
            f"pg_distrib_dir='{cfg.bin_dir}'",
            "listen_pg_addr='127.0.0.1:6400'",
            "listen_http_addr='127.0.0.1:9898'",
            f"remote_storage={cfg.remote_storage('/pageserver')}",
            # No storage controller in this setup: tenants are attached through the HTTP API,
            # and the gateway answers the pageserver's generation validation calls.
            f"control_plane_api='{cfg.gateway_url}/'",
            "control_plane_emergency_mode=true",
            # Overlay filesystems do not reliably support O_DIRECT.
            "virtual_file_io_mode='buffered'",
            "[tenant_config]",
            f"checkpoint_timeout='{cfg.checkpoint_timeout}'",
            f"checkpoint_distance={cfg.checkpoint_distance}",
            "",
        ]
    )


def safekeeper_args(cfg: NeonConfig, datadir: Path) -> list[str]:
    return [
        cfg.binary("safekeeper"),
        "--id=1",
        f"-D={datadir}",
        "--listen-pg=127.0.0.1:5454",
        "--listen-http=127.0.0.1:7676",
        "--broker-endpoint=http://127.0.0.1:50051",
        f"--remote-storage={cfg.remote_storage('/safekeeper')}",
        f"--partial-backup-timeout={cfg.partial_backup_timeout}",
    ]


def compute_spec(cfg: NeonConfig, state: TenantState) -> dict:
    password_md5 = hashlib.md5((cfg.pg_password + cfg.pg_user).encode()).hexdigest()
    settings = {
        "port": str(cfg.pg_port),
        "listen_addresses": "0.0.0.0",
        "shared_buffers": cfg.shared_buffers,
        "max_connections": str(cfg.max_connections),
        "fsync": "off",
        "wal_level": "logical",
        "wal_log_hints": "on",
        "log_connections": "on",
        "log_statement": cfg.log_statement,
        "log_line_prefix": cfg.log_line_prefix,
        "max_wal_senders": "10",
        "max_replication_slots": "10",
        "wal_sender_timeout": "5s",
        "wal_keep_size": "0",
        "password_encryption": "md5",
        "restart_after_crash": "off",
        "synchronous_standby_names": "walproposer",
        "shared_preload_libraries": "neon",
        "neon.safekeepers": "127.0.0.1:5454",
        "neon.timeline_id": state.timeline_id,
        "neon.tenant_id": state.tenant_id,
        "neon.pageserver_connstring": "host=127.0.0.1 port=6400",
        "max_replication_write_lag": "500MB",
        "max_replication_flush_lag": "10GB",
    }
    settings.update(cfg.extra_settings)
    return {
        "spec": {
            "format_version": 1.0,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "operation_uuid": str(uuid.uuid4()),
            "suspend_timeout_seconds": -1,
            "cluster": {
                "cluster_id": "neon-postgres",
                "name": "neon-postgres",
                "state": "restarted",
                "roles": [{"name": cfg.pg_user, "encrypted_password": password_md5, "options": None}],
                "databases": [],
                "settings": [{"name": name, "value": value, "vartype": "string"} for name, value in settings.items()],
            },
            "delta_operations": [],
        },
        "compute_ctl_config": {"jwks": COMPUTE_CTL_JWKS},
    }


def compute_ctl_preload(cfg: NeonConfig, hard_core_limit: int) -> str | None:
    """The LD_PRELOAD compute_ctl needs, or None.

    compute_ctl raises the core dump limit to unlimited and aborts with EPERM when
    the container's hard limit is finite, which is the case on Datatailr nodes.
    The shim built by build_pre.sh turns that one failure into success.
    """
    if hard_core_limit == resource.RLIM_INFINITY or not cfg.setrlimit_shim.is_file():
        return None
    return str(cfg.setrlimit_shim)


def compute_ctl_args(cfg: NeonConfig, datadir: Path, spec_path: Path) -> list[str]:
    return [
        cfg.binary("compute_ctl"),
        f"--pgdata={datadir / 'pgdata'}",
        f"--connstr=postgresql://{cfg.pg_user}@127.0.0.1:{cfg.pg_port}/postgres",
        f"--pgbin={cfg.pg_dir / 'bin' / 'postgres'}",
        f"--compute-id=neon-postgres-{secrets.token_hex(4)}",
        f"--config={spec_path}",
        "--dev",
    ]


# --------------------------------------------------------------------------- pageserver HTTP API


def _http(method: str, url: str, body: dict | None = None, timeout: float = 30) -> dict | list | None:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw) if raw else None


def wait_for(description: str, probe, timeout: float = 120, interval: float = 0.5, abort=None) -> None:
    """Poll ``probe`` until it is true. ``abort`` returns a reason to give up early."""
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        if abort is not None:
            reason = abort()
            if reason:
                raise RuntimeError(f"gave up waiting for {description}: {reason}")
        try:
            if probe():
                return
        except Exception as exc:  # the service is not up yet
            last_error = exc
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {description}: {last_error}")


# --------------------------------------------------------------------------- the stack


class NeonStack:
    def __init__(self, cfg: NeonConfig, backend: Backend) -> None:
        self.cfg = cfg
        self.backend = backend
        self.root = Path(cfg.data_dir)
        self.processes: dict[str, subprocess.Popen] = {}
        self.state: TenantState | None = None
        self._lock = threading.Lock()
        self.stopping = False

    # ---- process helpers

    def child_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.update(
            {
                # Any credentials work: the gateway does not check signatures.
                "AWS_ACCESS_KEY_ID": "neon",
                "AWS_SECRET_ACCESS_KEY": "neon",
                "AWS_REGION": "local",
                "AWS_EC2_METADATA_DISABLED": "true",
                "RUST_LOG": env.get("RUST_LOG", "info"),
                # compute_ctl would otherwise try to export traces to a collector that is not there.
                "OTEL_SDK_DISABLED": "true",
                # compute_ctl calls pg_ctl / initdb next to the postgres binary, and the
                # Postgres client tools need libpq from the same build.
                "PATH": f"{self.cfg.pg_dir / 'bin'}:{env.get('PATH', '')}",
                "LD_LIBRARY_PATH": f"{self.cfg.pg_dir / 'lib'}:{env.get('LD_LIBRARY_PATH', '')}",
            }
        )
        return env

    def _spawn(
        self, name: str, args: list[str], cwd: Path | None = None, extra_env: dict[str, str] | None = None
    ) -> subprocess.Popen:
        log.info("starting %s: %s", name, " ".join(args))
        env = self.child_env()
        env.update(extra_env or {})
        process = subprocess.Popen(args, cwd=cwd, env=env)
        with self._lock:
            self.processes[name] = process
        return process

    def dead_processes(self) -> list[str]:
        with self._lock:
            return [name for name, process in self.processes.items() if process.poll() is not None]

    def stop(self, grace: float = 30) -> None:
        self.stopping = True
        for name in ["compute_ctl", "pageserver", "safekeeper", "storage_broker"]:
            process = self.processes.get(name)
            if process is None or process.poll() is not None:
                continue
            log.info("stopping %s", name)
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                process.kill()

    # ---- startup sequence

    def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._start_broker()
        self._start_safekeeper()
        self._start_pageserver()
        self._bootstrap_tenant()
        self._start_compute()

    def _start_broker(self) -> None:
        self._spawn("storage_broker", [self.cfg.binary("storage_broker"), "--listen-addr=127.0.0.1:50051"])

    def _start_safekeeper(self) -> None:
        datadir = self.root / "safekeeper"
        datadir.mkdir(exist_ok=True)
        self._spawn("safekeeper", safekeeper_args(self.cfg, datadir))
        wait_for("safekeeper", lambda: _http("GET", "http://127.0.0.1:7676/v1/status") is not None)

    def _start_pageserver(self) -> None:
        datadir = self.root / "pageserver"
        datadir.mkdir(exist_ok=True)
        (datadir / "identity.toml").write_text("id=1\n")
        (datadir / "pageserver.toml").write_text(pageserver_toml(self.cfg))
        self._spawn("pageserver", [self.cfg.binary("pageserver"), "-D", str(datadir)])
        wait_for("pageserver", lambda: _http("GET", "http://127.0.0.1:9898/v1/status") is not None)

    def _bootstrap_tenant(self) -> None:
        state = TenantState.load(self.backend, self.cfg.bucket)
        if state is None:
            state = TenantState(tenant_id=secrets.token_hex(16), timeline_id=None, generation=0)
            log.info("no previous state in bucket %s: creating tenant %s", self.cfg.bucket, state.tenant_id)
        else:
            log.info("found tenant %s (generation %d) in bucket %s", state.tenant_id, state.generation, self.cfg.bucket)

        # Persist the new generation before attaching, so a crash in between never reuses it.
        state.generation += 1
        state.save(self.backend, self.cfg.bucket)
        _http(
            "PUT",
            f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}/location_config",
            {"mode": "AttachedSingle", "generation": state.generation, "tenant_conf": {}},
        )
        wait_for(
            "tenant to become active",
            lambda: _http("GET", f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}")["state"]["slug"] == "Active",
            timeout=300,
            abort=self._abort_reason,
        )

        if state.timeline_id is None:
            timeline_id = secrets.token_hex(16)
            _http(
                "POST",
                f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}/timeline/",
                {"new_timeline_id": timeline_id, "pg_version": self.cfg.pg_version},
                timeout=300,
            )
            state.timeline_id = timeline_id
            state.save(self.backend, self.cfg.bucket)
            log.info("created timeline %s", timeline_id)
        else:
            wait_for(
                "timeline to load",
                lambda: _http("GET", f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}/timeline/{state.timeline_id}")
                is not None,
                timeout=300,
                abort=self._abort_reason,
            )
            self._branch_timeline(state)
        self.state = state

    def _branch_timeline(self, state: TenantState) -> None:
        """Continue on a new branch that starts where the old timeline durably ends.

        Two things are gone after a restart: the safekeeper's copy of the timeline
        (it lives on local disk only) and the pageserver's in-memory record of the
        previous WAL record, which Postgres needs before it writes its first new
        record. A branch point is the one place where Postgres may start without
        that record, so every restart branches at the last durable LSN. The old
        timeline stays as the ancestor of the new one.
        """
        detail = _http("GET", f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}/timeline/{state.timeline_id}")
        new_timeline_id = secrets.token_hex(16)
        log.info(
            "branching timeline %s at its last durable LSN %s into %s",
            state.timeline_id,
            detail["last_record_lsn"],
            new_timeline_id,
        )
        _http(
            "POST",
            f"http://127.0.0.1:9898/v1/tenant/{state.tenant_id}/timeline/",
            {"new_timeline_id": new_timeline_id, "ancestor_timeline_id": state.timeline_id},
            timeout=300,
        )
        state.timeline_id = new_timeline_id
        state.save(self.backend, self.cfg.bucket)

    def _abort_reason(self) -> str:
        dead = self.dead_processes()
        return f"process(es) exited: {', '.join(dead)}" if dead else ""

    def _start_compute(self) -> None:
        assert self.state is not None
        datadir = self.root / "compute"
        datadir.mkdir(exist_ok=True)
        spec_path = datadir / "spec.json"
        spec_path.write_text(json.dumps(compute_spec(self.cfg, self.state), indent=2))
        hard_core_limit = resource.getrlimit(resource.RLIMIT_CORE)[1]
        preload = compute_ctl_preload(self.cfg, hard_core_limit)
        if preload:
            log.info("hard core dump limit is %s, preloading %s for compute_ctl", hard_core_limit, preload)
        self._spawn(
            "compute_ctl",
            compute_ctl_args(self.cfg, datadir, spec_path),
            cwd=datadir,
            extra_env={"LD_PRELOAD": preload} if preload else None,
        )
        wait_for("postgres", self.postgres_ready, timeout=300, abort=self._abort_reason)

    def postgres_ready(self) -> bool:
        result = subprocess.run(
            [str(self.cfg.pg_dir / "bin" / "pg_isready"), "-h", "127.0.0.1", "-p", str(self.cfg.pg_port), "-U", self.cfg.pg_user],
            capture_output=True,
            timeout=10,
            check=False,
            env=self.child_env(),
        )
        return result.returncode == 0
