"""Shared pieces of the access control scheme: the role secret and role passwords.

The service creates one Postgres login role per platform user. Its password is
derived from a secret that the service and the explorer share, so the explorer
can connect as the platform user who is using it without storing a password
per user anywhere.

The secret lives in the platform's secret store (``dt kv put -s``). The deploy
creates it there when it is missing and tells both jobs its key in
``NEON_ROLE_SECRET_KEY``; the jobs read the value at runtime. ``NEON_ROLE_SECRET``
holds the value itself, for tests and for running off the platform.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets as random_secrets
import threading
import time

log = logging.getLogger("neon_postgres.access")

ROLE_SECRET_VAR = "NEON_ROLE_SECRET"
ROLE_SECRET_KEY_VAR = "NEON_ROLE_SECRET_KEY"
DEFAULT_ROLE_SECRET_KEY = "neon-postgres/role-secret"
JOB_ENV_PREFIX = "DATATAILR_JOB_ENV_"
# How long a job keeps a secret it read from the store before reading it again.
STORE_CACHE_SECONDS = 60.0

# Roles that belong to Postgres or to Neon. The sync never creates or changes these.
RESERVED_ROLES = frozenset({"cloud_admin", "neon_superuser", "postgres", "public"})


def env_value(name: str, default: str | None = None) -> str | None:
    """Read a job variable; the platform stores ``env_vars`` with a ``DATATAILR_JOB_ENV_`` prefix."""
    return os.environ.get(JOB_ENV_PREFIX + name, os.environ.get(name, default))


def normalize_key(key: str) -> str:
    """A key as the KV server stores it: lowercase, without leading slashes.

    The server lowercases every key on put, get and list, so ``Team/Secret``
    and ``team/secret`` are the same entry. Normalising here keeps the name the
    deploy writes equal to the name the jobs read.
    """
    return key.strip().lower().lstrip("/")


def role_secret_key() -> str | None:
    """The store key of the role secret, from ``NEON_ROLE_SECRET_KEY``, or None."""
    value = normalize_key(env_value(ROLE_SECRET_KEY_VAR) or "")
    return value or None


def role_sync_enabled() -> bool:
    """True when the deploy gave this job a secret value or a store key."""
    return bool((env_value(ROLE_SECRET_VAR) or "").strip()) or role_secret_key() is not None


def _kv_client():
    from datatailr.wrapper import dt__Kv  # type: ignore

    return dt__Kv()


def read_stored_secret(key: str) -> str:
    """The value of secret ``key`` in the current environment, through the platform CLI.

    Works inside the platform only: a laptop can write and list secrets of an
    environment but cannot read them.
    """
    from datatailr import Secrets  # type: ignore

    return str(Secrets().get(key))


_cache_lock = threading.Lock()
_cached: tuple[str, str, float] | None = None  # key, value, read time


def forget_role_secret() -> None:
    """Drop the cached store value, so the next call reads the store again."""
    global _cached
    with _cache_lock:
        _cached = None


def role_secret() -> str | None:
    """The role secret of this job: ``NEON_ROLE_SECRET`` itself, else the store value.

    A value read from the store is kept for ``STORE_CACHE_SECONDS``, so a rotated
    secret reaches the job within a minute and the explorer does not call the
    CLI on every request. None when nothing is configured or the store fails;
    the failure is logged.
    """
    global _cached
    value = (env_value(ROLE_SECRET_VAR) or "").strip()
    if value:
        return value
    key = role_secret_key()
    if key is None:
        return None
    with _cache_lock:
        if _cached is not None and _cached[0] == key and time.monotonic() - _cached[2] < STORE_CACHE_SECONDS:
            return _cached[1]
        try:
            secret = read_stored_secret(key).strip()
        except Exception as exc:
            log.warning("cannot read the role secret %r from the secret store: %s", key, exc)
            return None
        if not secret:
            log.warning("the role secret %r in the secret store is empty", key)
            return None
        _cached = (key, secret, time.monotonic())
        return secret


def ensure_role_secret(environment: str, key: str, value: str | None = None, client=None) -> tuple[str, str]:
    """Make sure the secret store of ``environment`` holds the role secret under ``key``.

    Returns the stored key name and what happened: ``"stored"`` when ``value``
    was written, ``"kept"`` when the store already had the key, ``"created"``
    when a new random secret was written. The existing value is never read,
    because the platform does not let a laptop read an environment's secrets.
    """
    client = client or _kv_client()
    key = normalize_key(key)
    if not key:
        raise ValueError("the role secret key is empty")
    if value is not None:
        client.put(key, value, secure=True, environment=environment)
        return key, "stored"
    listing = client.ls(key, secure=True, environment=environment) or {}
    items = listing.get("items", listing) if isinstance(listing, dict) else listing
    if any(normalize_key(str(item.get("key", ""))) == key for item in items or []):
        return key, "kept"
    client.put(key, random_secrets.token_hex(32), secure=True, environment=environment)
    return key, "created"


def role_password(secret: str, username: str) -> str:
    """The password of the Postgres role that stands for platform user ``username``."""
    digest = hmac.new(secret.encode(), f"neon-postgres-role:{username}".encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def is_reserved_role(name: str) -> bool:
    return name in RESERVED_ROLES or name.startswith("pg_")


def main(argv: list[str] | None = None) -> int:
    """Print the role password of a platform user: ``python -m neon_postgres.access alice``.

    The secret is ``NEON_ROLE_SECRET`` when set, else the store entry named by
    ``--key`` (default ``NEON_ROLE_SECRET_KEY``, else ``neon-postgres/role-secret``).
    Reading the store works inside the platform, for example in a workstation.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Print the Postgres password of a platform user's role.")
    parser.add_argument("user", help="platform user name")
    parser.add_argument("--key", help="store key of the role secret", default=None)
    args = parser.parse_args(argv)
    secret = (env_value(ROLE_SECRET_VAR) or "").strip()
    if not secret:
        key = normalize_key(args.key) if args.key else (role_secret_key() or DEFAULT_ROLE_SECRET_KEY)
        try:
            secret = read_stored_secret(key).strip()
        except Exception as exc:
            print(f"cannot read the secret {key!r} from the secret store: {exc}", file=sys.stderr)
            return 2
        if not secret:
            print(f"the secret {key!r} in the secret store is empty", file=sys.stderr)
            return 2
    print(role_password(secret, args.user))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
