"""Shared pieces of the access control scheme: the role secret and role passwords.

The service creates one Postgres login role per platform user. Its password is
derived from a secret that the service and the explorer share, so the explorer
can connect as the platform user who is using it without storing a password
per user anywhere.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os

ROLE_SECRET_VAR = "NEON_ROLE_SECRET"
JOB_ENV_PREFIX = "DATATAILR_JOB_ENV_"

# Roles that belong to Postgres or to Neon. The sync never creates or changes these.
RESERVED_ROLES = frozenset({"cloud_admin", "neon_superuser", "postgres", "public"})


def env_value(name: str, default: str | None = None) -> str | None:
    """Read a job variable; the platform stores ``env_vars`` with a ``DATATAILR_JOB_ENV_`` prefix."""
    return os.environ.get(JOB_ENV_PREFIX + name, os.environ.get(name, default))


def derive_role_secret(pg_password: str) -> str:
    """The default role secret, derived from the superuser password.

    The explorer gets the secret but not the password, so knowing the secret
    does not give away ``cloud_admin``.
    """
    return hashlib.sha256(f"neon-postgres-role-secret:{pg_password}".encode()).hexdigest()


def role_secret() -> str | None:
    """The role secret of this job, or None when the deploy did not set one."""
    value = (env_value(ROLE_SECRET_VAR) or "").strip()
    return value or None


def role_password(secret: str, username: str) -> str:
    """The password of the Postgres role that stands for platform user ``username``."""
    digest = hmac.new(secret.encode(), f"neon-postgres-role:{username}".encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def is_reserved_role(name: str) -> bool:
    return name in RESERVED_ROLES or name.startswith("pg_")


def main(argv: list[str] | None = None) -> int:
    """Print the role password of a platform user: ``python -m neon_postgres.access alice``.

    The secret comes from ``NEON_ROLE_SECRET`` in the environment.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Print the Postgres password of a platform user's role.")
    parser.add_argument("user", help="platform user name")
    args = parser.parse_args(argv)
    secret = role_secret()
    if secret is None:
        print(f"{ROLE_SECRET_VAR} is not set", file=sys.stderr)
        return 2
    print(role_password(secret, args.user))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
