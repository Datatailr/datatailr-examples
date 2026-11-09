"""Mirror platform users and groups into Postgres roles.

The mapping is one Postgres role per platform group and one per platform
user. A group role cannot log in; it is what tables are granted to. A user
role can log in, with a password derived from the shared role secret, and is a
member of the roles of the user's groups. Postgres then enforces the grants:
a user sees exactly the tables that were granted to one of their groups or to
them.

Roles the sync created carry a comment (``datatailr:user`` or
``datatailr:group``). The sync never touches a role without that comment, so a
role that an administrator created by hand keeps its settings, and a platform
user whose name collides with such a role is skipped.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from typing import Callable
from dataclasses import dataclass, field

import psycopg
from psycopg import sql

from neon_postgres.access import is_reserved_role, role_password

log = logging.getLogger("neon_roles")

USER_MARK = "datatailr:user"
GROUP_MARK = "datatailr:group"


@dataclass
class Directory:
    """Platform users and the groups each one belongs to, plus all group names."""

    users: dict[str, set[str]] = field(default_factory=dict)
    groups: set[str] = field(default_factory=set)


@dataclass
class ExistingRole:
    name: str
    login: bool
    comment: str | None
    member_of: set[str] = field(default_factory=set)
    # The stored password hash, when the server lets us read it (superuser only).
    password_hash: str | None = None

    @property
    def mark(self) -> str | None:
        return self.comment if self.comment in (USER_MARK, GROUP_MARK) else None

    def has_password(self, password: str) -> bool:
        """True when the stored hash is the md5 hash of ``password``.

        The compute runs with ``password_encryption = md5``, so a sync whose
        secret did not change makes no statement for an existing user. With
        SCRAM hashes this is always False and the password is set again, which
        is harmless.
        """
        return self.password_hash == "md5" + hashlib.md5((password + self.name).encode()).hexdigest()


def build_directory(users: list[dict], groups: list[dict]) -> Directory:
    """A directory from ``dt user ls`` and ``dt group ls`` style records.

    A user record has ``name``, ``user_id``, ``groups`` (a list of names) and
    ``is_system``; a group record has ``name`` and ``members`` (user ids). Both
    sides are read, because the platform lists ``dtusers``, the group of
    everyone, in the group's members and not in the user's groups. System
    users and ``root`` are left out: nobody logs in to the explorer as them.
    """
    directory = Directory(groups={group["name"] for group in groups})
    by_id: dict[int, str] = {}
    for user in users:
        if user.get("is_system") or user.get("user_id") == 0 or not user.get("name"):
            continue
        directory.users[user["name"]] = {group for group in user.get("groups") or [] if group}
        if user.get("user_id") is not None:
            by_id[int(user["user_id"])] = user["name"]
    for group in groups:
        for member in group.get("members") or []:
            name = by_id.get(int(member))
            if name is not None:
                directory.users[name].add(group["name"])
    return directory


def load_directory() -> Directory:
    """Users and groups from the platform, through the datatailr SDK.

    Raises when the SDK is unavailable or the call fails, so the caller can
    keep the previous state.
    """
    from datatailr import Group, User  # imported here: not needed off-platform

    users = [
        {"name": user.name, "user_id": user.user_id, "groups": list(user.groups), "is_system": user.is_system_user}
        for user in User.ls()
    ]
    groups = [{"name": group.name, "members": list(group.members)} for group in Group.ls()]
    return build_directory(users, groups)


def fetch_existing_roles(conn: psycopg.Connection) -> dict[str, ExistingRole]:
    rows = conn.execute(
        """
        select r.rolname, r.rolcanlogin, d.description,
               coalesce(array_agg(g.rolname) filter (where g.rolname is not null), '{}') as member_of,
               a.rolpassword
        from pg_roles r
        left join pg_authid a on a.oid = r.oid
        left join pg_shdescription d on d.objoid = r.oid and d.classoid = 'pg_authid'::regclass
        left join pg_auth_members m on m.member = r.oid
        left join pg_roles g on g.oid = m.roleid
        group by r.rolname, r.rolcanlogin, d.description, a.rolpassword
        """
    ).fetchall()
    return {
        name: ExistingRole(name, login, comment, set(member_of), password_hash)
        for name, login, comment, member_of, password_hash in rows
    }


def plan_sync(
    directory: Directory, existing: dict[str, ExistingRole], secret: str, skipped: dict[str, str] | None = None
) -> list[sql.Composable]:
    """The statements that bring the roles in line with the directory.

    Group roles are created and never dropped: the grants on them are the
    administrator's work. A user role whose user left the platform loses the
    right to log in and its group memberships, but stays, because it may own
    tables. Users and groups that get no role are reported in ``skipped``
    (name to reason) when a dict is passed, and logged otherwise.
    """
    statements: list[sql.Composable] = []
    reasons: dict[str, str] = {} if skipped is None else skipped

    def skip(name: str, reason: str) -> None:
        reasons[name] = reason
        if skipped is None:
            log.warning("%s skipped: %s", name, reason)

    group_roles: set[str] = set()
    for group in sorted(directory.groups):
        if is_reserved_role(group):
            skip(group, "group has the name of a reserved role")
            continue
        role = existing.get(group)
        if role is None:
            statements.append(sql.SQL("create role {} nologin").format(sql.Identifier(group)))
            statements.append(sql.SQL("comment on role {} is {}").format(sql.Identifier(group), sql.Literal(GROUP_MARK)))
        elif role.mark != GROUP_MARK:
            skip(group, "group has the name of a role the sync did not create")
            continue
        group_roles.add(group)

    managed_users = {name for name, role in existing.items() if role.mark == USER_MARK}
    for user in sorted(directory.users):
        if is_reserved_role(user):
            skip(user, "user has the name of a reserved role")
            continue
        if user in directory.groups:
            skip(user, "user has the same name as a group")
            continue
        role = existing.get(user)
        password = role_password(secret, user)
        if role is None:
            statements.append(
                sql.SQL("create role {} login password {}").format(sql.Identifier(user), sql.Literal(password))
            )
            statements.append(sql.SQL("comment on role {} is {}").format(sql.Identifier(user), sql.Literal(USER_MARK)))
            current: set[str] = set()
        elif role.mark != USER_MARK:
            skip(user, "user has the name of a role the sync did not create")
            continue
        else:
            if not role.login or not role.has_password(password):
                statements.append(
                    sql.SQL("alter role {} login password {}").format(sql.Identifier(user), sql.Literal(password))
                )
            current = role.member_of & group_roles
        managed_users.discard(user)
        wanted = directory.users[user] & group_roles
        for group in sorted(wanted - current):
            statements.append(sql.SQL("grant {} to {}").format(sql.Identifier(group), sql.Identifier(user)))
        for group in sorted(current - wanted):
            statements.append(sql.SQL("revoke {} from {}").format(sql.Identifier(group), sql.Identifier(user)))

    for user in sorted(managed_users):
        role = existing[user]
        if role.login:
            statements.append(sql.SQL("alter role {} nologin").format(sql.Identifier(user)))
        for group in sorted(role.member_of & group_roles):
            statements.append(sql.SQL("revoke {} from {}").format(sql.Identifier(group), sql.Identifier(user)))
    return statements


def sync_roles(conn: psycopg.Connection, directory: Directory, secret: str, skipped: dict[str, str] | None = None) -> int:
    """Apply the plan in one transaction; return the number of statements run."""
    statements = plan_sync(directory, fetch_existing_roles(conn), secret, skipped)
    with conn.transaction():
        for statement in statements:
            conn.execute(statement)
    return len(statements)


class RoleSyncer:
    """Sync roles on a timer, in a background thread, and report the last result."""

    def __init__(self, dsn: str, secret: "str | Callable[[], str | None]", interval: float = 60.0, load=load_directory) -> None:
        self.dsn = dsn
        # A value, or a function that returns the current value: the service
        # passes ``access.role_secret`` so a rotated secret is used at the next sync.
        self.secret = secret
        self.interval = interval
        self.load = load
        self.last_sync: float | None = None
        self.last_error: str | None = None
        self.users = 0
        self.groups = 0
        self.skipped: dict[str, str] = {}
        self._stop = threading.Event()

    def current_secret(self) -> str | None:
        return self.secret() if callable(self.secret) else self.secret

    def sync_once(self) -> None:
        secret = self.current_secret()
        if not secret:
            self.last_error = "no role secret: set NEON_ROLE_SECRET or NEON_ROLE_SECRET_KEY"
            return
        try:
            directory = self.load()
            skipped: dict[str, str] = {}
            with psycopg.connect(self.dsn, connect_timeout=10, autocommit=True) as conn:
                changed = sync_roles(conn, directory, secret, skipped)
            if skipped != self.skipped:  # say it once, not every minute
                for name, reason in sorted(skipped.items()):
                    log.warning("%s gets no Postgres role: %s", name, reason)
            self.skipped = skipped
            self.users, self.groups = len(directory.users), len(directory.groups)
            self.last_sync, self.last_error = time.time(), None
            if changed:
                log.info("synced %d users and %d groups into Postgres roles (%d statements)", self.users, self.groups, changed)
        except Exception as exc:  # keep the previous roles and try again next time
            self.last_error = str(exc)
            log.warning("role sync failed: %s", exc)

    def run(self) -> None:
        while not self._stop.is_set():
            self.sync_once()
            self._stop.wait(self.interval)

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.run, name="role-sync", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict:
        return {
            "last_sync": self.last_sync,
            "last_error": self.last_error,
            "users": self.users,
            "groups": self.groups,
            "skipped": self.skipped,
            "interval_seconds": self.interval,
        }
