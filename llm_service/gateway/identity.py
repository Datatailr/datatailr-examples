"""Who is calling, and who is an admin."""

from __future__ import annotations

import json
import time
from typing import Callable, Mapping

USER_HEADER = "x-datatailr-user"
FAILED_LOOKUP_SECONDS = 10.0


def user_from_headers(headers: Mapping[str, str]) -> str | None:
    """Return the platform user name from the request headers.

    The header value is either a JSON object with a `name` key or a plain
    name. Return None when the header is missing, broken or empty.
    """
    value = headers.get(USER_HEADER)
    if not value:
        return None
    value = value.strip()
    if value.startswith("{"):
        try:
            name = json.loads(value).get("name")
        except (ValueError, AttributeError):
            return None
        return name if isinstance(name, str) and name else None
    return value or None


ACTING_USER_HEADER = "X-LLM-Acting-User"
MAX_ACTING_USER_LENGTH = 256


def acting_user_from_headers(headers: Mapping[str, str]) -> str | None:
    """Return the acting user name from the request headers.

    Return None when the header is missing or empty. Raise `ValueError` when
    the name is too long or has a control character.
    """
    name = (headers.get(ACTING_USER_HEADER) or "").strip()
    if not name:
        return None
    if len(name) > MAX_ACTING_USER_LENGTH:
        raise ValueError(f"the acting user name is longer than {MAX_ACTING_USER_LENGTH} characters")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        raise ValueError("the acting user name has a control character")
    return name


class AdminCheck:
    """Decide whether a user is an admin.

    The owner is always an admin. Members of the admin group are admins too.
    The group members are cached for `ttl` seconds. After a failed lookup, the
    last known members stay in use for 10 seconds, or no members if there are none.
    """

    def __init__(
        self,
        owner: str,
        group: str | None,
        members: Callable[[str], list[str]],
        ttl: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._owner = owner
        self._group = group
        self._members = members
        self._ttl = ttl
        self._clock = clock
        self._cached: set[str] = set()
        self._fetched_at: float | None = None
        self._failed_at: float | None = None

    def is_admin(self, user: str) -> bool:
        """Return True when the user is the owner or an admin group member."""
        if not user:
            return False
        if user == self._owner:
            return True
        if not self._group:
            return False
        return user in self._group_members()

    def _group_members(self) -> set[str]:
        now = self._clock()
        if self._fetched_at is not None and now - self._fetched_at < self._ttl:
            return self._cached
        # A failed lookup waits 10 seconds, so a slow group service does not get a call for each request.
        if self._failed_at is not None and now - self._failed_at < FAILED_LOOKUP_SECONDS:
            return self._cached
        try:
            self._cached = set(self._members(self._group))
        except Exception:
            self._failed_at = now
            return self._cached
        self._fetched_at = now
        self._failed_at = None
        return self._cached


class AccessCheck:
    """Decide whether a user has access to the service.

    An admin has access. A member of `group` has access too. Without a group, only
    admins have access. The group members are cached for `ttl` seconds. A failed
    lookup keeps the last known members for 10 seconds, or no members if there are none.
    """

    def __init__(
        self,
        admin: AdminCheck,
        group: str | None,
        members: Callable[[str], list[str]],
        ttl: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._admin = admin
        # An AdminCheck without an owner is a cached group lookup.
        self._group = AdminCheck("", group, members, ttl, clock)

    def has_access(self, user: str) -> bool:
        """Return True when the user is an admin or a member of the access group."""
        return self._admin.is_admin(user) or self._group.is_admin(user)


def platform_group_members(group: str) -> list[str]:
    """Return the user names in a platform group.

    The SDK keeps the numeric user ids in `Group.members`, so this function
    looks up each id to get the name. It skips ids that no longer resolve.
    """
    from datatailr import Group, User

    names = []
    for member in Group.get(group).members:
        if isinstance(member, str):
            names.append(member)
            continue
        try:
            name = User.get(member).name
        except ValueError:
            continue
        if name:
            names.append(name)
    return names
