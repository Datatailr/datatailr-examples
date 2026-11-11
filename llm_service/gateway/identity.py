"""Who is calling, and who is an admin."""

from __future__ import annotations

import json
import time
from typing import Callable, Mapping

USER_HEADER = "x-datatailr-user"


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
    The group members are cached for `ttl` seconds. A failed lookup means
    that the user is not an admin.
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
        try:
            self._cached = set(self._members(self._group))
        except Exception:
            return set()
        self._fetched_at = now
        return self._cached


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
