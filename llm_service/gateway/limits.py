"""Limits for each user: the limits file and the lookup of the limits of one user."""

from __future__ import annotations

import copy
import json
import logging
import threading

from .storage import BlobStore

logger = logging.getLogger(__name__)

LIMIT_FIELDS = ("requests_per_minute", "tokens_per_day", "parallel_requests")
MAX_LIMIT = 1_000_000_000


class InvalidLimit(ValueError):
    """A limits value is not valid. The message is the text for the user."""


class StorageUnavailable(Exception):
    """The limits file cannot be written."""


def check_limits(value: object, partial: bool) -> dict:
    """Return `value` if it is a valid limits object. Raise `InvalidLimit` if not.

    A full object has exactly the three fields. A partial object has one to three.
    Each value is None or a whole number from 1 to `MAX_LIMIT`.
    """
    if not isinstance(value, dict):
        raise InvalidLimit("The limits must be a JSON object.")
    unknown = sorted(str(key) for key in value if key not in LIMIT_FIELDS)
    if unknown:
        raise InvalidLimit(f"Unknown limit field: {', '.join(unknown)}.")
    if partial and not value:
        raise InvalidLimit("Give at least one of: " + ", ".join(LIMIT_FIELDS) + ".")
    if not partial:
        missing = [name for name in LIMIT_FIELDS if name not in value]
        if missing:
            raise InvalidLimit(f"Missing limit field: {', '.join(missing)}.")
    for name, number in value.items():
        if number is None:
            continue
        if isinstance(number, bool) or not isinstance(number, int):
            raise InvalidLimit(f"The limit {name} must be null or a whole number.")
        if not 1 <= number <= MAX_LIMIT:
            raise InvalidLimit(f"The limit {name} must be from 1 to {MAX_LIMIT}.")
    return value


class LimitStore:
    """The limits of the service, in memory and in `<blob_root>/limits.json`."""

    def __init__(self, store: BlobStore, blob_root: str) -> None:
        self._store = store
        self._path = f"{blob_root.strip('/')}/limits.json"
        self._write_lock = threading.Lock()  # serializes `load` and the writers
        self._lock = threading.Lock()  # guards `_default` and `_users`, never held for a blob call
        self._default: dict | None = None
        self._users: dict[str, dict] = {}
        self._loaded = False  # False after a read that failed for a reason other than content

    def load(self) -> None:
        """Read the file. A missing or broken file means no limits.

        Another read error also means no limits and an error log. The next write then reads again.
        """
        with self._write_lock:
            self._load()

    def _load(self) -> None:
        loaded = True
        try:
            raw = self._store.get(self._path)
        except KeyError:
            default, users = None, {}
        except Exception as exc:  # noqa: BLE001
            logger.error("cannot read %s, no limits apply: %s", self._path, exc)
            default, users, loaded = None, {}, False
        else:
            try:
                default, users = self._parse(raw)
            except (ValueError, TypeError, RecursionError) as exc:
                logger.error("ignoring broken %s, no limits apply: %s", self._path, exc)
                default, users = None, {}
        with self._lock:
            self._default, self._users = default, users
            self._loaded = loaded

    @staticmethod
    def _parse(raw: bytes) -> tuple[dict | None, dict[str, dict]]:
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) - {"default", "users"}:
            raise ValueError("the file must be an object with default and users")
        default = data.get("default")
        if default is not None:
            default = check_limits(default, partial=False)
        users = data.get("users", {})
        if not isinstance(users, dict):
            raise ValueError("users must be an object")
        for entry in users.values():
            check_limits(entry, partial=True)
        return default, users

    def default(self) -> dict | None:
        with self._lock:
            return copy.deepcopy(self._default)

    def users(self) -> dict[str, dict]:
        with self._lock:
            return copy.deepcopy(self._users)

    def _write(self, default: dict | None, users: dict[str, dict]) -> None:
        body = json.dumps({"default": default, "users": users}, indent=2)
        try:
            self._store.put(self._path, body)
        except Exception as exc:  # noqa: BLE001
            logger.error("cannot write %s: %s", self._path, exc)
            raise StorageUnavailable("The limits cannot be saved now.") from exc

    def _prepare_write(self) -> None:
        """Read the file again after a failed read. Call it with the write lock held."""
        if not self._loaded:
            self._load()
        if not self._loaded:
            raise StorageUnavailable("The limits cannot be read now.")

    def set_default(self, limits: dict) -> dict | None:
        """Replace the default. Return the old default."""
        new = copy.deepcopy(check_limits(limits, partial=False))
        with self._write_lock:
            self._prepare_write()
            self._write(new, self._users)
            with self._lock:
                old, self._default = self._default, new
            return old

    def set_user(self, name: str, limits: dict) -> dict | None:
        """Set the entry of one user. Return the old entry."""
        new = copy.deepcopy(check_limits(limits, partial=True))
        with self._write_lock:
            self._prepare_write()
            users = {**self._users, name: new}
            self._write(self._default, users)
            with self._lock:
                old = self._users.get(name)
                self._users = users
            return old

    def remove_user(self, name: str) -> dict:
        """Remove the entry of one user. Return it. Raise `KeyError` without one."""
        with self._write_lock:
            self._prepare_write()
            if name not in self._users:
                raise KeyError(name)
            users = {k: v for k, v in self._users.items() if k != name}
            self._write(self._default, users)
            with self._lock:
                old, self._users = self._users[name], users
            return old

    def limits_for(self, user: str, is_admin: bool) -> tuple[dict, str]:
        """Return the three limits of `user` and where they come from.

        The source is "user", "admin", "default" or "none".
        """
        with self._lock:
            entry = self._users.get(user)
            default = self._default
            base = default or {name: None for name in LIMIT_FIELDS}
            if entry is not None:
                return {name: entry.get(name, base[name]) for name in LIMIT_FIELDS}, "user"
            if is_admin:
                return {name: None for name in LIMIT_FIELDS}, "admin"
            if default is not None:
                return dict(default), "default"
            return dict(base), "none"
