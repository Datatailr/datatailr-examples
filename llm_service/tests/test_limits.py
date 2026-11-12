"""Tests for the limits file and the lookup of the limits of a user."""

from __future__ import annotations

import json
import logging

import pytest

from llm_service.gateway.limits import (
    LIMIT_FIELDS,
    MAX_LIMIT,
    InvalidLimit,
    LimitStore,
    StorageUnavailable,
    check_limits,
)
from llm_service.gateway.storage import MemoryBlobStore

ROOT = "llm"
PATH = "llm/limits.json"
FULL = {"requests_per_minute": 60, "tokens_per_day": 2000, "parallel_requests": 4}
NONE = {name: None for name in LIMIT_FIELDS}


class FailingPut(MemoryBlobStore):
    def put(self, path, data):
        raise OSError("disk full")


def make(file=None, store=None):
    store = store or MemoryBlobStore()
    if file is not None:
        store.put(PATH, file if isinstance(file, (str, bytes)) else json.dumps(file))
    limits = LimitStore(store, ROOT)
    limits.load()
    return limits, store


def test_check_full_and_partial():
    assert check_limits(FULL, partial=False) == FULL
    assert check_limits(dict(NONE), partial=False) == NONE
    assert check_limits({"tokens_per_day": None}, partial=True) == {"tokens_per_day": None}
    assert check_limits({"parallel_requests": MAX_LIMIT}, partial=True) == {
        "parallel_requests": MAX_LIMIT
    }


@pytest.mark.parametrize(
    "value, partial",
    [
        ({**FULL, "extra": 1}, False),
        ({"requests_per_minute": 1, "tokens_per_day": 1}, False),
        ({}, True),
        ({}, False),
        ({"requests_per_minute": 0}, True),
        ({"requests_per_minute": -3}, True),
        ({"requests_per_minute": MAX_LIMIT + 1}, True),
        ({"requests_per_minute": 1.5}, True),
        ({"requests_per_minute": 2.0}, True),
        ({"requests_per_minute": "5"}, True),
        ({"requests_per_minute": True}, True),
        ({"requests_per_minute": [1]}, True),
        ({"unknown": 1}, True),
        ([1, 2], True),
        ("x", False),
        (None, True),
    ],
)
def test_check_rejects(value, partial):
    with pytest.raises(InvalidLimit) as err:
        check_limits(value, partial=partial)
    assert str(err.value)


def test_load_missing_file():
    limits, _ = make()
    assert limits.default() is None
    assert limits.users() == {}
    assert limits.limits_for("alice", False) == (NONE, "none")


def test_load_good_file():
    limits, _ = make({"default": FULL, "users": {"bob": {"tokens_per_day": None}}})
    assert limits.default() == FULL
    assert limits.users() == {"bob": {"tokens_per_day": None}}


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        b"\xff\xfe",
        "[]",
        json.dumps({"default": {"requests_per_minute": 0, **{k: 1 for k in LIMIT_FIELDS[1:]}}, "users": {}}),
        json.dumps({"default": {"requests_per_minute": 5}, "users": {}}),
        json.dumps({"default": None, "users": {"bob": {"tokens_per_day": "x"}}}),
        json.dumps({"default": None, "users": {"bob": {}}}),
        json.dumps({"default": None, "users": []}),
        json.dumps({"default": None, "users": {}, "other": 1}),
    ],
)
def test_load_broken_file(content, caplog):
    with caplog.at_level(logging.ERROR):
        limits, _ = make(content)
    assert limits.default() is None
    assert limits.users() == {}
    assert limits.limits_for("alice", False) == (NONE, "none")
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1


def test_load_replaces_old_state():
    limits, store = make({"default": FULL, "users": {}})
    store.delete(PATH)
    limits.load()
    assert limits.default() is None


def test_set_default_persists_and_returns_old():
    limits, store = make()
    assert limits.set_default(FULL) is None
    assert limits.set_default(dict(NONE)) == FULL
    assert json.loads(store.get(PATH)) == {"default": NONE, "users": {}}
    other = LimitStore(store, ROOT)
    other.load()
    assert other.default() == NONE


def test_set_user_persists_and_returns_old():
    limits, store = make()
    assert limits.set_user("bob", {"tokens_per_day": 5}) is None
    assert limits.set_user("bob", {"parallel_requests": 2}) == {"tokens_per_day": 5}
    assert json.loads(store.get(PATH))["users"] == {"bob": {"parallel_requests": 2}}
    other = LimitStore(store, ROOT)
    other.load()
    assert other.users() == {"bob": {"parallel_requests": 2}}


def test_remove_user():
    limits, store = make({"default": None, "users": {"bob": {"tokens_per_day": 5}}})
    assert limits.remove_user("bob") == {"tokens_per_day": 5}
    assert json.loads(store.get(PATH))["users"] == {}
    with pytest.raises(KeyError):
        limits.remove_user("bob")
    with pytest.raises(KeyError):
        limits.remove_user("carol")


def test_writes_validate():
    limits, store = make()
    with pytest.raises(InvalidLimit):
        limits.set_default({"requests_per_minute": 1})
    with pytest.raises(InvalidLimit):
        limits.set_user("bob", {})
    assert limits.default() is None
    assert limits.users() == {}
    assert not store.exists(PATH)


def test_failed_write_changes_nothing():
    limits = LimitStore(FailingPut(), ROOT)
    limits.load()
    with pytest.raises(StorageUnavailable):
        limits.set_default(FULL)
    with pytest.raises(StorageUnavailable):
        limits.set_user("bob", {"tokens_per_day": 1})
    assert limits.default() is None
    assert limits.users() == {}
    store = FailingPut()
    MemoryBlobStore.put(store, PATH, json.dumps({"default": FULL, "users": {"bob": {"tokens_per_day": 1}}}))
    limits = LimitStore(store, ROOT)
    limits.load()
    with pytest.raises(StorageUnavailable):
        limits.set_default(dict(NONE))
    with pytest.raises(StorageUnavailable):
        limits.set_user("bob", {"tokens_per_day": 9})
    with pytest.raises(StorageUnavailable):
        limits.remove_user("bob")
    assert limits.default() == FULL
    assert limits.users() == {"bob": {"tokens_per_day": 1}}


def test_getters_return_copies():
    limits, _ = make({"default": FULL, "users": {"bob": {"tokens_per_day": 1}}})
    limits.default()["tokens_per_day"] = 99
    limits.users()["bob"]["tokens_per_day"] = 99
    limits.users()["carol"] = {}
    assert limits.default() == FULL
    assert limits.users() == {"bob": {"tokens_per_day": 1}}


def test_setters_copy_input():
    limits, _ = make()
    entry = {"tokens_per_day": 1}
    limits.set_user("bob", entry)
    entry["tokens_per_day"] = 2
    assert limits.users()["bob"] == {"tokens_per_day": 1}


def test_limits_for_sources():
    limits, _ = make(
        {"default": FULL, "users": {"bob": {"requests_per_minute": 1, "tokens_per_day": 2, "parallel_requests": 3}}}
    )
    assert limits.limits_for("bob", False) == (
        {"requests_per_minute": 1, "tokens_per_day": 2, "parallel_requests": 3},
        "user",
    )
    assert limits.limits_for("alice", True) == (NONE, "admin")
    assert limits.limits_for("alice", False) == (FULL, "default")


def test_limits_for_none_without_default():
    limits, _ = make({"default": None, "users": {}})
    assert limits.limits_for("alice", False) == (NONE, "none")


def test_entry_with_one_field_takes_default():
    limits, _ = make({"default": FULL, "users": {"carol": {"requests_per_minute": 10}}})
    assert limits.limits_for("carol", False) == (
        {"requests_per_minute": 10, "tokens_per_day": 2000, "parallel_requests": 4},
        "user",
    )


def test_entry_with_one_field_and_no_default():
    limits, _ = make({"default": None, "users": {"carol": {"requests_per_minute": 10}}})
    assert limits.limits_for("carol", False) == (
        {"requests_per_minute": 10, "tokens_per_day": None, "parallel_requests": None},
        "user",
    )


def test_entry_with_null_field_beats_default():
    limits, _ = make({"default": FULL, "users": {"bob": {"tokens_per_day": None}}})
    assert limits.limits_for("bob", False) == (
        {"requests_per_minute": 60, "tokens_per_day": None, "parallel_requests": 4},
        "user",
    )


def test_admin_with_entry_uses_entry():
    limits, _ = make({"default": FULL, "users": {"dave": {"parallel_requests": 1}}})
    assert limits.limits_for("dave", True) == (
        {"requests_per_minute": 60, "tokens_per_day": 2000, "parallel_requests": 1},
        "user",
    )
