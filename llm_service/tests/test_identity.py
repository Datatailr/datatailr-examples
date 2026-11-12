import sys
import types

import pytest

from llm_service.gateway.identity import (
    AccessCheck,
    AdminCheck,
    acting_user_from_headers,
    platform_group_members,
    user_from_headers,
)


def test_user_from_headers():
    assert user_from_headers({"x-datatailr-user": '{"name": "alice", "signature": "s"}'}) == "alice"
    assert user_from_headers({"x-datatailr-user": "bob"}) == "bob"
    assert user_from_headers({}) is None
    assert user_from_headers({"x-datatailr-user": "{broken"}) is None
    assert user_from_headers({"x-datatailr-user": '{"name": ""}'}) is None


def test_user_from_headers_is_case_insensitive_with_starlette_headers():
    from starlette.datastructures import Headers

    assert user_from_headers(Headers({"X-Datatailr-User": "carol"})) == "carol"


def test_acting_user_from_headers():
    assert acting_user_from_headers({"X-LLM-Acting-User": "  carol "}) == "carol"
    assert acting_user_from_headers({"X-LLM-Acting-User": "   "}) is None
    assert acting_user_from_headers({}) is None
    assert acting_user_from_headers({"X-LLM-Acting-User": "x" * 256}) == "x" * 256


@pytest.mark.parametrize("name", ["x" * 257, "a\nb", "a\x00b", "a\tb", "a\x7fb"])
def test_acting_user_from_headers_rejects_bad_names(name):
    with pytest.raises(ValueError):
        acting_user_from_headers({"X-LLM-Acting-User": name})


def test_acting_user_from_headers_is_case_insensitive_with_starlette_headers():
    from starlette.datastructures import Headers

    assert acting_user_from_headers(Headers({"x-llm-acting-user": "carol"})) == "carol"


def test_admin_owner_and_group_cache():
    calls, now = [], [0.0]

    def members(g):
        calls.append(g)
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("alice") and check.is_admin("carol") and not check.is_admin("dave")
    assert calls == ["llm-admins"]  # owner needs no lookup, group fetched once
    now[0] = 61
    check.is_admin("carol")
    assert len(calls) == 2


def test_admin_without_group_and_lookup_error():
    assert not AdminCheck("alice", None, lambda g: 1 / 0).is_admin("bob")

    def boom(g):
        raise RuntimeError("kv down")

    assert not AdminCheck("alice", "g", boom).is_admin("bob")  # an error means not admin
    assert not AdminCheck("", None, lambda g: []).is_admin("")  # empty owner matches nobody


def test_platform_group_members_returns_user_names(monkeypatch):
    # In the SDK, Group.members is a property that holds numeric user ids.
    names = {1: "alice", 2: "bob"}
    fake = types.ModuleType("datatailr")
    asked = []

    class FakeGroup:
        @staticmethod
        def get(name):
            asked.append(name)
            return types.SimpleNamespace(members=[1, 2, 3])

    class FakeUser:
        @staticmethod
        def get(user_id):
            if user_id not in names:
                raise ValueError("not found")
            return types.SimpleNamespace(name=names[user_id])

    fake.Group = FakeGroup
    fake.User = FakeUser
    monkeypatch.setitem(sys.modules, "datatailr", fake)
    assert platform_group_members("llm-admins") == ["alice", "bob"]
    assert asked == ["llm-admins"]


def test_access_for_admin_member_and_others():
    admin = AdminCheck("alice", None, lambda g: [])
    check = AccessCheck(admin, "team", lambda g: ["bob"])
    assert check.has_access("alice")
    assert check.has_access("bob")
    assert not check.has_access("carol")
    assert not check.has_access("")


def test_access_without_group_is_for_admins_only():
    asked = []

    def members(group):
        asked.append(group)
        return ["bob"]

    check = AccessCheck(AdminCheck("alice", None, lambda g: []), None, members)
    assert check.has_access("alice")
    assert not check.has_access("bob")
    assert asked == []


def test_access_failed_lookup_means_no_access():
    def boom(group):
        raise RuntimeError("kv down")

    check = AccessCheck(AdminCheck("alice", None, lambda g: []), "team", boom)
    assert not check.has_access("bob")
    assert check.has_access("alice")


def test_access_group_members_are_cached_for_60_seconds():
    calls, now = [], [0.0]

    def members(group):
        calls.append(group)
        return ["bob"]

    check = AccessCheck(AdminCheck("alice", None, lambda g: []), "team", members, clock=lambda: now[0])
    assert check.has_access("bob") and check.has_access("bob") and not check.has_access("carol")
    assert calls == ["team"]
    now[0] = 59
    check.has_access("bob")
    assert len(calls) == 1
    now[0] = 61
    check.has_access("bob")
    assert len(calls) == 2


def test_a_failed_group_lookup_is_cached_for_10_seconds_and_keeps_the_last_members():
    calls, now, failing = [], [0.0], [False]

    def members(group):
        calls.append(group)
        if failing[0]:
            raise RuntimeError("kv down")
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("carol") and len(calls) == 1
    now[0], failing[0] = 61.0, True
    assert check.is_admin("carol") and len(calls) == 2  # the last known members stay in use
    now[0] = 70.0
    assert check.is_admin("carol") and len(calls) == 2  # no new lookup within 10 seconds
    now[0] = 72.0
    assert check.is_admin("carol") and len(calls) == 3
    now[0], failing[0] = 83.0, False
    assert check.is_admin("carol") and len(calls) == 4


def test_a_failed_first_lookup_means_no_members_for_10_seconds():
    calls, now = [], [0.0]

    def boom(group):
        calls.append(group)
        raise RuntimeError("kv down")

    access = AccessCheck(AdminCheck("alice", None, boom), "team", boom, clock=lambda: now[0])
    assert not access.has_access("bob") and calls == ["team"]
    now[0] = 9.0
    assert not access.has_access("bob") and calls == ["team"]
    now[0] = 11.0
    assert not access.has_access("bob") and calls == ["team", "team"]
