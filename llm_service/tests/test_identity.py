import sys
import types

from llm_service.gateway.identity import (
    AdminCheck,
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
