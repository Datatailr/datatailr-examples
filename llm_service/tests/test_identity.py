import sys
import time
import types

import pytest

from llm_service.gateway.identity import (
    AccessCheck,
    AdminCheck,
    acting_user_from_headers,
    check_user_name,
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


def test_a_failed_group_lookup_means_no_members_and_waits_10_seconds():
    calls, now, failing = [], [0.0], [False]

    def members(group):
        calls.append(group)
        if failing[0]:
            raise RuntimeError("kv down")
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("carol") and len(calls) == 1
    now[0], failing[0] = 61.0, True
    assert not check.is_admin("carol") and len(calls) == 2  # the member loses the right at once
    assert check.is_admin("alice")  # the owner needs no lookup
    now[0] = 70.0
    assert not check.is_admin("carol") and len(calls) == 2  # no new lookup within 10 seconds
    for retry in range(1, 6):  # a long outage never brings the old member back
        now[0] = 61.0 + 11.0 * retry
        assert not check.is_admin("carol") and len(calls) == 2 + retry
        now[0] += 5.0
        assert not check.is_admin("carol") and len(calls) == 2 + retry
    now[0], failing[0] = 130.0, False
    assert check.is_admin("carol") and len(calls) == 8
    now[0] = 189.0
    assert check.is_admin("carol") and len(calls) == 8  # a good lookup is cached for 60 seconds


def test_a_failed_access_lookup_means_no_access_and_waits_10_seconds():
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


def test_one_caller_refreshes_the_group_and_the_others_use_the_last_members():
    import threading

    calls, now = [], [0.0]
    started, release = threading.Event(), threading.Event()

    def members(group):
        calls.append(group)
        if len(calls) > 1:
            started.set()
            release.wait(10)
            return ["carol", "dave"]
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("carol") and len(calls) == 1
    now[0] = 61.0  # the cache ended
    first = threading.Thread(target=check.is_admin, args=("carol",))
    first.start()
    assert started.wait(10)
    answers = []
    others = [threading.Thread(target=lambda: answers.append(check.is_admin("carol"))) for _ in range(8)]
    for thread in others:
        thread.start()
    for thread in others:
        thread.join(10)
    assert answers == [True] * 8  # the last members during the refresh
    assert not check.is_admin("dave")
    release.set()
    first.join(10)
    assert len(calls) == 2
    assert check.is_admin("dave") and len(calls) == 2


def test_a_failed_refresh_still_means_no_members_for_the_next_callers():
    import threading

    calls, now = [], [0.0]
    started, release = threading.Event(), threading.Event()

    def members(group):
        calls.append(group)
        if len(calls) > 1:
            started.set()
            release.wait(10)
            raise RuntimeError("kv down")
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert check.is_admin("carol")
    now[0] = 61.0
    first = threading.Thread(target=check.is_admin, args=("carol",))
    first.start()
    assert started.wait(10)
    release.set()
    first.join(10)
    assert not check.is_admin("carol") and len(calls) == 2
    now[0] = 70.0
    assert not check.is_admin("carol") and len(calls) == 2  # the retry waits 10 seconds
    now[0] = 72.0
    assert not check.is_admin("carol") and len(calls) == 3


def test_check_user_name_strips_and_names_each_problem():
    assert check_user_name("  carol ") == "carol"
    assert check_user_name("x" * 256) == "x" * 256
    for name, problem in [
        ("   ", "the user name is empty"),
        ("x" * 257, "the user name is longer than 256 characters"),
        ("a\tb", "the user name has a control character"),
        ("a\x7fb", "the user name has a control character"),
    ]:
        with pytest.raises(ValueError, match=problem):
            check_user_name(name)


def test_the_acting_user_header_uses_the_same_rules_with_its_own_words():
    with pytest.raises(ValueError, match="the acting user name is longer than 256 characters"):
        acting_user_from_headers({"X-LLM-Acting-User": "x" * 257})
    with pytest.raises(ValueError, match="the acting user name has a control character"):
        acting_user_from_headers({"X-LLM-Acting-User": "a\x00b"})


def _first_lookup_race(fail):
    """Start a first lookup that waits, then 8 callers. Return the lookups, the answers and the first answer."""
    import threading

    calls = []
    started, release = threading.Event(), threading.Event()

    def members(group):
        calls.append(group)
        started.set()
        release.wait(10)
        if fail:
            raise RuntimeError("kv down")
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: 0.0)
    first_answer = []
    first = threading.Thread(target=lambda: first_answer.append(check.is_admin("carol")))
    first.start()
    assert started.wait(10)
    answers = []
    others = [threading.Thread(target=lambda: answers.append(check.is_admin("carol"))) for _ in range(8)]
    for thread in others:
        thread.start()
    time.sleep(0.1)
    assert answers == []  # the callers wait for the first lookup
    release.set()
    for thread in [first, *others]:
        thread.join(10)
    return calls, answers, first_answer


def test_the_callers_wait_for_the_first_lookup_and_get_its_members():
    calls, answers, first = _first_lookup_race(fail=False)
    assert len(calls) == 1
    assert first == [True] and answers == [True] * 8


def test_the_callers_wake_when_the_first_lookup_fails_and_get_no_members():
    calls, answers, first = _first_lookup_race(fail=True)
    assert len(calls) == 1
    assert first == [False] and answers == [False] * 8


def test_a_refresh_after_a_failure_gives_the_other_callers_no_members_at_once():
    import threading

    calls, now = [], [0.0]
    started, release = threading.Event(), threading.Event()

    def members(group):
        calls.append(group)
        if len(calls) == 1:
            raise RuntimeError("kv down")
        started.set()
        release.wait(10)
        return ["carol"]

    check = AdminCheck("alice", "llm-admins", members, clock=lambda: now[0])
    assert not check.is_admin("carol")
    now[0] = 11.0
    first = threading.Thread(target=check.is_admin, args=("carol",))
    first.start()
    assert started.wait(10)
    assert not check.is_admin("carol")  # no wait, no members
    release.set()
    first.join(10)
    assert check.is_admin("carol") and len(calls) == 2
