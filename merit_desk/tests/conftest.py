from types import SimpleNamespace

import pytest
from datatailr import Workflow
from datatailr.scheduler import base
from datatailr.user import User

from merit_desk.admin import setup_demo
from merit_desk.common.store import MemoryStore


@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def offline_sdk(monkeypatch):
    """The real Workflow. Only the two questions it asks the platform get local answers."""
    me = User(name="tester", first_name="Test", last_name="User", email="t@example.com", id=1,
              groups=[])
    monkeypatch.setattr(User, "signed_user", staticmethod(lambda: me))  # run_as and the ACL
    monkeypatch.setattr(base, "Budget", lambda name: SimpleNamespace(name=name, id=42))

    def contact_the_platform(*args, **kwargs):
        raise AssertionError("building the workflow must not run or save it")

    for name in ("run", "save", "start", "instant_run"):
        monkeypatch.setattr(Workflow, name, contact_the_platform)


@pytest.fixture
def cluster(monkeypatch):
    """A fake platform that holds the groups, budgets and variables the setup changes."""
    state = SimpleNamespace(groups=set(), budgets={}, variables={}, group_adds=0)

    class FakeGroup:
        @staticmethod
        def exists(name):
            return name in state.groups

        @staticmethod
        def add(name):
            state.group_adds += 1
            state.groups.add(name)
            return object()

    class FakeBudget:
        @staticmethod
        def get(name):
            if name not in state.budgets:
                raise ValueError(f"Budget '{name}' does not exist.")
            return state.budgets[name]

        @staticmethod
        def add(name, amount, *, prevent_overflow=False, acl=None):
            state.budgets[name] = (amount, prevent_overflow)

    class FakeKV:
        def get(self, key):
            if key not in state.variables:
                raise KeyError(key)  # the platform may raise for a missing key
            return state.variables[key]

        def put(self, key, value):
            state.variables[key] = value

    monkeypatch.setattr(setup_demo, "Group", FakeGroup)
    monkeypatch.setattr(setup_demo, "Budget", FakeBudget)
    monkeypatch.setattr(setup_demo, "KV", FakeKV)
    return state
