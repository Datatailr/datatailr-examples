import json
import logging

import pytest

from llm_service.gateway.audit import AuditLog
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.gateway.usage import UsageTotals
from llm_service.tests.fakes import FlakyBlobStore

D1 = "2026-11-09"
D2 = "2026-11-10"
T1 = "2026-11-09T08:00:00.000Z"
T2 = "2026-11-10T09:00:00.000Z"
T3 = "2026-11-10T10:00:00.000Z"
ROOT = "root"


def rec(user, ts, model="m1", prompt=10, completion=5, status=200, endpoint="/v1/chat/completions"):
    total = None if prompt is None or completion is None else prompt + completion
    return {
        "ts": ts,
        "request_id": f"{user}-{ts}",
        "user": user,
        "endpoint": endpoint,
        "model": model,
        "stream": False,
        "prompt": {},
        "status": status,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "duration_ms": 1,
    }


def make(today=D2, store=None):
    store = store or MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    state = {"today": today}
    usage = UsageTotals(store, ROOT, audit, lambda: state["today"])
    audit.listeners.append(usage.record)
    return store, audit, usage, state


def seed(audit):
    audit.add(rec("alice", T2, prompt=10, completion=5))
    audit.add(rec("alice", T3, prompt=20, completion=5, status=500))
    audit.add(rec("bob", T1, model="m2", prompt=1, completion=1))


def test_summary_by_user():
    _, audit, usage, _ = make()
    seed(audit)
    rows = usage.summary(None, None, None, None)
    assert rows[0] == {
        "user": "alice",
        "requests": 2,
        "failed_requests": 1,
        "prompt_tokens": 30,
        "completion_tokens": 10,
        "total_tokens": 40,
        "last_request": T3,
    }
    assert rows[1]["user"] == "bob"
    assert rows[1]["total_tokens"] == 2


def test_group_by_model_and_day_and_filters():
    _, audit, usage, _ = make()
    seed(audit)
    by_model = usage.summary(None, None, None, None, group_by="model")
    assert [r["model"] for r in by_model] == ["m1", "m2"]
    assert by_model[0]["requests"] == 2
    by_day = usage.summary(None, None, None, None, group_by="day")
    assert [r["day"] for r in by_day] == [D2, D1]
    assert by_day[0]["total_tokens"] == 40
    assert usage.summary(D2, None, None, None)[0]["user"] == "alice"
    assert [r["user"] for r in usage.summary(D2, None, None, None)] == ["alice"]
    assert [r["user"] for r in usage.summary(None, D1, None, None)] == ["bob"]
    assert [r["user"] for r in usage.summary(D1, D2, "bob", None)] == ["bob"]
    assert [r["user"] for r in usage.summary(None, None, None, "m2")] == ["bob"]
    assert usage.summary(None, None, "carol", None) == []


def test_ties_sort_by_key():
    _, audit, usage, _ = make()
    audit.add(rec("zed", T2))
    audit.add(rec("amy", T2))
    assert [r["user"] for r in usage.summary(None, None, None, None)] == ["amy", "zed"]


def test_group_by_invalid():
    _, _, usage, _ = make()
    with pytest.raises(ValueError):
        usage.summary(None, None, None, None, group_by="hour")


def test_null_tokens_and_admin_events_ignored():
    _, audit, usage, _ = make()
    audit.add(rec("alice", T2, prompt=None, completion=None))
    audit.add(rec("alice", T3, prompt=3, completion=4))
    audit.add(rec("admin", T3, endpoint="/admin/models/switch"))
    rows = usage.summary(None, None, None, None)
    assert len(rows) == 1
    assert rows[0]["requests"] == 2
    assert rows[0]["prompt_tokens"] == 3
    assert rows[0]["completion_tokens"] == 4
    assert rows[0]["total_tokens"] == 7


def test_user_detail_shape():
    _, audit, usage, _ = make()
    seed(audit)
    detail = usage.user_detail("alice", None, None)
    assert detail["user"] == "alice"
    assert detail["totals"]["requests"] == 2
    assert detail["totals"]["user"] == "alice"
    assert [r["model"] for r in detail["by_model"]] == ["m1"]
    assert [r["day"] for r in detail["by_day"]] == [D2]
    assert detail["by_day"][0]["total_tokens"] == 40


def test_user_detail_unknown_user():
    _, _, usage, _ = make()
    detail = usage.user_detail("nobody", None, None)
    assert detail["totals"] == {
        "user": "nobody",
        "requests": 0,
        "failed_requests": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "last_request": None,
    }
    assert detail["by_model"] == []
    assert detail["by_day"] == []


def test_load_uses_daily_file_and_rebuilds_missing_day():
    store = MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    audit.add(rec("alice", T1))  # day 1: one audit record
    audit.add(rec("bob", "2026-11-08T08:00:00.000Z"))  # day 0: no daily file
    audit.add(rec("carol", T2))  # today
    audit.flush()
    day1 = {
        "day": D1,
        "rows": [
            {
                "user": "alice",
                "model": "m1",
                "requests": 99,
                "failed_requests": 0,
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
                "last_request": T1,
            }
        ],
    }
    store.put(f"{ROOT}/usage/{D1}.json", json.dumps(day1))
    usage = UsageTotals(store, ROOT, audit, lambda: D2)
    usage.load()
    rows = {r["user"]: r for r in usage.summary(None, None, None, None)}
    assert rows["alice"]["requests"] == 99
    assert rows["bob"]["requests"] == 1
    assert rows["carol"]["requests"] == 1
    assert store.exists(f"{ROOT}/usage/2026-11-08.json")
    assert not store.exists(f"{ROOT}/usage/{D2}.json")


def test_load_treats_invalid_daily_file_as_missing(caplog):
    store = MemoryBlobStore()
    audit = AuditLog(store, ROOT)
    audit.add(rec("alice", T1))
    audit.flush()
    store.put(f"{ROOT}/usage/{D1}.json", "{not json")
    usage = UsageTotals(store, ROOT, audit, lambda: D2)
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        usage.load()
    assert usage.summary(None, None, None, None)[0]["requests"] == 1
    assert any("llm_service.usage" == r.name for r in caplog.records)
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["day"] == D1


def test_rollover_writes_daily_file():
    store, audit, usage, state = make(today=D1)
    audit.add(rec("alice", T1))
    assert not store.exists(f"{ROOT}/usage/{D1}.json")
    state["today"] = D2
    audit.add(rec("alice", T2))
    saved = json.loads(store.get(f"{ROOT}/usage/{D1}.json"))
    assert saved["day"] == D1
    assert saved["rows"] == [
        {
            "user": "alice",
            "model": "m1",
            "requests": 1,
            "failed_requests": 0,
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "last_request": T1,
        }
    ]
    assert not store.exists(f"{ROOT}/usage/{D2}.json")


def test_failed_daily_write_is_retried_and_never_raises(caplog):
    store = FlakyBlobStore()
    audit = AuditLog(store, ROOT)
    state = {"today": D1}
    usage = UsageTotals(store, ROOT, audit, lambda: state["today"])
    audit.listeners.append(usage.record)
    audit.add(rec("alice", T1))
    store.fail_puts = True
    state["today"] = D2
    with caplog.at_level(logging.WARNING, logger="llm_service.usage"):
        audit.add(rec("alice", T2))  # must not raise
    assert any(r.name == "llm_service.usage" for r in caplog.records)
    assert not store.exists(f"{ROOT}/usage/{D1}.json")
    store.fail_puts = False
    usage.close_days()
    assert store.exists(f"{ROOT}/usage/{D1}.json")


def test_late_record_for_finished_day_rewrites_file():
    store, audit, usage, state = make(today=D2)
    audit.add(rec("alice", T1))
    usage.close_days()
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["rows"][0]["requests"] == 1
    audit.add(rec("alice", T1))
    usage.close_days()
    assert json.loads(store.get(f"{ROOT}/usage/{D1}.json"))["rows"][0]["requests"] == 2
