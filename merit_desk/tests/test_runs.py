import logging
import re

import pytest

from merit_desk.common.runs import (
    TERMINAL, finish_run, list_run_ids, list_runs, load_run, new_run, run_label, save_run,
)
from merit_desk.common.store import LocalDirStore, MemoryStore


def test_a_run_record_round_trips_and_lists_newest_first():
    store = MemoryStore()
    inputs = {"market": "DE", "tenor": 2, "threshold": 1.5}
    a = new_run("backtest", "Backtest DE M2 t1.5", inputs, "denis")
    save_run(a, store)
    b = new_run("scenario", "Scenario DE -3.0C", {"market": "DE"}, None)
    save_run(b, store)
    assert load_run(a["id"], store)["inputs"] == a["inputs"]
    assert [r["id"] for r in list_runs(store)] == [b["id"], a["id"]]


def test_a_new_record_is_queued_and_its_id_sorts_in_time_order():
    first, second = new_run("job", "x", {}, None), new_run("job", "x", {}, None)
    assert re.fullmatch(r"\d{8}T\d{6}-[0-9a-f]{6}", first["id"])
    assert first["id"] < second["id"]  # also when both are made within one second
    assert first["status"] == "queued"
    assert (first["job_name"], first["result"], first["error"]) == (None, None, None)
    assert first["created"] == first["updated"] and first["created"].endswith("+00:00")


def test_save_run_sets_updated():
    store = MemoryStore()
    record = new_run("job", "x", {}, "denis")
    record["updated"] = "2000-01-01T00:00:00+00:00"
    save_run(record, store)
    assert record["updated"] > "2020"
    assert load_run(record["id"], store)["updated"] == record["updated"]


def test_each_environment_has_its_own_runs(monkeypatch):
    monkeypatch.delenv("DATATAILR_JOB_ENVIRONMENT", raising=False)
    store = MemoryStore()
    record = new_run("job", "x", {}, "denis")
    save_run(record, store)
    assert store.list("merit-desk/dev/runs/") == [f"merit-desk/dev/runs/{record['id']}.json"]
    monkeypatch.setenv("DATATAILR_JOB_ENVIRONMENT", "prod")
    assert load_run(record["id"], store) is None and list_runs(store) == []


def test_load_run_is_none_for_an_unknown_id():
    assert load_run("20261030T101500-000000", MemoryStore()) is None


def test_list_runs_reads_only_the_newest_ones():
    class CountingStore(MemoryStore):
        reads = 0

        def get_text(self, name):
            self.reads += 1
            return super().get_text(name)

    store = CountingStore()
    ids = []
    for _ in range(5):
        record = new_run("job", "x", {}, None)
        save_run(record, store)
        ids.append(record["id"])
    assert [r["id"] for r in list_runs(store, limit=2)] == [ids[4], ids[3]]
    assert store.reads == 2


def test_list_run_ids_gives_the_ids_newest_first_and_reads_no_record():
    class NoReads(MemoryStore):
        def get_text(self, name):
            raise AssertionError("a record was read")

    store = NoReads()
    ids = make_runs(store, 3)
    store.put_text("merit-desk/dev/runs/notes.txt", "not a run")
    assert list_run_ids(store) == ids[::-1]


def make_runs(store, count):
    """Saved runs, oldest first, and their record ids."""
    ids = []
    for _ in range(count):
        record = new_run("job", "x", {}, None)
        save_run(record, store)
        ids.append(record["id"])
    return ids


@pytest.mark.parametrize("text", ['{"id": "x", "label": "SECRET"', "", "[]", '"SECRET"', "null"])
def test_a_record_that_does_not_parse_is_none_and_is_logged_by_id_only(caplog, text):
    store = MemoryStore()
    (run_id,) = make_runs(store, 1)
    store.put_text(f"merit-desk/dev/runs/{run_id}.json", text)
    with caplog.at_level(logging.WARNING, logger="merit_desk.common.runs"):
        assert load_run(run_id, store) is None
    assert run_id in caplog.text and "SECRET" not in caplog.text


def test_a_record_that_is_not_utf_8_is_none_and_is_logged_by_id_only(caplog, tmp_path):
    store = LocalDirStore(tmp_path)
    ids = make_runs(store, 3)
    (tmp_path / f"merit-desk/dev/runs/{ids[1]}.json").write_bytes(b'{"label": "SECRET\xff"}')
    with caplog.at_level(logging.WARNING, logger="merit_desk.common.runs"):
        assert load_run(ids[1], store) is None
    assert ids[1] in caplog.text and "SECRET" not in caplog.text
    assert [r["id"] for r in list_runs(store)] == [ids[2], ids[0]]  # the others still list


def test_a_store_that_cannot_be_read_still_raises():
    class Down(MemoryStore):
        def get_text(self, name):
            raise OSError("the store is down")

    with pytest.raises(OSError):
        load_run("20261030T101500-000000", Down())


def test_list_runs_skips_a_corrupt_record_and_still_fills_the_limit():
    store = MemoryStore()
    ids = make_runs(store, 4)
    store.put_text(f"merit-desk/dev/runs/{ids[3]}.json", '{"id": "x"')  # the newest is damaged
    assert [r["id"] for r in list_runs(store, limit=2)] == [ids[2], ids[1]]
    assert [r["id"] for r in list_runs(store)] == [ids[2], ids[1], ids[0]]


def test_list_runs_skips_a_record_that_vanished_after_the_listing():
    class VanishingStore(MemoryStore):
        def list(self, prefix):  # a name that is listed, but has no file when it is read
            return super().list(prefix) + [f"{prefix}29991231T235959-ffffff.json"]

    store = VanishingStore()
    ids = make_runs(store, 3)
    assert [r["id"] for r in list_runs(store, limit=2)] == [ids[2], ids[1]]


def test_finish_run_sets_the_status():
    store = MemoryStore()
    r = new_run("backtest", "x", {}, None)
    save_run(r, store)
    assert finish_run(r["id"], store, error="boom")["status"] == "failed"


def test_finish_run_keeps_the_result_or_the_error_in_the_stored_record():
    store = MemoryStore()
    ok, bad = new_run("backtest", "x", {}, None), new_run("backtest", "y", {}, None)
    save_run(ok, store)
    save_run(bad, store)
    finish_run(ok["id"], store, result={"pnl": 12.5})
    finish_run(bad["id"], store, error="boom")
    done, failed = load_run(ok["id"], store), load_run(bad["id"], store)
    assert (done["status"], done["result"], done["error"]) == ("done", {"pnl": 12.5}, None)
    assert (failed["status"], failed["result"], failed["error"]) == ("failed", None, "boom")
    assert done["status"] in TERMINAL and failed["status"] in TERMINAL


def test_finish_run_fails_the_run_even_when_the_error_text_is_empty():
    store = MemoryStore()
    r = new_run("backtest", "x", {}, None)
    save_run(r, store)
    assert finish_run(r["id"], store, error="")["status"] == "failed"


def test_finish_run_names_a_record_that_does_not_exist():
    with pytest.raises(LookupError, match="20261030T101500-000000"):
        finish_run("20261030T101500-000000", MemoryStore(), result={})


@pytest.mark.parametrize("kind, inputs, label", [
    ("scenario", {"market": "DE", "shift_c": -3.0, "overrides": []}, "Scenario DE -3.0C"),
    ("scenario", {"market": "GB", "shift_c": 2.5, "overrides": []}, "Scenario GB +2.5C"),
    ("scenario", {"market": "GB", "shift_c": -0.0, "overrides": []}, "Scenario GB +0.0C"),
    ("scenario", {"market": "DE", "shift_c": -3, "overrides": [{"name": "CCGT"}, {"name": "Coal"}]},
     "Scenario DE -3.0C 2 plants"),
    ("scenario", {"market": "FR", "shift_c": 0, "overrides": [{"name": "Nuclear"}]},
     "Stack FR 1 plant"),
    ("scenario", {"market": "DE", "shift_c": 0.0, "overrides": [{"name": "CCGT"}, {"name": "Coal"}]},
     "Stack DE 2 plants"),
    ("backtest", {"market": "DE", "tenor": 2, "threshold": 1.5}, "Backtest DE M2 t1.5"),
    ("backtest", {"market": "GB", "tenor": 6, "threshold": 3.0}, "Backtest GB M6 t3"),
    ("job", {"name": "Merit Desk Daily Curve"}, "Merit Desk Daily Curve"),
])
def test_run_labels(kind, inputs, label):
    assert run_label(kind, inputs) == label and label.isascii()


def test_run_label_refuses_an_unknown_kind():
    with pytest.raises(ValueError, match="nonsense"):
        run_label("nonsense", {})
