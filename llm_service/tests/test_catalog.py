"""Tests for the catalog of recommended models."""

from __future__ import annotations

import json
import logging
import threading

import pytest

from llm_service.gateway.catalog import (
    MAX_ENTRIES,
    MAX_GPU_MEMORY_MIB,
    MAX_NOTE,
    CatalogFull,
    CatalogStore,
    InvalidEntry,
    StorageUnavailable,
    check_entry,
)
from llm_service.gateway import limits
from llm_service.gateway.storage import MemoryBlobStore

ROOT = "llm"
PATH = "llm/catalog.json"
FULL = {"model": "org/a", "revision": "main", "note": "Small.", "gpu_memory_mib": 6000}
MINIMAL = {"model": "org/b", "revision": "v1", "note": "", "gpu_memory_mib": None}


class FailingPut(MemoryBlobStore):
    def put(self, path, data):
        raise OSError("disk full")


def make(file=None, store=None):
    store = store or MemoryBlobStore()
    if file is not None:
        store.put(PATH, file if isinstance(file, (str, bytes)) else json.dumps(file))
    catalog = CatalogStore(store, ROOT)
    catalog.load()
    return catalog, store


def entry(index):
    return {"model": f"org/m{index}", "revision": "main", "note": "", "gpu_memory_mib": None}


def errors(caplog):
    return [r for r in caplog.records if r.levelno == logging.ERROR]


def test_storage_unavailable_is_the_class_of_the_limits_module():
    assert StorageUnavailable is limits.StorageUnavailable


def test_constants():
    assert (MAX_ENTRIES, MAX_NOTE, MAX_GPU_MEMORY_MIB) == (200, 500, 10_000_000)


def test_check_full_entry():
    assert check_entry(dict(FULL)) == FULL


def test_check_entry_with_model_and_revision_only_gets_defaults():
    assert check_entry({"model": "org/b", "revision": "v1"}) == MINIMAL


def test_check_entry_limits_pass():
    assert check_entry({**FULL, "note": "x" * MAX_NOTE})["note"] == "x" * MAX_NOTE
    assert check_entry({**FULL, "gpu_memory_mib": 1})["gpu_memory_mib"] == 1
    assert check_entry({**FULL, "gpu_memory_mib": MAX_GPU_MEMORY_MIB})["gpu_memory_mib"] == MAX_GPU_MEMORY_MIB


@pytest.mark.parametrize(
    "value",
    [
        {**FULL, "model": "nope"},
        {**FULL, "model": "a/b/c"},
        {**FULL, "model": ""},
        {**FULL, "model": 5},
        {**FULL, "model": None},
        {**FULL, "revision": "a/b"},
        {**FULL, "revision": ".."},
        {**FULL, "revision": ""},
        {**FULL, "revision": 1},
        {"revision": "main"},
        {"model": "org/a"},
        {**FULL, "note": "x" * (MAX_NOTE + 1)},
        {**FULL, "note": 5},
        {**FULL, "note": None},
        {**FULL, "gpu_memory_mib": 0},
        {**FULL, "gpu_memory_mib": -1},
        {**FULL, "gpu_memory_mib": MAX_GPU_MEMORY_MIB + 1},
        {**FULL, "gpu_memory_mib": True},
        {**FULL, "gpu_memory_mib": False},
        {**FULL, "gpu_memory_mib": 1.5},
        {**FULL, "gpu_memory_mib": 2.0},
        {**FULL, "gpu_memory_mib": "5"},
        {**FULL, "extra": 1},
        ["org/a", "main"],
        "org/a",
        None,
        5,
    ],
)
def test_check_entry_rejects(value):
    with pytest.raises(InvalidEntry) as err:
        check_entry(value)
    assert str(err.value)


def test_invalid_entry_carries_the_message_of_the_weights_check():
    with pytest.raises(InvalidEntry, match="invalid model name"):
        check_entry({**FULL, "model": "nope"})
    with pytest.raises(InvalidEntry, match="invalid revision"):
        check_entry({**FULL, "revision": "a/b"})


def test_check_entry_returns_a_copy():
    value = dict(FULL)
    result = check_entry(value)
    result["note"] = "changed"
    assert value == FULL


def test_load_missing_file():
    catalog, _ = make()
    assert catalog.entries() == []
    assert catalog.loaded() is True


def test_load_good_file_sorts_by_model_then_revision():
    b2 = {**MINIMAL, "revision": "v2"}
    catalog, _ = make({"models": [b2, MINIMAL, FULL]})
    assert catalog.entries() == [FULL, MINIMAL, b2]
    assert catalog.get("org/a", "main") == FULL
    assert catalog.get("org/a", "other") is None


def test_load_fills_defaults_of_a_stored_entry():
    catalog, _ = make({"models": [{"model": "org/b", "revision": "v1"}]})
    assert catalog.entries() == [MINIMAL]


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        b"\xff\xfe",
        "[]",
        "{}",
        '{"models": {}}',
        '{"models": [], "other": 1}',
        '{"models": [5]}',
        json.dumps({"models": [{**FULL, "model": "nope"}]}),
        json.dumps({"models": [FULL, {**FULL, "note": "other"}]}),
        json.dumps({"models": [entry(i) for i in range(MAX_ENTRIES + 1)]}),
    ],
)
def test_load_broken_file(content, caplog):
    with caplog.at_level(logging.ERROR):
        catalog, _ = make(content)
    assert catalog.entries() == []
    assert catalog.loaded() is True
    assert len(errors(caplog)) == 1


def test_load_deeply_nested_json_is_a_broken_file(caplog):
    with caplog.at_level(logging.ERROR):
        catalog, _ = make("[" * 100000 + "]" * 100000)
    assert catalog.entries() == []
    assert len(errors(caplog)) == 1


def test_file_with_200_entries_is_good():
    catalog, _ = make({"models": [entry(i) for i in range(MAX_ENTRIES)]})
    assert len(catalog.entries()) == MAX_ENTRIES


def test_load_replaces_old_state():
    catalog, store = make({"models": [FULL]})
    store.delete(PATH)
    catalog.load()
    assert catalog.entries() == []


def test_put_adds_and_persists():
    catalog, store = make()
    assert catalog.put(dict(FULL)) is None
    assert catalog.put(dict(MINIMAL)) is None
    assert catalog.entries() == [FULL, MINIMAL]
    assert json.loads(store.get(PATH)) == {"models": [FULL, MINIMAL]}


def test_put_fills_defaults():
    catalog, _ = make()
    catalog.put({"model": "org/b", "revision": "v1"})
    assert catalog.get("org/b", "v1") == MINIMAL


def test_put_replaces_and_returns_the_old_entry():
    catalog, store = make({"models": [FULL]})
    new = {**FULL, "note": "Better.", "gpu_memory_mib": None}
    assert catalog.put(dict(new)) == FULL
    assert catalog.entries() == [new]
    assert json.loads(store.get(PATH)) == {"models": [new]}


def test_same_model_with_another_revision_is_another_entry():
    catalog, _ = make({"models": [FULL]})
    assert catalog.put({**FULL, "revision": "v2"}) is None
    assert len(catalog.entries()) == 2


def test_put_validates():
    catalog, store = make()
    with pytest.raises(InvalidEntry):
        catalog.put({**FULL, "model": "nope"})
    assert catalog.entries() == []
    assert not store.exists(PATH)


def test_201st_new_entry_raises_catalog_full():
    catalog, store = make({"models": [entry(i) for i in range(MAX_ENTRIES)]})
    with pytest.raises(CatalogFull):
        catalog.put(entry(MAX_ENTRIES))
    assert len(catalog.entries()) == MAX_ENTRIES
    assert len(json.loads(store.get(PATH))["models"]) == MAX_ENTRIES


def test_replace_at_200_entries_works():
    catalog, _ = make({"models": [entry(i) for i in range(MAX_ENTRIES)]})
    new = {**entry(7), "note": "changed"}
    assert catalog.put(new) == entry(7)
    assert catalog.get("org/m7", "main")["note"] == "changed"
    assert len(catalog.entries()) == MAX_ENTRIES


def test_remove_returns_the_old_entry():
    catalog, store = make({"models": [FULL, MINIMAL]})
    assert catalog.remove("org/a", "main") == FULL
    assert catalog.entries() == [MINIMAL]
    assert json.loads(store.get(PATH)) == {"models": [MINIMAL]}


def test_remove_without_an_entry_raises_key_error():
    catalog, store = make({"models": [FULL]})
    with pytest.raises(KeyError):
        catalog.remove("org/a", "other")
    assert catalog.entries() == [FULL]


def test_failed_put_raises_storage_unavailable_and_changes_nothing(caplog):
    store = FailingPut()
    MemoryBlobStore.put(store, PATH, json.dumps({"models": [FULL]}))
    catalog, _ = make(store=store)
    with caplog.at_level(logging.ERROR):
        with pytest.raises(StorageUnavailable):
            catalog.put(dict(MINIMAL))
        with pytest.raises(StorageUnavailable):
            catalog.put({**FULL, "note": "changed"})
    assert catalog.entries() == [FULL]
    assert len(errors(caplog)) == 2


def test_failed_remove_raises_storage_unavailable_and_changes_nothing():
    store = FailingPut()
    MemoryBlobStore.put(store, PATH, json.dumps({"models": [FULL]}))
    catalog, _ = make(store=store)
    with pytest.raises(StorageUnavailable):
        catalog.remove("org/a", "main")
    assert catalog.entries() == [FULL]


def test_getters_return_copies():
    catalog, _ = make({"models": [FULL]})
    catalog.entries()[0]["note"] = "changed"
    catalog.get("org/a", "main")["note"] = "changed"
    assert catalog.entries() == [FULL]
    old = catalog.put({**FULL, "note": "new"})
    old["note"] = "changed"
    assert catalog.get("org/a", "main")["note"] == "new"
    removed = catalog.remove("org/a", "main")
    assert removed == {**FULL, "note": "new"}
    removed["note"] = "changed"
    assert catalog.get("org/a", "main") is None
    assert catalog.entries() == []


def test_put_copies_input():
    catalog, _ = make()
    value = dict(FULL)
    catalog.put(value)
    value["note"] = "changed"
    assert catalog.get("org/a", "main") == FULL


def test_entries_and_get_do_not_wait_for_a_slow_write():
    entered, release = threading.Event(), threading.Event()

    class SlowPut(MemoryBlobStore):
        def put(self, path, data):
            entered.set()
            assert release.wait(5)
            super().put(path, data)

    store = SlowPut()
    MemoryBlobStore.put(store, PATH, json.dumps({"models": [FULL]}))
    catalog, _ = make(store=store)
    writer = threading.Thread(target=catalog.put, args=(dict(MINIMAL),))
    writer.start()
    try:
        assert entered.wait(5)
        done = []
        reader = threading.Thread(
            target=lambda: done.append((catalog.entries(), catalog.get("org/a", "main"), catalog.loaded()))
        )
        reader.start()
        reader.join(2)
        assert not reader.is_alive()
        assert done == [([FULL], FULL, True)]
    finally:
        release.set()
        writer.join(5)
    assert catalog.entries() == [FULL, MINIMAL]


class FlakyGet(MemoryBlobStore):
    def __init__(self, failures):
        super().__init__()
        self.failures = failures

    def get(self, path):
        if self.failures:
            self.failures -= 1
            raise OSError("storage down")
        return super().get(path)


def flaky(failures):
    store = FlakyGet(0)
    MemoryBlobStore.put(store, PATH, json.dumps({"models": [FULL]}))
    store.failures = failures
    return CatalogStore(store, ROOT), store


@pytest.mark.parametrize(
    "write, expected",
    [
        (lambda c: c.put(dict(MINIMAL)), [FULL, MINIMAL]),
        (lambda c: c.remove("org/a", "main"), []),
    ],
)
def test_write_retries_a_failed_read(write, expected):
    catalog, store = flaky(1)
    catalog.load()
    assert catalog.entries() == []
    assert catalog.loaded() is False
    write(catalog)
    assert json.loads(store.get(PATH)) == {"models": expected}
    assert catalog.entries() == expected
    assert catalog.loaded() is True


@pytest.mark.parametrize(
    "write",
    [
        lambda c: c.put(dict(MINIMAL)),
        lambda c: c.remove("org/a", "main"),
    ],
)
def test_write_refused_while_read_keeps_failing(write, caplog):
    catalog, store = flaky(100)
    with caplog.at_level(logging.ERROR):
        catalog.load()
    assert len(errors(caplog)) == 1
    with pytest.raises(StorageUnavailable):
        write(catalog)
    assert catalog.entries() == []
    store.failures = 0
    assert json.loads(store.get(PATH)) == {"models": [FULL]}


def test_broken_file_counts_as_loaded_and_write_repairs_it():
    catalog, store = make("not json")
    catalog.put(dict(FULL))
    assert json.loads(store.get(PATH)) == {"models": [FULL]}


def test_missing_file_counts_as_loaded():
    catalog, store = make()
    assert catalog.loaded() is True
    catalog.put(dict(FULL))
    assert json.loads(store.get(PATH)) == {"models": [FULL]}


def test_retry_load_reads_again_only_after_a_failed_read():
    catalog, store = flaky(2)
    catalog.load()
    assert catalog.loaded() is False and catalog.entries() == []
    assert catalog.retry_load() is False
    assert catalog.loaded() is False
    assert catalog.retry_load() is True
    assert catalog.loaded() is True and catalog.entries() == [FULL]
    store.failures = 5
    assert catalog.retry_load() is True
    assert store.failures == 5


def test_retry_load_never_raises(caplog):
    class Broken(MemoryBlobStore):
        def get(self, path):
            raise RuntimeError("storage down")

    catalog = CatalogStore(Broken(), ROOT)
    with caplog.at_level(logging.ERROR):
        catalog.load()
        assert catalog.retry_load() is False
    catalog._load = lambda: 1 / 0
    assert catalog.retry_load() is False
    assert catalog.loaded() is False


def test_a_lasting_read_error_logs_one_line_until_its_text_changes_or_a_read_works(caplog):
    class Errors(MemoryBlobStore):
        error = "storage down"

        def get(self, path):
            if self.error:
                raise OSError(self.error)
            return super().get(path)

    store = Errors()
    catalog = CatalogStore(store, ROOT)
    with caplog.at_level(logging.ERROR):
        catalog.load()
        catalog.retry_load()
        catalog.retry_load()
        assert len(errors(caplog)) == 1
        store.error = "other error"
        catalog.retry_load()
        assert len(errors(caplog)) == 2
        store.error = None
        assert catalog.retry_load() is True
        store.error = "storage down"
        catalog.load()
        assert len(errors(caplog)) == 3


def test_blob_root_slashes_are_stripped():
    store = MemoryBlobStore()
    CatalogStore(store, "/llm/").put(dict(FULL))
    assert store.exists("llm/catalog.json")
