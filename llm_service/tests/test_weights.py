import json
import shutil
import sys
import threading
import time
import types

import pytest

from llm_service.gateway.storage import MemoryBlobStore
from llm_service.gateway.weights import (
    WeightCache,
    hf_downloader,
    validate_model_name,
    validate_revision,
)


class CountingStore(MemoryBlobStore):
    """A memory store that counts file downloads and listings."""

    def __init__(self):
        super().__init__()
        self.file_gets = 0
        self.listings = 0

    def get_file(self, path, local):
        self.file_gets += 1
        super().get_file(path, local)

    def ls(self, prefix):
        self.listings += 1
        return super().ls(prefix)


MARKER = "root/models/org--m/main/.complete"


def manifest(store):
    return json.loads(store.get(MARKER))


class Fixture:
    def __init__(self, tmp_path, store=None):
        self.store = store or CountingStore()
        self.local = tmp_path / "local"
        self.calls = 0
        self.fail_next = False
        self.delay = 0.0
        self.cache = WeightCache(self.store, "root", self.local, self.download)

    def download(self, model, revision, target):
        self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        (target / "sub").mkdir(parents=True, exist_ok=True)
        (target / "config.json").write_text("{}")
        (target / "sub" / "model.safetensors").write_bytes(b"w")
        (target / ".cache").mkdir(exist_ok=True)
        (target / ".cache" / "meta").write_text("x")
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("download failed")


@pytest.fixture
def fx(tmp_path):
    return Fixture(tmp_path)


def test_first_use_downloads_and_uploads(fx):
    path = fx.cache.ensure("org/m", "main")
    assert path == fx.local / "org--m" / "main"
    assert (path / "sub" / "model.safetensors").exists() and fx.calls == 1
    assert (path / ".complete").exists()
    assert fx.store.exists("root/models/org--m/main/sub/model.safetensors")
    assert fx.store.exists("root/models/org--m/main/config.json")
    assert fx.store.exists("root/models/org--m/main/.complete")
    assert not any("/.cache/" in p for p in fx.store.ls("root/models"))


def test_local_hit_touches_nothing(fx):
    fx.cache.ensure("org/m", "main")
    fx.store.file_gets = 0
    fx.cache.ensure("org/m", "main")
    assert fx.calls == 1 and fx.store.file_gets == 0


def test_blob_hit_after_local_loss(fx, tmp_path):
    fx.cache.ensure("org/m", "main")
    shutil.rmtree(fx.local)
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 1
    assert (path / "sub" / "model.safetensors").read_bytes() == b"w"
    assert (path / "config.json").exists() and (path / ".complete").exists()


def test_interrupted_upload_is_not_a_hit(fx):
    fx.store.put("root/models/org--m/main/config.json", "{}")
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 1
    assert fx.store.exists("root/models/org--m/main/.complete")
    assert (path / ".complete").exists()


def test_partial_local_dir_is_replaced(fx):
    stale = fx.local / "org--m" / "main"
    stale.mkdir(parents=True)
    (stale / "stray.bin").write_bytes(b"old")
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 1
    assert not (path / "stray.bin").exists()
    assert (path / ".complete").exists()


def test_partial_local_dir_is_replaced_on_blob_hit(fx):
    fx.cache.ensure("org/m", "main")
    (fx.local / "org--m" / "main" / ".complete").unlink()
    (fx.local / "org--m" / "main" / "stray.bin").write_bytes(b"old")
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 1
    assert not (path / "stray.bin").exists()
    assert (path / ".complete").exists()


def test_failed_download_raises_and_a_retry_works(fx):
    fx.fail_next = True
    with pytest.raises(RuntimeError, match="download failed"):
        fx.cache.ensure("org/m", "main")
    assert not fx.store.exists("root/models/org--m/main/.complete")
    assert not (fx.local / "org--m" / "main" / ".complete").exists()
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 2 and (path / ".complete").exists()
    assert fx.store.exists("root/models/org--m/main/.complete")


def test_failed_upload_writes_no_marker_and_a_retry_works(tmp_path):
    class BrokenUpload(CountingStore):
        broken = True

        def put_file(self, path, local):
            if self.broken:
                raise OSError("upload failed")
            super().put_file(path, local)

    f = Fixture(tmp_path, BrokenUpload())
    with pytest.raises(OSError, match="upload failed"):
        f.cache.ensure("org/m", "main")
    assert not f.store.exists("root/models/org--m/main/.complete")
    assert not (f.local / "org--m" / "main" / ".complete").exists()
    f.store.broken = False
    path = f.cache.ensure("org/m", "main")
    assert (path / ".complete").exists()
    assert f.store.exists("root/models/org--m/main/.complete")


def test_concurrent_ensure_downloads_once(fx):
    fx.delay = 0.2
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(fx.cache.ensure("org/m", "main")))
        for _ in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert fx.calls == 1 and len(results) == 2


def test_cached_lists_only_complete(fx):
    fx.cache.ensure("org/b", "v2")
    fx.cache.ensure("org/a", "main")
    fx.store.put("root/models/org--c/main/config.json", "{}")
    assert fx.cache.cached() == [
        {"model": "org/a", "revision": "main", "size_bytes": 3},
        {"model": "org/b", "revision": "v2", "size_bytes": 3},
    ]


def test_cached_reports_the_size_from_the_manifest(fx):
    fx.cache.ensure("org/m", "main")
    assert fx.cache.cached() == [{"model": "org/m", "revision": "main", "size_bytes": 3}]


def test_cached_size_is_none_for_an_unusable_manifest(fx):
    fx.store.put(MARKER, b"not json")
    assert fx.cache.cached()[0]["size_bytes"] is None


def test_cached_size_is_zero_for_a_manifest_without_files(fx):
    fx.store.put(MARKER, json.dumps({"files": []}))
    assert fx.cache.cached()[0]["size_bytes"] == 0


def test_remove_deletes_marker_files_and_local_directory(fx):
    fx.cache.ensure("org/m", "main")
    fx.cache.ensure("org/m", "v2")
    assert fx.cache.remove("org/m", "main") is True
    assert fx.store.ls("root/models/org--m/main") == []
    assert not (fx.local / "org--m" / "main").exists()
    assert (fx.local / "org--m" / "v2").exists()
    assert [e["revision"] for e in fx.cache.cached()] == ["v2"]
    assert fx.cache.remove("org/m", "v2") is True
    assert fx.store.ls("root/models/org--m") == []
    assert not (fx.local / "org--m").exists()
    assert fx.cache.cached() == []


def test_remove_of_an_unknown_revision_returns_false(fx):
    fx.cache.ensure("org/m", "main")
    assert fx.cache.remove("org/m", "other") is False
    assert fx.cache.cached()[0]["revision"] == "main"


def test_remove_deletes_the_marker_first(tmp_path):
    class BreakingStore(MemoryBlobStore):
        def __init__(self):
            super().__init__()
            self.deletes = 0

        def delete(self, path):
            self.deletes += 1
            if self.deletes == 2:
                raise OSError("simulated storage failure")
            super().delete(path)

    fx = Fixture(tmp_path, BreakingStore())
    fx.cache.ensure("org/m", "main")
    with pytest.raises(OSError):
        fx.cache.remove("org/m", "main")
    assert not fx.store.exists(MARKER)
    assert fx.cache.cached() == []


def test_remove_rejects_invalid_names(fx):
    with pytest.raises(ValueError):
        fx.cache.remove("../x", "main")
    with pytest.raises(ValueError):
        fx.cache.remove("org/m", "a/b")


def test_ensure_validates_names(fx):
    with pytest.raises(ValueError):
        fx.cache.ensure("../x", "main")
    with pytest.raises(ValueError):
        fx.cache.ensure("org/m", "a/b")
    assert fx.calls == 0


def test_validate_model_name():
    for bad in ["../x", "/abs/path", "a/b/c", "org/..", "org", "", "org/na me", "org/-x", "./m", "a--b/c", "a/b--c", "a-/b", "a/b-"]:
        with pytest.raises(ValueError):
            validate_model_name(bad)
    assert validate_model_name("Qwen/Qwen2.5-1.5B-Instruct") == "Qwen/Qwen2.5-1.5B-Instruct"


def test_validate_revision():
    for bad in ["", "a/b", "..", ".", "-x", "a b"]:
        with pytest.raises(ValueError):
            validate_revision(bad)
    assert validate_revision("main") == "main"
    assert validate_revision("v1.0-rc_1") == "v1.0-rc_1"


def test_hf_downloader_calls_snapshot_download(monkeypatch, tmp_path):
    seen = {}
    fake = types.ModuleType("huggingface_hub")

    def snapshot_download(**kwargs):
        seen.update(kwargs)

    fake.snapshot_download = snapshot_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake)
    hf_downloader("tok")("org/m", "main", tmp_path)
    assert seen == {
        "repo_id": "org/m",
        "revision": "main",
        "local_dir": tmp_path,
        "token": "tok",
    }


def test_cached_returns_the_names_given_to_ensure(fx):
    names = ["Qwen/Qwen2.5-1.5B-Instruct", "a-b/c_d.e"]
    for name in names:
        fx.cache.ensure(name, "main")
    assert [e["model"] for e in fx.cache.cached()] == sorted(names)


def test_prune_keeps_models_in_use_and_deletes_the_rest(fx):
    for name in ("org/a", "org/b", "org/c"):
        fx.cache.ensure(name, "main")
    fx.cache.ensure("org/a", "v2")
    fx.cache.prune(lambda: [{"model": "org/a", "revision": "main"}, {"model": "org/c", "revision": "main"}])
    assert (fx.local / "org--a" / "main" / ".complete").exists()  # active
    assert (fx.local / "org--c" / "main" / ".complete").exists()  # loading
    assert not (fx.local / "org--a" / "v2").exists()
    assert not (fx.local / "org--b").exists()
    assert fx.store.exists("root/models/org--b/main/.complete")  # blob copies stay


def test_prune_logs_a_deletion_error_and_goes_on(fx, monkeypatch, caplog):
    fx.cache.ensure("org/a", "main")
    fx.cache.ensure("org/b", "main")

    def broken_rmtree(path, *args, **kwargs):
        raise OSError("device busy")

    monkeypatch.setattr("llm_service.gateway.weights.shutil.rmtree", broken_rmtree)
    with caplog.at_level("WARNING", logger="llm_service.weights"):
        fx.cache.prune(lambda: [])
    assert sum("device busy" in m for m in caplog.messages) == 2


def test_prune_without_a_local_directory_does_nothing(fx):
    fx.cache.prune(lambda: [])
    assert not fx.local.exists()


def test_upload_writes_a_manifest_marker(fx):
    fx.cache.ensure("org/m", "main")
    assert manifest(fx.store) == {
        "files": [
            {"path": "config.json", "size": 2},
            {"path": "sub/model.safetensors", "size": 1},
        ]
    }


def test_blob_fetch_downloads_only_the_manifest_files(fx):
    fx.cache.ensure("org/m", "main")
    fx.store.put("root/models/org--m/main/stale.bin", b"old upload")
    shutil.rmtree(fx.local)
    fx.store.file_gets = fx.store.listings = 0
    path = fx.cache.ensure("org/m", "main")
    assert fx.store.file_gets == 2 and fx.store.listings == 0
    assert not (path / "stale.bin").exists()
    assert (path / ".complete").exists() and fx.calls == 1


def test_missing_blob_file_fails_and_writes_no_local_marker(fx):
    fx.cache.ensure("org/m", "main")
    del fx.store._files["root/models/org--m/main/sub/model.safetensors"]
    shutil.rmtree(fx.local)
    with pytest.raises(Exception, match="sub/model.safetensors"):
        fx.cache.ensure("org/m", "main")
    assert not (fx.local / "org--m" / "main").exists()
    assert fx.calls == 1


def test_size_mismatch_fails_and_removes_the_partial_copy(fx):
    fx.cache.ensure("org/m", "main")
    fx.store.put("root/models/org--m/main/config.json", "{ }")
    shutil.rmtree(fx.local)
    with pytest.raises(Exception, match="config.json"):
        fx.cache.ensure("org/m", "main")
    assert not (fx.local / "org--m" / "main").exists()


@pytest.mark.parametrize(
    "marker",
    [
        "",
        "not json",
        "[]",
        json.dumps({"files": "x"}),
        json.dumps({"files": [{"path": "../evil", "size": 1}]}),
        json.dumps({"files": [{"path": "/abs", "size": 1}]}),
        json.dumps({"files": [{"path": "a", "size": -1}]}),
        json.dumps({"files": [{"path": "a", "size": True}]}),
    ],
)
def test_old_or_broken_marker_downloads_again_and_overwrites(fx, marker):
    fx.store.put(MARKER, marker)
    path = fx.cache.ensure("org/m", "main")
    assert fx.calls == 1 and (path / ".complete").exists()
    assert [f["path"] for f in manifest(fx.store)["files"]] == ["config.json", "sub/model.safetensors"]


def test_upload_writes_the_marker_after_the_files(tmp_path):
    class OrderStore(CountingStore):
        def __init__(self):
            super().__init__()
            self.order = []

        def put(self, path, data):
            self.order.append(path)
            super().put(path, data)

        def put_file(self, path, local):
            self.order.append(path)
            super().put_file(path, local)

    f = Fixture(tmp_path, OrderStore())
    f.cache.ensure("org/m", "main")
    assert f.store.order[-1] == MARKER and f.store.order.count(MARKER) == 1


def test_blob_fetch_of_an_empty_manifest_creates_the_directory(fx):
    fx.store.put(MARKER, json.dumps({"files": []}))
    path = fx.cache.ensure("org/m", "main")
    assert path == fx.local / "org--m" / "main"
    assert (path / ".complete").exists()
    assert fx.calls == 0  # the blob copy was used, not Hugging Face


class GetCountingStore(MemoryBlobStore):
    """A memory store that counts reads and fails them for paths that contain `broken`."""

    def __init__(self):
        super().__init__()
        self.gets: list[str] = []
        self.broken: str | None = None

    def get(self, path):
        self.gets.append(path)
        if self.broken and self.broken in path:
            raise OSError("simulated storage failure")
        return super().get(path)


def test_cached_reads_a_known_manifest_once(tmp_path):
    fx = Fixture(tmp_path, GetCountingStore())
    fx.cache.ensure("org/m", "main")
    first = fx.cache.cached()
    fx.store.gets.clear()
    assert fx.cache.cached() == first
    assert fx.store.gets == []


def test_cached_lists_every_model_when_one_manifest_read_fails(tmp_path, caplog):
    fx = Fixture(tmp_path, GetCountingStore())
    fx.cache.ensure("org/a", "main")
    fx.cache.ensure("org/b", "main")
    fx.store.broken = "org--b/main/.complete"
    with caplog.at_level("WARNING", logger="llm_service.weights"):
        assert fx.cache.cached() == [
            {"model": "org/a", "revision": "main", "size_bytes": 3},
            {"model": "org/b", "revision": "main", "size_bytes": None},
        ]
    assert len([m for m in caplog.messages if "org--b" in m]) == 1
    # The failed read is not kept: the next call reads the manifest again.
    fx.store.broken = None
    assert fx.cache.cached()[1]["size_bytes"] == 3


def test_remove_clears_the_size_memo(fx):
    fx.cache.ensure("org/m", "main")
    assert fx.cache.cached()[0]["size_bytes"] == 3
    fx.cache.remove("org/m", "main")
    fx.store.put("root/models/org--m/main/big.bin", b"x" * 10)
    fx.store.put(MARKER, json.dumps({"files": [{"path": "big.bin", "size": 10}]}))
    assert fx.cache.cached()[0]["size_bytes"] == 10


def test_a_new_marker_after_an_unusable_one_shows_its_size(fx):
    fx.store.put(MARKER, b"not json")
    assert fx.cache.cached()[0]["size_bytes"] is None
    fx.cache.ensure("org/m", "main")  # downloads again and writes a usable marker
    assert fx.cache.cached()[0]["size_bytes"] == 3


def test_stored_lists_revisions_with_files_with_or_without_a_marker(fx):
    fx.cache.ensure("org/m", "v2")
    fx.store.put("root/models/org--m/main/config.json", "{}")
    fx.store.put("root/models/org--other/main/.complete", "")
    assert fx.cache.stored("org/m") == ["main", "v2"]
    assert fx.cache.stored("org/none") == []
    with pytest.raises(ValueError):
        fx.cache.stored("../x")


def test_remove_after_a_failed_remove_deletes_the_rest(tmp_path):
    class FailOnce(MemoryBlobStore):
        def __init__(self):
            super().__init__()
            self.failed = False

        def delete(self, path):
            if path.endswith("config.json") and not self.failed:
                self.failed = True
                raise OSError("simulated storage failure")
            super().delete(path)

    fx = Fixture(tmp_path, FailOnce())
    fx.cache.ensure("org/m", "main")
    with pytest.raises(OSError):
        fx.cache.remove("org/m", "main")
    assert fx.cache.cached() == []
    assert fx.cache.stored("org/m") == ["main"]
    fx.cache.remove("org/m", "main")
    assert fx.cache.stored("org/m") == []
    assert fx.store.ls("root/models/org--m") == []


def test_is_cached_is_true_only_with_the_blob_marker(fx):
    assert fx.cache.is_cached("org/m", "main") is False
    fx.store.put("root/models/org--m/main/config.json", "{}")
    assert fx.cache.is_cached("org/m", "main") is False
    fx.cache.ensure("org/m", "main")
    assert fx.cache.is_cached("org/m", "main") is True
    assert fx.cache.is_cached("org/m", "v2") is False


def test_is_cached_ignores_the_local_copy(fx):
    fx.cache.ensure("org/m", "main")
    fx.store.delete(MARKER)
    assert (fx.local / "org--m" / "main" / ".complete").exists()
    assert fx.cache.is_cached("org/m", "main") is False


@pytest.mark.parametrize("model,revision", [("nonsense", "main"), ("org/m", "a/b"), ("org/m", "..")])
def test_is_cached_validates_the_names(fx, model, revision):
    with pytest.raises(ValueError):
        fx.cache.is_cached(model, revision)


def test_local_copy_gives_the_directory_of_a_complete_local_copy(fx):
    assert fx.cache.local_copy("org/m", "main") is None
    path = fx.cache.ensure("org/m", "main")
    assert fx.cache.local_copy("org/m", "main") == path
    (path / ".complete").unlink()  # a partial copy does not count
    assert fx.cache.local_copy("org/m", "main") is None


def test_local_copy_does_not_take_the_cache_lock(fx):
    path = fx.cache.ensure("org/m", "main")
    with fx.cache._lock:
        assert fx.cache.local_copy("org/m", "main") == path


@pytest.mark.parametrize("model,revision", [("nonsense", "main"), ("org/m", "a/b"), ("org/m", "..")])
def test_local_copy_validates_the_names(fx, model, revision):
    with pytest.raises(ValueError):
        fx.cache.local_copy(model, revision)
