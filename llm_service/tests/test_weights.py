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
        {"model": "org/a", "revision": "main"},
        {"model": "org/b", "revision": "v2"},
    ]


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
