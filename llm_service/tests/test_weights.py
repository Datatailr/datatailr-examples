import sys
import threading
import time
import types
from pathlib import Path

import pytest

from llm_service.gateway.storage import MemoryBlobStore
from llm_service.gateway.weights import (
    WeightCache,
    hf_downloader,
    validate_model_name,
    validate_revision,
)


class CountingStore(MemoryBlobStore):
    """A memory store that counts file downloads."""

    def __init__(self):
        super().__init__()
        self.file_gets = 0

    def get_file(self, path, local):
        self.file_gets += 1
        super().get_file(path, local)


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
    import shutil

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
    for bad in ["../x", "/abs/path", "a/b/c", "org/..", "org", "", "org/na me", "org/-x", "./m"]:
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
