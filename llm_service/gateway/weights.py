"""Weight cache: local disk first, then blob storage, then Hugging Face."""

from __future__ import annotations

import json
import logging
import re
import shutil
import threading
from pathlib import Path
from typing import Callable, Iterable

from .storage import BlobStore

log = logging.getLogger("llm_service.weights")

COMPLETE = ".complete"
_PART = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")



class IncompleteCopy(Exception):
    """A file of the blob storage copy is missing or has the wrong size."""


# Arguments: model, revision, target directory.
Downloader = Callable[[str, str, Path], None]


def _valid_part(part: str) -> bool:
    return bool(_PART.fullmatch(part)) and part not in (".", "..")


def _safe_relative(path: str) -> bool:
    """Return True if `path` is a relative POSIX path that stays in its directory."""
    parts = path.split("/")
    return bool(path) and all(part not in ("", ".", "..") for part in parts)


def validate_model_name(model: str) -> str:
    """Return `model` if it has the form `org/name`. Else raise `ValueError`."""
    parts = model.split("/")
    # "--" joins org and name in paths. A part must not contain it or end with
    # "-", or the split of the joined path does not give the same parts.
    if len(parts) != 2 or not all(
        _valid_part(p) and "--" not in p and not p.endswith("-") for p in parts
    ):
        raise ValueError(f"invalid model name: {model!r}")
    return model


def validate_revision(revision: str) -> str:
    """Return `revision` if it is a safe name without `/`. Else raise `ValueError`."""
    if not _valid_part(revision):
        raise ValueError(f"invalid revision: {revision!r}")
    return revision


def hf_downloader(token: str | None) -> Downloader:
    """Return a downloader that fetches a repository from Hugging Face."""

    def download(model: str, revision: str, target: Path) -> None:
        from huggingface_hub import snapshot_download

        snapshot_download(
            repo_id=model, revision=revision, local_dir=target, token=token
        )

    return download


class WeightCache:
    """Get model files on local disk. Blob storage keeps a shared copy."""

    def __init__(
        self, store: BlobStore, root: str, local_dir: Path, download: Downloader
    ) -> None:
        self._store = store
        self._root = root.strip("/")
        self._local_dir = Path(local_dir)
        self._download = download
        self._lock = threading.Lock()
        # Sizes of complete markers, keyed by the marker path. A usable marker
        # never changes, so `cached` reads each one once. `remove` and a new
        # upload drop entries and bump the generation, so a read that started
        # before them keeps no stale size. `cached` runs in worker threads.
        self._sizes: dict[str, int] = {}
        self._sizes_generation = 0
        self._sizes_lock = threading.Lock()

    def _blob_dir(self, model: str, revision: str) -> str:
        return f"{self._root}/models/{model.replace('/', '--')}/{revision}"

    def _forget_size(self, marker: str) -> None:
        """Drop the memo of one marker. Call it when the marker is removed or written."""
        with self._sizes_lock:
            self._sizes.pop(marker, None)
            self._sizes_generation += 1

    def ensure(self, model: str, revision: str) -> Path:
        """Return the local directory of the model. Fetch the files if needed."""
        validate_model_name(model)
        validate_revision(revision)
        target = self._local_dir / model.replace("/", "--") / revision
        blob_dir = self._blob_dir(model, revision)
        with self._lock:
            if (target / COMPLETE).exists():
                return target
            # A directory without the marker is a partial copy. Remove it.
            shutil.rmtree(target, ignore_errors=True)
            files = self._read_manifest(f"{blob_dir}/{COMPLETE}")
            if files is not None:
                self._fetch_from_blob(blob_dir, files, target)
            else:
                self._fetch_from_hub(model, revision, blob_dir, target)
            (target / COMPLETE).write_text("")
        return target

    def _read_manifest(self, marker: str) -> list[dict] | None:
        """Return the file list of the blob marker, or None if it is not usable.

        The marker holds `{"files": [{"path": ..., "size": ...}, ...]}`. A
        missing, empty or invalid marker means that blob storage has no
        complete copy. A store error other than "missing" is raised.
        """
        try:
            raw = self._store.get(marker)
        except KeyError:
            return None
        try:
            files = json.loads(raw)["files"]
            if not isinstance(files, list):
                raise ValueError("files is not a list")
            for entry in files:
                path, size = entry["path"], entry["size"]
                if not isinstance(path, str) or not _safe_relative(path):
                    raise ValueError(f"unsafe path {path!r}")
                if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                    raise ValueError(f"bad size for {path!r}")
            return files
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("ignoring the unusable marker %s: %s", marker, exc)
            return None

    def _fetch_from_blob(self, blob_dir: str, files: list[dict], target: Path) -> None:
        try:
            # Create the directory first. A manifest without files creates none.
            target.mkdir(parents=True, exist_ok=True)
            for entry in files:
                relative, size = entry["path"], entry["size"]
                local = target / relative
                try:
                    self._store.get_file(f"{blob_dir}/{relative}", local)
                except KeyError:
                    raise IncompleteCopy(f"{blob_dir}/{relative} is missing in blob storage") from None
                actual = local.stat().st_size if local.is_file() else None
                if actual != size:
                    raise IncompleteCopy(
                        f"{blob_dir}/{relative} has {actual} bytes, the manifest says {size}"
                    )
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise

    def _fetch_from_hub(
        self, model: str, revision: str, blob_dir: str, target: Path
    ) -> None:
        # The blob marker of this directory is missing or unusable. It stays
        # so until all files are uploaded. The new marker is written last.
        try:
            target.mkdir(parents=True, exist_ok=True)
            self._download(model, revision, target)
            files = []
            for file in sorted(target.rglob("*")):
                relative = file.relative_to(target)
                if not file.is_file() or ".cache" in relative.parts:
                    continue
                if relative.as_posix() == COMPLETE:
                    continue
                self._store.put_file(f"{blob_dir}/{relative.as_posix()}", file)
                files.append({"path": relative.as_posix(), "size": file.stat().st_size})
            self._forget_size(f"{blob_dir}/{COMPLETE}")
            self._store.put(f"{blob_dir}/{COMPLETE}", json.dumps({"files": files}))
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise

    def prune(self, in_use: Callable[[], Iterable[dict]]) -> None:
        """Delete the local directories of the models that are not in use.

        `in_use` returns the models to keep, as dicts with `model` and
        `revision`. It runs under the cache lock, so no fetch runs at the same
        time. The blob storage copies stay. A deletion error is logged.
        """
        with self._lock:
            keep = {
                self._local_dir / entry["model"].replace("/", "--") / entry["revision"]
                for entry in in_use()
            }
            if not self._local_dir.is_dir():
                return
            for model_dir in sorted(self._local_dir.iterdir()):
                if not model_dir.is_dir():
                    continue
                for revision_dir in sorted(model_dir.iterdir()):
                    if revision_dir in keep:
                        continue
                    try:
                        shutil.rmtree(revision_dir)
                    except OSError as exc:
                        log.warning("the delete of %s failed: %s", revision_dir, exc)
                try:
                    model_dir.rmdir()  # only an empty directory goes
                except OSError:
                    pass

    def remove(self, model: str, revision: str) -> bool:
        """Delete the blob storage copy and the local copy of a model revision.

        Return True if the revision had a `.complete` marker. The marker goes
        first, so an interrupted removal leaves an unusable copy and not a
        broken one. `stored` still lists such a copy, so a second call removes
        the rest. The lock keeps a fetch from running at the same time.
        """
        validate_model_name(model)
        validate_revision(revision)
        blob_dir = self._blob_dir(model, revision)
        marker = f"{blob_dir}/{COMPLETE}"
        local_model = self._local_dir / model.replace("/", "--")
        with self._lock:
            self._forget_size(marker)
            existed = self._store.exists(marker)
            if existed:
                self._store.delete(marker)
            for path in self._store.ls(blob_dir):
                self._store.delete(path)
            self._forget_size(marker)
            shutil.rmtree(local_model / revision, ignore_errors=True)
            try:
                local_model.rmdir()  # only an empty directory goes
            except OSError:
                pass
        return existed

    def stored(self, model: str) -> list[str]:
        """Return the sorted revisions of `model` that have files in blob storage.

        A revision counts with or without a `.complete` marker. So the list
        also holds the rest of a copy that a failed removal left.
        """
        validate_model_name(model)
        prefix = f"{self._root}/models/{model.replace('/', '--')}/"
        revisions = set()
        for path in self._store.ls(prefix):
            parts = path[len(prefix):].split("/")
            if len(parts) >= 2 and _valid_part(parts[0]):
                revisions.add(parts[0])
        return sorted(revisions)

    def _size(self, marker: str) -> int | None:
        """Return the total size of the files in a marker, or None if it is not known.

        A store error gives None with a warning. Only a usable size is kept.
        """
        with self._sizes_lock:
            if marker in self._sizes:
                return self._sizes[marker]
            generation = self._sizes_generation
        try:
            files = self._read_manifest(marker)
        except Exception as exc:  # noqa: BLE001
            log.warning("the read of the marker %s failed: %s", marker, exc)
            return None
        if files is None:
            return None
        size = sum(f["size"] for f in files)
        with self._sizes_lock:
            if generation == self._sizes_generation:
                self._sizes[marker] = size
        return size

    def cached(self) -> list[dict]:
        """List the models in blob storage that have a `.complete` marker.

        `size_bytes` is the total size of the files in the marker, or None if
        the marker is not usable or cannot be read now.
        """
        prefix = f"{self._root}/models/"
        found = []
        for path in self._store.ls(prefix):
            parts = path[len(prefix):].split("/")
            if len(parts) == 3 and parts[2] == COMPLETE:
                found.append(
                    {
                        "model": parts[0].replace("--", "/", 1),
                        "revision": parts[1],
                        "size_bytes": self._size(path),
                    }
                )
        return sorted(found, key=lambda e: (e["model"], e["revision"]))
