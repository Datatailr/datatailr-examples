"""Weight cache: local disk first, then blob storage, then Hugging Face."""

from __future__ import annotations

import re
import shutil
import threading
from pathlib import Path
from typing import Callable

from .storage import BlobStore

COMPLETE = ".complete"
_PART = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")

# Arguments: model, revision, target directory.
Downloader = Callable[[str, str, Path], None]


def _valid_part(part: str) -> bool:
    return bool(_PART.fullmatch(part)) and part not in (".", "..")


def validate_model_name(model: str) -> str:
    """Return `model` if it has the form `org/name`. Else raise `ValueError`."""
    parts = model.split("/")
    # "--" joins org and name in paths. A part must not contain it or end with
    # "-", or the joined path could not be split back into the same parts.
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

    def _blob_dir(self, model: str, revision: str) -> str:
        return f"{self._root}/models/{model.replace('/', '--')}/{revision}"

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
            if self._store.exists(f"{blob_dir}/{COMPLETE}"):
                self._fetch_from_blob(blob_dir, target)
            else:
                self._fetch_from_hub(model, revision, blob_dir, target)
            (target / COMPLETE).write_text("")
        return target

    def _fetch_from_blob(self, blob_dir: str, target: Path) -> None:
        prefix = blob_dir + "/"
        try:
            for path in self._store.ls(blob_dir):
                relative = path[len(prefix):]
                if relative != COMPLETE:
                    self._store.get_file(path, target / relative)
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise

    def _fetch_from_hub(
        self, model: str, revision: str, blob_dir: str, target: Path
    ) -> None:
        try:
            target.mkdir(parents=True, exist_ok=True)
            self._download(model, revision, target)
            for file in sorted(target.rglob("*")):
                relative = file.relative_to(target)
                if not file.is_file() or ".cache" in relative.parts:
                    continue
                if relative.as_posix() == COMPLETE:
                    continue
                self._store.put_file(f"{blob_dir}/{relative.as_posix()}", file)
            self._store.put(f"{blob_dir}/{COMPLETE}", "")
        except BaseException:
            shutil.rmtree(target, ignore_errors=True)
            raise

    def cached(self) -> list[dict]:
        """List the models in blob storage that have a `.complete` marker."""
        prefix = f"{self._root}/models/"
        found = []
        for path in self._store.ls(prefix):
            parts = path[len(prefix):].split("/")
            if len(parts) == 3 and parts[2] == COMPLETE:
                found.append(
                    {"model": parts[0].replace("--", "/", 1), "revision": parts[1]}
                )
        return sorted(found, key=lambda e: (e["model"], e["revision"]))
