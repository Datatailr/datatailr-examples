"""Where the desk keeps its files: Datatailr blob storage, a local folder, or memory."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Protocol

from merit_desk.common.weather import Forecast

SAMPLE_FORECAST = Path(__file__).parent / "sample_data" / "forecast_sample.json"


class TextStore(Protocol):
    """Text files by name. A name is a full path such as merit-desk/dev/forecasts/a.json."""

    def put_text(self, name: str, text: str) -> None: ...

    def get_text(self, name: str) -> str | None: ...

    def list(self, prefix: str) -> list[str]: ...


class BlobStore:
    """Datatailr blob storage, shared by every job on the platform."""

    def __init__(self) -> None:
        from datatailr import Blob

        self._blob = Blob()

    def put_text(self, name: str, text: str) -> None:
        self._blob.put(name, text)

    def get_text(self, name: str) -> str | None:
        return self._blob.get(name).decode("utf-8") if self._blob.exists(name) else None

    def list(self, prefix: str) -> list[str]:
        items = self._blob.ls(prefix, recursive=True) or []  # the SDK gives None for no output
        return sorted(item["name"] for item in items if item.get("is_file", True))


class LocalDirStore:
    """A folder on disk, for running the desk on a laptop without Datatailr."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def put_text(self, name: str, text: str) -> None:
        """Write a temporary file, then rename it: a reader gets the old text or the new one."""
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.",
                                             suffix=".tmp")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as file:
                file.write(text)
            os.replace(temporary, path)
        except Exception:
            os.unlink(temporary)
            raise

    def get_text(self, name: str) -> str | None:
        path = self.root / name
        return path.read_text(encoding="utf-8") if path.is_file() else None

    def list(self, prefix: str) -> list[str]:
        names = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())
        return sorted(name for name in names if name.startswith(prefix))


class MemoryStore:
    """A dict, for tests."""

    def __init__(self) -> None:
        self._texts: dict[str, str] = {}

    def put_text(self, name: str, text: str) -> None:
        self._texts[name] = text

    def get_text(self, name: str) -> str | None:
        return self._texts.get(name)

    def list(self, prefix: str) -> list[str]:
        return sorted(name for name in self._texts if name.startswith(prefix))


def default_store() -> TextStore:
    """Blob storage, or a local folder when MERIT_DESK_STORE is 'local'."""
    if os.environ.get("MERIT_DESK_STORE") == "local":
        return LocalDirStore(Path(".merit_desk_data"))
    return BlobStore()


def env_prefix() -> str:
    """Every desk file lives under merit-desk/<environment>/, so dev and prod never mix."""
    env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "dev").lower()
    return f"merit-desk/{env}/"


def save_forecast(key: str, f: Forecast, store: TextStore) -> None:
    store.put_text(f"{env_prefix()}forecasts/{key}.json", f.to_json())


def load_forecast(key: str, store: TextStore) -> Forecast | None:
    """The stored forecast, or None. The key 'sample' falls back to the bundled file."""
    text = store.get_text(f"{env_prefix()}forecasts/{key}.json")
    if text is None and key == "sample" and SAMPLE_FORECAST.is_file():
        text = SAMPLE_FORECAST.read_text(encoding="utf-8")
    return Forecast.from_json(text) if text is not None else None


def list_forecast_keys(store: TextStore) -> list[str]:
    """Forecast keys (the run dates), oldest first."""
    names = store.list(f"{env_prefix()}forecasts/")
    return sorted(PurePosixPath(name).stem for name in names if name.endswith(".json"))


def newest_forecast(store: TextStore) -> Forecast:
    """The newest saved forecast, or the bundled sample when none is saved or it will not load."""
    keys = list_forecast_keys(store)
    try:
        forecast = load_forecast(keys[-1], store) if keys else None
    except (ValueError, KeyError, TypeError, AttributeError):  # the file is not a forecast
        forecast = None
    return forecast if forecast is not None else load_forecast("sample", store)


def save_curves(curves: list[dict], store: TextStore) -> None:
    """Write each curve twice: as the latest for its market, and under its own date."""
    for curve in curves:
        text = json.dumps(curve)
        store.put_text(f"{env_prefix()}curves/latest/{curve['market']}.json", text)
        store.put_text(f"{env_prefix()}curves/{curve['as_of']}/{curve['market']}.json", text)


def latest_curve(market: str, store: TextStore | None = None) -> dict | None:
    store = default_store() if store is None else store
    text = store.get_text(f"{env_prefix()}curves/latest/{market.strip().upper()}.json")
    return json.loads(text) if text is not None else None


def save_backtest_summary(summary: dict, store: TextStore) -> None:
    store.put_text(f"{env_prefix()}backtests/latest.json", json.dumps(summary))


def latest_backtest_summary(store: TextStore | None = None) -> dict | None:
    store = default_store() if store is None else store
    text = store.get_text(f"{env_prefix()}backtests/latest.json")
    return json.loads(text) if text is not None else None


def save_model(name: str, data: bytes) -> None:
    """Keep a trained model in blob storage (there is no local-folder version)."""
    from datatailr import Blob

    Blob().put(f"{env_prefix()}models/{name}", data)
