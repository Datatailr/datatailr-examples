import json
import threading
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import merit_desk.common.store as store_module
from merit_desk.common.store import (
    BlobStore, LocalDirStore, MemoryStore, default_store, env_prefix, latest_backtest_summary,
    latest_curve, list_forecast_keys, load_forecast, newest_forecast, save_backtest_summary,
    save_curves, save_forecast, save_model,
)
from merit_desk.common.weather import Forecast


def curve_dict(market: str, as_of: str = "2026-10-29") -> dict:
    return {"market": market, "as_of": as_of, "source": "test forecast",
            "months": ["2026-10", "2026-11"], "prices": [88.5, 91.25]}


def forecast(day: date = date(2026, 10, 29)) -> Forecast:
    return Forecast(run_time=datetime(2026, 10, 29, 6, tzinfo=timezone.utc), source="test",
                    temps={"DE": {day: 7.5}})


class FakeBlob:
    """Stands in for datatailr.Blob: bytes by name, listed like Blob.ls."""

    files: dict[str, bytes] = {}

    def put(self, name, blob):
        self.files[name] = blob.encode() if isinstance(blob, str) else blob

    def get(self, name):
        return self.files[name]

    def exists(self, name):
        return name in self.files

    def ls(self, prefix, recursive=False):
        return [{"name": n, "is_file": True, "size": len(b)}
                for n, b in sorted(self.files.items()) if n.startswith(prefix)]


@pytest.fixture
def fake_blob(monkeypatch):
    FakeBlob.files = {}
    monkeypatch.setattr("datatailr.Blob", FakeBlob)
    return FakeBlob


def test_env_prefix_defaults_to_dev(monkeypatch):
    monkeypatch.delenv("DATATAILR_JOB_ENVIRONMENT", raising=False)
    assert env_prefix() == "merit-desk/dev/"


def test_paths_carry_the_environment(monkeypatch, memory_store):
    monkeypatch.setenv("DATATAILR_JOB_ENVIRONMENT", "PROD")
    save_curves([curve_dict("DE")], memory_store)
    assert "merit-desk/prod/curves/latest/DE.json" in memory_store.list("merit-desk/prod/")


def test_latest_curve_none_when_empty(memory_store):
    assert latest_curve("DE", memory_store) is None


def test_save_curves_writes_latest_and_dated_copy(memory_store):
    save_curves([curve_dict("DE"), curve_dict("GB")], memory_store)
    assert memory_store.list("merit-desk/dev/curves/") == [
        "merit-desk/dev/curves/2026-10-29/DE.json", "merit-desk/dev/curves/2026-10-29/GB.json",
        "merit-desk/dev/curves/latest/DE.json", "merit-desk/dev/curves/latest/GB.json"]
    assert latest_curve("de", memory_store) == curve_dict("DE")


def test_latest_curve_is_the_newest_save(memory_store):
    save_curves([curve_dict("DE", "2026-10-28")], memory_store)
    save_curves([curve_dict("DE", "2026-10-29")], memory_store)
    assert latest_curve("DE", memory_store)["as_of"] == "2026-10-29"


def test_backtest_summary_round_trip(memory_store):
    assert latest_backtest_summary(memory_store) is None
    save_backtest_summary({"best": "momentum", "sharpe": 1.4}, memory_store)
    assert memory_store.list("merit-desk/dev/backtests/") == [
        "merit-desk/dev/backtests/latest.json"]
    assert latest_backtest_summary(memory_store) == {"best": "momentum", "sharpe": 1.4}


def test_forecast_round_trip(memory_store):
    save_forecast("2026-10-29", forecast(), memory_store)
    assert load_forecast("2026-10-29", memory_store) == forecast()


def test_load_forecast_none_when_missing(memory_store):
    assert load_forecast("2026-10-29", memory_store) is None


def test_list_forecast_keys_sorted_oldest_first(memory_store):
    for key in ("2026-10-29", "2026-10-27", "2026-10-28"):
        save_forecast(key, forecast(), memory_store)
    save_curves([curve_dict("DE")], memory_store)  # other folders are not forecasts
    assert list_forecast_keys(memory_store) == ["2026-10-27", "2026-10-28", "2026-10-29"]


def test_newest_forecast_is_the_newest_saved_one(memory_store):
    save_forecast("2026-10-27", forecast(date(2026, 10, 27)), memory_store)
    save_forecast("2026-10-29", forecast(), memory_store)
    assert newest_forecast(memory_store) == forecast()


def test_newest_forecast_is_the_sample_when_none_is_saved(memory_store):
    assert newest_forecast(memory_store) == load_forecast("sample", memory_store) is not None


def test_newest_forecast_is_the_sample_when_the_newest_file_does_not_load(memory_store):
    save_forecast("2026-10-27", forecast(date(2026, 10, 27)), memory_store)
    memory_store.put_text("merit-desk/dev/forecasts/2026-10-29.json", "not a forecast")
    assert newest_forecast(memory_store) == load_forecast("sample", memory_store)
    memory_store.put_text("merit-desk/dev/forecasts/2026-10-29.json", '{"run_time": "2026-10-29"}')
    assert newest_forecast(memory_store) == load_forecast("sample", memory_store)  # a field is missing


@pytest.mark.parametrize("text", [
    "[]", "null", '{"run_time": "2026-10-29T06:00:00+00:00", "source": "x", "temps": []}'])
def test_newest_forecast_is_the_sample_for_json_of_the_wrong_shape(memory_store, text):
    memory_store.put_text("merit-desk/dev/forecasts/2026-10-29.json", text)
    assert newest_forecast(memory_store) == load_forecast("sample", memory_store)


def test_sample_forecast_falls_back_to_the_bundled_file(monkeypatch, tmp_path, memory_store):
    bundled = tmp_path / "forecast_sample.json"
    bundled.write_text(forecast().to_json(), encoding="utf-8")
    monkeypatch.setattr(store_module, "SAMPLE_FORECAST", bundled)
    assert load_forecast("sample", memory_store) == forecast()
    assert load_forecast("2026-10-29", memory_store) is None  # only "sample" falls back


def test_stored_sample_beats_the_bundled_file(monkeypatch, tmp_path, memory_store):
    bundled = tmp_path / "forecast_sample.json"
    bundled.write_text(forecast().to_json(), encoding="utf-8")
    monkeypatch.setattr(store_module, "SAMPLE_FORECAST", bundled)
    save_forecast("sample", forecast(date(2026, 11, 1)), memory_store)
    assert load_forecast("sample", memory_store) == forecast(date(2026, 11, 1))


def test_sample_forecast_none_when_bundled_file_is_missing(monkeypatch, tmp_path, memory_store):
    monkeypatch.setattr(store_module, "SAMPLE_FORECAST", tmp_path / "missing.json")
    assert load_forecast("sample", memory_store) is None


def test_local_dir_store_mirrors_names_as_files(tmp_path):
    store = LocalDirStore(tmp_path)
    store.put_text("merit-desk/dev/forecasts/a.json", "héllo")
    store.put_text("merit-desk/dev/curves/latest/DE.json", "{}")
    assert (tmp_path / "merit-desk/dev/forecasts/a.json").read_text(encoding="utf-8") == "héllo"
    assert store.get_text("merit-desk/dev/forecasts/a.json") == "héllo"
    assert store.get_text("merit-desk/dev/forecasts/b.json") is None
    assert store.list("merit-desk/dev/forecasts/") == ["merit-desk/dev/forecasts/a.json"]
    assert store.list("merit-desk/dev/") == ["merit-desk/dev/curves/latest/DE.json",
                                             "merit-desk/dev/forecasts/a.json"]
    assert store.list("merit-desk/prod/") == []


def test_local_dir_store_before_the_first_write(tmp_path):
    store = LocalDirStore(tmp_path / "not_created_yet")
    assert store.list("merit-desk/") == [] and list_forecast_keys(store) == []
    assert store.get_text("merit-desk/dev/forecasts/a.json") is None


def test_a_local_reader_never_sees_a_half_written_text(tmp_path):
    store, name = LocalDirStore(tmp_path), "merit-desk/dev/runs/a.json"
    texts = [json.dumps({"n": n, "pad": "x" * 200_000}) for n in range(2)]
    store.put_text(name, texts[0])
    writer = threading.Thread(target=lambda: [store.put_text(name, texts[i % 2])
                                              for i in range(200)])
    writer.start()
    reads = 0
    while writer.is_alive():
        json.loads(store.get_text(name))  # a half-written text does not parse
        reads += 1
    writer.join()
    assert reads > 0 and store.list("merit-desk/") == [name]  # no temporary file is left


def test_memory_store_list_is_sorted_and_filtered_by_prefix(memory_store):
    memory_store.put_text("b/2", "x")
    memory_store.put_text("a/1", "x")
    memory_store.put_text("b/1", "x")
    assert memory_store.list("b/") == ["b/1", "b/2"]


def test_default_store_is_local_when_asked(monkeypatch):
    monkeypatch.setenv("MERIT_DESK_STORE", "local")
    chosen = default_store()
    assert isinstance(chosen, LocalDirStore) and chosen.root == Path(".merit_desk_data")


def test_default_store_is_blob_storage_otherwise(monkeypatch, fake_blob):
    monkeypatch.delenv("MERIT_DESK_STORE", raising=False)
    assert isinstance(default_store(), BlobStore)


def test_blob_store_text_round_trip(fake_blob):
    store = BlobStore()
    store.put_text("merit-desk/dev/forecasts/a.json", "héllo")
    assert fake_blob.files["merit-desk/dev/forecasts/a.json"] == "héllo".encode()
    assert store.get_text("merit-desk/dev/forecasts/a.json") == "héllo"
    assert store.get_text("merit-desk/dev/forecasts/b.json") is None
    assert store.list("merit-desk/dev/") == ["merit-desk/dev/forecasts/a.json"]


def test_blob_store_lists_nothing_when_the_cli_prints_nothing(monkeypatch, fake_blob):
    monkeypatch.setattr(FakeBlob, "ls", lambda self, prefix, recursive=False: None)
    store = BlobStore()
    assert store.list("merit-desk/dev/forecasts/") == []
    assert list_forecast_keys(store) == []


def test_blob_store_serves_forecasts_and_curves(fake_blob):
    store = BlobStore()
    save_forecast("2026-10-29", forecast(), store)
    save_curves([curve_dict("DE")], store)
    assert list_forecast_keys(store) == ["2026-10-29"]
    assert latest_curve("DE", store) == curve_dict("DE")


def test_save_model_writes_bytes_under_the_models_folder(fake_blob):
    save_model("demand.pkl", b"\x80\x04model")
    assert fake_blob.files == {"merit-desk/dev/models/demand.pkl": b"\x80\x04model"}
