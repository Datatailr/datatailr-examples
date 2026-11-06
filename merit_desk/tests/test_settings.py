import logging

import pytest

from merit_desk.common.settings import get_markets, get_sweep_size


def kv_returns(monkeypatch, values: dict) -> None:
    """Replace datatailr.KV with one that answers from a dict, like the real store."""
    class FakeKV:
        def get(self, key):
            return values[key]
    monkeypatch.setattr("datatailr.KV", FakeKV)


def kv_fails(monkeypatch) -> None:
    class BrokenKV:
        def get(self, key):
            raise RuntimeError("kv is down")
    monkeypatch.setattr("datatailr.KV", BrokenKV)


def codes(markets) -> list[str]:
    return [m.code for m in markets]


def test_get_markets_default_when_kv_fails(monkeypatch):
    kv_fails(monkeypatch)
    assert codes(get_markets()) == ["GB", "DE", "FR"]


def test_get_markets_default_when_variable_is_not_set(monkeypatch):
    kv_returns(monkeypatch, {})  # a missing key raises KeyError in the fake
    assert codes(get_markets()) == ["GB", "DE", "FR"]


def test_get_markets_reads_lower_case_list(monkeypatch):
    kv_returns(monkeypatch, {"markets": "gb, de"})
    assert codes(get_markets()) == ["GB", "DE"]


def test_get_markets_drops_repeated_codes(monkeypatch):
    kv_returns(monkeypatch, {"markets": "fr,FR, de"})
    assert codes(get_markets()) == ["FR", "DE"]


def test_get_markets_unknown_code_warns_and_uses_default(monkeypatch, caplog):
    kv_returns(monkeypatch, {"markets": "GB,NL"})
    with caplog.at_level(logging.WARNING):
        assert codes(get_markets()) == ["GB", "DE", "FR"]
    assert "markets" in caplog.text and "NL" in caplog.text


def test_get_markets_non_text_value_warns_and_uses_default(monkeypatch, caplog):
    kv_returns(monkeypatch, {"markets": ["GB", "DE"]})  # the SDK decodes JSON text
    with caplog.at_level(logging.WARNING):
        assert codes(get_markets()) == ["GB", "DE", "FR"]
    assert "markets" in caplog.text


@pytest.mark.parametrize("value", ["", " , "])
def test_get_markets_blank_value_uses_default(monkeypatch, value):
    kv_returns(monkeypatch, {"markets": value})
    assert codes(get_markets()) == ["GB", "DE", "FR"]


def test_get_sweep_size_default_when_kv_fails(monkeypatch):
    kv_fails(monkeypatch)
    assert get_sweep_size() == "small"


@pytest.mark.parametrize("value, expected",
                         [("full", "full"), (" Full ", "full"), ("small", "small")])
def test_get_sweep_size_reads_the_variable(monkeypatch, value, expected):
    kv_returns(monkeypatch, {"sweep_size": value})
    assert get_sweep_size() == expected


def test_get_sweep_size_non_text_value_warns_and_uses_small(monkeypatch, caplog):
    kv_returns(monkeypatch, {"sweep_size": 3})  # the SDK decodes JSON text
    with caplog.at_level(logging.WARNING):
        assert get_sweep_size() == "small"
    assert "sweep_size" in caplog.text


def test_get_sweep_size_invalid_value_uses_small(monkeypatch, caplog):
    kv_returns(monkeypatch, {"sweep_size": "huge"})
    with caplog.at_level(logging.WARNING):
        assert get_sweep_size() == "small"
    assert "sweep_size" in caplog.text and "huge" in caplog.text
