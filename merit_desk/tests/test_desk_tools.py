import pytest
import requests
from datatailr.excel.type_check import validate_excel_addin_annotations

from merit_desk.common.markets import UnknownMarketError
from merit_desk.excel import desk_tools
from merit_desk.excel.desk_tools import curve_price_for, live_price_once, stack_price_for


class FakeResponse:
    def __init__(self, status_code: int, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("No JSON in the body.")
        return self._body


def get_answers(monkeypatch, response: FakeResponse) -> list[dict]:
    """Make requests.get answer with this response; returns the calls it receives."""
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return response
    monkeypatch.setattr(requests, "get", fake_get)
    return calls


def get_fails(monkeypatch, error: Exception) -> None:
    def fake_get(url, **kwargs):
        raise error
    monkeypatch.setattr(requests, "get", fake_get)


def test_stack_price_for():
    assert stack_price_for("de", 4.5) == 170.0


@pytest.mark.parametrize("temp_c", [float("nan"), float("inf"), float("-inf"), 61, -61])
def test_stack_price_for_refuses_a_temperature_that_is_not_between_minus_60_and_60(temp_c):
    with pytest.raises(ValueError, match="temp_c must be a number between -60 and 60."):
        stack_price_for("DE", temp_c)


def test_stack_price_for_accepts_the_limits():
    assert stack_price_for("DE", -60) > 0 and stack_price_for("DE", 60) > 0


@pytest.mark.parametrize("price_for", [
    lambda market: curve_price_for(market, "2026-11"),
    live_price_once,
])
def test_unknown_market_is_refused_before_any_request(monkeypatch, price_for):
    get_fails(monkeypatch, AssertionError("A request went out for an unknown market."))
    with pytest.raises(UnknownMarketError, match="Unknown market 'NL'"):
        price_for("NL")


def test_curve_price_uses_pricing_api(monkeypatch):
    reply = {"market": "DE", "month": "2026-11", "price": 118.2}
    calls = get_answers(monkeypatch, FakeResponse(200, reply))
    assert curve_price_for(" de ", "2026-11") == 118.2
    assert calls == [{"url": f"{desk_tools.PRICING_API}/curve/DE/2026-11", "timeout": 5}]


def test_live_price_once_uses_market_feed(monkeypatch):
    reply = {"market": "FR", "price": 88.4, "time": "2026-10-29T10:00:00+00:00"}
    calls = get_answers(monkeypatch, FakeResponse(200, reply))
    assert live_price_once("fr") == 88.4
    assert calls == [{"url": f"{desk_tools.MARKET_FEED}/latest/FR", "timeout": 5}]


def test_the_services_are_called_by_their_names_on_the_platform():
    assert desk_tools.PRICING_API == "http://pricing-api"
    assert desk_tools.MARKET_FEED == "http://market-feed"


@pytest.mark.parametrize("status, detail", [
    (503, "No curve yet. The daily curve workflow has not run."),
    (404, "Month 2030-01 is not in the newest curve (2026-10 to 2027-09)."),
])
def test_service_error_text_comes_through(monkeypatch, status, detail):
    get_answers(monkeypatch, FakeResponse(status, {"detail": detail}))
    with pytest.raises(ValueError) as raised:
        curve_price_for("DE", "2030-01")
    assert str(raised.value) == detail


@pytest.mark.parametrize("error", [requests.ConnectionError("refused"), requests.Timeout("slow")])
def test_connection_error_says_the_service_is_not_reachable(monkeypatch, error):
    get_fails(monkeypatch, error)
    with pytest.raises(ValueError, match="The Pricing API is not reachable."):
        curve_price_for("DE", "2026-11")
    with pytest.raises(ValueError, match="The Market Feed is not reachable."):
        live_price_once("DE")


@pytest.mark.parametrize("body", [{}, {"price": None}, {"price": "high"}, [101.2]])
def test_a_200_reply_without_a_numeric_price_says_the_service_is_not_reachable(monkeypatch, body):
    get_answers(monkeypatch, FakeResponse(200, body))
    with pytest.raises(ValueError, match="The Market Feed is not reachable."):
        live_price_once("DE")


def test_the_add_in_annotations_pass_the_sdk_check():
    validate_excel_addin_annotations(desk_tools.__file__)  # the platform runs this check at deploy


@pytest.mark.parametrize("response", [
    FakeResponse(200),
    FakeResponse(502, {"error": "bad gateway"}),
    FakeResponse(502, {"detail": ""}),
])
def test_a_reply_with_no_usable_message_says_the_service_is_not_reachable(monkeypatch, response):
    get_answers(monkeypatch, response)
    with pytest.raises(ValueError, match="The Market Feed is not reachable."):
        live_price_once("DE")
