import logging

import pytest
import requests

from merit_desk.common.notify import post_summary

WEBHOOK = "https://hooks.example.com/services/T000/B000/s3cr3t-token"


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


def secret_is(monkeypatch, value: str) -> list[str]:
    """Replace datatailr.Secrets; returns the list of secret names that were asked for."""
    asked = []

    class FakeSecrets:
        def get(self, key):
            asked.append(key)
            return value
    monkeypatch.setattr("datatailr.Secrets", FakeSecrets)
    return asked


def secret_is_missing(monkeypatch) -> None:
    class FakeSecrets:
        def get(self, key):
            raise RuntimeError(f"secret '{key}' not found")
    monkeypatch.setattr("datatailr.Secrets", FakeSecrets)


def post_answers(monkeypatch, status_code: int) -> list[dict]:
    calls = []

    def fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(status_code)
    monkeypatch.setattr(requests, "post", fake_post)
    return calls


def test_post_summary_false_when_secret_missing(monkeypatch, caplog):
    secret_is_missing(monkeypatch)
    calls = post_answers(monkeypatch, 200)
    with caplog.at_level(logging.WARNING):
        assert post_summary("hello") is False
    assert "desk_webhook_url" in caplog.text
    assert calls == []


@pytest.mark.parametrize("status_code", [200, 204])
def test_post_summary_true_on_2xx(monkeypatch, status_code):
    asked = secret_is(monkeypatch, WEBHOOK)
    calls = post_answers(monkeypatch, status_code)
    assert post_summary("hello") is True
    assert asked == ["desk_webhook_url"]
    assert calls == [{"url": WEBHOOK, "json": {"text": "hello"}, "timeout": 10}]


def test_post_summary_reads_the_named_secret(monkeypatch):
    asked = secret_is(monkeypatch, WEBHOOK)
    post_answers(monkeypatch, 200)
    assert post_summary("hello", secret_name="other_hook") is True
    assert asked == ["other_hook"]


def test_post_summary_false_on_500_without_raising(monkeypatch, caplog):
    secret_is(monkeypatch, WEBHOOK)
    post_answers(monkeypatch, 500)
    with caplog.at_level(logging.WARNING):
        assert post_summary("hello") is False
    assert "500" in caplog.text


def test_post_summary_false_on_network_error_and_keeps_the_url_out_of_the_log(
        monkeypatch, caplog):
    secret_is(monkeypatch, WEBHOOK)

    def fake_post(url, **kwargs):
        raise requests.ConnectionError(f"Max retries exceeded with url: {url}")
    monkeypatch.setattr(requests, "post", fake_post)
    with caplog.at_level(logging.WARNING):
        assert post_summary("hello") is False
    assert "ConnectionError" in caplog.text
    assert "s3cr3t-token" not in caplog.text
