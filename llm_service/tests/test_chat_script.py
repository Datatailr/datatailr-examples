from types import SimpleNamespace

import pytest

pytest.importorskip("openai")

from llm_service import chat  # noqa: E402

OUTSIDE = "https://example.test/job/dev/llm-service/v1"


def no_login():
    raise AssertionError("the login is not needed here")


def test_a_client_on_the_platform_sends_no_login():
    client = chat.make_client({}, cookie=no_login)
    assert str(client.base_url) == "http://llm-service/v1/"
    assert "Cookie" not in client.default_headers
    assert client.default_query == {}


def test_an_outside_client_sends_the_login_of_the_cli():
    client = chat.make_client({"LLM_BASE_URL": OUTSIDE}, cookie=lambda: "X-Datatailr-Oidc-Data=abc")
    assert client.default_headers["Cookie"] == "X-Datatailr-Oidc-Data=abc"
    assert client.default_query == {}


def test_a_token_goes_into_the_query_and_not_into_a_cookie():
    client = chat.make_client({"LLM_BASE_URL": OUTSIDE, "LLM_TOKEN": " secret "}, cookie=no_login)
    assert client.default_query == {"__datatailr_user_cookie__": "secret"}
    assert "Cookie" not in client.default_headers


def test_no_login_gives_a_clear_error(monkeypatch):
    import builtins

    real = builtins.__import__

    def without_datatailr(name, *args, **kwargs):
        if name == "datatailr":
            raise ImportError("no datatailr")
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_datatailr)
    with pytest.raises(RuntimeError, match="datatailr login"):
        chat.login_cookie()


def test_a_login_page_as_the_answer_gives_a_clear_error():
    class Models:
        def list(self):
            raise AttributeError("'str' object has no attribute '_set_private_attributes'")

    class Client:
        models = Models()

    with pytest.raises(RuntimeError, match="did not accept the login"):
        chat.served_model(Client())


def test_main_asks_the_service_also_with_a_model_set(monkeypatch, capsys):
    asked = []

    def served(client):
        asked.append(client)
        raise RuntimeError(chat.NOT_LOGGED_IN)

    monkeypatch.setattr(chat, "served_model", served)
    with pytest.raises(SystemExit) as stop:
        chat.main({"LLM_MODEL": "org/a"})
    assert len(asked) == 1
    assert "did not accept the login" in str(stop.value)


def _stream(*deltas):
    chunks = [SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(**delta))]) for delta in deltas]
    chunks.append(SimpleNamespace(choices=[]))  # the usage chunk has no choice
    create = lambda **kwargs: iter(chunks)  # noqa: E731
    return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))


@pytest.mark.parametrize("field", ["reasoning", "reasoning_content"])
def test_answer_shows_the_thinking_and_returns_only_the_reply(capsys, field):
    client = _stream(
        {"content": None, field: "Let me "},
        {"content": None, field: "see."},
        {"content": "Hello", field: None},
        {"content": "!", field: None},
    )
    reply = chat.answer(client, "org/m", [{"role": "user", "content": "hi"}])
    out = capsys.readouterr().out
    assert reply == "Hello!"
    assert out.index("Let me see.") < out.index("Hello!")
    assert chat.THINKING_END in out


def test_answer_works_without_a_reasoning_field(capsys):
    client = _stream({"content": "Hi"}, {"content": " there"})
    assert chat.answer(client, "org/m", []) == "Hi there"
    assert capsys.readouterr().out == "Hi there\n"
