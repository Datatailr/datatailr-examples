"""A conversation with the LLM service in the terminal.

The service keeps no memory between requests. This script keeps the conversation: it
holds a list of messages, adds each prompt and each answer to it, and sends the list with
each request. So the model sees the whole conversation each time.

Usage, from a workstation or a job on the platform:
    pip install openai
    python chat.py

Usage, from a machine outside the platform:
    pip install openai datatailr
    datatailr login
    LLM_BASE_URL=https://<your-domain>/job/<environment>/<service name>/v1 python chat.py

Type a prompt and press Enter. The answer streams to the terminal.
    /reset    starts a new conversation
    Ctrl+C    during an answer stops that answer
    Ctrl+C    at the prompt ends the script

Environment variables, all optional:
    LLM_BASE_URL        URL of the API (default http://llm-service/v1)
    LLM_MODEL           model name (default: the model that the service serves now)
    LLM_SYSTEM_PROMPT   first message, which tells the model how to behave
    LLM_MAX_MESSAGES    number of past messages sent with each request (default 40)
    LLM_TOKEN           API token of a platform user, only for a client outside the platform

Who you are. On the platform, the platform adds your user name to each request. Outside the
platform, the script uses an https URL and must prove who you are. It has two ways:
    1. The login of the Datatailr CLI. Run `datatailr login` once. The script sends that login.
    2. An API token of your user in LLM_TOKEN. The script sends it as the query argument
       `__datatailr_user_cookie__`, which is the only place where the platform reads it.
If the answer is a login page, the script tells you so. Then log in again, or check the token.

The model has a limit for the size of one request. Old messages leave the request when the
conversation is longer than LLM_MAX_MESSAGES. Use a lower number if your messages are long.
Each request sends the past messages again, so the prompt tokens of a turn grow with the
conversation.
"""

from __future__ import annotations

import os

from openai import OpenAI

DEFAULT_BASE_URL = "http://llm-service/v1"
DEFAULT_SYSTEM_PROMPT = "You are a helpful assistant."
DEFAULT_MAX_MESSAGES = 40
TOKEN_ARGUMENT = "__datatailr_user_cookie__"


def login_cookie() -> str:
    """Return the cookie line of the Datatailr CLI login. Raise RuntimeError without a login."""
    try:
        import datatailr

        return datatailr.get_remote_oidc_cookie_line()
    except Exception as error:
        raise RuntimeError(
            "No login for a client outside the platform. Run `pip install datatailr` and "
            f"`datatailr login`, or set LLM_TOKEN. ({type(error).__name__}: {error})"
        ) from error


def make_client(env=os.environ, cookie=login_cookie) -> OpenAI:
    """Return a client for the service. The platform adds the user, so the key is unused.

    An https URL means a client outside the platform. Such a client sends the API token
    of LLM_TOKEN as a query argument, or the login of the Datatailr CLI as a cookie.
    """
    base_url = env.get("LLM_BASE_URL", "").strip() or DEFAULT_BASE_URL
    token = env.get("LLM_TOKEN", "").strip()
    headers, query = {}, {}
    if token:
        # On a job path, the platform edge reads the token only from this query argument.
        query[TOKEN_ARGUMENT] = token
    elif base_url.lower().startswith("https://"):
        headers["Cookie"] = cookie()
    return OpenAI(base_url=base_url, api_key="unused", default_headers=headers, default_query=query)


def served_model(client: OpenAI) -> str:
    """Return the name of the model that the service serves now."""
    try:
        models = list(client.models.list())
    except (AttributeError, TypeError, ValueError) as error:
        # A login page is HTML, so the client cannot read it as a list of models.
        raise RuntimeError(NOT_LOGGED_IN) from error
    for model in models:
        if getattr(model, "status", None) == "active":
            return model.id
    if not models:
        raise RuntimeError("the service lists no model")
    return models[0].id


def window(history: list[dict], max_messages: int) -> list[dict]:
    """Return the last `max_messages` messages. The first one is a user message."""
    recent = history[-max_messages:]
    while recent and recent[0]["role"] != "user":
        recent = recent[1:]
    return recent


def answer(client: OpenAI, model: str, messages: list[dict]) -> str:
    """Send the messages, print the answer while it streams, and return it."""
    stream = client.chat.completions.create(model=model, messages=messages, stream=True)
    reply = ""
    for chunk in stream:
        if chunk.choices:
            text = chunk.choices[0].delta.content or ""
            reply += text
            print(text, end="", flush=True)
    print()
    return reply


NOT_LOGGED_IN = (
    "The platform did not accept the login and answered with a page, not with the API. "
    "Run `datatailr login` again, or check LLM_TOKEN and LLM_BASE_URL."
)


def main(env=os.environ) -> None:
    try:
        client = make_client(env)
        model = env.get("LLM_MODEL", "").strip() or served_model(client)
    except RuntimeError as error:
        raise SystemExit(str(error)) from None
    system = {"role": "system", "content": env.get("LLM_SYSTEM_PROMPT") or DEFAULT_SYSTEM_PROMPT}
    max_messages = max(1, int(env.get("LLM_MAX_MESSAGES") or DEFAULT_MAX_MESSAGES))
    history: list[dict] = []
    print(f"Model: {model}. /reset starts a new conversation. Ctrl+C ends the script.")

    while True:
        try:
            prompt = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print()
            return
        if not prompt:
            continue
        if prompt == "/reset":
            history.clear()
            print("The conversation starts again.")
            continue

        history.append({"role": "user", "content": prompt})
        try:
            print("< ", end="", flush=True)
            reply = answer(client, model, [system] + window(history, max_messages))
        except KeyboardInterrupt:
            print("\nStopped this answer.")
            history.pop()  # forget the prompt that got no full answer
            continue
        except Exception as error:
            print(f"\nThe request failed: {error}")
            history.pop()
            continue
        history.append({"role": "assistant", "content": reply})


if __name__ == "__main__":
    main()
