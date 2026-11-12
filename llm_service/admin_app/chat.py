"""Check and build the chat request of the testing area."""

from __future__ import annotations

import math

MAX_MESSAGES = 50
MAX_CHARACTERS = 100_000
MAX_TOKENS = 4096
DEFAULT_MAX_TOKENS = 512
ROLES = ("system", "user", "assistant")


class ChatError(Exception):
    """The chat body breaks a rule. `code` is the error code of the answer."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def chat_request(body: object) -> dict:
    """Build the request for the service from the body of the browser.

    Keep `messages`, `max_tokens`, `model` and `temperature`. Drop each other
    key. Always set `stream` and `stream_options.include_usage`. Raise
    `ChatError` for a body that breaks a rule.
    """
    if not isinstance(body, dict):
        raise ChatError("invalid_request", "The request body must be a JSON object.")
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ChatError("invalid_request", "messages must be a list with at least one message.")
    if len(messages) > MAX_MESSAGES:
        raise ChatError("request_too_large", f"A request has at most {MAX_MESSAGES} messages.")
    kept = []
    characters = 0
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in ROLES:
            raise ChatError("invalid_request", "Each message needs the role system, user or assistant.")
        content = message.get("content")
        if not isinstance(content, str):
            raise ChatError("invalid_request", "The content of a message must be a string.")
        characters += len(content)
        kept.append({"role": message["role"], "content": content})
    if characters > MAX_CHARACTERS:
        raise ChatError("request_too_large", f"The messages have at most {MAX_CHARACTERS} characters in total.")
    max_tokens = body.get("max_tokens", DEFAULT_MAX_TOKENS)
    if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or not 1 <= max_tokens <= MAX_TOKENS:
        raise ChatError("invalid_request", f"max_tokens is an integer from 1 to {MAX_TOKENS}.")
    built = {
        "messages": kept,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if "model" in body:
        if not isinstance(body["model"], str):
            raise ChatError("invalid_request", "model must be a string.")
        built["model"] = body["model"]
    if "temperature" in body:
        if not _number(body["temperature"]) or not 0 <= body["temperature"] <= 2:
            raise ChatError("invalid_request", "temperature is a number from 0 to 2.")
        built["temperature"] = body["temperature"]
    return built
