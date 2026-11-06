"""The daily chat notice: one message to a webhook whose URL is a Datatailr secret."""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


def post_summary(text: str, secret_name: str = "desk_webhook_url") -> bool:
    """Post the text to the chat webhook. True on success; on any problem, warn and say False."""
    try:
        from datatailr import Secrets

        url = Secrets().get(secret_name)
    except Exception as err:
        logger.warning("Chat notice skipped: secret '%s' could not be read (%s).",
                       secret_name, err)
        return False
    try:
        response = requests.post(url, json={"text": text}, timeout=10)
    except Exception as err:
        # The webhook URL is the secret and requests puts it in its messages: log the type only.
        logger.warning("Chat notice failed: %s.", type(err).__name__)
        return False
    if not 200 <= response.status_code < 300:
        logger.warning("Chat notice failed: the webhook answered HTTP %s.", response.status_code)
        return False
    return True
