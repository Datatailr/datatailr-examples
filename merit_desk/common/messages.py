"""Text from the platform, made fit for a run card on the page."""

from __future__ import annotations

import re

MAX_TEXT = 300  # characters of a message that reach the page


def clean_platform_text(text: str | None, default: str, limit: int = MAX_TEXT) -> str:
    """The platform's own words. The SDK puts the command line, with a temp file path in it,
    before them ("Command failed: dt ...\\n<stderr>"): keep the stderr only.
    Paths are removed and the text is cut at `limit` characters. An empty text gives `default`."""
    text = text or ""
    if text.startswith("Command timed out"):
        return "The platform did not answer in time."
    if text.startswith("Command failed: "):
        text = text.partition("\n")[2]
    text = re.sub(r"\s*file://[^\s\"']+", "", text).strip()
    if len(text) > limit:
        text = text[:limit - 3].rstrip() + "..."
    return text or default
