"""AI summarizer for vendor emails.

Tries OpenAI's chat completion API first (using a key fetched from the
Datatailr Secrets manager or the OPENAI_API_KEY environment variable).
Falls back to a deterministic extractive summarizer so the demo runs even
without an API key configured.
"""

from __future__ import annotations

import logging
import os
import re

log = logging.getLogger("summarizer")
log.setLevel(logging.INFO)


_PROMPT_SYSTEM = (
    "You are an assistant for a buy-side trading desk. You receive vendor "
    "emails -- research notes, broker reports, newswire alerts -- about "
    "specific stocks. For each email, produce a JSON object with these keys:\n"
    "  summary: 1-2 sentence summary suitable for a trader.\n"
    "  key_points: list of up to 3 bullet strings highlighting the most "
    "important facts (numbers, ratings, catalysts).\n"
    "  sentiment: one of 'bullish', 'bearish', 'neutral'.\n"
    "  action: a short suggested next action for the trader (<= 12 words).\n"
    "Return ONLY the JSON object, no prose."
)


def _get_openai_key() -> str | None:
    key = os.environ.get("OPENAI_API_KEY")
    if key:
        return key.strip() or None
    try:
        from datatailr import Secrets

        return (Secrets().get("OPENAI_API_KEY") or "").strip() or None
    except Exception as exc:
        log.debug("OPENAI_API_KEY not available from Secrets: %s", exc)
        return None


_BULLISH_WORDS = (
    "buy", "overweight", "raise", "raises", "beat", "beats", "outperform",
    "constructive", "ahead of", "upside", "positive", "expansion", "wins",
)
_BEARISH_WORDS = (
    "sell", "underweight", "cut", "cuts", "miss", "misses", "below", "headwind",
    "delays", "warning", "negative", "downgrade", "incident",
)


def _heuristic_sentiment(text: str) -> str:
    t = text.lower()
    pos = sum(t.count(w) for w in _BULLISH_WORDS)
    neg = sum(t.count(w) for w in _BEARISH_WORDS)
    if pos > neg + 1:
        return "bullish"
    if neg > pos + 1:
        return "bearish"
    return "neutral"


def _heuristic_summary(email: dict) -> dict:
    body = email.get("body", "")
    subject = email.get("subject", "")

    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", body) if s.strip()]
    summary = " ".join(sentences[:2]) if sentences else subject

    numeric_lines = [s for s in sentences if re.search(r"\d", s)]
    key_points = numeric_lines[:3] if numeric_lines else sentences[:3]

    sentiment = _heuristic_sentiment(subject + " " + body)
    if sentiment == "bullish":
        action = f"Consider adding {email.get('ticker')} on the next pullback."
    elif sentiment == "bearish":
        action = f"Review {email.get('ticker')} exposure; tighten stops."
    else:
        action = f"Monitor {email.get('ticker')} into next catalyst."

    return {
        "summary": summary,
        "key_points": key_points,
        "sentiment": sentiment,
        "action": action,
        "model": "heuristic-extractive",
    }


def _llm_summary(email: dict, api_key: str) -> dict | None:
    """Try the OpenAI API; return None on any failure so caller can fall back."""
    try:
        from openai import OpenAI
    except ImportError:
        log.warning("openai package not installed; using heuristic summary")
        return None

    model_name = os.environ.get("EMAIL_SUMMARY_MODEL", "gpt-4o-mini")
    user_msg = (
        f"From: {email.get('from_name')} <{email.get('from_email')}>\n"
        f"Ticker: {email.get('ticker')}\n"
        f"Subject: {email.get('subject')}\n\n"
        f"{email.get('body')}"
    )

    try:
        client = OpenAI(api_key=api_key)
        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": _PROMPT_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=400,
        )
        raw = completion.choices[0].message.content or ""
    except Exception as exc:
        log.warning("OpenAI call failed (%s); falling back to heuristic", exc)
        return None

    import json
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("OpenAI returned non-JSON content; falling back")
        return None

    return {
        "summary": str(parsed.get("summary", "")).strip(),
        "key_points": [str(x) for x in (parsed.get("key_points") or [])][:3],
        "sentiment": str(parsed.get("sentiment", "neutral")).lower(),
        "action": str(parsed.get("action", "")).strip(),
        "model": model_name,
    }


def summarize_email(email: dict) -> dict:
    """Return a structured summary for one email, never raising."""
    api_key = _get_openai_key()
    if api_key:
        result = _llm_summary(email, api_key)
        if result and result.get("summary"):
            return result
    return _heuristic_summary(email)


def summarize_emails(emails: list[dict]) -> list[dict]:
    out: list[dict] = []
    for em in emails:
        s = summarize_email(em)
        out.append({**em, "ai_summary": s})
    return out
