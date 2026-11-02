from datetime import date, datetime
from zoneinfo import ZoneInfo


def london_today() -> date:
    """Today's date in Europe/London, where the desk works."""
    return datetime.now(ZoneInfo("Europe/London")).date()
