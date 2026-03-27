from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

_DURATION_RESOLUTION_RE = re.compile(r"-(\d+)([mh])-([0-9]{10})$")
_MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_CALENDAR_RESOLUTION_RE = re.compile(
    r"-(january|february|march|april|may|june|july|august|september|october|november|december)-"
    r"([0-9]{1,2})(?:-([0-9]{4}))?-([0-9]{1,4})(am|pm)-et$"
)
_ET_ZONE = ZoneInfo("America/New_York")


def infer_resolution_timestamp_seconds_from_slug(
    slug: str | None,
    reference_timestamp_seconds: int | None = None,
) -> int | None:
    if not slug:
        return None
    duration_match = _DURATION_RESOLUTION_RE.search(slug)
    if duration_match:
        duration = int(duration_match.group(1))
        unit = duration_match.group(2)
        start_ts = int(duration_match.group(3))
        return start_ts + duration * (60 if unit == "m" else 3600)
    calendar_match = _CALENDAR_RESOLUTION_RE.search(slug)
    if not calendar_match:
        return None
    month = _MONTH_NAME_TO_NUMBER[calendar_match.group(1)]
    day = int(calendar_match.group(2))
    year = _infer_calendar_year(calendar_match.group(3), reference_timestamp_seconds)
    parsed_time = _parse_calendar_time_token(calendar_match.group(4), calendar_match.group(5))
    if parsed_time is None:
        return None
    hour, minute = parsed_time
    return int(datetime(year, month, day, hour, minute, tzinfo=_ET_ZONE).timestamp()) + 3600


def _infer_calendar_year(raw_year: str | None, reference_timestamp_seconds: int | None) -> int:
    if raw_year:
        return int(raw_year)
    if reference_timestamp_seconds is not None:
        return datetime.fromtimestamp(reference_timestamp_seconds, tz=_ET_ZONE).year
    return datetime.now(_ET_ZONE).year


def _parse_calendar_time_token(time_token: str, meridiem: str) -> Optional[Tuple[int, int]]:
    if not time_token.isdigit():
        return None
    if len(time_token) <= 2:
        hour = int(time_token)
        minute = 0
    elif len(time_token) == 3:
        hour = int(time_token[0])
        minute = int(time_token[1:])
    elif len(time_token) == 4:
        hour = int(time_token[:2])
        minute = int(time_token[2:])
    else:
        return None
    if hour < 1 or hour > 12 or minute < 0 or minute >= 60:
        return None
    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    return hour, minute
