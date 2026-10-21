"""Event-time windows shared by Stage 2 and Stage 3.

Taxi timestamps have no timezone. Both stages convert the clock reading
with time.gmtime-style math so a window id does not depend on the host TZ.
"""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta, timezone


def parse_event_time(text: str) -> datetime:
    return datetime.fromisoformat(text)


def window_bounds(moment: datetime, window_minutes: int) -> tuple[int, int]:
    width = int(window_minutes) * 60
    epoch = calendar.timegm(moment.timetuple())
    start = epoch - (epoch % width)
    return start, start + width


def format_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def watermark_time(max_event: datetime | None, allowed_lateness_seconds: int) -> datetime | None:
    if max_event is None:
        return None
    return max_event - timedelta(seconds=int(allowed_lateness_seconds))
