"""Envelope timestamp conversion (Beijing wall-clock ⇄ UTC).

The frontend produces and consumes the MQ envelope ``timestamp`` in **Beijing
time** (UTC+8) using a tz-naive wall-clock string::

    2026-06-04 14:58:19.097   # space separator, 3-digit millis, no tz suffix

This module is the single conversion chokepoint:

* :func:`now_beijing_str` — formats "now" in that exact shape; used by the
  downlink publisher (``MQStreamBridge._build_envelope``) so the frontend reads
  result/progress/error/pong timestamps in its own timezone.
* :func:`parse_beijing_to_utc` — parses an inbound envelope timestamp into a
  UTC-aware ``datetime``. This is the canonical place to convert an uplink
  timestamp before *using* it. Note: as of today the inbound ``timestamp`` is
  carried but never consumed by scheduling/ordering (those use the integer
  ``message_seq`` / ``thread_msg_seq``), so this exists for logging and future
  use without affecting dispatch.

Beijing has had no DST since 1991, so a fixed UTC+8 offset is exact and avoids a
tzdata dependency inside slim GPU containers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

# Fixed UTC+8; exact for China Standard Time (no DST).
BEIJING_TZ = timezone(timedelta(hours=8))

# strftime pattern for the millisecond-precision wall-clock the frontend uses.
# Note: %f yields 6 digits, so millis are sliced manually in now_beijing_str.
_WALL_CLOCK_FMT = "%Y-%m-%d %H:%M:%S."


def format_beijing(dt: datetime) -> str:
    """Format any ``datetime`` as a Beijing wall-clock string, e.g. ``2026-06-04 14:58:19.097``.

    Aware datetimes are converted to UTC+8; a naive datetime is assumed to be UTC
    (matching the DB columns, which store ``datetime.now(UTC)``).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(BEIJING_TZ)
    return dt.strftime(_WALL_CLOCK_FMT) + f"{dt.microsecond // 1000:03d}"


def now_beijing_str() -> str:
    """Return the current time as a Beijing wall-clock string, e.g. ``2026-06-04 14:58:19.097``."""
    return format_beijing(datetime.now(UTC))


def parse_beijing_to_utc(value: str | None) -> datetime | None:
    """Parse an inbound envelope timestamp into a UTC-aware ``datetime``.

    Accepts the frontend's tz-naive Beijing wall-clock (``2026-06-04 14:58:19.097``,
    ``T`` separator, with or without millis). A naive value is interpreted as
    Beijing time; a value that already carries an offset (``Z`` / ``+08:00``) is
    honoured as-is. Returns ``None`` for empty or unparseable input rather than
    raising, so callers never break on a malformed timestamp.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BEIJING_TZ)
    return dt.astimezone(UTC)
