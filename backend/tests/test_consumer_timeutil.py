"""Tests for envelope timestamp conversion (app/consumer/timeutil.py).

Locks the Beijing wall-clock ⇄ UTC contract used by the downlink publisher
(now_beijing_str) and the inbound conversion chokepoint (parse_beijing_to_utc).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

from app.consumer.timeutil import BEIJING_TZ, format_beijing, now_beijing_str, parse_beijing_to_utc

# 2026-06-04 14:58:19.097  → space separator, 3-digit millis, no tz suffix
_FORMAT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$")


def test_now_beijing_str_matches_frontend_format():
    assert _FORMAT_RE.match(now_beijing_str())


def test_now_beijing_str_is_beijing_wall_clock():
    # The formatted wall-clock parsed back as Beijing must equal now within a few seconds.
    s = now_beijing_str()
    parsed = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=BEIJING_TZ)
    assert abs((parsed - datetime.now(BEIJING_TZ)).total_seconds()) < 5


def test_format_beijing_converts_utc_aware_to_beijing_wall_clock():
    # 06:58 UTC → 14:58 Beijing (UTC+8). Used for pong last_heartbeat (DB stores UTC).
    dt = datetime(2026, 6, 4, 6, 58, 19, 97000, tzinfo=UTC)
    assert format_beijing(dt) == "2026-06-04 14:58:19.097"


def test_format_beijing_treats_naive_as_utc():
    dt = datetime(2026, 6, 4, 6, 58, 19, 97000)  # naive == UTC by convention
    assert format_beijing(dt) == "2026-06-04 14:58:19.097"


def test_format_beijing_honours_other_offsets():
    # Same instant expressed as +09:00 → still maps to correct Beijing wall-clock.
    dt = datetime(2026, 6, 4, 15, 58, 19, 97000, tzinfo=timezone(timedelta(hours=9)))
    assert format_beijing(dt) == "2026-06-04 14:58:19.097"


def test_parse_naive_string_is_interpreted_as_beijing():
    # 14:58 Beijing == 06:58 UTC (UTC+8).
    dt = parse_beijing_to_utc("2026-06-04 14:58:19.097")
    assert dt == datetime(2026, 6, 4, 6, 58, 19, 97000, tzinfo=UTC)
    assert dt.tzinfo == UTC


def test_parse_accepts_t_separator_and_no_millis():
    assert parse_beijing_to_utc("2026-06-04T14:58:19") == datetime(2026, 6, 4, 6, 58, 19, tzinfo=UTC)


def test_parse_honours_explicit_offset():
    # Already carries +08:00 → not double-shifted.
    assert parse_beijing_to_utc("2026-06-04 14:58:19.097+08:00") == datetime(
        2026, 6, 4, 6, 58, 19, 97000, tzinfo=UTC
    )


def test_parse_honours_utc_z_suffix():
    assert parse_beijing_to_utc("2026-06-04T06:58:19.097Z") == datetime(
        2026, 6, 4, 6, 58, 19, 97000, tzinfo=UTC
    )


def test_roundtrip_now_str_parses_back_to_utc():
    before = datetime.now(UTC)
    parsed = parse_beijing_to_utc(now_beijing_str())
    assert parsed is not None
    assert abs((parsed - before).total_seconds()) < 5


def test_parse_empty_and_invalid_return_none():
    assert parse_beijing_to_utc("") is None
    assert parse_beijing_to_utc(None) is None
    assert parse_beijing_to_utc("not-a-timestamp") is None


def test_beijing_tz_is_utc_plus_8():
    assert BEIJING_TZ.utcoffset(None) == timedelta(hours=8)
    assert BEIJING_TZ == timezone(timedelta(hours=8))
