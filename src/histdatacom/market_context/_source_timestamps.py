"""Compatibility parsing without rewriting retained source timestamp text."""

from __future__ import annotations

import re
from datetime import datetime

_BASIC_OFFSET = re.compile(r"([+-][0-9]{2})([0-9]{2})$")


def parse_source_iso_datetime(value: str) -> datetime:
    """Parse source ``Z``/``+HHMM`` offsets on Python 3.10 and newer.

    Python 3.10's ``fromisoformat`` requires a colon in numeric UTC offsets.
    Normalize only that lexical spelling and terminal ``Z`` in a temporary
    string; callers retain the original text in their content-addressed
    evidence and continue to enforce their own timezone-awareness policy.
    Calendar/time/offset validity remains the standard-library parser's job.
    """
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        offset = _BASIC_OFFSET.search(normalized)
        if offset is None or offset.start() < 13:
            raise
        # A terminal date fragment must never become a timezone offset.
        # Require an independently valid date-and-time prefix with no offset.
        prefix = normalized[: offset.start()]
        if datetime.fromisoformat(prefix).tzinfo is not None:
            raise ValueError("multiple source timestamp offsets")
        return datetime.fromisoformat(
            prefix + offset.group(1) + ":" + offset.group(2)
        )
