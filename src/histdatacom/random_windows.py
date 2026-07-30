"""Deterministic random and session-window selection for tick projections.

HistData source archives and canonical caches are monthly evidence.  This
module therefore resolves a compact selection contract during planning and
filters only consumer projections; it never rewrites source artifacts.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache, reduce
from operator import or_
from typing import Any, Mapping, Sequence, cast
from zoneinfo import ZoneInfo

RANDOM_WINDOW_EXPRESSION_SCHEMA_VERSION = (
    "histdatacom.random-window-expression.v1"
)
RANDOM_WINDOW_SELECTION_SCHEMA_VERSION = (
    "histdatacom.random-window-selection.v1"
)
RANDOM_WINDOW_SESSION_PROFILE_VERSION = (
    "histdatacom.random-window-session-profile.v1"
)
RANDOM_WINDOW_SELECTION_METADATA_KEY = "random_window_selection"

RANDOM_WINDOW_MODE_RANDOM = "random"
RANDOM_WINDOW_MODE_ALL_SESSIONS = "all_sessions"

MILLISECONDS_PER_MINUTE = 60_000
MAX_RANDOM_WINDOW_DURATION_COUNT = 1_000_000
MAX_RANDOM_WINDOW_SESSION_OCCURRENCES = 20_000

_DURATION_RE = re.compile(r"^([1-9][0-9]*)([yqMwdhm])$")
_SESSION_CODES = frozenset(
    {"fra", "ldn", "ny", "chi", "la", "auk", "syd", "tyo", "hk"}
)
_SMALL_DURATION_UNITS = frozenset({"h", "m"})
_LARGE_DURATION_UNITS = frozenset({"y", "q", "M", "w", "d"})


class RandomWindowError(ValueError):
    """Base class for bounded random-window contract failures."""


class RandomWindowSyntaxError(RandomWindowError):
    """A random-window expression is malformed or ambiguous."""


class RandomWindowSupportError(RandomWindowError):
    """Requested symbols or bounds do not contain a valid selection."""


class RandomWindowEmptySelectionError(RandomWindowError):
    """A resolved selection produced no projected tick rows."""


@dataclass(frozen=True, slots=True)
class RandomWindowSessionProfileV1:
    """Explicit local-clock sampling window for one legacy session code."""

    code: str
    label: str
    timezone_name: str
    start_minute_local: int = 8 * 60
    end_minute_local: int = 17 * 60
    schema_version: str = RANDOM_WINDOW_SESSION_PROFILE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RANDOM_WINDOW_SESSION_PROFILE_VERSION:
            raise ValueError("unsupported random-window session profile")
        if self.code not in _SESSION_CODES:
            raise ValueError("unsupported random-window session code")
        if not self.label.strip():
            raise ValueError("random-window session label is required")
        try:
            ZoneInfo(self.timezone_name)
        except Exception as err:
            raise ValueError("invalid random-window IANA timezone") from err
        for name, value in (
            ("start_minute_local", self.start_minute_local),
            ("end_minute_local", self.end_minute_local),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if not 0 <= value < 24 * 60:
                raise ValueError(f"{name} must be within one local day")
        if self.start_minute_local == self.end_minute_local:
            raise ValueError("random-window session cannot have zero duration")

    def to_dict(self) -> dict[str, Any]:
        """Return deterministic public session-profile metadata."""
        return {
            "schema_version": self.schema_version,
            "code": self.code,
            "label": self.label,
            "timezone": self.timezone_name,
            "start_minute_local": self.start_minute_local,
            "end_minute_local": self.end_minute_local,
            "dst_policy": "iana_zone_rules",
            "semantics": "sampling_window_not_exchange_hours",
        }


RANDOM_WINDOW_SESSION_PROFILES = {
    profile.code: profile
    for profile in (
        RandomWindowSessionProfileV1(
            code="fra",
            label="Frankfurt/Paris",
            timezone_name="Europe/Paris",
        ),
        RandomWindowSessionProfileV1(
            code="ldn",
            label="London",
            timezone_name="Europe/London",
        ),
        RandomWindowSessionProfileV1(
            code="ny",
            label="New York",
            timezone_name="America/New_York",
        ),
        RandomWindowSessionProfileV1(
            code="chi",
            label="Chicago",
            timezone_name="America/Chicago",
        ),
        RandomWindowSessionProfileV1(
            code="la",
            label="San Francisco/Los Angeles",
            timezone_name="America/Los_Angeles",
        ),
        RandomWindowSessionProfileV1(
            code="auk",
            label="Auckland/Wellington",
            timezone_name="Pacific/Auckland",
        ),
        RandomWindowSessionProfileV1(
            code="syd",
            label="Sydney",
            timezone_name="Australia/Sydney",
        ),
        RandomWindowSessionProfileV1(
            code="tyo",
            label="Tokyo",
            timezone_name="Asia/Tokyo",
        ),
        RandomWindowSessionProfileV1(
            code="hk",
            label="Hong Kong/Singapore",
            timezone_name="Asia/Hong_Kong",
        ),
    )
}


@dataclass(frozen=True, slots=True)
class RandomWindowExpressionV1:
    """Parsed deterministic duration or session expression."""

    expression: str
    duration_count: int | None = None
    duration_unit: str = ""
    prefix_minutes: int = 0
    start_session: str = ""
    bridge_count: int | None = None
    bridge_unit: str = ""
    end_session: str = ""
    suffix_minutes: int = 0
    schema_version: str = RANDOM_WINDOW_EXPRESSION_SCHEMA_VERSION

    @property
    def has_session(self) -> bool:
        """Return whether this expression is session anchored."""
        return bool(self.start_session)

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic parser payload."""
        return {
            "schema_version": self.schema_version,
            "expression": self.expression,
            "duration_count": self.duration_count,
            "duration_unit": self.duration_unit,
            "prefix_minutes": self.prefix_minutes,
            "start_session": self.start_session,
            "bridge_count": self.bridge_count,
            "bridge_unit": self.bridge_unit,
            "end_session": self.end_session,
            "suffix_minutes": self.suffix_minutes,
        }


@dataclass(frozen=True, slots=True)
class RandomWindowSelectionV1:
    """Compact resolved selection carried through Temporal work items."""

    expression: str
    mode: str
    support_start_utc_ms: int
    support_end_utc_ms: int
    seed: int | None = None
    selected_start_utc_ms: int | None = None
    selected_end_utc_ms: int | None = None
    occurrence_count: int = 1
    selection_id: str = ""
    schema_version: str = RANDOM_WINDOW_SELECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RANDOM_WINDOW_SELECTION_SCHEMA_VERSION:
            raise ValueError("unsupported random-window selection schema")
        specification = parse_random_window_expression(self.expression)
        if self.mode not in {
            RANDOM_WINDOW_MODE_RANDOM,
            RANDOM_WINDOW_MODE_ALL_SESSIONS,
        }:
            raise ValueError("unsupported random-window selection mode")
        _validate_ms_interval(
            self.support_start_utc_ms,
            self.support_end_utc_ms,
            "random-window support",
        )
        if self.seed is not None:
            _validate_seed(self.seed)
        if self.mode == RANDOM_WINDOW_MODE_RANDOM:
            if self.seed is None:
                raise ValueError("random-window selection requires a seed")
            if (
                self.selected_start_utc_ms is None
                or self.selected_end_utc_ms is None
            ):
                raise ValueError(
                    "random-window selection requires one interval"
                )
            _validate_ms_interval(
                self.selected_start_utc_ms,
                self.selected_end_utc_ms,
                "selected random window",
            )
            if (
                self.selected_start_utc_ms < self.support_start_utc_ms
                or self.selected_end_utc_ms > self.support_end_utc_ms
            ):
                raise ValueError("selected random window exceeds support")
            if self.occurrence_count != 1:
                raise ValueError("random mode must contain one occurrence")
            if specification.has_session:
                _validate_session_selection(
                    specification,
                    self.selected_start_utc_ms,
                    self.selected_end_utc_ms,
                )
            else:
                _validate_duration_selection(
                    specification,
                    self.selected_start_utc_ms,
                    self.selected_end_utc_ms,
                )
        elif (
            self.selected_start_utc_ms is not None
            or self.selected_end_utc_ms is not None
        ):
            raise ValueError(
                "all-session mode cannot persist expanded intervals"
            )
        elif not specification.has_session:
            raise ValueError("all-session mode requires a session expression")
        if (
            isinstance(self.occurrence_count, bool)
            or not isinstance(self.occurrence_count, int)
            or self.occurrence_count < 1
            or self.occurrence_count > MAX_RANDOM_WINDOW_SESSION_OCCURRENCES
        ):
            raise ValueError("random-window occurrence count is out of bounds")
        expected = _stable_id(
            "random-window-selection", self.identity_payload()
        )
        if self.selection_id and self.selection_id != expected:
            raise ValueError(
                "random-window selection ID does not match content"
            )
        object.__setattr__(self, "selection_id", expected)

    def identity_payload(self) -> dict[str, Any]:
        """Return fields that determine this selection."""
        return {
            "schema_version": self.schema_version,
            "expression": self.expression,
            "mode": self.mode,
            "support_start_utc_ms": self.support_start_utc_ms,
            "support_end_utc_ms": self.support_end_utc_ms,
            "seed": self.seed,
            "selected_start_utc_ms": self.selected_start_utc_ms,
            "selected_end_utc_ms": self.selected_end_utc_ms,
            "occurrence_count": self.occurrence_count,
            "session_profile_version": RANDOM_WINDOW_SESSION_PROFILE_VERSION,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return compact JSON-compatible selection metadata."""
        return {**self.identity_payload(), "selection_id": self.selection_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RandomWindowSelectionV1":
        """Restore and verify a serialized selection."""
        return cls(
            expression=str(data.get("expression", "")),
            mode=str(data.get("mode", "")),
            support_start_utc_ms=cast(int, data.get("support_start_utc_ms")),
            support_end_utc_ms=cast(int, data.get("support_end_utc_ms")),
            seed=cast(int | None, data.get("seed")),
            selected_start_utc_ms=cast(
                int | None, data.get("selected_start_utc_ms")
            ),
            selected_end_utc_ms=cast(
                int | None, data.get("selected_end_utc_ms")
            ),
            occurrence_count=cast(int, data.get("occurrence_count", 0)),
            selection_id=str(data.get("selection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def parse_random_window_expression(
    expression: str,
) -> RandomWindowExpressionV1:
    """Parse one documented duration or session expression."""
    normalized = str(expression).strip()
    if not normalized or normalized != expression:
        raise RandomWindowSyntaxError(
            "random-window expression must be non-empty without outer whitespace"
        )
    tokens = normalized.split("-")
    if any(not token for token in tokens):
        raise RandomWindowSyntaxError(
            "random-window expression contains an empty token"
        )
    session_positions = [
        index for index, token in enumerate(tokens) if token in _SESSION_CODES
    ]
    unknown = [
        token
        for token in tokens
        if token not in _SESSION_CODES and _duration_token(token) is None
    ]
    if unknown:
        raise RandomWindowSyntaxError(
            f"unsupported random-window token: {unknown[0]!r}"
        )
    if not session_positions:
        if len(tokens) != 1:
            raise RandomWindowSyntaxError(
                "duration-only random windows require exactly one token"
            )
        count, unit = cast(tuple[int, str], _duration_token(tokens[0]))
        return RandomWindowExpressionV1(
            expression=normalized,
            duration_count=count,
            duration_unit=unit,
        )
    if len(session_positions) > 2:
        raise RandomWindowSyntaxError(
            "session expressions support at most two session anchors"
        )

    first = session_positions[0]
    last = session_positions[-1]
    prefix = tokens[:first]
    between = tokens[first + 1 : last] if len(session_positions) == 2 else []
    suffix = tokens[last + 1 :]
    if len(prefix) > 1 or len(between) > 1 or len(suffix) > 1:
        raise RandomWindowSyntaxError(
            "random-window padding and bridge sections accept one token each"
        )
    prefix_minutes = _padding_minutes(prefix, "prefix")
    bridge_count: int | None = None
    bridge_unit = ""
    suffix_minutes = 0
    end_session = ""

    if len(session_positions) == 2:
        end_session = tokens[last]
        if between:
            bridge_count, bridge_unit = _large_duration(
                between[0], "session bridge"
            )
        suffix_minutes = _padding_minutes(suffix, "suffix")
    elif suffix:
        count, unit = cast(tuple[int, str], _duration_token(suffix[0]))
        if unit in _LARGE_DURATION_UNITS:
            bridge_count, bridge_unit = count, unit
        elif unit in _SMALL_DURATION_UNITS:
            suffix_minutes = _duration_minutes(count, unit)
        else:  # pragma: no cover - parser unit set is closed
            raise RandomWindowSyntaxError("unsupported session suffix")

    return RandomWindowExpressionV1(
        expression=normalized,
        prefix_minutes=prefix_minutes,
        start_session=tokens[first],
        bridge_count=bridge_count,
        bridge_unit=bridge_unit,
        end_session=end_session,
        suffix_minutes=suffix_minutes,
    )


def random_window_requires_seed(
    expression: str,
    *,
    start_yearmonth: str | None,
    end_yearmonth: str | None,
) -> bool:
    """Return whether CLI inputs resolve by seeded random selection."""
    spec = parse_random_window_expression(expression)
    return not (spec.has_session and start_yearmonth and end_yearmonth)


def resolve_random_window_selection(
    expression: str,
    *,
    seed: int | None,
    pairs: Sequence[str],
    repository_ranges: Mapping[str, Any],
    start_yearmonth: str | None = None,
    end_yearmonth: str | None = None,
    current_yearmonth: str | None = None,
) -> RandomWindowSelectionV1:
    """Resolve a selection against common repository support."""
    spec = parse_random_window_expression(expression)
    support_start, support_end = _common_support_interval(
        pairs,
        repository_ranges=repository_ranges,
        start_yearmonth=start_yearmonth,
        end_yearmonth=end_yearmonth,
        current_yearmonth=current_yearmonth,
    )
    all_sessions = bool(spec.has_session and start_yearmonth and end_yearmonth)
    if all_sessions:
        occurrences = _session_candidates(spec, support_start, support_end)
        if not occurrences:
            raise RandomWindowSupportError(
                "session expression has no occurrence inside common support"
            )
        return RandomWindowSelectionV1(
            expression=spec.expression,
            mode=RANDOM_WINDOW_MODE_ALL_SESSIONS,
            support_start_utc_ms=_to_ms(support_start),
            support_end_utc_ms=_to_ms(support_end),
            seed=seed,
            occurrence_count=len(occurrences),
        )

    if seed is None:
        raise RandomWindowSupportError(
            "random-window selection requires --random-seed"
        )
    _validate_seed(seed)
    rng = random.Random(seed)
    if spec.has_session:
        candidates = _session_candidates(spec, support_start, support_end)
        if not candidates:
            raise RandomWindowSupportError(
                "session expression has no occurrence inside common support"
            )
        selected_start, selected_end = candidates[
            rng.randrange(len(candidates))
        ]
    else:
        selected_start, selected_end = _duration_selection(
            spec,
            support_start,
            support_end,
            rng,
        )
    return RandomWindowSelectionV1(
        expression=spec.expression,
        mode=RANDOM_WINDOW_MODE_RANDOM,
        support_start_utc_ms=_to_ms(support_start),
        support_end_utc_ms=_to_ms(support_end),
        seed=seed,
        selected_start_utc_ms=_to_ms(selected_start),
        selected_end_utc_ms=_to_ms(selected_end),
    )


def random_window_planning_yearmonths(
    selection: RandomWindowSelectionV1,
) -> tuple[str, str]:
    """Return inclusive monthly source bounds for a resolved selection."""
    if selection.mode == RANDOM_WINDOW_MODE_RANDOM:
        assert selection.selected_start_utc_ms is not None
        assert selection.selected_end_utc_ms is not None
        start_ms = selection.selected_start_utc_ms
        end_ms = selection.selected_end_utc_ms
    else:
        start_ms = selection.support_start_utc_ms
        end_ms = selection.support_end_utc_ms
    start = _from_ms(start_ms)
    inclusive_end = _from_ms(end_ms - 1)
    return start.strftime("%Y%m"), inclusive_end.strftime("%Y%m")


def random_window_selection_from_metadata(
    metadata: Mapping[str, Any] | None,
) -> RandomWindowSelectionV1 | None:
    """Return a verified selection from work-item metadata."""
    if not metadata:
        return None
    if RANDOM_WINDOW_SELECTION_METADATA_KEY not in metadata:
        return None
    payload = metadata[RANDOM_WINDOW_SELECTION_METADATA_KEY]
    if not isinstance(payload, Mapping):
        raise RandomWindowError(
            "random-window work-item metadata must be a mapping"
        )
    return RandomWindowSelectionV1.from_dict(payload)


def random_window_intervals_for_range(
    selection: RandomWindowSelectionV1,
    *,
    range_start_utc_ms: int,
    range_end_utc_ms: int,
) -> tuple[tuple[int, int], ...]:
    """Return selected half-open intervals overlapping one bounded range."""
    _validate_ms_interval(
        range_start_utc_ms,
        range_end_utc_ms,
        "random-window projection range",
    )
    candidates: tuple[tuple[int, int], ...]
    if selection.mode == RANDOM_WINDOW_MODE_RANDOM:
        assert selection.selected_start_utc_ms is not None
        assert selection.selected_end_utc_ms is not None
        candidates = (
            (
                selection.selected_start_utc_ms,
                selection.selected_end_utc_ms,
            ),
        )
    else:
        candidates = _cached_all_session_intervals(
            selection.expression,
            selection.support_start_utc_ms,
            selection.support_end_utc_ms,
        )
    return tuple(
        (max(start, range_start_utc_ms), min(end, range_end_utc_ms))
        for start, end in candidates
        if start < range_end_utc_ms and end > range_start_utc_ms
    )


def filter_polars_frame_to_random_window(
    frame: Any,
    selection: RandomWindowSelectionV1 | Mapping[str, Any] | None,
    *,
    timestamp_column: str = "datetime",
) -> Any:
    """Filter one eager Polars frame to the exact selected interval union."""
    if selection is None:
        return frame
    resolved = (
        selection
        if isinstance(selection, RandomWindowSelectionV1)
        else RandomWindowSelectionV1.from_dict(selection)
    )
    if timestamp_column not in getattr(frame, "columns", ()):
        raise RandomWindowError(
            f"random-window filtering requires {timestamp_column!r}"
        )
    if int(getattr(frame, "height", 0)) == 0:
        return frame
    import polars as pl

    bounds = frame.select(
        pl.col(timestamp_column).min().alias("start"),
        pl.col(timestamp_column).max().alias("end"),
    ).row(0)
    range_start = int(bounds[0])
    range_end = int(bounds[1]) + 1
    intervals = random_window_intervals_for_range(
        resolved,
        range_start_utc_ms=range_start,
        range_end_utc_ms=range_end,
    )
    if not intervals:
        return frame.head(0)
    predicates = [
        (pl.col(timestamp_column) >= start) & (pl.col(timestamp_column) < end)
        for start, end in intervals
    ]
    return frame.filter(reduce(or_, predicates))


@lru_cache(maxsize=64)
def _cached_all_session_intervals(
    expression: str,
    support_start_utc_ms: int,
    support_end_utc_ms: int,
) -> tuple[tuple[int, int], ...]:
    spec = parse_random_window_expression(expression)
    candidates = _session_candidates(
        spec,
        _from_ms(support_start_utc_ms),
        _from_ms(support_end_utc_ms),
    )
    return tuple((_to_ms(start), _to_ms(end)) for start, end in candidates)


def _common_support_interval(
    pairs: Sequence[str],
    *,
    repository_ranges: Mapping[str, Any],
    start_yearmonth: str | None,
    end_yearmonth: str | None,
    current_yearmonth: str | None,
) -> tuple[datetime, datetime]:
    normalized_pairs = tuple(sorted({str(pair).lower() for pair in pairs}))
    if not normalized_pairs:
        raise RandomWindowSupportError(
            "random-window selection requires at least one symbol"
        )
    user_start = _yearmonth_start(start_yearmonth) if start_yearmonth else None
    user_end = _yearmonth_end(end_yearmonth) if end_yearmonth else None
    current_end = (
        _yearmonth_end(current_yearmonth) if current_yearmonth else None
    )
    starts: list[datetime] = []
    ends: list[datetime] = []
    inventory_available = bool(repository_ranges)
    for pair in normalized_pairs:
        pair_range = repository_ranges.get(pair)
        repo_start: datetime | None = None
        repo_end: datetime | None = None
        if isinstance(pair_range, Mapping):
            if pair_range.get("start"):
                repo_start = _yearmonth_start(str(pair_range["start"]))
            if pair_range.get("end"):
                repo_end = _yearmonth_end(str(pair_range["end"]))
        if repo_start is None or repo_end is None:
            if inventory_available:
                raise RandomWindowSupportError(
                    f"repository support is unavailable for {pair}"
                )
            if user_start is None or user_end is None:
                raise RandomWindowSupportError(
                    f"repository support is unavailable for {pair}"
                )
            repo_start, repo_end = user_start, user_end
        starts.append(max(item for item in (repo_start, user_start) if item))
        pair_ends = [item for item in (repo_end, user_end, current_end) if item]
        ends.append(min(pair_ends))
    support_start = max(starts)
    support_end = min(ends)
    if support_start >= support_end:
        raise RandomWindowSupportError(
            "requested symbols and bounds have no common repository support"
        )
    return support_start, support_end


def _duration_selection(
    spec: RandomWindowExpressionV1,
    support_start: datetime,
    support_end: datetime,
    rng: random.Random,
) -> tuple[datetime, datetime]:
    assert spec.duration_count is not None
    count = spec.duration_count
    unit = spec.duration_unit
    if unit in {"y", "q", "M"}:
        month_step = {"y": 12, "q": 3, "M": 1}[unit]
        duration_months = count * month_step
        starts: list[datetime] = []
        cursor = datetime(
            support_start.year,
            support_start.month,
            1,
            tzinfo=timezone.utc,
        )
        if unit == "y":
            cursor = datetime(cursor.year, 1, 1, tzinfo=timezone.utc)
            if cursor < support_start:
                cursor = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        elif unit == "q":
            quarter_month = ((cursor.month - 1) // 3) * 3 + 1
            cursor = datetime(
                cursor.year, quarter_month, 1, tzinfo=timezone.utc
            )
            if cursor < support_start:
                cursor = _add_months(cursor, 3)
        elif cursor < support_start:
            cursor = _add_months(cursor, 1)
        while True:
            end = _add_months(cursor, duration_months)
            if end > support_end:
                break
            starts.append(cursor)
            cursor = _add_months(cursor, month_step)
        if not starts:
            raise RandomWindowSupportError(
                "calendar-aligned duration does not fit inside common support"
            )
        start = starts[rng.randrange(len(starts))]
        return start, _add_months(start, duration_months)

    duration_minutes = _duration_minutes(count, unit)
    duration = timedelta(minutes=duration_minutes)
    latest_start = support_end - duration
    if latest_start < support_start:
        raise RandomWindowSupportError(
            "random-window duration exceeds common repository support"
        )
    slots = int((latest_start - support_start).total_seconds() // 60) + 1
    start_index = rng.randrange(slots)
    search_limit = min(slots, 7 * 24 * 60)
    for offset in range(search_limit):
        start = support_start + timedelta(
            minutes=(start_index + offset) % slots
        )
        if start.weekday() < 5:
            return start, start + duration
    raise RandomWindowSupportError(
        "duration selection has no weekday-aligned candidate"
    )


def _session_candidates(
    spec: RandomWindowExpressionV1,
    support_start: datetime,
    support_end: datetime,
) -> tuple[tuple[datetime, datetime], ...]:
    if not spec.has_session:
        raise RandomWindowSyntaxError(
            "session candidate generation requires a session expression"
        )
    profile = RANDOM_WINDOW_SESSION_PROFILES[spec.start_session]
    zone = ZoneInfo(profile.timezone_name)
    cursor = support_start.astimezone(zone).date() - timedelta(days=2)
    last = support_end.astimezone(zone).date() + timedelta(days=2)
    candidates: list[tuple[datetime, datetime]] = []
    while cursor <= last:
        if cursor.weekday() < 5:
            interval = _session_interval_for_date(spec, cursor)
            if interval[0] >= support_start and interval[1] <= support_end:
                candidates.append(interval)
                if len(candidates) > MAX_RANDOM_WINDOW_SESSION_OCCURRENCES:
                    raise RandomWindowSupportError(
                        "session selection exceeds occurrence resource limit"
                    )
        cursor += timedelta(days=1)
    return tuple(candidates)


def _session_interval_for_date(
    spec: RandomWindowExpressionV1,
    local_date: date,
) -> tuple[datetime, datetime]:
    start_profile = RANDOM_WINDOW_SESSION_PROFILES[spec.start_session]
    start_open, start_close = _session_bounds(start_profile, local_date)
    start = start_open - timedelta(minutes=spec.prefix_minutes)
    if spec.end_session:
        base = (
            _add_duration(start_open, spec.bridge_count, spec.bridge_unit)
            if spec.bridge_count is not None
            else start_open
        )
        end_profile = RANDOM_WINDOW_SESSION_PROFILES[spec.end_session]
        end_zone = ZoneInfo(end_profile.timezone_name)
        target_date = base.astimezone(end_zone).date()
        end_open, end_close = _session_bounds(end_profile, target_date)
        if spec.bridge_count is None:
            if spec.end_session == spec.start_session or end_open <= start_open:
                target_date += timedelta(days=1)
                end_open, end_close = _session_bounds(end_profile, target_date)
        while end_close <= base:
            target_date += timedelta(days=1)
            end_open, end_close = _session_bounds(end_profile, target_date)
        end = end_close
    elif spec.bridge_count is not None:
        end = _add_duration(start_open, spec.bridge_count, spec.bridge_unit)
    else:
        end = start_close
    end += timedelta(minutes=spec.suffix_minutes)
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    if start_utc >= end_utc:
        raise RandomWindowSyntaxError(
            "session expression resolves to an empty or reversed interval"
        )
    return start_utc, end_utc


def _session_bounds(
    profile: RandomWindowSessionProfileV1,
    local_date: date,
) -> tuple[datetime, datetime]:
    zone = ZoneInfo(profile.timezone_name)
    start = datetime.combine(
        local_date,
        time(
            hour=profile.start_minute_local // 60,
            minute=profile.start_minute_local % 60,
        ),
        tzinfo=zone,
    )
    end_date = local_date
    if profile.end_minute_local < profile.start_minute_local:
        end_date += timedelta(days=1)
    end = datetime.combine(
        end_date,
        time(
            hour=profile.end_minute_local // 60,
            minute=profile.end_minute_local % 60,
        ),
        tzinfo=zone,
    )
    return start, end


def _add_duration(
    value: datetime,
    count: int | None,
    unit: str,
) -> datetime:
    if count is None:
        return value
    if unit == "y":
        return _add_months(value, count * 12)
    if unit == "q":
        return _add_months(value, count * 3)
    if unit == "M":
        return _add_months(value, count)
    return value + timedelta(minutes=_duration_minutes(count, unit))


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.year * 12 + value.month - 1 + months
    year, zero_month = divmod(month_index, 12)
    month = zero_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _duration_token(token: str) -> tuple[int, str] | None:
    match = _DURATION_RE.fullmatch(token)
    if match is None:
        return None
    count = int(match.group(1))
    if count > MAX_RANDOM_WINDOW_DURATION_COUNT:
        raise RandomWindowSyntaxError(
            "random-window duration count exceeds resource limit"
        )
    return count, match.group(2)


def _large_duration(token: str, name: str) -> tuple[int, str]:
    count, unit = cast(tuple[int, str], _duration_token(token))
    if unit not in _LARGE_DURATION_UNITS:
        raise RandomWindowSyntaxError(f"{name} requires d, w, M, q, or y")
    return count, unit


def _padding_minutes(tokens: Sequence[str], name: str) -> int:
    if not tokens:
        return 0
    count, unit = cast(tuple[int, str], _duration_token(tokens[0]))
    if unit not in _SMALL_DURATION_UNITS:
        raise RandomWindowSyntaxError(f"{name} padding requires h or m")
    return _duration_minutes(count, unit)


def _duration_minutes(count: int, unit: str) -> int:
    factors = {
        "w": 7 * 24 * 60,
        "d": 24 * 60,
        "h": 60,
        "m": 1,
    }
    try:
        return count * factors[unit]
    except KeyError as err:
        raise RandomWindowSyntaxError(
            f"{unit!r} is not a fixed-duration unit"
        ) from err


def _yearmonth_start(value: str | None) -> datetime:
    normalized = _normalize_yearmonth(value, end=False)
    return datetime(
        int(normalized[:4]),
        int(normalized[4:]),
        1,
        tzinfo=timezone.utc,
    )


def _yearmonth_end(value: str | None) -> datetime:
    normalized = _normalize_yearmonth(value, end=True)
    start = datetime(
        int(normalized[:4]),
        int(normalized[4:]),
        1,
        tzinfo=timezone.utc,
    )
    return _add_months(start, 1)


def _normalize_yearmonth(value: str | None, *, end: bool) -> str:
    normalized = str(value or "").replace("-", "")
    if len(normalized) == 4 and normalized.isdigit():
        return f"{normalized}{'12' if end else '01'}"
    if len(normalized) != 6 or not normalized.isdigit():
        raise RandomWindowSupportError(
            "random-window bounds require YYYY or YYYYMM values"
        )
    month = int(normalized[4:])
    if not 1 <= month <= 12:
        raise RandomWindowSupportError(
            "random-window bound month must be between 01 and 12"
        )
    return normalized


def _validate_seed(seed: int) -> None:
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("random-window seed must be a non-negative integer")
    if seed > 2**63 - 1:
        raise ValueError("random-window seed exceeds signed int64 range")


def _validate_ms_interval(start: int, end: int, name: str) -> None:
    for field_name, value in (("start", start), ("end", end)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} {field_name} must be an integer")
        if value < 0 or value > 2**63 - 1:
            raise ValueError(f"{name} {field_name} exceeds int64 range")
    if start >= end:
        raise ValueError(f"{name} must be a non-empty half-open interval")


def _validate_duration_selection(
    specification: RandomWindowExpressionV1,
    start_ms: int,
    end_ms: int,
) -> None:
    """Verify a serialized duration selection still matches its expression."""
    assert specification.duration_count is not None
    count = specification.duration_count
    unit = specification.duration_unit
    start = _from_ms(start_ms)
    end = _from_ms(end_ms)
    if unit in {"y", "q", "M"}:
        month_step = {"y": 12, "q": 3, "M": 1}[unit]
        if start != datetime(
            start.year,
            start.month,
            1,
            tzinfo=timezone.utc,
        ):
            raise ValueError("calendar random window is not month aligned")
        if unit == "y" and start.month != 1:
            raise ValueError("calendar-year random window is not year aligned")
        if unit == "q" and start.month not in {1, 4, 7, 10}:
            raise ValueError("calendar-quarter random window is not aligned")
        expected_end = _add_months(start, count * month_step)
    else:
        if start_ms % MILLISECONDS_PER_MINUTE:
            raise ValueError("fixed random window is not UTC-minute aligned")
        expected_end = start + timedelta(minutes=_duration_minutes(count, unit))
    if end != expected_end:
        raise ValueError("selected random window does not match its expression")


def _validate_session_selection(
    specification: RandomWindowExpressionV1,
    start_ms: int,
    end_ms: int,
) -> None:
    """Verify a compact session selection against its local-clock profile."""
    profile = RANDOM_WINDOW_SESSION_PROFILES[specification.start_session]
    local_date = (
        (_from_ms(start_ms) + timedelta(minutes=specification.prefix_minutes))
        .astimezone(ZoneInfo(profile.timezone_name))
        .date()
    )
    if local_date.weekday() >= 5:
        raise ValueError("selected session random window starts on a weekend")
    expected_start, expected_end = _session_interval_for_date(
        specification,
        local_date,
    )
    if (start_ms, end_ms) != (_to_ms(expected_start), _to_ms(expected_end)):
        raise ValueError(
            "selected session window does not match its expression"
        )


def _to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


__all__ = [
    "MAX_RANDOM_WINDOW_SESSION_OCCURRENCES",
    "RANDOM_WINDOW_EXPRESSION_SCHEMA_VERSION",
    "RANDOM_WINDOW_MODE_ALL_SESSIONS",
    "RANDOM_WINDOW_MODE_RANDOM",
    "RANDOM_WINDOW_SELECTION_METADATA_KEY",
    "RANDOM_WINDOW_SELECTION_SCHEMA_VERSION",
    "RANDOM_WINDOW_SESSION_PROFILES",
    "RANDOM_WINDOW_SESSION_PROFILE_VERSION",
    "RandomWindowEmptySelectionError",
    "RandomWindowError",
    "RandomWindowExpressionV1",
    "RandomWindowSelectionV1",
    "RandomWindowSessionProfileV1",
    "RandomWindowSupportError",
    "RandomWindowSyntaxError",
    "filter_polars_frame_to_random_window",
    "parse_random_window_expression",
    "random_window_intervals_for_range",
    "random_window_planning_yearmonths",
    "random_window_requires_seed",
    "random_window_selection_from_metadata",
    "resolve_random_window_selection",
]
