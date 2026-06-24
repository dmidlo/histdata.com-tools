"""Offline calendar-profile contracts for domain quality checks."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from histdatacom.runtime_contracts import JSONValue

CALENDAR_PROFILE_SCHEMA_VERSION = "histdatacom.calendar-profile.v1"
DEFAULT_CALENDAR_PROFILE_NAME = "static-major-holidays"
DEFAULT_CALENDAR_PROFILE_SOURCE = "static_month_day_major_holidays"

_UNIX_EPOCH_DATE = date(1970, 1, 1)


@dataclass(frozen=True, slots=True)
class HistDataCalendarDateTag:
    """One fixed or movable source-calendar date tag."""

    name: str
    tag: str
    month: int = 0
    day: int = 0
    rule: str = ""
    offset_days: int = 0
    category: str = "holiday"
    description: str = ""
    markets: tuple[str, ...] = ()
    asset_classes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate date-tag shape."""
        _validate_name(self.name, path="calendar_profile.date_tags.name")
        _validate_name(
            self.tag, path=f"calendar_profile.date_tags.{self.name}.tag"
        )
        if self.rule:
            _movable_date_for_year(self.rule, 2024, self.offset_days)
        elif not (1 <= self.month <= 12 and 1 <= self.day <= 31):
            msg = (
                f"calendar date tag {self.name!r} requires either a supported "
                "rule or month/day values"
            )
            raise ValueError(msg)

    @property
    def is_movable(self) -> bool:
        """Return whether the tag is computed from a calendar rule."""
        return bool(self.rule)

    def matches(self, source: datetime, *, asset_class: str = "") -> bool:
        """Return whether this tag applies to a source timestamp."""
        if not _scope_matches(self.asset_classes, asset_class):
            return False
        if self.rule:
            match = _movable_date_for_year(
                self.rule,
                source.year,
                self.offset_days,
            )
            return source.month == match.month and source.day == match.day
        return source.month == self.month and source.day == self.day

    def matches_fields(
        self,
        *,
        source_year: int,
        source_month: int,
        source_day: int,
        asset_class: str = "",
    ) -> bool:
        """Return whether this tag applies to projected source-date fields."""
        if not _scope_matches(self.asset_classes, asset_class):
            return False
        if self.rule:
            match = _movable_date_for_year(
                self.rule,
                source_year,
                self.offset_days,
            )
            return source_month == match.month and source_day == match.day
        return source_month == self.month and source_day == self.day

    def movable_date_for_year(self, year: int) -> date:
        """Return the source-calendar date for a movable tag in one year."""
        if not self.rule:
            return date(year, self.month, self.day)
        return _movable_date_for_year(self.rule, year, self.offset_days)

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible profile metadata."""
        metadata: dict[str, JSONValue] = {
            "name": self.name,
            "tag": self.tag,
            "category": self.category,
            "description": self.description,
            "markets": list(self.markets),
            "asset_classes": list(self.asset_classes),
            "movable": self.is_movable,
        }
        if self.rule:
            metadata["rule"] = self.rule
            metadata["offset_days"] = self.offset_days
        else:
            metadata["month"] = self.month
            metadata["day"] = self.day
        return metadata


@dataclass(frozen=True, slots=True)
class HistDataCalendarWindowTag:
    """One source-calendar date window tag."""

    name: str
    tag: str
    category: str
    start_month: int = 0
    start_day: int = 0
    end_month: int = 0
    end_day: int = 0
    start_date: str = ""
    end_date: str = ""
    description: str = ""
    markets: tuple[str, ...] = ()
    asset_classes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate window-tag shape."""
        _validate_name(self.name, path="calendar_profile.window_tags.name")
        _validate_name(
            self.tag,
            path=f"calendar_profile.window_tags.{self.name}.tag",
        )
        if self.start_date or self.end_date:
            _parse_iso_date(self.start_date, path=f"{self.name}.start_date")
            _parse_iso_date(self.end_date, path=f"{self.name}.end_date")
            if self.start_day_number > self.end_day_number:
                msg = (
                    f"calendar window tag {self.name!r} start_date must be "
                    "before or equal to end_date"
                )
                raise ValueError(msg)
            return
        for key, value in {
            "start_month": self.start_month,
            "end_month": self.end_month,
        }.items():
            if not 1 <= value <= 12:
                raise ValueError(
                    f"calendar window tag {self.name!r}: {key} invalid"
                )
        for key, value in {
            "start_day": self.start_day,
            "end_day": self.end_day,
        }.items():
            if not 1 <= value <= 31:
                raise ValueError(
                    f"calendar window tag {self.name!r}: {key} invalid"
                )

    @property
    def uses_absolute_dates(self) -> bool:
        """Return whether the window is a year-specific date range."""
        return bool(self.start_date or self.end_date)

    @property
    def start_day_number(self) -> int:
        """Return start date as days since Unix epoch."""
        return _source_day_number(
            _parse_iso_date(self.start_date, path=f"{self.name}.start_date")
        )

    @property
    def end_day_number(self) -> int:
        """Return end date as days since Unix epoch."""
        return _source_day_number(
            _parse_iso_date(self.end_date, path=f"{self.name}.end_date")
        )

    def matches(self, source: datetime, *, asset_class: str = "") -> bool:
        """Return whether this window applies to a source timestamp."""
        if not _scope_matches(self.asset_classes, asset_class):
            return False
        source_date = source.date()
        if self.uses_absolute_dates:
            return (
                self.start_day_number
                <= _source_day_number(source_date)
                <= (self.end_day_number)
            )
        return _month_day_in_window(
            source_date.month,
            source_date.day,
            start_month=self.start_month,
            start_day=self.start_day,
            end_month=self.end_month,
            end_day=self.end_day,
        )

    def matches_fields(
        self,
        *,
        source_month: int,
        source_day: int,
        source_day_number: int | None = None,
        asset_class: str = "",
    ) -> bool:
        """Return whether this window applies to projected source-date fields."""
        if not _scope_matches(self.asset_classes, asset_class):
            return False
        if self.uses_absolute_dates:
            return (
                source_day_number is not None
                and self.start_day_number
                <= source_day_number
                <= self.end_day_number
            )
        return _month_day_in_window(
            source_month,
            source_day,
            start_month=self.start_month,
            start_day=self.start_day,
            end_month=self.end_month,
            end_day=self.end_day,
        )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible profile metadata."""
        metadata: dict[str, JSONValue] = {
            "name": self.name,
            "tag": self.tag,
            "category": self.category,
            "description": self.description,
            "markets": list(self.markets),
            "asset_classes": list(self.asset_classes),
        }
        if self.uses_absolute_dates:
            metadata["start_date"] = self.start_date
            metadata["end_date"] = self.end_date
        else:
            metadata["start_month"] = self.start_month
            metadata["start_day"] = self.start_day
            metadata["end_month"] = self.end_month
            metadata["end_day"] = self.end_day
        return metadata


@dataclass(frozen=True, slots=True)
class HistDataCalendarProfile:
    """Offline holiday, event, and regime profile for HistData source dates."""

    name: str = DEFAULT_CALENDAR_PROFILE_NAME
    source: str = DEFAULT_CALENDAR_PROFILE_SOURCE
    version: str = "1"
    schema_version: str = CALENDAR_PROFILE_SCHEMA_VERSION
    complete: bool = False
    static_advisory: bool = True
    limitations: tuple[str, ...] = ()
    date_tags: tuple[HistDataCalendarDateTag, ...] = ()
    window_tags: tuple[HistDataCalendarWindowTag, ...] = ()

    def __post_init__(self) -> None:
        """Validate profile metadata."""
        if self.schema_version != CALENDAR_PROFILE_SCHEMA_VERSION:
            msg = (
                "unsupported calendar profile schema_version: "
                f"{self.schema_version!r}"
            )
            raise ValueError(msg)
        _validate_name(self.name, path="calendar_profile.name")
        _validate_name(self.source, path="calendar_profile.source")

    def holiday_tags_for(
        self,
        source: datetime,
        *,
        asset_class: str = "",
    ) -> tuple[str, ...]:
        """Return holiday tags for one source timestamp."""
        return tuple(
            tag.tag
            for tag in self.date_tags
            if tag.matches(source, asset_class=asset_class)
        )

    def holiday_tags_for_fields(
        self,
        *,
        source_year: int,
        source_month: int,
        source_day: int,
        asset_class: str = "",
    ) -> tuple[str, ...]:
        """Return holiday tags for projected source-date fields."""
        return tuple(
            tag.tag
            for tag in self.date_tags
            if tag.matches_fields(
                source_year=source_year,
                source_month=source_month,
                source_day=source_day,
                asset_class=asset_class,
            )
        )

    def event_tags_for(
        self,
        source: datetime,
        *,
        asset_class: str = "",
    ) -> tuple[str, ...]:
        """Return event/regime/window tags for one source timestamp."""
        return tuple(
            tag.tag
            for tag in self.window_tags
            if tag.matches(source, asset_class=asset_class)
        )

    def event_tags_for_fields(
        self,
        *,
        source_month: int,
        source_day: int,
        source_day_number: int | None = None,
        asset_class: str = "",
    ) -> tuple[str, ...]:
        """Return event/regime/window tags for projected source-date fields."""
        return tuple(
            tag.tag
            for tag in self.window_tags
            if tag.matches_fields(
                source_month=source_month,
                source_day=source_day,
                source_day_number=source_day_number,
                asset_class=asset_class,
            )
        )

    def applicable_date_tags(
        self,
        *,
        asset_class: str = "",
    ) -> tuple[HistDataCalendarDateTag, ...]:
        """Return date tags that can apply to the supplied asset class."""
        return tuple(
            tag
            for tag in self.date_tags
            if _scope_matches(tag.asset_classes, asset_class)
        )

    def applicable_window_tags(
        self,
        *,
        asset_class: str = "",
    ) -> tuple[HistDataCalendarWindowTag, ...]:
        """Return window tags that can apply to the supplied asset class."""
        return tuple(
            tag
            for tag in self.window_tags
            if _scope_matches(tag.asset_classes, asset_class)
        )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible profile metadata."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "source": self.source,
            "version": self.version,
            "complete": self.complete,
            "static_advisory": self.static_advisory,
            "limitations": list(self.limitations),
            "date_tags": [tag.to_metadata() for tag in self.date_tags],
            "window_tags": [tag.to_metadata() for tag in self.window_tags],
        }


def default_calendar_profile() -> HistDataCalendarProfile:
    """Return the static offline fallback calendar profile."""
    return HistDataCalendarProfile(
        limitations=(
            "No network-backed or exchange-specific holiday calendar is bundled; "
            "static holiday tags are advisory.",
        ),
        date_tags=(
            HistDataCalendarDateTag(
                name="new_years_day",
                tag="major_holiday:new_years_day",
                month=1,
                day=1,
                description="Static source-calendar New Year's Day tag.",
            ),
            HistDataCalendarDateTag(
                name="christmas_day",
                tag="major_holiday:christmas_day",
                month=12,
                day=25,
                description="Static source-calendar Christmas Day tag.",
            ),
        ),
    )


def calendar_profile_from_mapping(
    payload: Mapping[str, Any] | None,
) -> HistDataCalendarProfile:
    """Validate and return a calendar profile from a public mapping."""
    if not payload:
        return default_calendar_profile()
    _reject_unknown_keys(
        payload,
        {
            "schema_version",
            "name",
            "source",
            "version",
            "complete",
            "static_advisory",
            "limitations",
            "date_tags",
            "fixed_holidays",
            "movable_holidays",
            "window_tags",
            "event_windows",
        },
        "calendar_profile",
    )
    date_tags = [
        *_date_tags_from_value(
            payload.get("fixed_holidays"), path="fixed_holidays"
        ),
        *_date_tags_from_value(
            payload.get("movable_holidays"), path="movable_holidays"
        ),
        *_date_tags_from_value(payload.get("date_tags"), path="date_tags"),
    ]
    window_tags = [
        *_window_tags_from_value(
            payload.get("event_windows"), path="event_windows"
        ),
        *_window_tags_from_value(
            payload.get("window_tags"), path="window_tags"
        ),
    ]
    return HistDataCalendarProfile(
        schema_version=str(
            payload.get("schema_version") or CALENDAR_PROFILE_SCHEMA_VERSION
        ),
        name=str(payload.get("name") or "operator-calendar"),
        source=str(payload.get("source") or "operator-config"),
        version=str(payload.get("version") or "operator"),
        complete=_bool_value(payload.get("complete"), default=False),
        static_advisory=_bool_value(
            payload.get("static_advisory"), default=False
        ),
        limitations=_string_tuple(payload.get("limitations")),
        date_tags=tuple(date_tags),
        window_tags=tuple(window_tags),
    )


def _date_tags_from_value(
    value: Any,
    *,
    path: str,
) -> tuple[HistDataCalendarDateTag, ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple):
        raise ValueError(f"calendar_profile.{path} must be an array")
    return tuple(
        _date_tag_from_mapping(_expect_mapping(item, path=f"{path}[{index}]"))
        for index, item in enumerate(value)
    )


def _date_tag_from_mapping(
    payload: Mapping[str, Any],
) -> HistDataCalendarDateTag:
    _reject_unknown_keys(
        payload,
        {
            "name",
            "tag",
            "month",
            "day",
            "rule",
            "offset_days",
            "category",
            "description",
            "markets",
            "asset_classes",
        },
        "calendar_profile.date_tags",
    )
    name = str(payload.get("name") or "")
    category = str(payload.get("category") or "holiday")
    return HistDataCalendarDateTag(
        name=name,
        tag=str(payload.get("tag") or f"{category}:{name}"),
        month=_int_value(payload.get("month"), default=0),
        day=_int_value(payload.get("day"), default=0),
        rule=str(payload.get("rule") or ""),
        offset_days=_int_value(payload.get("offset_days"), default=0),
        category=category,
        description=str(payload.get("description") or ""),
        markets=_string_tuple(payload.get("markets")),
        asset_classes=_string_tuple(payload.get("asset_classes"), lower=True),
    )


def _window_tags_from_value(
    value: Any,
    *,
    path: str,
) -> tuple[HistDataCalendarWindowTag, ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple):
        raise ValueError(f"calendar_profile.{path} must be an array")
    return tuple(
        _window_tag_from_mapping(_expect_mapping(item, path=f"{path}[{index}]"))
        for index, item in enumerate(value)
    )


def _window_tag_from_mapping(
    payload: Mapping[str, Any],
) -> HistDataCalendarWindowTag:
    _reject_unknown_keys(
        payload,
        {
            "name",
            "tag",
            "category",
            "start_month",
            "start_day",
            "end_month",
            "end_day",
            "start_date",
            "end_date",
            "description",
            "markets",
            "asset_classes",
        },
        "calendar_profile.window_tags",
    )
    name = str(payload.get("name") or "")
    category = str(payload.get("category") or "event")
    return HistDataCalendarWindowTag(
        name=name,
        tag=str(payload.get("tag") or f"{category}:{name}"),
        category=category,
        start_month=_int_value(payload.get("start_month"), default=0),
        start_day=_int_value(payload.get("start_day"), default=0),
        end_month=_int_value(payload.get("end_month"), default=0),
        end_day=_int_value(payload.get("end_day"), default=0),
        start_date=str(payload.get("start_date") or ""),
        end_date=str(payload.get("end_date") or ""),
        description=str(payload.get("description") or ""),
        markets=_string_tuple(payload.get("markets")),
        asset_classes=_string_tuple(payload.get("asset_classes"), lower=True),
    )


def _movable_date_for_year(rule: str, year: int, offset_days: int) -> date:
    normalized = rule.strip().lower().replace("-", "_")
    if normalized in {"good_friday", "western_good_friday"}:
        return (
            _western_easter(year)
            - timedelta(days=2)
            + timedelta(days=offset_days)
        )
    if normalized in {
        "western_easter",
        "western_easter_offset",
        "western_easter_offset_days",
    }:
        return _western_easter(year) + timedelta(days=offset_days)
    if normalized in {"western_easter_minus_days", "easter_minus_days"}:
        return _western_easter(year) - timedelta(days=abs(offset_days))
    msg = f"unsupported calendar movable-date rule: {rule!r}"
    raise ValueError(msg)


def _western_easter(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    correction = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * correction) // 451
    month = (h + correction - 7 * m + 114) // 31
    day = ((h + correction - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _month_day_in_window(
    month: int,
    day: int,
    *,
    start_month: int,
    start_day: int,
    end_month: int,
    end_day: int,
) -> bool:
    value = month * 100 + day
    start = start_month * 100 + start_day
    end = end_month * 100 + end_day
    if start <= end:
        return start <= value <= end
    return value >= start or value <= end


def _source_day_number(source: date) -> int:
    return (source - _UNIX_EPOCH_DATE).days


def _parse_iso_date(value: str, *, path: str) -> date:
    if not value:
        raise ValueError(f"calendar_profile.{path} is required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        msg = f"calendar_profile.{path} must use YYYY-MM-DD format"
        raise ValueError(msg) from exc


def _scope_matches(configured: tuple[str, ...], value: str) -> bool:
    if not configured:
        return True
    return value.strip().lower() in configured


def _string_tuple(value: Any, *, lower: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items: Iterable[Any] = (value,)
    elif isinstance(value, list | tuple):
        items = value
    else:
        raise ValueError(
            "calendar profile string-list values must be strings or arrays"
        )
    result = []
    for item in items:
        text = str(item).strip()
        if text:
            result.append(text.lower() if lower else text)
    return tuple(dict.fromkeys(result))


def _bool_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"calendar profile boolean value invalid: {value!r}")


def _int_value(value: Any, *, default: int) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ValueError("calendar profile integer values cannot be booleans")
    return int(value)


def _expect_mapping(value: Any, *, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"calendar_profile.{path} must be an object")
    return value


def _reject_unknown_keys(
    payload: Mapping[str, Any],
    allowed: set[str],
    path: str,
) -> None:
    unknown = sorted(str(key) for key in payload if str(key) not in allowed)
    if unknown:
        msg = f"{path}: unknown keys: {', '.join(unknown)}"
        raise ValueError(msg)


def _validate_name(value: str, *, path: str) -> None:
    if not value.strip():
        raise ValueError(f"{path} is required")
