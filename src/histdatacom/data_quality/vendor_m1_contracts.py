"""Bounded validation-only HistData M1 wires, never dataset authority.

Decimal lexemes and reduced rational differences are distinct from the native
floating bar projection. Parsing a report checks structure, not source truth;
only the source-bound replay operation recomputes its claims.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, fields
from fractions import Fraction
from typing import Any, ClassVar, TypeVar, cast

MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_REPORT_BYTES = 64 * 1024 * 1024
MAX_TICKS = 262_144
MAX_REFERENCE_ROWS = 65_536
MAX_MINUTES = 65_536
MAX_LINE_BYTES = 512
MAX_REGRESSION_MS = 3_600_000
MAX_REGRESSIONS = 1_024
MAX_JSON_STRUCTURAL_TOKENS = 1_048_576
MINUTE_NS = 60_000_000_000
OHLC = ("open", "high", "low", "close")
STATUSES = (
    "exact_match",
    "bounded_rounding_match",
    "material_mismatch",
    "missing_m1",
    "missing_tick_support",
    "partial_query_refused",
)
VOLUME_STATUS = "unqualified_nominal_integer_not_traded_volume"
CLOCK_POLICY = "histdata-fixed-est-no-dst-plus-five-hours-v1"
PRICE_POLICY = "raw-provider-bid-no-quote-repair-v1"
DIFFERENCE_CONVENTION = "tick_minus_vendor_v1"
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RATIO = re.compile(r"-?(?:0|[1-9][0-9]*)/[1-9][0-9]*\Z")
R = TypeVar("R", bound="_Wire")


def require_int(
    value: Any, name: str, low: int = 0, high: int = 2**63 - 1
) -> int:
    """Reject coercion and booleans at integer boundaries."""
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"invalid {name}")
    return value


def require_sha(value: Any) -> str:
    """Read an exact lower-case SHA-256, not a caller truth assertion."""
    if type(value) is not str or not _SHA.fullmatch(value):
        raise ValueError("invalid sha256")
    return value


def require_id(value: Any, prefix: str) -> str:
    """Check a selected immutable root before reading its large body."""
    lead = prefix + ":sha256:"
    if type(value) is not str or not value.startswith(lead):
        raise ValueError(f"invalid {prefix} identity")
    require_sha(value[len(lead) :])
    return value


def decimal_value(value: Any, *, positive: bool = True) -> Fraction:
    """Parse bounded plain decimal exactly, without Decimal context or floats."""
    if (
        type(value) is not str
        or len(value) > 33
        or not _DECIMAL.fullmatch(value)
        or len(value.replace(".", "")) > 32
        or ("." in value and len(value.split(".")[1]) > 15)
    ):
        raise ValueError("invalid bounded decimal lexeme")
    result = Fraction(value)
    if result < 0 or (positive and not result):
        raise ValueError("decimal must be positive")
    return bounded_fraction(result)


def bounded_fraction(value: Fraction) -> Fraction:
    """Bound intermediate/output exact arithmetic to 256-bit components."""
    if (
        max(abs(value.numerator).bit_length(), value.denominator.bit_length())
        > 256
    ):
        raise ValueError("exact arithmetic exceeds 256-bit bound")
    return value


def rational_text(value: Fraction) -> str:
    """Serialize exact values independently of JSON numeric precision."""
    bounded_fraction(value)
    return f"{value.numerator}/{value.denominator}"


def _ratio(value: Any) -> Fraction:
    if (
        type(value) is not str
        or len(value) > 157
        or not _RATIO.fullmatch(value)
    ):
        raise ValueError("invalid rational text")
    numerator, denominator = value.split("/")
    result = bounded_fraction(Fraction(int(numerator), int(denominator)))
    if rational_text(result) != value:
        raise ValueError("rational text must be reduced")
    return result


def _plain_budget(value: Any, depth: int = 0) -> int:
    if depth > 20:
        raise ValueError("wire nesting exceeds bound")
    if value is None:
        return 4
    if type(value) is bool:
        return 5
    if type(value) is int:
        if abs(value).bit_length() > 256:
            raise ValueError("wire integer exceeds bound")
        return 80
    if type(value) is str:
        if len(value) > MAX_REPORT_BYTES:
            raise ValueError("wire string exceeds bound")
        # ASCII-only wire is intentional; source grammar is ASCII too.
        if not value.isascii():
            raise ValueError("wire text must be ASCII")
        return 2 + sum(
            6 if ord(c) < 32 or ord(c) == 127 else 2 if c in '\\"' else 1
            for c in value
        )
    if type(value) in (dict, list):
        if len(value) > MAX_MINUTES:
            raise ValueError("wire collection exceeds bound")
        total = 2
        for key, item in (
            value.items() if type(value) is dict else ((None, x) for x in value)
        ):
            if key is not None:
                if type(key) is not str:
                    raise ValueError("wire keys must be strings")
                total += _plain_budget(key, depth + 1) + 1
            total += _plain_budget(item, depth + 1) + 1
            if total > MAX_REPORT_BYTES:
                raise ValueError("wire exceeds byte bound")
        return total
    raise ValueError("unsupported wire value")


def canonical(value: Any) -> str:
    """Preflight a plain bounded wire before canonical ASCII encoding."""
    if _plain_budget(value) > MAX_REPORT_BYTES:
        raise ValueError("wire exceeds byte bound")
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate wire key")
        result[key] = value
    return result


def _json_integer(text: str) -> int:
    # Python 3.10 does not provide the later interpreter digit-count guard.
    if len(text) > 80:
        raise ValueError("JSON integer token exceeds 256-bit work bound")
    result = int(text)
    if abs(result).bit_length() > 256:
        raise ValueError("JSON integer exceeds 256-bit bound")
    return result


def _json_float(text: str) -> Any:
    del text
    raise ValueError("floating JSON numbers are not admitted exact wire values")


def _json(text: str) -> dict[str, Any]:
    if (
        type(text) is not str
        or len(text) > MAX_REPORT_BYTES
        or not text.isascii()
    ):
        raise ValueError("invalid bounded ASCII JSON")
    depth = 0
    structural_tokens = 0
    quoted = escaped = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            structural_tokens += 1
            depth += 1
            if depth > 20:
                raise ValueError("wire nesting exceeds bound")
        elif char in ",:":
            structural_tokens += 1
        elif char in "]}":
            depth -= 1
        if structural_tokens > MAX_JSON_STRUCTURAL_TOKENS:
            raise ValueError(
                "JSON structural token bound exceeded before decode"
            )
    try:
        result = json.loads(
            text,
            object_pairs_hook=_pairs,
            parse_int=_json_integer,
            parse_float=_json_float,
            parse_constant=lambda _: (_ for _ in ()).throw(
                ValueError("nonfinite JSON")
            ),
        )
    except (RecursionError, OverflowError) as exc:
        raise ValueError("invalid bounded JSON") from exc
    if type(result) is not dict or canonical(result) != text:
        raise ValueError("wire must be canonical JSON")
    return result


def _mapping(value: Any, expected: set[str]) -> dict[str, Any]:
    if (
        type(value) is not dict
        or len(value) != len(expected)
        or set(value) != expected
    ):
        raise ValueError("wire fields differ")
    return value


def _tuple(value: Any, maximum: int) -> tuple[Any, ...]:
    if type(value) is not list or len(value) > maximum:
        raise ValueError("invalid bounded wire array")
    return tuple(value)


def _optional_time(value: Any) -> None:
    if value is not None:
        require_int(value, "availability/as-of time")


def _axis(symbol: Any, period: Any) -> None:
    if type(symbol) is not str or not re.fullmatch(r"[A-Z]{6}", symbol):
        raise ValueError("symbol must be six upper-case letters")
    if type(period) is not str or not re.fullmatch(r"[0-9]{6}", period):
        raise ValueError("source_period must be YYYYMM")
    require_int(int(period[:4]), "year", 1970, 2261)
    require_int(int(period[4:]), "month", 1, 12)


class _Wire:
    """Closed immutable wire implementation; no arbitrary callback registry."""

    schema_version: ClassVar[str]
    PREFIX: ClassVar[str]
    artifact_id: str

    def _payload(self) -> dict[str, Any]:
        raise NotImplementedError

    def _seal(self) -> None:
        if type(self.artifact_id) is not str:
            raise ValueError("wire identity must be text")
        expected = (
            self.PREFIX
            + ":sha256:"
            + hashlib.sha256(
                canonical(self._payload()).encode("ascii")
            ).hexdigest()
        )
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("wire identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def to_dict(self) -> dict[str, Any]:
        """Re-admit exact fields before invoking any nested serializer."""
        admitted = readmit(self, type(self))
        return {**admitted._payload(), "artifact_id": admitted.artifact_id}

    def to_json(self) -> str:
        """Return canonical bounded JSON, without a trailing newline."""
        return canonical(self.to_dict())

    @classmethod
    def from_json(cls: type[R], text: str) -> R:  # noqa: PYI019
        """Parse structural claims only; not a source replay qualification."""
        return cls.from_dict(_json(text))

    @classmethod
    def from_dict(cls: type[R], value: dict[str, Any]) -> R:  # noqa: PYI019
        """Reconstruct the closed wire and verify every derived identity."""
        if cls not in _TYPES:
            raise ValueError("unsupported wire class")
        names = {item.name for item in fields(cast(Any, cls))}
        extra = (
            {
                "volume_status",
                "clock_policy",
                "price_policy",
                "claim_kind",
                "availability_decision",
                "as_of_eligible",
                "difference_convention",
                "status_counts",
                "native_tick_parser",
            }
            if cls is VendorM1ReportV1
            else (
                {"difference_convention"}
                if cls is VendorM1DifferenceV1
                else set()
            )
        )
        body = _mapping(value, names | {"schema_version"} | extra)
        if body["schema_version"] != cls.schema_version:
            raise ValueError("unsupported wire schema")
        require_id(body["artifact_id"], cls.PREFIX)
        kwargs = {name: body[name] for name in names}
        if cls is VendorM1MinuteV1:
            for name in ("reference_ohlc", "tick_ohlc", "native_bid_ohlc"):
                if kwargs[name] is not None:
                    kwargs[name] = _tuple(kwargs[name], 4)
            kwargs["differences"] = tuple(
                VendorM1DifferenceV1.from_dict(x)
                for x in _tuple(kwargs["differences"], 4)
            )
        elif cls is VendorM1ReportV1:
            kwargs["policy"] = VendorM1PolicyV1.from_dict(kwargs["policy"])
            rows = _tuple(kwargs["minutes"], MAX_MINUTES)
            reserve_report(len(rows))
            kwargs["minutes"] = tuple(
                VendorM1MinuteV1.from_dict(x) for x in rows
            )
        result = cls(**kwargs)
        if any(
            canonical(result._payload()[name]) != canonical(body[name])
            for name in extra
        ):
            raise ValueError("derived wire claim differs")
        return result


def readmit(value: R, expected: type[R]) -> R:
    """Rebuild exact native records without trusting their serializer methods."""
    if expected not in _TYPES or type(value) is not expected:
        raise ValueError("exact validation wire type required")
    return expected(
        **{
            item.name: getattr(value, item.name)
            for item in fields(cast(Any, expected))
        }
    )


def reserve_report(count: int) -> None:
    """Conservative serialized-byte reservation, not a process RSS bound."""
    require_int(count, "minute count", 0, MAX_MINUTES)
    if 65536 + count * 8192 > MAX_REPORT_BYTES:
        raise ValueError("prospective report byte bound exceeded")


@dataclass(frozen=True, slots=True)
class VendorM1PolicyV1(_Wire):
    """Declared finite comparison law; availability fields are declarations."""

    symbol: str
    source_period: str
    start_ns: int
    end_ns: int
    pip_size: str
    rounding_tolerance_pips: str = "0"
    tick_order: str = "strict_source_order"
    as_of_ns: int | None = None
    tick_available_at_ns: int | None = None
    artifact_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.vendor-m1-policy.v1"
    PREFIX: ClassVar[str] = "vendor-m1-policy"

    def __post_init__(self) -> None:
        _axis(self.symbol, self.source_period)
        require_int(self.start_ns, "start_ns")
        require_int(self.end_ns, "end_ns", self.start_ns + 1)
        if self.end_ns - self.start_ns > MAX_MINUTES * MINUTE_NS:
            raise ValueError("query span exceeds minute bound")
        decimal_value(self.pip_size)
        decimal_value(self.rounding_tolerance_pips, positive=False)
        if type(self.tick_order) is not str or self.tick_order not in (
            "strict_source_order",
            "bounded_regressions_event_time_row",
        ):
            raise ValueError("unsupported tick order policy")
        _optional_time(self.as_of_ns)
        _optional_time(self.tick_available_at_ns)
        self._seal()

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "artifact_id"
            },
        }


@dataclass(frozen=True, slots=True)
class VendorM1ReferenceV1(_Wire):
    """Exact admitted M1 bytes; content possession is not provider authenticity."""

    symbol: str
    source_period: str
    source_ascii: str
    source_sha256: str
    available_at_ns: int | None = None
    artifact_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.vendor-m1-reference.v1"
    PREFIX: ClassVar[str] = "vendor-m1-reference"

    def __post_init__(self) -> None:
        _axis(self.symbol, self.source_period)
        require_sha(self.source_sha256)
        _optional_time(self.available_at_ns)
        if (
            type(self.source_ascii) is not str
            or len(self.source_ascii) > MAX_SOURCE_BYTES
            or not self.source_ascii.isascii()
        ):
            raise ValueError("invalid bounded source ASCII")
        if (
            hashlib.sha256(self.source_ascii.encode("ascii")).hexdigest()
            != self.source_sha256
        ):
            raise ValueError("reference source hash differs")
        # Lazy import keeps declaration/import paths lightweight.
        from histdatacom.data_quality.vendor_m1 import _parse_m1

        rows = _parse_m1(self.source_ascii, self.source_period)
        if (
            rows
            and self.available_at_ns is not None
            and self.available_at_ns < rows[-1].time_ns + MINUTE_NS
        ):
            raise ValueError(
                "M1 availability precedes full snapshot minute completion"
            )
        self._seal()

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "artifact_id"
            },
        }


@dataclass(frozen=True, slots=True)
class VendorM1DifferenceV1(_Wire):
    """Exact signed tick-minus-vendor, absolute, and pip-normalized values."""

    field: str
    vendor_price: str
    tick_price: str
    pip_size: str
    signed_difference: str
    absolute_difference: str
    pip_difference: str
    artifact_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.vendor-m1-difference.v1"
    PREFIX: ClassVar[str] = "vendor-m1-difference"

    def __post_init__(self) -> None:
        if type(self.field) is not str or self.field not in OHLC:
            raise ValueError("unknown OHLC field")
        delta = bounded_fraction(
            decimal_value(self.tick_price) - decimal_value(self.vendor_price)
        )
        if (
            _ratio(self.signed_difference) != delta
            or _ratio(self.absolute_difference) != abs(delta)
            or _ratio(self.pip_difference)
            != bounded_fraction(delta / decimal_value(self.pip_size))
        ):
            raise ValueError("exact price differences disagree")
        self._seal()

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "artifact_id"
            },
            "difference_convention": DIFFERENCE_CONVENTION,
        }


def _prices(values: Any) -> None:
    if type(values) is not tuple or len(values) != 4:
        raise ValueError("OHLC requires exact four-element tuple")
    opening, high, low, close = tuple(decimal_value(x) for x in values)
    if not low <= min(opening, close) <= max(opening, close) <= high:
        raise ValueError("OHLC bounds disagree")


@dataclass(frozen=True, slots=True)
class VendorM1MinuteV1(_Wire):
    """One actual union-support minute, with explicit ineligible outcomes."""

    start_ns: int
    status: str
    reference_row: int | None
    reference_ohlc: tuple[str, ...] | None
    raw_volume: int | None
    tick_count: int
    tick_ohlc: tuple[str, ...] | None
    ordered_tick_rows_sha256: str | None
    native_bar_id: str | None
    native_bid_ohlc: tuple[str, ...] | None
    differences: tuple[VendorM1DifferenceV1, ...]
    artifact_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.vendor-m1-minute.v1"
    PREFIX: ClassVar[str] = "vendor-m1-minute"

    def __post_init__(self) -> None:
        require_int(self.start_ns, "minute start")
        if self.start_ns % MINUTE_NS:
            raise ValueError("minute must be UTC aligned")
        if type(self.status) is not str or self.status not in STATUSES:
            raise ValueError("unknown comparison status")
        require_int(self.tick_count, "tick_count", 0, MAX_TICKS)
        if self.reference_row is None:
            if self.reference_ohlc is not None or self.raw_volume is not None:
                raise ValueError("missing reference has price/volume")
        else:
            require_int(
                self.reference_row, "reference row", 1, MAX_REFERENCE_ROWS
            )
            _prices(self.reference_ohlc)
            require_int(
                self.raw_volume, "raw nominal volume", -(2**63), 2**63 - 1
            )
        if not self.tick_count:
            if any(
                x is not None
                for x in (
                    self.tick_ohlc,
                    self.ordered_tick_rows_sha256,
                    self.native_bar_id,
                    self.native_bid_ohlc,
                )
            ):
                raise ValueError("missing ticks have native/value evidence")
        else:
            _prices(self.tick_ohlc)
            require_sha(self.ordered_tick_rows_sha256)
            require_id(self.native_bar_id, "derived-bar")
            if (
                type(self.native_bid_ohlc) is not tuple
                or len(self.native_bid_ohlc) != 4
            ):
                raise ValueError("native OHLC requires four values")
            for value in self.native_bid_ohlc:
                if (
                    type(value) is not str
                    or len(value) > 32
                    or not re.fullmatch(r"[0-9.e+\-]+", value)
                ):
                    raise ValueError("invalid native float representation")
                try:
                    parsed = float(value)
                except ValueError as exc:
                    raise ValueError(
                        "invalid native float representation"
                    ) from exc
                if (
                    not math.isfinite(parsed)
                    or parsed <= 0
                    or repr(parsed) != value
                ):
                    raise ValueError(
                        "native float text must be finite/positive/canonical"
                    )
        if self.reference_row is None and not self.tick_count:
            raise ValueError("empty minute has no actual support")
        if type(self.differences) is not tuple or len(self.differences) > 4:
            raise ValueError("differences exceed OHLC bound")
        differences = tuple(
            readmit(x, VendorM1DifferenceV1) for x in self.differences
        )
        object.__setattr__(self, "differences", differences)
        comparable = (
            self.reference_row is not None
            and self.tick_count > 0
            and self.status != "partial_query_refused"
        )
        if comparable:
            if tuple(x.field for x in differences) != OHLC:
                raise ValueError("comparable minute lacks complete differences")
            if self.status not in STATUSES[:3]:
                raise ValueError("supported minute has missing status")
            assert (
                self.reference_ohlc is not None and self.tick_ohlc is not None
            )
            if any(
                x.vendor_price != self.reference_ohlc[i]
                or x.tick_price != self.tick_ohlc[i]
                for i, x in enumerate(differences)
            ):
                raise ValueError("differences describe other prices")
        elif differences:
            raise ValueError("ineligible minute must not claim differences")
        elif self.status != "partial_query_refused" and self.status != (
            "missing_m1"
            if self.reference_row is None
            else "missing_tick_support"
        ):
            raise ValueError("missing minute status disagrees")
        self._seal()

    def _payload(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "artifact_id"
            },
        }
        for name in ("reference_ohlc", "tick_ohlc", "native_bid_ohlc"):
            result[name] = None if result[name] is None else list(result[name])
        result["differences"] = [x.to_dict() for x in self.differences]
        return result


@dataclass(frozen=True, slots=True)
class VendorM1ReportV1(_Wire):
    """Validation evidence only; no historical coverage/publication authority."""

    reference_id: str
    reference_sha256: str
    reference_size_bytes: int
    reference_row_count: int
    reference_latest_minute_end_ns: int | None
    reference_available_at_ns: int | None
    tick_sha256: str
    tick_size_bytes: int
    policy: VendorM1PolicyV1
    tick_row_count: int
    tick_latest_event_ns: int | None
    selected_tick_count: int
    tick_regression_count: int
    maximum_tick_regression_ms: int
    native_policy_id: str
    implementation_id: str
    minutes: tuple[VendorM1MinuteV1, ...]
    artifact_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.vendor-m1-report.v1"
    PREFIX: ClassVar[str] = "vendor-m1-report"

    def __post_init__(self) -> None:
        require_id(self.reference_id, "vendor-m1-reference")
        require_sha(self.reference_sha256)
        require_sha(self.tick_sha256)
        require_int(
            self.reference_size_bytes, "reference bytes", 0, MAX_SOURCE_BYTES
        )
        require_int(self.tick_size_bytes, "tick bytes", 0, MAX_SOURCE_BYTES)
        _optional_time(self.reference_available_at_ns)
        object.__setattr__(
            self, "policy", readmit(self.policy, VendorM1PolicyV1)
        )
        require_int(self.tick_row_count, "tick rows", 0, MAX_TICKS)
        require_int(
            self.reference_row_count, "reference rows", 0, MAX_REFERENCE_ROWS
        )
        for count, latest, available, name in (
            (
                self.reference_row_count,
                self.reference_latest_minute_end_ns,
                self.reference_available_at_ns,
                "M1",
            ),
            (
                self.tick_row_count,
                self.tick_latest_event_ns,
                self.policy.tick_available_at_ns,
                "tick",
            ),
        ):
            if not count:
                if latest is not None:
                    raise ValueError(
                        "empty snapshot must not invent latest event"
                    )
            else:
                require_int(latest, "latest whole-snapshot time")
                if (
                    available is not None
                    and latest is not None
                    and available < latest
                ):
                    raise ValueError(
                        f"{name} availability precedes full snapshot"
                    )
        if (
            self.reference_latest_minute_end_ns is not None
            and self.reference_latest_minute_end_ns % MINUTE_NS
        ):
            raise ValueError("latest reference minute end must be aligned")
        require_int(
            self.selected_tick_count,
            "selected tick rows",
            0,
            self.tick_row_count,
        )
        require_int(
            self.tick_regression_count, "regression count", 0, MAX_REGRESSIONS
        )
        require_int(
            self.maximum_tick_regression_ms,
            "maximum regression",
            0,
            MAX_REGRESSION_MS,
        )
        if (
            self.policy.tick_order == "strict_source_order"
            and self.tick_regression_count
        ):
            raise ValueError("strict order cannot report regressions")
        require_id(self.native_policy_id, "derived-bar-policy")
        require_id(self.implementation_id, "vendor-m1-implementation")
        if type(self.minutes) is not tuple or len(self.minutes) > MAX_MINUTES:
            raise ValueError("minutes exceed bound")
        reserve_report(len(self.minutes))
        minutes = tuple(readmit(x, VendorM1MinuteV1) for x in self.minutes)
        object.__setattr__(self, "minutes", minutes)
        if tuple(x.start_ns for x in minutes) != tuple(
            sorted({x.start_ns for x in minutes})
        ):
            raise ValueError("minutes must be unique and sorted")
        if sum(x.tick_count for x in minutes) != self.selected_tick_count:
            raise ValueError("tick denominator differs")
        tolerance = decimal_value(
            self.policy.rounding_tolerance_pips, positive=False
        )
        for minute in minutes:
            if (
                not minute.start_ns < self.policy.end_ns
                or minute.start_ns + MINUTE_NS <= self.policy.start_ns
            ):
                raise ValueError("minute lies outside query")
            partial = (
                minute.start_ns < self.policy.start_ns
                or minute.start_ns + MINUTE_NS > self.policy.end_ns
            )
            if partial != (minute.status == "partial_query_refused"):
                raise ValueError("partial-query eligibility differs")
            if minute.differences:
                if any(
                    x.pip_size != self.policy.pip_size
                    for x in minute.differences
                ):
                    raise ValueError("pip policy differs")
                values = tuple(
                    abs(_ratio(x.pip_difference)) for x in minute.differences
                )
                expected = (
                    "exact_match"
                    if not any(values)
                    else (
                        "bounded_rounding_match"
                        if max(values) <= tolerance
                        else "material_mismatch"
                    )
                )
                if minute.status != expected:
                    raise ValueError(
                        "comparison status differs from exact differences"
                    )
        self._seal()

    @property
    def report_id(self) -> str:
        """Independent expected root for replay selection."""
        return self.artifact_id

    @property
    def availability_decision(self) -> str:
        """Declared availability is not inferred from observation timestamps."""
        if self.policy.as_of_ns is None:
            return "historical_ex_post_only"
        values = (
            self.reference_available_at_ns,
            self.policy.tick_available_at_ns,
        )
        if any(x is None for x in values):
            return "availability_unknown"
        if self.policy.end_ns > self.policy.as_of_ns or any(
            x is not None and x > self.policy.as_of_ns for x in values
        ):
            return "declared_unavailable_as_of"
        return "declared_available_as_of_not_independently_attested"

    @property
    def status_counts(self) -> dict[str, int]:
        """Complete outcome denominator; mismatches/missing rows are retained."""
        return {
            name: sum(x.status == name for x in self.minutes)
            for name in STATUSES
        }

    @property
    def as_of_eligible(self) -> bool:
        """Declared PIT eligibility only; never inferred/attested availability.

        False makes all retained price outcomes explicitly ex-post and
        non-actionable as-of. True remains conditional on the declared source
        availability, not a verified provider release or source qualification.
        """
        return (
            self.availability_decision
            == "declared_available_as_of_not_independently_attested"
        )

    def _payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name not in ("artifact_id", "policy", "minutes")
            },
            "policy": self.policy.to_dict(),
            "minutes": [x.to_dict() for x in self.minutes],
            "volume_status": VOLUME_STATUS,
            "clock_policy": CLOCK_POLICY,
            "price_policy": PRICE_POLICY,
            "claim_kind": "validation_only_not_source_or_publication_authority",
            "availability_decision": self.availability_decision,
            "as_of_eligible": self.as_of_eligible,
            "difference_convention": DIFFERENCE_CONVENTION,
            "status_counts": self.status_counts,
            "native_tick_parser": (
                "actual_parse_ascii_lines"
                if self.tick_row_count
                else "empty_source_no_native_batch"
            ),
        }


_TYPES = (
    VendorM1PolicyV1,
    VendorM1ReferenceV1,
    VendorM1DifferenceV1,
    VendorM1MinuteV1,
    VendorM1ReportV1,
)
