"""Provider-neutral monetary-policy meeting and decision semantics.

The contracts in this module preserve central-bank policy settings without
forcing target ranges, facility sets, yield targets, currency boards, or
exchange-rate regimes into one scalar ``interest rate`` field.  Meeting phases
are separately identified and all schedule and publication evidence remains
explicit so retrospective calendars cannot manufacture unscheduled actions.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, TypeVar, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import EconomicEventFamily
from histdatacom.market_context.official_sources import OfficialSourceRegistryV1
from histdatacom.runtime_contracts import JSONValue

MONETARY_POLICY_SETTING_COMPONENT_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-setting-component.v1"
)
MONETARY_POLICY_SETTING_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-setting.v1"
)
MONETARY_POLICY_MEETING_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-meeting.v1"
)
MONETARY_POLICY_PHASE_EVENT_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-phase-event.v1"
)
MONETARY_POLICY_EXPECTATION_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-expectation.v1"
)
MONETARY_POLICY_DECISION_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-decision.v1"
)
MONETARY_POLICY_VOTE_BUCKET_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-vote-bucket.v1"
)
MONETARY_POLICY_VOTE_TALLY_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-vote-tally.v1"
)
MONETARY_POLICY_SURPRISE_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-surprise.v1"
)
MONETARY_POLICY_MEETING_BUNDLE_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-meeting-bundle.v1"
)
MONETARY_POLICY_FRAMEWORK_PROFILE_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-framework-profile.v1"
)
MONETARY_POLICY_FRAMEWORK_AUDIT_SCHEMA_VERSION = (
    "histdatacom.monetary-policy-framework-audit.v1"
)

MAX_MONETARY_POLICY_PHASE_EVENTS = 64
MAX_MONETARY_POLICY_SETTING_COMPONENTS = 32
MAX_MONETARY_POLICY_VOTE_BUCKETS = 32

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class MonetaryPolicyFramework(str, Enum):
    """Institutional framework governing a policy decision."""

    POLICY_RATE = "policy-rate"
    TARGET_RANGE = "target-range"
    RATE_SET = "rate-set"
    YIELD_CURVE_CONTROL = "yield-curve-control"
    CURRENCY_BOARD = "currency-board"
    EXCHANGE_RATE_BAND = "exchange-rate-band"
    LIQUIDITY_OPERATING_TARGET = "liquidity-operating-target"
    CATEGORICAL = "categorical"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyFramework
    ) -> MonetaryPolicyFramework:
        return _enum_value(cls, value, "monetary-policy framework")


class MonetaryPolicySettingKind(str, Enum):
    """Lossless shape of one monetary-policy setting."""

    SCALAR_RATE = "scalar-rate"
    TARGET_RANGE = "target-range"
    RATE_SET = "rate-set"
    YIELD_TARGET = "yield-target"
    EXCHANGE_RATE_BAND = "exchange-rate-band"
    CURRENCY_BOARD_RATE = "currency-board-rate"
    CATEGORICAL = "categorical"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicySettingKind
    ) -> MonetaryPolicySettingKind:
        return _enum_value(cls, value, "monetary-policy setting kind")


class MonetaryPolicyEventPhase(str, Enum):
    """Distinct publication phase linked to one policy meeting."""

    MEETING_SCHEDULE = "meeting-schedule"
    RESCHEDULE = "reschedule"
    CANCELLATION = "cancellation"
    DECISION = "decision"
    EMERGENCY_ACTION = "emergency-action"
    STATEMENT = "statement"
    VOTE_SPLIT = "vote-split"
    PRESS_CONFERENCE = "press-conference"
    MINUTES = "minutes"
    PROJECTIONS = "projections"
    FORWARD_GUIDANCE = "forward-guidance"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyEventPhase
    ) -> MonetaryPolicyEventPhase:
        return _enum_value(cls, value, "monetary-policy event phase")


class MonetaryPolicyActionTiming(str, Enum):
    """Whether a meeting or action was known from a schedule."""

    SCHEDULED = "scheduled"
    UNSCHEDULED = "unscheduled"
    EMERGENCY = "emergency"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyActionTiming
    ) -> MonetaryPolicyActionTiming:
        return _enum_value(cls, value, "monetary-policy action timing")


class MonetaryPolicyScheduleEvidenceKind(str, Enum):
    """Contemporaneous evidence allowed to establish meeting timing."""

    CONTEMPORANEOUS_CALENDAR = "contemporaneous-calendar"
    ARCHIVED_CALENDAR = "archived-calendar"
    CONTEMPORANEOUS_RELEASE = "contemporaneous-release"
    ARCHIVED_RELEASE = "archived-release"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyScheduleEvidenceKind
    ) -> MonetaryPolicyScheduleEvidenceKind:
        return _enum_value(cls, value, "schedule evidence kind")


class MonetaryPolicyDecisionDirection(str, Enum):
    """Interpretation of a decision without assuming scalar comparability."""

    TIGHTEN = "tighten"
    EASE = "ease"
    HOLD = "hold"
    MIXED = "mixed"
    FRAMEWORK_CHANGE = "framework-change"
    GUIDANCE_ONLY = "guidance-only"
    OPERATIONAL = "operational"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyDecisionDirection
    ) -> MonetaryPolicyDecisionDirection:
        return _enum_value(cls, value, "monetary-policy decision direction")


class MonetaryPolicyExpectationKind(str, Enum):
    """Point-in-time provenance and target scope of an expectation."""

    EVENT_CONSENSUS = "event-consensus"
    OFFICIAL_SURVEY_EVENT_TARGET = "official-survey-event-target"
    OFFICIAL_SURVEY_PERIOD_TARGET = "official-survey-period-target"
    CENTRAL_BANK_PROJECTION = "central-bank-projection"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicyExpectationKind
    ) -> MonetaryPolicyExpectationKind:
        return _enum_value(cls, value, "monetary-policy expectation kind")

    @property
    def calendar_style_eligible(self) -> bool:
        """Return whether this is an exact pre-decision expectation."""
        return self in {
            MonetaryPolicyExpectationKind.EVENT_CONSENSUS,
            MonetaryPolicyExpectationKind.OFFICIAL_SURVEY_EVENT_TARGET,
        }


class MonetaryPolicySurpriseKind(str, Enum):
    """Surprise representation that preserves the setting shape."""

    SCALAR_RATE_DELTA = "scalar-rate-delta"
    RANGE_VECTOR = "range-vector"
    COMPONENT_VECTOR = "component-vector"
    CATEGORICAL_MATCH = "categorical-match"
    TEXTUAL_LATENT = "textual-latent"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | MonetaryPolicySurpriseKind
    ) -> MonetaryPolicySurpriseKind:
        return _enum_value(cls, value, "monetary-policy surprise kind")


def _enum_value(
    enum_type: type[_EnumT], value: str | _EnumT, label: str
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as exc:
        raise ValueError(f"unsupported {label}") from exc


def _required_text(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    if len(text) > 4096:
        raise ValueError(f"{label} exceeds text bound")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _key(value: object, label: str) -> str:
    text = _required_text(value, label).lower()
    if _KEY_RE.fullmatch(text) is None:
        raise ValueError(f"{label} is not a canonical key")
    return text


def _economy(value: object) -> str:
    text = _required_text(value, "economy_code").upper()
    if _ECONOMY_RE.fullmatch(text) is None:
        raise ValueError("economy_code is invalid")
    return text


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _optional_finite(value: object, label: str) -> float | None:
    return None if value is None else _finite(value, label)


def _bounded_ns(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{label} is outside the supported range")
    return value


def _optional_ns(value: object, label: str) -> int | None:
    return None if value is None else _bounded_ns(value, label)


def _sha256(value: object, label: str) -> str:
    text = _required_text(value, label)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _https_uri(value: object, label: str) -> str:
    text = _required_text(value, label)
    parsed = urlparse(text)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{label} is not a valid HTTPS URI") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or port not in {None, 443}
    ):
        raise ValueError(f"{label} must be a credential-free HTTPS URI")
    return text


def _texts(
    values: Iterable[object], label: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > 128:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _mapping(value: object, label: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return value


def _sequence(value: object, label: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{label} must be a sequence")
    return value


def _aware_datetime(value: object, label: str) -> datetime:
    text = _required_text(value, label)
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} is not ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{label} must retain a UTC offset")
    return result


def _datetime_ns(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1_000


def _time_evidence(
    timestamp_ns: int, lexical: object, zone_name: object, label: str
) -> tuple[str, str]:
    timestamp = _bounded_ns(timestamp_ns, f"{label}_ns")
    text = _required_text(lexical, f"{label}_lexical")
    zone_text = _required_text(zone_name, "source_timezone")
    parsed = _aware_datetime(text, f"{label}_lexical")
    if _datetime_ns(parsed) != timestamp:
        raise ValueError(f"{label} lexical evidence differs from timestamp")
    try:
        zone = ZoneInfo(zone_text)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("source_timezone is not an IANA timezone") from exc
    if parsed.astimezone(zone).utcoffset() != parsed.utcoffset():
        raise ValueError(f"{label} offset differs from source timezone")
    return text, zone_text


@dataclass(frozen=True, slots=True)
class MonetaryPolicySettingComponentV1:
    """One named rate or target within a multi-component setting."""

    component_key: str
    label: str
    value: float
    unit: str
    source_lexical: str
    component_id: str = ""
    schema_version: str = MONETARY_POLICY_SETTING_COMPONENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != MONETARY_POLICY_SETTING_COMPONENT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported monetary-policy component schema")
        object.__setattr__(
            self, "component_key", _key(self.component_key, "component_key")
        )
        for name in ("label", "unit", "source_lexical"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "value", _finite(self.value, "value"))
        expected = _stable_id(
            "monetary-policy-component", self.identity_payload()
        )
        supplied = _optional_text(self.component_id)
        if supplied is not None and supplied != expected:
            raise ValueError("component_id differs from deterministic identity")
        object.__setattr__(self, "component_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "component_key": self.component_key,
            "label": self.label,
            "value": self.value,
            "unit": self.unit,
            "source_lexical": self.source_lexical,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "component_id": self.component_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> MonetaryPolicySettingComponentV1:
        return cls(
            component_key=str(data.get("component_key", "")),
            label=str(data.get("label", "")),
            value=cast(float, data.get("value")),
            unit=str(data.get("unit", "")),
            source_lexical=str(data.get("source_lexical", "")),
            component_id=str(data.get("component_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicySettingV1:
    """A lossless scalar, range, component, yield, or categorical setting."""

    kind: MonetaryPolicySettingKind
    unit: str
    definition_version: str
    source_lexical: str
    scalar_value: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    components: tuple[MonetaryPolicySettingComponentV1, ...] = ()
    categorical_code: str | None = None
    categorical_label: str | None = None
    target_tenor: str | None = None
    setting_id: str = ""
    schema_version: str = MONETARY_POLICY_SETTING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_SETTING_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy setting schema")
        kind = MonetaryPolicySettingKind.from_value(self.kind)
        object.__setattr__(self, "kind", kind)
        for name in ("unit", "definition_version", "source_lexical"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        scalar = _optional_finite(self.scalar_value, "scalar_value")
        lower = _optional_finite(self.lower_bound, "lower_bound")
        upper = _optional_finite(self.upper_bound, "upper_bound")
        components = tuple(
            sorted(self.components, key=lambda item: item.component_key)
        )
        if len(components) > MAX_MONETARY_POLICY_SETTING_COMPONENTS:
            raise ValueError("setting component count exceeds bound")
        if any(
            not isinstance(item, MonetaryPolicySettingComponentV1)
            for item in components
        ):
            raise TypeError("setting contains an invalid component")
        if len({item.component_key for item in components}) != len(components):
            raise ValueError("setting repeats a component key")
        categorical_code = _optional_text(self.categorical_code)
        categorical_label = _optional_text(self.categorical_label)
        target_tenor = _optional_text(self.target_tenor)
        numeric_fields = (scalar, lower, upper)
        if kind in {
            MonetaryPolicySettingKind.SCALAR_RATE,
            MonetaryPolicySettingKind.CURRENCY_BOARD_RATE,
        }:
            if scalar is None or any(
                item is not None for item in (lower, upper)
            ):
                raise ValueError("scalar setting requires only scalar_value")
            if (
                components
                or categorical_code
                or categorical_label
                or target_tenor
            ):
                raise ValueError("scalar setting contains incompatible fields")
        elif kind in {
            MonetaryPolicySettingKind.TARGET_RANGE,
            MonetaryPolicySettingKind.EXCHANGE_RATE_BAND,
        }:
            if lower is None or upper is None or lower > upper:
                raise ValueError("range setting requires ordered bounds")
            if scalar is not None or components or target_tenor:
                raise ValueError("range setting contains incompatible fields")
            if categorical_code or categorical_label:
                raise ValueError("range setting cannot carry a category")
        elif kind is MonetaryPolicySettingKind.RATE_SET:
            if len(components) < 2 or any(
                item is not None for item in numeric_fields
            ):
                raise ValueError("rate-set setting requires only components")
            if categorical_code or categorical_label or target_tenor:
                raise ValueError(
                    "rate-set setting contains incompatible fields"
                )
            if any(item.unit != self.unit for item in components):
                raise ValueError("rate-set component units differ")
        elif kind is MonetaryPolicySettingKind.YIELD_TARGET:
            if scalar is None or target_tenor is None:
                raise ValueError("yield target requires scalar_value and tenor")
            if any(item is not None for item in (lower, upper)) or components:
                raise ValueError("yield target contains incompatible fields")
            if categorical_code or categorical_label:
                raise ValueError("yield target cannot carry a category")
        else:
            if categorical_code is None or categorical_label is None:
                raise ValueError("categorical setting requires code and label")
            if any(item is not None for item in numeric_fields) or components:
                raise ValueError(
                    "categorical setting cannot carry numeric fields"
                )
            if target_tenor is not None:
                raise ValueError("categorical setting cannot carry a tenor")
            categorical_code = _key(categorical_code, "categorical_code")
        object.__setattr__(self, "scalar_value", scalar)
        object.__setattr__(self, "lower_bound", lower)
        object.__setattr__(self, "upper_bound", upper)
        object.__setattr__(self, "components", components)
        object.__setattr__(self, "categorical_code", categorical_code)
        object.__setattr__(self, "categorical_label", categorical_label)
        object.__setattr__(self, "target_tenor", target_tenor)
        expected = _stable_id(
            "monetary-policy-setting", self.identity_payload()
        )
        supplied = _optional_text(self.setting_id)
        if supplied is not None and supplied != expected:
            raise ValueError("setting_id differs from deterministic identity")
        object.__setattr__(self, "setting_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "unit": self.unit,
            "definition_version": self.definition_version,
            "source_lexical": self.source_lexical,
            "scalar_value": self.scalar_value,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "components": [item.to_dict() for item in self.components],
            "categorical_code": self.categorical_code,
            "categorical_label": self.categorical_label,
            "target_tenor": self.target_tenor,
        }

    def semantic_payload(self) -> dict[str, JSONValue]:
        """Return meaning without source wording or content identity."""
        return {
            "kind": self.kind.value,
            "unit": self.unit,
            "definition_version": self.definition_version,
            "scalar_value": self.scalar_value,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "components": [
                {
                    "component_key": item.component_key,
                    "value": item.value,
                    "unit": item.unit,
                }
                for item in self.components
            ],
            "categorical_code": self.categorical_code,
            "target_tenor": self.target_tenor,
        }

    @property
    def semantic_id(self) -> str:
        """Return a deterministic identity for the represented policy state."""
        return _stable_id(
            "monetary-policy-setting-semantic", self.semantic_payload()
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "setting_id": self.setting_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicySettingV1:
        return cls(
            kind=MonetaryPolicySettingKind.from_value(
                str(data.get("kind", ""))
            ),
            unit=str(data.get("unit", "")),
            definition_version=str(data.get("definition_version", "")),
            source_lexical=str(data.get("source_lexical", "")),
            scalar_value=cast(float | None, data.get("scalar_value")),
            lower_bound=cast(float | None, data.get("lower_bound")),
            upper_bound=cast(float | None, data.get("upper_bound")),
            components=tuple(
                MonetaryPolicySettingComponentV1.from_dict(
                    _mapping(item, "setting component")
                )
                for item in _sequence(data.get("components"), "components")
            ),
            categorical_code=_optional_text(data.get("categorical_code")),
            categorical_label=_optional_text(data.get("categorical_label")),
            target_tenor=_optional_text(data.get("target_tenor")),
            setting_id=str(data.get("setting_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> MonetaryPolicySettingV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("monetary-policy setting is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))


@dataclass(frozen=True, slots=True)
class MonetaryPolicyMeetingV1:
    """A meeting identity backed by contemporaneous schedule evidence."""

    meeting_key: str
    economy_code: str
    institution: str
    framework: MonetaryPolicyFramework
    action_timing: MonetaryPolicyActionTiming
    first_known_at_ns: int
    schedule_evidence_kind: MonetaryPolicyScheduleEvidenceKind
    schedule_source_key: str
    schedule_snapshot_id: str
    schedule_source_uri: str
    source_timezone: str
    limitations: tuple[str, ...]
    scheduled_start_ns: int | None = None
    scheduled_lexical: str | None = None
    meeting_id: str = ""
    schema_version: str = MONETARY_POLICY_MEETING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_MEETING_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy meeting schema")
        object.__setattr__(
            self, "meeting_key", _key(self.meeting_key, "meeting_key")
        )
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self, "institution", _required_text(self.institution, "institution")
        )
        framework = MonetaryPolicyFramework.from_value(self.framework)
        timing = MonetaryPolicyActionTiming.from_value(self.action_timing)
        evidence = MonetaryPolicyScheduleEvidenceKind.from_value(
            self.schedule_evidence_kind
        )
        object.__setattr__(self, "framework", framework)
        object.__setattr__(self, "action_timing", timing)
        object.__setattr__(self, "schedule_evidence_kind", evidence)
        known = _bounded_ns(self.first_known_at_ns, "first_known_at_ns")
        object.__setattr__(self, "first_known_at_ns", known)
        object.__setattr__(
            self,
            "schedule_source_key",
            _key(self.schedule_source_key, "schedule_source_key"),
        )
        object.__setattr__(
            self,
            "schedule_snapshot_id",
            _required_text(self.schedule_snapshot_id, "schedule_snapshot_id"),
        )
        object.__setattr__(
            self,
            "schedule_source_uri",
            _https_uri(self.schedule_source_uri, "schedule_source_uri"),
        )
        limitations = _texts(self.limitations, "limitation", required=True)
        object.__setattr__(self, "limitations", limitations)
        scheduled = _optional_ns(self.scheduled_start_ns, "scheduled_start_ns")
        lexical = _optional_text(self.scheduled_lexical)
        zone = _required_text(self.source_timezone, "source_timezone")
        if timing is MonetaryPolicyActionTiming.SCHEDULED:
            if scheduled is None or lexical is None:
                raise ValueError(
                    "scheduled meeting requires schedule time evidence"
                )
            lexical, zone = _time_evidence(
                scheduled, lexical, zone, "scheduled_start"
            )
            if evidence not in {
                MonetaryPolicyScheduleEvidenceKind.CONTEMPORANEOUS_CALENDAR,
                MonetaryPolicyScheduleEvidenceKind.ARCHIVED_CALENDAR,
            }:
                raise ValueError("scheduled meeting requires calendar evidence")
            if known > scheduled:
                raise ValueError(
                    "meeting was first known after its scheduled start"
                )
        else:
            if scheduled is not None or lexical is not None:
                raise ValueError(
                    "unscheduled action cannot contain a scheduled time"
                )
            if evidence not in {
                MonetaryPolicyScheduleEvidenceKind.CONTEMPORANEOUS_RELEASE,
                MonetaryPolicyScheduleEvidenceKind.ARCHIVED_RELEASE,
            }:
                raise ValueError("unscheduled action requires release evidence")
            try:
                ZoneInfo(zone)
            except ZoneInfoNotFoundError as exc:
                raise ValueError(
                    "source_timezone is not an IANA timezone"
                ) from exc
        object.__setattr__(self, "scheduled_start_ns", scheduled)
        object.__setattr__(self, "scheduled_lexical", lexical)
        object.__setattr__(self, "source_timezone", zone)
        expected = _stable_id(
            "monetary-policy-meeting", self.identity_payload()
        )
        supplied = _optional_text(self.meeting_id)
        if supplied is not None and supplied != expected:
            raise ValueError("meeting_id differs from deterministic identity")
        object.__setattr__(self, "meeting_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_key": self.meeting_key,
            "economy_code": self.economy_code,
            "institution": self.institution,
            "framework": self.framework.value,
            "action_timing": self.action_timing.value,
            "first_known_at_ns": self.first_known_at_ns,
            "schedule_evidence_kind": self.schedule_evidence_kind.value,
            "schedule_source_key": self.schedule_source_key,
            "schedule_snapshot_id": self.schedule_snapshot_id,
            "schedule_source_uri": self.schedule_source_uri,
            "source_timezone": self.source_timezone,
            "limitations": list(self.limitations),
            "scheduled_start_ns": self.scheduled_start_ns,
            "scheduled_lexical": self.scheduled_lexical,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "meeting_id": self.meeting_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyMeetingV1:
        return cls(
            meeting_key=str(data.get("meeting_key", "")),
            economy_code=str(data.get("economy_code", "")),
            institution=str(data.get("institution", "")),
            framework=MonetaryPolicyFramework.from_value(
                str(data.get("framework", ""))
            ),
            action_timing=MonetaryPolicyActionTiming.from_value(
                str(data.get("action_timing", ""))
            ),
            first_known_at_ns=cast(int, data.get("first_known_at_ns")),
            schedule_evidence_kind=MonetaryPolicyScheduleEvidenceKind.from_value(
                str(data.get("schedule_evidence_kind", ""))
            ),
            schedule_source_key=str(data.get("schedule_source_key", "")),
            schedule_snapshot_id=str(data.get("schedule_snapshot_id", "")),
            schedule_source_uri=str(data.get("schedule_source_uri", "")),
            source_timezone=str(data.get("source_timezone", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            scheduled_start_ns=cast(int | None, data.get("scheduled_start_ns")),
            scheduled_lexical=_optional_text(data.get("scheduled_lexical")),
            meeting_id=str(data.get("meeting_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyPhaseEventV1:
    """One separately timestamped publication phase of a meeting."""

    meeting_id: str
    event_key: str
    phase: MonetaryPolicyEventPhase
    published_at_ns: int
    published_lexical: str
    available_at_ns: int
    source_timezone: str
    source_key: str
    source_snapshot_id: str
    source_uri: str
    content_sha256: str
    limitations: tuple[str, ...]
    economic_release_id: str | None = None
    phase_event_id: str = ""
    schema_version: str = MONETARY_POLICY_PHASE_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_PHASE_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy phase-event schema")
        object.__setattr__(
            self, "meeting_id", _required_text(self.meeting_id, "meeting_id")
        )
        object.__setattr__(self, "event_key", _key(self.event_key, "event_key"))
        object.__setattr__(
            self, "phase", MonetaryPolicyEventPhase.from_value(self.phase)
        )
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("phase availability precedes publication")
        lexical, zone = _time_evidence(
            published,
            self.published_lexical,
            self.source_timezone,
            "published_at",
        )
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "published_lexical", lexical)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_timezone", zone)
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "source_snapshot_id",
            _required_text(self.source_snapshot_id, "source_snapshot_id"),
        )
        object.__setattr__(
            self, "source_uri", _https_uri(self.source_uri, "source_uri")
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        object.__setattr__(
            self,
            "economic_release_id",
            _optional_text(self.economic_release_id),
        )
        expected = _stable_id("monetary-policy-phase", self.identity_payload())
        supplied = _optional_text(self.phase_event_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "phase_event_id differs from deterministic identity"
            )
        object.__setattr__(self, "phase_event_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_id": self.meeting_id,
            "event_key": self.event_key,
            "phase": self.phase.value,
            "published_at_ns": self.published_at_ns,
            "published_lexical": self.published_lexical,
            "available_at_ns": self.available_at_ns,
            "source_timezone": self.source_timezone,
            "source_key": self.source_key,
            "source_snapshot_id": self.source_snapshot_id,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "limitations": list(self.limitations),
            "economic_release_id": self.economic_release_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "phase_event_id": self.phase_event_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyPhaseEventV1:
        return cls(
            meeting_id=str(data.get("meeting_id", "")),
            event_key=str(data.get("event_key", "")),
            phase=MonetaryPolicyEventPhase.from_value(
                str(data.get("phase", ""))
            ),
            published_at_ns=cast(int, data.get("published_at_ns")),
            published_lexical=str(data.get("published_lexical", "")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_timezone=str(data.get("source_timezone", "")),
            source_key=str(data.get("source_key", "")),
            source_snapshot_id=str(data.get("source_snapshot_id", "")),
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            economic_release_id=_optional_text(data.get("economic_release_id")),
            phase_event_id=str(data.get("phase_event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyExpectationV1:
    """A pre-decision expectation or an explicit unavailable declaration."""

    meeting_id: str
    target_phase: MonetaryPolicyEventPhase
    kind: MonetaryPolicyExpectationKind
    expected_setting: MonetaryPolicySettingV1 | None
    collection_start_ns: int | None
    collection_cutoff_ns: int | None
    available_at_ns: int | None
    source_key: str | None
    source_snapshot_id: str | None
    source_uri: str | None
    unavailable_reason: str | None
    limitations: tuple[str, ...]
    expectation_id: str = ""
    schema_version: str = MONETARY_POLICY_EXPECTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_EXPECTATION_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy expectation schema")
        object.__setattr__(
            self, "meeting_id", _required_text(self.meeting_id, "meeting_id")
        )
        phase = MonetaryPolicyEventPhase.from_value(self.target_phase)
        if phase not in {
            MonetaryPolicyEventPhase.DECISION,
            MonetaryPolicyEventPhase.EMERGENCY_ACTION,
        }:
            raise ValueError("expectation must target a decision phase")
        object.__setattr__(self, "target_phase", phase)
        kind = MonetaryPolicyExpectationKind.from_value(self.kind)
        object.__setattr__(self, "kind", kind)
        start = _optional_ns(self.collection_start_ns, "collection_start_ns")
        cutoff = _optional_ns(self.collection_cutoff_ns, "collection_cutoff_ns")
        available = _optional_ns(self.available_at_ns, "available_at_ns")
        source_key = _optional_text(self.source_key)
        snapshot = _optional_text(self.source_snapshot_id)
        uri = _optional_text(self.source_uri)
        reason = _optional_text(self.unavailable_reason)
        if kind is MonetaryPolicyExpectationKind.UNAVAILABLE:
            if self.expected_setting is not None or any(
                item is not None
                for item in (
                    start,
                    cutoff,
                    available,
                    source_key,
                    snapshot,
                    uri,
                )
            ):
                raise ValueError(
                    "unavailable expectation contains forecast evidence"
                )
            if reason is None:
                raise ValueError("unavailable expectation requires a reason")
        else:
            if not isinstance(self.expected_setting, MonetaryPolicySettingV1):
                raise TypeError("observed expectation requires a typed setting")
            if any(item is None for item in (start, cutoff, available)):
                raise ValueError(
                    "observed expectation requires collection times"
                )
            assert (
                start is not None
                and cutoff is not None
                and available is not None
            )
            if not start <= cutoff <= available:
                raise ValueError("expectation collection chronology is invalid")
            if source_key is None or snapshot is None or uri is None:
                raise ValueError(
                    "observed expectation requires source evidence"
                )
            source_key = _key(source_key, "source_key")
            uri = _https_uri(uri, "source_uri")
            if reason is not None:
                raise ValueError("observed expectation cannot be unavailable")
        object.__setattr__(self, "collection_start_ns", start)
        object.__setattr__(self, "collection_cutoff_ns", cutoff)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_snapshot_id", snapshot)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "unavailable_reason", reason)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "monetary-policy-expectation", self.identity_payload()
        )
        supplied = _optional_text(self.expectation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "expectation_id differs from deterministic identity"
            )
        object.__setattr__(self, "expectation_id", expected)

    @property
    def calendar_style_eligible(self) -> bool:
        return self.kind.calendar_style_eligible

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_id": self.meeting_id,
            "target_phase": self.target_phase.value,
            "kind": self.kind.value,
            "expected_setting": (
                None
                if self.expected_setting is None
                else self.expected_setting.to_dict()
            ),
            "collection_start_ns": self.collection_start_ns,
            "collection_cutoff_ns": self.collection_cutoff_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_snapshot_id": self.source_snapshot_id,
            "source_uri": self.source_uri,
            "unavailable_reason": self.unavailable_reason,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "expectation_id": self.expectation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyExpectationV1:
        setting = data.get("expected_setting")
        return cls(
            meeting_id=str(data.get("meeting_id", "")),
            target_phase=MonetaryPolicyEventPhase.from_value(
                str(data.get("target_phase", ""))
            ),
            kind=MonetaryPolicyExpectationKind.from_value(
                str(data.get("kind", ""))
            ),
            expected_setting=(
                None
                if setting is None
                else MonetaryPolicySettingV1.from_dict(
                    _mapping(setting, "expected_setting")
                )
            ),
            collection_start_ns=cast(
                int | None, data.get("collection_start_ns")
            ),
            collection_cutoff_ns=cast(
                int | None, data.get("collection_cutoff_ns")
            ),
            available_at_ns=cast(int | None, data.get("available_at_ns")),
            source_key=_optional_text(data.get("source_key")),
            source_snapshot_id=_optional_text(data.get("source_snapshot_id")),
            source_uri=_optional_text(data.get("source_uri")),
            unavailable_reason=_optional_text(data.get("unavailable_reason")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            expectation_id=str(data.get("expectation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyDecisionV1:
    """New, previous, and forecast settings for one decision event."""

    meeting_id: str
    decision_event_id: str
    decided_at_ns: int
    framework: MonetaryPolicyFramework
    previous_setting: MonetaryPolicySettingV1
    previous_setting_as_known_at_ns: int
    previous_setting_evidence_id: str
    new_setting: MonetaryPolicySettingV1
    expectation: MonetaryPolicyExpectationV1
    direction: MonetaryPolicyDecisionDirection
    decision_code: str
    statement_event_id: str | None = None
    decision_id: str = ""
    schema_version: str = MONETARY_POLICY_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_DECISION_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy decision schema")
        meeting_id = _required_text(self.meeting_id, "meeting_id")
        object.__setattr__(self, "meeting_id", meeting_id)
        object.__setattr__(
            self,
            "decision_event_id",
            _required_text(self.decision_event_id, "decision_event_id"),
        )
        decided = _bounded_ns(self.decided_at_ns, "decided_at_ns")
        object.__setattr__(self, "decided_at_ns", decided)
        previous_as_known = _bounded_ns(
            self.previous_setting_as_known_at_ns,
            "previous_setting_as_known_at_ns",
        )
        if previous_as_known > decided:
            raise ValueError("previous setting was not known before decision")
        object.__setattr__(
            self, "previous_setting_as_known_at_ns", previous_as_known
        )
        object.__setattr__(
            self,
            "previous_setting_evidence_id",
            _required_text(
                self.previous_setting_evidence_id,
                "previous_setting_evidence_id",
            ),
        )
        object.__setattr__(
            self,
            "framework",
            MonetaryPolicyFramework.from_value(self.framework),
        )
        if not isinstance(
            self.previous_setting, MonetaryPolicySettingV1
        ) or not isinstance(self.new_setting, MonetaryPolicySettingV1):
            raise TypeError(
                "decision settings must use MonetaryPolicySettingV1"
            )
        if not isinstance(self.expectation, MonetaryPolicyExpectationV1):
            raise TypeError("decision expectation has an unsupported type")
        allowed_kinds = _SETTING_KINDS_BY_FRAMEWORK[self.framework]
        if (
            self.previous_setting.kind not in allowed_kinds
            or self.new_setting.kind not in allowed_kinds
        ):
            raise ValueError("decision setting shape is invalid for framework")
        if (
            self.expectation.expected_setting is not None
            and self.expectation.expected_setting.kind not in allowed_kinds
        ):
            raise ValueError(
                "expectation setting shape is invalid for framework"
            )
        if self.expectation.meeting_id != meeting_id:
            raise ValueError("decision expectation belongs to another meeting")
        if (
            self.expectation.available_at_ns is not None
            and self.expectation.available_at_ns > decided
        ):
            raise ValueError("decision expectation was not available ex ante")
        direction = MonetaryPolicyDecisionDirection.from_value(self.direction)
        object.__setattr__(self, "direction", direction)
        settings_equal = (
            self.previous_setting.semantic_id == self.new_setting.semantic_id
        )
        if (
            direction
            in {
                MonetaryPolicyDecisionDirection.HOLD,
                MonetaryPolicyDecisionDirection.GUIDANCE_ONLY,
            }
            and not settings_equal
        ):
            raise ValueError(
                "hold or guidance-only decision changes the setting"
            )
        if (
            direction
            in {
                MonetaryPolicyDecisionDirection.TIGHTEN,
                MonetaryPolicyDecisionDirection.EASE,
            }
            and settings_equal
        ):
            raise ValueError("tightening or easing must change the setting")
        object.__setattr__(
            self, "decision_code", _key(self.decision_code, "decision_code")
        )
        statement = _optional_text(self.statement_event_id)
        if (
            direction is MonetaryPolicyDecisionDirection.GUIDANCE_ONLY
            and statement is None
        ):
            raise ValueError(
                "guidance-only decision requires a statement event"
            )
        object.__setattr__(self, "statement_event_id", statement)
        expected = _stable_id(
            "monetary-policy-decision", self.identity_payload()
        )
        supplied = _optional_text(self.decision_id)
        if supplied is not None and supplied != expected:
            raise ValueError("decision_id differs from deterministic identity")
        object.__setattr__(self, "decision_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_id": self.meeting_id,
            "decision_event_id": self.decision_event_id,
            "decided_at_ns": self.decided_at_ns,
            "framework": self.framework.value,
            "previous_setting": self.previous_setting.to_dict(),
            "previous_setting_as_known_at_ns": (
                self.previous_setting_as_known_at_ns
            ),
            "previous_setting_evidence_id": (self.previous_setting_evidence_id),
            "new_setting": self.new_setting.to_dict(),
            "expectation": self.expectation.to_dict(),
            "direction": self.direction.value,
            "decision_code": self.decision_code,
            "statement_event_id": self.statement_event_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyDecisionV1:
        return cls(
            meeting_id=str(data.get("meeting_id", "")),
            decision_event_id=str(data.get("decision_event_id", "")),
            decided_at_ns=cast(int, data.get("decided_at_ns")),
            framework=MonetaryPolicyFramework.from_value(
                str(data.get("framework", ""))
            ),
            previous_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("previous_setting"), "previous_setting")
            ),
            previous_setting_as_known_at_ns=cast(
                int, data.get("previous_setting_as_known_at_ns")
            ),
            previous_setting_evidence_id=str(
                data.get("previous_setting_evidence_id", "")
            ),
            new_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("new_setting"), "new_setting")
            ),
            expectation=MonetaryPolicyExpectationV1.from_dict(
                _mapping(data.get("expectation"), "expectation")
            ),
            direction=MonetaryPolicyDecisionDirection.from_value(
                str(data.get("direction", ""))
            ),
            decision_code=str(data.get("decision_code", "")),
            statement_event_id=_optional_text(data.get("statement_event_id")),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyVoteBucketV1:
    """Published count for one vote position."""

    position_key: str
    label: str
    count: int
    preferred_setting_id: str | None = None
    bucket_id: str = ""
    schema_version: str = MONETARY_POLICY_VOTE_BUCKET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_VOTE_BUCKET_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy vote-bucket schema")
        object.__setattr__(
            self, "position_key", _key(self.position_key, "position_key")
        )
        object.__setattr__(self, "label", _required_text(self.label, "label"))
        if isinstance(self.count, bool) or not isinstance(self.count, int):
            raise TypeError("vote count must be an integer")
        if self.count < 0:
            raise ValueError("vote count must not be negative")
        object.__setattr__(
            self,
            "preferred_setting_id",
            _optional_text(self.preferred_setting_id),
        )
        expected = _stable_id(
            "monetary-policy-vote-bucket", self.identity_payload()
        )
        supplied = _optional_text(self.bucket_id)
        if supplied is not None and supplied != expected:
            raise ValueError("bucket_id differs from deterministic identity")
        object.__setattr__(self, "bucket_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "position_key": self.position_key,
            "label": self.label,
            "count": self.count,
            "preferred_setting_id": self.preferred_setting_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "bucket_id": self.bucket_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyVoteBucketV1:
        return cls(
            position_key=str(data.get("position_key", "")),
            label=str(data.get("label", "")),
            count=cast(int, data.get("count")),
            preferred_setting_id=_optional_text(
                data.get("preferred_setting_id")
            ),
            bucket_id=str(data.get("bucket_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyVoteTallyV1:
    """A source-published vote split linked to decision and vote phases."""

    meeting_id: str
    decision_id: str
    vote_event_id: str
    eligible_voters: int
    not_voting: int
    buckets: tuple[MonetaryPolicyVoteBucketV1, ...]
    tally_id: str = ""
    schema_version: str = MONETARY_POLICY_VOTE_TALLY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_VOTE_TALLY_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy vote-tally schema")
        for name in ("meeting_id", "decision_id", "vote_event_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        for name in ("eligible_voters", "not_voting"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.not_voting > self.eligible_voters:
            raise ValueError("not_voting exceeds eligible_voters")
        buckets = tuple(
            sorted(self.buckets, key=lambda item: item.position_key)
        )
        if not buckets or len(buckets) > MAX_MONETARY_POLICY_VOTE_BUCKETS:
            raise ValueError("vote bucket count is outside bounds")
        if any(
            not isinstance(item, MonetaryPolicyVoteBucketV1) for item in buckets
        ):
            raise TypeError("vote tally contains an invalid bucket")
        if len({item.position_key for item in buckets}) != len(buckets):
            raise ValueError("vote tally repeats a position")
        if (
            sum(item.count for item in buckets) + self.not_voting
            != self.eligible_voters
        ):
            raise ValueError("vote tally does not reconcile to eligible voters")
        object.__setattr__(self, "buckets", buckets)
        expected = _stable_id("monetary-policy-vote", self.identity_payload())
        supplied = _optional_text(self.tally_id)
        if supplied is not None and supplied != expected:
            raise ValueError("tally_id differs from deterministic identity")
        object.__setattr__(self, "tally_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_id": self.meeting_id,
            "decision_id": self.decision_id,
            "vote_event_id": self.vote_event_id,
            "eligible_voters": self.eligible_voters,
            "not_voting": self.not_voting,
            "buckets": [item.to_dict() for item in self.buckets],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "tally_id": self.tally_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicyVoteTallyV1:
        return cls(
            meeting_id=str(data.get("meeting_id", "")),
            decision_id=str(data.get("decision_id", "")),
            vote_event_id=str(data.get("vote_event_id", "")),
            eligible_voters=cast(int, data.get("eligible_voters")),
            not_voting=cast(int, data.get("not_voting")),
            buckets=tuple(
                MonetaryPolicyVoteBucketV1.from_dict(
                    _mapping(item, "vote bucket")
                )
                for item in _sequence(data.get("buckets"), "buckets")
            ),
            tally_id=str(data.get("tally_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class MonetaryPolicySurpriseV1:
    """Shape-preserving rate surprise or separately typed text score."""

    decision_id: str | None
    phase_event_id: str
    kind: MonetaryPolicySurpriseKind
    method_version: str
    explanation: str
    scalar_delta: float | None = None
    lower_delta: float | None = None
    upper_delta: float | None = None
    component_deltas: tuple[tuple[str, float], ...] = ()
    categorical_match: bool | None = None
    latent_score: float | None = None
    surprise_id: str = ""
    schema_version: str = MONETARY_POLICY_SURPRISE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_SURPRISE_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy surprise schema")
        kind = MonetaryPolicySurpriseKind.from_value(self.kind)
        object.__setattr__(self, "kind", kind)
        decision_id = _optional_text(self.decision_id)
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(
            self,
            "phase_event_id",
            _required_text(self.phase_event_id, "phase_event_id"),
        )
        for name in ("method_version", "explanation"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        scalar = _optional_finite(self.scalar_delta, "scalar_delta")
        lower = _optional_finite(self.lower_delta, "lower_delta")
        upper = _optional_finite(self.upper_delta, "upper_delta")
        latent = _optional_finite(self.latent_score, "latent_score")
        deltas = tuple(
            sorted(
                (
                    _key(key, "component_key"),
                    _finite(value, "component_delta"),
                )
                for key, value in self.component_deltas
            )
        )
        if len({key for key, _ in deltas}) != len(deltas):
            raise ValueError("component surprise repeats a component")
        categorical = self.categorical_match
        if categorical is not None and not isinstance(categorical, bool):
            raise TypeError("categorical_match must be boolean")
        if kind is MonetaryPolicySurpriseKind.SCALAR_RATE_DELTA:
            valid = scalar is not None and decision_id is not None
        elif kind is MonetaryPolicySurpriseKind.RANGE_VECTOR:
            valid = (
                lower is not None
                and upper is not None
                and decision_id is not None
            )
        elif kind is MonetaryPolicySurpriseKind.COMPONENT_VECTOR:
            valid = bool(deltas) and decision_id is not None
        elif kind is MonetaryPolicySurpriseKind.CATEGORICAL_MATCH:
            valid = categorical is not None and decision_id is not None
        elif kind is MonetaryPolicySurpriseKind.TEXTUAL_LATENT:
            valid = latent is not None and decision_id is None
        else:
            valid = decision_id is not None
        allowed = {
            MonetaryPolicySurpriseKind.SCALAR_RATE_DELTA: (
                scalar,
                None,
                None,
                (),
                None,
                None,
            ),
            MonetaryPolicySurpriseKind.RANGE_VECTOR: (
                None,
                lower,
                upper,
                (),
                None,
                None,
            ),
            MonetaryPolicySurpriseKind.COMPONENT_VECTOR: (
                None,
                None,
                None,
                deltas,
                None,
                None,
            ),
            MonetaryPolicySurpriseKind.CATEGORICAL_MATCH: (
                None,
                None,
                None,
                (),
                categorical,
                None,
            ),
            MonetaryPolicySurpriseKind.TEXTUAL_LATENT: (
                None,
                None,
                None,
                (),
                None,
                latent,
            ),
            MonetaryPolicySurpriseKind.UNAVAILABLE: (
                None,
                None,
                None,
                (),
                None,
                None,
            ),
        }[kind]
        actual = (scalar, lower, upper, deltas, categorical, latent)
        if not valid or actual != allowed:
            raise ValueError("surprise fields do not match surprise kind")
        object.__setattr__(self, "scalar_delta", scalar)
        object.__setattr__(self, "lower_delta", lower)
        object.__setattr__(self, "upper_delta", upper)
        object.__setattr__(self, "component_deltas", deltas)
        object.__setattr__(self, "categorical_match", categorical)
        object.__setattr__(self, "latent_score", latent)
        expected = _stable_id(
            "monetary-policy-surprise", self.identity_payload()
        )
        supplied = _optional_text(self.surprise_id)
        if supplied is not None and supplied != expected:
            raise ValueError("surprise_id differs from deterministic identity")
        object.__setattr__(self, "surprise_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "decision_id": self.decision_id,
            "phase_event_id": self.phase_event_id,
            "kind": self.kind.value,
            "method_version": self.method_version,
            "explanation": self.explanation,
            "scalar_delta": self.scalar_delta,
            "lower_delta": self.lower_delta,
            "upper_delta": self.upper_delta,
            "component_deltas": [list(item) for item in self.component_deltas],
            "categorical_match": self.categorical_match,
            "latent_score": self.latent_score,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "surprise_id": self.surprise_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MonetaryPolicySurpriseV1:
        deltas: list[tuple[str, float]] = []
        for item in _sequence(data.get("component_deltas"), "component_deltas"):
            pair = _sequence(item, "component delta")
            if len(pair) != 2:
                raise ValueError("component delta must contain key and value")
            deltas.append((str(pair[0]), cast(float, pair[1])))
        return cls(
            decision_id=_optional_text(data.get("decision_id")),
            phase_event_id=str(data.get("phase_event_id", "")),
            kind=MonetaryPolicySurpriseKind.from_value(
                str(data.get("kind", ""))
            ),
            method_version=str(data.get("method_version", "")),
            explanation=str(data.get("explanation", "")),
            scalar_delta=cast(float | None, data.get("scalar_delta")),
            lower_delta=cast(float | None, data.get("lower_delta")),
            upper_delta=cast(float | None, data.get("upper_delta")),
            component_deltas=tuple(deltas),
            categorical_match=cast(bool | None, data.get("categorical_match")),
            latent_score=cast(float | None, data.get("latent_score")),
            surprise_id=str(data.get("surprise_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def compute_monetary_policy_rate_surprise(
    decision: MonetaryPolicyDecisionV1,
) -> MonetaryPolicySurpriseV1:
    """Compute a setting-shaped rate surprise from eligible ex-ante evidence."""
    expectation = decision.expectation
    if (
        not expectation.calendar_style_eligible
        or expectation.expected_setting is None
    ):
        return MonetaryPolicySurpriseV1(
            decision_id=decision.decision_id,
            phase_event_id=decision.decision_event_id,
            kind=MonetaryPolicySurpriseKind.UNAVAILABLE,
            method_version="monetary-policy-rate-surprise.v1",
            explanation="no exact pre-decision event expectation is available",
        )
    actual = decision.new_setting
    expected = expectation.expected_setting
    if (
        actual.kind != expected.kind
        or actual.unit != expected.unit
        or actual.definition_version != expected.definition_version
        or (
            actual.kind is MonetaryPolicySettingKind.YIELD_TARGET
            and actual.target_tenor != expected.target_tenor
        )
    ):
        return MonetaryPolicySurpriseV1(
            decision_id=decision.decision_id,
            phase_event_id=decision.decision_event_id,
            kind=MonetaryPolicySurpriseKind.UNAVAILABLE,
            method_version="monetary-policy-rate-surprise.v1",
            explanation="actual and expectation setting shapes are incompatible",
        )
    if actual.kind in {
        MonetaryPolicySettingKind.SCALAR_RATE,
        MonetaryPolicySettingKind.CURRENCY_BOARD_RATE,
        MonetaryPolicySettingKind.YIELD_TARGET,
    }:
        assert (
            actual.scalar_value is not None
            and expected.scalar_value is not None
        )
        return MonetaryPolicySurpriseV1(
            decision_id=decision.decision_id,
            phase_event_id=decision.decision_event_id,
            kind=MonetaryPolicySurpriseKind.SCALAR_RATE_DELTA,
            method_version="monetary-policy-rate-surprise.v1",
            explanation="new setting minus exact pre-decision expectation",
            scalar_delta=actual.scalar_value - expected.scalar_value,
        )
    if actual.kind in {
        MonetaryPolicySettingKind.TARGET_RANGE,
        MonetaryPolicySettingKind.EXCHANGE_RATE_BAND,
    }:
        assert actual.lower_bound is not None and actual.upper_bound is not None
        assert (
            expected.lower_bound is not None
            and expected.upper_bound is not None
        )
        return MonetaryPolicySurpriseV1(
            decision_id=decision.decision_id,
            phase_event_id=decision.decision_event_id,
            kind=MonetaryPolicySurpriseKind.RANGE_VECTOR,
            method_version="monetary-policy-rate-surprise.v1",
            explanation="lower and upper setting deltas retained separately",
            lower_delta=actual.lower_bound - expected.lower_bound,
            upper_delta=actual.upper_bound - expected.upper_bound,
        )
    if actual.kind is MonetaryPolicySettingKind.RATE_SET:
        expected_by_key = {
            item.component_key: item for item in expected.components
        }
        if set(expected_by_key) != {
            item.component_key for item in actual.components
        }:
            return MonetaryPolicySurpriseV1(
                decision_id=decision.decision_id,
                phase_event_id=decision.decision_event_id,
                kind=MonetaryPolicySurpriseKind.UNAVAILABLE,
                method_version="monetary-policy-rate-surprise.v1",
                explanation="actual and expectation rate-set components differ",
            )
        return MonetaryPolicySurpriseV1(
            decision_id=decision.decision_id,
            phase_event_id=decision.decision_event_id,
            kind=MonetaryPolicySurpriseKind.COMPONENT_VECTOR,
            method_version="monetary-policy-rate-surprise.v1",
            explanation="facility or rate-set deltas retained by component",
            component_deltas=tuple(
                (
                    item.component_key,
                    item.value - expected_by_key[item.component_key].value,
                )
                for item in actual.components
            ),
        )
    return MonetaryPolicySurpriseV1(
        decision_id=decision.decision_id,
        phase_event_id=decision.decision_event_id,
        kind=MonetaryPolicySurpriseKind.CATEGORICAL_MATCH,
        method_version="monetary-policy-rate-surprise.v1",
        explanation="categorical decisions are compared without a numeric delta",
        categorical_match=(
            actual.categorical_code == expected.categorical_code
        ),
    )


def declare_monetary_policy_text_surprise(
    event: MonetaryPolicyPhaseEventV1,
    *,
    method_version: str,
    latent_score: float,
    explanation: str,
) -> MonetaryPolicySurpriseV1:
    """Represent statement or press surprise outside the rate metric."""
    if event.phase not in {
        MonetaryPolicyEventPhase.STATEMENT,
        MonetaryPolicyEventPhase.PRESS_CONFERENCE,
        MonetaryPolicyEventPhase.FORWARD_GUIDANCE,
    }:
        raise ValueError("text surprise requires a textual policy phase")
    return MonetaryPolicySurpriseV1(
        decision_id=None,
        phase_event_id=event.phase_event_id,
        kind=MonetaryPolicySurpriseKind.TEXTUAL_LATENT,
        method_version=method_version,
        explanation=explanation,
        latent_score=latent_score,
    )


@dataclass(frozen=True, slots=True)
class MonetaryPolicyMeetingBundleV1:
    """A meeting with phase, decision, and optional vote relationships."""

    meeting: MonetaryPolicyMeetingV1
    phase_events: tuple[MonetaryPolicyPhaseEventV1, ...]
    decisions: tuple[MonetaryPolicyDecisionV1, ...]
    vote_tallies: tuple[MonetaryPolicyVoteTallyV1, ...]
    bundle_id: str = ""
    schema_version: str = MONETARY_POLICY_MEETING_BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MONETARY_POLICY_MEETING_BUNDLE_SCHEMA_VERSION:
            raise ValueError("unsupported monetary-policy bundle schema")
        if not isinstance(self.meeting, MonetaryPolicyMeetingV1):
            raise TypeError("bundle requires a monetary-policy meeting")
        events = tuple(
            sorted(self.phase_events, key=lambda item: item.phase_event_id)
        )
        decisions = tuple(
            sorted(self.decisions, key=lambda item: item.decision_id)
        )
        votes = tuple(sorted(self.vote_tallies, key=lambda item: item.tally_id))
        if not events or len(events) > MAX_MONETARY_POLICY_PHASE_EVENTS:
            raise ValueError("phase-event count is outside bounds")
        if len({item.phase_event_id for item in events}) != len(events):
            raise ValueError("bundle repeats a phase event")
        if len({item.decision_id for item in decisions}) != len(decisions):
            raise ValueError("bundle repeats a decision")
        if len({item.tally_id for item in votes}) != len(votes):
            raise ValueError("bundle repeats a vote tally")
        if any(item.meeting_id != self.meeting.meeting_id for item in events):
            raise ValueError("phase event belongs to another meeting")
        event_by_id = {item.phase_event_id: item for item in events}
        for decision in decisions:
            if decision.meeting_id != self.meeting.meeting_id:
                raise ValueError("decision belongs to another meeting")
            event = event_by_id.get(decision.decision_event_id)
            if event is None or event.phase not in {
                MonetaryPolicyEventPhase.DECISION,
                MonetaryPolicyEventPhase.EMERGENCY_ACTION,
            }:
                raise ValueError("decision does not bind a decision phase")
            if event.published_at_ns != decision.decided_at_ns:
                raise ValueError("decision and phase timestamps differ")
            if decision.expectation.target_phase is not event.phase:
                raise ValueError("expectation targets another decision phase")
            if decision.framework is not self.meeting.framework:
                raise ValueError("decision framework differs from meeting")
            if decision.statement_event_id is not None:
                statement = event_by_id.get(decision.statement_event_id)
                if statement is None or statement.phase not in {
                    MonetaryPolicyEventPhase.STATEMENT,
                    MonetaryPolicyEventPhase.FORWARD_GUIDANCE,
                }:
                    raise ValueError("decision statement link is invalid")
        decision_by_id = {item.decision_id: item for item in decisions}
        for vote in votes:
            if vote.meeting_id != self.meeting.meeting_id:
                raise ValueError("vote tally belongs to another meeting")
            event = event_by_id.get(vote.vote_event_id)
            if (
                event is None
                or event.phase is not MonetaryPolicyEventPhase.VOTE_SPLIT
            ):
                raise ValueError("vote tally does not bind a vote phase")
            if vote.decision_id not in decision_by_id:
                raise ValueError("vote tally does not bind a bundle decision")
        phases = {item.phase for item in events}
        action_event_ids = {
            item.phase_event_id
            for item in events
            if item.phase
            in {
                MonetaryPolicyEventPhase.DECISION,
                MonetaryPolicyEventPhase.EMERGENCY_ACTION,
            }
        }
        decision_event_ids = {item.decision_event_id for item in decisions}
        if action_event_ids != decision_event_ids:
            raise ValueError(
                "action phases and typed decisions do not reconcile"
            )
        if MonetaryPolicyEventPhase.CANCELLATION in phases and decisions:
            raise ValueError("cancelled meeting cannot contain a decision")
        if self.meeting.action_timing in {
            MonetaryPolicyActionTiming.UNSCHEDULED,
            MonetaryPolicyActionTiming.EMERGENCY,
        }:
            action_events = tuple(
                item
                for item in events
                if item.phase
                in {
                    MonetaryPolicyEventPhase.DECISION,
                    MonetaryPolicyEventPhase.EMERGENCY_ACTION,
                }
            )
            if not action_events:
                raise ValueError("unscheduled meeting lacks an action phase")
            first_action = min(
                action_events, key=lambda item: item.published_at_ns
            )
            if not (
                first_action.published_at_ns
                <= self.meeting.first_known_at_ns
                <= first_action.available_at_ns
            ):
                raise ValueError(
                    "unscheduled action first-known time is not "
                    "publication-bound"
                )
        if (
            self.meeting.action_timing is MonetaryPolicyActionTiming.EMERGENCY
            and (MonetaryPolicyEventPhase.EMERGENCY_ACTION not in phases)
        ):
            raise ValueError("emergency meeting lacks an emergency phase")
        decision_times = [
            item.published_at_ns
            for item in events
            if item.phase
            in {
                MonetaryPolicyEventPhase.DECISION,
                MonetaryPolicyEventPhase.EMERGENCY_ACTION,
            }
        ]
        if decision_times:
            first_decision = min(decision_times)
            for item in events:
                if (
                    item.phase
                    in {
                        MonetaryPolicyEventPhase.PRESS_CONFERENCE,
                        MonetaryPolicyEventPhase.MINUTES,
                        MonetaryPolicyEventPhase.VOTE_SPLIT,
                    }
                    and item.published_at_ns < first_decision
                ):
                    raise ValueError(
                        "post-decision phase precedes the decision"
                    )
        object.__setattr__(self, "phase_events", events)
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(self, "vote_tallies", votes)
        expected = _stable_id("monetary-policy-bundle", self.identity_payload())
        supplied = _optional_text(self.bundle_id)
        if supplied is not None and supplied != expected:
            raise ValueError("bundle_id differs from deterministic identity")
        object.__setattr__(self, "bundle_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting": self.meeting.to_dict(),
            "phase_events": [item.to_dict() for item in self.phase_events],
            "decisions": [item.to_dict() for item in self.decisions],
            "vote_tallies": [item.to_dict() for item in self.vote_tallies],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "bundle_id": self.bundle_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> MonetaryPolicyMeetingBundleV1:
        return cls(
            meeting=MonetaryPolicyMeetingV1.from_dict(
                _mapping(data.get("meeting"), "meeting")
            ),
            phase_events=tuple(
                MonetaryPolicyPhaseEventV1.from_dict(
                    _mapping(item, "phase event")
                )
                for item in _sequence(data.get("phase_events"), "phase_events")
            ),
            decisions=tuple(
                MonetaryPolicyDecisionV1.from_dict(_mapping(item, "decision"))
                for item in _sequence(data.get("decisions"), "decisions")
            ),
            vote_tallies=tuple(
                MonetaryPolicyVoteTallyV1.from_dict(
                    _mapping(item, "vote tally")
                )
                for item in _sequence(data.get("vote_tallies"), "vote_tallies")
            ),
            bundle_id=str(data.get("bundle_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> MonetaryPolicyMeetingBundleV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("monetary-policy bundle is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class MonetaryPolicyFrameworkProfileV1:
    """Qualified policy-framework semantics for one scoped economy."""

    economy_code: str
    institution: str
    source_key: str
    framework: MonetaryPolicyFramework
    accepted_setting_kinds: tuple[MonetaryPolicySettingKind, ...]
    supported_phases: tuple[MonetaryPolicyEventPhase, ...]
    supports_unscheduled_actions: bool
    rationale: str
    profile_id: str = ""
    schema_version: str = MONETARY_POLICY_FRAMEWORK_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != MONETARY_POLICY_FRAMEWORK_PROFILE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported monetary-policy profile schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self, "institution", _required_text(self.institution, "institution")
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "framework",
            MonetaryPolicyFramework.from_value(self.framework),
        )
        kinds = tuple(
            sorted(
                {
                    MonetaryPolicySettingKind.from_value(item)
                    for item in self.accepted_setting_kinds
                },
                key=lambda item: item.value,
            )
        )
        phases = tuple(
            sorted(
                {
                    MonetaryPolicyEventPhase.from_value(item)
                    for item in self.supported_phases
                },
                key=lambda item: item.value,
            )
        )
        if not kinds or MonetaryPolicyEventPhase.DECISION not in phases:
            raise ValueError("profile requires settings and decision semantics")
        if not isinstance(self.supports_unscheduled_actions, bool):
            raise TypeError("supports_unscheduled_actions must be boolean")
        if self.supports_unscheduled_actions and (
            MonetaryPolicyEventPhase.EMERGENCY_ACTION not in phases
        ):
            raise ValueError(
                "unscheduled support requires emergency-action phase"
            )
        object.__setattr__(self, "accepted_setting_kinds", kinds)
        object.__setattr__(self, "supported_phases", phases)
        object.__setattr__(
            self, "rationale", _required_text(self.rationale, "rationale")
        )
        expected = _stable_id(
            "monetary-policy-profile", self.identity_payload()
        )
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("profile_id differs from deterministic identity")
        object.__setattr__(self, "profile_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "institution": self.institution,
            "source_key": self.source_key,
            "framework": self.framework.value,
            "accepted_setting_kinds": [
                item.value for item in self.accepted_setting_kinds
            ],
            "supported_phases": [item.value for item in self.supported_phases],
            "supports_unscheduled_actions": self.supports_unscheduled_actions,
            "rationale": self.rationale,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> MonetaryPolicyFrameworkProfileV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            institution=str(data.get("institution", "")),
            source_key=str(data.get("source_key", "")),
            framework=MonetaryPolicyFramework.from_value(
                str(data.get("framework", ""))
            ),
            accepted_setting_kinds=tuple(
                MonetaryPolicySettingKind.from_value(str(item))
                for item in _sequence(
                    data.get("accepted_setting_kinds"),
                    "accepted_setting_kinds",
                )
            ),
            supported_phases=tuple(
                MonetaryPolicyEventPhase.from_value(str(item))
                for item in _sequence(
                    data.get("supported_phases"), "supported_phases"
                )
            ),
            supports_unscheduled_actions=cast(
                bool, data.get("supports_unscheduled_actions")
            ),
            rationale=str(data.get("rationale", "")),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_FRAMEWORK_BY_ECONOMY: Mapping[str, MonetaryPolicyFramework] = {
    "US": MonetaryPolicyFramework.TARGET_RANGE,
    "EA": MonetaryPolicyFramework.RATE_SET,
    "DE": MonetaryPolicyFramework.RATE_SET,
    "FR": MonetaryPolicyFramework.RATE_SET,
    "JP": MonetaryPolicyFramework.YIELD_CURVE_CONTROL,
    "HK": MonetaryPolicyFramework.CURRENCY_BOARD,
    "SG": MonetaryPolicyFramework.EXCHANGE_RATE_BAND,
}

_SETTING_KINDS_BY_FRAMEWORK: Mapping[
    MonetaryPolicyFramework, tuple[MonetaryPolicySettingKind, ...]
] = {
    MonetaryPolicyFramework.POLICY_RATE: (
        MonetaryPolicySettingKind.SCALAR_RATE,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.TARGET_RANGE: (
        MonetaryPolicySettingKind.TARGET_RANGE,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.RATE_SET: (
        MonetaryPolicySettingKind.RATE_SET,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.YIELD_CURVE_CONTROL: (
        MonetaryPolicySettingKind.SCALAR_RATE,
        MonetaryPolicySettingKind.RATE_SET,
        MonetaryPolicySettingKind.YIELD_TARGET,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.CURRENCY_BOARD: (
        MonetaryPolicySettingKind.CURRENCY_BOARD_RATE,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.EXCHANGE_RATE_BAND: (
        MonetaryPolicySettingKind.EXCHANGE_RATE_BAND,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.LIQUIDITY_OPERATING_TARGET: (
        MonetaryPolicySettingKind.SCALAR_RATE,
        MonetaryPolicySettingKind.RATE_SET,
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
    MonetaryPolicyFramework.CATEGORICAL: (
        MonetaryPolicySettingKind.CATEGORICAL,
    ),
}


def built_in_monetary_policy_profiles(
    registry: OfficialSourceRegistryV1,
) -> tuple[MonetaryPolicyFrameworkProfileV1, ...]:
    """Build the reviewed framework profile for every registry economy."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    profiles: list[MonetaryPolicyFrameworkProfileV1] = []
    for economy_code in registry.scoped_economies:
        source = registry.primary_source(
            economy_code, EconomicEventFamily.MONETARY_POLICY
        )
        framework = _FRAMEWORK_BY_ECONOMY.get(
            economy_code, MonetaryPolicyFramework.POLICY_RATE
        )
        profiles.append(
            MonetaryPolicyFrameworkProfileV1(
                economy_code=economy_code,
                institution=source.institution,
                source_key=source.source_key,
                framework=framework,
                accepted_setting_kinds=_SETTING_KINDS_BY_FRAMEWORK[framework],
                supported_phases=tuple(MonetaryPolicyEventPhase),
                supports_unscheduled_actions=True,
                rationale=(
                    "Preserve the institution's operating framework. Map only "
                    "source-published phases; unsupported ones remain gaps."
                ),
            )
        )
    return tuple(sorted(profiles, key=lambda item: item.economy_code))


@dataclass(frozen=True, slots=True)
class MonetaryPolicyFrameworkAuditV1:
    """Coverage proof for registry ownership and special framework mappings."""

    registry_id: str
    profile_ids: tuple[str, ...]
    missing_economies: tuple[str, ...]
    duplicate_economies: tuple[str, ...]
    source_mismatches: tuple[str, ...]
    framework_mismatches: tuple[str, ...]
    complete: bool
    audit_id: str = ""
    schema_version: str = MONETARY_POLICY_FRAMEWORK_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != MONETARY_POLICY_FRAMEWORK_AUDIT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported monetary-policy audit schema")
        object.__setattr__(
            self, "registry_id", _required_text(self.registry_id, "registry_id")
        )
        object.__setattr__(
            self, "profile_ids", _texts(self.profile_ids, "profile_id")
        )
        for name in (
            "missing_economies",
            "duplicate_economies",
            "source_mismatches",
            "framework_mismatches",
        ):
            object.__setattr__(self, name, _texts(getattr(self, name), name))
        expected_complete = not any(
            (
                self.missing_economies,
                self.duplicate_economies,
                self.source_mismatches,
                self.framework_mismatches,
            )
        )
        if (
            not isinstance(self.complete, bool)
            or self.complete != expected_complete
        ):
            raise ValueError(
                "monetary-policy audit completeness is inconsistent"
            )
        expected = _stable_id("monetary-policy-audit", self.identity_payload())
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id differs from deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_ids": list(self.profile_ids),
            "missing_economies": list(self.missing_economies),
            "duplicate_economies": list(self.duplicate_economies),
            "source_mismatches": list(self.source_mismatches),
            "framework_mismatches": list(self.framework_mismatches),
            "complete": self.complete,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> MonetaryPolicyFrameworkAuditV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_ids=tuple(
                str(item)
                for item in _sequence(data.get("profile_ids"), "profile_ids")
            ),
            missing_economies=tuple(
                str(item)
                for item in _sequence(
                    data.get("missing_economies"), "missing_economies"
                )
            ),
            duplicate_economies=tuple(
                str(item)
                for item in _sequence(
                    data.get("duplicate_economies"), "duplicate_economies"
                )
            ),
            source_mismatches=tuple(
                str(item)
                for item in _sequence(
                    data.get("source_mismatches"), "source_mismatches"
                )
            ),
            framework_mismatches=tuple(
                str(item)
                for item in _sequence(
                    data.get("framework_mismatches"),
                    "framework_mismatches",
                )
            ),
            complete=cast(bool, data.get("complete")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def audit_monetary_policy_frameworks(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[MonetaryPolicyFrameworkProfileV1],
) -> MonetaryPolicyFrameworkAuditV1:
    """Audit one exact profile and official owner for every scoped economy."""
    values = tuple(profiles)
    counts = {
        code: sum(item.economy_code == code for item in values)
        for code in registry.scoped_economies
    }
    missing = tuple(code for code, count in counts.items() if count == 0)
    duplicates = tuple(code for code, count in counts.items() if count > 1)
    source_mismatches: list[str] = []
    framework_mismatches: list[str] = []
    for profile in values:
        if profile.economy_code not in registry.scoped_economies:
            source_mismatches.append(f"{profile.economy_code}:outside-scope")
            continue
        source = registry.primary_source(
            profile.economy_code, EconomicEventFamily.MONETARY_POLICY
        )
        if (profile.source_key, profile.institution) != (
            source.source_key,
            source.institution,
        ):
            source_mismatches.append(profile.economy_code)
        expected_framework = _FRAMEWORK_BY_ECONOMY.get(
            profile.economy_code, MonetaryPolicyFramework.POLICY_RATE
        )
        expected_kinds = set(_SETTING_KINDS_BY_FRAMEWORK[expected_framework])
        if (
            profile.framework is not expected_framework
            or not expected_kinds.issubset(profile.accepted_setting_kinds)
            or not profile.supports_unscheduled_actions
            or MonetaryPolicyEventPhase.EMERGENCY_ACTION
            not in profile.supported_phases
        ):
            framework_mismatches.append(profile.economy_code)
    unknown_profile_ids = len({item.profile_id for item in values}) != len(
        values
    )
    if unknown_profile_ids:
        duplicates = tuple(sorted({*duplicates, "profile-id"}))
    return MonetaryPolicyFrameworkAuditV1(
        registry_id=registry.registry_id,
        profile_ids=tuple(item.profile_id for item in values),
        missing_economies=missing,
        duplicate_economies=duplicates,
        source_mismatches=tuple(source_mismatches),
        framework_mismatches=tuple(framework_mismatches),
        complete=not any(
            (missing, duplicates, source_mismatches, framework_mismatches)
        ),
    )


def require_monetary_policy_framework_coverage(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[MonetaryPolicyFrameworkProfileV1],
) -> MonetaryPolicyFrameworkAuditV1:
    """Return a complete audit or fail closed on a semantic coverage gap."""
    audit = audit_monetary_policy_frameworks(registry, profiles)
    if not audit.complete:
        raise ValueError("monetary-policy framework coverage is incomplete")
    return audit


__all__ = [
    "MAX_MONETARY_POLICY_PHASE_EVENTS",
    "MAX_MONETARY_POLICY_SETTING_COMPONENTS",
    "MAX_MONETARY_POLICY_VOTE_BUCKETS",
    "MONETARY_POLICY_DECISION_SCHEMA_VERSION",
    "MONETARY_POLICY_EXPECTATION_SCHEMA_VERSION",
    "MONETARY_POLICY_FRAMEWORK_AUDIT_SCHEMA_VERSION",
    "MONETARY_POLICY_FRAMEWORK_PROFILE_SCHEMA_VERSION",
    "MONETARY_POLICY_MEETING_BUNDLE_SCHEMA_VERSION",
    "MONETARY_POLICY_MEETING_SCHEMA_VERSION",
    "MONETARY_POLICY_PHASE_EVENT_SCHEMA_VERSION",
    "MONETARY_POLICY_SETTING_COMPONENT_SCHEMA_VERSION",
    "MONETARY_POLICY_SETTING_SCHEMA_VERSION",
    "MONETARY_POLICY_SURPRISE_SCHEMA_VERSION",
    "MONETARY_POLICY_VOTE_BUCKET_SCHEMA_VERSION",
    "MONETARY_POLICY_VOTE_TALLY_SCHEMA_VERSION",
    "MonetaryPolicyActionTiming",
    "MonetaryPolicyDecisionDirection",
    "MonetaryPolicyDecisionV1",
    "MonetaryPolicyEventPhase",
    "MonetaryPolicyExpectationKind",
    "MonetaryPolicyExpectationV1",
    "MonetaryPolicyFramework",
    "MonetaryPolicyFrameworkAuditV1",
    "MonetaryPolicyFrameworkProfileV1",
    "MonetaryPolicyMeetingBundleV1",
    "MonetaryPolicyMeetingV1",
    "MonetaryPolicyPhaseEventV1",
    "MonetaryPolicyScheduleEvidenceKind",
    "MonetaryPolicySettingComponentV1",
    "MonetaryPolicySettingKind",
    "MonetaryPolicySettingV1",
    "MonetaryPolicySurpriseKind",
    "MonetaryPolicySurpriseV1",
    "MonetaryPolicyVoteBucketV1",
    "MonetaryPolicyVoteTallyV1",
    "audit_monetary_policy_frameworks",
    "built_in_monetary_policy_profiles",
    "compute_monetary_policy_rate_surprise",
    "declare_monetary_policy_text_surprise",
    "require_monetary_policy_framework_coverage",
]
