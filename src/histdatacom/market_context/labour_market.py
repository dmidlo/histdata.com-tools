"""Provider-neutral labour-market release and revision semantics.

The contracts preserve survey, population, measure, transformation, seasonal,
and vintage identity.  Published employment changes remain distinct from
changes recomputed from level vintages, and multi-indicator release packages
share evidence without collapsing payrolls, unemployment, earnings, or hours
into one synthetic event.
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

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicReleaseStage,
)
from histdatacom.market_context.official_sources import OfficialSourceRegistryV1
from histdatacom.runtime_contracts import JSONValue

LABOUR_CONCEPT_SCHEMA_VERSION = "histdatacom.labour-concept.v1"
LABOUR_OBSERVATION_SCHEMA_VERSION = "histdatacom.labour-observation.v1"
LABOUR_VINTAGE_CHAIN_SCHEMA_VERSION = "histdatacom.labour-vintage-chain.v1"
LABOUR_CHANGE_DERIVATION_SCHEMA_VERSION = (
    "histdatacom.labour-change-derivation.v1"
)
LABOUR_EXPECTATION_SCHEMA_VERSION = "histdatacom.labour-expectation.v1"
LABOUR_RELEASE_TRIPLET_SCHEMA_VERSION = "histdatacom.labour-release-triplet.v1"
LABOUR_RELEASE_SCHEMA_VERSION = "histdatacom.labour-release.v1"
LABOUR_RELEASE_PACKAGE_SCHEMA_VERSION = "histdatacom.labour-release-package.v1"
LABOUR_METHODOLOGY_EVENT_SCHEMA_VERSION = (
    "histdatacom.labour-methodology-event.v1"
)
LABOUR_ECONOMY_PROFILE_SCHEMA_VERSION = "histdatacom.labour-economy-profile.v1"
LABOUR_PROFILE_AUDIT_SCHEMA_VERSION = "histdatacom.labour-profile-audit.v1"

MAX_LABOUR_REVISIONS = 10_000
MAX_LABOUR_RELEASE_OBSERVATIONS = 10_000
MAX_LABOUR_PACKAGE_RELEASES = 128
_WEEK_NS = 7 * 86_400 * 1_000_000_000

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class LabourMeasure(str, Enum):
    """Exact labour statistic represented by a concept."""

    EMPLOYMENT_LEVEL = "employment-level"
    EMPLOYMENT_CHANGE = "employment-change"
    PAYROLL_LEVEL = "payroll-level"
    PAYROLL_CHANGE = "payroll-change"
    UNEMPLOYMENT_LEVEL = "unemployment-level"
    UNEMPLOYMENT_RATE = "unemployment-rate"
    PARTICIPATION_RATE = "participation-rate"
    EMPLOYMENT_POPULATION_RATIO = "employment-population-ratio"
    EARNINGS_LEVEL = "earnings-level"
    EARNINGS_CHANGE = "earnings-change"
    HOURS = "hours"
    VACANCIES = "vacancies"
    HIRES = "hires"
    SEPARATIONS = "separations"
    CLAIMANT_COUNT = "claimant-count"
    INITIAL_BENEFIT_CLAIMS = "initial-benefit-claims"
    CONTINUING_BENEFIT_CLAIMS = "continuing-benefit-claims"

    @classmethod
    def from_value(cls, value: str | LabourMeasure) -> LabourMeasure:
        return _enum_value(cls, value, "labour measure")


class LabourSurveyBasis(str, Enum):
    """Survey or administrative system that produced a statistic."""

    HOUSEHOLD_SURVEY = "household-survey"
    ESTABLISHMENT_SURVEY = "establishment-survey"
    VACANCY_SURVEY = "vacancy-survey"
    BUSINESS_SURVEY = "business-survey"
    ADMINISTRATIVE_REGISTER = "administrative-register"
    BENEFIT_CLAIMS_ADMINISTRATIVE = "benefit-claims-administrative"
    MIXED_SOURCE = "mixed-source"

    @classmethod
    def from_value(cls, value: str | LabourSurveyBasis) -> LabourSurveyBasis:
        return _enum_value(cls, value, "labour survey basis")


class LabourTransformation(str, Enum):
    """Displayed transformation without inferring from its unit."""

    LEVEL = "level"
    PERIOD_CHANGE = "period-change"
    RATE_PERCENT = "rate-percent"
    MONTH_OVER_MONTH_PERCENT = "month-over-month-percent"
    YEAR_OVER_YEAR_PERCENT = "year-over-year-percent"
    INDEX_LEVEL = "index-level"
    AVERAGE_HOURS = "average-hours"

    @classmethod
    def from_value(
        cls, value: str | LabourTransformation
    ) -> LabourTransformation:
        return _enum_value(cls, value, "labour transformation")


class LabourFrequency(str, Enum):
    """Reference-period frequency of a labour concept."""

    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"

    @classmethod
    def from_value(cls, value: str | LabourFrequency) -> LabourFrequency:
        return _enum_value(cls, value, "labour frequency")


class LabourSeasonalBasis(str, Enum):
    """Publisher-declared seasonal/calendar adjustment."""

    NOT_SEASONALLY_ADJUSTED = "not-seasonally-adjusted"
    SEASONALLY_ADJUSTED = "seasonally-adjusted"
    SEASONALLY_AND_CALENDAR_ADJUSTED = "seasonally-and-calendar-adjusted"
    CALENDAR_ADJUSTED = "calendar-adjusted"
    TREND_CYCLE = "trend-cycle"
    NOT_APPLICABLE = "not-applicable"

    @classmethod
    def from_value(
        cls, value: str | LabourSeasonalBasis
    ) -> LabourSeasonalBasis:
        return _enum_value(cls, value, "labour seasonal basis")


class LabourObservationBasis(str, Enum):
    """Whether a statistic was published or derived from level vintages."""

    SOURCE_PUBLISHED = "source-published"
    DERIVED_FROM_LEVEL_VINTAGES = "derived-from-level-vintages"

    @classmethod
    def from_value(
        cls, value: str | LabourObservationBasis
    ) -> LabourObservationBasis:
        return _enum_value(cls, value, "labour observation basis")


class LabourRevisionKind(str, Enum):
    """Reason one labour observation superseded another."""

    INITIAL = "initial"
    ROUTINE = "routine"
    SIMULTANEOUS_PREVIOUS = "simultaneous-previous"
    BENCHMARK = "benchmark"
    METHODOLOGY = "methodology"
    SEASONAL_REANALYSIS = "seasonal-reanalysis"

    @classmethod
    def from_value(cls, value: str | LabourRevisionKind) -> LabourRevisionKind:
        return _enum_value(cls, value, "labour revision kind")


class LabourDerivationVintageBasis(str, Enum):
    """Vintage policy used to derive a change from two levels."""

    INITIAL_RELEASE = "initial-release"
    AS_KNOWN_AT_RELEASE = "as-known-at-release"
    LATEST_CURRENT = "latest-current"

    @classmethod
    def from_value(
        cls, value: str | LabourDerivationVintageBasis
    ) -> LabourDerivationVintageBasis:
        return _enum_value(cls, value, "labour derivation vintage basis")


class LabourExpectationKind(str, Enum):
    """Provenance and target scope of a labour expectation."""

    EVENT_CONSENSUS = "event-consensus"
    OFFICIAL_SURVEY_EVENT_TARGET = "official-survey-event-target"
    OFFICIAL_SURVEY_PERIOD_TARGET = "official-survey-period-target"
    GOVERNMENT_PROJECTION = "government-projection"
    MACHINE_EVENT_TARGET = "machine-event-target"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | LabourExpectationKind
    ) -> LabourExpectationKind:
        return _enum_value(cls, value, "labour expectation kind")

    @property
    def calendar_style_eligible(self) -> bool:
        return self in {
            LabourExpectationKind.EVENT_CONSENSUS,
            LabourExpectationKind.OFFICIAL_SURVEY_EVENT_TARGET,
            LabourExpectationKind.MACHINE_EVENT_TARGET,
        }


class LabourMethodologyChangeKind(str, Enum):
    """Structural change affecting labour-series comparability."""

    BENCHMARK_REVISION = "benchmark-revision"
    POPULATION_CONTROL = "population-control"
    SEASONAL_REANALYSIS = "seasonal-reanalysis"
    CLASSIFICATION_CHANGE = "classification-change"
    COVERAGE_CHANGE = "coverage-change"
    SURVEY_REDESIGN = "survey-redesign"
    ADMINISTRATIVE_SYSTEM_CHANGE = "administrative-system-change"

    @classmethod
    def from_value(
        cls, value: str | LabourMethodologyChangeKind
    ) -> LabourMethodologyChangeKind:
        return _enum_value(cls, value, "labour methodology change kind")


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


def _optional_key(value: object, label: str) -> str | None:
    text = _optional_text(value)
    return None if text is None else _key(text, label)


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


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value < 0:
        raise ValueError(f"{label} must be nonnegative")
    return value


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
    if len(result) > MAX_LABOUR_RELEASE_OBSERVATIONS:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _keys(
    values: Iterable[object], label: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > MAX_LABOUR_RELEASE_OBSERVATIONS:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _mapping(value: object, label: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return value


def _sequence(value: object, label: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{label} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _month_index(timestamp_ns: int) -> int:
    moment = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, timezone.utc)
    return moment.year * 12 + moment.month - 1


@dataclass(frozen=True, slots=True)
class LabourConceptV1:
    """One exact labour statistic, survey, population, and transformation."""

    economy_code: str
    indicator_key: str
    source_series_id: str
    measure: LabourMeasure
    survey_basis: LabourSurveyBasis
    transformation: LabourTransformation
    frequency: LabourFrequency
    seasonal_basis: LabourSeasonalBasis
    population_scope: str
    industry_scope: str | None
    worker_scope: str
    duration_scope: str | None
    unit: str
    scale: float
    methodology_era_id: str
    concept_id: str = ""
    schema_version: str = LABOUR_CONCEPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_CONCEPT_SCHEMA_VERSION:
            raise ValueError("unsupported labour concept schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        for name in ("indicator_key", "source_series_id", "methodology_era_id"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        measure = LabourMeasure.from_value(self.measure)
        survey = LabourSurveyBasis.from_value(self.survey_basis)
        transformation = LabourTransformation.from_value(self.transformation)
        frequency = LabourFrequency.from_value(self.frequency)
        seasonal = LabourSeasonalBasis.from_value(self.seasonal_basis)
        expected_transformations: Mapping[
            LabourMeasure, frozenset[LabourTransformation]
        ] = {
            LabourMeasure.EMPLOYMENT_LEVEL: frozenset(
                {LabourTransformation.LEVEL, LabourTransformation.INDEX_LEVEL}
            ),
            LabourMeasure.EMPLOYMENT_CHANGE: frozenset(
                {LabourTransformation.PERIOD_CHANGE}
            ),
            LabourMeasure.PAYROLL_LEVEL: frozenset(
                {LabourTransformation.LEVEL}
            ),
            LabourMeasure.PAYROLL_CHANGE: frozenset(
                {LabourTransformation.PERIOD_CHANGE}
            ),
            LabourMeasure.UNEMPLOYMENT_LEVEL: frozenset(
                {LabourTransformation.LEVEL}
            ),
            LabourMeasure.UNEMPLOYMENT_RATE: frozenset(
                {LabourTransformation.RATE_PERCENT}
            ),
            LabourMeasure.PARTICIPATION_RATE: frozenset(
                {LabourTransformation.RATE_PERCENT}
            ),
            LabourMeasure.EMPLOYMENT_POPULATION_RATIO: frozenset(
                {LabourTransformation.RATE_PERCENT}
            ),
            LabourMeasure.EARNINGS_LEVEL: frozenset(
                {LabourTransformation.LEVEL, LabourTransformation.INDEX_LEVEL}
            ),
            LabourMeasure.EARNINGS_CHANGE: frozenset(
                {
                    LabourTransformation.MONTH_OVER_MONTH_PERCENT,
                    LabourTransformation.YEAR_OVER_YEAR_PERCENT,
                }
            ),
            LabourMeasure.HOURS: frozenset(
                {LabourTransformation.AVERAGE_HOURS}
            ),
            LabourMeasure.VACANCIES: frozenset({LabourTransformation.LEVEL}),
            LabourMeasure.HIRES: frozenset({LabourTransformation.LEVEL}),
            LabourMeasure.SEPARATIONS: frozenset({LabourTransformation.LEVEL}),
            LabourMeasure.CLAIMANT_COUNT: frozenset(
                {LabourTransformation.LEVEL, LabourTransformation.PERIOD_CHANGE}
            ),
            LabourMeasure.INITIAL_BENEFIT_CLAIMS: frozenset(
                {LabourTransformation.LEVEL}
            ),
            LabourMeasure.CONTINUING_BENEFIT_CLAIMS: frozenset(
                {LabourTransformation.LEVEL}
            ),
        }
        if transformation not in expected_transformations[measure]:
            raise ValueError("transformation is invalid for labour measure")
        if (
            measure
            in {
                LabourMeasure.PAYROLL_LEVEL,
                LabourMeasure.PAYROLL_CHANGE,
            }
            and survey is not LabourSurveyBasis.ESTABLISHMENT_SURVEY
        ):
            raise ValueError(
                "payroll measure requires establishment survey identity"
            )
        if (
            measure
            in {
                LabourMeasure.INITIAL_BENEFIT_CLAIMS,
                LabourMeasure.CONTINUING_BENEFIT_CLAIMS,
            }
            and survey is not LabourSurveyBasis.BENEFIT_CLAIMS_ADMINISTRATIVE
        ):
            raise ValueError("benefit claims require administrative identity")
        if measure in {
            LabourMeasure.VACANCIES,
            LabourMeasure.HIRES,
            LabourMeasure.SEPARATIONS,
        } and survey not in {
            LabourSurveyBasis.VACANCY_SURVEY,
            LabourSurveyBasis.BUSINESS_SURVEY,
            LabourSurveyBasis.ADMINISTRATIVE_REGISTER,
        }:
            raise ValueError(
                "labour-flow measure requires an eligible source basis"
            )
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "survey_basis", survey)
        object.__setattr__(self, "transformation", transformation)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "seasonal_basis", seasonal)
        object.__setattr__(
            self,
            "population_scope",
            _required_text(self.population_scope, "population_scope"),
        )
        object.__setattr__(
            self, "industry_scope", _optional_text(self.industry_scope)
        )
        object.__setattr__(
            self,
            "worker_scope",
            _required_text(self.worker_scope, "worker_scope"),
        )
        object.__setattr__(
            self, "duration_scope", _optional_text(self.duration_scope)
        )
        object.__setattr__(self, "unit", _required_text(self.unit, "unit"))
        object.__setattr__(self, "scale", scale)
        expected = _stable_id("labour-concept", self.identity_payload())
        supplied = _optional_text(self.concept_id)
        if supplied is not None and supplied != expected:
            raise ValueError("concept_id differs from deterministic identity")
        object.__setattr__(self, "concept_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "indicator_key": self.indicator_key,
            "source_series_id": self.source_series_id,
            "measure": self.measure.value,
            "survey_basis": self.survey_basis.value,
            "transformation": self.transformation.value,
            "frequency": self.frequency.value,
            "seasonal_basis": self.seasonal_basis.value,
            "population_scope": self.population_scope,
            "industry_scope": self.industry_scope,
            "worker_scope": self.worker_scope,
            "duration_scope": self.duration_scope,
            "unit": self.unit,
            "scale": self.scale,
            "methodology_era_id": self.methodology_era_id,
        }

    def level_series_payload(self) -> dict[str, JSONValue]:
        payload = self.identity_payload()
        for key in (
            "schema_version",
            "measure",
            "transformation",
            "unit",
            "scale",
        ):
            payload.pop(key)
        return payload

    @property
    def level_series_id(self) -> str:
        return _stable_id("labour-level-series", self.level_series_payload())

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "concept_id": self.concept_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourConceptV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            indicator_key=str(data.get("indicator_key", "")),
            source_series_id=str(data.get("source_series_id", "")),
            measure=LabourMeasure.from_value(str(data.get("measure", ""))),
            survey_basis=LabourSurveyBasis.from_value(
                str(data.get("survey_basis", ""))
            ),
            transformation=LabourTransformation.from_value(
                str(data.get("transformation", ""))
            ),
            frequency=LabourFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            seasonal_basis=LabourSeasonalBasis.from_value(
                str(data.get("seasonal_basis", ""))
            ),
            population_scope=str(data.get("population_scope", "")),
            industry_scope=_optional_text(data.get("industry_scope")),
            worker_scope=str(data.get("worker_scope", "")),
            duration_scope=_optional_text(data.get("duration_scope")),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            methodology_era_id=str(data.get("methodology_era_id", "")),
            concept_id=str(data.get("concept_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> LabourConceptV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("labour concept is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class LabourObservationV1:
    """One immutable published or derived labour-statistic vintage."""

    concept: LabourConceptV1
    logical_event_key: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    basis: LabourObservationBasis
    value: float
    raw_lexical: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    revision_sequence: int
    revision_kind: LabourRevisionKind
    supersedes_observation_id: str | None
    derived_from_observation_ids: tuple[str, ...]
    derivation_id: str | None
    limitations: tuple[str, ...]
    observation_id: str = ""
    schema_version: str = LABOUR_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported labour observation schema")
        if not isinstance(self.concept, LabourConceptV1):
            raise TypeError("observation requires a labour concept")
        for name in (
            "logical_event_key",
            "source_key",
            "source_release_id",
            "source_request_id",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        object.__setattr__(
            self,
            "reference_period_end_ns",
            _bounded_ns(
                self.reference_period_end_ns, "reference_period_end_ns"
            ),
        )
        object.__setattr__(
            self, "stage", EconomicReleaseStage.from_value(self.stage)
        )
        basis = LabourObservationBasis.from_value(self.basis)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "value", _finite(self.value, "value"))
        object.__setattr__(
            self, "raw_lexical", _required_text(self.raw_lexical, "raw_lexical")
        )
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("observation availability precedes publication")
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        revision = _nonnegative_int(self.revision_sequence, "revision_sequence")
        if revision > MAX_LABOUR_REVISIONS:
            raise ValueError("revision_sequence exceeds bound")
        kind = LabourRevisionKind.from_value(self.revision_kind)
        supersedes = _optional_text(self.supersedes_observation_id)
        if revision == 0:
            if kind is not LabourRevisionKind.INITIAL or supersedes is not None:
                raise ValueError(
                    "initial observation has invalid revision metadata"
                )
        elif kind is LabourRevisionKind.INITIAL or supersedes is None:
            raise ValueError("revision must identify kind and predecessor")
        derived_ids = _texts(
            self.derived_from_observation_ids, "derived observation id"
        )
        derivation = _optional_text(self.derivation_id)
        if basis is LabourObservationBasis.SOURCE_PUBLISHED:
            if derived_ids or derivation is not None:
                raise ValueError(
                    "published observation contains derivation links"
                )
        elif len(derived_ids) != 2 or derivation is None:
            raise ValueError("derived change requires exactly two level inputs")
        object.__setattr__(self, "revision_sequence", revision)
        object.__setattr__(self, "revision_kind", kind)
        object.__setattr__(self, "supersedes_observation_id", supersedes)
        object.__setattr__(self, "derived_from_observation_ids", derived_ids)
        object.__setattr__(self, "derivation_id", derivation)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("labour-observation", self.identity_payload())
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "observation_id differs from deterministic identity"
            )
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "concept": self.concept.to_dict(),
            "logical_event_key": self.logical_event_key,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "stage": self.stage.value,
            "basis": self.basis.value,
            "value": self.value,
            "raw_lexical": self.raw_lexical,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "revision_sequence": self.revision_sequence,
            "revision_kind": self.revision_kind.value,
            "supersedes_observation_id": self.supersedes_observation_id,
            "derived_from_observation_ids": list(
                self.derived_from_observation_ids
            ),
            "derivation_id": self.derivation_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourObservationV1:
        return cls(
            concept=LabourConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            logical_event_key=str(data.get("logical_event_key", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            basis=LabourObservationBasis.from_value(str(data.get("basis", ""))),
            value=cast(float, data.get("value")),
            raw_lexical=str(data.get("raw_lexical", "")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            revision_sequence=cast(int, data.get("revision_sequence")),
            revision_kind=LabourRevisionKind.from_value(
                str(data.get("revision_kind", ""))
            ),
            supersedes_observation_id=_optional_text(
                data.get("supersedes_observation_id")
            ),
            derived_from_observation_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("derived_from_observation_ids"),
                    "derived_from_observation_ids",
                )
            ),
            derivation_id=_optional_text(data.get("derivation_id")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourVintageChainV1:
    """Initial labour value plus every routine or benchmark revision."""

    observations: tuple[LabourObservationV1, ...]
    chain_id: str = ""
    schema_version: str = LABOUR_VINTAGE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_VINTAGE_CHAIN_SCHEMA_VERSION:
            raise ValueError("unsupported labour vintage-chain schema")
        observations = tuple(
            sorted(self.observations, key=lambda item: item.revision_sequence)
        )
        if not observations or len(observations) > MAX_LABOUR_REVISIONS + 1:
            raise ValueError("labour vintage-chain size is outside bounds")
        if any(
            not isinstance(item, LabourObservationV1) for item in observations
        ):
            raise TypeError("vintage chain contains a non-v1 observation")
        first = observations[0]
        common = (
            first.concept.concept_id,
            first.logical_event_key,
            first.reference_period,
            first.reference_period_end_ns,
            first.stage,
        )
        for position, observation in enumerate(observations):
            if observation.revision_sequence != position:
                raise ValueError("vintage chain revision sequence has a gap")
            if (
                observation.concept.concept_id,
                observation.logical_event_key,
                observation.reference_period,
                observation.reference_period_end_ns,
                observation.stage,
            ) != common:
                raise ValueError("vintage chain changes the released concept")
            if position and (
                observation.supersedes_observation_id
                != observations[position - 1].observation_id
            ):
                raise ValueError("revision does not supersede its predecessor")
            if position and (
                observation.available_at_ns
                < observations[position - 1].available_at_ns
            ):
                raise ValueError("revision availability moves backward")
        object.__setattr__(self, "observations", observations)
        expected = _stable_id("labour-vintage-chain", self.identity_payload())
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id differs from deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    @property
    def initial(self) -> LabourObservationV1:
        return self.observations[0]

    def as_known_at(self, decision_at_ns: int) -> LabourObservationV1 | None:
        cutoff = _bounded_ns(decision_at_ns, "decision_at_ns")
        visible = [
            item for item in self.observations if item.available_at_ns <= cutoff
        ]
        return visible[-1] if visible else None

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "observations": [item.to_dict() for item in self.observations],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "chain_id": self.chain_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourVintageChainV1:
        return cls(
            observations=tuple(
                LabourObservationV1.from_dict(_mapping(item, "observation"))
                for item in _sequence(data.get("observations"), "observations")
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourChangeDerivationV1:
    """A period change recomputed from two exact retained level vintages."""

    target_concept: LabourConceptV1
    current_level: LabourObservationV1
    previous_level: LabourObservationV1
    vintage_basis: LabourDerivationVintageBasis
    source_change_unavailable: bool
    claimed_initial_actual: bool
    derived_value: float
    tolerance: float
    derivation_id: str = ""
    schema_version: str = LABOUR_CHANGE_DERIVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_CHANGE_DERIVATION_SCHEMA_VERSION:
            raise ValueError("unsupported labour change-derivation schema")
        if not isinstance(self.target_concept, LabourConceptV1):
            raise TypeError("derivation requires a target labour concept")
        if (
            self.target_concept.measure
            not in {
                LabourMeasure.EMPLOYMENT_CHANGE,
                LabourMeasure.PAYROLL_CHANGE,
            }
            or self.target_concept.transformation
            is not LabourTransformation.PERIOD_CHANGE
        ):
            raise ValueError(
                "target concept is not an employment period change"
            )
        if not isinstance(
            self.current_level, LabourObservationV1
        ) or not isinstance(self.previous_level, LabourObservationV1):
            raise TypeError("derivation inputs must use labour observations")
        expected_level_measure = (
            LabourMeasure.EMPLOYMENT_LEVEL
            if self.target_concept.measure is LabourMeasure.EMPLOYMENT_CHANGE
            else LabourMeasure.PAYROLL_LEVEL
        )
        if any(
            item.concept.measure is not expected_level_measure
            or item.concept.transformation is not LabourTransformation.LEVEL
            for item in (self.current_level, self.previous_level)
        ):
            raise ValueError(
                "change derivation requires matching level measures"
            )
        if (
            self.target_concept.level_series_id
            != self.current_level.concept.level_series_id
            or self.current_level.concept.level_series_id
            != self.previous_level.concept.level_series_id
        ):
            raise ValueError(
                "change derivation switches survey or series identity"
            )
        lag = _month_index(
            self.current_level.reference_period_end_ns
        ) - _month_index(self.previous_level.reference_period_end_ns)
        expected_lag = {
            LabourFrequency.MONTHLY: 1,
            LabourFrequency.QUARTERLY: 3,
            LabourFrequency.ANNUAL: 12,
        }.get(self.target_concept.frequency)
        if expected_lag is None or lag != expected_lag:
            raise ValueError("level vintages do not cover one target period")
        vintage_basis = LabourDerivationVintageBasis.from_value(
            self.vintage_basis
        )
        for name in ("source_change_unavailable", "claimed_initial_actual"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if self.claimed_initial_actual and (
            not self.source_change_unavailable
            or vintage_basis is not LabourDerivationVintageBasis.INITIAL_RELEASE
            or self.current_level.revision_sequence != 0
            or self.previous_level.revision_sequence != 0
        ):
            raise ValueError(
                "initial derived change requires unavailable published change "
                "and both initial level vintages"
            )
        derived = _finite(self.derived_value, "derived_value")
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        expected_value = self.current_level.value - self.previous_level.value
        if not math.isclose(
            derived, expected_value, rel_tol=0.0, abs_tol=tolerance
        ):
            raise ValueError(
                "derived change differs from retained level vintages"
            )
        object.__setattr__(self, "vintage_basis", vintage_basis)
        object.__setattr__(self, "derived_value", derived)
        object.__setattr__(self, "tolerance", tolerance)
        expected = _stable_id(
            "labour-change-derivation", self.identity_payload()
        )
        supplied = _optional_text(self.derivation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "derivation_id differs from deterministic identity"
            )
        object.__setattr__(self, "derivation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "target_concept": self.target_concept.to_dict(),
            "current_level": self.current_level.to_dict(),
            "previous_level": self.previous_level.to_dict(),
            "vintage_basis": self.vintage_basis.value,
            "source_change_unavailable": self.source_change_unavailable,
            "claimed_initial_actual": self.claimed_initial_actual,
            "derived_value": self.derived_value,
            "tolerance": self.tolerance,
            "formula": "L_t-L_t-1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "derivation_id": self.derivation_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourChangeDerivationV1:
        return cls(
            target_concept=LabourConceptV1.from_dict(
                _mapping(data.get("target_concept"), "target_concept")
            ),
            current_level=LabourObservationV1.from_dict(
                _mapping(data.get("current_level"), "current_level")
            ),
            previous_level=LabourObservationV1.from_dict(
                _mapping(data.get("previous_level"), "previous_level")
            ),
            vintage_basis=LabourDerivationVintageBasis.from_value(
                str(data.get("vintage_basis", ""))
            ),
            source_change_unavailable=cast(
                bool, data.get("source_change_unavailable")
            ),
            claimed_initial_actual=cast(
                bool, data.get("claimed_initial_actual")
            ),
            derived_value=cast(float, data.get("derived_value")),
            tolerance=cast(float, data.get("tolerance")),
            derivation_id=str(data.get("derivation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourExpectationV1:
    """Pre-release expectation for one exact labour concept and event."""

    target_event_key: str
    concept: LabourConceptV1
    reference_period: str
    kind: LabourExpectationKind
    expected_value: float | None
    collection_start_ns: int | None
    collection_cutoff_ns: int | None
    available_at_ns: int | None
    source_key: str | None
    source_snapshot_id: str | None
    source_uri: str | None
    unavailable_reason: str | None
    limitations: tuple[str, ...]
    expectation_id: str = ""
    schema_version: str = LABOUR_EXPECTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_EXPECTATION_SCHEMA_VERSION:
            raise ValueError("unsupported labour expectation schema")
        object.__setattr__(
            self,
            "target_event_key",
            _key(self.target_event_key, "target_event_key"),
        )
        if not isinstance(self.concept, LabourConceptV1):
            raise TypeError("expectation requires a labour concept")
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        kind = LabourExpectationKind.from_value(self.kind)
        expected_value = _optional_finite(self.expected_value, "expected_value")
        start = (
            None
            if self.collection_start_ns is None
            else _bounded_ns(self.collection_start_ns, "collection_start_ns")
        )
        cutoff = (
            None
            if self.collection_cutoff_ns is None
            else _bounded_ns(self.collection_cutoff_ns, "collection_cutoff_ns")
        )
        available = (
            None
            if self.available_at_ns is None
            else _bounded_ns(self.available_at_ns, "available_at_ns")
        )
        source_key = _optional_key(self.source_key, "source_key")
        snapshot = _optional_text(self.source_snapshot_id)
        uri = _optional_text(self.source_uri)
        reason = _optional_text(self.unavailable_reason)
        if kind is LabourExpectationKind.UNAVAILABLE:
            if expected_value is not None or any(
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
            if expected_value is None:
                raise ValueError("observed expectation requires a value")
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
            uri = _https_uri(uri, "source_uri")
            if reason is not None:
                raise ValueError("observed expectation cannot be unavailable")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "expected_value", expected_value)
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
        expected = _stable_id("labour-expectation", self.identity_payload())
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
            "target_event_key": self.target_event_key,
            "concept": self.concept.to_dict(),
            "reference_period": self.reference_period,
            "kind": self.kind.value,
            "expected_value": self.expected_value,
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
    def from_dict(cls, data: Mapping[str, Any]) -> LabourExpectationV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            concept=LabourConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            reference_period=str(data.get("reference_period", "")),
            kind=LabourExpectationKind.from_value(str(data.get("kind", ""))),
            expected_value=cast(float | None, data.get("expected_value")),
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
class LabourReleaseTripletV1:
    """Initial actual, preceding as-known value, and exact event forecast."""

    target_event_key: str
    actual_initial: LabourObservationV1
    previous_as_known: LabourObservationV1
    previous_as_known_at_ns: int
    expectation: LabourExpectationV1
    triplet_id: str = ""
    schema_version: str = LABOUR_RELEASE_TRIPLET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_RELEASE_TRIPLET_SCHEMA_VERSION:
            raise ValueError("unsupported labour release-triplet schema")
        target = _key(self.target_event_key, "target_event_key")
        if not isinstance(
            self.actual_initial, LabourObservationV1
        ) or not isinstance(self.previous_as_known, LabourObservationV1):
            raise TypeError("triplet actuals must use labour observations")
        if not isinstance(self.expectation, LabourExpectationV1):
            raise TypeError("triplet expectation must use the v1 contract")
        actual = self.actual_initial
        previous = self.previous_as_known
        if actual.logical_event_key != target:
            raise ValueError("initial actual belongs to another event")
        if actual.revision_sequence != 0:
            raise ValueError(
                "triplet actual must be the first published vintage"
            )
        if actual.concept.concept_id != previous.concept.concept_id:
            raise ValueError("previous field silently changes labour concept")
        lag = _month_index(actual.reference_period_end_ns) - _month_index(
            previous.reference_period_end_ns
        )
        expected_lag = {
            LabourFrequency.MONTHLY: 1,
            LabourFrequency.QUARTERLY: 3,
            LabourFrequency.ANNUAL: 12,
        }.get(actual.concept.frequency)
        if expected_lag is None:
            if (
                actual.reference_period_end_ns
                - previous.reference_period_end_ns
                != _WEEK_NS
            ):
                raise ValueError(
                    "weekly previous field is not the immediately preceding "
                    "week"
                )
        elif lag != expected_lag:
            raise ValueError(
                "previous field is not the immediately preceding period"
            )
        cutoff = _bounded_ns(
            self.previous_as_known_at_ns, "previous_as_known_at_ns"
        )
        if previous.available_at_ns > cutoff or cutoff > actual.published_at_ns:
            raise ValueError("previous-as-known evidence is not pre-release")
        if (
            previous.revision_kind is LabourRevisionKind.SIMULTANEOUS_PREVIOUS
            and previous.available_at_ns != actual.published_at_ns
        ):
            raise ValueError(
                "simultaneous previous revision has wrong timestamp"
            )
        expectation = self.expectation
        if (
            expectation.target_event_key != target
            or expectation.concept.concept_id != actual.concept.concept_id
            or expectation.reference_period != actual.reference_period
        ):
            raise ValueError(
                "forecast does not match the exact released concept"
            )
        if expectation.kind is not LabourExpectationKind.UNAVAILABLE:
            if not expectation.calendar_style_eligible:
                raise ValueError(
                    "period projection cannot populate release forecast"
                )
            if (
                expectation.available_at_ns is None
                or expectation.available_at_ns >= actual.published_at_ns
            ):
                raise ValueError("forecast was not available before release")
        object.__setattr__(self, "target_event_key", target)
        object.__setattr__(self, "previous_as_known_at_ns", cutoff)
        expected = _stable_id("labour-release-triplet", self.identity_payload())
        supplied = _optional_text(self.triplet_id)
        if supplied is not None and supplied != expected:
            raise ValueError("triplet_id differs from deterministic identity")
        object.__setattr__(self, "triplet_id", expected)

    @property
    def actual(self) -> float:
        return self.actual_initial.value

    @property
    def previous(self) -> float:
        return self.previous_as_known.value

    @property
    def forecast(self) -> float | None:
        return self.expectation.expected_value

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "target_event_key": self.target_event_key,
            "actual_initial": self.actual_initial.to_dict(),
            "previous_as_known": self.previous_as_known.to_dict(),
            "previous_as_known_at_ns": self.previous_as_known_at_ns,
            "expectation": self.expectation.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "triplet_id": self.triplet_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourReleaseTripletV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            actual_initial=LabourObservationV1.from_dict(
                _mapping(data.get("actual_initial"), "actual_initial")
            ),
            previous_as_known=LabourObservationV1.from_dict(
                _mapping(data.get("previous_as_known"), "previous_as_known")
            ),
            previous_as_known_at_ns=cast(
                int, data.get("previous_as_known_at_ns")
            ),
            expectation=LabourExpectationV1.from_dict(
                _mapping(data.get("expectation"), "expectation")
            ),
            triplet_id=str(data.get("triplet_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourReleaseV1:
    """One independently surprising indicator inside a release package."""

    package_key: str
    logical_event_key: str
    economy_code: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    observations: tuple[LabourObservationV1, ...]
    display_observation_id: str
    limitations: tuple[str, ...]
    release_id: str = ""
    schema_version: str = LABOUR_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported labour release schema")
        package_key = _key(self.package_key, "package_key")
        event_key = _key(self.logical_event_key, "logical_event_key")
        economy = _economy(self.economy_code)
        period = _required_text(self.reference_period, "reference_period")
        period_end = _bounded_ns(
            self.reference_period_end_ns, "reference_period_end_ns"
        )
        stage = EconomicReleaseStage.from_value(self.stage)
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("release availability precedes publication")
        source_key = _key(self.source_key, "source_key")
        source_release = _key(self.source_release_id, "source_release_id")
        source_request = _key(self.source_request_id, "source_request_id")
        digest = _sha256(self.content_sha256, "content_sha256")
        observations = tuple(
            sorted(self.observations, key=lambda item: item.observation_id)
        )
        if (
            not observations
            or len(observations) > MAX_LABOUR_RELEASE_OBSERVATIONS
        ):
            raise ValueError("release observation count is outside bounds")
        if any(
            not isinstance(item, LabourObservationV1) for item in observations
        ):
            raise TypeError("release contains a non-v1 observation")
        if len({item.concept.concept_id for item in observations}) != len(
            observations
        ):
            raise ValueError("release repeats a labour concept")
        for observation in observations:
            if (
                observation.logical_event_key,
                observation.concept.economy_code,
                observation.reference_period,
                observation.reference_period_end_ns,
                observation.stage,
                observation.published_at_ns,
                observation.available_at_ns,
                observation.source_key,
                observation.source_release_id,
                observation.source_request_id,
                observation.content_sha256,
            ) != (
                event_key,
                economy,
                period,
                period_end,
                stage,
                published,
                available,
                source_key,
                source_release,
                source_request,
                digest,
            ):
                raise ValueError(
                    "release and observation evidence do not reconcile"
                )
        display = _required_text(
            self.display_observation_id, "display_observation_id"
        )
        if display not in {item.observation_id for item in observations}:
            raise ValueError("display observation is not retained in release")
        object.__setattr__(self, "package_key", package_key)
        object.__setattr__(self, "logical_event_key", event_key)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "reference_period_end_ns", period_end)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_release_id", source_release)
        object.__setattr__(self, "source_request_id", source_request)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "display_observation_id", display)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("labour-release", self.identity_payload())
        supplied = _optional_text(self.release_id)
        if supplied is not None and supplied != expected:
            raise ValueError("release_id differs from deterministic identity")
        object.__setattr__(self, "release_id", expected)

    @property
    def display_observation(self) -> LabourObservationV1:
        return next(
            item
            for item in self.observations
            if item.observation_id == self.display_observation_id
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_key": self.package_key,
            "logical_event_key": self.logical_event_key,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "stage": self.stage.value,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "observations": [item.to_dict() for item in self.observations],
            "display_observation_id": self.display_observation_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourReleaseV1:
        return cls(
            package_key=str(data.get("package_key", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            observations=tuple(
                LabourObservationV1.from_dict(_mapping(item, "observation"))
                for item in _sequence(data.get("observations"), "observations")
            ),
            display_observation_id=str(data.get("display_observation_id", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourReleasePackageV1:
    """Shared publication evidence for distinct labour indicators."""

    package_key: str
    economy_code: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    releases: tuple[LabourReleaseV1, ...]
    limitations: tuple[str, ...]
    package_id: str = ""
    schema_version: str = LABOUR_RELEASE_PACKAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_RELEASE_PACKAGE_SCHEMA_VERSION:
            raise ValueError("unsupported labour release-package schema")
        package_key = _key(self.package_key, "package_key")
        economy = _economy(self.economy_code)
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("package availability precedes publication")
        source_key = _key(self.source_key, "source_key")
        source_release = _key(self.source_release_id, "source_release_id")
        source_request = _key(self.source_request_id, "source_request_id")
        digest = _sha256(self.content_sha256, "content_sha256")
        releases = tuple(
            sorted(self.releases, key=lambda item: item.release_id)
        )
        if not releases or len(releases) > MAX_LABOUR_PACKAGE_RELEASES:
            raise ValueError("package release count is outside bounds")
        if any(not isinstance(item, LabourReleaseV1) for item in releases):
            raise TypeError("package contains a non-v1 release")
        if len({item.release_id for item in releases}) != len(releases):
            raise ValueError("package repeats a release")
        if len({item.logical_event_key for item in releases}) != len(releases):
            raise ValueError("package collapses distinct indicators")
        display_concepts = [
            item.display_observation.concept for item in releases
        ]
        if len({item.concept_id for item in display_concepts}) != len(
            display_concepts
        ):
            raise ValueError("package repeats a displayed labour concept")
        for release in releases:
            if (
                release.package_key,
                release.economy_code,
                release.published_at_ns,
                release.available_at_ns,
                release.source_key,
                release.source_release_id,
                release.source_request_id,
                release.content_sha256,
            ) != (
                package_key,
                economy,
                published,
                available,
                source_key,
                source_release,
                source_request,
                digest,
            ):
                raise ValueError("release does not share package evidence")
        object.__setattr__(self, "package_key", package_key)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_release_id", source_release)
        object.__setattr__(self, "source_request_id", source_request)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("labour-release-package", self.identity_payload())
        supplied = _optional_text(self.package_id)
        if supplied is not None and supplied != expected:
            raise ValueError("package_id differs from deterministic identity")
        object.__setattr__(self, "package_id", expected)

    def release_for_measure(self, measure: LabourMeasure) -> LabourReleaseV1:
        selected = LabourMeasure.from_value(measure)
        matches = [
            item
            for item in self.releases
            if item.display_observation.concept.measure is selected
        ]
        if len(matches) != 1:
            raise ValueError("package has no unique release for labour measure")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_key": self.package_key,
            "economy_code": self.economy_code,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "releases": [item.to_dict() for item in self.releases],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "package_id": self.package_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourReleasePackageV1:
        return cls(
            package_key=str(data.get("package_key", "")),
            economy_code=str(data.get("economy_code", "")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            releases=tuple(
                LabourReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            package_id=str(data.get("package_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourMethodologyEventV1:
    """Benchmark, population, seasonal, coverage, or survey change evidence."""

    economy_code: str
    change_kind: LabourMethodologyChangeKind
    affected_concept_ids: tuple[str, ...]
    effective_reference_period: str
    effective_from_ns: int
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_request_id: str
    content_sha256: str
    prior_methodology_era_id: str
    new_methodology_era_id: str
    benchmark_reference_period: str | None
    prior_population_control_id: str | None
    new_population_control_id: str | None
    comparability_bridge_id: str | None
    comparable_across_change: bool
    limitations: tuple[str, ...]
    event_id: str = ""
    schema_version: str = LABOUR_METHODOLOGY_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_METHODOLOGY_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported labour methodology-event schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        kind = LabourMethodologyChangeKind.from_value(self.change_kind)
        concepts = _texts(
            self.affected_concept_ids, "affected_concept_id", required=True
        )
        object.__setattr__(self, "change_kind", kind)
        object.__setattr__(self, "affected_concept_ids", concepts)
        object.__setattr__(
            self,
            "effective_reference_period",
            _required_text(
                self.effective_reference_period, "effective_reference_period"
            ),
        )
        effective = _bounded_ns(self.effective_from_ns, "effective_from_ns")
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("methodology availability precedes publication")
        object.__setattr__(self, "effective_from_ns", effective)
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "source_request_id",
            _key(self.source_request_id, "source_request_id"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        prior_era = _key(
            self.prior_methodology_era_id, "prior_methodology_era_id"
        )
        new_era = _key(self.new_methodology_era_id, "new_methodology_era_id")
        if prior_era == new_era:
            raise ValueError("methodology event requires distinct eras")
        benchmark = _optional_text(self.benchmark_reference_period)
        prior_population = _optional_key(
            self.prior_population_control_id, "prior_population_control_id"
        )
        new_population = _optional_key(
            self.new_population_control_id, "new_population_control_id"
        )
        bridge = _optional_key(
            self.comparability_bridge_id, "comparability_bridge_id"
        )
        if not isinstance(self.comparable_across_change, bool):
            raise TypeError("comparable_across_change must be boolean")
        if (
            kind is LabourMethodologyChangeKind.BENCHMARK_REVISION
            and benchmark is None
        ):
            raise ValueError("benchmark revision requires a reference period")
        if kind is LabourMethodologyChangeKind.POPULATION_CONTROL and (
            prior_population is None
            or new_population is None
            or prior_population == new_population
        ):
            raise ValueError(
                "population-control change requires old and new controls"
            )
        if self.comparable_across_change and bridge is None:
            raise ValueError("comparability claim requires a bridge identifier")
        object.__setattr__(self, "prior_methodology_era_id", prior_era)
        object.__setattr__(self, "new_methodology_era_id", new_era)
        object.__setattr__(self, "benchmark_reference_period", benchmark)
        object.__setattr__(
            self, "prior_population_control_id", prior_population
        )
        object.__setattr__(self, "new_population_control_id", new_population)
        object.__setattr__(self, "comparability_bridge_id", bridge)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "labour-methodology-event", self.identity_payload()
        )
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id differs from deterministic identity")
        object.__setattr__(self, "event_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "change_kind": self.change_kind.value,
            "affected_concept_ids": list(self.affected_concept_ids),
            "effective_reference_period": self.effective_reference_period,
            "effective_from_ns": self.effective_from_ns,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "prior_methodology_era_id": self.prior_methodology_era_id,
            "new_methodology_era_id": self.new_methodology_era_id,
            "benchmark_reference_period": self.benchmark_reference_period,
            "prior_population_control_id": self.prior_population_control_id,
            "new_population_control_id": self.new_population_control_id,
            "comparability_bridge_id": self.comparability_bridge_id,
            "comparable_across_change": self.comparable_across_change,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "event_id": self.event_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourMethodologyEventV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            change_kind=LabourMethodologyChangeKind.from_value(
                str(data.get("change_kind", ""))
            ),
            affected_concept_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("affected_concept_ids"), "affected_concept_ids"
                )
            ),
            effective_reference_period=str(
                data.get("effective_reference_period", "")
            ),
            effective_from_ns=cast(int, data.get("effective_from_ns")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            prior_methodology_era_id=str(
                data.get("prior_methodology_era_id", "")
            ),
            new_methodology_era_id=str(data.get("new_methodology_era_id", "")),
            benchmark_reference_period=_optional_text(
                data.get("benchmark_reference_period")
            ),
            prior_population_control_id=_optional_text(
                data.get("prior_population_control_id")
            ),
            new_population_control_id=_optional_text(
                data.get("new_population_control_id")
            ),
            comparability_bridge_id=_optional_text(
                data.get("comparability_bridge_id")
            ),
            comparable_across_change=cast(
                bool, data.get("comparable_across_change")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class LabourEconomyProfileV1:
    """Reviewed legal producer and difficult-case semantics for one economy."""

    economy_code: str
    institution: str
    source_key: str
    required_survey_bases: tuple[LabourSurveyBasis, ...]
    required_measures: tuple[LabourMeasure, ...]
    supports_release_packages: bool
    supports_simultaneous_previous_revisions: bool
    supports_benchmark_revisions: bool
    special_cases: tuple[str, ...]
    rationale: str
    profile_id: str = ""
    schema_version: str = LABOUR_ECONOMY_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_ECONOMY_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported labour economy-profile schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self, "institution", _required_text(self.institution, "institution")
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        surveys = tuple(
            sorted(
                {
                    LabourSurveyBasis.from_value(item)
                    for item in self.required_survey_bases
                },
                key=lambda item: item.value,
            )
        )
        measures = tuple(
            sorted(
                {
                    LabourMeasure.from_value(item)
                    for item in self.required_measures
                },
                key=lambda item: item.value,
            )
        )
        if not surveys or not {
            LabourMeasure.EMPLOYMENT_LEVEL,
            LabourMeasure.UNEMPLOYMENT_RATE,
        }.issubset(measures):
            raise ValueError("profile omits core labour semantics")
        for name in (
            "supports_release_packages",
            "supports_simultaneous_previous_revisions",
            "supports_benchmark_revisions",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if not all(
            (
                self.supports_release_packages,
                self.supports_simultaneous_previous_revisions,
                self.supports_benchmark_revisions,
            )
        ):
            raise ValueError("profile must support required labour edge cases")
        cases = _keys(self.special_cases, "special_case", required=True)
        required_cases = {
            "benchmark-revisions",
            "published-change-and-level-vintages",
            "release-package-identity",
            "simultaneous-previous-revisions",
            "survey-source-distinction",
        }
        if not required_cases.issubset(cases):
            raise ValueError("profile omits universal labour edge cases")
        object.__setattr__(self, "required_survey_bases", surveys)
        object.__setattr__(self, "required_measures", measures)
        object.__setattr__(self, "special_cases", cases)
        object.__setattr__(
            self, "rationale", _required_text(self.rationale, "rationale")
        )
        expected = _stable_id("labour-economy-profile", self.identity_payload())
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
            "required_survey_bases": [
                item.value for item in self.required_survey_bases
            ],
            "required_measures": [
                item.value for item in self.required_measures
            ],
            "supports_release_packages": self.supports_release_packages,
            "supports_simultaneous_previous_revisions": (
                self.supports_simultaneous_previous_revisions
            ),
            "supports_benchmark_revisions": self.supports_benchmark_revisions,
            "special_cases": list(self.special_cases),
            "rationale": self.rationale,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourEconomyProfileV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            institution=str(data.get("institution", "")),
            source_key=str(data.get("source_key", "")),
            required_survey_bases=tuple(
                LabourSurveyBasis.from_value(str(item))
                for item in _sequence(
                    data.get("required_survey_bases"), "required_survey_bases"
                )
            ),
            required_measures=tuple(
                LabourMeasure.from_value(str(item))
                for item in _sequence(
                    data.get("required_measures"), "required_measures"
                )
            ),
            supports_release_packages=cast(
                bool, data.get("supports_release_packages")
            ),
            supports_simultaneous_previous_revisions=cast(
                bool, data.get("supports_simultaneous_previous_revisions")
            ),
            supports_benchmark_revisions=cast(
                bool, data.get("supports_benchmark_revisions")
            ),
            special_cases=tuple(
                str(item)
                for item in _sequence(
                    data.get("special_cases"), "special_cases"
                )
            ),
            rationale=str(data.get("rationale", "")),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_UNIVERSAL_SURVEYS: tuple[LabourSurveyBasis, ...] = (
    LabourSurveyBasis.HOUSEHOLD_SURVEY,
    LabourSurveyBasis.ESTABLISHMENT_SURVEY,
    LabourSurveyBasis.ADMINISTRATIVE_REGISTER,
)
_UNIVERSAL_MEASURES: tuple[LabourMeasure, ...] = (
    LabourMeasure.EMPLOYMENT_LEVEL,
    LabourMeasure.EMPLOYMENT_CHANGE,
    LabourMeasure.PAYROLL_CHANGE,
    LabourMeasure.UNEMPLOYMENT_RATE,
    LabourMeasure.PARTICIPATION_RATE,
    LabourMeasure.EMPLOYMENT_POPULATION_RATIO,
    LabourMeasure.EARNINGS_CHANGE,
    LabourMeasure.HOURS,
    LabourMeasure.VACANCIES,
    LabourMeasure.CLAIMANT_COUNT,
)
_UNIVERSAL_SPECIAL_CASES: tuple[str, ...] = (
    "benchmark-revisions",
    "methodology-and-sampling-breaks",
    "published-change-and-level-vintages",
    "release-package-identity",
    "seasonal-reanalysis",
    "simultaneous-previous-revisions",
    "survey-source-distinction",
)
_SPECIAL_CASES_BY_ECONOMY: Mapping[str, tuple[str, ...]] = {
    "EA": ("harmonized-unemployment-versus-national-series",),
    "GB": ("labour-force-survey-claimant-and-payrolled-employees",),
    "JP": ("jobs-to-applicants-and-household-survey",),
    "US": (
        "establishment-versus-household-employment",
        "jolts-vacancies-hires-separations",
        "nonfarm-payroll-benchmark-revisions",
        "weekly-benefit-claims",
    ),
}


def built_in_labour_profiles(
    registry: OfficialSourceRegistryV1,
) -> tuple[LabourEconomyProfileV1, ...]:
    """Build semantic/source profiles for every scoped economy."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    profiles: list[LabourEconomyProfileV1] = []
    for economy_code in registry.scoped_economies:
        source = registry.primary_source(
            economy_code, EconomicEventFamily.LABOUR_MARKET
        )
        surveys = _UNIVERSAL_SURVEYS
        measures = _UNIVERSAL_MEASURES
        if economy_code == "US":
            surveys = (
                *surveys,
                LabourSurveyBasis.VACANCY_SURVEY,
                LabourSurveyBasis.BENEFIT_CLAIMS_ADMINISTRATIVE,
            )
            measures = (
                *measures,
                LabourMeasure.HIRES,
                LabourMeasure.SEPARATIONS,
                LabourMeasure.INITIAL_BENEFIT_CLAIMS,
                LabourMeasure.CONTINUING_BENEFIT_CLAIMS,
            )
        profiles.append(
            LabourEconomyProfileV1(
                economy_code=economy_code,
                institution=source.institution,
                source_key=source.source_key,
                required_survey_bases=surveys,
                required_measures=measures,
                supports_release_packages=True,
                supports_simultaneous_previous_revisions=True,
                supports_benchmark_revisions=True,
                special_cases=(
                    *_UNIVERSAL_SPECIAL_CASES,
                    *_SPECIAL_CASES_BY_ECONOMY.get(economy_code, ()),
                ),
                rationale=(
                    "Retain exact survey, source, population, measure, "
                    "transformation, release-package, and revision identity. "
                    "Unsupported series remain adapter coverage gaps."
                ),
            )
        )
    return tuple(sorted(profiles, key=lambda item: item.economy_code))


@dataclass(frozen=True, slots=True)
class LabourProfileAuditV1:
    """Coverage proof for labour profiles and official source ownership."""

    registry_id: str
    profile_ids: tuple[str, ...]
    missing_economies: tuple[str, ...]
    duplicate_economies: tuple[str, ...]
    source_mismatches: tuple[str, ...]
    semantic_mismatches: tuple[str, ...]
    complete: bool
    audit_id: str = ""
    schema_version: str = LABOUR_PROFILE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LABOUR_PROFILE_AUDIT_SCHEMA_VERSION:
            raise ValueError("unsupported labour profile-audit schema")
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
            "semantic_mismatches",
        ):
            object.__setattr__(self, name, _texts(getattr(self, name), name))
        expected_complete = not any(
            (
                self.missing_economies,
                self.duplicate_economies,
                self.source_mismatches,
                self.semantic_mismatches,
            )
        )
        if (
            not isinstance(self.complete, bool)
            or self.complete != expected_complete
        ):
            raise ValueError(
                "labour profile audit completeness is inconsistent"
            )
        expected = _stable_id("labour-profile-audit", self.identity_payload())
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
            "semantic_mismatches": list(self.semantic_mismatches),
            "complete": self.complete,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LabourProfileAuditV1:
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
            semantic_mismatches=tuple(
                str(item)
                for item in _sequence(
                    data.get("semantic_mismatches"), "semantic_mismatches"
                )
            ),
            complete=cast(bool, data.get("complete")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def audit_labour_profiles(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[LabourEconomyProfileV1],
) -> LabourProfileAuditV1:
    """Audit exact profile/source/semantic coverage for registry scope."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    values = tuple(profiles)
    if any(not isinstance(item, LabourEconomyProfileV1) for item in values):
        raise TypeError("profiles must use LabourEconomyProfileV1")
    counts = {
        code: sum(item.economy_code == code for item in values)
        for code in registry.scoped_economies
    }
    missing = tuple(code for code, count in counts.items() if count == 0)
    duplicates = tuple(code for code, count in counts.items() if count > 1)
    source_mismatches: list[str] = []
    semantic_mismatches: list[str] = []
    for profile in values:
        code = profile.economy_code
        if code not in registry.scoped_economies:
            source_mismatches.append(f"{code}:outside-scope")
            continue
        source = registry.primary_source(
            code, EconomicEventFamily.LABOUR_MARKET
        )
        if (profile.source_key, profile.institution) != (
            source.source_key,
            source.institution,
        ):
            source_mismatches.append(code)
        expected_cases = set(_UNIVERSAL_SPECIAL_CASES) | set(
            _SPECIAL_CASES_BY_ECONOMY.get(code, ())
        )
        if (
            not set(_UNIVERSAL_SURVEYS).issubset(profile.required_survey_bases)
            or not set(_UNIVERSAL_MEASURES).issubset(profile.required_measures)
            or not expected_cases.issubset(profile.special_cases)
            or not profile.supports_release_packages
            or not profile.supports_simultaneous_previous_revisions
            or not profile.supports_benchmark_revisions
        ):
            semantic_mismatches.append(code)
        if code == "US" and (
            LabourSurveyBasis.VACANCY_SURVEY
            not in profile.required_survey_bases
            or LabourSurveyBasis.BENEFIT_CLAIMS_ADMINISTRATIVE
            not in profile.required_survey_bases
            or LabourMeasure.HIRES not in profile.required_measures
            or LabourMeasure.SEPARATIONS not in profile.required_measures
            or LabourMeasure.INITIAL_BENEFIT_CLAIMS
            not in profile.required_measures
        ):
            semantic_mismatches.append("US:multi-source")
    if len({item.profile_id for item in values}) != len(values):
        duplicates = tuple(sorted({*duplicates, "profile-id"}))
    source_result = tuple(sorted(set(source_mismatches)))
    semantic_result = tuple(sorted(set(semantic_mismatches)))
    return LabourProfileAuditV1(
        registry_id=registry.registry_id,
        profile_ids=tuple(item.profile_id for item in values),
        missing_economies=missing,
        duplicate_economies=duplicates,
        source_mismatches=source_result,
        semantic_mismatches=semantic_result,
        complete=not any((missing, duplicates, source_result, semantic_result)),
    )


def require_labour_profile_coverage(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[LabourEconomyProfileV1],
) -> LabourProfileAuditV1:
    """Return a complete audit or fail closed on a source/semantic gap."""
    audit = audit_labour_profiles(registry, profiles)
    if not audit.complete:
        raise ValueError("labour profile coverage is incomplete")
    return audit


__all__ = [
    "LABOUR_CHANGE_DERIVATION_SCHEMA_VERSION",
    "LABOUR_CONCEPT_SCHEMA_VERSION",
    "LABOUR_ECONOMY_PROFILE_SCHEMA_VERSION",
    "LABOUR_EXPECTATION_SCHEMA_VERSION",
    "LABOUR_METHODOLOGY_EVENT_SCHEMA_VERSION",
    "LABOUR_OBSERVATION_SCHEMA_VERSION",
    "LABOUR_PROFILE_AUDIT_SCHEMA_VERSION",
    "LABOUR_RELEASE_PACKAGE_SCHEMA_VERSION",
    "LABOUR_RELEASE_SCHEMA_VERSION",
    "LABOUR_RELEASE_TRIPLET_SCHEMA_VERSION",
    "LABOUR_VINTAGE_CHAIN_SCHEMA_VERSION",
    "MAX_LABOUR_PACKAGE_RELEASES",
    "MAX_LABOUR_RELEASE_OBSERVATIONS",
    "MAX_LABOUR_REVISIONS",
    "LabourChangeDerivationV1",
    "LabourConceptV1",
    "LabourDerivationVintageBasis",
    "LabourEconomyProfileV1",
    "LabourExpectationKind",
    "LabourExpectationV1",
    "LabourFrequency",
    "LabourMeasure",
    "LabourMethodologyChangeKind",
    "LabourMethodologyEventV1",
    "LabourObservationBasis",
    "LabourObservationV1",
    "LabourProfileAuditV1",
    "LabourReleasePackageV1",
    "LabourReleaseTripletV1",
    "LabourReleaseV1",
    "LabourRevisionKind",
    "LabourSeasonalBasis",
    "LabourSurveyBasis",
    "LabourTransformation",
    "LabourVintageChainV1",
    "audit_labour_profiles",
    "built_in_labour_profiles",
    "require_labour_profile_coverage",
]
