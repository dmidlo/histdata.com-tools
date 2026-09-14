"""Provider-neutral GDP and national-accounts release semantics.

The contracts preserve output basis, valuation, transformation, frequency,
release stage, component framework, and vintage identity.  A first value at an
advance, preliminary, second, or final stage is an immutable stage actual;
later releases and benchmark revisions extend the graph rather than rewriting
that historical snapshot.
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
from histdatacom.market_context.official_sources import (
    OfficialSourceRegistryV1,
    OfficialSourceRole,
)
from histdatacom.runtime_contracts import JSONValue

NATIONAL_ACCOUNTS_CONCEPT_SCHEMA_VERSION = (
    "histdatacom.national-accounts-concept.v1"
)
NATIONAL_ACCOUNTS_OBSERVATION_SCHEMA_VERSION = (
    "histdatacom.national-accounts-observation.v1"
)
NATIONAL_ACCOUNTS_VINTAGE_CHAIN_SCHEMA_VERSION = (
    "histdatacom.national-accounts-vintage-chain.v1"
)
NATIONAL_ACCOUNTS_GROWTH_DERIVATION_SCHEMA_VERSION = (
    "histdatacom.national-accounts-growth-derivation.v1"
)
NATIONAL_ACCOUNTS_EXPECTATION_SCHEMA_VERSION = (
    "histdatacom.national-accounts-expectation.v1"
)
NATIONAL_ACCOUNTS_RELEASE_TRIPLET_SCHEMA_VERSION = (
    "histdatacom.national-accounts-release-triplet.v1"
)
NATIONAL_ACCOUNTS_COMPONENT_ENTRY_SCHEMA_VERSION = (
    "histdatacom.national-accounts-component-entry.v1"
)
NATIONAL_ACCOUNTS_COMPONENT_RECONCILIATION_SCHEMA_VERSION = (
    "histdatacom.national-accounts-component-reconciliation.v1"
)
NATIONAL_ACCOUNTS_RELEASE_SCHEMA_VERSION = (
    "histdatacom.national-accounts-release.v1"
)
NATIONAL_ACCOUNTS_RELEASE_SEQUENCE_SCHEMA_VERSION = (
    "histdatacom.national-accounts-release-sequence.v1"
)
NATIONAL_ACCOUNTS_BENCHMARK_REVISION_SCHEMA_VERSION = (
    "histdatacom.national-accounts-benchmark-revision.v1"
)
NATIONAL_ACCOUNTS_METHODOLOGY_EVENT_SCHEMA_VERSION = (
    "histdatacom.national-accounts-methodology-event.v1"
)
NATIONAL_ACCOUNTS_ECONOMY_PROFILE_SCHEMA_VERSION = (
    "histdatacom.national-accounts-economy-profile.v1"
)
NATIONAL_ACCOUNTS_PROFILE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.national-accounts-profile-audit.v1"
)

MAX_NATIONAL_ACCOUNTS_REVISIONS = 10_000
MAX_NATIONAL_ACCOUNTS_COMPONENTS = 2_000
MAX_NATIONAL_ACCOUNTS_RELEASE_OBSERVATIONS = 10_000
MAX_NATIONAL_ACCOUNTS_RELEASE_STAGES = 16
MAX_NATIONAL_ACCOUNTS_BENCHMARK_OBSERVATIONS = 100_000

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class NationalAccountsMeasure(str, Enum):
    """Aggregate or component represented by a national-accounts concept."""

    GDP = "gross-domestic-product"
    GROSS_NATIONAL_INCOME = "gross-national-income"
    GROSS_VALUE_ADDED = "gross-value-added"
    EXPENDITURE_COMPONENT = "expenditure-component"
    INDUSTRY_COMPONENT = "industry-component"
    INCOME_COMPONENT = "income-component"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsMeasure
    ) -> NationalAccountsMeasure:
        return _enum_value(cls, value, "national-accounts measure")

    @property
    def is_component(self) -> bool:
        return self in {
            NationalAccountsMeasure.EXPENDITURE_COMPONENT,
            NationalAccountsMeasure.INDUSTRY_COMPONENT,
            NationalAccountsMeasure.INCOME_COMPONENT,
        }


class NationalAccountsComponentFramework(str, Enum):
    """Accounting presentation used by a component table."""

    EXPENDITURE = "expenditure"
    INDUSTRY = "industry"
    INCOME = "income"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsComponentFramework
    ) -> NationalAccountsComponentFramework:
        return _enum_value(cls, value, "component framework")


class NationalAccountsOutputBasis(str, Enum):
    """Price/volume basis of the published output measure."""

    REAL_CHAIN_VOLUME = "real-chain-volume"
    REAL_FIXED_PRICE = "real-fixed-price"
    NOMINAL_CURRENT_PRICE = "nominal-current-price"
    VOLUME_INDEX = "volume-index"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsOutputBasis
    ) -> NationalAccountsOutputBasis:
        return _enum_value(cls, value, "national-accounts output basis")

    @property
    def is_real(self) -> bool:
        return self is not NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE


class NationalAccountsTransformation(str, Enum):
    """Exact published or derived representation of output."""

    LEVEL = "level"
    ANNUALIZED_LEVEL = "annualized-level"
    INDEX_LEVEL = "index-level"
    ABSOLUTE_CHANGE = "absolute-change"
    MONTH_OVER_MONTH_PERCENT = "month-over-month-percent"
    QUARTER_OVER_QUARTER_PERCENT = "quarter-over-quarter-percent"
    ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT = (
        "annualized-quarter-over-quarter-percent"
    )
    YEAR_OVER_YEAR_PERCENT = "year-over-year-percent"
    CONTRIBUTION_PERCENTAGE_POINTS = "contribution-percentage-points"
    SHARE_PERCENT = "share-percent"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsTransformation
    ) -> NationalAccountsTransformation:
        return _enum_value(cls, value, "national-accounts transformation")

    @property
    def derivable_from_levels(self) -> bool:
        return self in {
            NationalAccountsTransformation.ABSOLUTE_CHANGE,
            NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT,
            NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
            _ANNUALIZED_QOQ,
            NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT,
        }


class NationalAccountsFrequency(str, Enum):
    """Reference-period frequency of the source series."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsFrequency
    ) -> NationalAccountsFrequency:
        return _enum_value(cls, value, "national-accounts frequency")


class NationalAccountsSeasonalBasis(str, Enum):
    """Seasonal and annual-rate treatment of a concept."""

    NOT_SEASONALLY_ADJUSTED = "not-seasonally-adjusted"
    SEASONALLY_ADJUSTED = "seasonally-adjusted"
    SEASONALLY_AND_CALENDAR_ADJUSTED = "seasonally-and-calendar-adjusted"
    CALENDAR_ADJUSTED = "calendar-adjusted"
    SEASONALLY_ADJUSTED_ANNUAL_RATE = "seasonally-adjusted-annual-rate"
    TREND_CYCLE = "trend-cycle"
    NOT_APPLICABLE = "not-applicable"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsSeasonalBasis
    ) -> NationalAccountsSeasonalBasis:
        return _enum_value(cls, value, "national-accounts seasonal basis")


_ANNUALIZED_QOQ = (
    NationalAccountsTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
)
_CONTRIBUTION_PP = NationalAccountsTransformation.CONTRIBUTION_PERCENTAGE_POINTS
_SAAR_BASIS = NationalAccountsSeasonalBasis.SEASONALLY_ADJUSTED_ANNUAL_RATE


class NationalAccountsObservationBasis(str, Enum):
    """Whether a value was source-published or derived from level vintages."""

    SOURCE_PUBLISHED = "source-published"
    DERIVED_FROM_LEVEL_VINTAGES = "derived-from-level-vintages"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsObservationBasis
    ) -> NationalAccountsObservationBasis:
        return _enum_value(cls, value, "national-accounts observation basis")


class NationalAccountsDerivationVintageBasis(str, Enum):
    """Vintage policy used by a level-to-growth derivation."""

    INITIAL_AT_STAGE = "initial-at-stage"
    AS_KNOWN_AT_RELEASE = "as-known-at-release"
    LATEST_CURRENT = "latest-current"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsDerivationVintageBasis
    ) -> NationalAccountsDerivationVintageBasis:
        return _enum_value(cls, value, "national-accounts vintage basis")


class NationalAccountsRevisionKind(str, Enum):
    """Reason a value follows the first published value at one stage."""

    STAGE_INITIAL = "stage-initial"
    ROUTINE = "routine-revision"
    SIMULTANEOUS_PREVIOUS = "simultaneous-previous-revision"
    BENCHMARK = "benchmark-revision"
    METHODOLOGY = "methodology-revision"
    SEASONAL = "seasonal-reanalysis"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsRevisionKind
    ) -> NationalAccountsRevisionKind:
        return _enum_value(cls, value, "national-accounts revision kind")


class NationalAccountsExpectationKind(str, Enum):
    """Provenance and target scope of an output expectation."""

    EVENT_CONSENSUS = "event-consensus"
    OFFICIAL_SURVEY_EVENT_TARGET = "official-survey-event-target"
    OFFICIAL_SURVEY_PERIOD_TARGET = "official-survey-period-target"
    CENTRAL_BANK_PROJECTION = "central-bank-projection"
    GOVERNMENT_PROJECTION = "government-projection"
    MACHINE_EVENT_TARGET = "machine-event-target"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsExpectationKind
    ) -> NationalAccountsExpectationKind:
        return _enum_value(cls, value, "national-accounts expectation kind")

    @property
    def calendar_style_eligible(self) -> bool:
        return self in {
            NationalAccountsExpectationKind.EVENT_CONSENSUS,
            NationalAccountsExpectationKind.OFFICIAL_SURVEY_EVENT_TARGET,
            NationalAccountsExpectationKind.MACHINE_EVENT_TARGET,
        }


class NationalAccountsComponentRole(str, Enum):
    """Accounting sign carried by a component entry."""

    ADDITIVE = "additive"
    SUBTRACTIVE = "subtractive"
    STATISTICAL_DISCREPANCY = "statistical-discrepancy"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsComponentRole
    ) -> NationalAccountsComponentRole:
        return _enum_value(cls, value, "national-accounts component role")


class NationalAccountsReconciliationBasis(str, Enum):
    """Mathematical status of an official aggregate/component table."""

    ADDITIVE_LEVELS = "additive-levels"
    SOURCE_PUBLISHED_CONTRIBUTIONS = "source-published-contributions"
    CHAIN_VOLUME_NON_ADDITIVE = "chain-volume-non-additive"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsReconciliationBasis
    ) -> NationalAccountsReconciliationBasis:
        return _enum_value(cls, value, "component reconciliation basis")


class NationalAccountsComponentCoverage(str, Enum):
    """Completeness and additivity state of a component table."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    NON_ADDITIVE = "non-additive"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsComponentCoverage
    ) -> NationalAccountsComponentCoverage:
        return _enum_value(cls, value, "component coverage")


class NationalAccountsMethodologyChangeKind(str, Enum):
    """Change that may break comparison across national-account eras."""

    BENCHMARK_REVISION = "benchmark-revision"
    REBASE = "rebase"
    CHAIN_LINKING_CHANGE = "chain-linking-change"
    CLASSIFICATION_CHANGE = "classification-change"
    COVERAGE_CHANGE = "coverage-change"
    SEASONAL_REANALYSIS = "seasonal-reanalysis"
    VALUATION_CHANGE = "valuation-change"

    @classmethod
    def from_value(
        cls, value: str | NationalAccountsMethodologyChangeKind
    ) -> NationalAccountsMethodologyChangeKind:
        return _enum_value(cls, value, "national-accounts methodology kind")


def _enum_value(
    enum_type: type[_EnumT], value: str | _EnumT, label: str
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value))
    except ValueError as exc:
        raise ValueError(f"unsupported {label}") from exc


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be text")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty")
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("optional text must be text")
    normalized = value.strip()
    return normalized or None


def _key(value: object, label: str) -> str:
    normalized = _required_text(value, label)
    if _KEY_RE.fullmatch(normalized) is None:
        raise ValueError(f"{label} is not a canonical key")
    return normalized


def _optional_key(value: object, label: str) -> str | None:
    normalized = _optional_text(value)
    return None if normalized is None else _key(normalized, label)


def _economy(value: object) -> str:
    normalized = _required_text(value, "economy_code").upper()
    if _ECONOMY_RE.fullmatch(normalized) is None:
        raise ValueError("economy_code is invalid")
    return normalized


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
    if value < 0 or value > 9_223_372_036_854_775_807:
        raise ValueError(f"{label} is outside int64 nanoseconds")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value < 0:
        raise ValueError(f"{label} must be nonnegative")
    return value


def _sha256(value: object, label: str) -> str:
    normalized = _required_text(value, label).lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return normalized


def _https_uri(value: object, label: str) -> str:
    normalized = _required_text(value, label)
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{label} must be an absolute HTTPS URI")
    return normalized


def _texts(
    values: Iterable[object],
    label: str,
    *,
    required: bool = False,
    maximum: int = MAX_NATIONAL_ACCOUNTS_COMPONENTS,
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > maximum:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _keys(
    values: Iterable[object],
    label: str,
    *,
    required: bool = False,
    maximum: int = MAX_NATIONAL_ACCOUNTS_COMPONENTS,
) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > maximum:
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


_STAGE_ORDER: Mapping[EconomicReleaseStage, int] = {
    EconomicReleaseStage.FLASH: 0,
    EconomicReleaseStage.ADVANCE: 1,
    EconomicReleaseStage.INITIAL: 1,
    EconomicReleaseStage.PRELIMINARY: 2,
    EconomicReleaseStage.SECOND: 3,
    EconomicReleaseStage.FINAL: 4,
    EconomicReleaseStage.REVISION: 5,
}


@dataclass(frozen=True, slots=True)
class NationalAccountsConceptV1:
    """One exact national-accounts series and displayed transformation."""

    economy_code: str
    indicator_key: str
    source_series_id: str
    measure: NationalAccountsMeasure
    component_framework: NationalAccountsComponentFramework | None
    component_path: tuple[str, ...]
    output_basis: NationalAccountsOutputBasis
    transformation: NationalAccountsTransformation
    frequency: NationalAccountsFrequency
    seasonal_basis: NationalAccountsSeasonalBasis
    valuation_basis: str
    unit: str
    scale: float
    price_base_period: str | None
    chain_linking_method: str | None
    methodology_era_id: str
    concept_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_CONCEPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NATIONAL_ACCOUNTS_CONCEPT_SCHEMA_VERSION:
            raise ValueError("unsupported national-accounts concept schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        for name in ("indicator_key", "source_series_id", "methodology_era_id"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        measure = NationalAccountsMeasure.from_value(self.measure)
        framework = (
            None
            if self.component_framework is None
            else NationalAccountsComponentFramework.from_value(
                self.component_framework
            )
        )
        path = _keys(self.component_path, "component_path")
        if measure.is_component:
            if not path or framework is None:
                raise ValueError(
                    "component measure requires framework and component path"
                )
        elif path or framework is not None:
            raise ValueError(
                "aggregate measure cannot claim component identity"
            )
        output = NationalAccountsOutputBasis.from_value(self.output_basis)
        transformation = NationalAccountsTransformation.from_value(
            self.transformation
        )
        frequency = NationalAccountsFrequency.from_value(self.frequency)
        seasonal = NationalAccountsSeasonalBasis.from_value(self.seasonal_basis)
        if (
            transformation
            is NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT
            and frequency is not NationalAccountsFrequency.MONTHLY
        ):
            raise ValueError("monthly growth requires monthly frequency")
        if (
            transformation
            in {
                NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
                _ANNUALIZED_QOQ,
            }
            and frequency is not NationalAccountsFrequency.QUARTERLY
        ):
            raise ValueError("quarterly growth requires quarterly frequency")
        if (
            transformation is NationalAccountsTransformation.ANNUALIZED_LEVEL
            and (
                frequency is not NationalAccountsFrequency.QUARTERLY
                or seasonal is not _SAAR_BASIS
            )
        ):
            raise ValueError(
                "annualized level requires quarterly SAAR identity"
            )
        if (
            transformation
            in {
                _CONTRIBUTION_PP,
                NationalAccountsTransformation.SHARE_PERCENT,
            }
            and not measure.is_component
        ):
            raise ValueError(
                "component transformation requires component identity"
            )
        base = _optional_text(self.price_base_period)
        link = _optional_text(self.chain_linking_method)
        if (
            output
            in {
                NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
                NationalAccountsOutputBasis.REAL_FIXED_PRICE,
                NationalAccountsOutputBasis.VOLUME_INDEX,
            }
            and base is None
        ):
            raise ValueError(
                "real or volume output requires a price base period"
            )
        if (
            output is NationalAccountsOutputBasis.REAL_CHAIN_VOLUME
            and link is None
        ):
            raise ValueError("chain-volume output requires a linking method")
        if output is NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE and (
            base is not None or link is not None
        ):
            raise ValueError("nominal current-price output cannot claim a base")
        if (
            transformation is NationalAccountsTransformation.INDEX_LEVEL
            and base is None
        ):
            raise ValueError("index level requires a base period")
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "component_framework", framework)
        object.__setattr__(self, "component_path", path)
        object.__setattr__(self, "output_basis", output)
        object.__setattr__(self, "transformation", transformation)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "seasonal_basis", seasonal)
        object.__setattr__(
            self,
            "valuation_basis",
            _required_text(self.valuation_basis, "valuation_basis"),
        )
        object.__setattr__(self, "unit", _required_text(self.unit, "unit"))
        object.__setattr__(self, "scale", scale)
        object.__setattr__(self, "price_base_period", base)
        object.__setattr__(self, "chain_linking_method", link)
        expected = _stable_id(
            "national-accounts-concept", self.identity_payload()
        )
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
            "component_framework": (
                None
                if self.component_framework is None
                else self.component_framework.value
            ),
            "component_path": list(self.component_path),
            "output_basis": self.output_basis.value,
            "transformation": self.transformation.value,
            "frequency": self.frequency.value,
            "seasonal_basis": self.seasonal_basis.value,
            "valuation_basis": self.valuation_basis,
            "unit": self.unit,
            "scale": self.scale,
            "price_base_period": self.price_base_period,
            "chain_linking_method": self.chain_linking_method,
            "methodology_era_id": self.methodology_era_id,
        }

    def base_series_payload(self) -> dict[str, JSONValue]:
        payload = self.identity_payload()
        for key in ("schema_version", "transformation", "unit", "scale"):
            payload.pop(key)
        return payload

    @property
    def base_series_id(self) -> str:
        return _stable_id(
            "national-accounts-base-series", self.base_series_payload()
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "concept_id": self.concept_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NationalAccountsConceptV1:
        framework = data.get("component_framework")
        return cls(
            economy_code=str(data.get("economy_code", "")),
            indicator_key=str(data.get("indicator_key", "")),
            source_series_id=str(data.get("source_series_id", "")),
            measure=NationalAccountsMeasure.from_value(
                str(data.get("measure", ""))
            ),
            component_framework=(
                None
                if framework is None
                else NationalAccountsComponentFramework.from_value(
                    str(framework)
                )
            ),
            component_path=tuple(
                str(item)
                for item in _sequence(
                    data.get("component_path"), "component_path"
                )
            ),
            output_basis=NationalAccountsOutputBasis.from_value(
                str(data.get("output_basis", ""))
            ),
            transformation=NationalAccountsTransformation.from_value(
                str(data.get("transformation", ""))
            ),
            frequency=NationalAccountsFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            seasonal_basis=NationalAccountsSeasonalBasis.from_value(
                str(data.get("seasonal_basis", ""))
            ),
            valuation_basis=str(data.get("valuation_basis", "")),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            price_base_period=_optional_text(data.get("price_base_period")),
            chain_linking_method=_optional_text(
                data.get("chain_linking_method")
            ),
            methodology_era_id=str(data.get("methodology_era_id", "")),
            concept_id=str(data.get("concept_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> NationalAccountsConceptV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "national-accounts concept is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class NationalAccountsObservationV1:
    """One immutable source-published or level-derived stage vintage."""

    concept: NationalAccountsConceptV1
    logical_event_key: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    basis: NationalAccountsObservationBasis
    value: float
    raw_lexical: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    revision_sequence: int
    revision_kind: NationalAccountsRevisionKind
    supersedes_observation_id: str | None
    derived_from_observation_ids: tuple[str, ...]
    derivation_id: str | None
    limitations: tuple[str, ...]
    observation_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NATIONAL_ACCOUNTS_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported national-accounts observation schema")
        if not isinstance(self.concept, NationalAccountsConceptV1):
            raise TypeError("observation requires a national-accounts concept")
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
        basis = NationalAccountsObservationBasis.from_value(self.basis)
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
        if revision > MAX_NATIONAL_ACCOUNTS_REVISIONS:
            raise ValueError("revision_sequence exceeds bound")
        kind = NationalAccountsRevisionKind.from_value(self.revision_kind)
        supersedes = _optional_text(self.supersedes_observation_id)
        if revision == 0:
            if kind is not NationalAccountsRevisionKind.STAGE_INITIAL:
                raise ValueError(
                    "first stage value must use stage-initial kind"
                )
            if supersedes is not None:
                raise ValueError("first stage value cannot supersede another")
        elif (
            kind is NationalAccountsRevisionKind.STAGE_INITIAL
            or supersedes is None
        ):
            raise ValueError("revision must identify its kind and predecessor")
        derived_ids = _texts(
            self.derived_from_observation_ids,
            "derived observation id",
            maximum=2,
        )
        derivation_id = _optional_text(self.derivation_id)
        if basis is NationalAccountsObservationBasis.SOURCE_PUBLISHED:
            if derived_ids or derivation_id is not None:
                raise ValueError(
                    "source-published value contains derivation links"
                )
        elif len(derived_ids) != 2 or derivation_id is None:
            raise ValueError("derived value requires exactly two level inputs")
        object.__setattr__(self, "revision_sequence", revision)
        object.__setattr__(self, "revision_kind", kind)
        object.__setattr__(self, "supersedes_observation_id", supersedes)
        object.__setattr__(self, "derived_from_observation_ids", derived_ids)
        object.__setattr__(self, "derivation_id", derivation_id)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "national-accounts-observation", self.identity_payload()
        )
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "observation_id differs from deterministic identity"
            )
        object.__setattr__(self, "observation_id", expected)

    @property
    def stage_initial_actual(self) -> bool:
        return self.revision_sequence == 0

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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsObservationV1:
        return cls(
            concept=NationalAccountsConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            logical_event_key=str(data.get("logical_event_key", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            basis=NationalAccountsObservationBasis.from_value(
                str(data.get("basis", ""))
            ),
            value=cast(float, data.get("value")),
            raw_lexical=str(data.get("raw_lexical", "")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            revision_sequence=cast(int, data.get("revision_sequence")),
            revision_kind=NationalAccountsRevisionKind.from_value(
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
class NationalAccountsVintageChainV1:
    """First value and all later revisions for one exact stage event."""

    observations: tuple[NationalAccountsObservationV1, ...]
    chain_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_VINTAGE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_VINTAGE_CHAIN_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported national-accounts vintage-chain schema"
            )
        observations = tuple(
            sorted(self.observations, key=lambda item: item.revision_sequence)
        )
        if (
            not observations
            or len(observations) > MAX_NATIONAL_ACCOUNTS_REVISIONS + 1
        ):
            raise ValueError("national-accounts vintage-chain size is invalid")
        if any(
            not isinstance(item, NationalAccountsObservationV1)
            for item in observations
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
                raise ValueError("vintage chain changes stage or concept")
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
        expected = _stable_id(
            "national-accounts-vintage-chain", self.identity_payload()
        )
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id differs from deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    @property
    def initial(self) -> NationalAccountsObservationV1:
        return self.observations[0]

    def as_known_at(
        self, decision_at_ns: int
    ) -> NationalAccountsObservationV1 | None:
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsVintageChainV1:
        return cls(
            observations=tuple(
                NationalAccountsObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _growth_lag(
    transformation: NationalAccountsTransformation,
    frequency: NationalAccountsFrequency,
) -> int:
    if (
        transformation
        is NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT
    ):
        if frequency is not NationalAccountsFrequency.MONTHLY:
            raise ValueError("monthly growth requires monthly frequency")
        return 1
    if transformation in {
        NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
        _ANNUALIZED_QOQ,
    }:
        if frequency is not NationalAccountsFrequency.QUARTERLY:
            raise ValueError("quarterly growth requires quarterly frequency")
        return 3
    if transformation is NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT:
        return 12
    if transformation is NationalAccountsTransformation.ABSOLUTE_CHANGE:
        return {
            NationalAccountsFrequency.MONTHLY: 1,
            NationalAccountsFrequency.QUARTERLY: 3,
            NationalAccountsFrequency.ANNUAL: 12,
        }[frequency]
    raise ValueError("transformation is not derivable from levels")


@dataclass(frozen=True, slots=True)
class NationalAccountsGrowthDerivationV1:
    """Exact level vintages and formula used to derive a growth actual."""

    target_concept: NationalAccountsConceptV1
    current_level: NationalAccountsObservationV1
    comparison_level: NationalAccountsObservationV1
    vintage_basis: NationalAccountsDerivationVintageBasis
    source_transformation_unavailable: bool
    claimed_stage_initial_actual: bool
    derived_value: float
    tolerance: float
    derivation_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_GROWTH_DERIVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_GROWTH_DERIVATION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported national-accounts derivation schema")
        if not isinstance(self.target_concept, NationalAccountsConceptV1):
            raise TypeError("derivation requires a target concept")
        if not self.target_concept.transformation.derivable_from_levels:
            raise ValueError("target transformation is not level-derivable")
        if not isinstance(
            self.current_level, NationalAccountsObservationV1
        ) or not isinstance(
            self.comparison_level, NationalAccountsObservationV1
        ):
            raise TypeError("derivation inputs must be observations")
        current = self.current_level
        comparison = self.comparison_level
        if current.concept.transformation not in {
            NationalAccountsTransformation.LEVEL,
            NationalAccountsTransformation.ANNUALIZED_LEVEL,
            NationalAccountsTransformation.INDEX_LEVEL,
        } or comparison.concept.transformation not in {
            NationalAccountsTransformation.LEVEL,
            NationalAccountsTransformation.ANNUALIZED_LEVEL,
            NationalAccountsTransformation.INDEX_LEVEL,
        }:
            raise ValueError("growth derivation inputs must be level concepts")
        if (
            current.concept.concept_id != comparison.concept.concept_id
            or current.concept.base_series_id
            != self.target_concept.base_series_id
        ):
            raise ValueError(
                "growth derivation switches source series identity"
            )
        lag = _month_index(current.reference_period_end_ns) - _month_index(
            comparison.reference_period_end_ns
        )
        if lag != _growth_lag(
            self.target_concept.transformation,
            self.target_concept.frequency,
        ):
            raise ValueError("level vintages do not cover the required lag")
        basis = NationalAccountsDerivationVintageBasis.from_value(
            self.vintage_basis
        )
        for name in (
            "source_transformation_unavailable",
            "claimed_stage_initial_actual",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if not self.source_transformation_unavailable:
            raise ValueError("source-published transformation takes precedence")
        if (
            self.claimed_stage_initial_actual
            and basis is NationalAccountsDerivationVintageBasis.LATEST_CURRENT
        ):
            raise ValueError(
                "current levels cannot fabricate a historical actual"
            )
        if self.claimed_stage_initial_actual and (
            basis is not NationalAccountsDerivationVintageBasis.INITIAL_AT_STAGE
            or not current.stage_initial_actual
            or not comparison.stage_initial_actual
        ):
            raise ValueError(
                "historical stage initial requires exact initial level vintages"
            )
        transformation = self.target_concept.transformation
        if transformation is NationalAccountsTransformation.ABSOLUTE_CHANGE:
            computed = current.value - comparison.value
        else:
            if comparison.value == 0:
                raise ValueError("comparison level cannot be zero for growth")
            ratio = current.value / comparison.value
            computed = 100.0 * (
                ratio**4 - 1.0
                if transformation is _ANNUALIZED_QOQ
                else ratio - 1.0
            )
        supplied = _finite(self.derived_value, "derived_value")
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        if not math.isclose(supplied, computed, rel_tol=0.0, abs_tol=tolerance):
            raise ValueError(
                "derived growth does not match exact level vintages"
            )
        object.__setattr__(self, "vintage_basis", basis)
        object.__setattr__(self, "derived_value", supplied)
        object.__setattr__(self, "tolerance", tolerance)
        expected = _stable_id(
            "national-accounts-growth-derivation", self.identity_payload()
        )
        supplied_id = _optional_text(self.derivation_id)
        if supplied_id is not None and supplied_id != expected:
            raise ValueError(
                "derivation_id differs from deterministic identity"
            )
        object.__setattr__(self, "derivation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        annualized = self.target_concept.transformation is _ANNUALIZED_QOQ
        return {
            "schema_version": self.schema_version,
            "target_concept": self.target_concept.to_dict(),
            "current_level": self.current_level.to_dict(),
            "comparison_level": self.comparison_level.to_dict(),
            "vintage_basis": self.vintage_basis.value,
            "source_transformation_unavailable": (
                self.source_transformation_unavailable
            ),
            "claimed_stage_initial_actual": self.claimed_stage_initial_actual,
            "derived_value": self.derived_value,
            "tolerance": self.tolerance,
            "formula": (
                "L_t-L_lag"
                if self.target_concept.transformation
                is NationalAccountsTransformation.ABSOLUTE_CHANGE
                else (
                    "100*((L_t/L_t-1)^4-1)"
                    if annualized
                    else "100*(L_t/L_lag-1)"
                )
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "derivation_id": self.derivation_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsGrowthDerivationV1:
        return cls(
            target_concept=NationalAccountsConceptV1.from_dict(
                _mapping(data.get("target_concept"), "target_concept")
            ),
            current_level=NationalAccountsObservationV1.from_dict(
                _mapping(data.get("current_level"), "current_level")
            ),
            comparison_level=NationalAccountsObservationV1.from_dict(
                _mapping(data.get("comparison_level"), "comparison_level")
            ),
            vintage_basis=NationalAccountsDerivationVintageBasis.from_value(
                str(data.get("vintage_basis", ""))
            ),
            source_transformation_unavailable=cast(
                bool, data.get("source_transformation_unavailable")
            ),
            claimed_stage_initial_actual=cast(
                bool, data.get("claimed_stage_initial_actual")
            ),
            derived_value=cast(float, data.get("derived_value")),
            tolerance=cast(float, data.get("tolerance")),
            derivation_id=str(data.get("derivation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsExpectationV1:
    """Pre-release expectation for one exact concept, stage, and event."""

    target_event_key: str
    target_stage: EconomicReleaseStage
    concept: NationalAccountsConceptV1
    reference_period: str
    kind: NationalAccountsExpectationKind
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
    schema_version: str = NATIONAL_ACCOUNTS_EXPECTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NATIONAL_ACCOUNTS_EXPECTATION_SCHEMA_VERSION:
            raise ValueError("unsupported national-accounts expectation schema")
        object.__setattr__(
            self,
            "target_event_key",
            _key(self.target_event_key, "target_event_key"),
        )
        object.__setattr__(
            self,
            "target_stage",
            EconomicReleaseStage.from_value(self.target_stage),
        )
        if not isinstance(self.concept, NationalAccountsConceptV1):
            raise TypeError("expectation requires a national-accounts concept")
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        kind = NationalAccountsExpectationKind.from_value(self.kind)
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
        if kind is NationalAccountsExpectationKind.UNAVAILABLE:
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
                raise ValueError("unavailable expectation contains evidence")
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
        expected = _stable_id(
            "national-accounts-expectation", self.identity_payload()
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
            "target_event_key": self.target_event_key,
            "target_stage": self.target_stage.value,
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsExpectationV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            target_stage=EconomicReleaseStage.from_value(
                str(data.get("target_stage", ""))
            ),
            concept=NationalAccountsConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            reference_period=str(data.get("reference_period", "")),
            kind=NationalAccountsExpectationKind.from_value(
                str(data.get("kind", ""))
            ),
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
class NationalAccountsReleaseTripletV1:
    """Stage-initial actual, previous-as-known, and exact-stage forecast."""

    target_event_key: str
    actual_stage_initial: NationalAccountsObservationV1
    previous_as_known: NationalAccountsObservationV1
    previous_as_known_at_ns: int
    expectation: NationalAccountsExpectationV1
    triplet_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_RELEASE_TRIPLET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_RELEASE_TRIPLET_SCHEMA_VERSION
        ):
            raise ValueError("unsupported national-accounts triplet schema")
        target = _key(self.target_event_key, "target_event_key")
        if not isinstance(
            self.actual_stage_initial, NationalAccountsObservationV1
        ) or not isinstance(
            self.previous_as_known, NationalAccountsObservationV1
        ):
            raise TypeError("triplet actuals must use national-accounts values")
        if not isinstance(self.expectation, NationalAccountsExpectationV1):
            raise TypeError("triplet expectation must use the v1 contract")
        actual = self.actual_stage_initial
        previous = self.previous_as_known
        if actual.logical_event_key != target:
            raise ValueError("stage actual belongs to another event")
        if not actual.stage_initial_actual:
            raise ValueError("actual must be first published at its stage")
        if actual.concept.concept_id != previous.concept.concept_id:
            raise ValueError("previous field silently changes output concept")
        lag = _month_index(actual.reference_period_end_ns) - _month_index(
            previous.reference_period_end_ns
        )
        expected_lag = {
            NationalAccountsFrequency.MONTHLY: 1,
            NationalAccountsFrequency.QUARTERLY: 3,
            NationalAccountsFrequency.ANNUAL: 12,
        }[actual.concept.frequency]
        if lag != expected_lag:
            raise ValueError(
                "previous field is not immediately preceding period"
            )
        if previous.logical_event_key == target:
            raise ValueError("previous field reuses the current stage event")
        cutoff = _bounded_ns(
            self.previous_as_known_at_ns, "previous_as_known_at_ns"
        )
        if previous.available_at_ns > cutoff or cutoff > actual.published_at_ns:
            raise ValueError(
                "previous-as-known evidence is not release-bounded"
            )
        if (
            previous.revision_kind
            is NationalAccountsRevisionKind.SIMULTANEOUS_PREVIOUS
            and previous.available_at_ns != actual.published_at_ns
        ):
            raise ValueError(
                "simultaneous previous revision has wrong timestamp"
            )
        expectation = self.expectation
        if (
            expectation.target_event_key != target
            or expectation.target_stage is not actual.stage
            or expectation.concept.concept_id != actual.concept.concept_id
            or expectation.reference_period != actual.reference_period
        ):
            raise ValueError(
                "forecast does not match exact GDP stage and concept"
            )
        if expectation.kind is not NationalAccountsExpectationKind.UNAVAILABLE:
            if not expectation.calendar_style_eligible:
                raise ValueError(
                    "period projection cannot populate stage forecast"
                )
            if (
                expectation.available_at_ns is None
                or expectation.available_at_ns >= actual.published_at_ns
            ):
                raise ValueError(
                    "forecast was not available before stage release"
                )
        object.__setattr__(self, "target_event_key", target)
        object.__setattr__(self, "previous_as_known_at_ns", cutoff)
        expected = _stable_id(
            "national-accounts-release-triplet", self.identity_payload()
        )
        supplied = _optional_text(self.triplet_id)
        if supplied is not None and supplied != expected:
            raise ValueError("triplet_id differs from deterministic identity")
        object.__setattr__(self, "triplet_id", expected)

    @property
    def actual(self) -> float:
        return self.actual_stage_initial.value

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
            "actual_stage_initial": self.actual_stage_initial.to_dict(),
            "previous_as_known": self.previous_as_known.to_dict(),
            "previous_as_known_at_ns": self.previous_as_known_at_ns,
            "expectation": self.expectation.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "triplet_id": self.triplet_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsReleaseTripletV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            actual_stage_initial=NationalAccountsObservationV1.from_dict(
                _mapping(
                    data.get("actual_stage_initial"), "actual_stage_initial"
                )
            ),
            previous_as_known=NationalAccountsObservationV1.from_dict(
                _mapping(data.get("previous_as_known"), "previous_as_known")
            ),
            previous_as_known_at_ns=cast(
                int, data.get("previous_as_known_at_ns")
            ),
            expectation=NationalAccountsExpectationV1.from_dict(
                _mapping(data.get("expectation"), "expectation")
            ),
            triplet_id=str(data.get("triplet_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsComponentEntryV1:
    """One signed component value in an official reconciliation table."""

    observation: NationalAccountsObservationV1
    role: NationalAccountsComponentRole
    entry_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_COMPONENT_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_COMPONENT_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported national-accounts component-entry schema"
            )
        if not isinstance(self.observation, NationalAccountsObservationV1):
            raise TypeError("component entry requires an observation")
        if not self.observation.concept.measure.is_component:
            raise ValueError("component entry requires component identity")
        object.__setattr__(
            self, "role", NationalAccountsComponentRole.from_value(self.role)
        )
        expected = _stable_id(
            "national-accounts-component-entry", self.identity_payload()
        )
        supplied = _optional_text(self.entry_id)
        if supplied is not None and supplied != expected:
            raise ValueError("entry_id differs from deterministic identity")
        object.__setattr__(self, "entry_id", expected)

    @property
    def signed_value(self) -> float:
        return self.observation.value * (
            -1.0
            if self.role is NationalAccountsComponentRole.SUBTRACTIVE
            else 1.0
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "observation": self.observation.to_dict(),
            "role": self.role.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsComponentEntryV1:
        return cls(
            observation=NationalAccountsObservationV1.from_dict(
                _mapping(data.get("observation"), "observation")
            ),
            role=NationalAccountsComponentRole.from_value(
                str(data.get("role", ""))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsComponentReconciliationV1:
    """Explicit additive, contribution, non-additive, or unavailable table."""

    aggregate: NationalAccountsObservationV1
    framework: NationalAccountsComponentFramework
    entries: tuple[NationalAccountsComponentEntryV1, ...]
    basis: NationalAccountsReconciliationBasis
    coverage: NationalAccountsComponentCoverage
    missing_component_keys: tuple[str, ...]
    official_table_id: str | None
    residual: float | None
    tolerance: float
    reconciled: bool
    reconciliation_id: str = ""
    schema_version: str = (
        NATIONAL_ACCOUNTS_COMPONENT_RECONCILIATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_COMPONENT_RECONCILIATION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported component-reconciliation schema")
        if not isinstance(self.aggregate, NationalAccountsObservationV1):
            raise TypeError("component reconciliation requires an aggregate")
        if self.aggregate.concept.measure.is_component:
            raise ValueError("reconciliation aggregate cannot be a component")
        framework = NationalAccountsComponentFramework.from_value(
            self.framework
        )
        entries = tuple(sorted(self.entries, key=lambda item: item.entry_id))
        if len(entries) > MAX_NATIONAL_ACCOUNTS_COMPONENTS or any(
            not isinstance(item, NationalAccountsComponentEntryV1)
            for item in entries
        ):
            raise ValueError("component entry set is invalid")
        if len(
            {item.observation.concept.concept_id for item in entries}
        ) != len(entries):
            raise ValueError("component reconciliation repeats a concept")
        aggregate = self.aggregate
        for entry in entries:
            observation = entry.observation
            concept = observation.concept
            if concept.component_framework is not framework:
                raise ValueError("component belongs to another framework")
            if (
                observation.logical_event_key,
                observation.reference_period,
                observation.reference_period_end_ns,
                observation.stage,
                observation.published_at_ns,
                observation.source_key,
                observation.source_release_id,
                observation.source_request_id,
                observation.content_sha256,
            ) != (
                aggregate.logical_event_key,
                aggregate.reference_period,
                aggregate.reference_period_end_ns,
                aggregate.stage,
                aggregate.published_at_ns,
                aggregate.source_key,
                aggregate.source_release_id,
                aggregate.source_request_id,
                aggregate.content_sha256,
            ):
                raise ValueError("component is not from the aggregate release")
            left = aggregate.concept
            if (
                concept.economy_code,
                concept.output_basis,
                concept.frequency,
                concept.seasonal_basis,
                concept.valuation_basis,
                concept.price_base_period,
                concept.chain_linking_method,
                concept.methodology_era_id,
            ) != (
                left.economy_code,
                left.output_basis,
                left.frequency,
                left.seasonal_basis,
                left.valuation_basis,
                left.price_base_period,
                left.chain_linking_method,
                left.methodology_era_id,
            ):
                raise ValueError("component silently changes accounting basis")
        basis = NationalAccountsReconciliationBasis.from_value(self.basis)
        coverage = NationalAccountsComponentCoverage.from_value(self.coverage)
        missing = _keys(self.missing_component_keys, "missing_component_key")
        table_id = _optional_key(self.official_table_id, "official_table_id")
        residual = _optional_finite(self.residual, "residual")
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        if not isinstance(self.reconciled, bool):
            raise TypeError("reconciled must be boolean")
        if basis is NationalAccountsReconciliationBasis.UNAVAILABLE:
            if (
                coverage is not NationalAccountsComponentCoverage.UNAVAILABLE
                or entries
                or not missing
                or table_id is not None
                or residual is not None
                or self.reconciled
            ):
                raise ValueError("unavailable component table is inconsistent")
        elif (
            basis
            is NationalAccountsReconciliationBasis.CHAIN_VOLUME_NON_ADDITIVE
        ):
            if (
                coverage is not NationalAccountsComponentCoverage.NON_ADDITIVE
                or not entries
                or table_id is None
                or residual is not None
                or self.reconciled
            ):
                raise ValueError("chain-volume non-additivity is inconsistent")
            if aggregate.concept.output_basis is not (
                NationalAccountsOutputBasis.REAL_CHAIN_VOLUME
            ):
                raise ValueError(
                    "non-additivity claim requires chain-volume output"
                )
        else:
            if table_id is None or coverage not in {
                NationalAccountsComponentCoverage.COMPLETE,
                NationalAccountsComponentCoverage.PARTIAL,
            }:
                raise ValueError("additive table requires explicit coverage")
            if basis is NationalAccountsReconciliationBasis.ADDITIVE_LEVELS:
                if (
                    aggregate.concept.output_basis
                    is NationalAccountsOutputBasis.REAL_CHAIN_VOLUME
                ):
                    raise ValueError(
                        "chain-volume levels cannot be forced additive"
                    )
                if aggregate.concept.transformation not in {
                    NationalAccountsTransformation.LEVEL,
                    NationalAccountsTransformation.ANNUALIZED_LEVEL,
                    NationalAccountsTransformation.INDEX_LEVEL,
                } or any(
                    entry.observation.concept.transformation
                    is not aggregate.concept.transformation
                    for entry in entries
                ):
                    raise ValueError(
                        "additive-level table mixes transformations"
                    )
            elif aggregate.concept.transformation not in {
                NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT,
                NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
                _ANNUALIZED_QOQ,
                NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT,
            } or any(
                entry.observation.concept.transformation is not _CONTRIBUTION_PP
                for entry in entries
            ):
                raise ValueError("contribution table mixes transformations")
            if coverage is NationalAccountsComponentCoverage.PARTIAL:
                if (
                    not entries
                    or not missing
                    or residual is not None
                    or self.reconciled
                ):
                    raise ValueError("partial component table is inconsistent")
            else:
                if not entries or missing or residual is None:
                    raise ValueError("complete component table is incomplete")
                computed = aggregate.value - sum(
                    entry.signed_value for entry in entries
                )
                if not math.isclose(
                    residual, computed, rel_tol=0.0, abs_tol=tolerance
                ):
                    raise ValueError("component residual is not reproducible")
                expected_reconciled = abs(computed) <= tolerance
                if self.reconciled != expected_reconciled:
                    raise ValueError(
                        "component reconciliation flag is inconsistent"
                    )
        object.__setattr__(self, "framework", framework)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "missing_component_keys", missing)
        object.__setattr__(self, "official_table_id", table_id)
        object.__setattr__(self, "residual", residual)
        object.__setattr__(self, "tolerance", tolerance)
        expected = _stable_id(
            "national-accounts-component-reconciliation",
            self.identity_payload(),
        )
        supplied = _optional_text(self.reconciliation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "reconciliation_id differs from deterministic identity"
            )
        object.__setattr__(self, "reconciliation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "aggregate": self.aggregate.to_dict(),
            "framework": self.framework.value,
            "entries": [item.to_dict() for item in self.entries],
            "basis": self.basis.value,
            "coverage": self.coverage.value,
            "missing_component_keys": list(self.missing_component_keys),
            "official_table_id": self.official_table_id,
            "residual": self.residual,
            "tolerance": self.tolerance,
            "reconciled": self.reconciled,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "reconciliation_id": self.reconciliation_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsComponentReconciliationV1:
        return cls(
            aggregate=NationalAccountsObservationV1.from_dict(
                _mapping(data.get("aggregate"), "aggregate")
            ),
            framework=NationalAccountsComponentFramework.from_value(
                str(data.get("framework", ""))
            ),
            entries=tuple(
                NationalAccountsComponentEntryV1.from_dict(
                    _mapping(item, "component entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            basis=NationalAccountsReconciliationBasis.from_value(
                str(data.get("basis", ""))
            ),
            coverage=NationalAccountsComponentCoverage.from_value(
                str(data.get("coverage", ""))
            ),
            missing_component_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("missing_component_keys"),
                    "missing_component_keys",
                )
            ),
            official_table_id=_optional_text(data.get("official_table_id")),
            residual=cast(float | None, data.get("residual")),
            tolerance=cast(float, data.get("tolerance")),
            reconciled=cast(bool, data.get("reconciled")),
            reconciliation_id=str(data.get("reconciliation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsReleaseV1:
    """One stage-specific GDP publication and its retained table values."""

    sequence_key: str
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
    observations: tuple[NationalAccountsObservationV1, ...]
    reconciliations: tuple[NationalAccountsComponentReconciliationV1, ...]
    display_observation_id: str
    limitations: tuple[str, ...]
    release_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != NATIONAL_ACCOUNTS_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported national-accounts release schema")
        sequence_key = _key(self.sequence_key, "sequence_key")
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
            or len(observations) > MAX_NATIONAL_ACCOUNTS_RELEASE_OBSERVATIONS
            or any(
                not isinstance(item, NationalAccountsObservationV1)
                for item in observations
            )
        ):
            raise ValueError("release observation set is invalid")
        if len({item.concept.concept_id for item in observations}) != len(
            observations
        ):
            raise ValueError("release repeats a national-accounts concept")
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
                    "release mixes stage, period, or source evidence"
                )
            if not observation.stage_initial_actual:
                raise ValueError(
                    "release contains a later within-stage revision"
                )
        display = _required_text(
            self.display_observation_id, "display_observation_id"
        )
        observation_ids = {item.observation_id for item in observations}
        if display not in observation_ids:
            raise ValueError("display observation is not retained by release")
        reconciliations = tuple(
            sorted(
                self.reconciliations, key=lambda item: item.reconciliation_id
            )
        )
        if len(reconciliations) > MAX_NATIONAL_ACCOUNTS_COMPONENTS or any(
            not isinstance(item, NationalAccountsComponentReconciliationV1)
            for item in reconciliations
        ):
            raise ValueError("release reconciliation set is invalid")
        if len({item.framework for item in reconciliations}) != len(
            reconciliations
        ):
            raise ValueError("release repeats a component framework")
        for reconciliation in reconciliations:
            referenced = {
                reconciliation.aggregate.observation_id,
                *(
                    entry.observation.observation_id
                    for entry in reconciliation.entries
                ),
            }
            if not referenced.issubset(observation_ids):
                raise ValueError(
                    "reconciliation references an unretained value"
                )
        object.__setattr__(self, "sequence_key", sequence_key)
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
        object.__setattr__(self, "reconciliations", reconciliations)
        object.__setattr__(self, "display_observation_id", display)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "national-accounts-release", self.identity_payload()
        )
        supplied = _optional_text(self.release_id)
        if supplied is not None and supplied != expected:
            raise ValueError("release_id differs from deterministic identity")
        object.__setattr__(self, "release_id", expected)

    @property
    def display_observation(self) -> NationalAccountsObservationV1:
        return next(
            item
            for item in self.observations
            if item.observation_id == self.display_observation_id
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "sequence_key": self.sequence_key,
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
            "reconciliations": [
                item.to_dict() for item in self.reconciliations
            ],
            "display_observation_id": self.display_observation_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NationalAccountsReleaseV1:
        return cls(
            sequence_key=str(data.get("sequence_key", "")),
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
                NationalAccountsObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            reconciliations=tuple(
                NationalAccountsComponentReconciliationV1.from_dict(
                    _mapping(item, "reconciliation")
                )
                for item in _sequence(
                    data.get("reconciliations"), "reconciliations"
                )
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
class NationalAccountsReleaseSequenceV1:
    """Chronological stage publications for one reference period/concept."""

    sequence_key: str
    releases: tuple[NationalAccountsReleaseV1, ...]
    sequence_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_RELEASE_SEQUENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_RELEASE_SEQUENCE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported national-accounts release-sequence schema"
            )
        sequence_key = _key(self.sequence_key, "sequence_key")
        releases = tuple(
            sorted(self.releases, key=lambda item: item.published_at_ns)
        )
        if (
            not releases
            or len(releases) > MAX_NATIONAL_ACCOUNTS_RELEASE_STAGES
            or any(
                not isinstance(item, NationalAccountsReleaseV1)
                for item in releases
            )
        ):
            raise ValueError("release sequence is outside bounds")
        first = releases[0]
        common = (
            first.sequence_key,
            first.economy_code,
            first.reference_period,
            first.reference_period_end_ns,
            first.display_observation.concept.concept_id,
        )
        if common[0] != sequence_key:
            raise ValueError("release sequence key does not match releases")
        previous_order = -1
        previous_published = -1
        event_keys: set[str] = set()
        stage_values: set[EconomicReleaseStage] = set()
        for release in releases:
            if (
                release.sequence_key,
                release.economy_code,
                release.reference_period,
                release.reference_period_end_ns,
                release.display_observation.concept.concept_id,
            ) != common:
                raise ValueError("release sequence changes period or concept")
            order = _STAGE_ORDER[release.stage]
            if (
                order <= previous_order
                or release.published_at_ns <= previous_published
            ):
                raise ValueError(
                    "release stages are not strictly chronological"
                )
            if (
                release.logical_event_key in event_keys
                or release.stage in stage_values
            ):
                raise ValueError(
                    "each publication stage must be a distinct event"
                )
            event_keys.add(release.logical_event_key)
            stage_values.add(release.stage)
            previous_order = order
            previous_published = release.published_at_ns
        object.__setattr__(self, "sequence_key", sequence_key)
        object.__setattr__(self, "releases", releases)
        expected = _stable_id(
            "national-accounts-release-sequence", self.identity_payload()
        )
        supplied = _optional_text(self.sequence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("sequence_id differs from deterministic identity")
        object.__setattr__(self, "sequence_id", expected)

    def release_for_stage(
        self, stage: EconomicReleaseStage
    ) -> NationalAccountsReleaseV1:
        selected = EconomicReleaseStage.from_value(stage)
        matches = [item for item in self.releases if item.stage is selected]
        if len(matches) != 1:
            raise ValueError("release sequence has no unique requested stage")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "sequence_key": self.sequence_key,
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "sequence_id": self.sequence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsReleaseSequenceV1:
        return cls(
            sequence_key=str(data.get("sequence_key", "")),
            releases=tuple(
                NationalAccountsReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            sequence_id=str(data.get("sequence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsBenchmarkRevisionV1:
    """One publication revising multiple retained reference-period vintages."""

    benchmark_event_key: str
    economy_code: str
    benchmark_reference_period: str
    observations: tuple[NationalAccountsObservationV1, ...]
    methodology_event_id: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    limitations: tuple[str, ...]
    benchmark_revision_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_BENCHMARK_REVISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_BENCHMARK_REVISION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported benchmark-revision schema")
        event_key = _key(self.benchmark_event_key, "benchmark_event_key")
        economy = _economy(self.economy_code)
        benchmark_period = _required_text(
            self.benchmark_reference_period, "benchmark_reference_period"
        )
        methodology_event_id = _required_text(
            self.methodology_event_id, "methodology_event_id"
        )
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("benchmark availability precedes publication")
        source_key = _key(self.source_key, "source_key")
        source_release = _key(self.source_release_id, "source_release_id")
        source_request = _key(self.source_request_id, "source_request_id")
        digest = _sha256(self.content_sha256, "content_sha256")
        observations = tuple(
            sorted(
                self.observations,
                key=lambda item: (
                    item.reference_period_end_ns,
                    item.concept.concept_id,
                    item.stage.value,
                ),
            )
        )
        if (
            len(observations) < 2
            or len(observations) > MAX_NATIONAL_ACCOUNTS_BENCHMARK_OBSERVATIONS
            or any(
                not isinstance(item, NationalAccountsObservationV1)
                for item in observations
            )
        ):
            raise ValueError("benchmark revision observation set is invalid")
        cells: set[tuple[str, str, EconomicReleaseStage]] = set()
        for observation in observations:
            if (
                observation.concept.economy_code,
                observation.revision_kind,
                observation.published_at_ns,
                observation.available_at_ns,
                observation.source_key,
                observation.source_release_id,
                observation.source_request_id,
                observation.content_sha256,
            ) != (
                economy,
                NationalAccountsRevisionKind.BENCHMARK,
                published,
                available,
                source_key,
                source_release,
                source_request,
                digest,
            ):
                raise ValueError(
                    "benchmark revision mixes source or revision kind"
                )
            cell = (
                observation.concept.concept_id,
                observation.reference_period,
                observation.stage,
            )
            if cell in cells:
                raise ValueError(
                    "benchmark revision repeats a stage-period cell"
                )
            cells.add(cell)
        object.__setattr__(self, "benchmark_event_key", event_key)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "benchmark_reference_period", benchmark_period)
        object.__setattr__(self, "methodology_event_id", methodology_event_id)
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_release_id", source_release)
        object.__setattr__(self, "source_request_id", source_request)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "national-accounts-benchmark-revision", self.identity_payload()
        )
        supplied = _optional_text(self.benchmark_revision_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "benchmark_revision_id differs from deterministic identity"
            )
        object.__setattr__(self, "benchmark_revision_id", expected)

    @property
    def reference_period_bounds_ns(self) -> tuple[int, int]:
        periods = [item.reference_period_end_ns for item in self.observations]
        return min(periods), max(periods)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "benchmark_event_key": self.benchmark_event_key,
            "economy_code": self.economy_code,
            "benchmark_reference_period": self.benchmark_reference_period,
            "observations": [item.to_dict() for item in self.observations],
            "methodology_event_id": self.methodology_event_id,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "benchmark_revision_id": self.benchmark_revision_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsBenchmarkRevisionV1:
        return cls(
            benchmark_event_key=str(data.get("benchmark_event_key", "")),
            economy_code=str(data.get("economy_code", "")),
            benchmark_reference_period=str(
                data.get("benchmark_reference_period", "")
            ),
            observations=tuple(
                NationalAccountsObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            methodology_event_id=str(data.get("methodology_event_id", "")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            benchmark_revision_id=str(data.get("benchmark_revision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsMethodologyEventV1:
    """Versioned benchmark, rebase, linking, or classification change."""

    event_key: str
    economy_code: str
    kind: NationalAccountsMethodologyChangeKind
    affected_concept_ids: tuple[str, ...]
    old_methodology_era_id: str
    new_methodology_era_id: str
    effective_reference_period: str
    benchmark_reference_period: str | None
    comparable_across_event: bool
    bridge_id: str | None
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_request_id: str
    content_sha256: str
    source_uri: str
    notes: tuple[str, ...]
    event_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_METHODOLOGY_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_METHODOLOGY_EVENT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported national-accounts methodology schema")
        event_key = _key(self.event_key, "event_key")
        economy = _economy(self.economy_code)
        kind = NationalAccountsMethodologyChangeKind.from_value(self.kind)
        concepts = _texts(
            self.affected_concept_ids,
            "affected_concept_id",
            required=True,
        )
        old_era = _key(self.old_methodology_era_id, "old_methodology_era_id")
        new_era = _key(self.new_methodology_era_id, "new_methodology_era_id")
        if old_era == new_era:
            raise ValueError("methodology event must change era identity")
        effective = _required_text(
            self.effective_reference_period, "effective_reference_period"
        )
        benchmark = _optional_text(self.benchmark_reference_period)
        if (
            kind
            in {
                NationalAccountsMethodologyChangeKind.BENCHMARK_REVISION,
                NationalAccountsMethodologyChangeKind.REBASE,
                NationalAccountsMethodologyChangeKind.CHAIN_LINKING_CHANGE,
            }
            and benchmark is None
        ):
            raise ValueError("methodology kind requires benchmark period")
        if not isinstance(self.comparable_across_event, bool):
            raise TypeError("comparable_across_event must be boolean")
        bridge = _optional_key(self.bridge_id, "bridge_id")
        if self.comparable_across_event and bridge is None:
            raise ValueError("comparability requires an explicit bridge")
        if not self.comparable_across_event and bridge is not None:
            raise ValueError("non-comparable eras cannot claim a bridge")
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("methodology availability precedes publication")
        object.__setattr__(self, "event_key", event_key)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "affected_concept_ids", concepts)
        object.__setattr__(self, "old_methodology_era_id", old_era)
        object.__setattr__(self, "new_methodology_era_id", new_era)
        object.__setattr__(self, "effective_reference_period", effective)
        object.__setattr__(self, "benchmark_reference_period", benchmark)
        object.__setattr__(self, "bridge_id", bridge)
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
        object.__setattr__(
            self, "source_uri", _https_uri(self.source_uri, "source_uri")
        )
        object.__setattr__(
            self, "notes", _texts(self.notes, "note", required=True)
        )
        expected = _stable_id(
            "national-accounts-methodology-event", self.identity_payload()
        )
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id differs from deterministic identity")
        object.__setattr__(self, "event_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_key": self.event_key,
            "economy_code": self.economy_code,
            "kind": self.kind.value,
            "affected_concept_ids": list(self.affected_concept_ids),
            "old_methodology_era_id": self.old_methodology_era_id,
            "new_methodology_era_id": self.new_methodology_era_id,
            "effective_reference_period": self.effective_reference_period,
            "benchmark_reference_period": self.benchmark_reference_period,
            "comparable_across_event": self.comparable_across_event,
            "bridge_id": self.bridge_id,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "source_uri": self.source_uri,
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "event_id": self.event_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsMethodologyEventV1:
        return cls(
            event_key=str(data.get("event_key", "")),
            economy_code=str(data.get("economy_code", "")),
            kind=NationalAccountsMethodologyChangeKind.from_value(
                str(data.get("kind", ""))
            ),
            affected_concept_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("affected_concept_ids"), "affected_concept_ids"
                )
            ),
            old_methodology_era_id=str(data.get("old_methodology_era_id", "")),
            new_methodology_era_id=str(data.get("new_methodology_era_id", "")),
            effective_reference_period=str(
                data.get("effective_reference_period", "")
            ),
            benchmark_reference_period=_optional_text(
                data.get("benchmark_reference_period")
            ),
            comparable_across_event=cast(
                bool, data.get("comparable_across_event")
            ),
            bridge_id=_optional_text(data.get("bridge_id")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            source_uri=str(data.get("source_uri", "")),
            notes=tuple(
                str(item) for item in _sequence(data.get("notes"), "notes")
            ),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class NationalAccountsEconomyProfileV1:
    """Reviewed legal producer and GDP semantics for one economy."""

    economy_code: str
    institution: str
    source_key: str
    supported_frequencies: tuple[NationalAccountsFrequency, ...]
    required_output_bases: tuple[NationalAccountsOutputBasis, ...]
    required_transformations: tuple[NationalAccountsTransformation, ...]
    supported_stages: tuple[EconomicReleaseStage, ...]
    component_frameworks: tuple[NationalAccountsComponentFramework, ...]
    supports_benchmark_revision_sets: bool
    special_cases: tuple[str, ...]
    rationale: str
    profile_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_ECONOMY_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_ECONOMY_PROFILE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported national-accounts profile schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self, "institution", _required_text(self.institution, "institution")
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        frequencies = tuple(
            sorted(
                {
                    NationalAccountsFrequency.from_value(item)
                    for item in self.supported_frequencies
                },
                key=lambda item: item.value,
            )
        )
        outputs = tuple(
            sorted(
                {
                    NationalAccountsOutputBasis.from_value(item)
                    for item in self.required_output_bases
                },
                key=lambda item: item.value,
            )
        )
        transformations = tuple(
            sorted(
                {
                    NationalAccountsTransformation.from_value(item)
                    for item in self.required_transformations
                },
                key=lambda item: item.value,
            )
        )
        stages = tuple(
            sorted(
                {
                    EconomicReleaseStage.from_value(item)
                    for item in self.supported_stages
                },
                key=lambda item: (_STAGE_ORDER[item], item.value),
            )
        )
        frameworks = tuple(
            sorted(
                {
                    NationalAccountsComponentFramework.from_value(item)
                    for item in self.component_frameworks
                },
                key=lambda item: item.value,
            )
        )
        if not {
            NationalAccountsFrequency.QUARTERLY,
            NationalAccountsFrequency.ANNUAL,
        }.issubset(frequencies):
            raise ValueError("profile omits quarterly or annual accounts")
        if not {
            NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
            NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
        }.issubset(outputs):
            raise ValueError("profile must distinguish real and nominal output")
        if not {
            NationalAccountsTransformation.LEVEL,
            NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
            _ANNUALIZED_QOQ,
            NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT,
        }.issubset(transformations):
            raise ValueError("profile omits required GDP transformations")
        if len(stages) < 2 or EconomicReleaseStage.REVISION not in stages:
            raise ValueError("profile omits stage or revision semantics")
        if set(frameworks) != set(NationalAccountsComponentFramework):
            raise ValueError("profile omits a component framework")
        if not isinstance(self.supports_benchmark_revision_sets, bool):
            raise TypeError("supports_benchmark_revision_sets must be boolean")
        if not self.supports_benchmark_revision_sets:
            raise ValueError("profile must retain benchmark revision sets")
        cases = _keys(self.special_cases, "special_case", required=True)
        required_cases = {
            "annualized-versus-simple-quarterly-growth",
            "benchmark-vintage-graphs",
            "chain-volume-non-additivity",
            "component-reconciliation",
            "exact-stage-forecast-targets",
            "previous-as-known",
            "real-versus-nominal-output",
            "stage-specific-first-published-actuals",
        }
        if not required_cases.issubset(cases):
            raise ValueError("profile omits universal GDP edge cases")
        object.__setattr__(self, "supported_frequencies", frequencies)
        object.__setattr__(self, "required_output_bases", outputs)
        object.__setattr__(self, "required_transformations", transformations)
        object.__setattr__(self, "supported_stages", stages)
        object.__setattr__(self, "component_frameworks", frameworks)
        object.__setattr__(self, "special_cases", cases)
        object.__setattr__(
            self, "rationale", _required_text(self.rationale, "rationale")
        )
        expected = _stable_id(
            "national-accounts-economy-profile", self.identity_payload()
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
            "supported_frequencies": [
                item.value for item in self.supported_frequencies
            ],
            "required_output_bases": [
                item.value for item in self.required_output_bases
            ],
            "required_transformations": [
                item.value for item in self.required_transformations
            ],
            "supported_stages": [item.value for item in self.supported_stages],
            "component_frameworks": [
                item.value for item in self.component_frameworks
            ],
            "supports_benchmark_revision_sets": (
                self.supports_benchmark_revision_sets
            ),
            "special_cases": list(self.special_cases),
            "rationale": self.rationale,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsEconomyProfileV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            institution=str(data.get("institution", "")),
            source_key=str(data.get("source_key", "")),
            supported_frequencies=tuple(
                NationalAccountsFrequency.from_value(str(item))
                for item in _sequence(
                    data.get("supported_frequencies"), "supported_frequencies"
                )
            ),
            required_output_bases=tuple(
                NationalAccountsOutputBasis.from_value(str(item))
                for item in _sequence(
                    data.get("required_output_bases"), "required_output_bases"
                )
            ),
            required_transformations=tuple(
                NationalAccountsTransformation.from_value(str(item))
                for item in _sequence(
                    data.get("required_transformations"),
                    "required_transformations",
                )
            ),
            supported_stages=tuple(
                EconomicReleaseStage.from_value(str(item))
                for item in _sequence(
                    data.get("supported_stages"), "supported_stages"
                )
            ),
            component_frameworks=tuple(
                NationalAccountsComponentFramework.from_value(str(item))
                for item in _sequence(
                    data.get("component_frameworks"), "component_frameworks"
                )
            ),
            supports_benchmark_revision_sets=cast(
                bool, data.get("supports_benchmark_revision_sets")
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


_UNIVERSAL_SPECIAL_CASES: tuple[str, ...] = (
    "annualized-versus-simple-quarterly-growth",
    "benchmark-vintage-graphs",
    "chain-volume-non-additivity",
    "component-reconciliation",
    "exact-stage-forecast-targets",
    "previous-as-known",
    "real-versus-nominal-output",
    "rebase-and-chain-link-eras",
    "simultaneous-previous-revisions",
    "stage-specific-first-published-actuals",
)
_SPECIAL_CASES_BY_ECONOMY: Mapping[str, tuple[str, ...]] = {
    "CA": ("monthly-and-quarterly-gdp",),
    "EA": ("preliminary-flash-and-final-chain-volume-gdp",),
    "GB": ("monthly-and-quarterly-gdp",),
    "JP": ("first-and-second-preliminary-estimates",),
    "US": (
        "advance-second-and-third-estimate-sequence",
        "nipa-comprehensive-updates",
        "saar-levels-and-annualized-quarterly-growth",
    ),
}
_STAGES_BY_ECONOMY: Mapping[str, tuple[EconomicReleaseStage, ...]] = {
    "EA": (
        EconomicReleaseStage.FLASH,
        EconomicReleaseStage.PRELIMINARY,
        EconomicReleaseStage.FINAL,
        EconomicReleaseStage.REVISION,
    ),
    "JP": (
        EconomicReleaseStage.PRELIMINARY,
        EconomicReleaseStage.SECOND,
        EconomicReleaseStage.FINAL,
        EconomicReleaseStage.REVISION,
    ),
    "US": (
        EconomicReleaseStage.ADVANCE,
        EconomicReleaseStage.SECOND,
        EconomicReleaseStage.FINAL,
        EconomicReleaseStage.REVISION,
    ),
}
_DEFAULT_STAGES: tuple[EconomicReleaseStage, ...] = (
    EconomicReleaseStage.INITIAL,
    EconomicReleaseStage.FINAL,
    EconomicReleaseStage.REVISION,
)
_BASE_TRANSFORMATIONS: tuple[NationalAccountsTransformation, ...] = (
    NationalAccountsTransformation.LEVEL,
    NationalAccountsTransformation.ANNUALIZED_LEVEL,
    NationalAccountsTransformation.INDEX_LEVEL,
    NationalAccountsTransformation.QUARTER_OVER_QUARTER_PERCENT,
    _ANNUALIZED_QOQ,
    NationalAccountsTransformation.YEAR_OVER_YEAR_PERCENT,
    _CONTRIBUTION_PP,
    NationalAccountsTransformation.SHARE_PERCENT,
)


def built_in_national_accounts_profiles(
    registry: OfficialSourceRegistryV1,
) -> tuple[NationalAccountsEconomyProfileV1, ...]:
    """Build legal-producer and semantic profiles for all scoped economies."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    profiles: list[NationalAccountsEconomyProfileV1] = []
    for economy_code in registry.scoped_economies:
        source = registry.primary_source(
            economy_code, EconomicEventFamily.GDP_NATIONAL_ACCOUNTS
        )
        frequencies = (
            (
                NationalAccountsFrequency.MONTHLY,
                NationalAccountsFrequency.QUARTERLY,
                NationalAccountsFrequency.ANNUAL,
            )
            if economy_code in {"CA", "GB"}
            else (
                NationalAccountsFrequency.QUARTERLY,
                NationalAccountsFrequency.ANNUAL,
            )
        )
        transformations = (
            (
                *_BASE_TRANSFORMATIONS,
                NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT,
            )
            if NationalAccountsFrequency.MONTHLY in frequencies
            else _BASE_TRANSFORMATIONS
        )
        profiles.append(
            NationalAccountsEconomyProfileV1(
                economy_code=economy_code,
                institution=source.institution,
                source_key=source.source_key,
                supported_frequencies=frequencies,
                required_output_bases=(
                    NationalAccountsOutputBasis.REAL_CHAIN_VOLUME,
                    NationalAccountsOutputBasis.REAL_FIXED_PRICE,
                    NationalAccountsOutputBasis.NOMINAL_CURRENT_PRICE,
                    NationalAccountsOutputBasis.VOLUME_INDEX,
                ),
                required_transformations=transformations,
                supported_stages=_STAGES_BY_ECONOMY.get(
                    economy_code, _DEFAULT_STAGES
                ),
                component_frameworks=tuple(NationalAccountsComponentFramework),
                supports_benchmark_revision_sets=True,
                special_cases=(
                    *_UNIVERSAL_SPECIAL_CASES,
                    *_SPECIAL_CASES_BY_ECONOMY.get(economy_code, ()),
                ),
                rationale=(
                    "Bind GDP and national accounts to the legal producer and "
                    "retain exact stage, output basis, transformation, "
                    "component, and revision identity. Availability remains "
                    "a separate adapter qualification claim."
                ),
            )
        )
    return tuple(sorted(profiles, key=lambda item: item.economy_code))


@dataclass(frozen=True, slots=True)
class NationalAccountsProfileAuditV1:
    """Coverage proof for GDP profiles, legal owners, and edge cases."""

    registry_id: str
    profile_ids: tuple[str, ...]
    missing_economies: tuple[str, ...]
    duplicate_economies: tuple[str, ...]
    source_mismatches: tuple[str, ...]
    semantic_mismatches: tuple[str, ...]
    complete: bool
    audit_id: str = ""
    schema_version: str = NATIONAL_ACCOUNTS_PROFILE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != NATIONAL_ACCOUNTS_PROFILE_AUDIT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported national-accounts profile-audit schema"
            )
        object.__setattr__(
            self, "registry_id", _required_text(self.registry_id, "registry_id")
        )
        object.__setattr__(
            self,
            "profile_ids",
            _texts(self.profile_ids, "profile_id", maximum=128),
        )
        for name in (
            "missing_economies",
            "duplicate_economies",
            "source_mismatches",
            "semantic_mismatches",
        ):
            object.__setattr__(
                self,
                name,
                _texts(getattr(self, name), name, maximum=512),
            )
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
            raise ValueError("profile audit completeness is inconsistent")
        expected = _stable_id(
            "national-accounts-profile-audit", self.identity_payload()
        )
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> NationalAccountsProfileAuditV1:
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


def audit_national_accounts_profiles(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[NationalAccountsEconomyProfileV1],
) -> NationalAccountsProfileAuditV1:
    """Audit one complete GDP semantic profile and legal owner per economy."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    values = tuple(profiles)
    if any(
        not isinstance(item, NationalAccountsEconomyProfileV1)
        for item in values
    ):
        raise TypeError("profiles must use NationalAccountsEconomyProfileV1")
    counts = {
        code: sum(item.economy_code == code for item in values)
        for code in registry.scoped_economies
    }
    missing = tuple(code for code, count in counts.items() if count == 0)
    duplicates = tuple(code for code, count in counts.items() if count > 1)
    source_mismatches: list[str] = []
    semantic_mismatches: list[str] = []
    required_cases = set(_UNIVERSAL_SPECIAL_CASES)
    for profile in values:
        code = profile.economy_code
        if code not in registry.scoped_economies:
            source_mismatches.append(f"{code}:outside-scope")
            continue
        expected_source = registry.primary_source(
            code, EconomicEventFamily.GDP_NATIONAL_ACCOUNTS
        )
        try:
            source = registry.source(profile.source_key)
        except ValueError:
            source_mismatches.append(code)
        else:
            if (
                profile.source_key != expected_source.source_key
                or source.economy_code != code
                or source.institution != profile.institution
                or OfficialSourceRole.PRIMARY_PRODUCER not in source.roles
            ):
                source_mismatches.append(code)
        expected_cases = required_cases | set(
            _SPECIAL_CASES_BY_ECONOMY.get(code, ())
        )
        expected_stages = set(_STAGES_BY_ECONOMY.get(code, _DEFAULT_STAGES))
        expected_frequencies = {
            NationalAccountsFrequency.QUARTERLY,
            NationalAccountsFrequency.ANNUAL,
        }
        if code in {"CA", "GB"}:
            expected_frequencies.add(NationalAccountsFrequency.MONTHLY)
        if (
            set(profile.supported_frequencies) != expected_frequencies
            or set(profile.supported_stages) != expected_stages
            or set(profile.component_frameworks)
            != set(NationalAccountsComponentFramework)
            or not expected_cases.issubset(profile.special_cases)
            or not profile.supports_benchmark_revision_sets
            or (
                NationalAccountsTransformation.MONTH_OVER_MONTH_PERCENT
                in profile.required_transformations
            )
            != (NationalAccountsFrequency.MONTHLY in expected_frequencies)
        ):
            semantic_mismatches.append(code)
    if len({item.profile_id for item in values}) != len(values):
        duplicates = tuple(sorted({*duplicates, "profile-id"}))
    source_result = tuple(sorted(set(source_mismatches)))
    semantic_result = tuple(sorted(set(semantic_mismatches)))
    return NationalAccountsProfileAuditV1(
        registry_id=registry.registry_id,
        profile_ids=tuple(item.profile_id for item in values),
        missing_economies=missing,
        duplicate_economies=duplicates,
        source_mismatches=source_result,
        semantic_mismatches=semantic_result,
        complete=not any((missing, duplicates, source_result, semantic_result)),
    )


def require_national_accounts_profile_coverage(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[NationalAccountsEconomyProfileV1],
) -> NationalAccountsProfileAuditV1:
    """Return a complete audit or fail closed on any profile mismatch."""
    audit = audit_national_accounts_profiles(registry, profiles)
    if not audit.complete:
        raise ValueError("national-accounts profile coverage is incomplete")
    return audit


__all__ = [
    "MAX_NATIONAL_ACCOUNTS_BENCHMARK_OBSERVATIONS",
    "MAX_NATIONAL_ACCOUNTS_COMPONENTS",
    "MAX_NATIONAL_ACCOUNTS_RELEASE_OBSERVATIONS",
    "MAX_NATIONAL_ACCOUNTS_RELEASE_STAGES",
    "MAX_NATIONAL_ACCOUNTS_REVISIONS",
    "NATIONAL_ACCOUNTS_BENCHMARK_REVISION_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_COMPONENT_ENTRY_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_COMPONENT_RECONCILIATION_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_CONCEPT_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_ECONOMY_PROFILE_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_EXPECTATION_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_GROWTH_DERIVATION_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_METHODOLOGY_EVENT_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_OBSERVATION_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_PROFILE_AUDIT_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_RELEASE_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_RELEASE_SEQUENCE_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_RELEASE_TRIPLET_SCHEMA_VERSION",
    "NATIONAL_ACCOUNTS_VINTAGE_CHAIN_SCHEMA_VERSION",
    "NationalAccountsBenchmarkRevisionV1",
    "NationalAccountsComponentCoverage",
    "NationalAccountsComponentEntryV1",
    "NationalAccountsComponentFramework",
    "NationalAccountsComponentReconciliationV1",
    "NationalAccountsComponentRole",
    "NationalAccountsConceptV1",
    "NationalAccountsDerivationVintageBasis",
    "NationalAccountsEconomyProfileV1",
    "NationalAccountsExpectationKind",
    "NationalAccountsExpectationV1",
    "NationalAccountsFrequency",
    "NationalAccountsGrowthDerivationV1",
    "NationalAccountsMeasure",
    "NationalAccountsMethodologyChangeKind",
    "NationalAccountsMethodologyEventV1",
    "NationalAccountsObservationBasis",
    "NationalAccountsObservationV1",
    "NationalAccountsOutputBasis",
    "NationalAccountsProfileAuditV1",
    "NationalAccountsReconciliationBasis",
    "NationalAccountsReleaseSequenceV1",
    "NationalAccountsReleaseTripletV1",
    "NationalAccountsReleaseV1",
    "NationalAccountsRevisionKind",
    "NationalAccountsSeasonalBasis",
    "NationalAccountsTransformation",
    "NationalAccountsVintageChainV1",
    "audit_national_accounts_profiles",
    "built_in_national_accounts_profiles",
    "require_national_accounts_profile_coverage",
]
