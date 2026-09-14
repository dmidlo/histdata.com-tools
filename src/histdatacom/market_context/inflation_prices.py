"""Provider-neutral inflation, price-index, and component semantics.

The contracts in this module keep the published index concept separate from
its transformation, vintage, release stage, seasonal basis, and component
path.  Derived rates retain the exact index vintages used in their formula;
an ex-post current database can therefore never masquerade as an historical
initial-release actual.
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

INFLATION_CONCEPT_SCHEMA_VERSION = "histdatacom.inflation-concept.v1"
INFLATION_OBSERVATION_SCHEMA_VERSION = "histdatacom.inflation-observation.v1"
INFLATION_VINTAGE_CHAIN_SCHEMA_VERSION = (
    "histdatacom.inflation-vintage-chain.v1"
)
INFLATION_RATE_DERIVATION_SCHEMA_VERSION = (
    "histdatacom.inflation-rate-derivation.v1"
)
INFLATION_EXPECTATION_SCHEMA_VERSION = "histdatacom.inflation-expectation.v1"
INFLATION_RELEASE_TRIPLET_SCHEMA_VERSION = (
    "histdatacom.inflation-release-triplet.v1"
)
INFLATION_COMPONENT_CONTRIBUTION_SCHEMA_VERSION = (
    "histdatacom.inflation-component-contribution.v1"
)
INFLATION_COMPONENT_AGGREGATION_SCHEMA_VERSION = (
    "histdatacom.inflation-component-aggregation.v1"
)
INFLATION_RELEASE_SCHEMA_VERSION = "histdatacom.inflation-release.v1"
INFLATION_RELEASE_SEQUENCE_SCHEMA_VERSION = (
    "histdatacom.inflation-release-sequence.v1"
)
INFLATION_METHODOLOGY_EVENT_SCHEMA_VERSION = (
    "histdatacom.inflation-methodology-event.v1"
)
INFLATION_SOURCE_OWNER_SCHEMA_VERSION = "histdatacom.inflation-source-owner.v1"
INFLATION_ECONOMY_PROFILE_SCHEMA_VERSION = (
    "histdatacom.inflation-economy-profile.v1"
)
INFLATION_PROFILE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.inflation-profile-audit.v1"
)

MAX_INFLATION_REVISIONS = 10_000
MAX_INFLATION_RELEASE_OBSERVATIONS = 10_000
MAX_INFLATION_COMPONENTS = 1_000
MAX_INFLATION_RELEASE_STAGES = 16

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class InflationIndexFamily(str, Enum):
    """Official price-index or deflator family."""

    CPI = "cpi"
    HICP = "hicp"
    PPI = "ppi"
    PCE_DEFLATOR = "pce-deflator"
    GDP_DEFLATOR = "gdp-deflator"
    RETAIL_PRICE_INDEX = "retail-price-index"
    IMPORT_EXPORT_PRICE = "import-export-price"
    OTHER_OFFICIAL_PRICE_INDEX = "other-official-price-index"

    @classmethod
    def from_value(
        cls, value: str | InflationIndexFamily
    ) -> InflationIndexFamily:
        return _enum_value(cls, value, "inflation index family")


class InflationVariant(str, Enum):
    """Displayed aggregate or component identity."""

    HEADLINE = "headline"
    CORE = "core"
    TRIMMED_MEAN = "trimmed-mean"
    MEDIAN = "median"
    GOODS = "goods"
    SERVICES = "services"
    FOOD = "food"
    ENERGY = "energy"
    SHELTER = "shelter"
    OWNER_EQUIVALENT_RENT = "owner-equivalent-rent"
    PRODUCER_INPUT = "producer-input"
    PRODUCER_OUTPUT = "producer-output"
    COMPONENT = "component"
    CUSTOM_AGGREGATE = "custom-aggregate"

    @classmethod
    def from_value(cls, value: str | InflationVariant) -> InflationVariant:
        return _enum_value(cls, value, "inflation variant")


class InflationTransformation(str, Enum):
    """Exact transformation represented by an observation."""

    INDEX_LEVEL = "index-level"
    MONTH_OVER_MONTH_PERCENT = "month-over-month-percent"
    QUARTER_OVER_QUARTER_PERCENT = "quarter-over-quarter-percent"
    YEAR_OVER_YEAR_PERCENT = "year-over-year-percent"
    ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT = (
        "annualized-quarter-over-quarter-percent"
    )
    ABSOLUTE_INDEX_CHANGE = "absolute-index-change"
    CONTRIBUTION_PERCENTAGE_POINTS = "contribution-percentage-points"
    WEIGHT_PERCENT = "weight-percent"

    @classmethod
    def from_value(
        cls, value: str | InflationTransformation
    ) -> InflationTransformation:
        return _enum_value(cls, value, "inflation transformation")

    @property
    def derivable_from_index(self) -> bool:
        """Return whether two index levels determine this transformation."""
        return self in {
            InflationTransformation.MONTH_OVER_MONTH_PERCENT,
            InflationTransformation.QUARTER_OVER_QUARTER_PERCENT,
            InflationTransformation.YEAR_OVER_YEAR_PERCENT,
            InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT,
            InflationTransformation.ABSOLUTE_INDEX_CHANGE,
        }


class InflationFrequency(str, Enum):
    """Reference-period frequency."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"

    @classmethod
    def from_value(cls, value: str | InflationFrequency) -> InflationFrequency:
        return _enum_value(cls, value, "inflation frequency")


class InflationSeasonalBasis(str, Enum):
    """Seasonal/calendar adjustment applied by the publisher."""

    NOT_SEASONALLY_ADJUSTED = "not-seasonally-adjusted"
    SEASONALLY_ADJUSTED = "seasonally-adjusted"
    SEASONALLY_AND_CALENDAR_ADJUSTED = "seasonally-and-calendar-adjusted"
    CALENDAR_ADJUSTED = "calendar-adjusted"
    TREND_CYCLE = "trend-cycle"
    NOT_APPLICABLE = "not-applicable"

    @classmethod
    def from_value(
        cls, value: str | InflationSeasonalBasis
    ) -> InflationSeasonalBasis:
        return _enum_value(cls, value, "inflation seasonal basis")


class InflationObservationBasis(str, Enum):
    """Whether the value was published or reconstructed from index levels."""

    SOURCE_PUBLISHED = "source-published"
    DERIVED_FROM_INDEX_VINTAGES = "derived-from-index-vintages"

    @classmethod
    def from_value(
        cls, value: str | InflationObservationBasis
    ) -> InflationObservationBasis:
        return _enum_value(cls, value, "inflation observation basis")


class InflationDerivationVintageBasis(str, Enum):
    """Vintage policy used for both inputs to a derived rate."""

    INITIAL_RELEASE = "initial-release"
    AS_KNOWN_AT_RELEASE = "as-known-at-release"
    LATEST_CURRENT = "latest-current"

    @classmethod
    def from_value(
        cls, value: str | InflationDerivationVintageBasis
    ) -> InflationDerivationVintageBasis:
        return _enum_value(cls, value, "inflation derivation vintage basis")


class InflationExpectationKind(str, Enum):
    """Provenance and target scope of an inflation expectation."""

    EVENT_CONSENSUS = "event-consensus"
    OFFICIAL_SURVEY_EVENT_TARGET = "official-survey-event-target"
    OFFICIAL_SURVEY_PERIOD_TARGET = "official-survey-period-target"
    CENTRAL_BANK_PROJECTION = "central-bank-projection"
    GOVERNMENT_PROJECTION = "government-projection"
    MACHINE_EVENT_TARGET = "machine-event-target"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | InflationExpectationKind
    ) -> InflationExpectationKind:
        return _enum_value(cls, value, "inflation expectation kind")

    @property
    def calendar_style_eligible(self) -> bool:
        """Return whether this expectation targets one exact release."""
        return self in {
            InflationExpectationKind.EVENT_CONSENSUS,
            InflationExpectationKind.OFFICIAL_SURVEY_EVENT_TARGET,
            InflationExpectationKind.MACHINE_EVENT_TARGET,
        }


class InflationContributionBasis(str, Enum):
    """How a component contribution was obtained."""

    COMPUTED_FROM_WEIGHT = "computed-from-weight"
    SOURCE_PUBLISHED = "source-published"

    @classmethod
    def from_value(
        cls, value: str | InflationContributionBasis
    ) -> InflationContributionBasis:
        return _enum_value(cls, value, "inflation contribution basis")


class InflationComponentCoverage(str, Enum):
    """Completeness of one retained component table."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | InflationComponentCoverage
    ) -> InflationComponentCoverage:
        return _enum_value(cls, value, "inflation component coverage")


class InflationMethodologyChangeKind(str, Enum):
    """Methodological change that can alter comparability or revisions."""

    REBASE = "rebase"
    REWEIGHT = "reweight"
    SEASONAL_REANALYSIS = "seasonal-reanalysis"
    CLASSIFICATION_CHANGE = "classification-change"
    COVERAGE_CHANGE = "coverage-change"
    TAX_TREATMENT_CHANGE = "tax-treatment-change"
    OWNER_OCCUPIED_HOUSING_CHANGE = "owner-occupied-housing-change"

    @classmethod
    def from_value(
        cls, value: str | InflationMethodologyChangeKind
    ) -> InflationMethodologyChangeKind:
        return _enum_value(cls, value, "inflation methodology change kind")


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
    if len(result) > MAX_INFLATION_COMPONENTS:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _keys(
    values: Iterable[object], label: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > MAX_INFLATION_COMPONENTS:
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
class InflationConceptV1:
    """One exact displayed price concept and transformation."""

    economy_code: str
    indicator_key: str
    source_series_id: str
    index_family: InflationIndexFamily
    variant: InflationVariant
    transformation: InflationTransformation
    frequency: InflationFrequency
    seasonal_basis: InflationSeasonalBasis
    unit: str
    scale: float
    component_path: tuple[str, ...]
    excluded_components: tuple[str, ...]
    coverage: str
    price_basis: str
    index_base_period: str | None
    methodology_era_id: str
    concept_id: str = ""
    schema_version: str = INFLATION_CONCEPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_CONCEPT_SCHEMA_VERSION:
            raise ValueError("unsupported inflation concept schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        for name in (
            "indicator_key",
            "source_series_id",
            "methodology_era_id",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        family = InflationIndexFamily.from_value(self.index_family)
        variant = InflationVariant.from_value(self.variant)
        transformation = InflationTransformation.from_value(self.transformation)
        frequency = InflationFrequency.from_value(self.frequency)
        seasonal = InflationSeasonalBasis.from_value(self.seasonal_basis)
        unit = _required_text(self.unit, "unit")
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        path = _keys(self.component_path, "component_path")
        excluded = _keys(self.excluded_components, "excluded_components")
        if variant is InflationVariant.COMPONENT and not path:
            raise ValueError("component variant requires a component path")
        if variant is InflationVariant.CORE and not excluded:
            raise ValueError("core variant requires explicit exclusions")
        if variant is InflationVariant.HEADLINE and (path or excluded):
            raise ValueError(
                "headline concept cannot be a component or exclusion"
            )
        if (
            transformation
            in {
                InflationTransformation.CONTRIBUTION_PERCENTAGE_POINTS,
                InflationTransformation.WEIGHT_PERCENT,
            }
            and not path
        ):
            raise ValueError(
                "component transformation requires a component path"
            )
        base = _optional_text(self.index_base_period)
        if (
            transformation is InflationTransformation.INDEX_LEVEL
            and base is None
        ):
            raise ValueError("index level requires an index base period")
        object.__setattr__(self, "index_family", family)
        object.__setattr__(self, "variant", variant)
        object.__setattr__(self, "transformation", transformation)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "seasonal_basis", seasonal)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "scale", scale)
        object.__setattr__(self, "component_path", path)
        object.__setattr__(self, "excluded_components", excluded)
        object.__setattr__(
            self, "coverage", _required_text(self.coverage, "coverage")
        )
        object.__setattr__(
            self,
            "price_basis",
            _required_text(self.price_basis, "price_basis"),
        )
        object.__setattr__(self, "index_base_period", base)
        expected = _stable_id("inflation-concept", self.identity_payload())
        supplied = _optional_text(self.concept_id)
        if supplied is not None and supplied != expected:
            raise ValueError("concept_id differs from deterministic identity")
        object.__setattr__(self, "concept_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the lossless concept identity."""
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "indicator_key": self.indicator_key,
            "source_series_id": self.source_series_id,
            "index_family": self.index_family.value,
            "variant": self.variant.value,
            "transformation": self.transformation.value,
            "frequency": self.frequency.value,
            "seasonal_basis": self.seasonal_basis.value,
            "unit": self.unit,
            "scale": self.scale,
            "component_path": list(self.component_path),
            "excluded_components": list(self.excluded_components),
            "coverage": self.coverage,
            "price_basis": self.price_basis,
            "index_base_period": self.index_base_period,
            "methodology_era_id": self.methodology_era_id,
        }

    def index_series_payload(self) -> dict[str, JSONValue]:
        """Return dimensions that must agree for index-rate derivation."""
        payload = self.identity_payload()
        payload.pop("schema_version")
        payload.pop("transformation")
        payload.pop("unit")
        payload.pop("scale")
        return payload

    @property
    def index_series_id(self) -> str:
        return _stable_id("inflation-index-series", self.index_series_payload())

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "concept_id": self.concept_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationConceptV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            indicator_key=str(data.get("indicator_key", "")),
            source_series_id=str(data.get("source_series_id", "")),
            index_family=InflationIndexFamily.from_value(
                str(data.get("index_family", ""))
            ),
            variant=InflationVariant.from_value(str(data.get("variant", ""))),
            transformation=InflationTransformation.from_value(
                str(data.get("transformation", ""))
            ),
            frequency=InflationFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            seasonal_basis=InflationSeasonalBasis.from_value(
                str(data.get("seasonal_basis", ""))
            ),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            component_path=tuple(
                str(item)
                for item in _sequence(
                    data.get("component_path"), "component_path"
                )
            ),
            excluded_components=tuple(
                str(item)
                for item in _sequence(
                    data.get("excluded_components"), "excluded_components"
                )
            ),
            coverage=str(data.get("coverage", "")),
            price_basis=str(data.get("price_basis", "")),
            index_base_period=_optional_text(data.get("index_base_period")),
            methodology_era_id=str(data.get("methodology_era_id", "")),
            concept_id=str(data.get("concept_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> InflationConceptV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("inflation concept is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class InflationObservationV1:
    """One immutable published or index-derived value vintage."""

    concept: InflationConceptV1
    logical_event_key: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    basis: InflationObservationBasis
    value: float
    raw_lexical: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    revision_sequence: int
    supersedes_observation_id: str | None
    derived_from_observation_ids: tuple[str, ...]
    derivation_id: str | None
    limitations: tuple[str, ...]
    observation_id: str = ""
    schema_version: str = INFLATION_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported inflation observation schema")
        if not isinstance(self.concept, InflationConceptV1):
            raise TypeError("observation requires an inflation concept")
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
        basis = InflationObservationBasis.from_value(self.basis)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "value", _finite(self.value, "value"))
        object.__setattr__(
            self,
            "raw_lexical",
            _required_text(self.raw_lexical, "raw_lexical"),
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
        if revision > MAX_INFLATION_REVISIONS:
            raise ValueError("revision_sequence exceeds bound")
        supersedes = _optional_text(self.supersedes_observation_id)
        if revision == 0 and supersedes is not None:
            raise ValueError("initial observation cannot supersede another")
        if revision > 0 and supersedes is None:
            raise ValueError("revision must identify the prior observation")
        derived_ids = _texts(
            self.derived_from_observation_ids, "derived observation id"
        )
        derivation_id = _optional_text(self.derivation_id)
        if basis is InflationObservationBasis.SOURCE_PUBLISHED:
            if derived_ids or derivation_id is not None:
                raise ValueError(
                    "source-published value contains derivation links"
                )
        elif len(derived_ids) != 2 or derivation_id is None:
            raise ValueError("derived value requires exactly two index inputs")
        object.__setattr__(self, "revision_sequence", revision)
        object.__setattr__(self, "supersedes_observation_id", supersedes)
        object.__setattr__(self, "derived_from_observation_ids", derived_ids)
        object.__setattr__(self, "derivation_id", derivation_id)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("inflation-observation", self.identity_payload())
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "observation_id differs from deterministic identity"
            )
        object.__setattr__(self, "observation_id", expected)

    @property
    def initial_release_actual(self) -> bool:
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationObservationV1:
        return cls(
            concept=InflationConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            logical_event_key=str(data.get("logical_event_key", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            basis=InflationObservationBasis.from_value(
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
class InflationVintageChainV1:
    """Initial value and every later revision without overwrite."""

    observations: tuple[InflationObservationV1, ...]
    chain_id: str = ""
    schema_version: str = INFLATION_VINTAGE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_VINTAGE_CHAIN_SCHEMA_VERSION:
            raise ValueError("unsupported inflation vintage-chain schema")
        observations = tuple(
            sorted(self.observations, key=lambda item: item.revision_sequence)
        )
        if not observations or len(observations) > MAX_INFLATION_REVISIONS + 1:
            raise ValueError("inflation vintage-chain size is outside bounds")
        if any(
            not isinstance(item, InflationObservationV1)
            for item in observations
        ):
            raise TypeError("vintage chain contains a non-v1 observation")
        first = observations[0]
        if first.revision_sequence != 0:
            raise ValueError("vintage chain must begin with an initial value")
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
        expected = _stable_id(
            "inflation-vintage-chain", self.identity_payload()
        )
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id differs from deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    @property
    def initial(self) -> InflationObservationV1:
        return self.observations[0]

    def as_known_at(self, decision_at_ns: int) -> InflationObservationV1 | None:
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationVintageChainV1:
        return cls(
            observations=tuple(
                InflationObservationV1.from_dict(_mapping(item, "observation"))
                for item in _sequence(data.get("observations"), "observations")
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _required_lag(
    transformation: InflationTransformation, frequency: InflationFrequency
) -> int:
    if transformation is InflationTransformation.MONTH_OVER_MONTH_PERCENT:
        if frequency is not InflationFrequency.MONTHLY:
            raise ValueError(
                "month-over-month transformation requires monthly data"
            )
        return 1
    if transformation in {
        InflationTransformation.QUARTER_OVER_QUARTER_PERCENT,
        InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT,
    }:
        if frequency is not InflationFrequency.QUARTERLY:
            raise ValueError(
                "quarter-over-quarter transformation requires quarterly data"
            )
        return 3
    if transformation is InflationTransformation.YEAR_OVER_YEAR_PERCENT:
        return {
            InflationFrequency.MONTHLY: 12,
            InflationFrequency.QUARTERLY: 12,
            InflationFrequency.ANNUAL: 12,
        }[frequency]
    if transformation is InflationTransformation.ABSOLUTE_INDEX_CHANGE:
        return {
            InflationFrequency.MONTHLY: 1,
            InflationFrequency.QUARTERLY: 3,
            InflationFrequency.ANNUAL: 12,
        }[frequency]
    raise ValueError("transformation cannot be derived from two index levels")


@dataclass(frozen=True, slots=True)
class InflationRateDerivationV1:
    """Auditable rate derived from two exact index vintages."""

    transformation: InflationTransformation
    current_index: InflationObservationV1
    comparison_index: InflationObservationV1
    vintage_basis: InflationDerivationVintageBasis
    claimed_initial_actual: bool
    derived_value: float
    tolerance: float
    derivation_id: str = ""
    schema_version: str = INFLATION_RATE_DERIVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_RATE_DERIVATION_SCHEMA_VERSION:
            raise ValueError("unsupported inflation rate-derivation schema")
        transformation = InflationTransformation.from_value(self.transformation)
        if not transformation.derivable_from_index:
            raise ValueError("transformation is not index-derivable")
        if not isinstance(
            self.current_index, InflationObservationV1
        ) or not isinstance(self.comparison_index, InflationObservationV1):
            raise TypeError("derivation inputs must use inflation observations")
        if any(
            item.concept.transformation
            is not InflationTransformation.INDEX_LEVEL
            for item in (self.current_index, self.comparison_index)
        ):
            raise ValueError("rate derivation requires retained index levels")
        if (
            self.current_index.concept.index_series_id
            != self.comparison_index.concept.index_series_id
        ):
            raise ValueError("rate derivation changes the index concept")
        if (
            self.current_index.reference_period_end_ns
            <= self.comparison_index.reference_period_end_ns
        ):
            raise ValueError("comparison index does not precede current index")
        lag = _month_index(
            self.current_index.reference_period_end_ns
        ) - _month_index(self.comparison_index.reference_period_end_ns)
        required_lag = _required_lag(
            transformation, self.current_index.concept.frequency
        )
        if lag != required_lag:
            raise ValueError(
                "index observations do not match transformation horizon"
            )
        vintage_basis = InflationDerivationVintageBasis.from_value(
            self.vintage_basis
        )
        if not isinstance(self.claimed_initial_actual, bool):
            raise TypeError("claimed_initial_actual must be boolean")
        if self.claimed_initial_actual and (
            vintage_basis is not InflationDerivationVintageBasis.INITIAL_RELEASE
            or self.current_index.revision_sequence != 0
            or self.comparison_index.revision_sequence != 0
        ):
            raise ValueError(
                "historical initial actual requires both initial index vintages"
            )
        expected = self._calculate(transformation)
        supplied = _finite(self.derived_value, "derived_value")
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        if not math.isclose(supplied, expected, rel_tol=0.0, abs_tol=tolerance):
            raise ValueError(
                "derived value differs from retained index formula"
            )
        object.__setattr__(self, "transformation", transformation)
        object.__setattr__(self, "vintage_basis", vintage_basis)
        object.__setattr__(self, "derived_value", supplied)
        object.__setattr__(self, "tolerance", tolerance)
        identity = _stable_id(
            "inflation-rate-derivation", self.identity_payload()
        )
        supplied_id = _optional_text(self.derivation_id)
        if supplied_id is not None and supplied_id != identity:
            raise ValueError(
                "derivation_id differs from deterministic identity"
            )
        object.__setattr__(self, "derivation_id", identity)

    def _calculate(self, transformation: InflationTransformation) -> float:
        current = self.current_index.value
        comparison = self.comparison_index.value
        if transformation is InflationTransformation.ABSOLUTE_INDEX_CHANGE:
            return current - comparison
        if comparison == 0:
            raise ValueError("comparison index cannot be zero for a rate")
        ratio = current / comparison
        if (
            transformation
            is InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
        ):
            return 100.0 * (ratio**4 - 1.0)
        return 100.0 * (ratio - 1.0)

    def identity_payload(self) -> dict[str, JSONValue]:
        annualized_qoq = (
            InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT
        )
        return {
            "schema_version": self.schema_version,
            "transformation": self.transformation.value,
            "current_index": self.current_index.to_dict(),
            "comparison_index": self.comparison_index.to_dict(),
            "vintage_basis": self.vintage_basis.value,
            "claimed_initial_actual": self.claimed_initial_actual,
            "derived_value": self.derived_value,
            "tolerance": self.tolerance,
            "formula": (
                "I_t-I_lag"
                if self.transformation
                is InflationTransformation.ABSOLUTE_INDEX_CHANGE
                else (
                    "100*((I_t/I_t-1)^4-1)"
                    if self.transformation is annualized_qoq
                    else "100*(I_t/I_lag-1)"
                )
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "derivation_id": self.derivation_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationRateDerivationV1:
        return cls(
            transformation=InflationTransformation.from_value(
                str(data.get("transformation", ""))
            ),
            current_index=InflationObservationV1.from_dict(
                _mapping(data.get("current_index"), "current_index")
            ),
            comparison_index=InflationObservationV1.from_dict(
                _mapping(data.get("comparison_index"), "comparison_index")
            ),
            vintage_basis=InflationDerivationVintageBasis.from_value(
                str(data.get("vintage_basis", ""))
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
class InflationExpectationV1:
    """Pre-release expectation for one exact inflation concept and event."""

    target_event_key: str
    concept: InflationConceptV1
    reference_period: str
    kind: InflationExpectationKind
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
    schema_version: str = INFLATION_EXPECTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_EXPECTATION_SCHEMA_VERSION:
            raise ValueError("unsupported inflation expectation schema")
        object.__setattr__(
            self,
            "target_event_key",
            _key(self.target_event_key, "target_event_key"),
        )
        if not isinstance(self.concept, InflationConceptV1):
            raise TypeError("expectation requires an inflation concept")
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        kind = InflationExpectationKind.from_value(self.kind)
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
        if kind is InflationExpectationKind.UNAVAILABLE:
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
        expected = _stable_id("inflation-expectation", self.identity_payload())
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationExpectationV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            concept=InflationConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            reference_period=str(data.get("reference_period", "")),
            kind=InflationExpectationKind.from_value(str(data.get("kind", ""))),
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
class InflationReleaseTripletV1:
    """Initial actual, previous-as-known, and exact pre-release forecast."""

    target_event_key: str
    actual_initial: InflationObservationV1
    previous_as_known: InflationObservationV1
    previous_as_known_at_ns: int
    expectation: InflationExpectationV1
    triplet_id: str = ""
    schema_version: str = INFLATION_RELEASE_TRIPLET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_RELEASE_TRIPLET_SCHEMA_VERSION:
            raise ValueError("unsupported inflation triplet schema")
        target = _key(self.target_event_key, "target_event_key")
        if not isinstance(
            self.actual_initial, InflationObservationV1
        ) or not isinstance(self.previous_as_known, InflationObservationV1):
            raise TypeError("triplet actuals must use inflation observations")
        if not isinstance(self.expectation, InflationExpectationV1):
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
            raise ValueError(
                "previous field silently changes inflation concept"
            )
        if previous.reference_period_end_ns >= actual.reference_period_end_ns:
            raise ValueError(
                "previous field does not precede the actual period"
            )
        previous_lag = _month_index(
            actual.reference_period_end_ns
        ) - _month_index(previous.reference_period_end_ns)
        expected_previous_lag = {
            InflationFrequency.MONTHLY: 1,
            InflationFrequency.QUARTERLY: 3,
            InflationFrequency.ANNUAL: 12,
        }[actual.concept.frequency]
        if previous_lag != expected_previous_lag:
            raise ValueError(
                "previous field is not the immediately preceding period"
            )
        cutoff = _bounded_ns(
            self.previous_as_known_at_ns, "previous_as_known_at_ns"
        )
        if (
            previous.available_at_ns > cutoff
            or cutoff >= actual.published_at_ns
        ):
            raise ValueError("previous-as-known evidence is not pre-release")
        expectation = self.expectation
        if (
            expectation.target_event_key != target
            or expectation.concept.concept_id != actual.concept.concept_id
            or expectation.reference_period != actual.reference_period
        ):
            raise ValueError(
                "forecast does not match the exact released concept"
            )
        if expectation.kind is not InflationExpectationKind.UNAVAILABLE:
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
        expected = _stable_id(
            "inflation-release-triplet", self.identity_payload()
        )
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationReleaseTripletV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            actual_initial=InflationObservationV1.from_dict(
                _mapping(data.get("actual_initial"), "actual_initial")
            ),
            previous_as_known=InflationObservationV1.from_dict(
                _mapping(data.get("previous_as_known"), "previous_as_known")
            ),
            previous_as_known_at_ns=cast(
                int, data.get("previous_as_known_at_ns")
            ),
            expectation=InflationExpectationV1.from_dict(
                _mapping(data.get("expectation"), "expectation")
            ),
            triplet_id=str(data.get("triplet_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InflationComponentContributionV1:
    """One component observation, weight vintage, and contribution."""

    observation: InflationObservationV1
    weight_percent: float
    contribution_percentage_points: float
    contribution_basis: InflationContributionBasis
    weight_vintage_id: str
    contribution_raw_lexical: str
    tolerance: float
    contribution_id: str = ""
    schema_version: str = INFLATION_COMPONENT_CONTRIBUTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INFLATION_COMPONENT_CONTRIBUTION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported inflation component-contribution schema"
            )
        if not isinstance(self.observation, InflationObservationV1):
            raise TypeError("component contribution requires an observation")
        concept = self.observation.concept
        if (
            not concept.component_path
            or concept.variant is InflationVariant.HEADLINE
        ):
            raise ValueError(
                "component contribution requires component identity"
            )
        if concept.transformation in {
            InflationTransformation.INDEX_LEVEL,
            InflationTransformation.WEIGHT_PERCENT,
            InflationTransformation.CONTRIBUTION_PERCENTAGE_POINTS,
        }:
            raise ValueError("component contribution requires a component rate")
        weight = _finite(self.weight_percent, "weight_percent")
        if not 0 <= weight <= 100:
            raise ValueError("component weight must be between zero and 100")
        contribution = _finite(
            self.contribution_percentage_points,
            "contribution_percentage_points",
        )
        basis = InflationContributionBasis.from_value(self.contribution_basis)
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        if (
            basis is InflationContributionBasis.COMPUTED_FROM_WEIGHT
            and not math.isclose(
                contribution,
                weight * self.observation.value / 100.0,
                rel_tol=0.0,
                abs_tol=tolerance,
            )
        ):
            raise ValueError(
                "computed component contribution does not reconcile"
            )
        object.__setattr__(self, "weight_percent", weight)
        object.__setattr__(self, "contribution_percentage_points", contribution)
        object.__setattr__(self, "contribution_basis", basis)
        object.__setattr__(
            self,
            "weight_vintage_id",
            _key(self.weight_vintage_id, "weight_vintage_id"),
        )
        object.__setattr__(
            self,
            "contribution_raw_lexical",
            _required_text(
                self.contribution_raw_lexical, "contribution_raw_lexical"
            ),
        )
        object.__setattr__(self, "tolerance", tolerance)
        expected = _stable_id(
            "inflation-component-contribution", self.identity_payload()
        )
        supplied = _optional_text(self.contribution_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "contribution_id differs from deterministic identity"
            )
        object.__setattr__(self, "contribution_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "observation": self.observation.to_dict(),
            "weight_percent": self.weight_percent,
            "contribution_percentage_points": (
                self.contribution_percentage_points
            ),
            "contribution_basis": self.contribution_basis.value,
            "weight_vintage_id": self.weight_vintage_id,
            "contribution_raw_lexical": self.contribution_raw_lexical,
            "tolerance": self.tolerance,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "contribution_id": self.contribution_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InflationComponentContributionV1:
        return cls(
            observation=InflationObservationV1.from_dict(
                _mapping(data.get("observation"), "observation")
            ),
            weight_percent=cast(float, data.get("weight_percent")),
            contribution_percentage_points=cast(
                float, data.get("contribution_percentage_points")
            ),
            contribution_basis=InflationContributionBasis.from_value(
                str(data.get("contribution_basis", ""))
            ),
            weight_vintage_id=str(data.get("weight_vintage_id", "")),
            contribution_raw_lexical=str(
                data.get("contribution_raw_lexical", "")
            ),
            tolerance=cast(float, data.get("tolerance")),
            contribution_id=str(data.get("contribution_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InflationComponentAggregationV1:
    """Reconciliation of a headline aggregate to retained components."""

    aggregate: InflationObservationV1
    components: tuple[InflationComponentContributionV1, ...]
    coverage: InflationComponentCoverage
    missing_component_keys: tuple[str, ...]
    residual_percentage_points: float | None
    tolerance: float
    reconciled: bool
    aggregation_id: str = ""
    schema_version: str = INFLATION_COMPONENT_AGGREGATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INFLATION_COMPONENT_AGGREGATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported inflation component-aggregation schema"
            )
        if not isinstance(self.aggregate, InflationObservationV1):
            raise TypeError(
                "component aggregation requires an aggregate observation"
            )
        if self.aggregate.concept.component_path:
            raise ValueError(
                "aggregate observation cannot itself be a component"
            )
        components = tuple(
            sorted(
                self.components,
                key=lambda item: item.observation.concept.concept_id,
            )
        )
        if len(components) > MAX_INFLATION_COMPONENTS or any(
            not isinstance(item, InflationComponentContributionV1)
            for item in components
        ):
            raise ValueError("component contribution set is invalid")
        if len(
            {item.observation.concept.concept_id for item in components}
        ) != len(components):
            raise ValueError("component aggregation repeats a concept")
        if len({item.weight_vintage_id for item in components}) > 1:
            raise ValueError("component aggregation mixes weight vintages")
        aggregate = self.aggregate
        for component in components:
            value = component.observation
            if (
                value.reference_period != aggregate.reference_period
                or value.reference_period_end_ns
                != aggregate.reference_period_end_ns
                or value.stage is not aggregate.stage
                or value.logical_event_key != aggregate.logical_event_key
            ):
                raise ValueError(
                    "component belongs to another release period or stage"
                )
            left = aggregate.concept
            right = value.concept
            if (
                left.economy_code,
                left.index_family,
                left.transformation,
                left.frequency,
                left.seasonal_basis,
                left.methodology_era_id,
            ) != (
                right.economy_code,
                right.index_family,
                right.transformation,
                right.frequency,
                right.seasonal_basis,
                right.methodology_era_id,
            ):
                raise ValueError(
                    "component silently changes aggregation concept"
                )
        coverage = InflationComponentCoverage.from_value(self.coverage)
        missing = _keys(self.missing_component_keys, "missing_component_key")
        residual = _optional_finite(
            self.residual_percentage_points, "residual_percentage_points"
        )
        tolerance = _finite(self.tolerance, "tolerance")
        if tolerance < 0:
            raise ValueError("tolerance must be nonnegative")
        if not isinstance(self.reconciled, bool):
            raise TypeError("reconciled must be boolean")
        if coverage is InflationComponentCoverage.COMPLETE:
            if not components or missing or residual is None:
                raise ValueError(
                    "complete component table lacks retained evidence"
                )
            if not math.isclose(
                sum(item.weight_percent for item in components),
                100.0,
                rel_tol=0.0,
                abs_tol=tolerance,
            ):
                raise ValueError("complete component weights do not sum to 100")
            reconstructed = residual + sum(
                item.contribution_percentage_points for item in components
            )
            expected_reconciled = math.isclose(
                reconstructed,
                aggregate.value,
                rel_tol=0.0,
                abs_tol=tolerance,
            )
            if not self.reconciled or not expected_reconciled:
                raise ValueError("complete component table does not reconcile")
        elif coverage is InflationComponentCoverage.PARTIAL:
            if not components or not missing or self.reconciled:
                raise ValueError("partial component table must expose its gaps")
        elif (
            components or not missing or residual is not None or self.reconciled
        ):
            raise ValueError(
                "unavailable component table contains derived evidence"
            )
        object.__setattr__(self, "components", components)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "missing_component_keys", missing)
        object.__setattr__(self, "residual_percentage_points", residual)
        object.__setattr__(self, "tolerance", tolerance)
        expected = _stable_id(
            "inflation-component-aggregation", self.identity_payload()
        )
        supplied = _optional_text(self.aggregation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "aggregation_id differs from deterministic identity"
            )
        object.__setattr__(self, "aggregation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "aggregate": self.aggregate.to_dict(),
            "components": [item.to_dict() for item in self.components],
            "coverage": self.coverage.value,
            "missing_component_keys": list(self.missing_component_keys),
            "residual_percentage_points": self.residual_percentage_points,
            "tolerance": self.tolerance,
            "reconciled": self.reconciled,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "aggregation_id": self.aggregation_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InflationComponentAggregationV1:
        return cls(
            aggregate=InflationObservationV1.from_dict(
                _mapping(data.get("aggregate"), "aggregate")
            ),
            components=tuple(
                InflationComponentContributionV1.from_dict(
                    _mapping(item, "component")
                )
                for item in _sequence(data.get("components"), "components")
            ),
            coverage=InflationComponentCoverage.from_value(
                str(data.get("coverage", ""))
            ),
            missing_component_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("missing_component_keys"),
                    "missing_component_keys",
                )
            ),
            residual_percentage_points=cast(
                float | None, data.get("residual_percentage_points")
            ),
            tolerance=cast(float, data.get("tolerance")),
            reconciled=cast(bool, data.get("reconciled")),
            aggregation_id=str(data.get("aggregation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InflationReleaseV1:
    """One release stage containing its exact aggregate/component concepts."""

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
    observations: tuple[InflationObservationV1, ...]
    display_observation_id: str
    limitations: tuple[str, ...]
    release_id: str = ""
    schema_version: str = INFLATION_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported inflation release schema")
        event_key = _key(self.logical_event_key, "logical_event_key")
        economy = _economy(self.economy_code)
        reference_period = _required_text(
            self.reference_period, "reference_period"
        )
        reference_end = _bounded_ns(
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
            or len(observations) > MAX_INFLATION_RELEASE_OBSERVATIONS
        ):
            raise ValueError("release observation count is outside bounds")
        if any(
            not isinstance(item, InflationObservationV1)
            for item in observations
        ):
            raise TypeError("release contains a non-v1 observation")
        if len({item.concept.concept_id for item in observations}) != len(
            observations
        ):
            raise ValueError("release repeats an inflation concept")
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
                reference_period,
                reference_end,
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
            raise ValueError(
                "display observation is not retained in the release"
            )
        object.__setattr__(self, "logical_event_key", event_key)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference_period)
        object.__setattr__(self, "reference_period_end_ns", reference_end)
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
        expected = _stable_id("inflation-release", self.identity_payload())
        supplied = _optional_text(self.release_id)
        if supplied is not None and supplied != expected:
            raise ValueError("release_id differs from deterministic identity")
        object.__setattr__(self, "release_id", expected)

    @property
    def display_observation(self) -> InflationObservationV1:
        return next(
            item
            for item in self.observations
            if item.observation_id == self.display_observation_id
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationReleaseV1:
        return cls(
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
                InflationObservationV1.from_dict(_mapping(item, "observation"))
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


_STAGE_ORDER: Mapping[EconomicReleaseStage, int] = {
    EconomicReleaseStage.FLASH: 0,
    EconomicReleaseStage.ADVANCE: 0,
    EconomicReleaseStage.INITIAL: 0,
    EconomicReleaseStage.PRELIMINARY: 1,
    EconomicReleaseStage.SECOND: 2,
    EconomicReleaseStage.FINAL: 3,
    EconomicReleaseStage.REVISION: 4,
}


@dataclass(frozen=True, slots=True)
class InflationReleaseSequenceV1:
    """Flash/preliminary/final stages retained as distinct releases."""

    releases: tuple[InflationReleaseV1, ...]
    sequence_id: str = ""
    schema_version: str = INFLATION_RELEASE_SEQUENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_RELEASE_SEQUENCE_SCHEMA_VERSION:
            raise ValueError("unsupported inflation release-sequence schema")
        releases = tuple(
            sorted(self.releases, key=lambda item: item.published_at_ns)
        )
        if not releases or len(releases) > MAX_INFLATION_RELEASE_STAGES:
            raise ValueError("release-stage count is outside bounds")
        if any(not isinstance(item, InflationReleaseV1) for item in releases):
            raise TypeError("release sequence contains a non-v1 release")
        if len({item.release_id for item in releases}) != len(releases):
            raise ValueError("release sequence repeats a release")
        if len({item.stage for item in releases}) != len(releases):
            raise ValueError("release sequence repeats a publication stage")
        first = releases[0]
        common = (
            first.economy_code,
            first.reference_period,
            first.reference_period_end_ns,
        )
        prior_rank = -1
        prior_published = -1
        for release in releases:
            if (
                release.economy_code,
                release.reference_period,
                release.reference_period_end_ns,
            ) != common:
                raise ValueError("release sequence changes reference period")
            rank = _STAGE_ORDER[release.stage]
            if rank < prior_rank or release.published_at_ns <= prior_published:
                raise ValueError(
                    "release stages are not chronologically ordered"
                )
            prior_rank = rank
            prior_published = release.published_at_ns
        if len(releases) > 1 and len(
            {item.logical_event_key for item in releases}
        ) != len(releases):
            raise ValueError(
                "distinct stages cannot overwrite one logical event"
            )
        shared_concepts = {
            item.concept.concept_id for item in releases[0].observations
        }
        for release in releases[1:]:
            shared_concepts &= {
                item.concept.concept_id for item in release.observations
            }
        if len(releases) > 1 and not shared_concepts:
            raise ValueError(
                "release stages share no comparable inflation concept"
            )
        object.__setattr__(self, "releases", releases)
        expected = _stable_id(
            "inflation-release-sequence", self.identity_payload()
        )
        supplied = _optional_text(self.sequence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("sequence_id differs from deterministic identity")
        object.__setattr__(self, "sequence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "sequence_id": self.sequence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationReleaseSequenceV1:
        return cls(
            releases=tuple(
                InflationReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            sequence_id=str(data.get("sequence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InflationMethodologyEventV1:
    """Rebase, reweight, seasonal, coverage, classification, or tax change."""

    economy_code: str
    index_family: InflationIndexFamily
    change_kind: InflationMethodologyChangeKind
    effective_reference_period: str
    effective_from_ns: int
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_request_id: str
    content_sha256: str
    prior_base_period: str | None
    new_base_period: str | None
    prior_weight_vintage_id: str | None
    new_weight_vintage_id: str | None
    bridge_factor: float | None
    comparable_across_change: bool
    limitations: tuple[str, ...]
    event_id: str = ""
    schema_version: str = INFLATION_METHODOLOGY_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_METHODOLOGY_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported inflation methodology-event schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        family = InflationIndexFamily.from_value(self.index_family)
        kind = InflationMethodologyChangeKind.from_value(self.change_kind)
        object.__setattr__(self, "index_family", family)
        object.__setattr__(self, "change_kind", kind)
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
        prior_base = _optional_text(self.prior_base_period)
        new_base = _optional_text(self.new_base_period)
        prior_weights = _optional_key(
            self.prior_weight_vintage_id, "prior_weight_vintage_id"
        )
        new_weights = _optional_key(
            self.new_weight_vintage_id, "new_weight_vintage_id"
        )
        bridge = _optional_finite(self.bridge_factor, "bridge_factor")
        if bridge is not None and bridge <= 0:
            raise ValueError("bridge_factor must be positive")
        if not isinstance(self.comparable_across_change, bool):
            raise TypeError("comparable_across_change must be boolean")
        if kind is InflationMethodologyChangeKind.REBASE:
            if prior_base is None or new_base is None or prior_base == new_base:
                raise ValueError("rebase requires distinct prior and new bases")
            if self.comparable_across_change and bridge is None:
                raise ValueError(
                    "comparable rebase requires an explicit bridge"
                )
        elif kind is InflationMethodologyChangeKind.REWEIGHT:
            if (
                prior_weights is None
                or new_weights is None
                or prior_weights == new_weights
            ):
                raise ValueError("reweight requires distinct weight vintages")
        object.__setattr__(self, "prior_base_period", prior_base)
        object.__setattr__(self, "new_base_period", new_base)
        object.__setattr__(self, "prior_weight_vintage_id", prior_weights)
        object.__setattr__(self, "new_weight_vintage_id", new_weights)
        object.__setattr__(self, "bridge_factor", bridge)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "inflation-methodology-event", self.identity_payload()
        )
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id differs from deterministic identity")
        object.__setattr__(self, "event_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "index_family": self.index_family.value,
            "change_kind": self.change_kind.value,
            "effective_reference_period": self.effective_reference_period,
            "effective_from_ns": self.effective_from_ns,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "prior_base_period": self.prior_base_period,
            "new_base_period": self.new_base_period,
            "prior_weight_vintage_id": self.prior_weight_vintage_id,
            "new_weight_vintage_id": self.new_weight_vintage_id,
            "bridge_factor": self.bridge_factor,
            "comparable_across_change": self.comparable_across_change,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "event_id": self.event_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationMethodologyEventV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            index_family=InflationIndexFamily.from_value(
                str(data.get("index_family", ""))
            ),
            change_kind=InflationMethodologyChangeKind.from_value(
                str(data.get("change_kind", ""))
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
            prior_base_period=_optional_text(data.get("prior_base_period")),
            new_base_period=_optional_text(data.get("new_base_period")),
            prior_weight_vintage_id=_optional_text(
                data.get("prior_weight_vintage_id")
            ),
            new_weight_vintage_id=_optional_text(
                data.get("new_weight_vintage_id")
            ),
            bridge_factor=cast(float | None, data.get("bridge_factor")),
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
class InflationSourceOwnerV1:
    """Legal producer responsible for one price-index family in an economy."""

    index_family: InflationIndexFamily
    source_key: str
    institution: str
    owner_id: str = ""
    schema_version: str = INFLATION_SOURCE_OWNER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_SOURCE_OWNER_SCHEMA_VERSION:
            raise ValueError("unsupported inflation source-owner schema")
        object.__setattr__(
            self,
            "index_family",
            InflationIndexFamily.from_value(self.index_family),
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self,
            "institution",
            _required_text(self.institution, "institution"),
        )
        expected = _stable_id("inflation-source-owner", self.identity_payload())
        supplied = _optional_text(self.owner_id)
        if supplied is not None and supplied != expected:
            raise ValueError("owner_id differs from deterministic identity")
        object.__setattr__(self, "owner_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_family": self.index_family.value,
            "source_key": self.source_key,
            "institution": self.institution,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "owner_id": self.owner_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationSourceOwnerV1:
        return cls(
            index_family=InflationIndexFamily.from_value(
                str(data.get("index_family", ""))
            ),
            source_key=str(data.get("source_key", "")),
            institution=str(data.get("institution", "")),
            owner_id=str(data.get("owner_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InflationEconomyProfileV1:
    """Reviewed inflation semantics and explicit legal owners for one economy."""

    economy_code: str
    source_owners: tuple[InflationSourceOwnerV1, ...]
    required_variants: tuple[InflationVariant, ...]
    supported_transformations: tuple[InflationTransformation, ...]
    supported_stages: tuple[EconomicReleaseStage, ...]
    special_cases: tuple[str, ...]
    rationale: str
    profile_id: str = ""
    schema_version: str = INFLATION_ECONOMY_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_ECONOMY_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported inflation economy-profile schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        owners = tuple(
            sorted(self.source_owners, key=lambda item: item.index_family.value)
        )
        if not owners or any(
            not isinstance(item, InflationSourceOwnerV1) for item in owners
        ):
            raise ValueError("profile requires typed inflation source owners")
        if len({item.index_family for item in owners}) != len(owners):
            raise ValueError("profile repeats an index-family owner")
        variants = tuple(
            sorted(
                {
                    InflationVariant.from_value(item)
                    for item in self.required_variants
                },
                key=lambda item: item.value,
            )
        )
        transformations = tuple(
            sorted(
                {
                    InflationTransformation.from_value(item)
                    for item in self.supported_transformations
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
                key=lambda item: _STAGE_ORDER[item],
            )
        )
        if InflationVariant.HEADLINE not in variants:
            raise ValueError("profile requires headline semantics")
        if (
            InflationTransformation.INDEX_LEVEL not in transformations
            or not any(item.derivable_from_index for item in transformations)
        ):
            raise ValueError(
                "profile requires index and derived-rate semantics"
            )
        if not stages:
            raise ValueError("profile requires release-stage semantics")
        cases = _keys(self.special_cases, "special_case", required=True)
        required_cases = {
            "component-weight-vintages",
            "initial-revision-chain",
            "methodology-rebases",
            "seasonal-adjustment-revisions",
        }
        if not required_cases.issubset(cases):
            raise ValueError("profile omits universal inflation edge cases")
        object.__setattr__(self, "source_owners", owners)
        object.__setattr__(self, "required_variants", variants)
        object.__setattr__(self, "supported_transformations", transformations)
        object.__setattr__(self, "supported_stages", stages)
        object.__setattr__(self, "special_cases", cases)
        object.__setattr__(
            self, "rationale", _required_text(self.rationale, "rationale")
        )
        expected = _stable_id(
            "inflation-economy-profile", self.identity_payload()
        )
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("profile_id differs from deterministic identity")
        object.__setattr__(self, "profile_id", expected)

    @property
    def index_families(self) -> tuple[InflationIndexFamily, ...]:
        return tuple(item.index_family for item in self.source_owners)

    def owner_for(self, family: InflationIndexFamily) -> InflationSourceOwnerV1:
        selected = InflationIndexFamily.from_value(family)
        matches = [
            item for item in self.source_owners if item.index_family is selected
        ]
        if len(matches) != 1:
            raise ValueError("profile has no unique owner for index family")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "source_owners": [item.to_dict() for item in self.source_owners],
            "required_variants": [
                item.value for item in self.required_variants
            ],
            "supported_transformations": [
                item.value for item in self.supported_transformations
            ],
            "supported_stages": [item.value for item in self.supported_stages],
            "special_cases": list(self.special_cases),
            "rationale": self.rationale,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InflationEconomyProfileV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            source_owners=tuple(
                InflationSourceOwnerV1.from_dict(_mapping(item, "source owner"))
                for item in _sequence(
                    data.get("source_owners"), "source_owners"
                )
            ),
            required_variants=tuple(
                InflationVariant.from_value(str(item))
                for item in _sequence(
                    data.get("required_variants"), "required_variants"
                )
            ),
            supported_transformations=tuple(
                InflationTransformation.from_value(str(item))
                for item in _sequence(
                    data.get("supported_transformations"),
                    "supported_transformations",
                )
            ),
            supported_stages=tuple(
                EconomicReleaseStage.from_value(str(item))
                for item in _sequence(
                    data.get("supported_stages"), "supported_stages"
                )
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


_INDEX_FAMILIES_BY_ECONOMY: Mapping[str, tuple[InflationIndexFamily, ...]] = {
    "EA": (InflationIndexFamily.HICP, InflationIndexFamily.PPI),
    "DE": (
        InflationIndexFamily.CPI,
        InflationIndexFamily.HICP,
        InflationIndexFamily.PPI,
    ),
    "FR": (
        InflationIndexFamily.CPI,
        InflationIndexFamily.HICP,
        InflationIndexFamily.PPI,
    ),
    "GB": (
        InflationIndexFamily.CPI,
        InflationIndexFamily.PPI,
        InflationIndexFamily.RETAIL_PRICE_INDEX,
    ),
    "JP": (InflationIndexFamily.CPI, InflationIndexFamily.PPI),
    "US": (
        InflationIndexFamily.CPI,
        InflationIndexFamily.PPI,
        InflationIndexFamily.PCE_DEFLATOR,
        InflationIndexFamily.GDP_DEFLATOR,
    ),
}
_SUPPLEMENTAL_SOURCE_BY_ECONOMY_FAMILY: Mapping[
    tuple[str, InflationIndexFamily], str
] = {
    ("JP", InflationIndexFamily.PPI): "jp.boj.time-series",
    ("US", InflationIndexFamily.PCE_DEFLATOR): "us.bea.api",
    ("US", InflationIndexFamily.GDP_DEFLATOR): "us.bea.api",
}
_SPECIAL_CASES_BY_ECONOMY: Mapping[str, tuple[str, ...]] = {
    "EA": ("euro-area-flash-final-hicp",),
    "DE": ("national-cpi-versus-hicp",),
    "FR": ("national-cpi-versus-hicp",),
    "JP": ("tokyo-versus-national-cpi",),
    "US": (
        "owner-equivalent-rent-and-shelter",
        "pce-deflator-separate-legal-producer",
    ),
}
_UNIVERSAL_SPECIAL_CASES: tuple[str, ...] = (
    "component-weight-vintages",
    "energy-tax-changes",
    "initial-revision-chain",
    "methodology-rebases",
    "seasonal-adjustment-revisions",
    "simultaneous-previous-period-revisions",
)
_DEFAULT_TRANSFORMATIONS: tuple[InflationTransformation, ...] = (
    InflationTransformation.INDEX_LEVEL,
    InflationTransformation.MONTH_OVER_MONTH_PERCENT,
    InflationTransformation.QUARTER_OVER_QUARTER_PERCENT,
    InflationTransformation.YEAR_OVER_YEAR_PERCENT,
    InflationTransformation.ANNUALIZED_QUARTER_OVER_QUARTER_PERCENT,
    InflationTransformation.CONTRIBUTION_PERCENTAGE_POINTS,
    InflationTransformation.WEIGHT_PERCENT,
)


def built_in_inflation_profiles(
    registry: OfficialSourceRegistryV1,
) -> tuple[InflationEconomyProfileV1, ...]:
    """Build reviewed source ownership and semantics for all scoped economies."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    profiles: list[InflationEconomyProfileV1] = []
    for economy_code in registry.scoped_economies:
        primary = registry.primary_source(
            economy_code, EconomicEventFamily.INFLATION_PRICES
        )
        families = _INDEX_FAMILIES_BY_ECONOMY.get(
            economy_code,
            (InflationIndexFamily.CPI, InflationIndexFamily.PPI),
        )
        owners: list[InflationSourceOwnerV1] = []
        for family in families:
            source_key = _SUPPLEMENTAL_SOURCE_BY_ECONOMY_FAMILY.get(
                (economy_code, family), primary.source_key
            )
            source = registry.source(source_key)
            owners.append(
                InflationSourceOwnerV1(
                    index_family=family,
                    source_key=source.source_key,
                    institution=source.institution,
                )
            )
        stages = (
            (
                EconomicReleaseStage.FLASH,
                EconomicReleaseStage.FINAL,
                EconomicReleaseStage.REVISION,
            )
            if economy_code == "EA"
            else (
                EconomicReleaseStage.INITIAL,
                EconomicReleaseStage.FINAL,
                EconomicReleaseStage.REVISION,
            )
        )
        profiles.append(
            InflationEconomyProfileV1(
                economy_code=economy_code,
                source_owners=tuple(owners),
                required_variants=(
                    InflationVariant.HEADLINE,
                    InflationVariant.CORE,
                    InflationVariant.COMPONENT,
                ),
                supported_transformations=_DEFAULT_TRANSFORMATIONS,
                supported_stages=stages,
                special_cases=(
                    *_UNIVERSAL_SPECIAL_CASES,
                    *_SPECIAL_CASES_BY_ECONOMY.get(economy_code, ()),
                ),
                rationale=(
                    "Bind each family to its legal producer and retain exact "
                    "index, transformation, release-stage, component, and "
                    "methodology-vintage identity. Availability remains a "
                    "separate adapter qualification claim."
                ),
            )
        )
    return tuple(sorted(profiles, key=lambda item: item.economy_code))


@dataclass(frozen=True, slots=True)
class InflationProfileAuditV1:
    """Coverage proof for economy profiles, legal owners, and edge cases."""

    registry_id: str
    profile_ids: tuple[str, ...]
    missing_economies: tuple[str, ...]
    duplicate_economies: tuple[str, ...]
    source_mismatches: tuple[str, ...]
    semantic_mismatches: tuple[str, ...]
    complete: bool
    audit_id: str = ""
    schema_version: str = INFLATION_PROFILE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INFLATION_PROFILE_AUDIT_SCHEMA_VERSION:
            raise ValueError("unsupported inflation profile-audit schema")
        object.__setattr__(
            self,
            "registry_id",
            _required_text(self.registry_id, "registry_id"),
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
                "inflation profile audit completeness is inconsistent"
            )
        expected = _stable_id(
            "inflation-profile-audit", self.identity_payload()
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
    def from_dict(cls, data: Mapping[str, Any]) -> InflationProfileAuditV1:
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


def audit_inflation_profiles(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[InflationEconomyProfileV1],
) -> InflationProfileAuditV1:
    """Audit one complete semantic profile and legal owner per scope cell."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    values = tuple(profiles)
    if any(not isinstance(item, InflationEconomyProfileV1) for item in values):
        raise TypeError("profiles must use InflationEconomyProfileV1")
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
        primary = registry.primary_source(
            code, EconomicEventFamily.INFLATION_PRICES
        )
        if primary.source_key not in {
            item.source_key for item in profile.source_owners
        }:
            source_mismatches.append(f"{code}:primary-owner-missing")
        for owner in profile.source_owners:
            expected_source_key = _SUPPLEMENTAL_SOURCE_BY_ECONOMY_FAMILY.get(
                (code, owner.index_family), primary.source_key
            )
            try:
                source = registry.source(owner.source_key)
            except ValueError:
                source_mismatches.append(f"{code}:{owner.index_family.value}")
                continue
            if (
                owner.source_key != expected_source_key
                or source.economy_code != code
                or source.institution != owner.institution
                or OfficialSourceRole.PRIMARY_PRODUCER not in source.roles
            ):
                source_mismatches.append(f"{code}:{owner.index_family.value}")
        expected_families = set(
            _INDEX_FAMILIES_BY_ECONOMY.get(
                code, (InflationIndexFamily.CPI, InflationIndexFamily.PPI)
            )
        )
        expected_cases = set(_UNIVERSAL_SPECIAL_CASES) | set(
            _SPECIAL_CASES_BY_ECONOMY.get(code, ())
        )
        if (
            set(profile.index_families) != expected_families
            or not expected_cases.issubset(profile.special_cases)
            or InflationVariant.CORE not in profile.required_variants
            or InflationVariant.COMPONENT not in profile.required_variants
        ):
            semantic_mismatches.append(code)
        if code == "EA" and not {
            EconomicReleaseStage.FLASH,
            EconomicReleaseStage.FINAL,
        }.issubset(profile.supported_stages):
            semantic_mismatches.append(f"{code}:flash-final")
    if len({item.profile_id for item in values}) != len(values):
        duplicates = tuple(sorted({*duplicates, "profile-id"}))
    source_result = tuple(sorted(set(source_mismatches)))
    semantic_result = tuple(sorted(set(semantic_mismatches)))
    return InflationProfileAuditV1(
        registry_id=registry.registry_id,
        profile_ids=tuple(item.profile_id for item in values),
        missing_economies=missing,
        duplicate_economies=duplicates,
        source_mismatches=source_result,
        semantic_mismatches=semantic_result,
        complete=not any((missing, duplicates, source_result, semantic_result)),
    )


def require_inflation_profile_coverage(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[InflationEconomyProfileV1],
) -> InflationProfileAuditV1:
    """Return a complete audit or fail closed on semantic/source gaps."""
    audit = audit_inflation_profiles(registry, profiles)
    if not audit.complete:
        raise ValueError("inflation profile coverage is incomplete")
    return audit


__all__ = [
    "INFLATION_COMPONENT_AGGREGATION_SCHEMA_VERSION",
    "INFLATION_COMPONENT_CONTRIBUTION_SCHEMA_VERSION",
    "INFLATION_CONCEPT_SCHEMA_VERSION",
    "INFLATION_ECONOMY_PROFILE_SCHEMA_VERSION",
    "INFLATION_EXPECTATION_SCHEMA_VERSION",
    "INFLATION_METHODOLOGY_EVENT_SCHEMA_VERSION",
    "INFLATION_OBSERVATION_SCHEMA_VERSION",
    "INFLATION_PROFILE_AUDIT_SCHEMA_VERSION",
    "INFLATION_RATE_DERIVATION_SCHEMA_VERSION",
    "INFLATION_RELEASE_SCHEMA_VERSION",
    "INFLATION_RELEASE_SEQUENCE_SCHEMA_VERSION",
    "INFLATION_RELEASE_TRIPLET_SCHEMA_VERSION",
    "INFLATION_SOURCE_OWNER_SCHEMA_VERSION",
    "INFLATION_VINTAGE_CHAIN_SCHEMA_VERSION",
    "MAX_INFLATION_COMPONENTS",
    "MAX_INFLATION_RELEASE_OBSERVATIONS",
    "MAX_INFLATION_RELEASE_STAGES",
    "MAX_INFLATION_REVISIONS",
    "InflationComponentAggregationV1",
    "InflationComponentContributionV1",
    "InflationComponentCoverage",
    "InflationConceptV1",
    "InflationContributionBasis",
    "InflationDerivationVintageBasis",
    "InflationEconomyProfileV1",
    "InflationExpectationKind",
    "InflationExpectationV1",
    "InflationFrequency",
    "InflationIndexFamily",
    "InflationMethodologyChangeKind",
    "InflationMethodologyEventV1",
    "InflationObservationBasis",
    "InflationObservationV1",
    "InflationProfileAuditV1",
    "InflationRateDerivationV1",
    "InflationReleaseSequenceV1",
    "InflationReleaseTripletV1",
    "InflationReleaseV1",
    "InflationSeasonalBasis",
    "InflationSourceOwnerV1",
    "InflationTransformation",
    "InflationVariant",
    "InflationVintageChainV1",
    "audit_inflation_profiles",
    "built_in_inflation_profiles",
    "require_inflation_profile_coverage",
]
