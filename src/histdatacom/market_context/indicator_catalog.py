"""Canonical economic-indicator catalog and expected-occurrence coverage.

The catalog is deliberately distinct from observed releases.  It records what
official concepts are expected to exist, which recurrence rules are actually
qualified, and why an expected occurrence is absent.  Consequently a current
API row cannot make a historical catalog look complete and an unqualified
expected-occurrence model never produces a misleading coverage percentage.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
)
from histdatacom.market_context.official_sources import (
    SCOPED_ECONOMIES,
    OfficialSourceRegistryV1,
)
from histdatacom.runtime_contracts import JSONValue

ECONOMIC_INDICATOR_METHODOLOGY_ERA_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-methodology-era.v1"
)
ECONOMIC_EXPECTED_RELEASE_RULE_SCHEMA_VERSION = (
    "histdatacom.economic-expected-release-rule.v1"
)
ECONOMIC_INDICATOR_CATALOG_ENTRY_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-catalog-entry.v1"
)
ECONOMIC_INDICATOR_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-lineage.v1"
)
ECONOMIC_EXPECTED_OCCURRENCE_SCHEMA_VERSION = (
    "histdatacom.economic-expected-occurrence.v1"
)
ECONOMIC_INDICATOR_CATALOG_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-catalog.v1"
)
ECONOMIC_INDICATOR_COVERAGE_SLICE_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-coverage-slice.v1"
)
ECONOMIC_INDICATOR_COVERAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.economic-indicator-coverage-audit.v1"
)

MAX_ECONOMIC_INDICATORS = 10_000
MAX_ECONOMIC_EXPECTED_OCCURRENCES = 1_000_000
MAX_ECONOMIC_INDICATOR_CATALOG_BYTES = 64 * 1024 * 1024

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EconomicIndicatorFrequency(str, Enum):
    """Canonical recurrence class without pretending mixed families are exact."""

    IRREGULAR = "irregular"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    MIXED = "mixed"

    @classmethod
    def from_value(
        cls, value: str | EconomicIndicatorFrequency
    ) -> EconomicIndicatorFrequency:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError(
                "unsupported economic indicator frequency"
            ) from exc


class EconomicExpectedOccurrenceStatus(str, Enum):
    """Final or as-known state assigned to one expected occurrence."""

    SCHEDULED = "scheduled"
    RELEASED = "released"
    RESCHEDULED = "rescheduled"
    CANCELLED = "cancelled"
    DISCONTINUED = "discontinued"
    SOURCE_UNAVAILABLE = "source-unavailable"
    UNRESOLVED = "unresolved"

    @classmethod
    def from_value(
        cls, value: str | EconomicExpectedOccurrenceStatus
    ) -> EconomicExpectedOccurrenceStatus:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported expected occurrence status") from exc


class EconomicIndicatorLineageKind(str, Enum):
    """Relationship between immutable indicator definitions."""

    RENAMED = "renamed"
    SUCCESSOR = "successor"
    REBASED = "rebased"
    METHODOLOGY_BREAK = "methodology-break"
    SPLIT = "split"
    MERGED = "merged"

    @classmethod
    def from_value(
        cls, value: str | EconomicIndicatorLineageKind
    ) -> EconomicIndicatorLineageKind:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError(
                "unsupported economic indicator lineage kind"
            ) from exc


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _key(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _KEY_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a canonical key")
    return result


def _economy(value: object) -> str:
    result = _required_text(value, "economy_code").upper()
    if _ECONOMY_RE.fullmatch(result) is None:
        raise ValueError("economy_code is invalid")
    return result


def _currencies(values: Iterable[object]) -> tuple[str, ...]:
    result = tuple(
        sorted({_required_text(item, "currency").upper() for item in values})
    )
    if not result or any(
        _CURRENCY_RE.fullmatch(item) is None for item in result
    ):
        raise ValueError("affected_currencies must contain ISO-style codes")
    return result


def _texts(
    values: Iterable[object], name: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in values}))
    if required and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) > 256:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _keys(values: Iterable[object], name: str) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, name) for item in values}))
    if len(result) > 256:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    return result


def _optional_date(value: object, name: str) -> str | None:
    result = _optional_text(value)
    return None if result is None else _iso_date(result, name)


def _bounded_ns(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


@dataclass(frozen=True, slots=True)
class EconomicIndicatorMethodologyEraV1:
    """One bounded, source-supported semantic era of an indicator."""

    era_key: str
    effective_from: str
    effective_to: str | None
    source_series_ids: tuple[str, ...]
    source_table_ids: tuple[str, ...]
    source_release_ids: tuple[str, ...]
    unit: str
    scale: float
    seasonal_basis: str
    reference_period_convention: str
    methodology_evidence: tuple[str, ...]
    notes: tuple[str, ...]
    era_id: str = ""
    schema_version: str = ECONOMIC_INDICATOR_METHODOLOGY_ERA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_INDICATOR_METHODOLOGY_ERA_SCHEMA_VERSION
        ):
            raise ValueError("unsupported indicator methodology-era schema")
        object.__setattr__(self, "era_key", _key(self.era_key, "era_key"))
        start = _iso_date(self.effective_from, "effective_from")
        end = _optional_date(self.effective_to, "effective_to")
        if end is not None and end < start:
            raise ValueError("methodology era ends before it starts")
        object.__setattr__(self, "effective_from", start)
        object.__setattr__(self, "effective_to", end)
        for name in (
            "source_series_ids",
            "source_table_ids",
            "source_release_ids",
        ):
            object.__setattr__(self, name, _texts(getattr(self, name), name))
        for name in ("unit", "seasonal_basis", "reference_period_convention"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if isinstance(self.scale, bool) or not isinstance(
            self.scale, (int, float)
        ):
            raise TypeError("scale must be numeric")
        scale = float(self.scale)
        if not math.isfinite(scale) or scale <= 0:
            raise ValueError("scale must be finite and positive")
        object.__setattr__(self, "scale", scale)
        object.__setattr__(
            self,
            "methodology_evidence",
            _texts(
                self.methodology_evidence, "methodology_evidence", required=True
            ),
        )
        object.__setattr__(self, "notes", _texts(self.notes, "notes"))
        expected = _stable_id("economic-indicator-era", self.identity_payload())
        supplied = _optional_text(self.era_id)
        if supplied is not None and supplied != expected:
            raise ValueError("era_id does not match deterministic identity")
        object.__setattr__(self, "era_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "era_key": self.era_key,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to,
            "source_series_ids": list(self.source_series_ids),
            "source_table_ids": list(self.source_table_ids),
            "source_release_ids": list(self.source_release_ids),
            "unit": self.unit,
            "scale": self.scale,
            "seasonal_basis": self.seasonal_basis,
            "reference_period_convention": self.reference_period_convention,
            "methodology_evidence": list(self.methodology_evidence),
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "era_id": self.era_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicIndicatorMethodologyEraV1:
        return cls(
            era_key=str(data.get("era_key", "")),
            effective_from=str(data.get("effective_from", "")),
            effective_to=_optional_text(data.get("effective_to")),
            source_series_ids=tuple(
                str(item) for item in _sequence(data.get("source_series_ids"))
            ),
            source_table_ids=tuple(
                str(item) for item in _sequence(data.get("source_table_ids"))
            ),
            source_release_ids=tuple(
                str(item) for item in _sequence(data.get("source_release_ids"))
            ),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            seasonal_basis=str(data.get("seasonal_basis", "")),
            reference_period_convention=str(
                data.get("reference_period_convention", "")
            ),
            methodology_evidence=tuple(
                str(item)
                for item in _sequence(data.get("methodology_evidence"))
            ),
            notes=tuple(str(item) for item in _sequence(data.get("notes"))),
            era_id=str(data.get("era_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicExpectedReleaseRuleV1:
    """Versioned expected-occurrence rule, qualified only with evidence."""

    rule_key: str
    frequency: EconomicIndicatorFrequency
    effective_from: str
    effective_to: str | None
    source_key: str
    source_timezone: str
    time_precision: EconomicTimePrecision
    rule_description: str
    qualified: bool
    qualification_evidence: tuple[str, ...]
    limitations: tuple[str, ...]
    rule_id: str = ""
    schema_version: str = ECONOMIC_EXPECTED_RELEASE_RULE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_EXPECTED_RELEASE_RULE_SCHEMA_VERSION:
            raise ValueError("unsupported expected release-rule schema")
        object.__setattr__(self, "rule_key", _key(self.rule_key, "rule_key"))
        object.__setattr__(
            self,
            "frequency",
            EconomicIndicatorFrequency.from_value(self.frequency),
        )
        start = _iso_date(self.effective_from, "effective_from")
        end = _optional_date(self.effective_to, "effective_to")
        if end is not None and end < start:
            raise ValueError("expected release rule ends before it starts")
        object.__setattr__(self, "effective_from", start)
        object.__setattr__(self, "effective_to", end)
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        try:
            ZoneInfo(_required_text(self.source_timezone, "source_timezone"))
        except ZoneInfoNotFoundError as exc:
            raise ValueError("source_timezone is not an IANA timezone") from exc
        object.__setattr__(
            self,
            "time_precision",
            EconomicTimePrecision.from_value(self.time_precision),
        )
        object.__setattr__(
            self,
            "rule_description",
            _required_text(self.rule_description, "rule_description"),
        )
        if not isinstance(self.qualified, bool):
            raise TypeError("qualified must be boolean")
        evidence = _texts(self.qualification_evidence, "qualification_evidence")
        limitations = _texts(self.limitations, "limitations", required=True)
        if self.qualified and not evidence:
            raise ValueError("a qualified release rule requires evidence")
        if not self.qualified and not limitations:
            raise ValueError("an unqualified release rule requires limitations")
        object.__setattr__(self, "qualification_evidence", evidence)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "economic-expected-release-rule", self.identity_payload()
        )
        supplied = _optional_text(self.rule_id)
        if supplied is not None and supplied != expected:
            raise ValueError("rule_id does not match deterministic identity")
        object.__setattr__(self, "rule_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "rule_key": self.rule_key,
            "frequency": self.frequency.value,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to,
            "source_key": self.source_key,
            "source_timezone": self.source_timezone,
            "time_precision": self.time_precision.value,
            "rule_description": self.rule_description,
            "qualified": self.qualified,
            "qualification_evidence": list(self.qualification_evidence),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "rule_id": self.rule_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicExpectedReleaseRuleV1:
        return cls(
            rule_key=str(data.get("rule_key", "")),
            frequency=EconomicIndicatorFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            effective_from=str(data.get("effective_from", "")),
            effective_to=_optional_text(data.get("effective_to")),
            source_key=str(data.get("source_key", "")),
            source_timezone=str(data.get("source_timezone", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            rule_description=str(data.get("rule_description", "")),
            qualified=cast(bool, data.get("qualified")),
            qualification_evidence=tuple(
                str(item)
                for item in _sequence(data.get("qualification_evidence"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            rule_id=str(data.get("rule_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicIndicatorCatalogEntryV1:
    """Immutable provider-neutral definition of one economic indicator."""

    indicator_key: str
    economy_code: str
    economy_name: str
    affected_currencies: tuple[str, ...]
    event_family: EconomicEventFamily
    indicator_id: str
    display_name: str
    aliases: tuple[str, ...]
    legal_producer_source_key: str
    frequency: EconomicIndicatorFrequency
    reference_period_convention: str
    normal_schedule_rule: str
    release_stages: tuple[EconomicReleaseStage, ...]
    unit: str
    scale: float
    seasonal_basis: str
    valid_from: str
    valid_to: str | None
    methodology_eras: tuple[EconomicIndicatorMethodologyEraV1, ...]
    expected_release_rules: tuple[EconomicExpectedReleaseRuleV1, ...]
    structural_importance: int
    importance_policy_version: str
    importance_rationale: str
    limitations: tuple[str, ...]
    entry_id: str = ""
    schema_version: str = ECONOMIC_INDICATOR_CATALOG_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_INDICATOR_CATALOG_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError("unsupported indicator catalog-entry schema")
        object.__setattr__(
            self, "indicator_key", _key(self.indicator_key, "indicator_key")
        )
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self,
            "economy_name",
            _required_text(self.economy_name, "economy_name"),
        )
        object.__setattr__(
            self, "affected_currencies", _currencies(self.affected_currencies)
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        object.__setattr__(
            self, "indicator_id", _key(self.indicator_id, "indicator_id")
        )
        object.__setattr__(
            self,
            "display_name",
            _required_text(self.display_name, "display_name"),
        )
        object.__setattr__(
            self, "aliases", _texts(self.aliases, "aliases", required=True)
        )
        object.__setattr__(
            self,
            "legal_producer_source_key",
            _key(self.legal_producer_source_key, "legal_producer_source_key"),
        )
        object.__setattr__(
            self,
            "frequency",
            EconomicIndicatorFrequency.from_value(self.frequency),
        )
        for name in (
            "reference_period_convention",
            "normal_schedule_rule",
            "unit",
            "seasonal_basis",
            "importance_policy_version",
            "importance_rationale",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        stages = tuple(
            sorted(
                {
                    EconomicReleaseStage.from_value(item)
                    for item in self.release_stages
                },
                key=lambda item: item.value,
            )
        )
        if not stages:
            raise ValueError("release_stages must not be empty")
        object.__setattr__(self, "release_stages", stages)
        if isinstance(self.scale, bool) or not isinstance(
            self.scale, (int, float)
        ):
            raise TypeError("scale must be numeric")
        scale = float(self.scale)
        if not math.isfinite(scale) or scale <= 0:
            raise ValueError("scale must be finite and positive")
        object.__setattr__(self, "scale", scale)
        start = _iso_date(self.valid_from, "valid_from")
        end = _optional_date(self.valid_to, "valid_to")
        if end is not None and end < start:
            raise ValueError("indicator validity ends before it starts")
        object.__setattr__(self, "valid_from", start)
        object.__setattr__(self, "valid_to", end)
        eras = tuple(
            sorted(self.methodology_eras, key=lambda item: item.effective_from)
        )
        if not eras or any(
            not isinstance(item, EconomicIndicatorMethodologyEraV1)
            for item in eras
        ):
            raise ValueError("indicator requires v1 methodology eras")
        if len({item.era_key for item in eras}) != len(eras):
            raise ValueError("indicator repeats a methodology era key")
        for previous_era, current_era in pairwise(eras):
            if (
                previous_era.effective_to is None
                or previous_era.effective_to >= current_era.effective_from
            ):
                raise ValueError("indicator methodology eras overlap")
            if (
                date.fromisoformat(previous_era.effective_to).toordinal() + 1
                != date.fromisoformat(current_era.effective_from).toordinal()
            ):
                raise ValueError("indicator methodology eras contain a gap")
        if eras[0].effective_from != start:
            raise ValueError("first methodology era must start with indicator")
        if eras[-1].effective_to != end:
            raise ValueError("methodology eras must cover indicator validity")
        object.__setattr__(self, "methodology_eras", eras)
        rules = tuple(
            sorted(
                self.expected_release_rules,
                key=lambda item: item.effective_from,
            )
        )
        if not rules or any(
            not isinstance(item, EconomicExpectedReleaseRuleV1)
            for item in rules
        ):
            raise ValueError("indicator requires v1 expected release rules")
        if len({item.rule_key for item in rules}) != len(rules):
            raise ValueError("indicator repeats an expected release-rule key")
        if any(
            item.source_key != self.legal_producer_source_key for item in rules
        ):
            raise ValueError("release rule changes legal producer")
        for previous_rule, current_rule in pairwise(rules):
            if (
                previous_rule.effective_to is None
                or previous_rule.effective_to >= current_rule.effective_from
            ):
                raise ValueError("indicator expected release rules overlap")
            if (
                date.fromisoformat(previous_rule.effective_to).toordinal() + 1
                != date.fromisoformat(current_rule.effective_from).toordinal()
            ):
                raise ValueError(
                    "indicator expected release rules contain a gap"
                )
        if rules[0].effective_from != start:
            raise ValueError(
                "first expected release rule must start with indicator"
            )
        if rules[-1].effective_to != end:
            raise ValueError(
                "expected release rules must cover indicator validity"
            )
        if any(rule.qualified for rule in rules) and any(
            not (
                era.source_series_ids
                or era.source_table_ids
                or era.source_release_ids
            )
            for era in eras
        ):
            raise ValueError(
                "qualified expected rules require exact official source identifiers"
            )
        object.__setattr__(self, "expected_release_rules", rules)
        if isinstance(self.structural_importance, bool) or not isinstance(
            self.structural_importance, int
        ):
            raise TypeError("structural_importance must be an integer")
        if not 0 <= self.structural_importance <= 3:
            raise ValueError(
                "structural_importance must be between zero and three"
            )
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-indicator-catalog-entry", self.identity_payload()
        )
        supplied = _optional_text(self.entry_id)
        if supplied is not None and supplied != expected:
            raise ValueError("entry_id does not match deterministic identity")
        object.__setattr__(self, "entry_id", expected)

    @property
    def has_qualified_expected_model(self) -> bool:
        return any(item.qualified for item in self.expected_release_rules)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "indicator_key": self.indicator_key,
            "economy_code": self.economy_code,
            "economy_name": self.economy_name,
            "affected_currencies": list(self.affected_currencies),
            "event_family": self.event_family.value,
            "indicator_id": self.indicator_id,
            "display_name": self.display_name,
            "aliases": list(self.aliases),
            "legal_producer_source_key": self.legal_producer_source_key,
            "frequency": self.frequency.value,
            "reference_period_convention": self.reference_period_convention,
            "normal_schedule_rule": self.normal_schedule_rule,
            "release_stages": [item.value for item in self.release_stages],
            "unit": self.unit,
            "scale": self.scale,
            "seasonal_basis": self.seasonal_basis,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "methodology_eras": [
                item.to_dict() for item in self.methodology_eras
            ],
            "expected_release_rules": [
                item.to_dict() for item in self.expected_release_rules
            ],
            "structural_importance": self.structural_importance,
            "importance_policy_version": self.importance_policy_version,
            "importance_rationale": self.importance_rationale,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicIndicatorCatalogEntryV1:
        return cls(
            indicator_key=str(data.get("indicator_key", "")),
            economy_code=str(data.get("economy_code", "")),
            economy_name=str(data.get("economy_name", "")),
            affected_currencies=tuple(
                str(item) for item in _sequence(data.get("affected_currencies"))
            ),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            indicator_id=str(data.get("indicator_id", "")),
            display_name=str(data.get("display_name", "")),
            aliases=tuple(str(item) for item in _sequence(data.get("aliases"))),
            legal_producer_source_key=str(
                data.get("legal_producer_source_key", "")
            ),
            frequency=EconomicIndicatorFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            reference_period_convention=str(
                data.get("reference_period_convention", "")
            ),
            normal_schedule_rule=str(data.get("normal_schedule_rule", "")),
            release_stages=tuple(
                EconomicReleaseStage.from_value(str(item))
                for item in _sequence(data.get("release_stages"))
            ),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            seasonal_basis=str(data.get("seasonal_basis", "")),
            valid_from=str(data.get("valid_from", "")),
            valid_to=_optional_text(data.get("valid_to")),
            methodology_eras=tuple(
                EconomicIndicatorMethodologyEraV1.from_dict(_mapping(item))
                for item in _sequence(data.get("methodology_eras"))
            ),
            expected_release_rules=tuple(
                EconomicExpectedReleaseRuleV1.from_dict(_mapping(item))
                for item in _sequence(data.get("expected_release_rules"))
            ),
            structural_importance=cast(int, data.get("structural_importance")),
            importance_policy_version=str(
                data.get("importance_policy_version", "")
            ),
            importance_rationale=str(data.get("importance_rationale", "")),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicIndicatorLineageV1:
    """Directed, evidence-backed relationship between catalog entries."""

    from_indicator_key: str
    to_indicator_key: str
    kind: EconomicIndicatorLineageKind
    effective_on: str
    source_key: str
    evidence: tuple[str, ...]
    notes: tuple[str, ...]
    lineage_id: str = ""
    schema_version: str = ECONOMIC_INDICATOR_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_INDICATOR_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported indicator lineage schema")
        for name in ("from_indicator_key", "to_indicator_key", "source_key"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        if self.from_indicator_key == self.to_indicator_key:
            raise ValueError("indicator lineage cannot point to itself")
        object.__setattr__(
            self, "kind", EconomicIndicatorLineageKind.from_value(self.kind)
        )
        object.__setattr__(
            self, "effective_on", _iso_date(self.effective_on, "effective_on")
        )
        object.__setattr__(
            self, "evidence", _texts(self.evidence, "evidence", required=True)
        )
        object.__setattr__(self, "notes", _texts(self.notes, "notes"))
        expected = _stable_id(
            "economic-indicator-lineage", self.identity_payload()
        )
        supplied = _optional_text(self.lineage_id)
        if supplied is not None and supplied != expected:
            raise ValueError("lineage_id does not match deterministic identity")
        object.__setattr__(self, "lineage_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "from_indicator_key": self.from_indicator_key,
            "to_indicator_key": self.to_indicator_key,
            "kind": self.kind.value,
            "effective_on": self.effective_on,
            "source_key": self.source_key,
            "evidence": list(self.evidence),
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicIndicatorLineageV1:
        return cls(
            from_indicator_key=str(data.get("from_indicator_key", "")),
            to_indicator_key=str(data.get("to_indicator_key", "")),
            kind=EconomicIndicatorLineageKind.from_value(
                str(data.get("kind", ""))
            ),
            effective_on=str(data.get("effective_on", "")),
            source_key=str(data.get("source_key", "")),
            evidence=tuple(
                str(item) for item in _sequence(data.get("evidence"))
            ),
            notes=tuple(str(item) for item in _sequence(data.get("notes"))),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicIndicatorCatalogV1:
    """Versioned indicator definitions covering every declared matrix cell."""

    catalog_version: str
    reviewed_on: str
    source_registry_id: str
    scoped_economies: tuple[str, ...]
    scoped_families: tuple[EconomicEventFamily, ...]
    entries: tuple[EconomicIndicatorCatalogEntryV1, ...]
    lineage: tuple[EconomicIndicatorLineageV1, ...]
    limitations: tuple[str, ...]
    catalog_id: str = ""
    schema_version: str = ECONOMIC_INDICATOR_CATALOG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_INDICATOR_CATALOG_SCHEMA_VERSION:
            raise ValueError("unsupported indicator catalog schema")
        object.__setattr__(
            self,
            "catalog_version",
            _required_text(self.catalog_version, "catalog_version"),
        )
        object.__setattr__(
            self, "reviewed_on", _iso_date(self.reviewed_on, "reviewed_on")
        )
        registry_id = _required_text(
            self.source_registry_id, "source_registry_id"
        )
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "source_registry_id is not an official registry identity"
            )
        object.__setattr__(self, "source_registry_id", registry_id)
        economies = tuple(
            sorted({_economy(item) for item in self.scoped_economies})
        )
        families = tuple(
            sorted(
                {
                    EconomicEventFamily.from_value(item)
                    for item in self.scoped_families
                },
                key=lambda item: item.value,
            )
        )
        if not economies or not families:
            raise ValueError("indicator catalog scope must not be empty")
        object.__setattr__(self, "scoped_economies", economies)
        object.__setattr__(self, "scoped_families", families)
        entries = tuple(
            sorted(self.entries, key=lambda item: item.indicator_key)
        )
        if (
            not entries
            or len(entries) > MAX_ECONOMIC_INDICATORS
            or any(
                not isinstance(item, EconomicIndicatorCatalogEntryV1)
                for item in entries
            )
        ):
            raise ValueError("indicator entry count is outside v1 bounds")
        by_key = {item.indicator_key: item for item in entries}
        if len(by_key) != len(entries) or len(
            {item.entry_id for item in entries}
        ) != len(entries):
            raise ValueError("indicator catalog repeats an entry")
        if any(
            item.economy_code not in economies
            or item.event_family not in families
            for item in entries
        ):
            raise ValueError("indicator entry lies outside catalog scope")
        cells = {(item.economy_code, item.event_family) for item in entries}
        required_cells = {
            (economy, family) for economy in economies for family in families
        }
        if cells != required_cells:
            raise ValueError(
                "indicator catalog does not cover every economy/family cell"
            )
        object.__setattr__(self, "entries", entries)
        lineage = tuple(sorted(self.lineage, key=lambda item: item.lineage_id))
        if any(
            not isinstance(item, EconomicIndicatorLineageV1) for item in lineage
        ):
            raise ValueError("catalog lineage must use v1 contracts")
        if len({item.lineage_id for item in lineage}) != len(lineage):
            raise ValueError("indicator catalog repeats lineage evidence")
        for edge in lineage:
            if (
                edge.from_indicator_key not in by_key
                or edge.to_indicator_key not in by_key
            ):
                raise ValueError(
                    "indicator lineage references an unknown entry"
                )
            source = by_key[edge.from_indicator_key]
            target = by_key[edge.to_indicator_key]
            if (
                source.economy_code != target.economy_code
                or source.event_family != target.event_family
            ):
                raise ValueError("indicator lineage changes economy or family")
            if edge.source_key not in {
                source.legal_producer_source_key,
                target.legal_producer_source_key,
            }:
                raise ValueError("indicator lineage uses an unrelated producer")
        _reject_lineage_cycles(by_key, lineage)
        object.__setattr__(self, "lineage", lineage)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-indicator-catalog", self.identity_payload()
        )
        supplied = _optional_text(self.catalog_id)
        if supplied is not None and supplied != expected:
            raise ValueError("catalog_id does not match deterministic identity")
        object.__setattr__(self, "catalog_id", expected)

    def entry(self, indicator_key: str) -> EconomicIndicatorCatalogEntryV1:
        key = _key(indicator_key, "indicator_key")
        matches = [item for item in self.entries if item.indicator_key == key]
        if len(matches) != 1:
            raise ValueError("expected exactly one catalog indicator")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "catalog_version": self.catalog_version,
            "reviewed_on": self.reviewed_on,
            "source_registry_id": self.source_registry_id,
            "scoped_economies": list(self.scoped_economies),
            "scoped_families": [item.value for item in self.scoped_families],
            "entries": [item.to_dict() for item in self.entries],
            "lineage": [item.to_dict() for item in self.lineage],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "catalog_id": self.catalog_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicIndicatorCatalogV1:
        return cls(
            catalog_version=str(data.get("catalog_version", "")),
            reviewed_on=str(data.get("reviewed_on", "")),
            source_registry_id=str(data.get("source_registry_id", "")),
            scoped_economies=tuple(
                str(item) for item in _sequence(data.get("scoped_economies"))
            ),
            scoped_families=tuple(
                EconomicEventFamily.from_value(str(item))
                for item in _sequence(data.get("scoped_families"))
            ),
            entries=tuple(
                EconomicIndicatorCatalogEntryV1.from_dict(_mapping(item))
                for item in _sequence(data.get("entries"))
            ),
            lineage=tuple(
                EconomicIndicatorLineageV1.from_dict(_mapping(item))
                for item in _sequence(data.get("lineage"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            catalog_id=str(data.get("catalog_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicIndicatorCatalogV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic indicator catalog is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicExpectedOccurrenceV1:
    """One source-supported state in the qualified expected-event surface."""

    logical_event_key: str
    indicator_key: str
    rule_id: str
    reference_period: str
    reference_period_end_ns: int
    expected_for_ns: int
    status: EconomicExpectedOccurrenceStatus
    status_known_at_ns: int
    source_key: str
    source_occurrence_id: str
    evidence_sha256: str
    reason: str | None
    occurrence_id: str = ""
    schema_version: str = ECONOMIC_EXPECTED_OCCURRENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_EXPECTED_OCCURRENCE_SCHEMA_VERSION:
            raise ValueError("unsupported expected-occurrence schema")
        for name in (
            "logical_event_key",
            "indicator_key",
            "source_key",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "source_occurrence_id",
            _required_text(self.source_occurrence_id, "source_occurrence_id"),
        )
        rule_id = _required_text(self.rule_id, "rule_id")
        if not rule_id.startswith("economic-expected-release-rule:sha256:"):
            raise ValueError("rule_id is not an expected release-rule identity")
        object.__setattr__(self, "rule_id", rule_id)
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
            self,
            "expected_for_ns",
            _bounded_ns(self.expected_for_ns, "expected_for_ns"),
        )
        if self.reference_period_end_ns > self.expected_for_ns:
            raise ValueError("reference period ends after expected release")
        object.__setattr__(
            self,
            "status_known_at_ns",
            _bounded_ns(self.status_known_at_ns, "status_known_at_ns"),
        )
        object.__setattr__(
            self,
            "status",
            EconomicExpectedOccurrenceStatus.from_value(self.status),
        )
        digest = _required_text(self.evidence_sha256, "evidence_sha256")
        if _SHA256_RE.fullmatch(digest) is None:
            raise ValueError(
                "evidence_sha256 must be a lowercase SHA-256 digest"
            )
        object.__setattr__(self, "evidence_sha256", digest)
        reason = _optional_text(self.reason)
        if (
            self.status
            in {
                EconomicExpectedOccurrenceStatus.CANCELLED,
                EconomicExpectedOccurrenceStatus.DISCONTINUED,
                EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
                EconomicExpectedOccurrenceStatus.UNRESOLVED,
            }
            and reason is None
        ):
            raise ValueError("non-release expected status requires a reason")
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "economic-expected-occurrence", self.identity_payload()
        )
        supplied = _optional_text(self.occurrence_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "occurrence_id does not match deterministic identity"
            )
        object.__setattr__(self, "occurrence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "indicator_key": self.indicator_key,
            "rule_id": self.rule_id,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "expected_for_ns": self.expected_for_ns,
            "status": self.status.value,
            "status_known_at_ns": self.status_known_at_ns,
            "source_key": self.source_key,
            "source_occurrence_id": self.source_occurrence_id,
            "evidence_sha256": self.evidence_sha256,
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "occurrence_id": self.occurrence_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicExpectedOccurrenceV1:
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            indicator_key=str(data.get("indicator_key", "")),
            rule_id=str(data.get("rule_id", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            expected_for_ns=cast(int, data.get("expected_for_ns")),
            status=EconomicExpectedOccurrenceStatus.from_value(
                str(data.get("status", ""))
            ),
            status_known_at_ns=cast(int, data.get("status_known_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_occurrence_id=str(data.get("source_occurrence_id", "")),
            evidence_sha256=str(data.get("evidence_sha256", "")),
            reason=_optional_text(data.get("reason")),
            occurrence_id=str(data.get("occurrence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicExpectedOccurrenceV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic expected occurrence is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicIndicatorCoverageSliceV1:
    """Coverage result for one indicator and bounded era."""

    indicator_key: str
    expected_model_qualified: bool
    expected_count: int
    releasable_expected_count: int
    reconstructed_count: int
    missing_logical_event_keys: tuple[str, ...]
    status_counts: tuple[tuple[str, int], ...]
    structural_coverage: float | None
    limitations: tuple[str, ...]
    schema_version: str = ECONOMIC_INDICATOR_COVERAGE_SLICE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_INDICATOR_COVERAGE_SLICE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported indicator coverage-slice schema")
        object.__setattr__(
            self, "indicator_key", _key(self.indicator_key, "indicator_key")
        )
        if not isinstance(self.expected_model_qualified, bool):
            raise TypeError("expected_model_qualified must be boolean")
        for name in (
            "expected_count",
            "releasable_expected_count",
            "reconstructed_count",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            self.releasable_expected_count > self.expected_count
            or self.reconstructed_count > self.releasable_expected_count
        ):
            raise ValueError("indicator coverage counts are inconsistent")
        missing = _keys(
            self.missing_logical_event_keys, "missing_logical_event_keys"
        )
        if (
            len(missing)
            != self.releasable_expected_count - self.reconstructed_count
        ):
            raise ValueError("indicator missing-event count is inconsistent")
        object.__setattr__(self, "missing_logical_event_keys", missing)
        counts = tuple(
            sorted((str(key), int(value)) for key, value in self.status_counts)
        )
        if len({key for key, _ in counts}) != len(counts):
            raise ValueError(
                "expected occurrence status counts repeat a status"
            )
        if (
            any(value < 0 for _, value in counts)
            or sum(value for _, value in counts) != self.expected_count
        ):
            raise ValueError(
                "expected occurrence status counts are inconsistent"
            )
        if any(
            key not in {item.value for item in EconomicExpectedOccurrenceStatus}
            for key, _ in counts
        ):
            raise ValueError("coverage slice contains an unknown status")
        object.__setattr__(self, "status_counts", counts)
        if (
            not self.expected_model_qualified
            or self.releasable_expected_count == 0
        ):
            if self.structural_coverage is not None:
                raise ValueError(
                    "unqualified or empty model cannot report coverage"
                )
        else:
            expected = self.reconstructed_count / self.releasable_expected_count
            if self.structural_coverage is None or not math.isclose(
                self.structural_coverage, expected, rel_tol=0.0, abs_tol=1e-15
            ):
                raise ValueError(
                    "structural coverage differs from exact counts"
                )
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "indicator_key": self.indicator_key,
            "expected_model_qualified": self.expected_model_qualified,
            "expected_count": self.expected_count,
            "releasable_expected_count": self.releasable_expected_count,
            "reconstructed_count": self.reconstructed_count,
            "missing_logical_event_keys": list(self.missing_logical_event_keys),
            "status_counts": [
                {"status": key, "count": value}
                for key, value in self.status_counts
            ],
            "structural_coverage": self.structural_coverage,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicIndicatorCoverageSliceV1:
        return cls(
            indicator_key=str(data.get("indicator_key", "")),
            expected_model_qualified=cast(
                bool, data.get("expected_model_qualified")
            ),
            expected_count=cast(int, data.get("expected_count")),
            releasable_expected_count=cast(
                int, data.get("releasable_expected_count")
            ),
            reconstructed_count=cast(int, data.get("reconstructed_count")),
            missing_logical_event_keys=tuple(
                str(item)
                for item in _sequence(data.get("missing_logical_event_keys"))
            ),
            status_counts=tuple(
                (
                    str(_mapping(item, "status_count").get("status", "")),
                    cast(
                        int,
                        _mapping(item, "status_count").get("count"),
                    ),
                )
                for item in _sequence(data.get("status_counts"))
            ),
            structural_coverage=cast(
                float | None, data.get("structural_coverage")
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicIndicatorCoverageAuditV1:
    """Machine-verifiable catalog coverage, gaps, and unmatched releases."""

    catalog_id: str
    start_ns: int
    end_ns: int
    slices: tuple[EconomicIndicatorCoverageSliceV1, ...]
    unmatched_release_ids: tuple[str, ...]
    audit_id: str = ""
    schema_version: str = ECONOMIC_INDICATOR_COVERAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_INDICATOR_COVERAGE_AUDIT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported indicator coverage-audit schema")
        object.__setattr__(
            self, "catalog_id", _required_text(self.catalog_id, "catalog_id")
        )
        if not self.catalog_id.startswith("economic-indicator-catalog:sha256:"):
            raise ValueError("catalog_id is not an indicator catalog identity")
        start = _bounded_ns(self.start_ns, "start_ns")
        end = _bounded_ns(self.end_ns, "end_ns")
        if start >= end:
            raise ValueError("coverage interval must be nonempty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        slices = tuple(sorted(self.slices, key=lambda item: item.indicator_key))
        if len({item.indicator_key for item in slices}) != len(slices):
            raise ValueError("coverage audit repeats an indicator slice")
        object.__setattr__(self, "slices", slices)
        object.__setattr__(
            self,
            "unmatched_release_ids",
            _texts(self.unmatched_release_ids, "unmatched_release_ids"),
        )
        expected = _stable_id(
            "economic-indicator-coverage-audit", self.identity_payload()
        )
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def unexplained_gaps(self) -> tuple[str, ...]:
        return tuple(
            key
            for item in self.slices
            for key in item.missing_logical_event_keys
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "catalog_id": self.catalog_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "slices": [item.to_dict() for item in self.slices],
            "unmatched_release_ids": list(self.unmatched_release_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicIndicatorCoverageAuditV1:
        return cls(
            catalog_id=str(data.get("catalog_id", "")),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            slices=tuple(
                EconomicIndicatorCoverageSliceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("slices"))
            ),
            unmatched_release_ids=tuple(
                str(item)
                for item in _sequence(data.get("unmatched_release_ids"))
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicIndicatorCoverageAuditV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic indicator coverage audit is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


_FAMILY_METADATA: Mapping[EconomicEventFamily, tuple[str, str, int]] = {
    EconomicEventFamily.MONETARY_POLICY: (
        "policy-rate-and-decisions",
        "Monetary policy decisions",
        3,
    ),
    EconomicEventFamily.INFLATION_PRICES: (
        "consumer-and-producer-prices",
        "Consumer and producer prices",
        3,
    ),
    EconomicEventFamily.LABOUR_MARKET: (
        "employment-unemployment-and-earnings",
        "Employment, unemployment, and earnings",
        3,
    ),
    EconomicEventFamily.GDP_NATIONAL_ACCOUNTS: (
        "gdp-and-national-accounts",
        "GDP and national accounts",
        3,
    ),
    EconomicEventFamily.RETAIL_CONSUMPTION: (
        "retail-sales-and-household-consumption",
        "Retail sales and household consumption",
        2,
    ),
    EconomicEventFamily.INDUSTRIAL_PRODUCTION: (
        "industrial-production",
        "Industrial production",
        2,
    ),
    EconomicEventFamily.TRADE_EXTERNAL: (
        "trade-and-external-accounts",
        "Trade and external accounts",
        2,
    ),
    EconomicEventFamily.HOUSING: (
        "housing-and-construction",
        "Housing and construction",
        1,
    ),
    EconomicEventFamily.CONFIDENCE_SURVEY: (
        "official-confidence-and-tendency-surveys",
        "Official confidence and tendency surveys",
        1,
    ),
    EconomicEventFamily.MONEY_CREDIT: (
        "money-credit-and-financial-statistics",
        "Money, credit, and financial statistics",
        1,
    ),
    EconomicEventFamily.FISCAL: ("government-finance", "Government finance", 1),
    EconomicEventFamily.OTHER_OFFICIAL: (
        "other-official-macro",
        "Other official macroeconomic releases",
        0,
    ),
}

_CURRENCIES_BY_ECONOMY: Mapping[str, tuple[str, ...]] = {
    "AU": ("AUD",),
    "CA": ("CAD",),
    "CH": ("CHF",),
    "CZ": ("CZK",),
    "DE": ("EUR",),
    "DK": ("DKK",),
    "EA": ("EUR",),
    "FR": ("EUR",),
    "GB": ("GBP",),
    "HK": ("HKD",),
    "HU": ("HUF",),
    "JP": ("JPY",),
    "MX": ("MXN",),
    "NO": ("NOK",),
    "NZ": ("NZD",),
    "PL": ("PLN",),
    "SE": ("SEK",),
    "SG": ("SGD",),
    "TR": ("TRY",),
    "US": ("USD",),
    "ZA": ("ZAR",),
}


def build_official_indicator_catalog(
    registry: OfficialSourceRegistryV1,
    *,
    catalog_version: str = "1",
) -> EconomicIndicatorCatalogV1:
    """Build the complete conservative economy/family bootstrap catalog.

    These cells identify official ownership and semantic families.  Their
    recurrence rules intentionally remain unqualified until a series adapter
    supplies exact schedule evidence; downstream coverage therefore remains
    ``None`` instead of turning family-level placeholders into fake events.
    """
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("indicator catalog requires a v1 official registry")
    if registry.scoped_economies != SCOPED_ECONOMIES:
        raise ValueError(
            "official indicator bootstrap requires the 21-economy scope"
        )
    entries: list[EconomicIndicatorCatalogEntryV1] = []
    for economy in registry.scoped_economies:
        for family in EconomicEventFamily:
            source = registry.primary_source(economy, family)
            indicator_id, display_name, importance = _FAMILY_METADATA[family]
            indicator_key = f"{economy.lower()}.{indicator_id}"
            evidence = source.verification_evidence_uris or (
                source.endpoint_uri_template,
            )
            era = EconomicIndicatorMethodologyEraV1(
                era_key=f"{indicator_key}.bootstrap",
                effective_from="1970-01-01",
                effective_to=None,
                source_series_ids=source.source_series_ids,
                source_table_ids=source.source_table_ids,
                source_release_ids=source.source_release_ids,
                unit="source-defined",
                scale=1.0,
                seasonal_basis="source-defined; exact series qualification required",
                reference_period_convention="source-defined; exact series qualification required",
                methodology_evidence=evidence,
                notes=(
                    "Family-level bootstrap era; it is not an exact source series.",
                ),
            )
            rule = EconomicExpectedReleaseRuleV1(
                rule_key=f"{indicator_key}.bootstrap",
                frequency=EconomicIndicatorFrequency.MIXED,
                effective_from="1970-01-01",
                effective_to=None,
                source_key=source.source_key,
                source_timezone=source.source_timezone,
                time_precision=source.availability_time_precision,
                rule_description="Enumerate exact series rules from the registered official release calendar and archives.",
                qualified=False,
                qualification_evidence=(),
                limitations=(
                    "Family ownership is reviewed, but exact indicator recurrence and historical existence are not yet qualified.",
                ),
            )
            entries.append(
                EconomicIndicatorCatalogEntryV1(
                    indicator_key=indicator_key,
                    economy_code=economy,
                    economy_name=source.economy_name,
                    affected_currencies=_CURRENCIES_BY_ECONOMY[economy],
                    event_family=family,
                    indicator_id=indicator_id,
                    display_name=f"{source.economy_name} {display_name}",
                    aliases=(display_name, indicator_id.replace("-", " ")),
                    legal_producer_source_key=source.source_key,
                    frequency=EconomicIndicatorFrequency.MIXED,
                    reference_period_convention="Exact convention is versioned by qualified source series.",
                    normal_schedule_rule="Exact official recurrence must be qualified before expected occurrences are emitted.",
                    release_stages=(EconomicReleaseStage.INITIAL,),
                    unit="source-defined",
                    scale=1.0,
                    seasonal_basis="source-defined",
                    valid_from="1970-01-01",
                    valid_to=None,
                    methodology_eras=(era,),
                    expected_release_rules=(rule,),
                    structural_importance=importance,
                    importance_policy_version="structural-fx-family-prior.v1",
                    importance_rationale="Provider-neutral structural prior only; it is neither a copied vendor label nor retrospective impact evidence.",
                    limitations=(
                        "This bootstrap cell proves catalog coverage and official ownership, not exact series, occurrence, or vintage coverage.",
                    ),
                )
            )
    result = EconomicIndicatorCatalogV1(
        catalog_version=catalog_version,
        reviewed_on=registry.reviewed_on,
        source_registry_id=registry.registry_id,
        scoped_economies=registry.scoped_economies,
        scoped_families=tuple(EconomicEventFamily),
        entries=tuple(entries),
        lineage=(),
        limitations=(
            "Professional calendars may be used only as non-authoritative coverage checklists.",
            "Structural coverage is reported only for exact, evidence-qualified occurrence rules.",
        ),
    )
    validate_indicator_catalog_sources(result, registry)
    return result


def validate_indicator_catalog_sources(
    catalog: EconomicIndicatorCatalogV1,
    registry: OfficialSourceRegistryV1,
) -> None:
    """Fail closed if a catalog definition drifts from its frozen registry."""
    if catalog.source_registry_id != registry.registry_id:
        raise ValueError("indicator catalog source registry identity differs")
    for entry in catalog.entries:
        source = registry.source(entry.legal_producer_source_key)
        if not source.legal_producer:
            raise ValueError("indicator catalog source is not a legal producer")
        if (
            source.economy_code != entry.economy_code
            or entry.event_family not in source.event_families
        ):
            raise ValueError(
                "indicator catalog source does not own its economy/family"
            )
        if any(
            rule.source_key != source.source_key
            for rule in entry.expected_release_rules
        ):
            raise ValueError("indicator expected release rule changes source")


def audit_economic_indicator_coverage(
    catalog: EconomicIndicatorCatalogV1,
    occurrences: Sequence[EconomicExpectedOccurrenceV1],
    releases: Sequence[EconomicCalendarReleaseV1],
    *,
    start_ns: int,
    end_ns: int,
) -> EconomicIndicatorCoverageAuditV1:
    """Compare qualified expected occurrences with reconstructed releases."""
    start = _bounded_ns(start_ns, "start_ns")
    end = _bounded_ns(end_ns, "end_ns")
    if start >= end:
        raise ValueError("coverage interval must be nonempty")
    if len(occurrences) > MAX_ECONOMIC_EXPECTED_OCCURRENCES:
        raise ValueError("expected occurrence count exceeds v1 bound")
    by_indicator = {item.indicator_key: item for item in catalog.entries}
    rules_by_id = {
        rule.rule_id: rule
        for entry in catalog.entries
        for rule in entry.expected_release_rules
    }
    rule_owner = {
        rule.rule_id: entry
        for entry in catalog.entries
        for rule in entry.expected_release_rules
    }
    selected_occurrences: list[EconomicExpectedOccurrenceV1] = []
    seen_events: set[str] = set()
    for occurrence in occurrences:
        if not isinstance(occurrence, EconomicExpectedOccurrenceV1):
            raise TypeError("coverage requires v1 expected occurrences")
        entry = by_indicator.get(occurrence.indicator_key)
        if entry is None or rule_owner.get(occurrence.rule_id) != entry:
            raise ValueError(
                "expected occurrence is not bound to its catalog rule"
            )
        rule = rules_by_id[occurrence.rule_id]
        occurrence_date = (
            datetime.fromtimestamp(
                occurrence.expected_for_ns // 1_000_000_000,
                tz=timezone.utc,
            )
            .astimezone(ZoneInfo(rule.source_timezone))
            .date()
            .isoformat()
        )
        if occurrence_date < rule.effective_from or (
            rule.effective_to is not None
            and occurrence_date > rule.effective_to
        ):
            raise ValueError(
                "expected occurrence lies outside its release rule"
            )
        if occurrence.source_key != entry.legal_producer_source_key:
            raise ValueError("expected occurrence changes legal producer")
        if occurrence.logical_event_key in seen_events:
            raise ValueError("expected occurrence repeats a logical event")
        seen_events.add(occurrence.logical_event_key)
        if start <= occurrence.expected_for_ns < end:
            selected_occurrences.append(occurrence)
    occurrence_by_event = {
        item.logical_event_key: item for item in selected_occurrences
    }
    reconstructed_events: dict[str, EconomicCalendarReleaseV1] = {}
    unmatched: list[str] = []
    for release in releases:
        if not isinstance(release, EconomicCalendarReleaseV1):
            raise TypeError("coverage requires v1 economic releases")
        if release.status not in {
            EconomicReleaseStatus.RELEASED,
            EconomicReleaseStatus.UNSCHEDULED,
        }:
            continue
        matched_occurrence = occurrence_by_event.get(release.logical_event_key)
        if matched_occurrence is None:
            if start <= release.event_time_ns < end:
                unmatched.append(release.release_id)
            continue
        entry = by_indicator[matched_occurrence.indicator_key]
        if (
            release.series_key != entry.indicator_key
            or release.economy_code != entry.economy_code
            or release.event_family != entry.event_family
            or release.indicator_id != entry.indicator_id
        ):
            raise ValueError(
                "reconstructed release differs from catalog identity"
            )
        if (
            matched_occurrence.status
            is not EconomicExpectedOccurrenceStatus.RELEASED
        ):
            raise ValueError(
                "reconstructed release has a non-released expected occurrence"
            )
        reconstructed_events.setdefault(release.logical_event_key, release)
    grouped: dict[str, list[EconomicExpectedOccurrenceV1]] = {
        key: [] for key in by_indicator
    }
    for occurrence in selected_occurrences:
        grouped[occurrence.indicator_key].append(occurrence)
    slices: list[EconomicIndicatorCoverageSliceV1] = []
    excluded = {
        EconomicExpectedOccurrenceStatus.CANCELLED,
        EconomicExpectedOccurrenceStatus.DISCONTINUED,
    }
    for entry in catalog.entries:
        values = grouped[entry.indicator_key]
        releasable = [item for item in values if item.status not in excluded]
        reconstructed = [
            item
            for item in releasable
            if item.logical_event_key in reconstructed_events
        ]
        missing = tuple(
            item.logical_event_key
            for item in releasable
            if item.logical_event_key not in reconstructed_events
        )
        counts = Counter(item.status.value for item in values)
        qualified = entry.has_qualified_expected_model and all(
            rules_by_id[item.rule_id].qualified for item in values
        )
        coverage = (
            len(reconstructed) / len(releasable)
            if qualified and releasable
            else None
        )
        limitations = [
            "Cancelled and discontinued occurrences remain state evidence but are excluded from the releasable denominator."
        ]
        if not qualified:
            limitations.append(
                "Expected occurrence model is unqualified; structural coverage is intentionally unavailable."
            )
        elif not releasable:
            limitations.append(
                "No releasable expected occurrences exist in the bounded interval."
            )
        slices.append(
            EconomicIndicatorCoverageSliceV1(
                indicator_key=entry.indicator_key,
                expected_model_qualified=qualified,
                expected_count=len(values),
                releasable_expected_count=len(releasable),
                reconstructed_count=len(reconstructed),
                missing_logical_event_keys=missing,
                status_counts=tuple(counts.items()),
                structural_coverage=coverage,
                limitations=tuple(limitations),
            )
        )
    return EconomicIndicatorCoverageAuditV1(
        catalog_id=catalog.catalog_id,
        start_ns=start,
        end_ns=end,
        slices=tuple(slices),
        unmatched_release_ids=tuple(unmatched),
    )


def write_economic_indicator_catalog(
    catalog: EconomicIndicatorCatalogV1, path: str | Path
) -> Path:
    """Write a bounded deterministic catalog artifact."""
    destination = Path(path)
    payload = catalog.to_json().encode("utf-8")
    if len(payload) > MAX_ECONOMIC_INDICATOR_CATALOG_BYTES:
        raise ValueError("economic indicator catalog exceeds byte bound")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload + b"\n")
    return destination


def read_economic_indicator_catalog(
    path: str | Path,
) -> EconomicIndicatorCatalogV1:
    """Read and verify a bounded deterministic catalog artifact."""
    source = Path(path)
    if source.stat().st_size > MAX_ECONOMIC_INDICATOR_CATALOG_BYTES:
        raise ValueError("economic indicator catalog exceeds byte bound")
    try:
        return EconomicIndicatorCatalogV1.from_json(
            source.read_text(encoding="utf-8")
        )
    except UnicodeDecodeError as exc:
        raise ValueError("economic indicator catalog is not UTF-8") from exc


def _reject_lineage_cycles(
    entries: Mapping[str, EconomicIndicatorCatalogEntryV1],
    lineage: Sequence[EconomicIndicatorLineageV1],
) -> None:
    outgoing: dict[str, list[str]] = {key: [] for key in entries}
    for edge in lineage:
        outgoing[edge.from_indicator_key].append(edge.to_indicator_key)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            raise ValueError("indicator lineage contains a cycle")
        if key in visited:
            return
        visiting.add(key)
        for target in outgoing[key]:
            visit(target)
        visiting.remove(key)
        visited.add(key)

    for key in entries:
        visit(key)


__all__ = [
    "ECONOMIC_EXPECTED_OCCURRENCE_SCHEMA_VERSION",
    "ECONOMIC_EXPECTED_RELEASE_RULE_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_CATALOG_ENTRY_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_CATALOG_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_COVERAGE_AUDIT_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_COVERAGE_SLICE_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_LINEAGE_SCHEMA_VERSION",
    "ECONOMIC_INDICATOR_METHODOLOGY_ERA_SCHEMA_VERSION",
    "MAX_ECONOMIC_EXPECTED_OCCURRENCES",
    "MAX_ECONOMIC_INDICATORS",
    "MAX_ECONOMIC_INDICATOR_CATALOG_BYTES",
    "EconomicExpectedOccurrenceStatus",
    "EconomicExpectedOccurrenceV1",
    "EconomicExpectedReleaseRuleV1",
    "EconomicIndicatorCatalogEntryV1",
    "EconomicIndicatorCatalogV1",
    "EconomicIndicatorCoverageAuditV1",
    "EconomicIndicatorCoverageSliceV1",
    "EconomicIndicatorFrequency",
    "EconomicIndicatorLineageKind",
    "EconomicIndicatorLineageV1",
    "EconomicIndicatorMethodologyEraV1",
    "audit_economic_indicator_coverage",
    "build_official_indicator_catalog",
    "read_economic_indicator_catalog",
    "validate_indicator_catalog_sources",
    "write_economic_indicator_catalog",
]
