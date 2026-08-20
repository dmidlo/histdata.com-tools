"""Authoritative scientific target for HistData reconstruction.

The contracts in this module make the reconstruction estimand, assumptions,
context missingness semantics, validity boundary, and retained-artifact
migration treatment one content-addressed object.  The ledger is deliberately
provider-neutral at the domain boundary, while the current factory is scoped
only to HistData.com ASCII/T reconstruction.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from histdatacom.market_context import (
    CftcPositioningQueryStatus,
    CftcPositioningQueryV1,
    MarketContextMissingReason,
    MarketContextPrecision,
    MarketContextQueryStatus,
    MarketContextQueryV1,
    MarketContextView,
)
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.information import InformationMode

RECONSTRUCTION_ESTIMAND_SCHEMA_VERSION = (
    "histdatacom.reconstruction-estimand.v1"
)
RECONSTRUCTION_ASSUMPTION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-assumption.v1"
)
RECONSTRUCTION_CONTEXT_MISSINGNESS_DEFINITION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-context-missingness-definition.v1"
)
RECONSTRUCTION_LEGACY_SCIENTIFIC_REPLAY_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-legacy-scientific-replay-policy.v1"
)
RECONSTRUCTION_SCIENTIFIC_LEDGER_SCHEMA_VERSION = (
    "histdatacom.reconstruction-scientific-ledger.v1"
)
RECONSTRUCTION_CONDITIONING_STATE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-conditioning-state.v1"
)

RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND = (
    "reconstruction_scientific_ledger_v1"
)
RECONSTRUCTION_SCIENTIFIC_NONCLAIM = (
    "Output is a plausible counterfactual ensemble conditioned on declared "
    "artifacts and constraints; it is not recovered historical truth."
)
RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL = "invalid-for-backtest"
CURRENT_HISTDATA_SCIENTIFIC_SCOPE = "histdata.com/ascii/tick/eurusd-triangle"

MAX_SCIENTIFIC_LEDGER_BYTES = 1_048_576
MAX_SCIENTIFIC_TEXT_LENGTH = 16_384
MAX_SCIENTIFIC_ITEMS = 64

_REQUIRED_ASSUMPTION_KEYS = frozenset(
    {
        "anchor-conditioning-does-not-identify-path",
        "asynchronous-triangle-support-is-assumed",
        "context-completeness-varies",
        "ex-post-products-are-invalid-for-backtest",
        "modern-reference-is-not-latent-truth",
        "observation-and-market-processes-may-be-confounded",
        "operator-transport-is-support-bounded",
    }
)


class ReconstructionContextMissingnessCategory(str, Enum):
    """Exhaustive declared semantics for one context conditioning state."""

    COMPLETE_CALENDAR_NO_MATCHING_EVENT = "complete_calendar_no_matching_event"
    MATCHED_COMPLETE_CONTEMPORANEOUS_FIELDS = (
        "matched_complete_contemporaneous_fields"
    )
    MATCHED_MISSING_CONTEMPORANEOUS_FIELDS = (
        "matched_missing_contemporaneous_fields"
    )
    INCOMPLETE_CORPUS_COVERAGE = "incomplete_corpus_coverage"
    EVENT_KNOWN_ONLY_EX_POST = "event_known_only_ex_post"
    UNCERTAIN_FIRST_KNOWN_OR_PUBLICATION_TIME = (
        "uncertain_first_known_or_publication_time"
    )
    CFTC_LIMITED_AVAILABILITY = "cftc_limited_availability"
    EXPLICIT_QUALIFIED_UNCONDITIONED_MODE = (
        "explicit_qualified_unconditioned_mode"
    )

    @classmethod
    def from_value(
        cls, value: str | ReconstructionContextMissingnessCategory
    ) -> ReconstructionContextMissingnessCategory:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError(
                "unsupported reconstruction context missingness category"
            ) from err


class ReconstructionContextSourceKind(str, Enum):
    """The bounded query surface classified by one conditioning state."""

    MARKET_CONTEXT = "market_context"
    CFTC_POSITIONING = "cftc_positioning"


class ReconstructionContextCompleteness(str, Enum):
    """Operational completeness of a conditioning input."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    INCOMPLETE = "incomplete"
    UNAVAILABLE = "unavailable"
    QUALIFIED_UNCONDITIONED = "qualified_unconditioned"


@dataclass(frozen=True, slots=True)
class ReconstructionEstimandV1:
    """Exact target distribution and scientific nonclaim."""

    target: str
    observation_equation: str
    conditional_equation: str
    product_equation: str
    final_product_law: str
    nonclaim: str
    estimand_id: str = ""
    schema_version: str = RECONSTRUCTION_ESTIMAND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RECONSTRUCTION_ESTIMAND_SCHEMA_VERSION
        )
        for name in (
            "target",
            "observation_equation",
            "conditional_equation",
            "product_equation",
            "final_product_law",
            "nonclaim",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        expected = _stable_id(
            "reconstruction-estimand", self.identity_payload()
        )
        supplied = _optional_text(self.estimand_id)
        if supplied is not None and supplied != expected:
            raise ValueError("estimand_id differs from scientific content")
        object.__setattr__(self, "estimand_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "target": self.target,
            "observation_equation": self.observation_equation,
            "conditional_equation": self.conditional_equation,
            "product_equation": self.product_equation,
            "final_product_law": self.final_product_law,
            "nonclaim": self.nonclaim,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "estimand_id": self.estimand_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionEstimandV1:
        _require_schema_value(data, RECONSTRUCTION_ESTIMAND_SCHEMA_VERSION)
        return cls(
            target=str(data.get("target", "")),
            observation_equation=str(data.get("observation_equation", "")),
            conditional_equation=str(data.get("conditional_equation", "")),
            product_equation=str(data.get("product_equation", "")),
            final_product_law=str(data.get("final_product_law", "")),
            nonclaim=str(data.get("nonclaim", "")),
            estimand_id=str(data.get("estimand_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionAssumptionV1:
    """One machine-identifiable assumption with applicability scopes."""

    key: str
    statement: str
    applicability_scopes: tuple[str, ...]
    limitation: str
    assumption_id: str = ""
    schema_version: str = RECONSTRUCTION_ASSUMPTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RECONSTRUCTION_ASSUMPTION_SCHEMA_VERSION
        )
        object.__setattr__(self, "key", _identifier(self.key))
        object.__setattr__(self, "statement", _required_text(self.statement))
        scopes = _normalized_text_tuple(self.applicability_scopes)
        if not scopes:
            raise ValueError(
                "scientific assumption requires applicability scopes"
            )
        object.__setattr__(self, "applicability_scopes", scopes)
        object.__setattr__(self, "limitation", _required_text(self.limitation))
        expected = _stable_id(
            "reconstruction-assumption", self.identity_payload()
        )
        supplied = _optional_text(self.assumption_id)
        if supplied is not None and supplied != expected:
            raise ValueError("assumption_id differs from scientific content")
        object.__setattr__(self, "assumption_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "key": self.key,
            "statement": self.statement,
            "applicability_scopes": list(self.applicability_scopes),
            "limitation": self.limitation,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "assumption_id": self.assumption_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionAssumptionV1:
        _require_schema_value(data, RECONSTRUCTION_ASSUMPTION_SCHEMA_VERSION)
        return cls(
            key=str(data.get("key", "")),
            statement=str(data.get("statement", "")),
            applicability_scopes=_string_tuple(
                data.get("applicability_scopes")
            ),
            limitation=str(data.get("limitation", "")),
            assumption_id=str(data.get("assumption_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionContextMissingnessDefinitionV1:
    """Identity-bearing semantics for one context missingness category."""

    category: ReconstructionContextMissingnessCategory
    statement: str
    evidence_requirement: str
    conditioning_treatment: str
    provider_row_absence_proves_no_market_event: bool = False
    definition_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_CONTEXT_MISSINGNESS_DEFINITION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_CONTEXT_MISSINGNESS_DEFINITION_SCHEMA_VERSION,
        )
        object.__setattr__(
            self,
            "category",
            ReconstructionContextMissingnessCategory.from_value(self.category),
        )
        for name in (
            "statement",
            "evidence_requirement",
            "conditioning_treatment",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(
            self.provider_row_absence_proves_no_market_event, bool
        ):
            raise TypeError(
                "provider_row_absence_proves_no_market_event must be boolean"
            )
        if self.provider_row_absence_proves_no_market_event:
            raise ValueError(
                "provider-row absence cannot prove no market-moving event occurred"
            )
        expected = _stable_id(
            "reconstruction-context-missingness", self.identity_payload()
        )
        supplied = _optional_text(self.definition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("context missingness definition_id differs")
        object.__setattr__(self, "definition_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "category": self.category.value,
            "statement": self.statement,
            "evidence_requirement": self.evidence_requirement,
            "conditioning_treatment": self.conditioning_treatment,
            "provider_row_absence_proves_no_market_event": (
                self.provider_row_absence_proves_no_market_event
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "definition_id": self.definition_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionContextMissingnessDefinitionV1:
        _require_schema_value(
            data, RECONSTRUCTION_CONTEXT_MISSINGNESS_DEFINITION_SCHEMA_VERSION
        )
        return cls(
            category=ReconstructionContextMissingnessCategory.from_value(
                str(data.get("category", ""))
            ),
            statement=str(data.get("statement", "")),
            evidence_requirement=str(data.get("evidence_requirement", "")),
            conditioning_treatment=str(data.get("conditioning_treatment", "")),
            provider_row_absence_proves_no_market_event=_strict_bool(
                data.get("provider_row_absence_proves_no_market_event"),
                "provider_row_absence_proves_no_market_event",
            ),
            definition_id=str(data.get("definition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionLegacyScientificReplayPolicyV1:
    """Fail-closed treatment of retained artifacts created before this ledger."""

    retained_release_line: str
    scientific_binding_status: str
    identity_replayable: bool
    execution_allowed_without_replan: bool
    migration_action: str
    claim_limit: str
    policy_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_LEGACY_SCIENTIFIC_REPLAY_POLICY_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_LEGACY_SCIENTIFIC_REPLAY_POLICY_SCHEMA_VERSION,
        )
        for name in (
            "retained_release_line",
            "scientific_binding_status",
            "migration_action",
            "claim_limit",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("identity_replayable", "execution_allowed_without_replan"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if self.scientific_binding_status != "legacy-unbound":
            raise ValueError(
                "legacy scientific binding status must be explicit"
            )
        if (
            not self.identity_replayable
            or self.execution_allowed_without_replan
        ):
            raise ValueError(
                "legacy artifacts must remain readable but require current replanning"
            )
        expected = _stable_id(
            "reconstruction-legacy-scientific-replay", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("legacy scientific replay policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "retained_release_line": self.retained_release_line,
            "scientific_binding_status": self.scientific_binding_status,
            "identity_replayable": self.identity_replayable,
            "execution_allowed_without_replan": (
                self.execution_allowed_without_replan
            ),
            "migration_action": self.migration_action,
            "claim_limit": self.claim_limit,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionLegacyScientificReplayPolicyV1:
        _require_schema_value(
            data, RECONSTRUCTION_LEGACY_SCIENTIFIC_REPLAY_POLICY_SCHEMA_VERSION
        )
        return cls(
            retained_release_line=str(data.get("retained_release_line", "")),
            scientific_binding_status=str(
                data.get("scientific_binding_status", "")
            ),
            identity_replayable=_strict_bool(
                data.get("identity_replayable"), "identity_replayable"
            ),
            execution_allowed_without_replan=_strict_bool(
                data.get("execution_allowed_without_replan"),
                "execution_allowed_without_replan",
            ),
            migration_action=str(data.get("migration_action", "")),
            claim_limit=str(data.get("claim_limit", "")),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionScientificLedgerV1:
    """One authoritative, content-addressed reconstruction science ledger."""

    scope: str
    estimand: ReconstructionEstimandV1
    assumptions: tuple[ReconstructionAssumptionV1, ...]
    context_missingness: tuple[
        ReconstructionContextMissingnessDefinitionV1, ...
    ]
    legacy_replay_policy: ReconstructionLegacyScientificReplayPolicyV1
    generated_row_origin: str
    generated_row_forbidden_claims: tuple[str, ...]
    invalid_for_backtest_label: str
    ledger_id: str = ""
    schema_version: str = RECONSTRUCTION_SCIENTIFIC_LEDGER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RECONSTRUCTION_SCIENTIFIC_LEDGER_SCHEMA_VERSION
        )
        object.__setattr__(self, "scope", _required_text(self.scope))
        if not isinstance(self.estimand, ReconstructionEstimandV1):
            raise TypeError("scientific ledger requires estimand v1")
        assumptions = tuple(sorted(self.assumptions, key=lambda item: item.key))
        if any(
            not isinstance(item, ReconstructionAssumptionV1)
            for item in assumptions
        ):
            raise TypeError(
                "scientific ledger assumptions must use v1 contracts"
            )
        if {item.key for item in assumptions} != _REQUIRED_ASSUMPTION_KEYS:
            raise ValueError("scientific ledger assumption set is incomplete")
        if len({item.assumption_id for item in assumptions}) != len(
            assumptions
        ):
            raise ValueError("scientific ledger assumption identities repeat")
        object.__setattr__(self, "assumptions", assumptions)
        definitions = tuple(
            sorted(
                self.context_missingness, key=lambda item: item.category.value
            )
        )
        if any(
            not isinstance(item, ReconstructionContextMissingnessDefinitionV1)
            for item in definitions
        ):
            raise TypeError("scientific ledger context definitions must use v1")
        if {item.category for item in definitions} != set(
            ReconstructionContextMissingnessCategory
        ):
            raise ValueError("scientific ledger context taxonomy is incomplete")
        if len({item.definition_id for item in definitions}) != len(
            definitions
        ):
            raise ValueError("scientific ledger context identities repeat")
        object.__setattr__(self, "context_missingness", definitions)
        if not isinstance(
            self.legacy_replay_policy,
            ReconstructionLegacyScientificReplayPolicyV1,
        ):
            raise TypeError("scientific ledger requires a legacy replay policy")
        object.__setattr__(
            self,
            "generated_row_origin",
            _required_text(self.generated_row_origin),
        )
        if self.generated_row_origin != "synthetic":
            raise ValueError(
                "generated reconstruction rows must use synthetic origin"
            )
        forbidden = _normalized_text_tuple(self.generated_row_forbidden_claims)
        if forbidden != ("broker history", "observed", "recovered truth"):
            raise ValueError("generated-row forbidden claims differ")
        object.__setattr__(self, "generated_row_forbidden_claims", forbidden)
        object.__setattr__(
            self,
            "invalid_for_backtest_label",
            _required_text(self.invalid_for_backtest_label),
        )
        if (
            self.invalid_for_backtest_label
            != RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
        ):
            raise ValueError("scientific ledger backtest label differs")
        if self.estimand.nonclaim != RECONSTRUCTION_SCIENTIFIC_NONCLAIM:
            raise ValueError("scientific ledger nonclaim differs")
        expected = _stable_id(
            "reconstruction-scientific-ledger", self.identity_payload()
        )
        supplied = _optional_text(self.ledger_id)
        if supplied is not None and supplied != expected:
            raise ValueError("scientific ledger_id differs from content")
        object.__setattr__(self, "ledger_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_SCIENTIFIC_LEDGER_BYTES:
            raise ValueError("scientific ledger exceeds payload limit")

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scope": self.scope,
            "estimand": self.estimand.to_dict(),
            "assumptions": [item.to_dict() for item in self.assumptions],
            "context_missingness": [
                item.to_dict() for item in self.context_missingness
            ],
            "legacy_replay_policy": self.legacy_replay_policy.to_dict(),
            "generated_row_origin": self.generated_row_origin,
            "generated_row_forbidden_claims": list(
                self.generated_row_forbidden_claims
            ),
            "invalid_for_backtest_label": self.invalid_for_backtest_label,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "ledger_id": self.ledger_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionScientificLedgerV1:
        _require_schema_value(
            data, RECONSTRUCTION_SCIENTIFIC_LEDGER_SCHEMA_VERSION
        )
        return cls(
            scope=str(data.get("scope", "")),
            estimand=ReconstructionEstimandV1.from_dict(
                _mapping(data.get("estimand"))
            ),
            assumptions=tuple(
                ReconstructionAssumptionV1.from_dict(item)
                for item in _mapping_sequence(data.get("assumptions"))
            ),
            context_missingness=tuple(
                ReconstructionContextMissingnessDefinitionV1.from_dict(item)
                for item in _mapping_sequence(data.get("context_missingness"))
            ),
            legacy_replay_policy=(
                ReconstructionLegacyScientificReplayPolicyV1.from_dict(
                    _mapping(data.get("legacy_replay_policy"))
                )
            ),
            generated_row_origin=str(data.get("generated_row_origin", "")),
            generated_row_forbidden_claims=_string_tuple(
                data.get("generated_row_forbidden_claims")
            ),
            invalid_for_backtest_label=str(
                data.get("invalid_for_backtest_label", "")
            ),
            ledger_id=str(data.get("ledger_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionScientificLedgerV1:
        try:
            value = json.loads(text)
        except json.JSONDecodeError as err:
            raise ValueError("scientific ledger contains invalid JSON") from err
        return cls.from_dict(_mapping(value))


@dataclass(frozen=True, slots=True)
class ReconstructionConditioningStateV1:
    """Ledger-bound completeness and information mode for one query input."""

    scientific_ledger_id: str
    source_kind: ReconstructionContextSourceKind
    source_artifact_id: str
    query_id: str
    information_mode: InformationMode
    completeness: ReconstructionContextCompleteness
    categories: tuple[ReconstructionContextMissingnessCategory, ...]
    status: str
    reason_codes: tuple[str, ...]
    missing_fields: tuple[str, ...] = ()
    qualified_unconditioned: bool = False
    invalid_for_backtest_reason: str | None = None
    state_id: str = ""
    schema_version: str = RECONSTRUCTION_CONDITIONING_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_CONDITIONING_STATE_SCHEMA_VERSION,
        )
        for name in ("scientific_ledger_id", "source_artifact_id", "query_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "source_kind",
            ReconstructionContextSourceKind(self.source_kind),
        )
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "completeness",
            ReconstructionContextCompleteness(self.completeness),
        )
        categories = tuple(
            sorted(
                {
                    ReconstructionContextMissingnessCategory.from_value(item)
                    for item in self.categories
                },
                key=lambda item: item.value,
            )
        )
        if not categories:
            raise ValueError("conditioning state requires context semantics")
        object.__setattr__(self, "categories", categories)
        object.__setattr__(self, "status", _required_text(self.status))
        reasons = _normalized_text_tuple(self.reason_codes)
        if not reasons:
            raise ValueError("conditioning state requires reason codes")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self, "missing_fields", _normalized_text_tuple(self.missing_fields)
        )
        if not isinstance(self.qualified_unconditioned, bool):
            raise TypeError("qualified_unconditioned must be boolean")
        qualified_category = (
            ReconstructionContextMissingnessCategory.EXPLICIT_QUALIFIED_UNCONDITIONED_MODE
            in categories
        )
        if self.qualified_unconditioned != qualified_category:
            raise ValueError("qualified-unconditioned category and flag differ")
        if qualified_category and self.completeness is not (
            ReconstructionContextCompleteness.QUALIFIED_UNCONDITIONED
        ):
            raise ValueError("qualified-unconditioned completeness differs")
        invalid = _optional_text(self.invalid_for_backtest_reason)
        if self.information_mode is InformationMode.EX_POST_RECONSTRUCTION:
            if invalid != RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL:
                raise ValueError(
                    "ex-post conditioning state requires invalid-for-backtest"
                )
        elif invalid is not None:
            raise ValueError(
                "ex-ante conditioning state cannot be backtest-invalid"
            )
        object.__setattr__(self, "invalid_for_backtest_reason", invalid)
        expected = _stable_id(
            "reconstruction-conditioning-state", self.identity_payload()
        )
        supplied = _optional_text(self.state_id)
        if supplied is not None and supplied != expected:
            raise ValueError("conditioning state_id differs")
        object.__setattr__(self, "state_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "scientific_ledger_id": self.scientific_ledger_id,
            "source_kind": self.source_kind.value,
            "source_artifact_id": self.source_artifact_id,
            "query_id": self.query_id,
            "information_mode": self.information_mode.value,
            "completeness": self.completeness.value,
            "categories": [item.value for item in self.categories],
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "missing_fields": list(self.missing_fields),
            "qualified_unconditioned": self.qualified_unconditioned,
            "invalid_for_backtest_reason": self.invalid_for_backtest_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "state_id": self.state_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionConditioningStateV1:
        _require_schema_value(
            data, RECONSTRUCTION_CONDITIONING_STATE_SCHEMA_VERSION
        )
        invalid = data.get("invalid_for_backtest_reason")
        return cls(
            scientific_ledger_id=str(data.get("scientific_ledger_id", "")),
            source_kind=ReconstructionContextSourceKind(
                str(data.get("source_kind", ""))
            ),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            query_id=str(data.get("query_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            completeness=ReconstructionContextCompleteness(
                str(data.get("completeness", ""))
            ),
            categories=tuple(
                ReconstructionContextMissingnessCategory.from_value(str(item))
                for item in _sequence(data.get("categories"))
            ),
            status=str(data.get("status", "")),
            reason_codes=_string_tuple(data.get("reason_codes")),
            missing_fields=_string_tuple(data.get("missing_fields")),
            qualified_unconditioned=_strict_bool(
                data.get("qualified_unconditioned"), "qualified_unconditioned"
            ),
            invalid_for_backtest_reason=(
                str(invalid) if invalid is not None else None
            ),
            state_id=str(data.get("state_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def classify_market_context_query(
    query: MarketContextQueryV1,
    *,
    ledger: ReconstructionScientificLedgerV1 | None = None,
) -> ReconstructionConditioningStateV1:
    """Classify one market-context query without equating absence with no event."""
    if not isinstance(query, MarketContextQueryV1):
        raise TypeError("market-context classification requires query v1")
    selected_ledger = (
        ledger or current_histdata_reconstruction_scientific_ledger()
    )
    categories: set[ReconstructionContextMissingnessCategory] = set()
    reasons = {f"market_context_status:{query.status.value}"}
    missing_fields: set[str] = set()
    if query.status is MarketContextQueryStatus.MISSING:
        reason = cast(MarketContextMissingReason, query.missing_reason)
        reasons.add(f"market_context_reason:{reason.value}")
        complete_calendar = bool(
            query.calendar_state is not None
            and query.calendar_state.profile_complete
        )
        reasons.add(
            "calendar_profile:complete"
            if complete_calendar
            else "calendar_profile:incomplete"
        )
        if (
            reason is MarketContextMissingReason.NO_MATCHING_EVENT
            and complete_calendar
        ):
            categories.add(
                ReconstructionContextMissingnessCategory.COMPLETE_CALENDAR_NO_MATCHING_EVENT
            )
            completeness = ReconstructionContextCompleteness.COMPLETE
        else:
            categories.add(
                ReconstructionContextMissingnessCategory.INCOMPLETE_CORPUS_COVERAGE
            )
            completeness = ReconstructionContextCompleteness.INCOMPLETE
        if reason is MarketContextMissingReason.NOT_AVAILABLE_AS_OF:
            categories.add(
                ReconstructionContextMissingnessCategory.EVENT_KNOWN_ONLY_EX_POST
            )
    else:
        required_fields = (
            "expected_value",
            "previous_value",
            "revised_previous_value",
        )
        missing_fields.update(
            name
            for event in query.events
            for name in required_fields
            if getattr(event, name) is None
        )
        categories.add(
            ReconstructionContextMissingnessCategory.MATCHED_MISSING_CONTEMPORANEOUS_FIELDS
            if missing_fields
            else ReconstructionContextMissingnessCategory.MATCHED_COMPLETE_CONTEMPORANEOUS_FIELDS
        )
        if query.view is MarketContextView.EX_POST and any(
            event.first_known_at_ns > query.start_ns
            or event.available_at_ns > query.start_ns
            for event in query.events
        ):
            categories.add(
                ReconstructionContextMissingnessCategory.EVENT_KNOWN_ONLY_EX_POST
            )
        if any(
            event.precision is not MarketContextPrecision.EXACT
            or event.ambiguity_reason is not None
            for event in query.events
        ):
            categories.add(
                ReconstructionContextMissingnessCategory.UNCERTAIN_FIRST_KNOWN_OR_PUBLICATION_TIME
            )
        completeness = (
            ReconstructionContextCompleteness.PARTIAL
            if len(categories) > 1 or missing_fields
            else ReconstructionContextCompleteness.COMPLETE
        )
        reasons.add(f"matched_event_count:{len(query.events)}")
    return ReconstructionConditioningStateV1(
        scientific_ledger_id=selected_ledger.ledger_id,
        source_kind=ReconstructionContextSourceKind.MARKET_CONTEXT,
        source_artifact_id=query.timeline_id,
        query_id=query.query_id,
        information_mode=query.information_mode,
        completeness=completeness,
        categories=tuple(categories),
        status=query.status.value,
        reason_codes=tuple(reasons),
        missing_fields=tuple(missing_fields),
        invalid_for_backtest_reason=_invalid_for_backtest(
            query.information_mode
        ),
    )


def classify_cftc_positioning_query(
    query: CftcPositioningQueryV1,
    *,
    qualified_unconditioned: bool = False,
    ledger: ReconstructionScientificLedgerV1 | None = None,
) -> ReconstructionConditioningStateV1:
    """Classify one CFTC query and preserve qualified non-conditioning."""
    if not isinstance(query, CftcPositioningQueryV1):
        raise TypeError("CFTC classification requires positioning query v1")
    selected_ledger = (
        ledger or current_histdata_reconstruction_scientific_ledger()
    )
    categories: set[ReconstructionContextMissingnessCategory]
    completeness: ReconstructionContextCompleteness
    if query.status is CftcPositioningQueryStatus.READY:
        if qualified_unconditioned:
            raise ValueError("ready CFTC context cannot be unconditioned")
        categories = {
            ReconstructionContextMissingnessCategory.MATCHED_COMPLETE_CONTEMPORANEOUS_FIELDS
        }
        completeness = ReconstructionContextCompleteness.COMPLETE
    else:
        categories = {
            ReconstructionContextMissingnessCategory.CFTC_LIMITED_AVAILABILITY
        }
        completeness = ReconstructionContextCompleteness.UNAVAILABLE
        if qualified_unconditioned:
            categories.add(
                ReconstructionContextMissingnessCategory.EXPLICIT_QUALIFIED_UNCONDITIONED_MODE
            )
            completeness = (
                ReconstructionContextCompleteness.QUALIFIED_UNCONDITIONED
            )
    missing_fields = (
        ()
        if query.status is CftcPositioningQueryStatus.READY
        else ("positioning_state",)
    )
    return ReconstructionConditioningStateV1(
        scientific_ledger_id=selected_ledger.ledger_id,
        source_kind=ReconstructionContextSourceKind.CFTC_POSITIONING,
        source_artifact_id=query.corpus_id,
        query_id=query.query_id,
        information_mode=query.information_mode,
        completeness=completeness,
        categories=tuple(categories),
        status=query.status.value,
        reason_codes=(
            f"cftc_status:{query.status.value}",
            f"cftc_reason:{query.reason}",
        ),
        missing_fields=missing_fields,
        qualified_unconditioned=qualified_unconditioned,
        invalid_for_backtest_reason=_invalid_for_backtest(
            query.information_mode
        ),
    )


@lru_cache(maxsize=1)
def current_histdata_reconstruction_scientific_ledger() -> (
    ReconstructionScientificLedgerV1
):
    """Return the frozen scientific target for the current HistData milestone."""
    scopes = (
        "certification",
        "dataset",
        "experiment",
        "plan",
        "product",
        "runtime",
    )
    estimand = ReconstructionEstimandV1(
        target=(
            "A regime-conditioned constrained counterfactual distribution of "
            "missing market events anchored to surviving HistData observations."
        ),
        observation_equation=r"Y=O_{\phi,e}(X)",
        conditional_equation=(
            r"X_{\mathrm{miss}}\sim q_\theta(X_{\mathrm{miss}}\mid "
            r"Y_{\mathrm{anchors}},C,e,\phi)"
        ),
        product_equation=(
            r"X_{\mathrm{product}}=R(Y_{\mathrm{anchors}}\cup "
            r"K(X_{\mathrm{miss}}))"
        ),
        final_product_law=r"(R\circ K)_\# q_\theta",
        nonclaim=RECONSTRUCTION_SCIENTIFIC_NONCLAIM,
    )
    assumptions = (
        ReconstructionAssumptionV1(
            key="modern-reference-is-not-latent-truth",
            statement=(
                "Modern stable HistData epochs are reference observations, not "
                "latent market truth."
            ),
            applicability_scopes=scopes,
            limitation="Reference-feed behavior cannot identify an unobserved market path.",
        ),
        ReconstructionAssumptionV1(
            key="observation-and-market-processes-may-be-confounded",
            statement=(
                "Observation-process and market-process changes may be confounded."
            ),
            applicability_scopes=scopes,
            limitation="Feed-regime effects cannot always be separated from market change.",
        ),
        ReconstructionAssumptionV1(
            key="operator-transport-is-support-bounded",
            statement=(
                "Observation-operator estimates transport backward only within "
                "declared support and uncertainty."
            ),
            applicability_scopes=(
                "experiment",
                "plan",
                "runtime",
                "certification",
            ),
            limitation="Unsupported epochs or windows must be refused, not extrapolated.",
        ),
        ReconstructionAssumptionV1(
            key="anchor-conditioning-does-not-identify-path",
            statement=(
                "Anchor conditioning limits, but does not identify, the internal path."
            ),
            applicability_scopes=(
                "plan",
                "product",
                "runtime",
                "certification",
            ),
            limitation="Different qualified ensemble members may occupy the same anchors.",
        ),
        ReconstructionAssumptionV1(
            key="context-completeness-varies",
            statement=(
                "Context completeness differs by source, period, field, and "
                "information mode."
            ),
            applicability_scopes=("experiment", "plan", "runtime", "product"),
            limitation="A missing provider row is never proof of no market-moving event.",
        ),
        ReconstructionAssumptionV1(
            key="asynchronous-triangle-support-is-assumed",
            statement=(
                "Asynchronous triangle support is an explicit observation and "
                "alignment assumption."
            ),
            applicability_scopes=(
                "plan",
                "runtime",
                "product",
                "certification",
            ),
            limitation="Alignment support must be measured and cannot imply simultaneity.",
        ),
        ReconstructionAssumptionV1(
            key="ex-post-products-are-invalid-for-backtest",
            statement=(
                "Final ex-post products are invalid as newly observed point-in-time "
                "backtest evidence."
            ),
            applicability_scopes=("product", "dataset", "certification"),
            limitation="They support counterfactual sensitivity analysis, not new alpha claims.",
        ),
    )
    definition_specs = (
        (
            ReconstructionContextMissingnessCategory.COMPLETE_CALENDAR_NO_MATCHING_EVENT,
            "The declared calendar is complete for the query and contains no matching event.",
            "A complete calendar profile plus an explicit no-matching-event query result.",
            "Condition on the verified no-match state without claiming no other event occurred.",
        ),
        (
            ReconstructionContextMissingnessCategory.MATCHED_COMPLETE_CONTEMPORANEOUS_FIELDS,
            "A matched context record has all declared contemporaneous fields.",
            "A matched bounded query with forecast, previous, and revision fields present.",
            "Condition on the record under its declared information mode.",
        ),
        (
            ReconstructionContextMissingnessCategory.MATCHED_MISSING_CONTEMPORANEOUS_FIELDS,
            "A matched record lacks one or more forecast, previous, or revision fields.",
            "A matched query and an explicit list of missing fields.",
            "Condition only on present fields and retain partial-completeness lineage.",
        ),
        (
            ReconstructionContextMissingnessCategory.INCOMPLETE_CORPUS_COVERAGE,
            "The context corpus cannot establish complete coverage for the query.",
            "An explicit coverage, timeline, or calendar incompleteness reason.",
            "Refuse required conditioning unless a separately qualified unconditioned mode applies.",
        ),
        (
            ReconstructionContextMissingnessCategory.EVENT_KNOWN_ONLY_EX_POST,
            "The event or value is available only after the relevant point-in-time boundary.",
            "First-known and availability times relative to the query boundary.",
            "Permit only labeled ex-post reconstruction and preserve invalid-for-backtest.",
        ),
        (
            ReconstructionContextMissingnessCategory.UNCERTAIN_FIRST_KNOWN_OR_PUBLICATION_TIME,
            "The first-known or publication time is approximate, window-only, or ambiguous.",
            "Non-exact precision or an explicit ambiguity reason.",
            "Retain timing uncertainty and refuse any exact point-in-time claim.",
        ),
        (
            ReconstructionContextMissingnessCategory.CFTC_LIMITED_AVAILABILITY,
            "CFTC state is pre-coverage, stale, unavailable, missing, or restatement-incomplete.",
            "The exact CFTC query status and reason.",
            "Do not impute positioning state or condition on an unavailable value.",
        ),
        (
            ReconstructionContextMissingnessCategory.EXPLICIT_QUALIFIED_UNCONDITIONED_MODE,
            "A powered qualification explicitly permits operation without unavailable context.",
            "A strong qualification artifact bound to the selected engine and carving policy.",
            "Run unconditioned, label the omission, and forbid silent imputation.",
        ),
    )
    definitions = tuple(
        ReconstructionContextMissingnessDefinitionV1(
            category=category,
            statement=statement,
            evidence_requirement=evidence,
            conditioning_treatment=treatment,
        )
        for category, statement, evidence, treatment in definition_specs
    )
    return ReconstructionScientificLedgerV1(
        scope=CURRENT_HISTDATA_SCIENTIFIC_SCOPE,
        estimand=estimand,
        assumptions=assumptions,
        context_missingness=definitions,
        legacy_replay_policy=ReconstructionLegacyScientificReplayPolicyV1(
            retained_release_line="2.4.x",
            scientific_binding_status="legacy-unbound",
            identity_replayable=True,
            execution_allowed_without_replan=False,
            migration_action=(
                "Read and identity-replay the retained artifact as-is; regenerate "
                "its plan from original inputs to execute under the current ledger."
            ),
            claim_limit=(
                "A retained v2.4 artifact cannot claim current scientific-ledger "
                "binding merely because its older identity still verifies."
            ),
        ),
        generated_row_origin="synthetic",
        generated_row_forbidden_claims=(
            "broker history",
            "observed",
            "recovered truth",
        ),
        invalid_for_backtest_label=RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL,
    )


def read_reconstruction_scientific_ledger(
    path: str | Path,
) -> ReconstructionScientificLedgerV1:
    """Read and identity-check one bounded scientific ledger artifact."""
    target = Path(path).expanduser().resolve()
    if not target.is_file():
        raise ValueError("scientific ledger does not exist")
    if target.stat().st_size > MAX_SCIENTIFIC_LEDGER_BYTES:
        raise ValueError("scientific ledger exceeds payload limit")
    return ReconstructionScientificLedgerV1.from_json(
        target.read_text(encoding="utf-8")
    )


def _invalid_for_backtest(mode: InformationMode) -> str | None:
    return (
        RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
        if mode is InformationMode.EX_POST_RECONSTRUCTION
        else None
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _identifier(value: Any) -> str:
    text = str(value).strip().lower().replace("_", "-")
    if (
        not text
        or len(text) > 256
        or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789-"
            for character in text
        )
        or text.startswith("-")
        or text.endswith("-")
        or "--" in text
    ):
        raise ValueError("invalid scientific assumption key")
    return text


def _required_text(value: Any) -> str:
    text = str(value).strip()
    if not text or len(text) > MAX_SCIENTIFIC_TEXT_LENGTH:
        raise ValueError("required scientific text is invalid")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _required_text(text) if text else None


def _normalized_text_tuple(values: Sequence[Any]) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(value) for value in values}))
    if len(selected) > MAX_SCIENTIFIC_ITEMS:
        raise ValueError("scientific text collection exceeds limit")
    return selected


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a scientific JSON object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("expected a scientific JSON array")
    if len(value) > MAX_SCIENTIFIC_ITEMS:
        raise ValueError("scientific JSON array exceeds limit")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be boolean")
    return value


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported scientific schema; expected {expected}")


def _require_schema_value(data: Mapping[str, Any], expected: str) -> None:
    _require_schema(str(data.get("schema_version", "")), expected)


__all__ = [
    "CURRENT_HISTDATA_SCIENTIFIC_SCOPE",
    "RECONSTRUCTION_ASSUMPTION_SCHEMA_VERSION",
    "RECONSTRUCTION_CONDITIONING_STATE_SCHEMA_VERSION",
    "RECONSTRUCTION_CONTEXT_MISSINGNESS_DEFINITION_SCHEMA_VERSION",
    "RECONSTRUCTION_ESTIMAND_SCHEMA_VERSION",
    "RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL",
    "RECONSTRUCTION_LEGACY_SCIENTIFIC_REPLAY_POLICY_SCHEMA_VERSION",
    "RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND",
    "RECONSTRUCTION_SCIENTIFIC_LEDGER_SCHEMA_VERSION",
    "RECONSTRUCTION_SCIENTIFIC_NONCLAIM",
    "ReconstructionAssumptionV1",
    "ReconstructionConditioningStateV1",
    "ReconstructionContextCompleteness",
    "ReconstructionContextMissingnessCategory",
    "ReconstructionContextMissingnessDefinitionV1",
    "ReconstructionContextSourceKind",
    "ReconstructionEstimandV1",
    "ReconstructionLegacyScientificReplayPolicyV1",
    "ReconstructionScientificLedgerV1",
    "classify_cftc_positioning_query",
    "classify_market_context_query",
    "current_histdata_reconstruction_scientific_ledger",
    "read_reconstruction_scientific_ledger",
]
