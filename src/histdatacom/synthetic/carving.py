"""Deterministic historical carving for empirical-motif candidates.

Carving is the first stage allowed to turn candidate-only motif rows into
accepted synthetic events.  It is intentionally fail closed: immutable
anchors, market-context support, and a fingerprint-validation result are
bound before any conditioned thinning or spread projection is attempted.

Rejected candidates never become a retained row set.  The returned contract
contains exact reason counts and a bounded sample only.  Accepted rows keep a
compact pointer to their candidate and motif transformation, including the
original quote whenever a projection changed it.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from histdatacom.data_quality.synthetic_constraints import (
    SYNTHETIC_VALIDATION_SCHEMA_VERSION,
)
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.generation import (
    CANDIDATE_ONLY_CONSTRAINT_SET_ID,
    EmpiricalMotifCandidateBatchV1,
    MotifGenerationStatus,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.streaming import (
    CarryStateV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    RejectionSummaryV1,
)

if TYPE_CHECKING:
    from histdatacom.market_context.contracts import MarketContextQueryV1

CARVING_CONDITION_POLICY_SCHEMA_VERSION = (
    "histdatacom.historical-carving-condition-policy.v1"
)
CARVING_QUARANTINE_SCHEMA_VERSION = (
    "histdatacom.historical-carving-quarantine.v1"
)
CARVING_CONSTRAINT_SET_SCHEMA_VERSION = (
    "histdatacom.historical-carving-constraint-set.v1"
)
CARVING_FINGERPRINT_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.historical-carving-fingerprint-evidence.v1"
)
CARVING_REJECTION_EXAMPLE_SCHEMA_VERSION = (
    "histdatacom.historical-carving-rejection-example.v1"
)
CARVING_EVENT_LINEAGE_SCHEMA_VERSION = (
    "histdatacom.historical-carving-event-lineage.v1"
)
CARVING_VALIDATION_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.historical-carving-validation-evidence.v1"
)
CARVED_CANDIDATE_BATCH_SCHEMA_VERSION = (
    "histdatacom.historical-carved-candidate-batch.v1"
)

HISTORICAL_CARVING_ENGINE_ID = "histdatacom.historical-carving"
HISTORICAL_CARVING_ENGINE_VERSION = "1.0.0"
HISTORICAL_CARVING_RULE_PRECEDENCE = (
    "hard.candidate_integrity.v1",
    "hard.immutable_anchor.v1",
    "hard.resource_envelope.v1",
    "hard.fingerprint_validation.v1",
    "hard.context_support.v1",
    "hard.quarantine.v1",
    "hard.session_closure.v1",
    "conditioned.motif_eligibility.v1",
    "conditioned.intensity.v1",
    "conditioned.spread_projection.v1",
    "hard.final_local_validation.v1",
)


@runtime_checkable
class ReconstructionCandidateLineageV1(Protocol):
    """Minimum lineage pointer consumed by the carving engine."""

    @property
    def transformation_id(self) -> str:
        """Return the immutable generator transformation identity."""


@runtime_checkable
class ReconstructionCandidateBatchV1(Protocol):
    """Generator-neutral candidate surface accepted by historical carving."""

    @property
    def run_id(self) -> str: ...

    @property
    def window_id(self) -> str: ...

    @property
    def ensemble_member_id(self) -> str: ...

    @property
    def symbol(self) -> str: ...

    @property
    def anchor_interval_id(self) -> str: ...

    @property
    def left_anchor_event_id(self) -> str: ...

    @property
    def right_anchor_event_id(self) -> str: ...

    @property
    def generator_config_id(self) -> str: ...

    @property
    def information_mode(self) -> InformationMode: ...

    @property
    def session_state(self) -> str: ...

    @property
    def special_tags(self) -> tuple[str, ...]: ...

    @property
    def event_tags(self) -> tuple[str, ...]: ...

    @property
    def status(self) -> MotifGenerationStatus: ...

    @property
    def events(self) -> tuple[SyntheticEventV1, ...]: ...

    @property
    def batch_id(self) -> str: ...

    def lineage_for(self, event_id: str) -> ReconstructionCandidateLineageV1:
        """Return the compact lineage pointer for one candidate event."""


MAX_CARVING_POLICIES = 64
MAX_CARVING_QUARANTINES = 4096
MAX_CARVING_POLICY_TAGS = 64
MAX_CARVING_ELIGIBLE_MOTIFS = 4096
MAX_CARVING_REJECTION_EXAMPLES = 64
MAX_CARVING_RULE_IDS_PER_EVENT = 128
MAX_CARVING_CONTEXT_EVENT_IDS = 256
MAX_CARVING_INPUT_BATCHES = 64
MAX_CARVING_TEXT = 1024
DEFAULT_MAX_ANCHOR_GAP_NS = 31 * 24 * 60 * 60 * 1_000_000_000
DEFAULT_MAX_INPUT_CANDIDATES = 100_000
DEFAULT_MAX_COMBINED_SPREAD_MULTIPLIER = 10.0


class CarvingBatchStatus(str, Enum):
    """Terminal status of one process-local carving batch."""

    ACCEPTED = "accepted"
    PARTIAL = "partial"
    EMPTY = "empty"
    REFUSED = "refused"


class CarvingEventAction(str, Enum):
    """Stable action applied to one accepted event."""

    ACCEPTED = "accepted"
    PROJECTED = "projected"
    SUBSTITUTED = "substituted"
    SUBSTITUTED_AND_PROJECTED = "substituted_and_projected"


class CarvingReason(str, Enum):
    """Bounded hard-rejection and refusal reason codes."""

    UPSTREAM_EMPTY = "upstream_empty"
    UPSTREAM_REFUSED = "upstream_refused"
    ANCHOR_EVIDENCE_MISSING = "anchor_evidence_missing"
    ANCHOR_GAP_LIMIT = "anchor_gap_limit"
    RESOURCE_LIMIT = "resource_limit"
    FINGERPRINT_EVIDENCE_MISSING = "fingerprint_evidence_missing"
    FINGERPRINT_VALIDATION_FAILED = "fingerprint_validation_failed"
    CONTEXT_SUPPORT_MISSING = "context_support_missing"
    CONTEXT_PROFILE_INCOMPLETE = "context_profile_incomplete"
    CLOSED_SESSION = "closed_session"
    QUARANTINED_INTERVAL = "quarantined_interval"
    INVALID_CANDIDATE = "invalid_candidate"
    ANCHOR_VIOLATION = "anchor_violation"
    OUTSIDE_WINDOW_OWNERSHIP = "outside_window_ownership"
    MOTIF_INCOMPATIBLE = "motif_incompatible"
    INTENSITY_THINNED = "intensity_thinned"
    PROJECTION_LIMIT = "projection_limit"
    FINAL_VALIDATION_FAILED = "final_validation_failed"


@dataclass(frozen=True, slots=True)
class HistoricalCarvingConditionPolicyV1:
    """One explicit state-conditioned intensity and spread policy."""

    name: str
    match_tags: tuple[str, ...]
    acceptance_rate: float = 1.0
    spread_multiplier: float = 1.0
    eligible_motif_ids: tuple[str, ...] = ()
    priority: int = 0
    policy_id: str = ""
    schema_version: str = CARVING_CONDITION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_CONDITION_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported carving condition policy")
        object.__setattr__(self, "name", _bounded_text(self.name, "name"))
        tags = _normalized_text_tuple(
            self.match_tags,
            "match_tags",
            maximum=MAX_CARVING_POLICY_TAGS,
            lowercase=True,
        )
        if not tags:
            raise ValueError("condition policy requires match_tags")
        object.__setattr__(self, "match_tags", tags)
        rate = _finite_float(self.acceptance_rate, "acceptance_rate")
        if not 0.0 <= rate <= 1.0:
            raise ValueError("acceptance_rate must be inside [0,1]")
        object.__setattr__(self, "acceptance_rate", rate)
        multiplier = _finite_float(self.spread_multiplier, "spread_multiplier")
        if multiplier <= 0.0:
            raise ValueError("spread_multiplier must be positive")
        object.__setattr__(self, "spread_multiplier", multiplier)
        motifs = _normalized_text_tuple(
            self.eligible_motif_ids,
            "eligible_motif_ids",
            maximum=MAX_CARVING_ELIGIBLE_MOTIFS,
        )
        object.__setattr__(self, "eligible_motif_ids", motifs)
        priority = _strict_int(self.priority, "priority")
        if priority < 0:
            raise ValueError("priority must be non-negative")
        object.__setattr__(self, "priority", priority)
        expected = _stable_id("carving-condition-policy", self.payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("carving condition policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic policy identity."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "match_tags": list(self.match_tags),
            "acceptance_rate": self.acceptance_rate,
            "spread_multiplier": self.spread_multiplier,
            "eligible_motif_ids": list(self.eligible_motif_ids),
            "priority": self.priority,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy data."""
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingConditionPolicyV1":
        """Restore and verify one condition policy."""
        return cls(
            name=str(data.get("name", "")),
            match_tags=_string_tuple(data.get("match_tags")),
            acceptance_rate=cast(float, data.get("acceptance_rate")),
            spread_multiplier=cast(float, data.get("spread_multiplier")),
            eligible_motif_ids=_string_tuple(data.get("eligible_motif_ids")),
            priority=cast(int, data.get("priority")),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "HistoricalCarvingConditionPolicyV1":
        """Restore a condition policy from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class HistoricalCarvingQuarantineV1:
    """One half-open interval in which synthetic liquidity is forbidden."""

    symbol: str
    start_ns: int
    end_ns: int
    reason: str
    source_id: str
    quarantine_id: str = ""
    schema_version: str = CARVING_QUARANTINE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_QUARANTINE_SCHEMA_VERSION:
            raise ValueError("unsupported carving quarantine")
        object.__setattr__(self, "symbol", _required_text(self.symbol).upper())
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("quarantine end_ns must follow start_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(self, "reason", _bounded_text(self.reason, "reason"))
        object.__setattr__(
            self, "source_id", _bounded_text(self.source_id, "source_id")
        )
        expected = _stable_id("carving-quarantine", self.payload())
        supplied = _optional_text(self.quarantine_id)
        if supplied is not None and supplied != expected:
            raise ValueError("carving quarantine_id differs")
        object.__setattr__(self, "quarantine_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic quarantine identity."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval_semantics": "[start_ns,end_ns)",
            "reason": self.reason,
            "source_id": self.source_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic quarantine data."""
        return {**self.payload(), "quarantine_id": self.quarantine_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingQuarantineV1":
        """Restore and verify one quarantine interval."""
        return cls(
            symbol=str(data.get("symbol", "")),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            reason=str(data.get("reason", "")),
            source_id=str(data.get("source_id", "")),
            quarantine_id=str(data.get("quarantine_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "HistoricalCarvingQuarantineV1":
        """Restore a quarantine from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class HistoricalCarvingConstraintSetV1:
    """Versioned fail-closed hard and conditioned carving constraints."""

    fingerprint_constraint_id: str
    condition_policies: tuple[HistoricalCarvingConditionPolicyV1, ...] = ()
    quarantines: tuple[HistoricalCarvingQuarantineV1, ...] = ()
    closed_session_states: tuple[str, ...] = (
        "closed",
        "market_closed",
        "weekend_closed",
    )
    require_complete_calendar_profile: bool = True
    require_fingerprint_validation: bool = True
    max_anchor_gap_ns: int = DEFAULT_MAX_ANCHOR_GAP_NS
    max_input_candidate_events: int = DEFAULT_MAX_INPUT_CANDIDATES
    max_rejection_examples: int = 8
    max_combined_spread_multiplier: float = (
        DEFAULT_MAX_COMBINED_SPREAD_MULTIPLIER
    )
    price_precision_digits: int = 8
    constraint_set_id: str = ""
    schema_version: str = CARVING_CONSTRAINT_SET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_CONSTRAINT_SET_SCHEMA_VERSION:
            raise ValueError("unsupported historical carving constraint set")
        object.__setattr__(
            self,
            "fingerprint_constraint_id",
            _bounded_text(
                self.fingerprint_constraint_id,
                "fingerprint_constraint_id",
            ),
        )
        policies = tuple(
            sorted(
                self.condition_policies,
                key=lambda item: (-item.priority, item.policy_id),
            )
        )
        if len(policies) > MAX_CARVING_POLICIES:
            raise ValueError("condition policy count exceeds bounded limit")
        if any(
            not isinstance(item, HistoricalCarvingConditionPolicyV1)
            for item in policies
        ):
            raise ValueError("condition_policies requires v1 policies")
        if len({item.policy_id for item in policies}) != len(policies):
            raise ValueError("condition policies must be unique")
        object.__setattr__(self, "condition_policies", policies)
        quarantines = tuple(
            sorted(
                self.quarantines,
                key=lambda item: (
                    item.symbol,
                    item.start_ns,
                    item.quarantine_id,
                ),
            )
        )
        if len(quarantines) > MAX_CARVING_QUARANTINES:
            raise ValueError("quarantine count exceeds bounded limit")
        if any(
            not isinstance(item, HistoricalCarvingQuarantineV1)
            for item in quarantines
        ):
            raise ValueError("quarantines requires v1 intervals")
        if len({item.quarantine_id for item in quarantines}) != len(
            quarantines
        ):
            raise ValueError("quarantines must be unique")
        object.__setattr__(self, "quarantines", quarantines)
        object.__setattr__(
            self,
            "closed_session_states",
            _normalized_text_tuple(
                self.closed_session_states,
                "closed_session_states",
                maximum=64,
                lowercase=True,
            ),
        )
        for name in (
            "require_complete_calendar_profile",
            "require_fingerprint_validation",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")
        for name in ("max_anchor_gap_ns", "max_input_candidate_events"):
            value = _strict_int(getattr(self, name), name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        examples = _strict_int(
            self.max_rejection_examples, "max_rejection_examples"
        )
        if not 0 <= examples <= MAX_CARVING_REJECTION_EXAMPLES:
            raise ValueError("max_rejection_examples exceeds bounded limit")
        object.__setattr__(self, "max_rejection_examples", examples)
        multiplier = _finite_float(
            self.max_combined_spread_multiplier,
            "max_combined_spread_multiplier",
        )
        if multiplier < 1.0:
            raise ValueError(
                "max_combined_spread_multiplier must be at least one"
            )
        object.__setattr__(self, "max_combined_spread_multiplier", multiplier)
        digits = _strict_int(
            self.price_precision_digits, "price_precision_digits"
        )
        if not 0 <= digits <= 15:
            raise ValueError("price_precision_digits must be inside [0,15]")
        object.__setattr__(self, "price_precision_digits", digits)
        expected = _stable_id("historical-carving-constraints", self.payload())
        supplied = _optional_text(self.constraint_set_id)
        if supplied is not None and supplied != expected:
            raise ValueError("historical carving constraint_set_id differs")
        object.__setattr__(self, "constraint_set_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic constraint identity and explicit precedence."""
        return {
            "schema_version": self.schema_version,
            "engine_id": HISTORICAL_CARVING_ENGINE_ID,
            "engine_version": HISTORICAL_CARVING_ENGINE_VERSION,
            "rule_precedence": list(HISTORICAL_CARVING_RULE_PRECEDENCE),
            "fingerprint_constraint_id": self.fingerprint_constraint_id,
            "condition_policies": [
                item.to_dict() for item in self.condition_policies
            ],
            "quarantines": [item.to_dict() for item in self.quarantines],
            "closed_session_states": list(self.closed_session_states),
            "require_complete_calendar_profile": (
                self.require_complete_calendar_profile
            ),
            "require_fingerprint_validation": (
                self.require_fingerprint_validation
            ),
            "max_anchor_gap_ns": self.max_anchor_gap_ns,
            "max_input_candidate_events": self.max_input_candidate_events,
            "max_rejection_examples": self.max_rejection_examples,
            "max_combined_spread_multiplier": (
                self.max_combined_spread_multiplier
            ),
            "price_precision_digits": self.price_precision_digits,
            "hard_constraints_fail_closed": True,
            "conditioned_constraints_are_advisory": False,
            "incompatible_motif_behavior": (
                "same-position-substitution-else-reject"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible constraint data."""
        return {**self.payload(), "constraint_set_id": self.constraint_set_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingConstraintSetV1":
        """Restore and verify a version-one constraint set."""
        return cls(
            fingerprint_constraint_id=str(
                data.get("fingerprint_constraint_id", "")
            ),
            condition_policies=tuple(
                HistoricalCarvingConditionPolicyV1.from_dict(item)
                for item in _mapping_sequence(data.get("condition_policies"))
            ),
            quarantines=tuple(
                HistoricalCarvingQuarantineV1.from_dict(item)
                for item in _mapping_sequence(data.get("quarantines"))
            ),
            closed_session_states=_string_tuple(
                data.get("closed_session_states")
            ),
            require_complete_calendar_profile=_strict_bool(
                data.get("require_complete_calendar_profile"),
                "require_complete_calendar_profile",
            ),
            require_fingerprint_validation=_strict_bool(
                data.get("require_fingerprint_validation"),
                "require_fingerprint_validation",
            ),
            max_anchor_gap_ns=cast(int, data.get("max_anchor_gap_ns")),
            max_input_candidate_events=cast(
                int, data.get("max_input_candidate_events")
            ),
            max_rejection_examples=cast(
                int, data.get("max_rejection_examples")
            ),
            max_combined_spread_multiplier=cast(
                float, data.get("max_combined_spread_multiplier")
            ),
            price_precision_digits=cast(
                int, data.get("price_precision_digits")
            ),
            constraint_set_id=str(data.get("constraint_set_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "HistoricalCarvingConstraintSetV1":
        """Restore a constraint set from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class CarvingFingerprintEvidenceV1:
    """A matched existing validator result bound to exact candidate batches."""

    validation_payload: Mapping[str, JSONValue]
    candidate_batch_ids: tuple[str, ...]
    reference_report_id: str
    candidate_report_id: str
    evidence_id: str = ""
    schema_version: str = CARVING_FINGERPRINT_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_FINGERPRINT_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported carving fingerprint evidence")
        payload = dict(self.validation_payload)
        if payload.get("schema_version") != SYNTHETIC_VALIDATION_SCHEMA_VERSION:
            raise ValueError("fingerprint evidence has unsupported validation")
        object.__setattr__(self, "validation_payload", payload)
        batches = _normalized_text_tuple(
            self.candidate_batch_ids,
            "candidate_batch_ids",
            maximum=MAX_CARVING_INPUT_BATCHES,
        )
        if not batches:
            raise ValueError("fingerprint evidence requires candidate batches")
        object.__setattr__(self, "candidate_batch_ids", batches)
        for name in ("reference_report_id", "candidate_report_id"):
            object.__setattr__(
                self, name, _bounded_text(getattr(self, name), name)
            )
        expected = _stable_id("carving-fingerprint-evidence", self.payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("carving fingerprint evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    @property
    def status(self) -> str:
        """Return the existing validator's normalized aggregate status."""
        return str(self.validation_payload.get("status", "")).strip().lower()

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic evidence identity."""
        return {
            "schema_version": self.schema_version,
            "validator_schema_version": SYNTHETIC_VALIDATION_SCHEMA_VERSION,
            "validation_payload": dict(self.validation_payload),
            "candidate_batch_ids": list(self.candidate_batch_ids),
            "reference_report_id": self.reference_report_id,
            "candidate_report_id": self.candidate_report_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible evidence."""
        return {**self.payload(), "evidence_id": self.evidence_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CarvingFingerprintEvidenceV1":
        """Restore and verify fingerprint evidence."""
        return cls(
            validation_payload=_mapping(data.get("validation_payload")),
            candidate_batch_ids=_string_tuple(data.get("candidate_batch_ids")),
            reference_report_id=str(data.get("reference_report_id", "")),
            candidate_report_id=str(data.get("candidate_report_id", "")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "CarvingFingerprintEvidenceV1":
        """Restore fingerprint evidence from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class HistoricalCarvingRejectionExampleV1:
    """One bounded non-row rejection pointer retained for diagnostics."""

    candidate_event_id: str
    candidate_content_sha256: str
    candidate_batch_id: str
    event_time_ns: int
    event_sequence: int
    reason: CarvingReason
    rule_ids: tuple[str, ...]
    example_id: str = ""
    schema_version: str = CARVING_REJECTION_EXAMPLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_REJECTION_EXAMPLE_SCHEMA_VERSION:
            raise ValueError("unsupported carving rejection example")
        for name in (
            "candidate_event_id",
            "candidate_batch_id",
        ):
            object.__setattr__(
                self, name, _bounded_text(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "candidate_content_sha256",
            _sha256(self.candidate_content_sha256, "candidate_content_sha256"),
        )
        object.__setattr__(
            self,
            "event_time_ns",
            _strict_int(self.event_time_ns, "event_time_ns"),
        )
        sequence = _strict_int(self.event_sequence, "event_sequence")
        if sequence < 0:
            raise ValueError("event_sequence must be non-negative")
        object.__setattr__(self, "event_sequence", sequence)
        object.__setattr__(self, "reason", CarvingReason(self.reason))
        rules = _normalized_text_tuple(
            self.rule_ids,
            "rule_ids",
            maximum=MAX_CARVING_RULE_IDS_PER_EVENT,
        )
        if not rules:
            raise ValueError("rejection example requires rule_ids")
        object.__setattr__(self, "rule_ids", rules)
        expected = _stable_id("carving-rejection-example", self.payload())
        supplied = _optional_text(self.example_id)
        if supplied is not None and supplied != expected:
            raise ValueError("carving rejection example_id differs")
        object.__setattr__(self, "example_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return non-row rejection identity."""
        return {
            "schema_version": self.schema_version,
            "candidate_event_id": self.candidate_event_id,
            "candidate_content_sha256": self.candidate_content_sha256,
            "candidate_batch_id": self.candidate_batch_id,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "reason": self.reason.value,
            "rule_ids": list(self.rule_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic rejection evidence."""
        return {**self.payload(), "example_id": self.example_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingRejectionExampleV1":
        """Restore and verify one bounded example."""
        return cls(
            candidate_event_id=str(data.get("candidate_event_id", "")),
            candidate_content_sha256=str(
                data.get("candidate_content_sha256", "")
            ),
            candidate_batch_id=str(data.get("candidate_batch_id", "")),
            event_time_ns=cast(int, data.get("event_time_ns")),
            event_sequence=cast(int, data.get("event_sequence")),
            reason=CarvingReason(str(data.get("reason", ""))),
            rule_ids=_string_tuple(data.get("rule_ids")),
            example_id=str(data.get("example_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HistoricalCarvingEventLineageV1:
    """Compact accepted-event lineage across candidate and carving stages."""

    output_event_id: str
    output_content_sha256: str
    candidate_event_id: str
    candidate_content_sha256: str
    candidate_batch_id: str
    candidate_transformation_id: str
    action: CarvingEventAction
    rule_ids: tuple[str, ...]
    context_event_ids: tuple[str, ...]
    policy_ids: tuple[str, ...]
    original_constraint_set_id: str
    final_constraint_set_id: str
    acceptance_score: float
    spread_multiplier: float
    original_bid: float | None = None
    original_ask: float | None = None
    lineage_id: str = ""
    schema_version: str = CARVING_EVENT_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_EVENT_LINEAGE_SCHEMA_VERSION:
            raise ValueError("unsupported historical carving event lineage")
        for name in (
            "output_event_id",
            "candidate_event_id",
            "candidate_batch_id",
            "candidate_transformation_id",
            "original_constraint_set_id",
            "final_constraint_set_id",
        ):
            object.__setattr__(
                self, name, _bounded_text(getattr(self, name), name)
            )
        for name in ("output_content_sha256", "candidate_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(self, "action", CarvingEventAction(self.action))
        rules = _normalized_text_tuple(
            self.rule_ids,
            "rule_ids",
            maximum=MAX_CARVING_RULE_IDS_PER_EVENT,
        )
        if not rules:
            raise ValueError("accepted lineage requires rule_ids")
        object.__setattr__(self, "rule_ids", rules)
        object.__setattr__(
            self,
            "context_event_ids",
            _normalized_text_tuple(
                self.context_event_ids,
                "context_event_ids",
                maximum=MAX_CARVING_CONTEXT_EVENT_IDS,
            ),
        )
        object.__setattr__(
            self,
            "policy_ids",
            _normalized_text_tuple(
                self.policy_ids,
                "policy_ids",
                maximum=MAX_CARVING_POLICIES,
            ),
        )
        score = _finite_float(self.acceptance_score, "acceptance_score")
        if not 0.0 <= score < 1.0:
            raise ValueError("acceptance_score must be inside [0,1)")
        object.__setattr__(self, "acceptance_score", score)
        multiplier = _finite_float(self.spread_multiplier, "spread_multiplier")
        if multiplier <= 0.0:
            raise ValueError("spread_multiplier must be positive")
        object.__setattr__(self, "spread_multiplier", multiplier)
        projected = self.action in {
            CarvingEventAction.PROJECTED,
            CarvingEventAction.SUBSTITUTED_AND_PROJECTED,
        }
        if projected != (
            self.original_bid is not None and self.original_ask is not None
        ):
            raise ValueError("projected lineage requires both original quotes")
        if self.original_bid is not None:
            object.__setattr__(
                self,
                "original_bid",
                _finite_float(self.original_bid, "original_bid"),
            )
            object.__setattr__(
                self,
                "original_ask",
                _finite_float(self.original_ask, "original_ask"),
            )
        expected = _stable_id(
            "historical-carving-event-lineage", self.payload()
        )
        supplied = _optional_text(self.lineage_id)
        if supplied is not None and supplied != expected:
            raise ValueError("historical carving lineage_id differs")
        object.__setattr__(self, "lineage_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return complete accepted-event decision lineage."""
        return {
            "schema_version": self.schema_version,
            "output_event_id": self.output_event_id,
            "output_content_sha256": self.output_content_sha256,
            "candidate_event_id": self.candidate_event_id,
            "candidate_content_sha256": self.candidate_content_sha256,
            "candidate_batch_id": self.candidate_batch_id,
            "candidate_transformation_id": self.candidate_transformation_id,
            "action": self.action.value,
            "rule_ids": list(self.rule_ids),
            "context_event_ids": list(self.context_event_ids),
            "policy_ids": list(self.policy_ids),
            "original_constraint_set_id": self.original_constraint_set_id,
            "final_constraint_set_id": self.final_constraint_set_id,
            "acceptance_score": self.acceptance_score,
            "spread_multiplier": self.spread_multiplier,
            "original_bid": self.original_bid,
            "original_ask": self.original_ask,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic accepted lineage."""
        return {**self.payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingEventLineageV1":
        """Restore and verify accepted lineage."""
        return cls(
            output_event_id=str(data.get("output_event_id", "")),
            output_content_sha256=str(data.get("output_content_sha256", "")),
            candidate_event_id=str(data.get("candidate_event_id", "")),
            candidate_content_sha256=str(
                data.get("candidate_content_sha256", "")
            ),
            candidate_batch_id=str(data.get("candidate_batch_id", "")),
            candidate_transformation_id=str(
                data.get("candidate_transformation_id", "")
            ),
            action=CarvingEventAction(str(data.get("action", ""))),
            rule_ids=_string_tuple(data.get("rule_ids")),
            context_event_ids=_string_tuple(data.get("context_event_ids")),
            policy_ids=_string_tuple(data.get("policy_ids")),
            original_constraint_set_id=str(
                data.get("original_constraint_set_id", "")
            ),
            final_constraint_set_id=str(
                data.get("final_constraint_set_id", "")
            ),
            acceptance_score=cast(float, data.get("acceptance_score")),
            spread_multiplier=cast(float, data.get("spread_multiplier")),
            original_bid=cast(float | None, data.get("original_bid")),
            original_ask=cast(float | None, data.get("original_ask")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HistoricalCarvingValidationEvidenceV1:
    """Bound input and local final-validation evidence for one batch."""

    fingerprint_evidence_id: str | None
    local_validation_status: str
    observed_anchor_content_sha256: tuple[str, ...]
    validator_ids: tuple[str, ...]
    evidence_id: str = ""
    schema_version: str = CARVING_VALIDATION_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVING_VALIDATION_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported carving validation evidence")
        object.__setattr__(
            self,
            "fingerprint_evidence_id",
            _optional_text(self.fingerprint_evidence_id),
        )
        status = _required_text(self.local_validation_status).lower()
        if status not in {"passed", "failed", "not_run"}:
            raise ValueError("unsupported local validation status")
        object.__setattr__(self, "local_validation_status", status)
        anchors = tuple(
            _sha256(item, "observed_anchor_content_sha256")
            for item in self.observed_anchor_content_sha256
        )
        if len(anchors) > 2:
            raise ValueError("validation evidence accepts at most two anchors")
        object.__setattr__(self, "observed_anchor_content_sha256", anchors)
        validators = _normalized_text_tuple(
            self.validator_ids,
            "validator_ids",
            maximum=32,
        )
        object.__setattr__(self, "validator_ids", validators)
        expected = _stable_id("historical-carving-validation", self.payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "historical carving validation evidence_id differs"
            )
        object.__setattr__(self, "evidence_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return deterministic validation evidence identity."""
        return {
            "schema_version": self.schema_version,
            "fingerprint_evidence_id": self.fingerprint_evidence_id,
            "local_validation_status": self.local_validation_status,
            "observed_anchor_content_sha256": list(
                self.observed_anchor_content_sha256
            ),
            "validator_ids": list(self.validator_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic validation evidence."""
        return {**self.payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvingValidationEvidenceV1":
        """Restore and verify validation evidence."""
        return cls(
            fingerprint_evidence_id=_mapping_optional_text(
                data, "fingerprint_evidence_id"
            ),
            local_validation_status=str(
                data.get("local_validation_status", "")
            ),
            observed_anchor_content_sha256=_string_tuple(
                data.get("observed_anchor_content_sha256")
            ),
            validator_ids=_string_tuple(data.get("validator_ids")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class HistoricalCarvedCandidateBatchV1:
    """Accepted rows plus compact carving evidence; never rejected rows."""

    run_id: str
    window_id: str
    ensemble_member_id: str
    symbol: str
    anchor_interval_id: str
    left_anchor_event_id: str
    right_anchor_event_id: str
    constraint_set_id: str
    input_candidate_batch_ids: tuple[str, ...]
    market_context_query_id: str
    market_context_timeline_id: str
    status: CarvingBatchStatus
    accepted_events: tuple[SyntheticEventV1, ...]
    accepted_lineage: tuple[HistoricalCarvingEventLineageV1, ...]
    rejection_summary: RejectionSummaryV1
    rejection_examples: tuple[HistoricalCarvingRejectionExampleV1, ...]
    validation_evidence: HistoricalCarvingValidationEvidenceV1
    carry_state: CarryStateV1
    projected_event_count: int = 0
    substituted_event_count: int = 0
    refusal_reason: CarvingReason | None = None
    batch_id: str = ""
    schema_version: str = CARVED_CANDIDATE_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARVED_CANDIDATE_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported historical carved candidate batch")
        for name in (
            "run_id",
            "window_id",
            "ensemble_member_id",
            "symbol",
            "anchor_interval_id",
            "left_anchor_event_id",
            "right_anchor_event_id",
            "constraint_set_id",
            "market_context_query_id",
            "market_context_timeline_id",
        ):
            object.__setattr__(
                self, name, _bounded_text(getattr(self, name), name)
            )
        batches = _normalized_text_tuple(
            self.input_candidate_batch_ids,
            "input_candidate_batch_ids",
            maximum=MAX_CARVING_INPUT_BATCHES,
        )
        if not batches:
            raise ValueError("carved batch requires input candidate batches")
        object.__setattr__(self, "input_candidate_batch_ids", batches)
        object.__setattr__(self, "status", CarvingBatchStatus(self.status))
        events = tuple(
            sorted(
                self.accepted_events,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.event_id,
                ),
            )
        )
        lineage = tuple(
            sorted(self.accepted_lineage, key=lambda item: item.output_event_id)
        )
        if any(
            item.origin is not SyntheticEventOrigin.SYNTHETIC for item in events
        ):
            raise ValueError("carved batch accepts only synthetic events")
        if any(
            item.constraint_set_id != self.constraint_set_id for item in events
        ):
            raise ValueError("accepted event constraint_set_id differs")
        if {item.event_id for item in events} != {
            item.output_event_id for item in lineage
        }:
            raise ValueError("accepted lineage does not cover accepted events")
        if len(events) != len(lineage):
            raise ValueError("accepted event and lineage counts differ")
        object.__setattr__(self, "accepted_events", events)
        object.__setattr__(self, "accepted_lineage", lineage)
        if not isinstance(self.rejection_summary, RejectionSummaryV1):
            raise ValueError("carved batch requires a rejection summary")
        if self.rejection_summary.run_id != self.run_id:
            raise ValueError("rejection summary run differs")
        if self.rejection_summary.window_id != self.window_id:
            raise ValueError("rejection summary window differs")
        if self.rejection_summary.accepted_count != len(events):
            raise ValueError("rejection summary accepted count differs")
        examples = tuple(self.rejection_examples)
        if len(examples) > MAX_CARVING_REJECTION_EXAMPLES:
            raise ValueError("rejection examples exceed bounded limit")
        if any(
            item.reason.value not in self.rejection_summary.reason_counts
            for item in examples
        ):
            raise ValueError("rejection example lacks a summary reason")
        object.__setattr__(self, "rejection_examples", examples)
        if not isinstance(
            self.validation_evidence, HistoricalCarvingValidationEvidenceV1
        ):
            raise ValueError("carved batch requires validation evidence")
        if not isinstance(self.carry_state, CarryStateV1):
            raise ValueError("carved batch requires carry state")
        if (
            self.carry_state.run_id != self.run_id
            or self.carry_state.ensemble_member_id != self.ensemble_member_id
            or self.symbol not in self.carry_state.symbol_watermarks_ns
        ):
            raise ValueError("carved batch carry state differs")
        for name in ("projected_event_count", "substituted_event_count"):
            value = _strict_int(getattr(self, name), name)
            if not 0 <= value <= len(events):
                raise ValueError(f"{name} is outside accepted event count")
            object.__setattr__(self, name, value)
        refusal = self.refusal_reason
        if refusal is not None:
            refusal = CarvingReason(refusal)
        object.__setattr__(self, "refusal_reason", refusal)
        _validate_batch_status(
            self.status,
            self.rejection_summary,
            refusal,
        )
        expected = _stable_id(
            "historical-carved-candidate-batch", self.payload()
        )
        supplied = _optional_text(self.batch_id)
        if supplied is not None and supplied != expected:
            raise ValueError("historical carved candidate batch_id differs")
        object.__setattr__(self, "batch_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return batch identity with row payloads represented by hashes."""
        return {
            "schema_version": self.schema_version,
            "engine_id": HISTORICAL_CARVING_ENGINE_ID,
            "engine_version": HISTORICAL_CARVING_ENGINE_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "anchor_interval_id": self.anchor_interval_id,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "constraint_set_id": self.constraint_set_id,
            "input_candidate_batch_ids": list(self.input_candidate_batch_ids),
            "market_context_query_id": self.market_context_query_id,
            "market_context_timeline_id": self.market_context_timeline_id,
            "status": self.status.value,
            "accepted_event_count": len(self.accepted_events),
            "accepted_event_content_sha256": _content_sha256(
                [item.to_dict() for item in self.accepted_events]
            ),
            "accepted_lineage_content_sha256": _content_sha256(
                [item.to_dict() for item in self.accepted_lineage]
            ),
            "rejection_summary": self.rejection_summary.to_dict(),
            "rejection_examples": [
                item.to_dict() for item in self.rejection_examples
            ],
            "validation_evidence": self.validation_evidence.to_dict(),
            "carry_id": self.carry_state.carry_id,
            "projected_event_count": self.projected_event_count,
            "substituted_event_count": self.substituted_event_count,
            "refusal_reason": (
                self.refusal_reason.value if self.refusal_reason else None
            ),
            "rejected_rows_retained": False,
            "final_storage_status": "not_persisted",
        }

    def metadata(self) -> dict[str, JSONValue]:
        """Return bounded workflow metadata without accepted event rows."""
        return {
            **self.payload(),
            "batch_id": self.batch_id,
            "accepted_events_inline": False,
            "accepted_lineage_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return process-local accepted rows and bounded evidence."""
        return {
            **self.payload(),
            "batch_id": self.batch_id,
            "accepted_events": [
                item.to_dict() for item in self.accepted_events
            ],
            "accepted_lineage": [
                item.to_dict() for item in self.accepted_lineage
            ],
            "carry_state": self.carry_state.to_dict(),
        }

    def to_json(self) -> str:
        """Return deterministic process-local JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "HistoricalCarvedCandidateBatchV1":
        """Restore accepted rows and verify every derived identity."""
        refusal = data.get("refusal_reason")
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbol=str(data.get("symbol", "")),
            anchor_interval_id=str(data.get("anchor_interval_id", "")),
            left_anchor_event_id=str(data.get("left_anchor_event_id", "")),
            right_anchor_event_id=str(data.get("right_anchor_event_id", "")),
            constraint_set_id=str(data.get("constraint_set_id", "")),
            input_candidate_batch_ids=_string_tuple(
                data.get("input_candidate_batch_ids")
            ),
            market_context_query_id=str(
                data.get("market_context_query_id", "")
            ),
            market_context_timeline_id=str(
                data.get("market_context_timeline_id", "")
            ),
            status=CarvingBatchStatus(str(data.get("status", ""))),
            accepted_events=tuple(
                SyntheticEventV1.from_dict(item)
                for item in _mapping_sequence(data.get("accepted_events"))
            ),
            accepted_lineage=tuple(
                HistoricalCarvingEventLineageV1.from_dict(item)
                for item in _mapping_sequence(data.get("accepted_lineage"))
            ),
            rejection_summary=RejectionSummaryV1.from_dict(
                _mapping(data.get("rejection_summary"))
            ),
            rejection_examples=tuple(
                HistoricalCarvingRejectionExampleV1.from_dict(item)
                for item in _mapping_sequence(data.get("rejection_examples"))
            ),
            validation_evidence=(
                HistoricalCarvingValidationEvidenceV1.from_dict(
                    _mapping(data.get("validation_evidence"))
                )
            ),
            carry_state=CarryStateV1.from_dict(
                _mapping(data.get("carry_state"))
            ),
            projected_event_count=cast(int, data.get("projected_event_count")),
            substituted_event_count=cast(
                int, data.get("substituted_event_count")
            ),
            refusal_reason=(
                CarvingReason(str(refusal)) if refusal is not None else None
            ),
            batch_id=str(data.get("batch_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "HistoricalCarvedCandidateBatchV1":
        """Restore a carved batch from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))

    def merged_stream(
        self, observed_events: Sequence[SyntheticEventV1]
    ) -> SyntheticEventStreamV1:
        """Merge accepted rows with unchanged caller-owned observations."""
        return SyntheticEventStreamV1.merge(
            run_id=self.run_id,
            ensemble_member_id=self.ensemble_member_id,
            symbol=self.symbol,
            observed_events=observed_events,
            synthetic_events=self.accepted_events,
        )


def carve_empirical_motif_candidates(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    candidate_batch: EmpiricalMotifCandidateBatchV1,
    observed_events: Sequence[SyntheticEventV1],
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
    fingerprint_evidence: CarvingFingerprintEvidenceV1 | None,
    substitution_batches: Sequence[EmpiricalMotifCandidateBatchV1] = (),
) -> HistoricalCarvedCandidateBatchV1:
    """Preserve the v1 empirical entry point over generic carving."""
    if not isinstance(candidate_batch, EmpiricalMotifCandidateBatchV1) or any(
        not isinstance(item, EmpiricalMotifCandidateBatchV1)
        for item in substitution_batches
    ):
        raise TypeError("empirical carving requires empirical motif batches")
    return carve_reconstruction_candidates(
        run=run,
        window=window,
        candidate_batch=candidate_batch,
        observed_events=observed_events,
        market_context=market_context,
        constraints=constraints,
        fingerprint_evidence=fingerprint_evidence,
        substitution_batches=substitution_batches,
    )


def carve_reconstruction_candidates(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    candidate_batch: ReconstructionCandidateBatchV1,
    observed_events: Sequence[SyntheticEventV1],
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
    fingerprint_evidence: CarvingFingerprintEvidenceV1 | None,
    substitution_batches: Sequence[ReconstructionCandidateBatchV1] = (),
) -> HistoricalCarvedCandidateBatchV1:
    """Carve one structural candidate batch with fail-closed precedence."""
    if not isinstance(candidate_batch, ReconstructionCandidateBatchV1):
        raise TypeError("carving requires a reconstruction candidate batch")
    if any(
        not isinstance(item, ReconstructionCandidateBatchV1)
        for item in substitution_batches
    ):
        raise TypeError(
            "substitutions must be reconstruction candidate batches"
        )
    candidates = (candidate_batch, *tuple(substitution_batches))
    _validate_scope(run, window, candidates, market_context, constraints)
    ordered_batches = (
        candidate_batch,
        *tuple(sorted(substitution_batches, key=lambda item: item.batch_id)),
    )
    input_batch_ids = tuple(item.batch_id for item in ordered_batches)
    anchors = _observed_anchors(candidate_batch, observed_events)
    anchor_hashes = tuple(_event_content_sha256(item) for item in anchors)

    if candidate_batch.status is MotifGenerationStatus.EMPTY:
        return _terminal_batch(
            run=run,
            window=window,
            candidate_batch=candidate_batch,
            input_batch_ids=input_batch_ids,
            market_context=market_context,
            constraints=constraints,
            anchors=anchors,
            anchor_hashes=anchor_hashes,
            fingerprint_evidence=fingerprint_evidence,
            status=CarvingBatchStatus.EMPTY,
            reason=CarvingReason.UPSTREAM_EMPTY,
        )
    if candidate_batch.status is MotifGenerationStatus.REFUSED:
        return _terminal_batch(
            run=run,
            window=window,
            candidate_batch=candidate_batch,
            input_batch_ids=input_batch_ids,
            market_context=market_context,
            constraints=constraints,
            anchors=anchors,
            anchor_hashes=anchor_hashes,
            fingerprint_evidence=fingerprint_evidence,
            status=CarvingBatchStatus.REFUSED,
            reason=CarvingReason.UPSTREAM_REFUSED,
        )

    refusal = _support_refusal(
        run,
        candidate_batch,
        ordered_batches,
        anchors,
        market_context,
        constraints,
        fingerprint_evidence,
    )
    if refusal is not None:
        return _terminal_batch(
            run=run,
            window=window,
            candidate_batch=candidate_batch,
            input_batch_ids=input_batch_ids,
            market_context=market_context,
            constraints=constraints,
            anchors=anchors,
            anchor_hashes=anchor_hashes,
            fingerprint_evidence=fingerprint_evidence,
            status=CarvingBatchStatus.REFUSED,
            reason=refusal,
        )

    if _closed_session(candidate_batch, market_context, constraints):
        return _terminal_batch(
            run=run,
            window=window,
            candidate_batch=candidate_batch,
            input_batch_ids=input_batch_ids,
            market_context=market_context,
            constraints=constraints,
            anchors=anchors,
            anchor_hashes=anchor_hashes,
            fingerprint_evidence=fingerprint_evidence,
            status=CarvingBatchStatus.REFUSED,
            reason=CarvingReason.CLOSED_SESSION,
        )

    alternatives = _alternative_events(ordered_batches[1:])
    accepted: list[SyntheticEventV1] = []
    lineage: list[HistoricalCarvingEventLineageV1] = []
    rejected: Counter[str] = Counter()
    examples: list[HistoricalCarvingRejectionExampleV1] = []
    projected_count = 0
    substituted_count = 0
    left_anchor, right_anchor = anchors
    for primary in candidate_batch.events:
        hard_reason = _candidate_hard_reason(
            primary,
            window,
            left_anchor,
            right_anchor,
            constraints,
        )
        if hard_reason is not None:
            _record_rejection(
                rejected,
                examples,
                primary,
                candidate_batch,
                hard_reason,
                _rule_for_reason(hard_reason),
                constraints.max_rejection_examples,
            )
            continue
        policies, context_event_ids = _matching_policies(
            primary,
            candidate_batch,
            market_context,
            constraints,
        )
        selected_batch = candidate_batch
        selected = primary
        if not _motif_eligible(primary, policies):
            replacement = _eligible_substitution(
                primary,
                alternatives,
                policies,
            )
            if replacement is None:
                _record_rejection(
                    rejected,
                    examples,
                    primary,
                    candidate_batch,
                    CarvingReason.MOTIF_INCOMPATIBLE,
                    "conditioned.motif_eligibility.v1",
                    constraints.max_rejection_examples,
                )
                continue
            selected_batch, selected = replacement
            substituted_count += 1
        acceptance_rate = math.prod(item.acceptance_rate for item in policies)
        score = _acceptance_score(run, selected, constraints, policies)
        if score >= acceptance_rate:
            _record_rejection(
                rejected,
                examples,
                selected,
                selected_batch,
                CarvingReason.INTENSITY_THINNED,
                "conditioned.intensity.v1",
                constraints.max_rejection_examples,
            )
            continue
        spread_multiplier = math.prod(
            item.spread_multiplier for item in policies
        )
        if spread_multiplier > constraints.max_combined_spread_multiplier:
            _record_rejection(
                rejected,
                examples,
                selected,
                selected_batch,
                CarvingReason.PROJECTION_LIMIT,
                "conditioned.spread_projection.v1",
                constraints.max_rejection_examples,
            )
            continue
        try:
            output, projected = _accepted_event(
                selected,
                constraints,
                spread_multiplier,
            )
        except ValueError:
            _record_rejection(
                rejected,
                examples,
                selected,
                selected_batch,
                CarvingReason.PROJECTION_LIMIT,
                "conditioned.spread_projection.v1",
                constraints.max_rejection_examples,
            )
            continue
        substituted = selected_batch.batch_id != candidate_batch.batch_id
        action = _accepted_action(projected, substituted)
        if projected:
            projected_count += 1
        source_lineage = selected_batch.lineage_for(selected.event_id)
        rule_ids = (
            "hard.candidate_integrity.v1",
            "hard.immutable_anchor.v1",
            "hard.resource_envelope.v1",
            "hard.fingerprint_validation.v1",
            "hard.context_support.v1",
            "hard.quarantine.v1",
            "hard.session_closure.v1",
            "conditioned.motif_eligibility.v1",
            "conditioned.intensity.v1",
            "conditioned.spread_projection.v1",
            "hard.final_local_validation.v1",
        )
        lineage.append(
            HistoricalCarvingEventLineageV1(
                output_event_id=output.event_id,
                output_content_sha256=_event_content_sha256(output),
                candidate_event_id=selected.event_id,
                candidate_content_sha256=_event_content_sha256(selected),
                candidate_batch_id=selected_batch.batch_id,
                candidate_transformation_id=(source_lineage.transformation_id),
                action=action,
                rule_ids=rule_ids,
                context_event_ids=context_event_ids,
                policy_ids=tuple(item.policy_id for item in policies),
                original_constraint_set_id=(
                    selected.constraint_set_id
                    or CANDIDATE_ONLY_CONSTRAINT_SET_ID
                ),
                final_constraint_set_id=constraints.constraint_set_id,
                acceptance_score=score,
                spread_multiplier=spread_multiplier,
                original_bid=selected.bid if projected else None,
                original_ask=selected.ask if projected else None,
            )
        )
        accepted.append(output)

    try:
        _validate_accepted_events(
            accepted,
            observed_events,
            candidate_batch,
            window,
            constraints,
        )
    except ValueError:
        return _terminal_batch(
            run=run,
            window=window,
            candidate_batch=candidate_batch,
            input_batch_ids=input_batch_ids,
            market_context=market_context,
            constraints=constraints,
            anchors=anchors,
            anchor_hashes=anchor_hashes,
            fingerprint_evidence=fingerprint_evidence,
            status=CarvingBatchStatus.REFUSED,
            reason=CarvingReason.FINAL_VALIDATION_FAILED,
        )

    summary = RejectionSummaryV1(
        run_id=run.run_id,
        window_id=window.window_id,
        candidate_count=len(candidate_batch.events),
        accepted_count=len(accepted),
        rejected_count=sum(rejected.values()),
        reason_counts=dict(rejected),
    )
    status = (
        CarvingBatchStatus.ACCEPTED
        if not rejected
        else (
            CarvingBatchStatus.PARTIAL
            if accepted
            else CarvingBatchStatus.REFUSED
        )
    )
    refusal_reason = None
    if status is CarvingBatchStatus.REFUSED:
        refusal_reason = _primary_rejection_reason(rejected)
    validation = HistoricalCarvingValidationEvidenceV1(
        fingerprint_evidence_id=(
            fingerprint_evidence.evidence_id if fingerprint_evidence else None
        ),
        local_validation_status="passed",
        observed_anchor_content_sha256=anchor_hashes,
        validator_ids=(
            SYNTHETIC_VALIDATION_SCHEMA_VERSION,
            "histdatacom.historical-carving-local-event-validator.v1",
        ),
    )
    return HistoricalCarvedCandidateBatchV1(
        run_id=run.run_id,
        window_id=window.window_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol=candidate_batch.symbol,
        anchor_interval_id=candidate_batch.anchor_interval_id,
        left_anchor_event_id=candidate_batch.left_anchor_event_id,
        right_anchor_event_id=candidate_batch.right_anchor_event_id,
        constraint_set_id=constraints.constraint_set_id,
        input_candidate_batch_ids=input_batch_ids,
        market_context_query_id=market_context.query_id,
        market_context_timeline_id=market_context.timeline_id,
        status=status,
        accepted_events=tuple(accepted),
        accepted_lineage=tuple(lineage),
        rejection_summary=summary,
        rejection_examples=tuple(examples),
        validation_evidence=validation,
        carry_state=_carry_state(
            run,
            window,
            left_anchor,
            right_anchor,
            accepted,
        ),
        projected_event_count=projected_count,
        substituted_event_count=substituted_count,
        refusal_reason=refusal_reason,
    )


def _validate_scope(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    batches: Sequence[ReconstructionCandidateBatchV1],
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> None:
    if window.run_id != run.run_id:
        raise ValueError("carving window does not belong to run")
    if constraints.constraint_set_id not in run.configuration_ids:
        raise ValueError("carving constraint set is absent from run")
    if not batches or len(batches) > MAX_CARVING_INPUT_BATCHES:
        raise ValueError("carving input batch count is outside bounds")
    primary = batches[0]
    for batch in batches:
        if not isinstance(batch, ReconstructionCandidateBatchV1):
            raise TypeError("carving requires reconstruction candidate batches")
        if (
            batch.run_id != run.run_id
            or batch.window_id != window.window_id
            or batch.ensemble_member_id != window.ensemble_member_id
            or batch.symbol != primary.symbol
            or batch.anchor_interval_id != primary.anchor_interval_id
            or batch.left_anchor_event_id != primary.left_anchor_event_id
            or batch.right_anchor_event_id != primary.right_anchor_event_id
        ):
            raise ValueError("substitution candidate scope differs")
        if batch.generator_config_id not in run.configuration_ids:
            raise ValueError("candidate generator config is absent from run")
    if (
        primary.symbol not in run.symbols
        or primary.symbol not in window.symbols
    ):
        raise ValueError("carving symbol is outside run/window scope")
    if market_context.window_id is not None and (
        market_context.window_id != window.window_id
    ):
        raise ValueError("market context window_id differs")
    if (
        market_context.start_ns > window.core_start_ns
        or market_context.end_ns < window.core_end_ns
    ):
        raise ValueError("market context does not cover carving window")
    if market_context.requested_symbols and primary.symbol.upper() not in {
        item.upper() for item in market_context.requested_symbols
    }:
        raise ValueError("market context requested symbols omit candidate")
    if market_context.information_mode is not primary.information_mode:
        raise ValueError("market context information mode differs")


def _observed_anchors(
    batch: ReconstructionCandidateBatchV1,
    observed_events: Sequence[SyntheticEventV1],
) -> tuple[SyntheticEventV1, SyntheticEventV1]:
    by_id = {item.event_id: item for item in observed_events}
    left = by_id.get(batch.left_anchor_event_id)
    right = by_id.get(batch.right_anchor_event_id)
    if left is None or right is None:
        raise ValueError(CarvingReason.ANCHOR_EVIDENCE_MISSING.value)
    if (
        left.origin is not SyntheticEventOrigin.OBSERVED
        or right.origin is not SyntheticEventOrigin.OBSERVED
        or left.symbol != batch.symbol
        or right.symbol != batch.symbol
    ):
        raise ValueError("carving anchors are not immutable observations")
    return left, right


def _support_refusal(
    run: ReconstructionRunV1,
    primary: ReconstructionCandidateBatchV1,
    batches: Sequence[ReconstructionCandidateBatchV1],
    anchors: tuple[SyntheticEventV1, SyntheticEventV1],
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
    fingerprint_evidence: CarvingFingerprintEvidenceV1 | None,
) -> CarvingReason | None:
    if anchors[1].event_time_ns - anchors[0].event_time_ns > (
        constraints.max_anchor_gap_ns
    ):
        return CarvingReason.ANCHOR_GAP_LIMIT
    if sum(len(item.events) for item in batches) > (
        constraints.max_input_candidate_events
    ):
        return CarvingReason.RESOURCE_LIMIT
    if len(primary.events) > run.storage_policy.max_events_per_batch:
        return CarvingReason.RESOURCE_LIMIT
    if constraints.require_fingerprint_validation:
        if fingerprint_evidence is None:
            return CarvingReason.FINGERPRINT_EVIDENCE_MISSING
        if not set(item.batch_id for item in batches).issubset(
            fingerprint_evidence.candidate_batch_ids
        ):
            return CarvingReason.FINGERPRINT_VALIDATION_FAILED
        if fingerprint_evidence.status != "match":
            return CarvingReason.FINGERPRINT_VALIDATION_FAILED
    missing_reason = market_context.missing_reason
    missing_reason_value = (
        missing_reason.value if missing_reason is not None else None
    )
    if missing_reason_value not in {None, "no_matching_event"}:
        return CarvingReason.CONTEXT_SUPPORT_MISSING
    if market_context.calendar_state is None:
        return CarvingReason.CONTEXT_SUPPORT_MISSING
    if (
        constraints.require_complete_calendar_profile
        and not market_context.calendar_state.profile_complete
    ):
        return CarvingReason.CONTEXT_PROFILE_INCOMPLETE
    return None


def _closed_session(
    batch: ReconstructionCandidateBatchV1,
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> bool:
    states = set(constraints.closed_session_states)
    calendar = market_context.calendar_state
    return batch.session_state.lower() in states or (
        calendar is not None and calendar.session_state.lower() in states
    )


def _candidate_hard_reason(
    event: SyntheticEventV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> CarvingReason | None:
    if (
        event.origin is not SyntheticEventOrigin.SYNTHETIC
        or event.constraint_set_id != CANDIDATE_ONLY_CONSTRAINT_SET_ID
        or not math.isfinite(event.bid)
        or not math.isfinite(event.ask)
        or event.bid <= 0.0
        or event.ask < event.bid
    ):
        return CarvingReason.INVALID_CANDIDATE
    if not (
        left_anchor.event_time_ns
        < event.event_time_ns
        < right_anchor.event_time_ns
    ):
        return CarvingReason.ANCHOR_VIOLATION
    if not window.owns_event_time(event.event_time_ns):
        return CarvingReason.OUTSIDE_WINDOW_OWNERSHIP
    for quarantine in constraints.quarantines:
        if (
            quarantine.symbol == event.symbol.upper()
            and quarantine.start_ns <= event.event_time_ns < quarantine.end_ns
        ):
            return CarvingReason.QUARANTINED_INTERVAL
    return None


def _matching_policies(
    event: SyntheticEventV1,
    batch: ReconstructionCandidateBatchV1,
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> tuple[tuple[HistoricalCarvingConditionPolicyV1, ...], tuple[str, ...]]:
    tokens = {
        batch.session_state.lower(),
        *(item.lower() for item in batch.special_tags),
        *(item.lower() for item in batch.event_tags),
    }
    calendar = market_context.calendar_state
    if calendar is not None:
        tokens.update(
            item.lower()
            for item in (
                *calendar.clock_sessions,
                *calendar.active_sessions,
                *calendar.overlaps,
                *calendar.special_tags,
                *calendar.holiday_tags,
                *calendar.event_tags,
                *calendar.calendar_tags,
            )
        )
        tokens.add(calendar.session_state.lower())
    context_ids: list[str] = []
    for context_event in market_context.events:
        if not context_event.overlaps(
            event.event_time_ns, event.event_time_ns + 1
        ):
            continue
        if context_event.affected_symbols and event.symbol.upper() not in {
            item.upper() for item in context_event.affected_symbols
        }:
            continue
        context_ids.append(context_event.event_id)
        tokens.add(context_event.kind.value)
        tokens.update(item.lower() for item in context_event.tags)
    policies = tuple(
        item
        for item in constraints.condition_policies
        if set(item.match_tags).intersection(tokens)
    )
    return policies, tuple(sorted(context_ids))


def _motif_eligible(
    event: SyntheticEventV1,
    policies: Sequence[HistoricalCarvingConditionPolicyV1],
) -> bool:
    return all(
        not item.eligible_motif_ids or event.motif_id in item.eligible_motif_ids
        for item in policies
    )


def _alternative_events(
    batches: Sequence[ReconstructionCandidateBatchV1],
) -> dict[
    tuple[int, int],
    tuple[tuple[ReconstructionCandidateBatchV1, SyntheticEventV1], ...],
]:
    indexed: dict[
        tuple[int, int],
        list[tuple[ReconstructionCandidateBatchV1, SyntheticEventV1]],
    ] = {}
    for batch in batches:
        if batch.status is not MotifGenerationStatus.GENERATED:
            continue
        for event in batch.events:
            indexed.setdefault(
                (event.event_time_ns, event.event_sequence), []
            ).append((batch, event))
    return {
        key: tuple(sorted(value, key=lambda item: item[1].event_id))
        for key, value in indexed.items()
    }


def _eligible_substitution(
    primary: SyntheticEventV1,
    alternatives: Mapping[
        tuple[int, int],
        Sequence[tuple[ReconstructionCandidateBatchV1, SyntheticEventV1]],
    ],
    policies: Sequence[HistoricalCarvingConditionPolicyV1],
) -> tuple[ReconstructionCandidateBatchV1, SyntheticEventV1] | None:
    for batch, event in alternatives.get(
        (primary.event_time_ns, primary.event_sequence), ()
    ):
        if _motif_eligible(event, policies):
            return batch, event
    return None


def _acceptance_score(
    run: ReconstructionRunV1,
    event: SyntheticEventV1,
    constraints: HistoricalCarvingConstraintSetV1,
    policies: Sequence[HistoricalCarvingConditionPolicyV1],
) -> float:
    semantic_key = canonical_contract_json(
        {
            "stage": HISTORICAL_CARVING_ENGINE_ID,
            "constraint_set_id": constraints.constraint_set_id,
            "anchor_interval_id": event.anchor_interval_id,
            "event_time_ns": event.event_time_ns,
            "event_sequence": event.event_sequence,
            "policy_ids": [item.policy_id for item in policies],
        }
    )
    value = run.seed_for(event.ensemble_member_id, semantic_key)
    return float(value) / (2**64)


def _accepted_event(
    candidate: SyntheticEventV1,
    constraints: HistoricalCarvingConstraintSetV1,
    spread_multiplier: float,
) -> tuple[SyntheticEventV1, bool]:
    projected = not math.isclose(
        spread_multiplier, 1.0, rel_tol=0.0, abs_tol=1e-15
    )
    bid = candidate.bid
    ask = candidate.ask
    if projected:
        midpoint = (bid + ask) / 2.0
        half_spread = (ask - bid) * spread_multiplier / 2.0
        bid = round(midpoint - half_spread, constraints.price_precision_digits)
        ask = round(midpoint + half_spread, constraints.price_precision_digits)
        if bid <= 0.0 or ask < bid:
            raise ValueError("conditioned spread projection is invalid")
    return (
        SyntheticEventV1.generated(
            symbol=candidate.symbol,
            event_time_ns=candidate.event_time_ns,
            event_sequence=candidate.event_sequence,
            bid=bid,
            ask=ask,
            run_id=candidate.run_id,
            ensemble_member_id=candidate.ensemble_member_id,
            source_version_id=candidate.source_version_id,
            anchor_interval_id=candidate.anchor_interval_id,
            left_anchor_event_id=cast(str, candidate.left_anchor_event_id),
            right_anchor_event_id=cast(str, candidate.right_anchor_event_id),
            generator_id=cast(str, candidate.generator_id),
            generator_version=cast(str, candidate.generator_version),
            generator_config_id=cast(str, candidate.generator_config_id),
            reference_id=candidate.reference_id,
            motif_id=candidate.motif_id,
            feed_epoch_id=candidate.feed_epoch_id,
            broker_profile_id=candidate.broker_profile_id,
            constraint_set_id=constraints.constraint_set_id,
            confidence=cast(float, candidate.confidence),
        ),
        projected,
    )


def _accepted_action(projected: bool, substituted: bool) -> CarvingEventAction:
    if projected and substituted:
        return CarvingEventAction.SUBSTITUTED_AND_PROJECTED
    if projected:
        return CarvingEventAction.PROJECTED
    if substituted:
        return CarvingEventAction.SUBSTITUTED
    return CarvingEventAction.ACCEPTED


def _validate_accepted_events(
    accepted: Sequence[SyntheticEventV1],
    observed_events: Sequence[SyntheticEventV1],
    batch: ReconstructionCandidateBatchV1,
    window: ReconstructionWindowV1,
    constraints: HistoricalCarvingConstraintSetV1,
) -> None:
    if any(
        item.constraint_set_id != constraints.constraint_set_id
        or item.left_anchor_event_id != batch.left_anchor_event_id
        or item.right_anchor_event_id != batch.right_anchor_event_id
        or not window.owns_event_time(item.event_time_ns)
        for item in accepted
    ):
        raise ValueError("accepted event violates final local constraints")
    positions = [(item.event_time_ns, item.event_sequence) for item in accepted]
    if len(set(positions)) != len(positions):
        raise ValueError("accepted events contain duplicate positions")
    stream = SyntheticEventStreamV1.merge(
        run_id=batch.run_id,
        ensemble_member_id=batch.ensemble_member_id,
        symbol=batch.symbol,
        observed_events=observed_events,
        synthetic_events=accepted,
    )
    observed_ids = {
        item.event_id
        for item in stream.events
        if item.origin is SyntheticEventOrigin.OBSERVED
    }
    if observed_ids != {item.event_id for item in observed_events}:
        raise ValueError("final local validation dropped an observed event")


def _terminal_batch(
    *,
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    candidate_batch: ReconstructionCandidateBatchV1,
    input_batch_ids: tuple[str, ...],
    market_context: MarketContextQueryV1,
    constraints: HistoricalCarvingConstraintSetV1,
    anchors: tuple[SyntheticEventV1, SyntheticEventV1],
    anchor_hashes: tuple[str, ...],
    fingerprint_evidence: CarvingFingerprintEvidenceV1 | None,
    status: CarvingBatchStatus,
    reason: CarvingReason,
) -> HistoricalCarvedCandidateBatchV1:
    candidate_count = len(candidate_batch.events)
    reasons = {reason.value: candidate_count} if candidate_count else {}
    examples: list[HistoricalCarvingRejectionExampleV1] = []
    for event in candidate_batch.events[: constraints.max_rejection_examples]:
        examples.append(
            HistoricalCarvingRejectionExampleV1(
                candidate_event_id=event.event_id,
                candidate_content_sha256=_event_content_sha256(event),
                candidate_batch_id=candidate_batch.batch_id,
                event_time_ns=event.event_time_ns,
                event_sequence=event.event_sequence,
                reason=reason,
                rule_ids=(_rule_for_reason(reason),),
            )
        )
    summary = RejectionSummaryV1(
        run_id=run.run_id,
        window_id=window.window_id,
        candidate_count=candidate_count,
        accepted_count=0,
        rejected_count=candidate_count,
        reason_counts=reasons,
    )
    validation = HistoricalCarvingValidationEvidenceV1(
        fingerprint_evidence_id=(
            fingerprint_evidence.evidence_id if fingerprint_evidence else None
        ),
        local_validation_status=(
            "failed"
            if reason is CarvingReason.FINAL_VALIDATION_FAILED
            else "not_run"
        ),
        observed_anchor_content_sha256=anchor_hashes,
        validator_ids=tuple(
            item
            for item in (
                (
                    SYNTHETIC_VALIDATION_SCHEMA_VERSION
                    if fingerprint_evidence is not None
                    else None
                ),
                "histdatacom.historical-carving-local-event-validator.v1",
            )
            if item is not None
        ),
    )
    left_anchor, right_anchor = anchors
    return HistoricalCarvedCandidateBatchV1(
        run_id=run.run_id,
        window_id=window.window_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol=candidate_batch.symbol,
        anchor_interval_id=candidate_batch.anchor_interval_id,
        left_anchor_event_id=candidate_batch.left_anchor_event_id,
        right_anchor_event_id=candidate_batch.right_anchor_event_id,
        constraint_set_id=constraints.constraint_set_id,
        input_candidate_batch_ids=input_batch_ids,
        market_context_query_id=market_context.query_id,
        market_context_timeline_id=market_context.timeline_id,
        status=status,
        accepted_events=(),
        accepted_lineage=(),
        rejection_summary=summary,
        rejection_examples=tuple(examples),
        validation_evidence=validation,
        carry_state=_carry_state(run, window, left_anchor, right_anchor, ()),
        refusal_reason=reason,
    )


def _record_rejection(
    rejected: Counter[str],
    examples: list[HistoricalCarvingRejectionExampleV1],
    event: SyntheticEventV1,
    batch: ReconstructionCandidateBatchV1,
    reason: CarvingReason,
    rule_id: str,
    example_limit: int,
) -> None:
    rejected[reason.value] += 1
    if len(examples) >= example_limit:
        return
    examples.append(
        HistoricalCarvingRejectionExampleV1(
            candidate_event_id=event.event_id,
            candidate_content_sha256=_event_content_sha256(event),
            candidate_batch_id=batch.batch_id,
            event_time_ns=event.event_time_ns,
            event_sequence=event.event_sequence,
            reason=reason,
            rule_ids=(rule_id,),
        )
    )


def _carry_state(
    run: ReconstructionRunV1,
    window: ReconstructionWindowV1,
    left_anchor: SyntheticEventV1,
    right_anchor: SyntheticEventV1,
    accepted: Sequence[SyntheticEventV1],
) -> CarryStateV1:
    last_event = accepted[-1] if accepted else left_anchor
    if window.owns_event_time(right_anchor.event_time_ns):
        last_event = right_anchor
    watermark = min(
        window.core_end_ns - 1,
        max(window.core_start_ns, right_anchor.event_time_ns - 1),
    )
    return CarryStateV1(
        run_id=run.run_id,
        ensemble_member_id=window.ensemble_member_id,
        symbol_watermarks_ns={left_anchor.symbol: watermark},
        last_event_ids={left_anchor.symbol: last_event.event_id},
    )


def _validate_batch_status(
    status: CarvingBatchStatus,
    summary: RejectionSummaryV1,
    refusal: CarvingReason | None,
) -> None:
    if status is CarvingBatchStatus.ACCEPTED and (
        summary.accepted_count == 0 or summary.rejected_count != 0 or refusal
    ):
        raise ValueError("accepted carving status does not reconcile")
    if status is CarvingBatchStatus.PARTIAL and not (
        summary.accepted_count > 0
        and summary.rejected_count > 0
        and not refusal
    ):
        raise ValueError("partial carving status does not reconcile")
    if status is CarvingBatchStatus.EMPTY and (
        summary.candidate_count != 0
        or refusal is not CarvingReason.UPSTREAM_EMPTY
    ):
        raise ValueError("empty carving status does not reconcile")
    if status is CarvingBatchStatus.REFUSED and (
        summary.accepted_count != 0 or refusal is None
    ):
        raise ValueError("refused carving status does not reconcile")


def _rule_for_reason(reason: CarvingReason) -> str:
    return {
        CarvingReason.UPSTREAM_EMPTY: "hard.candidate_integrity.v1",
        CarvingReason.UPSTREAM_REFUSED: "hard.candidate_integrity.v1",
        CarvingReason.ANCHOR_EVIDENCE_MISSING: "hard.immutable_anchor.v1",
        CarvingReason.ANCHOR_GAP_LIMIT: "hard.resource_envelope.v1",
        CarvingReason.RESOURCE_LIMIT: "hard.resource_envelope.v1",
        CarvingReason.FINGERPRINT_EVIDENCE_MISSING: (
            "hard.fingerprint_validation.v1"
        ),
        CarvingReason.FINGERPRINT_VALIDATION_FAILED: (
            "hard.fingerprint_validation.v1"
        ),
        CarvingReason.CONTEXT_SUPPORT_MISSING: "hard.context_support.v1",
        CarvingReason.CONTEXT_PROFILE_INCOMPLETE: "hard.context_support.v1",
        CarvingReason.CLOSED_SESSION: "hard.session_closure.v1",
        CarvingReason.QUARANTINED_INTERVAL: "hard.quarantine.v1",
        CarvingReason.INVALID_CANDIDATE: "hard.candidate_integrity.v1",
        CarvingReason.ANCHOR_VIOLATION: "hard.immutable_anchor.v1",
        CarvingReason.OUTSIDE_WINDOW_OWNERSHIP: ("hard.candidate_integrity.v1"),
        CarvingReason.MOTIF_INCOMPATIBLE: ("conditioned.motif_eligibility.v1"),
        CarvingReason.INTENSITY_THINNED: "conditioned.intensity.v1",
        CarvingReason.PROJECTION_LIMIT: ("conditioned.spread_projection.v1"),
        CarvingReason.FINAL_VALIDATION_FAILED: (
            "hard.final_local_validation.v1"
        ),
    }[reason]


def _primary_rejection_reason(rejected: Mapping[str, int]) -> CarvingReason:
    reasons = tuple(CarvingReason(item) for item in rejected)
    return min(
        reasons,
        key=lambda item: HISTORICAL_CARVING_RULE_PRECEDENCE.index(
            _rule_for_reason(item)
        ),
    )


def _event_content_sha256(event: SyntheticEventV1) -> str:
    return _content_sha256(event.to_dict())


def _content_sha256(value: JSONValue) -> str:
    return hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(dict(payload))}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("value must be non-empty text")
    return value.strip()


def _bounded_text(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) > MAX_CARVING_TEXT:
        raise ValueError(f"{name} exceeds bounded text length")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _required_text(value)


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be boolean")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _normalized_text_tuple(
    values: Sequence[str],
    name: str,
    *,
    maximum: int,
    lowercase: bool = False,
) -> tuple[str, ...]:
    normalized = {
        (
            _bounded_text(item, name).lower()
            if lowercase
            else _bounded_text(item, name)
        )
        for item in values
    }
    if len(normalized) > maximum:
        raise ValueError(f"{name} exceeds bounded limit")
    return tuple(sorted(normalized))


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(item not in "0123456789abcdef" for item in text):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("expected a sequence")
    return tuple(_mapping(item) for item in value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("expected a string sequence")
    if any(not isinstance(item, str) for item in value):
        raise ValueError("expected string sequence values")
    return tuple(cast(Sequence[str], value))


def _mapping_optional_text(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    return None if value is None else str(value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)
