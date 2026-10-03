"""Bounded capability claims, never a substitute for independent verification.

Reading or rendering a matrix verifies its *structure and identity*. In
particular, ``executed_passed`` is a recorded claim, not authority conferred by
this module. Certification must additionally run its closed, requirement-
specific evidence verifiers for ``required_verification_rows``. Unsupported
verification is a blocker, not a successful structural read.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, TypeVar, cast

MAX_MATRIX_BYTES = 2 * 1024 * 1024
MAX_MATRIX_ROWS = 160
MAX_TEXT_LENGTH = 2048
MAX_LIMITATIONS = 16
_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_DIGEST_ID = re.compile(r"[a-z][a-z0-9._-]{0,95}:sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


class CapabilityStateV1(str, Enum):
    """The seven distinct claim states required by issue #525."""

    NOT_IMPLEMENTED = "not_implemented"
    IMPLEMENTED_UNEXECUTED = "implemented_unexecuted"
    EXECUTED_FAILED = "executed_failed"
    EXECUTED_INSUFFICIENT_EVIDENCE = "executed_insufficient_evidence"
    EXECUTED_PASSED = "executed_passed"
    DEFERRED_BLOCKED = "deferred_blocked"
    WAIVED_WITH_LIMITATION = "waived_with_limitation"


class CapabilityEvidenceScopeV1(str, Enum):
    """Non-interchangeable claim scopes, not an ordered confidence score."""

    NONE = "none"
    SOFTWARE = "software"
    BOUNDED = "bounded"
    COMPLETE = "complete"


class CapabilityClaimKindV1(str, Enum):
    """A full campaign and an explicitly narrower frozen claim differ."""

    FULL_V2_5_CAMPAIGN = "full_v2_5_campaign"
    NARROWER_PREDECLARED = "narrower_predeclared"


@dataclass(frozen=True, slots=True)
class CapabilityDefinitionV1:
    """One closed catalog row; the catalog is not execution evidence."""

    requirement_id: str
    parent_id: str | None
    title: str
    dependencies: tuple[str, ...] = ()


# Filled from the independently reviewed issue inventory, not discovered from
# implementation names or inferred from the presence of software.
CAPABILITY_CATALOG: tuple[CapabilityDefinitionV1, ...] = (
    CapabilityDefinitionV1(
        "source_experiment_identity",
        None,
        "Frozen source and experiment identity",
        (),
    ),
    CapabilityDefinitionV1(
        "model_bank_registration", None, "Model-bank registration", ()
    ),
    CapabilityDefinitionV1(
        "powered_engine_eligibility",
        None,
        "Powered engine eligibility",
        ("model_bank_registration",),
    ),
    CapabilityDefinitionV1(
        "product_engine_selection",
        None,
        "Product engine selection",
        ("powered_engine_eligibility",),
    ),
    CapabilityDefinitionV1(
        "observation_uncertainty",
        None,
        "Observation uncertainty propagation",
        (),
    ),
    CapabilityDefinitionV1(
        "transition_uncertainty", None, "Transition uncertainty", ()
    ),
    CapabilityDefinitionV1(
        "fresh_release_holdout", None, "Fresh release holdout", ()
    ),
    CapabilityDefinitionV1(
        "adaptive_partition", None, "Adaptive partition qualification", ()
    ),
    CapabilityDefinitionV1(
        "alignment", None, "Exact and bounded-prior alignment qualification", ()
    ),
    CapabilityDefinitionV1(
        "alignment.exact", "alignment", "Exact alignment qualification", ()
    ),
    CapabilityDefinitionV1(
        "alignment.bounded_prior",
        "alignment",
        "Bounded-prior alignment qualification",
        (),
    ),
    CapabilityDefinitionV1("projection_burden", None, "Projection burden", ()),
    CapabilityDefinitionV1(
        "support_map_replay",
        None,
        "Full support-map rebuild and independent replay",
        (),
    ),
    CapabilityDefinitionV1(
        "installed_temporal",
        None,
        "Representative installed Temporal execution",
        (),
    ),
    CapabilityDefinitionV1(
        "crash_cancel_resume", None, "Crash, cancel and resume", ()
    ),
    CapabilityDefinitionV1(
        "storage_disconnect",
        None,
        "Storage disconnect, remount and no-fallback",
        (),
    ),
    CapabilityDefinitionV1(
        "complete_temporal_campaign", None, "Complete Temporal campaign", ()
    ),
    CapabilityDefinitionV1(
        "complete_product_rectangle",
        None,
        "Complete product rectangle",
        ("complete_temporal_campaign",),
    ),
    CapabilityDefinitionV1(
        "campaign_product_index",
        None,
        "Certification-grade product index",
        ("complete_product_rectangle",),
    ),
    CapabilityDefinitionV1(
        "full_deep_verification",
        None,
        "Full deep-verification root",
        ("campaign_product_index",),
    ),
    CapabilityDefinitionV1(
        "era_stratified_audit",
        None,
        "Era-stratified audit",
        ("full_deep_verification",),
    ),
    CapabilityDefinitionV1(
        "derived_bars_reconciliation", None, "Derived bars reconciliation", ()
    ),
    CapabilityDefinitionV1(
        "provider_neutral_publication",
        None,
        "Provider-neutral dataset publication",
        (
            "campaign_product_index",
            "full_deep_verification",
            "era_stratified_audit",
            "derived_bars_reconciliation",
        ),
    ),
    CapabilityDefinitionV1(
        "package_release_promotion", None, "Package and release promotion", ()
    ),
    CapabilityDefinitionV1(
        "official_global_calendar",
        None,
        "First-party official-source global calendar",
        (),
    ),
    CapabilityDefinitionV1(
        "professional_calendar",
        None,
        "Professional calendar materialization and projection provenance",
        (),
    ),
    CapabilityDefinitionV1(
        "economic_release_forecasting",
        None,
        "Autonomous economic-release forecasting",
        (),
    ),
    CapabilityDefinitionV1(
        "synthetic_orderflow",
        None,
        "Deterministic synthetic-trader and customer-orderflow substrate",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem", None, "Public broker-plugin SDK and ecosystem", ()
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.sdk",
        "broker_ecosystem",
        "Public SDK and compatible software contract",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.discovery",
        "broker_ecosystem",
        "Plugin discovery and deterministic selection",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.capabilities",
        "broker_ecosystem",
        "Required capabilities and negotiation",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.permissions",
        "broker_ecosystem",
        "Host permission admission",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.security",
        "broker_ecosystem",
        "Host process, network and secret security",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.provider_policy",
        "broker_ecosystem",
        "Provider and data-rights policy admission",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.conformance",
        "broker_ecosystem",
        "Independent plugin conformance",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.host_health",
        "broker_ecosystem",
        "Host-owned capture-health evidence",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.provenance",
        "broker_ecosystem",
        "Tamper-evident retained root and replay",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.real_capture",
        "broker_ecosystem",
        "Sustained real-provider scientific capture qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.clock_model",
        "broker_ecosystem",
        "Cross-feed clock-model qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.matching",
        "broker_ecosystem",
        "Asynchronous matching and ambiguity qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.dependence",
        "broker_ecosystem",
        "Dependence-sensitive identification qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.untouched_operator",
        "broker_ecosystem",
        "Untouched cross-feed operator qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "broker_ecosystem.transfer",
        "broker_ecosystem",
        "Broker-transfer experiment qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "overlapping_feed_identification",
        None,
        "Independent overlapping-feed observation-process identification",
        (),
    ),
    CapabilityDefinitionV1(
        "overlapping_feed_identification.clock_quality",
        "overlapping_feed_identification",
        "Clock offset, drift, jitter and timestamp quality",
        (),
    ),
    CapabilityDefinitionV1(
        "overlapping_feed_identification.match_ambiguity",
        "overlapping_feed_identification",
        "Asynchronous match ambiguity",
        (),
    ),
    CapabilityDefinitionV1(
        "overlapping_feed_identification.dependence",
        "overlapping_feed_identification",
        "Dependence-sensitive retention identification",
        (),
    ),
    CapabilityDefinitionV1(
        "overlapping_feed_identification.untouched",
        "overlapping_feed_identification",
        "Untouched overlap and transport qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility",
        None,
        "Cross-version schema compatibility and semantic replay",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.registration",
        "schema_compatibility",
        "Schema and reader/writer registration",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.direct_read",
        "schema_compatibility",
        "Direct-reader compatibility",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.migration_reachability",
        "schema_compatibility",
        "Migration reachability",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.edge_qualification",
        "schema_compatibility",
        "Migration-edge qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.composition_qualification",
        "schema_compatibility",
        "Migration-composition qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.semantic_equivalence",
        "schema_compatibility",
        "Lossless-edge semantic equivalence",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.golden_replay",
        "schema_compatibility",
        "Golden cross-version replay",
        (),
    ),
    CapabilityDefinitionV1(
        "schema_compatibility.migration_receipts",
        "schema_compatibility",
        "Non-destructive migration receipts",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility",
        None,
        "Computational reproducibility and cross-platform replay",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.environment",
        "computational_reproducibility",
        "Execution-environment identity, dependency provenance and SBOM",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.rng",
        "computational_reproducibility",
        "Semantic RNG namespace invariance",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.tolerance",
        "computational_reproducibility",
        "Exact-versus-tolerant numerical policy",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.reference_kernels",
        "computational_reproducibility",
        "Numerical reference kernels",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.bitwise",
        "computational_reproducibility",
        "Actual bitwise replay qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.semantic",
        "computational_reproducibility",
        "Actual semantic replay qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.numerical",
        "computational_reproducibility",
        "Actual numerical replay qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.statistical",
        "computational_reproducibility",
        "Actual statistical replay qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "computational_reproducibility.cross_environment",
        "computational_reproducibility",
        "Cross-environment qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "durable_artifact_recovery",
        None,
        "Durable scientific artifact recovery",
        (),
    ),
    CapabilityDefinitionV1(
        "live_broker_transfer",
        None,
        "Live broker and fingerprint-transfer scientific qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "live_broker_transfer.capture",
        "live_broker_transfer",
        "Live provider capture scientific qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "live_broker_transfer.fingerprint_transfer",
        "live_broker_transfer",
        "Historical fingerprint-transfer qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features",
        None,
        "Multi-timeframe market and bar feature plane",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.derived_bars",
        "multi_timeframe_features",
        "Common 1m, 5m, 15m, 30m, 1h, 4h and 1d derived bars",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.campaign_reconciliation",
        "multi_timeframe_features",
        "Complete campaign bars materialized and reconciled",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.aggregation",
        "multi_timeframe_features",
        "Hierarchical downsampling and direct-tick equivalence qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.causal_contract",
        "multi_timeframe_features",
        "Causal closed and partial-bar feature contract",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.reconstruction",
        "multi_timeframe_features",
        "Reconstruction and reference-conditioning experiment qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.broker_transfer",
        "multi_timeframe_features",
        "Broker fingerprint and transfer multi-timeframe qualification",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.triangle",
        "multi_timeframe_features",
        "Triangle multi-timeframe features and verification",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.source_volume",
        "multi_timeframe_features",
        "HistData raw volume semantics",
        (),
    ),
    CapabilityDefinitionV1(
        "multi_timeframe_features.wide_corpus",
        "multi_timeframe_features",
        "Actual wide training corpus with market and bar columns",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate",
        None,
        "Origin-aware certified wide ML training substrate",
        ("multi_timeframe_features",),
    ),
    CapabilityDefinitionV1(
        "training_substrate.lineage",
        "training_substrate",
        "Origin and evidence-unit lineage",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.evidence_mass",
        "training_substrate",
        "Synthetic-member evidence-mass policy",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.causal_joins",
        "training_substrate",
        "Causal multi-timeframe joins",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.chronological_splits",
        "training_substrate",
        "Whole-evidence-unit chronological splits",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.leakage_canaries",
        "training_substrate",
        "Leakage canaries and origin separation",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.weighting_ess",
        "training_substrate",
        "Weighting and effective-sample-size mathematics",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.feature_schema",
        "training_substrate",
        "Deterministic feature-schema identity",
        (),
    ),
    CapabilityDefinitionV1(
        "training_substrate.published_corpus",
        "training_substrate",
        "Published complete training-corpus verification",
        (),
    ),
    CapabilityDefinitionV1("wider_graph", None, "Wider currency graph", ()),
    CapabilityDefinitionV1(
        "oanda_serving", None, "OANDA-compatible selected-product serving", ()
    ),
)
CAPABILITY_REQUIREMENT_IDS = tuple(
    row.requirement_id for row in CAPABILITY_CATALOG
)
CAPABILITY_PARENT_IDS = tuple(
    row.requirement_id for row in CAPABILITY_CATALOG if row.parent_id is None
)
CAPABILITY_DEPENDENCIES = tuple(
    (row.requirement_id, row.dependencies) for row in CAPABILITY_CATALOG
)
V2_5_CRITICAL_REQUIREMENT_IDS: tuple[str, ...] = (
    "source_experiment_identity",
    "model_bank_registration",
    "powered_engine_eligibility",
    "product_engine_selection",
    "observation_uncertainty",
    "transition_uncertainty",
    "fresh_release_holdout",
    "adaptive_partition",
    "alignment",
    "projection_burden",
    "support_map_replay",
    "installed_temporal",
    "crash_cancel_resume",
    "storage_disconnect",
    "complete_temporal_campaign",
    "complete_product_rectangle",
    "campaign_product_index",
    "full_deep_verification",
    "era_stratified_audit",
    "derived_bars_reconciliation",
    "provider_neutral_publication",
    "package_release_promotion",
    "alignment.exact",
    "alignment.bounded_prior",
)


CAPABILITY_CATALOG_ID = (
    "capability-catalog:sha256:"
    + hashlib.sha256(
        json.dumps(
            {
                "schema_version": "histdatacom.capability-catalog.v1",
                "definitions": [
                    [
                        row.requirement_id,
                        row.parent_id,
                        row.title,
                        list(row.dependencies),
                    ]
                    for row in CAPABILITY_CATALOG
                ],
                "full_claim_critical_ids": list(V2_5_CRITICAL_REQUIREMENT_IDS),
                "full_claim_critical_scope": "complete",
                "dependency_semantics": "parent-descendant-and-dependency-closure-v1",
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
)


def _text(value: Any, name: str, *, maximum: int = MAX_TEXT_LENGTH) -> str:
    if type(value) is not str or not value or len(value) > maximum:
        raise ValueError(f"{name} must be bounded nonempty text")
    if value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} contains whitespace/control ambiguity")
    return value


def _identity(value: Any, name: str) -> str:
    text = _text(value, name, maximum=256)
    if any(char.isspace() for char in text):
        raise ValueError(f"{name} cannot contain whitespace")
    return text


def _artifact(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not str or _DIGEST_ID.fullmatch(value) is None:
        raise ValueError(f"{name} requires a content-addressed artifact ID")


def _timestamp(value: Any, name: str) -> str:
    text = _text(value, name, maximum=20)
    try:
        parsed = datetime.strptime(text, _UTC_FORMAT)
    except ValueError as error:
        raise ValueError(f"{name} must be exact UTC seconds") from error
    if parsed.strftime(_UTC_FORMAT) != text:
        raise ValueError(f"{name} must be exact UTC seconds")
    return text


def _strings(value: Any, name: str) -> None:
    if type(value) is not tuple or len(value) > MAX_LIMITATIONS:
        raise ValueError(f"{name} requires a bounded tuple")
    for item in value:
        _text(item, name)
    if len(set(value)) != len(value):
        raise ValueError(f"{name} duplicates entries")


def _requirement(value: Any) -> str:
    text = _identity(value, "requirement_id")
    if text not in {item.requirement_id for item in CAPABILITY_CATALOG}:
        raise ValueError("unknown capability requirement")
    return text


def _tree_bound(value: Any) -> None:
    pending = [(value, 0)]
    cost = 0
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 30000 or depth > 12:
            raise ValueError("capability JSON traversal exceeds bound")
        if type(item) is str:
            if len(item) > MAX_MATRIX_BYTES:
                raise ValueError("capability JSON text exceeds bound")
            # Conservative ensure_ascii expansion, including astral pairs.
            cost += 2 + sum(12 if ord(char) > 0xFFFF else 6 for char in item)
        elif item is None or type(item) is bool:
            cost += 5
        elif type(item) is int and 0 <= item <= 2**31 - 1:
            cost += 11
        elif type(item) is list and len(item) <= MAX_MATRIX_ROWS:
            cost += len(item) + 2
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is dict and len(item) <= 32:
            cost += 2 * len(item) + 2
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("capability JSON key must be exact text")
                pending.extend(((key, depth + 1), (child, depth + 1)))
        else:
            raise ValueError("capability JSON contains unsupported/big value")
        if cost > MAX_MATRIX_BYTES:
            raise ValueError("capability escaped JSON exceeds bound")


def _canonical(value: Any) -> str:
    _tree_bound(value)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate capability JSON key")
        result[key] = value
    return result


def _load(text: str) -> dict[str, Any]:
    if (
        type(text) is not str
        or len(text) > MAX_MATRIX_BYTES
        or not text.isascii()
    ):
        raise ValueError("capability JSON requires bounded canonical ASCII")
    try:
        value = json.loads(text, object_pairs_hook=_pairs)
    except (ValueError, RecursionError) as error:
        raise ValueError("invalid capability JSON") from error
    if type(value) is not dict or _canonical(value) != text:
        raise ValueError("capability JSON is not canonical")
    return value


T = TypeVar("T", bound="_Record")


def _wire(value: Any) -> Any:
    if type(value) in _RECORD_TYPES:
        return _payload(value)
    if type(value) in (
        CapabilityStateV1,
        CapabilityEvidenceScopeV1,
        CapabilityClaimKindV1,
    ):
        return value.value
    if type(value) is tuple:
        return [_wire(item) for item in value]
    return value


def _payload(value: _Record, *, identity: bool = False) -> dict[str, Any]:
    result = {
        field.name: _wire(getattr(value, field.name))
        for field in fields(cast(Any, value))
        if not (identity and field.name == value._ID_FIELD)
    }
    result["schema_version"] = value.schema_version
    return result


def _readmit(value: T, expected: type[T]) -> T:
    if type(value) is not expected or expected not in _RECORD_TYPES:
        raise TypeError("capability record requires its exact declared type")
    return expected(
        **{
            field.name: getattr(value, field.name)
            for field in fields(cast(Any, value))
        }
    )


def _derive(value: _Record, field_name: str, prefix: str) -> None:
    expected = (
        prefix
        + ":sha256:"
        + hashlib.sha256(
            _canonical(_payload(value, identity=True)).encode("ascii")
        ).hexdigest()
    )
    supplied = getattr(value, field_name)
    if type(supplied) is not str or supplied not in ("", expected):
        raise ValueError(f"{field_name} differs from canonical content")
    object.__setattr__(value, field_name, expected)


class _Record:
    """Strict local wire behavior; no storage or scientific verification."""

    schema_version: ClassVar[str]
    _ID_FIELD: ClassVar[str] = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a detached, exactly re-admitted structural wire."""
        return _payload(_readmit(self, type(self)))

    def to_json(self) -> str:
        """Return canonical bounded ASCII JSON, without a terminator."""
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """Reject extra/missing fields and rederive all supplied identities."""
        if cls not in _RECORD_TYPES or type(data) is not dict:
            raise TypeError("capability reader requires an exact mapping")
        _tree_bound(data)
        expected = {field.name for field in fields(cast(Any, cls))} | {
            "schema_version"
        }
        if (
            set(data) != expected
            or data["schema_version"] != cls.schema_version
        ):
            raise ValueError("capability schema or field inventory differs")
        values = {
            key: value for key, value in data.items() if key != "schema_version"
        }
        return cls(**cls._decode(values))

    @classmethod
    def from_json(cls: type[T], text: str) -> T:
        """Read canonical structure; never qualify an execution claim."""
        result = cls.from_dict(_load(text))
        if result.to_json() != text:
            raise ValueError(
                "capability wire is not in normalized canonical order"
            )
        return result

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        return values


@dataclass(frozen=True, slots=True)
class CapabilityMatrixRequirementV1(_Record):
    """One exact scope demanded by a frozen certification policy."""

    requirement_id: str
    evidence_scope: CapabilityEvidenceScopeV1
    schema_version: ClassVar[str] = "histdatacom.capability-requirement.v1"

    def __post_init__(self) -> None:
        _requirement(self.requirement_id)
        if (
            type(self.evidence_scope) is not CapabilityEvidenceScopeV1
            or self.evidence_scope is CapabilityEvidenceScopeV1.NONE
        ):
            raise ValueError("required evidence scope must be explicit")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["evidence_scope"] = CapabilityEvidenceScopeV1(
            values["evidence_scope"]
        )
        return values


@dataclass(frozen=True, slots=True)
class CapabilityMatrixWaiverV1(_Record):
    """A particular frozen policy clause, not a caller's waiver Boolean."""

    requirement_id: str
    policy_clause: str
    limitation: str
    schema_version: ClassVar[str] = "histdatacom.capability-waiver.v1"

    def __post_init__(self) -> None:
        _requirement(self.requirement_id)
        _text(self.policy_clause, "policy_clause")
        _text(self.limitation, "limitation")


@dataclass(frozen=True, slots=True)
class CapabilityMatrixPolicyV1(_Record):
    """Frozen claim membership, exact scopes, and explicitly permitted waivers."""

    release_id: str
    dataset_id: str | None
    claim_kind: CapabilityClaimKindV1
    claim_label: str
    scope: str
    limitations: tuple[str, ...]
    requirements: tuple[CapabilityMatrixRequirementV1, ...]
    permitted_waivers: tuple[CapabilityMatrixWaiverV1, ...]
    policy_id: str = ""
    catalog_id: str = CAPABILITY_CATALOG_ID
    schema_version: ClassVar[str] = "histdatacom.capability-matrix-policy.v1"
    _ID_FIELD: ClassVar[str] = "policy_id"

    def __post_init__(self) -> None:
        if (
            type(self.catalog_id) is not str
            or self.catalog_id != CAPABILITY_CATALOG_ID
        ):
            raise ValueError("unsupported capability catalog identity")
        _identity(self.release_id, "release_id")
        if self.dataset_id is not None:
            _artifact(self.dataset_id, "dataset_id")
        if type(self.claim_kind) is not CapabilityClaimKindV1:
            raise TypeError("claim_kind must use the closed enum")
        _text(self.claim_label, "claim_label")
        _text(self.scope, "scope")
        _strings(self.limitations, "limitations")
        if (
            self.claim_kind is CapabilityClaimKindV1.NARROWER_PREDECLARED
            and not self.limitations
        ):
            raise ValueError("a narrower claim requires explicit limitations")
        if (
            type(self.requirements) is not tuple
            or not 1 <= len(self.requirements) <= MAX_MATRIX_ROWS
            or type(self.permitted_waivers) is not tuple
            or len(self.permitted_waivers) > MAX_MATRIX_ROWS
        ):
            raise ValueError("policy rows require bounded exact tuples")
        requirements = tuple(
            _readmit(item, CapabilityMatrixRequirementV1)
            for item in self.requirements
        )
        waivers = tuple(
            _readmit(item, CapabilityMatrixWaiverV1)
            for item in self.permitted_waivers
        )
        ids = {item.requirement_id for item in requirements}
        waiver_ids = {item.requirement_id for item in waivers}
        if len(ids) != len(requirements) or len(waiver_ids) != len(waivers):
            raise ValueError("policy duplicates requirements or waivers")
        if not waiver_ids <= ids:
            raise ValueError("waiver is outside frozen required membership")
        if _dependency_closure(ids) != ids:
            raise ValueError("policy omits required children or dependencies")
        if (
            self.claim_kind is CapabilityClaimKindV1.FULL_V2_5_CAMPAIGN
            and not set(V2_5_CRITICAL_REQUIREMENT_IDS) <= ids
        ):
            raise ValueError(
                "full v2.5 claim omits frozen critical requirements"
            )
        if self.claim_kind is CapabilityClaimKindV1.FULL_V2_5_CAMPAIGN and any(
            item.requirement_id in V2_5_CRITICAL_REQUIREMENT_IDS
            and item.evidence_scope is not CapabilityEvidenceScopeV1.COMPLETE
            for item in requirements
        ):
            raise ValueError("full v2.5 critical evidence must be COMPLETE")
        object.__setattr__(
            self,
            "requirements",
            tuple(sorted(requirements, key=lambda row: row.requirement_id)),
        )
        object.__setattr__(
            self,
            "permitted_waivers",
            tuple(sorted(waivers, key=lambda row: row.requirement_id)),
        )
        _derive(self, "policy_id", "capability-matrix-policy")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["claim_kind"] = CapabilityClaimKindV1(values["claim_kind"])
        values["limitations"] = _tuple_field(values["limitations"])
        values["requirements"] = tuple(
            CapabilityMatrixRequirementV1.from_dict(item)
            for item in _tuple_field(values["requirements"])
        )
        values["permitted_waivers"] = tuple(
            CapabilityMatrixWaiverV1.from_dict(item)
            for item in _tuple_field(values["permitted_waivers"])
        )
        return values


@dataclass(frozen=True, slots=True)
class CapabilityMatrixRowV1(_Record):
    """An exact, scope-labelled evidence claim with state-appropriate absence."""

    requirement_id: str
    policy_id: str
    implementation_commit: str | None
    execution_artifact_id: str | None
    independent_verification_artifact_id: str | None
    state: CapabilityStateV1
    evidence_scope: CapabilityEvidenceScopeV1
    scope: str
    limitations: tuple[str, ...]
    blocking_issues: tuple[int, ...]
    last_verified_at_utc: str | None
    release_id: str
    dataset_id: str | None
    waiver_policy_clause: str | None = None
    row_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.capability-matrix-row.v1"
    _ID_FIELD: ClassVar[str] = "row_id"

    def __post_init__(self) -> None:
        _requirement(self.requirement_id)
        _artifact(self.policy_id, "policy_id")
        _identity(self.release_id, "release_id")
        _artifact(self.dataset_id, "dataset_id", optional=True)
        if self.implementation_commit is not None and (
            type(self.implementation_commit) is not str
            or _COMMIT.fullmatch(self.implementation_commit) is None
        ):
            raise ValueError(
                "implementation_commit requires a full immutable ID"
            )
        _artifact(
            self.execution_artifact_id, "execution_artifact_id", optional=True
        )
        _artifact(
            self.independent_verification_artifact_id,
            "independent_verification_artifact_id",
            optional=True,
        )
        if (
            type(self.state) is not CapabilityStateV1
            or type(self.evidence_scope) is not CapabilityEvidenceScopeV1
        ):
            raise TypeError("row state and evidence scope require closed enums")
        _text(self.scope, "scope")
        _strings(self.limitations, "limitations")
        if (
            type(self.blocking_issues) is not tuple
            or len(self.blocking_issues) > 32
        ):
            raise ValueError("blocking_issues requires a bounded tuple")
        if any(
            type(item) is not int or not 1 <= item <= 2**31 - 1
            for item in self.blocking_issues
        ):
            raise ValueError("blocking issues require positive exact numbers")
        if tuple(sorted(set(self.blocking_issues))) != self.blocking_issues:
            raise ValueError("blocking issues must be unique and sorted")
        if self.last_verified_at_utc is not None:
            _timestamp(self.last_verified_at_utc, "last_verified_at_utc")
        executed = self.state in (
            CapabilityStateV1.EXECUTED_FAILED,
            CapabilityStateV1.EXECUTED_INSUFFICIENT_EVIDENCE,
            CapabilityStateV1.EXECUTED_PASSED,
        )
        if executed or self.state is CapabilityStateV1.IMPLEMENTED_UNEXECUTED:
            if self.implementation_commit is None:
                raise ValueError(
                    "implemented state requires immutable implementation"
                )
        if executed:
            if (
                self.execution_artifact_id is None
                or self.evidence_scope is CapabilityEvidenceScopeV1.NONE
            ):
                raise ValueError(
                    "executed state requires scoped execution evidence"
                )
        elif self.state in (
            CapabilityStateV1.NOT_IMPLEMENTED,
            CapabilityStateV1.IMPLEMENTED_UNEXECUTED,
        ) and (
            self.execution_artifact_id is not None
            or self.independent_verification_artifact_id is not None
        ):
            raise ValueError("unexecuted state cannot carry executed evidence")
        if self.independent_verification_artifact_id is not None and (
            self.execution_artifact_id is None
            or self.last_verified_at_utc is None
        ):
            raise ValueError(
                "verification identity requires execution and time"
            )
        if self.execution_artifact_id is not None and (
            self.implementation_commit is None
            or self.evidence_scope is CapabilityEvidenceScopeV1.NONE
        ):
            raise ValueError(
                "execution identity requires implementation and scope"
            )
        if (
            self.state is CapabilityStateV1.NOT_IMPLEMENTED
            and self.implementation_commit is not None
        ):
            raise ValueError("not_implemented cannot claim an implementation")
        if self.state is CapabilityStateV1.EXECUTED_PASSED:
            if (
                self.independent_verification_artifact_id is None
                or self.last_verified_at_utc is None
            ):
                raise ValueError(
                    "passed claim requires independent verification identity/time"
                )
            if (
                self.independent_verification_artifact_id
                == self.execution_artifact_id
            ):
                raise ValueError("execution cannot independently verify itself")
            if self.blocking_issues:
                raise ValueError("passed claim cannot hide blocking issues")
        if (
            self.state is CapabilityStateV1.DEFERRED_BLOCKED
            and not self.blocking_issues
        ):
            raise ValueError("deferred_blocked requires actual blocker issues")
        if (
            self.state is CapabilityStateV1.WAIVED_WITH_LIMITATION
            and not self.limitations
        ):
            raise ValueError("waived claim requires its explicit limitation")
        if self.state is CapabilityStateV1.WAIVED_WITH_LIMITATION:
            _text(self.waiver_policy_clause, "waiver_policy_clause")
        elif self.waiver_policy_clause is not None:
            raise ValueError("nonwaived row cannot claim a waiver clause")
        _derive(self, "row_id", "capability-matrix-row")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["state"] = CapabilityStateV1(values["state"])
        values["evidence_scope"] = CapabilityEvidenceScopeV1(
            values["evidence_scope"]
        )
        values["limitations"] = _tuple_field(values["limitations"])
        values["blocking_issues"] = _tuple_field(values["blocking_issues"])
        return values


@dataclass(frozen=True, slots=True)
class CapabilityMatrixV1(_Record):
    """Complete catalog claims for one exact release/dataset snapshot."""

    policy_id: str
    release_id: str
    dataset_id: str | None
    as_of_utc: str
    rows: tuple[CapabilityMatrixRowV1, ...]
    matrix_id: str = ""
    catalog_id: str = CAPABILITY_CATALOG_ID
    schema_version: ClassVar[str] = "histdatacom.capability-matrix.v1"
    _ID_FIELD: ClassVar[str] = "matrix_id"

    def __post_init__(self) -> None:
        if (
            type(self.catalog_id) is not str
            or self.catalog_id != CAPABILITY_CATALOG_ID
        ):
            raise ValueError("unsupported capability catalog identity")
        _artifact(self.policy_id, "policy_id")
        _identity(self.release_id, "release_id")
        _artifact(self.dataset_id, "dataset_id", optional=True)
        _timestamp(self.as_of_utc, "as_of_utc")
        if type(self.rows) is not tuple or len(self.rows) != len(
            CAPABILITY_CATALOG
        ):
            raise ValueError("matrix must contain the exact complete catalog")
        rows = tuple(
            _readmit(item, CapabilityMatrixRowV1) for item in self.rows
        )
        if {item.requirement_id for item in rows} != {
            item.requirement_id for item in CAPABILITY_CATALOG
        }:
            raise ValueError("matrix duplicates or omits catalog rows")
        for item in rows:
            if (item.policy_id, item.release_id, item.dataset_id) != (
                self.policy_id,
                self.release_id,
                self.dataset_id,
            ):
                raise ValueError("matrix row policy/release/dataset differs")
            if (
                item.last_verified_at_utc is not None
                and item.last_verified_at_utc > self.as_of_utc
            ):
                raise ValueError(
                    "row verification is after the matrix snapshot"
                )
        by_id = {item.requirement_id: item for item in rows}
        for item in rows:
            if item.state is CapabilityStateV1.EXECUTED_PASSED:
                dependencies = _dependency_closure({item.requirement_id}) - {
                    item.requirement_id
                }
                if any(
                    by_id[name].state is not CapabilityStateV1.EXECUTED_PASSED
                    for name in dependencies
                ):
                    raise ValueError(
                        "passed parent/dependent row has nonpassing prerequisites"
                    )
        object.__setattr__(
            self,
            "rows",
            tuple(sorted(rows, key=lambda row: row.requirement_id)),
        )
        _derive(self, "matrix_id", "capability-matrix")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["rows"] = tuple(
            CapabilityMatrixRowV1.from_dict(item)
            for item in _tuple_field(values["rows"])
        )
        return values


@dataclass(frozen=True, slots=True)
class CapabilityMatrixBlockerV1:
    """A structural blocker; absence is not an independent verification pass."""

    requirement_id: str
    reason: str


_RECORD_TYPES = (
    CapabilityMatrixRequirementV1,
    CapabilityMatrixWaiverV1,
    CapabilityMatrixPolicyV1,
    CapabilityMatrixRowV1,
    CapabilityMatrixV1,
)


def _tuple_field(value: Any) -> tuple[Any, ...]:
    if type(value) is not list or len(value) > MAX_MATRIX_ROWS:
        raise ValueError("wire tuple requires a bounded exact JSON array")
    return tuple(value)


def _dependency_closure(ids: set[str]) -> set[str]:
    selected = set(ids)
    while True:
        before = set(selected)
        for row in CAPABILITY_CATALOG:
            if row.parent_id in selected:
                selected.add(row.requirement_id)
            if row.requirement_id in selected:
                selected.update(row.dependencies)
        if selected == before:
            return selected


def _admit_pair(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> tuple[CapabilityMatrixV1, CapabilityMatrixPolicyV1]:
    matrix = _readmit(matrix, CapabilityMatrixV1)
    policy = _readmit(policy, CapabilityMatrixPolicyV1)
    if (matrix.policy_id, matrix.release_id, matrix.dataset_id) != (
        policy.policy_id,
        policy.release_id,
        policy.dataset_id,
    ):
        raise ValueError("matrix differs from frozen policy/release/dataset")
    return matrix, policy


def structural_matrix_blockers(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> tuple[CapabilityMatrixBlockerV1, ...]:
    """Find declared-state defects; an empty result NEVER certifies evidence."""
    matrix, policy = _admit_pair(matrix, policy)
    rows = {row.requirement_id: row for row in matrix.rows}
    waivers = {item.requirement_id: item for item in policy.permitted_waivers}
    blockers: list[CapabilityMatrixBlockerV1] = []
    if (
        policy.claim_kind is CapabilityClaimKindV1.FULL_V2_5_CAMPAIGN
        and policy.dataset_id is None
    ):
        blockers.append(
            CapabilityMatrixBlockerV1(
                "__matrix__", "full_claim_requires_materialized_dataset"
            )
        )
    for required in policy.requirements:
        row = rows[required.requirement_id]
        if row.evidence_scope is not required.evidence_scope:
            reason = "required_evidence_scope_differs"
        elif row.state is CapabilityStateV1.EXECUTED_PASSED:
            continue
        elif row.state is CapabilityStateV1.WAIVED_WITH_LIMITATION:
            waiver = waivers.get(row.requirement_id)
            if (
                waiver is not None
                and row.limitations == (waiver.limitation,)
                and row.waiver_policy_clause == waiver.policy_clause
            ):
                continue
            reason = "waiver_not_exactly_permitted"
        else:
            reason = "critical_" + row.state.value
        blockers.append(CapabilityMatrixBlockerV1(row.requirement_id, reason))
    return tuple(blockers)


def required_verification_rows(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> tuple[CapabilityMatrixRowV1, ...]:
    """Return claims needing concrete independent verifiers, not proof tokens."""
    matrix, policy = _admit_pair(matrix, policy)
    ids = {item.requirement_id for item in policy.requirements}
    return tuple(
        row
        for row in matrix.rows
        if row.requirement_id in ids
        and row.state is CapabilityStateV1.EXECUTED_PASSED
    )


def render_capability_matrix_markdown(matrix: CapabilityMatrixV1) -> str:
    """Render all retained claims without implying structural reads are proof."""
    matrix = _readmit(matrix, CapabilityMatrixV1)

    def escaped(value: str | None) -> str:
        return (
            "absent"
            if value is None
            else html.escape(value).replace("|", "&#124;").replace("`", "&#96;")
        )

    lines = [
        "# Capability and certification claims",
        "",
        f"Matrix: {matrix.matrix_id}",
        f"Release: {escaped(matrix.release_id)}; dataset: {escaped(matrix.dataset_id)}; as of {matrix.as_of_utc}.",
        "",
        "These are scoped retained claims. A structural read, software test or content hash is not independent scientific verification. Certification requires the frozen policy and concrete evidence verifiers; unsupported verification blocks it.",
        "",
        "| Requirement | State | Evidence scope | Implementation commit | Execution artifact | Independent verification artifact | Scope / limitations | Blocking issues | Last verified UTC | Policy / release / dataset |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in matrix.rows:
        values = (
            row.requirement_id,
            row.state.value,
            row.evidence_scope.value,
            row.implementation_commit,
            row.execution_artifact_id,
            row.independent_verification_artifact_id,
            row.scope
            + ("; " + "; ".join(row.limitations) if row.limitations else "")
            + (
                "; waiver clause: " + row.waiver_policy_clause
                if row.waiver_policy_clause
                else ""
            ),
            ", ".join(f"#{number}" for number in row.blocking_issues)
            or "none declared",
            row.last_verified_at_utc,
            f"{row.policy_id} / {row.release_id} / {row.dataset_id or 'absent'}",
        )
        lines.append(
            "| " + " | ".join(escaped(value) for value in values) + " |"
        )
    result = "\n".join(lines) + "\n"
    if len(result) > 4 * MAX_MATRIX_BYTES:
        raise ValueError("capability Markdown exceeds bound")
    return result
