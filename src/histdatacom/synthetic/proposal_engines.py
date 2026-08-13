"""First-party proposal-engine discovery, eligibility, and portfolio contracts.

The domain seam is deliberately provider-neutral, but the executable v2.4
policy is not: every evaluation and reconstruction binding in this module is
restricted to HistData.com ASCII/T datasets.  Broker targets, OANDA, live
feeds, and alternate historical providers remain deferred milestones.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TypeAlias, cast

from histdatacom.datasets.contracts import DATASET_RESOLUTION_SCHEMA_VERSION
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.add_thin import (
    ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION,
    ADD_THIN_CHECKPOINT_SCHEMA_VERSION,
    ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION,
    ADD_THIN_FIT_RESULT_SCHEMA_VERSION,
    ADD_THIN_GENERATOR_ID,
    ADD_THIN_IMPLEMENTATION_VERSION,
    ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION,
    AddThinConfigV1,
    AddThinFitResultV1,
    default_add_thin_config,
)
from histdatacom.synthetic.benchmark_corpus import (
    BenchmarkWindowMetricTraceV1,
    ReverseDegradationBenchmarkCampaignV1,
    read_reverse_degradation_benchmark_campaign,
    read_reverse_degradation_benchmark_corpus,
    run_reverse_degradation_benchmark_campaign,
    write_benchmark_window_metric_trace,
    write_reverse_degradation_benchmark_artifacts,
)
from histdatacom.synthetic.contracts import (
    JSONValue,
    canonical_contract_json,
)
from histdatacom.synthetic.event_clock import (
    ACD_GENERATOR_ID,
    COX_GENERATOR_ID,
    EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION,
    EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION,
    EVENT_CLOCK_IMPLEMENTATION_VERSION,
    HIDDEN_MARKOV_GENERATOR_ID,
    NHPP_GENERATOR_ID,
    EventClockConfigurationV1,
    EventClockFamily,
    EventClockFitResultV1,
    default_event_clock_configs,
)
from histdatacom.synthetic.generation import (
    EMPIRICAL_MOTIF_GENERATOR_ID,
    EMPIRICAL_MOTIF_GENERATOR_VERSION,
    MOTIF_CANDIDATE_BATCH_SCHEMA_VERSION,
    EmpiricalMotifGeneratorConfigV1,
)
from histdatacom.synthetic.marked_hawkes import (
    MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION,
    MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION,
    MARKED_HAWKES_IMPLEMENTATION_VERSION,
    HawkesExcitationStructure,
    MarkedHawkesConfigV1,
    MarkedHawkesFitResultV1,
    default_marked_hawkes_configs,
)
from histdatacom.synthetic.neural_tpp import (
    NEURAL_TPP_CANDIDATE_BATCH_SCHEMA_VERSION,
    NEURAL_TPP_CHECKPOINT_SCHEMA_VERSION,
    NEURAL_TPP_DATASET_MANIFEST_SCHEMA_VERSION,
    NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION,
    NEURAL_TPP_GENERATOR_ID,
    NEURAL_TPP_IMPLEMENTATION_VERSION,
    NEURAL_TPP_WINDOW_CONTEXT_SCHEMA_VERSION,
    NeuralTPPConfigV1,
    NeuralTPPFitResultV1,
    default_neural_tpp_config,
)
from histdatacom.synthetic.regime_hawkes import (
    REGIME_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION,
    REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION,
    REGIME_HAWKES_IMPLEMENTATION_VERSION,
    REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION,
    RegimeHawkesConfigV1,
    RegimeHawkesFitResultV1,
    RegimeHawkesModulation,
    default_regime_hawkes_configs,
)
from histdatacom.synthetic.schrodinger_bridge import (
    SB_CANDIDATE_BATCH_SCHEMA_VERSION,
    SB_CHECKPOINT_SCHEMA_VERSION,
    SB_DATASET_MANIFEST_SCHEMA_VERSION,
    SB_FIT_RESULT_SCHEMA_VERSION,
    SB_GENERATOR_ID,
    SB_IMPLEMENTATION_VERSION,
    SB_WINDOW_CONTEXT_SCHEMA_VERSION,
    SchrodingerBridgeConfigV1,
    SchrodingerBridgeFitResultV1,
    default_schrodinger_bridge_config,
)

PROPOSAL_ENGINE_DESCRIPTOR_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-descriptor.v1"
)
PROPOSAL_ENGINE_REGISTRY_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-registry.v1"
)
PROPOSAL_ENGINE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-evidence.v1"
)
PROPOSAL_ENGINE_BINDING_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-binding.v1"
)
PROPOSAL_ENGINE_ELIGIBILITY_AUDIT_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-eligibility-audit.v1"
)
PROPOSAL_ENGINE_PORTFOLIO_ENTRY_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-portfolio-entry.v1"
)
PROPOSAL_ENGINE_PORTFOLIO_SCHEMA_VERSION = (
    "histdatacom.proposal-engine-portfolio.v1"
)
PROPOSAL_PORTFOLIO_EVALUATION_SCHEMA_VERSION = (
    "histdatacom.proposal-portfolio-evaluation.v1"
)

CURRENT_PROPOSAL_PROVIDER_ID = "histdata.com"
CURRENT_PROPOSAL_SOURCE_FORMAT = "ascii"
CURRENT_PROPOSAL_TIMEFRAME = "T"
CURRENT_PROPOSAL_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
CURRENT_PROPOSAL_INFORMATION_MODES = (
    "ex_ante_simulation",
    "ex_post_reconstruction",
)
PROPOSAL_PORTFOLIO_FALLBACK_POLICY = "refuse-no-silent-fallback-v1"
PROPOSAL_ENGINE_SEED_POLICY = "semantic-content-seed-v1"
MAX_PROPOSAL_ENGINES = 32
MAX_PROPOSAL_EVIDENCE = 64
MAX_PROPOSAL_CONTEXT_REFS = 32
MAX_PROPOSAL_ARTIFACT_BYTES = 64 * 1024 * 1024


ProposalEngineConfigV1: TypeAlias = (
    EmpiricalMotifGeneratorConfigV1
    | EventClockConfigurationV1
    | MarkedHawkesConfigV1
    | RegimeHawkesConfigV1
    | NeuralTPPConfigV1
    | AddThinConfigV1
    | SchrodingerBridgeConfigV1
)

ProposalEngineFitResultV1: TypeAlias = (
    EventClockFitResultV1
    | MarkedHawkesFitResultV1
    | RegimeHawkesFitResultV1
    | NeuralTPPFitResultV1
    | AddThinFitResultV1
    | SchrodingerBridgeFitResultV1
)


class ProposalEngineFamily(str, Enum):
    """Stable scientific implementation family."""

    EMPIRICAL_MOTIF = "empirical_motif"
    EVENT_CLOCK = "event_clock"
    MARKED_HAWKES = "marked_hawkes"
    REGIME_HAWKES = "regime_hawkes"
    RECURRENT_MARKED_TPP = "recurrent_marked_tpp"
    ADD_THIN = "add_thin"
    SCHRODINGER_BRIDGE = "schrodinger_bridge"


class ProposalBenchmarkRole(str, Enum):
    """Role assigned within one evaluation or product portfolio."""

    BASELINE = "baseline"
    CONTROL = "control"
    CANDIDATE = "candidate"
    ABLATION = "ablation"
    REFERENCE = "reference"


class ProposalEligibility(str, Enum):
    """Maximum permission granted by current evidence and bindings."""

    REFUSED = "refused"
    RESEARCH_ONLY = "research_only"
    BENCHMARK_ELIGIBLE = "benchmark_eligible"
    RECONSTRUCTION_ELIGIBLE = "reconstruction_eligible"
    ENSEMBLE_ELIGIBLE = "ensemble_eligible"


@dataclass(frozen=True, slots=True)
class ProposalEngineDescriptorV1:
    """One installed concrete proposal-engine variant."""

    engine_id: str
    display_name: str
    family: ProposalEngineFamily
    variant: str
    implementation_version: str
    implementation_module: str
    implementation_sha256: str
    config_schema_versions: tuple[str, ...]
    fit_schema_versions: tuple[str, ...]
    checkpoint_schema_versions: tuple[str, ...]
    dataset_schema_versions: tuple[str, ...]
    context_schema_versions: tuple[str, ...]
    candidate_batch_schema_version: str
    information_modes: tuple[str, ...]
    supported_symbols: tuple[str, ...]
    mark_support: tuple[str, ...]
    deterministic_seed_policy: str
    resource_profile: Mapping[str, JSONValue]
    requires_broker_target: bool = False
    descriptor_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_DESCRIPTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_ENGINE_DESCRIPTOR_SCHEMA_VERSION:
            raise ValueError("unsupported proposal engine descriptor schema")
        object.__setattr__(self, "engine_id", _required_text(self.engine_id))
        object.__setattr__(
            self, "display_name", _required_text(self.display_name)
        )
        object.__setattr__(self, "family", ProposalEngineFamily(self.family))
        object.__setattr__(self, "variant", _required_text(self.variant))
        object.__setattr__(
            self,
            "implementation_version",
            _required_text(self.implementation_version),
        )
        object.__setattr__(
            self,
            "implementation_module",
            _required_text(self.implementation_module),
        )
        object.__setattr__(
            self,
            "implementation_sha256",
            _sha256(self.implementation_sha256, "implementation_sha256"),
        )
        for name in (
            "config_schema_versions",
            "fit_schema_versions",
            "checkpoint_schema_versions",
            "dataset_schema_versions",
            "context_schema_versions",
            "information_modes",
            "supported_symbols",
            "mark_support",
        ):
            object.__setattr__(
                self,
                name,
                _ordered_text_tuple(getattr(self, name), allow_empty=True),
            )
        if not self.config_schema_versions:
            raise ValueError("proposal engine descriptor lacks a config schema")
        if self.information_modes != CURRENT_PROPOSAL_INFORMATION_MODES:
            raise ValueError("proposal engine information modes differ")
        if self.supported_symbols != CURRENT_PROPOSAL_SYMBOLS:
            raise ValueError("proposal engine does not cover HistData triangle")
        object.__setattr__(
            self,
            "candidate_batch_schema_version",
            _required_text(self.candidate_batch_schema_version),
        )
        object.__setattr__(
            self,
            "deterministic_seed_policy",
            _required_text(self.deterministic_seed_policy),
        )
        object.__setattr__(
            self, "resource_profile", _json_mapping(self.resource_profile)
        )
        expected = _stable_id("proposal-engine-descriptor", self.payload())
        if self.descriptor_id and self.descriptor_id != expected:
            raise ValueError("proposal engine descriptor identity differs")
        object.__setattr__(self, "descriptor_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "display_name": self.display_name,
            "family": self.family.value,
            "variant": self.variant,
            "implementation_version": self.implementation_version,
            "implementation_module": self.implementation_module,
            "implementation_sha256": self.implementation_sha256,
            "config_schema_versions": list(self.config_schema_versions),
            "fit_schema_versions": list(self.fit_schema_versions),
            "checkpoint_schema_versions": list(self.checkpoint_schema_versions),
            "dataset_schema_versions": list(self.dataset_schema_versions),
            "context_schema_versions": list(self.context_schema_versions),
            "candidate_batch_schema_version": (
                self.candidate_batch_schema_version
            ),
            "information_modes": list(self.information_modes),
            "supported_symbols": list(self.supported_symbols),
            "mark_support": list(self.mark_support),
            "deterministic_seed_policy": self.deterministic_seed_policy,
            "resource_profile": dict(self.resource_profile),
            "requires_broker_target": self.requires_broker_target,
            "current_executable_scope": {
                "provider_id": CURRENT_PROPOSAL_PROVIDER_ID,
                "source_format": CURRENT_PROPOSAL_SOURCE_FORMAT,
                "timeframe": CURRENT_PROPOSAL_TIMEFRAME,
            },
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "descriptor_id": self.descriptor_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposalEngineDescriptorV1:
        _require_schema(data, PROPOSAL_ENGINE_DESCRIPTOR_SCHEMA_VERSION)
        scope = _mapping(data.get("current_executable_scope"))
        if scope != {
            "provider_id": CURRENT_PROPOSAL_PROVIDER_ID,
            "source_format": CURRENT_PROPOSAL_SOURCE_FORMAT,
            "timeframe": CURRENT_PROPOSAL_TIMEFRAME,
        }:
            raise ValueError("proposal engine executable scope differs")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            display_name=str(data.get("display_name", "")),
            family=ProposalEngineFamily(str(data.get("family", ""))),
            variant=str(data.get("variant", "")),
            implementation_version=str(data.get("implementation_version", "")),
            implementation_module=str(data.get("implementation_module", "")),
            implementation_sha256=str(data.get("implementation_sha256", "")),
            config_schema_versions=_string_tuple(
                data.get("config_schema_versions")
            ),
            fit_schema_versions=_string_tuple(data.get("fit_schema_versions")),
            checkpoint_schema_versions=_string_tuple(
                data.get("checkpoint_schema_versions")
            ),
            dataset_schema_versions=_string_tuple(
                data.get("dataset_schema_versions")
            ),
            context_schema_versions=_string_tuple(
                data.get("context_schema_versions")
            ),
            candidate_batch_schema_version=str(
                data.get("candidate_batch_schema_version", "")
            ),
            information_modes=_string_tuple(data.get("information_modes")),
            supported_symbols=_string_tuple(data.get("supported_symbols")),
            mark_support=_string_tuple(data.get("mark_support")),
            deterministic_seed_policy=str(
                data.get("deterministic_seed_policy", "")
            ),
            resource_profile=_mapping(data.get("resource_profile")),
            requires_broker_target=_strict_bool(
                data.get("requires_broker_target"),
                "requires_broker_target",
            ),
            descriptor_id=str(data.get("descriptor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEngineRegistryV1:
    """Deterministic installed registry of concrete proposal engines."""

    descriptors: tuple[ProposalEngineDescriptorV1, ...]
    registry_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_REGISTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_ENGINE_REGISTRY_SCHEMA_VERSION:
            raise ValueError("unsupported proposal engine registry schema")
        ordered = tuple(
            sorted(self.descriptors, key=lambda item: item.engine_id)
        )
        if not ordered or len(ordered) > MAX_PROPOSAL_ENGINES:
            raise ValueError("proposal engine registry size is invalid")
        if len({item.engine_id for item in ordered}) != len(ordered):
            raise ValueError("proposal engine registry IDs are duplicated")
        object.__setattr__(self, "descriptors", ordered)
        expected = _stable_id("proposal-engine-registry", self.payload())
        if self.registry_id and self.registry_id != expected:
            raise ValueError("proposal engine registry identity differs")
        object.__setattr__(self, "registry_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "descriptors": [item.to_dict() for item in self.descriptors],
            "current_executable_scope": {
                "provider_id": CURRENT_PROPOSAL_PROVIDER_ID,
                "source_format": CURRENT_PROPOSAL_SOURCE_FORMAT,
                "timeframe": CURRENT_PROPOSAL_TIMEFRAME,
                "symbols": list(CURRENT_PROPOSAL_SYMBOLS),
            },
            "deferred_scopes": [
                "oanda",
                "live_broker_feeds",
                "broker_specific_adaptation",
                "alternate_historical_providers",
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "registry_id": self.registry_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    def descriptor(self, engine_id: str) -> ProposalEngineDescriptorV1:
        selected = _required_text(engine_id)
        for item in self.descriptors:
            if item.engine_id == selected:
                return item
        raise ValueError(f"unknown proposal engine: {selected}")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposalEngineRegistryV1:
        _require_schema(data, PROPOSAL_ENGINE_REGISTRY_SCHEMA_VERSION)
        return cls(
            descriptors=tuple(
                ProposalEngineDescriptorV1.from_dict(_mapping(item))
                for item in _sequence(data.get("descriptors"))
            ),
            registry_id=str(data.get("registry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEngineEvidenceV1:
    """One compact promotion/evaluation observation for an engine."""

    engine_id: str
    campaign_id: str
    corpus_id: str
    report_id: str
    candidate_id: str
    method_name: str
    promotion_eligible: bool
    provisional: bool
    failure_count: int
    refusal_count: int
    failed_gate_ids: tuple[str, ...]
    config_ids: tuple[str, ...]
    fit_ids: tuple[str, ...]
    checkpoint_ids: tuple[str, ...]
    training_dataset_ids: tuple[str, ...]
    evidence_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_ENGINE_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported proposal engine evidence schema")
        for name in (
            "engine_id",
            "campaign_id",
            "corpus_id",
            "report_id",
            "candidate_id",
            "method_name",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "failure_count",
            _nonnegative_int(self.failure_count, "failure_count"),
        )
        object.__setattr__(
            self,
            "refusal_count",
            _nonnegative_int(self.refusal_count, "refusal_count"),
        )
        object.__setattr__(
            self,
            "failed_gate_ids",
            _ordered_text_tuple(self.failed_gate_ids, allow_empty=True),
        )
        for name in (
            "config_ids",
            "fit_ids",
            "checkpoint_ids",
            "training_dataset_ids",
        ):
            object.__setattr__(
                self,
                name,
                _ordered_text_tuple(getattr(self, name), allow_empty=True),
            )
        expected = _stable_id("proposal-engine-evidence", self.payload())
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("proposal engine evidence identity differs")
        object.__setattr__(self, "evidence_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "campaign_id": self.campaign_id,
            "corpus_id": self.corpus_id,
            "report_id": self.report_id,
            "candidate_id": self.candidate_id,
            "method_name": self.method_name,
            "promotion_eligible": self.promotion_eligible,
            "provisional": self.provisional,
            "failure_count": self.failure_count,
            "refusal_count": self.refusal_count,
            "failed_gate_ids": list(self.failed_gate_ids),
            "config_ids": list(self.config_ids),
            "fit_ids": list(self.fit_ids),
            "checkpoint_ids": list(self.checkpoint_ids),
            "training_dataset_ids": list(self.training_dataset_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposalEngineEvidenceV1:
        _require_schema(data, PROPOSAL_ENGINE_EVIDENCE_SCHEMA_VERSION)
        return cls(
            engine_id=str(data.get("engine_id", "")),
            campaign_id=str(data.get("campaign_id", "")),
            corpus_id=str(data.get("corpus_id", "")),
            report_id=str(data.get("report_id", "")),
            candidate_id=str(data.get("candidate_id", "")),
            method_name=str(data.get("method_name", "")),
            promotion_eligible=_strict_bool(
                data.get("promotion_eligible"), "promotion_eligible"
            ),
            provisional=_strict_bool(data.get("provisional"), "provisional"),
            failure_count=_strict_int(
                data.get("failure_count"), "failure_count"
            ),
            refusal_count=_strict_int(
                data.get("refusal_count"), "refusal_count"
            ),
            failed_gate_ids=_string_tuple(data.get("failed_gate_ids")),
            config_ids=_string_tuple(data.get("config_ids")),
            fit_ids=_string_tuple(data.get("fit_ids")),
            checkpoint_ids=_string_tuple(data.get("checkpoint_ids")),
            training_dataset_ids=_string_tuple(
                data.get("training_dataset_ids")
            ),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEngineBindingV1:
    """Strong artifacts required to reproduce or promote one engine."""

    engine_id: str
    descriptor_id: str
    config_id: str
    config_ref: ArtifactRef
    dataset_ref: ArtifactRef
    context_refs: tuple[ArtifactRef, ...]
    evidence_refs: tuple[ArtifactRef, ...]
    fit_ref: ArtifactRef | None = None
    checkpoint_ref: ArtifactRef | None = None
    binding_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_ENGINE_BINDING_SCHEMA_VERSION:
            raise ValueError("unsupported proposal engine binding schema")
        for name in ("engine_id", "descriptor_id", "config_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.config_ref, ArtifactRef) or not isinstance(
            self.dataset_ref, ArtifactRef
        ):
            raise TypeError("proposal engine binding requires strong artifacts")
        contexts = _artifact_refs(self.context_refs, MAX_PROPOSAL_CONTEXT_REFS)
        evidence = _artifact_refs(self.evidence_refs, MAX_PROPOSAL_EVIDENCE)
        object.__setattr__(self, "context_refs", contexts)
        object.__setattr__(self, "evidence_refs", evidence)
        for optional in (self.fit_ref, self.checkpoint_ref):
            if optional is not None and not isinstance(optional, ArtifactRef):
                raise TypeError("optional proposal binding is not ArtifactRef")
        expected = _stable_id("proposal-engine-binding", self.payload())
        if self.binding_id and self.binding_id != expected:
            raise ValueError("proposal engine binding identity differs")
        object.__setattr__(self, "binding_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "descriptor_id": self.descriptor_id,
            "config_id": self.config_id,
            "config_ref": self.config_ref.to_dict(),
            "dataset_ref": self.dataset_ref.to_dict(),
            "context_refs": [item.to_dict() for item in self.context_refs],
            "evidence_refs": [item.to_dict() for item in self.evidence_refs],
            "fit_ref": self.fit_ref.to_dict() if self.fit_ref else None,
            "checkpoint_ref": (
                self.checkpoint_ref.to_dict() if self.checkpoint_ref else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "binding_id": self.binding_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposalEngineBindingV1:
        _require_schema(data, PROPOSAL_ENGINE_BINDING_SCHEMA_VERSION)
        fit_value = data.get("fit_ref")
        checkpoint_value = data.get("checkpoint_ref")
        return cls(
            engine_id=str(data.get("engine_id", "")),
            descriptor_id=str(data.get("descriptor_id", "")),
            config_id=str(data.get("config_id", "")),
            config_ref=ArtifactRef.from_dict(_mapping(data.get("config_ref"))),
            dataset_ref=ArtifactRef.from_dict(
                _mapping(data.get("dataset_ref"))
            ),
            context_refs=tuple(
                ArtifactRef.from_dict(_mapping(item))
                for item in _sequence(data.get("context_refs"))
            ),
            evidence_refs=tuple(
                ArtifactRef.from_dict(_mapping(item))
                for item in _sequence(data.get("evidence_refs"))
            ),
            fit_ref=(
                ArtifactRef.from_dict(_mapping(fit_value))
                if fit_value is not None
                else None
            ),
            checkpoint_ref=(
                ArtifactRef.from_dict(_mapping(checkpoint_value))
                if checkpoint_value is not None
                else None
            ),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEngineEligibilityAuditV1:
    """Deterministic maximum permission for one bound engine."""

    engine_id: str
    descriptor_id: str
    binding_id: str
    eligibility: ProposalEligibility
    benchmark_eligible: bool
    reconstruction_eligible: bool
    ensemble_eligible: bool
    evidence_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    audit_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_ELIGIBILITY_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != PROPOSAL_ENGINE_ELIGIBILITY_AUDIT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported proposal eligibility audit schema")
        for name in ("engine_id", "descriptor_id", "binding_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "eligibility", ProposalEligibility(self.eligibility)
        )
        object.__setattr__(
            self,
            "evidence_ids",
            _ordered_text_tuple(self.evidence_ids, allow_empty=True),
        )
        object.__setattr__(
            self,
            "reason_codes",
            _ordered_text_tuple(self.reason_codes, allow_empty=False),
        )
        if self.ensemble_eligible and not self.reconstruction_eligible:
            raise ValueError(
                "ensemble eligibility requires reconstruction eligibility"
            )
        if self.reconstruction_eligible and not self.benchmark_eligible:
            raise ValueError(
                "reconstruction eligibility requires benchmark eligibility"
            )
        expected = _stable_id("proposal-engine-eligibility", self.payload())
        if self.audit_id and self.audit_id != expected:
            raise ValueError("proposal engine eligibility identity differs")
        object.__setattr__(self, "audit_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "descriptor_id": self.descriptor_id,
            "binding_id": self.binding_id,
            "eligibility": self.eligibility.value,
            "benchmark_eligible": self.benchmark_eligible,
            "reconstruction_eligible": self.reconstruction_eligible,
            "ensemble_eligible": self.ensemble_eligible,
            "evidence_ids": list(self.evidence_ids),
            "reason_codes": list(self.reason_codes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProposalEngineEligibilityAuditV1:
        _require_schema(data, PROPOSAL_ENGINE_ELIGIBILITY_AUDIT_SCHEMA_VERSION)
        return cls(
            engine_id=str(data.get("engine_id", "")),
            descriptor_id=str(data.get("descriptor_id", "")),
            binding_id=str(data.get("binding_id", "")),
            eligibility=ProposalEligibility(str(data.get("eligibility", ""))),
            benchmark_eligible=_strict_bool(
                data.get("benchmark_eligible"), "benchmark_eligible"
            ),
            reconstruction_eligible=_strict_bool(
                data.get("reconstruction_eligible"),
                "reconstruction_eligible",
            ),
            ensemble_eligible=_strict_bool(
                data.get("ensemble_eligible"), "ensemble_eligible"
            ),
            evidence_ids=_string_tuple(data.get("evidence_ids")),
            reason_codes=_string_tuple(data.get("reason_codes")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEnginePortfolioEntryV1:
    """One explicitly ordered engine, role, binding, and permission."""

    engine_id: str
    descriptor_id: str
    binding_id: str
    eligibility_audit_id: str
    role: ProposalBenchmarkRole
    priority: int
    selected_for_reconstruction: bool
    entry_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_PORTFOLIO_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != PROPOSAL_ENGINE_PORTFOLIO_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError("unsupported proposal portfolio entry schema")
        for name in (
            "engine_id",
            "descriptor_id",
            "binding_id",
            "eligibility_audit_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "role", ProposalBenchmarkRole(self.role))
        object.__setattr__(
            self, "priority", _nonnegative_int(self.priority, "priority")
        )
        expected = _stable_id("proposal-portfolio-entry", self.payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("proposal portfolio entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "descriptor_id": self.descriptor_id,
            "binding_id": self.binding_id,
            "eligibility_audit_id": self.eligibility_audit_id,
            "role": self.role.value,
            "priority": self.priority,
            "selected_for_reconstruction": self.selected_for_reconstruction,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProposalEnginePortfolioEntryV1:
        _require_schema(data, PROPOSAL_ENGINE_PORTFOLIO_ENTRY_SCHEMA_VERSION)
        return cls(
            engine_id=str(data.get("engine_id", "")),
            descriptor_id=str(data.get("descriptor_id", "")),
            binding_id=str(data.get("binding_id", "")),
            eligibility_audit_id=str(data.get("eligibility_audit_id", "")),
            role=ProposalBenchmarkRole(str(data.get("role", ""))),
            priority=_strict_int(data.get("priority"), "priority"),
            selected_for_reconstruction=_strict_bool(
                data.get("selected_for_reconstruction"),
                "selected_for_reconstruction",
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProposalEnginePortfolioV1:
    """Ordered engine portfolio with explicit selection and no hidden default."""

    registry_id: str
    dataset_version_id: str
    entries: tuple[ProposalEnginePortfolioEntryV1, ...]
    bindings: tuple[ProposalEngineBindingV1, ...]
    eligibility_audits: tuple[ProposalEngineEligibilityAuditV1, ...]
    evidence: tuple[ProposalEngineEvidenceV1, ...]
    qualification_dossier_id: str | None = None
    qualification_policy_id: str | None = None
    qualification_power_study_id: str | None = None
    qualification_portfolio_calibration_id: str | None = None
    qualification_decision_ids: Mapping[str, str] = field(default_factory=dict)
    portfolio_weights: Mapping[str, float] = field(default_factory=dict)
    fallback_policy: str = PROPOSAL_PORTFOLIO_FALLBACK_POLICY
    portfolio_id: str = ""
    schema_version: str = PROPOSAL_ENGINE_PORTFOLIO_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_ENGINE_PORTFOLIO_SCHEMA_VERSION:
            raise ValueError("unsupported proposal engine portfolio schema")
        object.__setattr__(
            self, "registry_id", _required_text(self.registry_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _required_text(self.dataset_version_id)
        )
        if self.fallback_policy != PROPOSAL_PORTFOLIO_FALLBACK_POLICY:
            raise ValueError("proposal portfolio permits a hidden fallback")
        entries = tuple(sorted(self.entries, key=lambda item: item.priority))
        if not entries or len(entries) > MAX_PROPOSAL_ENGINES:
            raise ValueError("proposal portfolio entry count is invalid")
        if tuple(item.priority for item in entries) != tuple(
            range(len(entries))
        ):
            raise ValueError("proposal portfolio priorities are not contiguous")
        if len({item.engine_id for item in entries}) != len(entries):
            raise ValueError("proposal portfolio engine IDs are duplicated")
        bindings = tuple(sorted(self.bindings, key=lambda item: item.engine_id))
        audits = tuple(
            sorted(self.eligibility_audits, key=lambda item: item.engine_id)
        )
        evidence = tuple(
            sorted(self.evidence, key=lambda item: item.evidence_id)
        )
        if {item.engine_id for item in bindings} != {
            item.engine_id for item in entries
        }:
            raise ValueError("proposal portfolio bindings do not cover entries")
        if {item.engine_id for item in audits} != {
            item.engine_id for item in entries
        }:
            raise ValueError("proposal portfolio audits do not cover entries")
        binding_by_engine = {item.engine_id: item for item in bindings}
        audit_by_engine = {item.engine_id: item for item in audits}
        for entry in entries:
            if (
                entry.binding_id
                != binding_by_engine[entry.engine_id].binding_id
            ):
                raise ValueError("proposal portfolio binding identity differs")
            audit = audit_by_engine[entry.engine_id]
            if entry.eligibility_audit_id != audit.audit_id:
                raise ValueError("proposal portfolio audit identity differs")
            if (
                entry.selected_for_reconstruction
                and not audit.reconstruction_eligible
            ):
                raise ValueError(
                    "unqualified engine selected for reconstruction"
                )
        qualification_ids = (
            self.qualification_dossier_id,
            self.qualification_policy_id,
            self.qualification_power_study_id,
            self.qualification_portfolio_calibration_id,
        )
        decisions = {
            _required_text(key): _required_text(value)
            for key, value in self.qualification_decision_ids.items()
        }
        weights = {
            _required_text(key): float(value)
            for key, value in self.portfolio_weights.items()
        }
        if self.qualification_dossier_id is None:
            if (
                any(value is not None for value in qualification_ids[1:])
                or decisions
                or weights
            ):
                raise ValueError(
                    "proposal portfolio has partial qualification evidence"
                )
        else:
            for name in (
                "qualification_dossier_id",
                "qualification_policy_id",
                "qualification_power_study_id",
                "qualification_portfolio_calibration_id",
            ):
                object.__setattr__(
                    self, name, _required_text(getattr(self, name))
                )
            entry_ids = {item.engine_id for item in entries}
            if set(decisions) != entry_ids:
                raise ValueError(
                    "proposal qualification decisions do not cover entries"
                )
            if not weights or not set(weights).issubset(entry_ids):
                raise ValueError(
                    "proposal qualification weights differ from entries"
                )
            if any(
                not math.isfinite(value) or value < 0.0
                for value in weights.values()
            ) or not math.isclose(sum(weights.values()), 1.0, abs_tol=1e-9):
                raise ValueError("proposal qualification weights are invalid")
            selected_ids = {
                item.engine_id
                for item in entries
                if item.selected_for_reconstruction
            }
            if not selected_ids.issubset(set(weights)):
                raise ValueError(
                    "selected proposal engine is absent from qualification weights"
                )
        object.__setattr__(
            self, "qualification_decision_ids", dict(sorted(decisions.items()))
        )
        object.__setattr__(
            self, "portfolio_weights", dict(sorted(weights.items()))
        )
        selected = tuple(
            item for item in entries if item.selected_for_reconstruction
        )
        if not selected:
            raise ValueError(
                "proposal portfolio has no qualified reconstruction engine"
            )
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "eligibility_audits", audits)
        object.__setattr__(self, "evidence", evidence)
        expected = _stable_id("proposal-engine-portfolio", self.payload())
        if self.portfolio_id and self.portfolio_id != expected:
            raise ValueError("proposal portfolio identity differs")
        object.__setattr__(self, "portfolio_id", expected)

    @property
    def selected_engine_ids(self) -> tuple[str, ...]:
        return tuple(
            item.engine_id
            for item in self.entries
            if item.selected_for_reconstruction
        )

    def binding(self, engine_id: str) -> ProposalEngineBindingV1:
        for item in self.bindings:
            if item.engine_id == engine_id:
                return item
        raise ValueError(f"proposal engine binding is absent: {engine_id}")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "dataset_version_id": self.dataset_version_id,
            "entries": [item.to_dict() for item in self.entries],
            "bindings": [item.to_dict() for item in self.bindings],
            "eligibility_audits": [
                item.to_dict() for item in self.eligibility_audits
            ],
            "evidence": [item.to_dict() for item in self.evidence],
            "qualification_dossier_id": self.qualification_dossier_id,
            "qualification_policy_id": self.qualification_policy_id,
            "qualification_power_study_id": self.qualification_power_study_id,
            "qualification_portfolio_calibration_id": (
                self.qualification_portfolio_calibration_id
            ),
            "qualification_decision_ids": dict(self.qualification_decision_ids),
            "portfolio_weights": dict(self.portfolio_weights),
            "fallback_policy": self.fallback_policy,
            "selected_engine_ids": list(self.selected_engine_ids),
            "portfolio_diversity_claim": (
                "single-qualified-engine"
                if len(self.selected_engine_ids) == 1
                else "qualified-multi-engine"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "portfolio_id": self.portfolio_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposalEnginePortfolioV1:
        _require_schema(data, PROPOSAL_ENGINE_PORTFOLIO_SCHEMA_VERSION)
        portfolio = cls(
            registry_id=str(data.get("registry_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            entries=tuple(
                ProposalEnginePortfolioEntryV1.from_dict(_mapping(item))
                for item in _sequence(data.get("entries"))
            ),
            bindings=tuple(
                ProposalEngineBindingV1.from_dict(_mapping(item))
                for item in _sequence(data.get("bindings"))
            ),
            eligibility_audits=tuple(
                ProposalEngineEligibilityAuditV1.from_dict(_mapping(item))
                for item in _sequence(data.get("eligibility_audits"))
            ),
            evidence=tuple(
                ProposalEngineEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("evidence"))
            ),
            qualification_dossier_id=(
                str(data["qualification_dossier_id"])
                if data.get("qualification_dossier_id") is not None
                else None
            ),
            qualification_policy_id=(
                str(data["qualification_policy_id"])
                if data.get("qualification_policy_id") is not None
                else None
            ),
            qualification_power_study_id=(
                str(data["qualification_power_study_id"])
                if data.get("qualification_power_study_id") is not None
                else None
            ),
            qualification_portfolio_calibration_id=(
                str(data["qualification_portfolio_calibration_id"])
                if data.get("qualification_portfolio_calibration_id")
                is not None
                else None
            ),
            qualification_decision_ids={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("qualification_decision_ids", {})
                ).items()
            },
            portfolio_weights={
                str(key): float(value)
                for key, value in _mapping(
                    data.get("portfolio_weights", {})
                ).items()
            },
            fallback_policy=str(data.get("fallback_policy", "")),
            portfolio_id=str(data.get("portfolio_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("selected_engine_ids") != list(
            portfolio.selected_engine_ids
        ):
            raise ValueError(
                "proposal portfolio selected engine derivation differs"
            )
        return portfolio


@dataclass(frozen=True, slots=True)
class ProposalPortfolioEvaluationV1:
    """Public result of executing every current benchmark-eligible engine."""

    registry_id: str
    corpus_id: str
    campaign_id: str
    requested_engine_ids: tuple[str, ...]
    reference_engine_ids: tuple[str, ...]
    executed_engine_ids: tuple[str, ...]
    refused_engine_ids: tuple[str, ...]
    engine_evidence: tuple[ProposalEngineEvidenceV1, ...]
    artifact_refs: Mapping[str, ArtifactRef]
    evaluation_id: str = ""
    schema_version: str = PROPOSAL_PORTFOLIO_EVALUATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_PORTFOLIO_EVALUATION_SCHEMA_VERSION:
            raise ValueError("unsupported proposal portfolio evaluation schema")
        for name in ("registry_id", "corpus_id", "campaign_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "requested_engine_ids",
            _ordered_text_tuple(self.requested_engine_ids, allow_empty=False),
        )
        object.__setattr__(
            self,
            "reference_engine_ids",
            _ordered_text_tuple(self.reference_engine_ids, allow_empty=True),
        )
        object.__setattr__(
            self,
            "executed_engine_ids",
            _ordered_text_tuple(self.executed_engine_ids, allow_empty=False),
        )
        object.__setattr__(
            self,
            "refused_engine_ids",
            _ordered_text_tuple(self.refused_engine_ids, allow_empty=True),
        )
        if not set(self.requested_engine_ids).issubset(
            set(self.executed_engine_ids) | set(self.refused_engine_ids)
        ):
            raise ValueError("proposal evaluation omitted a requested engine")
        if not set(self.reference_engine_ids).issubset(
            self.executed_engine_ids
        ):
            raise ValueError("proposal evaluation reference was not executed")
        refs = {
            _required_text(name): ref
            for name, ref in sorted(self.artifact_refs.items())
        }
        if not refs or any(
            not isinstance(ref, ArtifactRef) for ref in refs.values()
        ):
            raise TypeError("proposal evaluation lacks strong artifact refs")
        object.__setattr__(self, "artifact_refs", refs)
        object.__setattr__(
            self,
            "engine_evidence",
            tuple(
                sorted(self.engine_evidence, key=lambda item: item.engine_id)
            ),
        )
        expected = _stable_id("proposal-portfolio-evaluation", self.payload())
        if self.evaluation_id and self.evaluation_id != expected:
            raise ValueError("proposal portfolio evaluation identity differs")
        object.__setattr__(self, "evaluation_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "corpus_id": self.corpus_id,
            "campaign_id": self.campaign_id,
            "requested_engine_ids": list(self.requested_engine_ids),
            "reference_engine_ids": list(self.reference_engine_ids),
            "executed_engine_ids": list(self.executed_engine_ids),
            "refused_engine_ids": list(self.refused_engine_ids),
            "engine_evidence": [
                item.to_dict() for item in self.engine_evidence
            ],
            "artifact_refs": {
                name: ref.to_dict() for name, ref in self.artifact_refs.items()
            },
            "automatic_winner": False,
            "current_provider_id": CURRENT_PROPOSAL_PROVIDER_ID,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "evaluation_id": self.evaluation_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ProposalPortfolioEvaluationV1:
        _require_schema(data, PROPOSAL_PORTFOLIO_EVALUATION_SCHEMA_VERSION)
        if data.get("automatic_winner") is not False:
            raise ValueError("proposal evaluation cannot select a winner")
        return cls(
            registry_id=str(data.get("registry_id", "")),
            corpus_id=str(data.get("corpus_id", "")),
            campaign_id=str(data.get("campaign_id", "")),
            requested_engine_ids=_string_tuple(
                data.get("requested_engine_ids")
            ),
            reference_engine_ids=_string_tuple(
                data.get("reference_engine_ids")
            ),
            executed_engine_ids=_string_tuple(data.get("executed_engine_ids")),
            refused_engine_ids=_string_tuple(data.get("refused_engine_ids")),
            engine_evidence=tuple(
                ProposalEngineEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("engine_evidence"))
            ),
            artifact_refs={
                str(name): ArtifactRef.from_dict(_mapping(value))
                for name, value in _mapping(data.get("artifact_refs")).items()
            },
            evaluation_id=str(data.get("evaluation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def proposal_engine_default_configs() -> Mapping[str, ProposalEngineConfigV1]:
    """Return one deterministic default config per concrete engine variant."""
    configs: list[ProposalEngineConfigV1] = [
        EmpiricalMotifGeneratorConfigV1(),
        *default_event_clock_configs(),
        *default_marked_hawkes_configs(),
        *default_regime_hawkes_configs(),
        default_neural_tpp_config(),
        default_add_thin_config(),
        default_schrodinger_bridge_config(),
    ]
    mapped = {_engine_id_for_config(item): item for item in configs}
    if len(mapped) != 13:
        raise RuntimeError(
            "installed proposal engine config surface is incomplete"
        )
    return dict(sorted(mapped.items()))


def proposal_engine_registry() -> ProposalEngineRegistryV1:
    """Discover all installed concrete proposal engines deterministically."""
    configs = proposal_engine_default_configs()
    descriptors = tuple(
        _descriptor_for_config(engine_id, config)
        for engine_id, config in configs.items()
    )
    return ProposalEngineRegistryV1(descriptors=descriptors)


def proposal_evidence_from_campaigns(
    campaigns: Iterable[ReverseDegradationBenchmarkCampaignV1],
) -> tuple[ProposalEngineEvidenceV1, ...]:
    """Normalize retained campaign reports into engine-specific evidence."""
    by_id: dict[str, ProposalEngineEvidenceV1] = {}
    for campaign in sorted(
        campaigns, key=lambda item: (item.completed_at_utc, item.campaign_id)
    ):
        for report in campaign.candidate_reports:
            engine_id = _METHOD_TO_ENGINE_ID.get(report.method_name)
            if engine_id is None:
                continue
            failed = tuple(
                str(item.requirement_id)
                for item in report.gate_decision.checks
                if str(item.status.value) == "failed"
            )
            config_ids = _metric_ids(report.metrics, "_config_id")
            if engine_id == EMPIRICAL_MOTIF_GENERATOR_ID:
                # The benchmark campaign constructs this config internally and
                # its identity deliberately excludes resource-only limits.  Its
                # candidate report predates config IDs in the metrics payload,
                # so normalize the installed campaign contract explicitly
                # rather than treating qualified motif evidence as unbound.
                config_ids = (EmpiricalMotifGeneratorConfigV1().config_id,)
            fit_ids = _metric_ids(report.metrics, "_fit_id")
            checkpoint_ids = _metric_ids(report.metrics, "_checkpoint_id")
            training_dataset_ids = _metric_ids(report.metrics, "_dataset_id")
            evidence = ProposalEngineEvidenceV1(
                engine_id=engine_id,
                campaign_id=campaign.campaign_id,
                corpus_id=campaign.corpus_id,
                report_id=report.report_id,
                candidate_id=report.candidate_id,
                method_name=report.method_name,
                promotion_eligible=report.gate_decision.promotion_eligible,
                provisional=report.provisional,
                failure_count=report.failure_count,
                refusal_count=report.refusal_count,
                failed_gate_ids=failed,
                config_ids=config_ids,
                fit_ids=fit_ids,
                checkpoint_ids=checkpoint_ids,
                training_dataset_ids=training_dataset_ids,
            )
            by_id[evidence.evidence_id] = evidence
    return tuple(
        sorted(
            by_id.values(),
            key=lambda item: (
                item.engine_id,
                item.campaign_id,
                item.report_id,
            ),
        )
    )


def read_proposal_evidence_campaigns(
    paths: Iterable[str | Path],
) -> tuple[ReverseDegradationBenchmarkCampaignV1, ...]:
    """Hash-verify retained campaign scorecards for portfolio auditing."""
    selected = tuple(
        read_reverse_degradation_benchmark_campaign(path) for path in paths
    )
    if len(selected) > MAX_PROPOSAL_EVIDENCE:
        raise ValueError("proposal campaign evidence count exceeds limit")
    return selected


def audit_proposal_engine_binding(
    descriptor: ProposalEngineDescriptorV1,
    binding: ProposalEngineBindingV1,
    *,
    evidence: Sequence[ProposalEngineEvidenceV1],
    motif_qualification: Mapping[str, Any] | None = None,
    allow_legacy_motif_qualification: bool = False,
    qualification_decision: Any | None = None,
) -> ProposalEngineEligibilityAuditV1:
    """Derive maximum permission without silently promoting an engine."""
    if binding.engine_id != descriptor.engine_id:
        raise ValueError("proposal binding engine differs from descriptor")
    if binding.descriptor_id != descriptor.descriptor_id:
        raise ValueError("proposal binding descriptor identity differs")
    relevant = tuple(
        item for item in evidence if item.engine_id == descriptor.engine_id
    )
    matching = tuple(
        item for item in relevant if binding.config_id in item.config_ids
    )
    promoted = tuple(
        item
        for item in matching
        if item.promotion_eligible and not item.provisional
    )
    contradicted = tuple(
        item
        for item in matching
        if not item.promotion_eligible or item.provisional
    )
    evidence_ids = tuple(item.evidence_id for item in relevant)
    reasons: list[str] = []
    benchmark_eligible = True
    reconstruction_eligible = False
    ensemble_eligible = False

    if descriptor.requires_broker_target:
        benchmark_eligible = False
        eligibility = ProposalEligibility.RESEARCH_ONLY
        reasons.extend(
            (
                "broker_target_deferred_from_histdata_milestone",
                "registered_for_provider_neutral_contract_only",
            )
        )
    elif descriptor.engine_id == EMPIRICAL_MOTIF_GENERATOR_ID:
        qualification_present = bool(
            motif_qualification
            and (
                (
                    motif_qualification.get("candidate_promotion_eligible")
                    is True
                    and motif_qualification.get("candidate_provisional")
                    is False
                )
                or motif_qualification.get("qualified") is True
            )
        )
        if not relevant:
            reasons.append("retained_promotion_evidence_absent")
        elif not matching:
            reasons.append("retained_evidence_config_identity_differs")
        elif promoted and contradicted:
            reasons.append("conflicting_exact_config_promotion_evidence")
        elif contradicted or not promoted:
            reasons.append("retained_campaign_failed_promotion_gates")
        if not qualification_present:
            reasons.append("motif_qualification_missing_or_provisional")
        legacy_qualified = bool(
            allow_legacy_motif_qualification
            and qualification_present
            and not relevant
        )
        qualified = bool(
            qualification_present
            and ((promoted and not contradicted) or legacy_qualified)
        )
        if qualified:
            reconstruction_eligible = True
            ensemble_eligible = True
            eligibility = ProposalEligibility.ENSEMBLE_ELIGIBLE
            reasons.append(
                "legacy_v2_3_motif_qualification_translation"
                if legacy_qualified
                else "qualified_exact_config_modern_reference_evidence"
            )
        else:
            eligibility = ProposalEligibility.BENCHMARK_ELIGIBLE
    else:
        if not relevant:
            reasons.append("retained_promotion_evidence_absent")
        elif not matching:
            reasons.append("retained_evidence_config_identity_differs")
        elif promoted and contradicted:
            reasons.append("conflicting_exact_config_promotion_evidence")
        elif contradicted:
            reasons.append("retained_campaign_failed_promotion_gates")
        elif promoted:
            reasons.append("promotion_report_present")
        else:
            reasons.append("retained_campaign_failed_promotion_gates")
        if descriptor.fit_schema_versions and binding.fit_ref is None:
            reasons.append("reconstruction_fit_artifact_absent")
        fit_artifact_eligible = not descriptor.fit_schema_versions
        if binding.fit_ref is not None:
            promoted_fit_ids = {
                fit_id for item in promoted for fit_id in item.fit_ids
            }
            fit_artifact_eligible = bool(
                binding.fit_ref.metadata.get("engine_id")
                == descriptor.engine_id
                and binding.fit_ref.metadata.get("config_id")
                == binding.config_id
                and binding.fit_ref.metadata.get("schema_version")
                in descriptor.fit_schema_versions
                and binding.fit_ref.metadata.get("status") == "fitted"
                and binding.fit_ref.metadata.get("fit_id") in promoted_fit_ids
            )
            if not fit_artifact_eligible:
                reasons.append(
                    "reconstruction_fit_artifact_not_promoted_or_fitted"
                )
        if (
            descriptor.checkpoint_schema_versions
            and binding.checkpoint_ref is None
        ):
            reasons.append("reconstruction_checkpoint_artifact_absent")
        checkpoint_artifact_eligible = not descriptor.checkpoint_schema_versions
        if binding.checkpoint_ref is not None:
            promoted_checkpoint_ids = {
                checkpoint_id
                for item in promoted
                for checkpoint_id in item.checkpoint_ids
            }
            checkpoint_artifact_eligible = bool(
                binding.checkpoint_ref.metadata.get("engine_id")
                == descriptor.engine_id
                and binding.checkpoint_ref.metadata.get("config_id")
                == binding.config_id
                and binding.checkpoint_ref.metadata.get("schema_version")
                in descriptor.checkpoint_schema_versions
                and binding.checkpoint_ref.metadata.get("checkpoint_id")
                in promoted_checkpoint_ids
            )
            if not checkpoint_artifact_eligible:
                reasons.append(
                    "reconstruction_checkpoint_artifact_not_promoted"
                )
        can_reconstruct = (
            bool(promoted)
            and not contradicted
            and fit_artifact_eligible
            and checkpoint_artifact_eligible
        )
        if can_reconstruct:
            reconstruction_eligible = True
            ensemble_eligible = True
            eligibility = ProposalEligibility.ENSEMBLE_ELIGIBLE
            reasons.append("promotion_and_runtime_artifacts_complete")
        else:
            eligibility = ProposalEligibility.BENCHMARK_ELIGIBLE

    if qualification_decision is not None:
        if qualification_decision.engine_id != descriptor.engine_id:
            raise ValueError(
                "qualification decision engine differs from descriptor"
            )
        benchmark_eligible = bool(
            benchmark_eligible and qualification_decision.benchmark_eligible
        )
        reconstruction_eligible = bool(
            reconstruction_eligible
            and qualification_decision.reconstruction_eligible
        )
        ensemble_eligible = bool(
            ensemble_eligible
            and qualification_decision.ensemble_eligible
            and reconstruction_eligible
        )
        if ensemble_eligible:
            eligibility = ProposalEligibility.ENSEMBLE_ELIGIBLE
        elif reconstruction_eligible:
            eligibility = ProposalEligibility.RECONSTRUCTION_ELIGIBLE
        elif benchmark_eligible:
            eligibility = ProposalEligibility.BENCHMARK_ELIGIBLE
        else:
            eligibility = ProposalEligibility.RESEARCH_ONLY
        evidence_ids = (*evidence_ids, qualification_decision.decision_id)
        reasons.append(
            f"powered_qualification_{qualification_decision.status.value}"
        )
    return ProposalEngineEligibilityAuditV1(
        engine_id=descriptor.engine_id,
        descriptor_id=descriptor.descriptor_id,
        binding_id=binding.binding_id,
        eligibility=eligibility,
        benchmark_eligible=benchmark_eligible,
        reconstruction_eligible=reconstruction_eligible,
        ensemble_eligible=ensemble_eligible,
        evidence_ids=evidence_ids,
        reason_codes=tuple(reasons),
    )


def build_histdata_proposal_portfolio(
    *,
    registry: ProposalEngineRegistryV1,
    dataset_version_id: str,
    bindings: Sequence[ProposalEngineBindingV1],
    evidence: Sequence[ProposalEngineEvidenceV1],
    motif_qualification: Mapping[str, Any],
    engine_ids: Sequence[str] | None = None,
    selected_engine_ids: Sequence[str] | None = None,
    allow_legacy_motif_qualification: bool = False,
    qualification_dossier: Any | None = None,
) -> ProposalEnginePortfolioV1:
    """Build an explicitly ordered portfolio without automatic fallback."""
    ordered_ids = tuple(
        engine_ids
        if engine_ids is not None
        else (item.engine_id for item in registry.descriptors)
    )
    if not ordered_ids or len(set(ordered_ids)) != len(ordered_ids):
        raise ValueError("proposal engine ordering is empty or duplicated")
    for engine_id in ordered_ids:
        registry.descriptor(engine_id)
    selected_ids = tuple(
        selected_engine_ids
        if selected_engine_ids is not None
        else (EMPIRICAL_MOTIF_GENERATOR_ID,)
    )
    if not selected_ids or not set(selected_ids).issubset(ordered_ids):
        raise ValueError("proposal selection is absent from portfolio ordering")
    binding_by_engine = {item.engine_id: item for item in bindings}
    if set(binding_by_engine) != set(ordered_ids):
        raise ValueError("proposal bindings do not cover ordered portfolio")
    retained_evidence = tuple(
        item for item in evidence if item.engine_id in ordered_ids
    )
    decisions: dict[str, Any] = {}
    if qualification_dossier is not None:
        from histdatacom.synthetic.qualification import (
            PoweredQualificationDossierV1,
            verify_powered_qualification_dossier,
        )

        if not isinstance(qualification_dossier, PoweredQualificationDossierV1):
            raise TypeError("proposal qualification dossier must use v1")
        verify_powered_qualification_dossier(qualification_dossier)
        if qualification_dossier.registry_id != registry.registry_id:
            raise ValueError("proposal qualification registry differs")
        dossier_engine_ids = {
            item.engine_id for item in qualification_dossier.engine_decisions
        }
        if set(ordered_ids) != dossier_engine_ids:
            raise ValueError(
                "proposal engines differ from exact qualification coverage"
            )
        if retained_evidence and qualification_dossier.corpus_id not in {
            item.corpus_id for item in retained_evidence
        }:
            raise ValueError(
                "proposal qualification corpus differs from evidence"
            )
        decisions = {
            engine_id: qualification_dossier.decision(engine_id)
            for engine_id in ordered_ids
        }
        retained_evidence_ids = {item.evidence_id for item in retained_evidence}
        if any(
            not set(decision.evidence_ids).issubset(retained_evidence_ids)
            for decision in decisions.values()
        ):
            raise ValueError(
                "proposal evidence differs from exact qualification decisions"
            )
    audits = tuple(
        audit_proposal_engine_binding(
            descriptor,
            binding_by_engine[descriptor.engine_id],
            evidence=retained_evidence,
            motif_qualification=motif_qualification,
            allow_legacy_motif_qualification=(allow_legacy_motif_qualification),
            qualification_decision=decisions.get(descriptor.engine_id),
        )
        for descriptor in (
            registry.descriptor(engine_id) for engine_id in ordered_ids
        )
    )
    audit_by_engine = {item.engine_id: item for item in audits}
    refused_selection = tuple(
        engine_id
        for engine_id in selected_ids
        if not audit_by_engine[engine_id].reconstruction_eligible
    )
    if refused_selection:
        reasons = {
            engine_id: list(audit_by_engine[engine_id].reason_codes)
            for engine_id in refused_selection
        }
        raise ValueError(
            "selected proposal engines are not reconstruction eligible: "
            f"{canonical_contract_json(reasons)}"
        )
    entries = tuple(
        ProposalEnginePortfolioEntryV1(
            engine_id=engine_id,
            descriptor_id=registry.descriptor(engine_id).descriptor_id,
            binding_id=binding_by_engine[engine_id].binding_id,
            eligibility_audit_id=audit_by_engine[engine_id].audit_id,
            role=(
                ProposalBenchmarkRole.REFERENCE
                if engine_id == EMPIRICAL_MOTIF_GENERATOR_ID
                else ProposalBenchmarkRole.CANDIDATE
            ),
            priority=priority,
            selected_for_reconstruction=engine_id in selected_ids,
        )
        for priority, engine_id in enumerate(ordered_ids)
    )
    return ProposalEnginePortfolioV1(
        registry_id=registry.registry_id,
        dataset_version_id=dataset_version_id,
        entries=entries,
        bindings=tuple(bindings),
        eligibility_audits=audits,
        evidence=retained_evidence,
        qualification_dossier_id=(
            qualification_dossier.dossier_id
            if qualification_dossier is not None
            else None
        ),
        qualification_policy_id=(
            qualification_dossier.policy.policy_id
            if qualification_dossier is not None
            else None
        ),
        qualification_power_study_id=(
            qualification_dossier.power_study.study_id
            if qualification_dossier is not None
            else None
        ),
        qualification_portfolio_calibration_id=(
            qualification_dossier.portfolio_calibration.calibration_id
            if qualification_dossier is not None
            else None
        ),
        qualification_decision_ids=(
            {
                engine_id: decisions[engine_id].decision_id
                for engine_id in ordered_ids
            }
            if qualification_dossier is not None
            else {}
        ),
        portfolio_weights=(
            dict(qualification_dossier.portfolio_calibration.weights)
            if qualification_dossier is not None
            else {}
        ),
    )


def run_histdata_proposal_portfolio_evaluation(
    benchmark_manifest_path: str | Path,
    source_root: str | Path,
    *,
    output_directory: str | Path,
    engine_ids: Iterable[str] | None = None,
) -> ProposalPortfolioEvaluationV1:
    """Execute all non-broker benchmark-eligible engines on HistData.

    The bridge remains a machine-readable refusal because its currently
    implemented target is broker-conditioned.  No automatic winner or product
    promotion is performed here.
    """
    registry = proposal_engine_registry()
    requested = tuple(
        engine_ids
        if engine_ids is not None
        else (item.engine_id for item in registry.descriptors)
    )
    if not requested or len(set(requested)) != len(requested):
        raise ValueError("proposal evaluation engine selection is invalid")
    for engine_id in requested:
        registry.descriptor(engine_id)
    configs = proposal_engine_default_configs()
    event_clocks = tuple(
        cast(EventClockConfigurationV1, configs[engine_id])
        for engine_id in requested
        if engine_id.startswith("histdatacom.event-clock.")
    )
    marked_hawkes = tuple(
        cast(MarkedHawkesConfigV1, configs[engine_id])
        for engine_id in requested
        if engine_id.startswith("histdatacom.marked-hawkes.")
    )
    regime_hawkes = tuple(
        cast(RegimeHawkesConfigV1, configs[engine_id])
        for engine_id in requested
        if engine_id.startswith("histdatacom.regime-hawkes.")
    )
    corpus = read_reverse_degradation_benchmark_corpus(benchmark_manifest_path)
    metric_trace_out: list[BenchmarkWindowMetricTraceV1] = []
    fit_result_out: list[Any] = []
    campaign, motif_index = run_reverse_degradation_benchmark_campaign(
        corpus,
        source_root,
        motif_candidate_provisional=False,
        event_clock_configs=event_clocks,
        marked_hawkes_configs=marked_hawkes,
        regime_hawkes_configs=regime_hawkes,
        neural_tpp_config=(
            default_neural_tpp_config()
            if NEURAL_TPP_GENERATOR_ID in requested
            else None
        ),
        add_thin_config=(
            default_add_thin_config()
            if ADD_THIN_GENERATOR_ID in requested
            else None
        ),
        # Current HistData milestone deliberately supplies no broker target.
        schrodinger_bridge_config=None,
        schrodinger_bridge_broker_target=None,
        metric_trace_out=metric_trace_out,
        fit_result_out=fit_result_out,
    )
    if len(metric_trace_out) != 1:
        raise RuntimeError("proposal evaluation did not emit one metric trace")
    refs = dict(
        write_reverse_degradation_benchmark_artifacts(
            corpus, campaign, motif_index, output_directory
        )
    )
    refs["window_metric_trace"] = write_benchmark_window_metric_trace(
        metric_trace_out[0], output_directory
    )
    refs.update(
        _write_proposal_fit_artifacts(
            fit_result_out,
            configs={
                engine_id: configs[engine_id]
                for engine_id in requested
                if engine_id in configs
            },
            output_directory=output_directory,
        )
    )
    evidence = proposal_evidence_from_campaigns((campaign,))
    executed = tuple(item.engine_id for item in evidence)
    refused = tuple(
        engine_id
        for engine_id in requested
        if registry.descriptor(engine_id).requires_broker_target
    )
    reference = (
        (EMPIRICAL_MOTIF_GENERATOR_ID,)
        if EMPIRICAL_MOTIF_GENERATOR_ID not in requested
        else ()
    )
    result = ProposalPortfolioEvaluationV1(
        registry_id=registry.registry_id,
        corpus_id=corpus.corpus_id,
        campaign_id=campaign.campaign_id,
        requested_engine_ids=requested,
        reference_engine_ids=reference,
        executed_engine_ids=executed,
        refused_engine_ids=refused,
        engine_evidence=evidence,
        artifact_refs=refs,
    )
    _write_evaluation(result, output_directory)
    return result


def _write_proposal_fit_artifacts(
    fit_results: Sequence[Any],
    *,
    configs: Mapping[str, ProposalEngineConfigV1],
    output_directory: str | Path,
) -> dict[str, ArtifactRef]:
    """Persist every exact fit and its separately addressable model inputs.

    Failed and refused fits are evidence too, so they are retained even when
    they expose no checkpoint or dataset.  Product reconstruction may consume
    only the exact fitted artifact selected by a powered qualification; it must
    never refit from the historical product input.
    """
    config_to_engine = {
        config.config_id: engine_id for engine_id, config in configs.items()
    }
    if len(config_to_engine) != len(configs):
        raise RuntimeError("proposal engine configuration identities collide")
    by_engine: dict[str, Any] = {}
    for fit in fit_results:
        config_id = _required_text(getattr(fit, "config_id", None))
        try:
            engine_id = config_to_engine[config_id]
        except KeyError as err:
            raise ValueError(
                "proposal fit has no requested engine configuration"
            ) from err
        if engine_id in by_engine:
            raise ValueError(
                "proposal evaluation emitted duplicate engine fits"
            )
        by_engine[engine_id] = fit

    expected = {
        engine_id
        for engine_id, config in configs.items()
        if not isinstance(config, EmpiricalMotifGeneratorConfigV1)
        and not isinstance(config, SchrodingerBridgeConfigV1)
    }
    if set(by_engine) != expected:
        raise ValueError("proposal fit output differs from requested engines")

    refs: dict[str, ArtifactRef] = {}
    for index, (engine_id, fit) in enumerate(sorted(by_engine.items())):
        fit_id = _required_text(getattr(fit, "fit_id", None))
        schema_version = _required_text(getattr(fit, "schema_version", None))
        status_value = getattr(getattr(fit, "status", None), "value", None)
        fit_ref = _write_serialized_proposal_artifact(
            fit,
            output_directory,
            prefix="proposal-engine-fit",
            kind="proposal_engine_fit_v1",
            metadata={
                "artifact_role": "fit",
                "engine_id": engine_id,
                "config_id": fit.config_id,
                "fit_id": fit_id,
                "schema_version": schema_version,
                "status": _required_text(status_value),
            },
        )
        refs[f"engine_fit_{index:02d}"] = fit_ref

        dataset = getattr(fit, "dataset_manifest", None)
        if dataset is not None:
            dataset_id = _required_text(getattr(dataset, "dataset_id", None))
            refs[f"engine_training_dataset_{index:02d}"] = (
                _write_serialized_proposal_artifact(
                    dataset,
                    output_directory,
                    prefix="proposal-engine-training-dataset",
                    kind="proposal_engine_training_dataset_v1",
                    metadata={
                        "artifact_role": "training_dataset",
                        "engine_id": engine_id,
                        "config_id": fit.config_id,
                        "fit_id": fit_id,
                        "dataset_id": dataset_id,
                        "schema_version": _required_text(
                            getattr(dataset, "schema_version", None)
                        ),
                    },
                )
            )

        checkpoint = getattr(fit, "checkpoint", None)
        if checkpoint is not None:
            checkpoint_id = _required_text(
                getattr(checkpoint, "checkpoint_id", None)
            )
            refs[f"engine_checkpoint_{index:02d}"] = (
                _write_serialized_proposal_artifact(
                    checkpoint,
                    output_directory,
                    prefix="proposal-engine-checkpoint",
                    kind="proposal_engine_checkpoint_v1",
                    metadata={
                        "artifact_role": "checkpoint",
                        "engine_id": engine_id,
                        "config_id": fit.config_id,
                        "fit_id": fit_id,
                        "checkpoint_id": checkpoint_id,
                        "schema_version": _required_text(
                            getattr(checkpoint, "schema_version", None)
                        ),
                    },
                )
            )
    return refs


def _write_serialized_proposal_artifact(
    value: Any,
    root: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    serializer = getattr(value, "to_json", None)
    if not callable(serializer):
        raise TypeError("proposal artifact does not support canonical JSON")
    encoded = str(serializer()).encode("utf-8") + b"\n"
    if len(encoded) > MAX_PROPOSAL_ARTIFACT_BYTES:
        raise ValueError("proposal artifact exceeds size limit")
    directory = Path(root).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(encoded).hexdigest()
    target = directory / f"{prefix}-{digest}.json"
    if target.exists() and target.read_bytes() != encoded:
        raise ValueError("proposal artifact content-address collision")
    if not target.exists():
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, target)
    return ArtifactRef(
        kind=kind,
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(metadata),
    )


def read_proposal_portfolio_evaluation(
    path: str | Path,
) -> ProposalPortfolioEvaluationV1:
    payload = _read_content_addressed_json(
        path, "proposal-portfolio-evaluation"
    )
    return ProposalPortfolioEvaluationV1.from_dict(payload)


def proposal_evaluation_engine_artifacts(
    evaluation: ProposalPortfolioEvaluationV1,
    engine_id: str,
) -> Mapping[str, ArtifactRef]:
    """Return the unique retained model artifacts for one evaluated engine."""
    selected_engine = _required_text(engine_id)
    selected: dict[str, ArtifactRef] = {}
    for ref in evaluation.artifact_refs.values():
        if ref.metadata.get("engine_id") != selected_engine:
            continue
        role_value = ref.metadata.get("artifact_role")
        if role_value not in {"fit", "checkpoint", "training_dataset"}:
            continue
        role = str(role_value)
        if role in selected:
            raise ValueError(
                f"proposal evaluation has duplicate {role} artifacts for "
                f"{selected_engine}"
            )
        selected[role] = ref
    return dict(sorted(selected.items()))


def read_proposal_engine_fit_artifact(
    ref: ArtifactRef,
) -> ProposalEngineFitResultV1:
    """Hash-verify and restore one exact proposal-engine fit artifact."""
    if not isinstance(ref, ArtifactRef):
        raise TypeError("proposal fit reference must be an ArtifactRef")
    payload = _read_content_addressed_json(ref.path, "proposal-engine-fit")
    selected = Path(ref.path).expanduser().resolve()
    content = selected.read_bytes()
    if (
        ref.kind != "proposal_engine_fit_v1"
        or ref.size_bytes != len(content)
        or ref.sha256 != hashlib.sha256(content).hexdigest()
        or ref.metadata.get("artifact_role") != "fit"
    ):
        raise ValueError("proposal fit strong reference differs from bytes")
    schema = str(payload.get("schema_version", ""))
    readers: Mapping[str, Any] = {
        EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION: EventClockFitResultV1.from_dict,
        MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION: (
            MarkedHawkesFitResultV1.from_dict
        ),
        REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION: (
            RegimeHawkesFitResultV1.from_dict
        ),
        NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION: NeuralTPPFitResultV1.from_dict,
        ADD_THIN_FIT_RESULT_SCHEMA_VERSION: AddThinFitResultV1.from_dict,
        SB_FIT_RESULT_SCHEMA_VERSION: SchrodingerBridgeFitResultV1.from_dict,
    }
    try:
        fit = cast(ProposalEngineFitResultV1, readers[schema](payload))
    except KeyError as err:
        raise ValueError("proposal fit schema is unsupported") from err
    status = _required_text(getattr(fit.status, "value", None))
    if (
        ref.metadata.get("schema_version") != fit.schema_version
        or ref.metadata.get("config_id") != fit.config_id
        or ref.metadata.get("fit_id") != fit.fit_id
        or ref.metadata.get("status") != status
    ):
        raise ValueError("proposal fit metadata differs from restored model")
    return fit


def _descriptor_for_config(
    engine_id: str, config: ProposalEngineConfigV1
) -> ProposalEngineDescriptorV1:
    family, variant, version, module_name = _engine_metadata(engine_id, config)
    fit_schemas: tuple[str, ...]
    checkpoint_schemas: tuple[str, ...] = ()
    dataset_schemas: tuple[str, ...] = (DATASET_RESOLUTION_SCHEMA_VERSION,)
    context_schemas: tuple[str, ...] = ()
    candidate_schema: str
    requires_broker_target = False
    if isinstance(config, EmpiricalMotifGeneratorConfigV1):
        fit_schemas = ()
        candidate_schema = MOTIF_CANDIDATE_BATCH_SCHEMA_VERSION
        context_schemas = ("histdatacom.reference-motif-query.v1",)
    elif isinstance(config, EventClockConfigurationV1):
        fit_schemas = (EVENT_CLOCK_FIT_RESULT_SCHEMA_VERSION,)
        candidate_schema = EVENT_CLOCK_CANDIDATE_BATCH_SCHEMA_VERSION
    elif isinstance(config, MarkedHawkesConfigV1):
        fit_schemas = (MARKED_HAWKES_FIT_RESULT_SCHEMA_VERSION,)
        candidate_schema = MARKED_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION
    elif isinstance(config, RegimeHawkesConfigV1):
        fit_schemas = (REGIME_HAWKES_FIT_RESULT_SCHEMA_VERSION,)
        candidate_schema = REGIME_HAWKES_CANDIDATE_BATCH_SCHEMA_VERSION
        context_schemas = (REGIME_HAWKES_WINDOW_CONTEXT_SCHEMA_VERSION,)
    elif isinstance(config, NeuralTPPConfigV1):
        fit_schemas = (NEURAL_TPP_FIT_RESULT_SCHEMA_VERSION,)
        checkpoint_schemas = (NEURAL_TPP_CHECKPOINT_SCHEMA_VERSION,)
        dataset_schemas += (NEURAL_TPP_DATASET_MANIFEST_SCHEMA_VERSION,)
        context_schemas = (NEURAL_TPP_WINDOW_CONTEXT_SCHEMA_VERSION,)
        candidate_schema = NEURAL_TPP_CANDIDATE_BATCH_SCHEMA_VERSION
    elif isinstance(config, AddThinConfigV1):
        fit_schemas = (ADD_THIN_FIT_RESULT_SCHEMA_VERSION,)
        checkpoint_schemas = (ADD_THIN_CHECKPOINT_SCHEMA_VERSION,)
        dataset_schemas += (ADD_THIN_DATASET_MANIFEST_SCHEMA_VERSION,)
        context_schemas = (ADD_THIN_WINDOW_CONTEXT_SCHEMA_VERSION,)
        candidate_schema = ADD_THIN_CANDIDATE_BATCH_SCHEMA_VERSION
    elif isinstance(config, SchrodingerBridgeConfigV1):
        fit_schemas = (SB_FIT_RESULT_SCHEMA_VERSION,)
        checkpoint_schemas = (SB_CHECKPOINT_SCHEMA_VERSION,)
        dataset_schemas += (SB_DATASET_MANIFEST_SCHEMA_VERSION,)
        context_schemas = (SB_WINDOW_CONTEXT_SCHEMA_VERSION,)
        candidate_schema = SB_CANDIDATE_BATCH_SCHEMA_VERSION
        requires_broker_target = True
    else:  # pragma: no cover - guarded by default config construction
        raise TypeError("unsupported proposal engine config")
    return ProposalEngineDescriptorV1(
        engine_id=engine_id,
        display_name=variant.replace("_", " ").title(),
        family=family,
        variant=variant,
        implementation_version=version,
        implementation_module=module_name,
        implementation_sha256=_module_sha256(module_name),
        config_schema_versions=(config.schema_version,),
        fit_schema_versions=fit_schemas,
        checkpoint_schema_versions=checkpoint_schemas,
        dataset_schema_versions=dataset_schemas,
        context_schema_versions=context_schemas,
        candidate_batch_schema_version=candidate_schema,
        information_modes=CURRENT_PROPOSAL_INFORMATION_MODES,
        supported_symbols=CURRENT_PROPOSAL_SYMBOLS,
        mark_support=("ask_only", "bid_only", "joint", "unchanged"),
        deterministic_seed_policy=PROPOSAL_ENGINE_SEED_POLICY,
        resource_profile=_resource_profile(config),
        requires_broker_target=requires_broker_target,
    )


def _engine_metadata(
    engine_id: str, config: ProposalEngineConfigV1
) -> tuple[ProposalEngineFamily, str, str, str]:
    if isinstance(config, EmpiricalMotifGeneratorConfigV1):
        return (
            ProposalEngineFamily.EMPIRICAL_MOTIF,
            "empirical_motif_resampling",
            EMPIRICAL_MOTIF_GENERATOR_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, EventClockConfigurationV1):
        return (
            ProposalEngineFamily.EVENT_CLOCK,
            config.family.value,
            EVENT_CLOCK_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, MarkedHawkesConfigV1):
        return (
            ProposalEngineFamily.MARKED_HAWKES,
            config.excitation_structure.value,
            MARKED_HAWKES_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, RegimeHawkesConfigV1):
        return (
            ProposalEngineFamily.REGIME_HAWKES,
            config.modulation.value,
            REGIME_HAWKES_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, NeuralTPPConfigV1):
        return (
            ProposalEngineFamily.RECURRENT_MARKED_TPP,
            config.architecture,
            NEURAL_TPP_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, AddThinConfigV1):
        return (
            ProposalEngineFamily.ADD_THIN,
            config.architecture,
            ADD_THIN_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    if isinstance(config, SchrodingerBridgeConfigV1):
        return (
            ProposalEngineFamily.SCHRODINGER_BRIDGE,
            config.architecture,
            SB_IMPLEMENTATION_VERSION,
            config.__class__.__module__,
        )
    raise TypeError(f"unsupported proposal engine config for {engine_id}")


def _engine_id_for_config(config: ProposalEngineConfigV1) -> str:
    if isinstance(config, EmpiricalMotifGeneratorConfigV1):
        return str(EMPIRICAL_MOTIF_GENERATOR_ID)
    if isinstance(config, EventClockConfigurationV1):
        return str(
            {
                EventClockFamily.NHPP: NHPP_GENERATOR_ID,
                EventClockFamily.COX: COX_GENERATOR_ID,
                EventClockFamily.ACD: ACD_GENERATOR_ID,
                EventClockFamily.HIDDEN_MARKOV: HIDDEN_MARKOV_GENERATOR_ID,
            }[config.family]
        )
    if isinstance(config, MarkedHawkesConfigV1):
        return f"histdatacom.marked-hawkes.{config.excitation_structure.value}"
    if isinstance(config, RegimeHawkesConfigV1):
        return f"histdatacom.regime-hawkes.{config.modulation.value}"
    if isinstance(config, NeuralTPPConfigV1):
        return str(NEURAL_TPP_GENERATOR_ID)
    if isinstance(config, AddThinConfigV1):
        return str(ADD_THIN_GENERATOR_ID)
    if isinstance(config, SchrodingerBridgeConfigV1):
        return str(SB_GENERATOR_ID)
    raise TypeError("unsupported proposal engine config")


def _resource_profile(config: ProposalEngineConfigV1) -> dict[str, JSONValue]:
    limits = getattr(config, "limits", None)
    payload: dict[str, JSONValue] = {
        "runtime": "cpu",
        "config_id": config.config_id,
        "config_schema_version": config.schema_version,
    }
    if limits is not None and hasattr(limits, "to_dict"):
        payload["limits"] = cast(Any, limits).to_dict()
    else:
        payload["limits"] = {
            "max_events_per_interval": getattr(
                config, "max_events_per_interval", None
            ),
            "estimated_bytes_per_event": getattr(
                config, "estimated_bytes_per_event", None
            ),
        }
    return payload


def _module_sha256(module_name: str) -> str:
    module = __import__(module_name, fromlist=["__name__"])
    path_value = inspect.getsourcefile(module)
    if path_value is None:
        raise RuntimeError(
            f"proposal engine module has no source: {module_name}"
        )
    return hashlib.sha256(Path(path_value).read_bytes()).hexdigest()


def _write_evaluation(
    result: ProposalPortfolioEvaluationV1, root: str | Path
) -> ArtifactRef:
    directory = Path(root).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    encoded = result.to_json().encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    target = directory / f"proposal-portfolio-evaluation-{digest}.json"
    if target.exists() and target.read_bytes() != encoded:
        raise ValueError("proposal evaluation artifact collision")
    if not target.exists():
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, target)
    return ArtifactRef(
        kind="proposal_portfolio_evaluation_v1",
        path=str(target),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={"evaluation_id": result.evaluation_id},
    )


def _read_content_addressed_json(
    path: str | Path, prefix: str
) -> Mapping[str, Any]:
    selected = Path(path).expanduser().resolve()
    expected_prefix = f"{prefix}-"
    if not selected.name.startswith(
        expected_prefix
    ) or not selected.name.endswith(".json"):
        raise ValueError("proposal artifact name is not content addressed")
    digest = selected.name[len(expected_prefix) : -5]
    content = selected.read_bytes()
    if len(content) > MAX_PROPOSAL_ARTIFACT_BYTES:
        raise ValueError("proposal artifact exceeds size limit")
    if (
        _sha256(digest, "artifact filename digest")
        != hashlib.sha256(content).hexdigest()
    ):
        raise ValueError("proposal artifact digest differs")
    payload = json.loads(content.decode("utf-8"))
    return _mapping(payload)


def _artifact_refs(
    values: Iterable[ArtifactRef], maximum: int
) -> tuple[ArtifactRef, ...]:
    selected = tuple(
        sorted(
            values,
            key=lambda item: (item.kind, item.sha256, item.path),
        )
    )
    if len(selected) > maximum or any(
        not isinstance(item, ArtifactRef) for item in selected
    ):
        raise ValueError("proposal artifact reference set is invalid")
    if len({(item.kind, item.sha256) for item in selected}) != len(selected):
        raise ValueError("proposal artifact references are duplicated")
    return selected


def _metric_ids(
    metrics: Mapping[str, JSONValue], suffix: str
) -> tuple[str, ...]:
    return tuple(
        str(value)
        for name, value in metrics.items()
        if name.endswith(suffix) and isinstance(value, str) and value
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("required proposal text is empty")
    return value.strip()


def _sha256(value: Any, name: str) -> str:
    selected = _required_text(value)
    if len(selected) != 64 or any(
        character not in "0123456789abcdef" for character in selected
    ):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return selected


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _ordered_text_tuple(
    values: Iterable[Any], *, allow_empty: bool
) -> tuple[str, ...]:
    selected = tuple(sorted({_required_text(item) for item in values}))
    if not selected and not allow_empty:
        raise ValueError("proposal text collection is empty")
    return selected


def _json_mapping(values: Mapping[str, Any]) -> dict[str, JSONValue]:
    restored = json.loads(canonical_contract_json(dict(values)))
    if not isinstance(restored, dict):
        raise TypeError("proposal mapping does not encode as an object")
    return cast(dict[str, JSONValue], restored)


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("proposal value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("proposal value must be a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value))


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported proposal contract schema")


_METHOD_TO_ENGINE_ID = {
    "empirical_motif": EMPIRICAL_MOTIF_GENERATOR_ID,
    EventClockFamily.NHPP.value: NHPP_GENERATOR_ID,
    EventClockFamily.COX.value: COX_GENERATOR_ID,
    EventClockFamily.ACD.value: ACD_GENERATOR_ID,
    EventClockFamily.HIDDEN_MARKOV.value: HIDDEN_MARKOV_GENERATOR_ID,
    **{
        f"marked_hawkes_{item.value}": f"histdatacom.marked-hawkes.{item.value}"
        for item in HawkesExcitationStructure
    },
    **{
        f"regime_hawkes_{item.value}": f"histdatacom.regime-hawkes.{item.value}"
        for item in RegimeHawkesModulation
    },
    "neural_tpp_rmtpp_cpu_v1": NEURAL_TPP_GENERATOR_ID,
    "add_thin_histogram_marked_cpu_v1": ADD_THIN_GENERATOR_ID,
    "schrodinger_bridge_markov_sinkhorn_cpu_v1": SB_GENERATOR_ID,
}


__all__ = [
    "CURRENT_PROPOSAL_PROVIDER_ID",
    "PROPOSAL_ENGINE_BINDING_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_DESCRIPTOR_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_ELIGIBILITY_AUDIT_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_EVIDENCE_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_PORTFOLIO_ENTRY_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_PORTFOLIO_SCHEMA_VERSION",
    "PROPOSAL_ENGINE_REGISTRY_SCHEMA_VERSION",
    "PROPOSAL_PORTFOLIO_EVALUATION_SCHEMA_VERSION",
    "ProposalBenchmarkRole",
    "ProposalEligibility",
    "ProposalEngineBindingV1",
    "ProposalEngineDescriptorV1",
    "ProposalEngineEligibilityAuditV1",
    "ProposalEngineEvidenceV1",
    "ProposalEngineFamily",
    "ProposalEngineFitResultV1",
    "ProposalEnginePortfolioEntryV1",
    "ProposalEnginePortfolioV1",
    "ProposalEngineRegistryV1",
    "ProposalPortfolioEvaluationV1",
    "audit_proposal_engine_binding",
    "build_histdata_proposal_portfolio",
    "proposal_engine_default_configs",
    "proposal_engine_registry",
    "proposal_evaluation_engine_artifacts",
    "proposal_evidence_from_campaigns",
    "read_proposal_engine_fit_artifact",
    "read_proposal_evidence_campaigns",
    "read_proposal_portfolio_evaluation",
    "run_histdata_proposal_portfolio_evaluation",
]
