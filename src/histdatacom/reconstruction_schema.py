"""Deterministic schema discovery and compatibility for reconstruction.

The registry is intentionally provider-neutral at the domain-contract layer,
while the executable compatibility policy is narrower: v2.4 reconstruction
accepts only local HistData.com ASCII/T tick caches.  Alternate providers and
broker-conditioned delivery remain discoverable seams for later milestones.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import MISSING, dataclass, fields, is_dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any

from histdatacom.data_quality.training_features import (
    IDENTITY_COLUMNS,
    SYNTHETIC_PLACEHOLDER_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    training_feature_definitions,
)

RECONSTRUCTION_SCHEMA_REGISTRY_VERSION = (
    "histdatacom.reconstruction-schema-registry.v1"
)
RECONSTRUCTION_FIELD_DEFINITION_VERSION = (
    "histdatacom.reconstruction-field-definition.v1"
)
RECONSTRUCTION_CONTRACT_DEFINITION_VERSION = (
    "histdatacom.reconstruction-contract-definition.v1"
)
RECONSTRUCTION_COMPATIBILITY_FINDING_VERSION = (
    "histdatacom.reconstruction-compatibility-finding.v1"
)
RECONSTRUCTION_CACHE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-cache-schema.v1"
)
RECONSTRUCTION_COMPATIBILITY_REPORT_VERSION = (
    "histdatacom.reconstruction-compatibility-report.v1"
)

LEGACY_HISTDATA_CACHE_SCHEMA_VERSION = (
    "histdatacom.histdata-ascii-tick-cache.legacy"
)
CURRENT_PLAN_SCHEMA_VERSION = "histdatacom.reconstruction-plan-spec.v1"
PORTFOLIO_PLAN_SCHEMA_VERSION = "histdatacom.reconstruction-plan-spec.v2"
HISTDATA_PROVIDER_ID = "histdata.com"
SUPPORTED_SOURCE_FORMAT = "ascii"
SUPPORTED_TIMEFRAME = "T"
SUPPORTED_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
INFORMATION_MODES = ("ex_ante_simulation", "ex_post_reconstruction")

MAX_REGISTRY_CONTRACTS = 512
MAX_FIELDS_PER_CONTRACT = 1024
MAX_REGISTRY_BYTES = 8 * 1024 * 1024
MAX_COMPATIBILITY_FINDINGS = 128
MAX_SOURCE_PARTITIONS = 4096
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024


class ReconstructionContractStatus(str, Enum):
    """Lifecycle or exposure status for a contract or field."""

    REQUIRED = "required"
    OPTIONAL = "optional"
    DEPRECATED = "deprecated"
    RESERVED = "reserved"
    INTERNAL_ONLY = "internal_only"


class ReconstructionCompatibilityStatus(str, Enum):
    """Stable compatibility classifications, ordered by severity below."""

    EXACT = "exact"
    COMPATIBLE_TRANSLATION = "compatible_translation"
    DEPRECATED = "deprecated"
    RESEARCH_ONLY = "research_only"
    STALE = "stale"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class ReconstructionFieldDefinitionV1:
    """One publication-safe field definition in a versioned contract."""

    contract_schema_version: str
    name: str
    dtype: str
    nullable: bool
    status: ReconstructionContractStatus
    grain: str
    identity_role: str
    lineage: str
    basis: str
    source_derived_status: str
    availability: str
    publication_safety: str
    information_modes: tuple[str, ...]
    consumer_stages: tuple[str, ...]
    description: str
    schema_version: str = RECONSTRUCTION_FIELD_DEFINITION_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return deterministic machine-readable field metadata."""
        return {
            "schema_version": self.schema_version,
            "contract_schema_version": self.contract_schema_version,
            "name": self.name,
            "dtype": self.dtype,
            "nullable": self.nullable,
            "status": self.status.value,
            "grain": self.grain,
            "identity_role": self.identity_role,
            "lineage": self.lineage,
            "basis": self.basis,
            "source_derived_status": self.source_derived_status,
            "availability": self.availability,
            "publication_safety": self.publication_safety,
            "information_modes": list(self.information_modes),
            "consumer_stages": list(self.consumer_stages),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionContractDefinitionV1:
    """One versioned contract or explicitly audited internal schema."""

    contract_schema_version: str
    name: str
    family: str
    status: ReconstructionContractStatus
    grain: str
    publication_safety: str
    information_modes: tuple[str, ...]
    consumer_stages: tuple[str, ...]
    fields: tuple[ReconstructionFieldDefinitionV1, ...]
    implementation: str
    audit_note: str
    schema_version: str = RECONSTRUCTION_CONTRACT_DEFINITION_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return deterministic machine-readable contract metadata."""
        return {
            "schema_version": self.schema_version,
            "contract_schema_version": self.contract_schema_version,
            "name": self.name,
            "family": self.family,
            "status": self.status.value,
            "grain": self.grain,
            "publication_safety": self.publication_safety,
            "information_modes": list(self.information_modes),
            "consumer_stages": list(self.consumer_stages),
            "fields": [item.to_dict() for item in self.fields],
            "implementation": self.implementation,
            "audit_note": self.audit_note,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionSchemaRegistryV1:
    """Bounded deterministic inventory of reconstruction contracts."""

    contracts: tuple[ReconstructionContractDefinitionV1, ...]
    current_provider: str = HISTDATA_PROVIDER_ID
    current_source_format: str = SUPPORTED_SOURCE_FORMAT
    current_timeframe: str = SUPPORTED_TIMEFRAME
    registry_id: str = ""
    schema_version: str = RECONSTRUCTION_SCHEMA_REGISTRY_VERSION

    def __post_init__(self) -> None:
        ordered = tuple(
            sorted(
                self.contracts, key=lambda item: item.contract_schema_version
            )
        )
        if not ordered or len(ordered) > MAX_REGISTRY_CONTRACTS:
            raise ValueError("reconstruction schema registry size is invalid")
        versions = tuple(item.contract_schema_version for item in ordered)
        if len(set(versions)) != len(versions):
            raise ValueError("reconstruction schema registry versions differ")
        object.__setattr__(self, "contracts", ordered)
        expected = _stable_id("reconstruction-schema-registry", self.payload())
        if self.registry_id and self.registry_id != expected:
            raise ValueError("reconstruction schema registry identity differs")
        object.__setattr__(self, "registry_id", expected)
        if len(self.to_json().encode("utf-8")) > MAX_REGISTRY_BYTES:
            raise ValueError(
                "reconstruction schema registry exceeds size limit"
            )

    def payload(self) -> dict[str, Any]:
        """Return identity-bearing registry content."""
        counts = Counter(item.status.value for item in self.contracts)
        return {
            "schema_version": self.schema_version,
            "current_scope": {
                "provider": self.current_provider,
                "source_format": self.current_source_format,
                "timeframe": self.current_timeframe,
                "alternate_providers": "later_milestone",
                "broker_oanda": "later_milestone",
            },
            "contract_count": len(self.contracts),
            "status_counts": dict(sorted(counts.items())),
            "contracts": [item.to_dict() for item in self.contracts],
        }

    def to_dict(self) -> dict[str, Any]:
        """Return the complete deterministic registry."""
        return {**self.payload(), "registry_id": self.registry_id}

    def to_json(self) -> str:
        """Serialize canonical JSON without environment-specific state."""
        return _canonical_json(self.to_dict())


@dataclass(frozen=True, slots=True)
class ReconstructionCompatibilityFindingV1:
    """One bounded compatibility decision and remediation."""

    code: str
    status: ReconstructionCompatibilityStatus
    location: str
    message: str
    remediation: str
    schema_version: str = RECONSTRUCTION_COMPATIBILITY_FINDING_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return publication-safe finding metadata."""
        return {
            "schema_version": self.schema_version,
            "code": self.code,
            "status": self.status.value,
            "location": self.location,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionCacheSchemaV1:
    """Aggregated Arrow schema evidence without local paths or row payloads."""

    cache_schema_version: str
    status: ReconstructionCompatibilityStatus
    columns: tuple[str, ...]
    partition_count: int
    identity_policy: str
    schema_version: str = RECONSTRUCTION_CACHE_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return bounded cache-schema evidence."""
        return {
            "schema_version": self.schema_version,
            "cache_schema_version": self.cache_schema_version,
            "status": self.status.value,
            "columns": list(self.columns),
            "partition_count": self.partition_count,
            "identity_policy": self.identity_policy,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionCompatibilityReportV1:
    """Deterministic fail-closed compatibility result for one proposed plan."""

    subject_schema_version: str
    status: ReconstructionCompatibilityStatus
    executable: bool
    registry_id: str
    findings: tuple[ReconstructionCompatibilityFindingV1, ...]
    translations: tuple[str, ...]
    cache_schemas: tuple[ReconstructionCacheSchemaV1, ...]
    report_id: str = ""
    schema_version: str = RECONSTRUCTION_COMPATIBILITY_REPORT_VERSION

    def __post_init__(self) -> None:
        if len(self.findings) > MAX_COMPATIBILITY_FINDINGS:
            raise ValueError("compatibility finding count exceeds limit")
        expected = _stable_id("reconstruction-compatibility", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("compatibility report identity differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, Any]:
        """Return identity-bearing compatibility content."""
        return {
            "schema_version": self.schema_version,
            "subject_schema_version": self.subject_schema_version,
            "status": self.status.value,
            "executable": self.executable,
            "registry_id": self.registry_id,
            "findings": [item.to_dict() for item in self.findings],
            "translations": list(self.translations),
            "cache_schemas": [item.to_dict() for item in self.cache_schemas],
        }

    def to_dict(self) -> dict[str, Any]:
        """Return the complete deterministic compatibility report."""
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        """Serialize canonical JSON."""
        return _canonical_json(self.to_dict())


_AUDITED_MODULES = (
    "histdatacom.datasets.contracts",
    "histdatacom.datasets.catalog",
    "histdatacom.datasets.adapters",
    "histdatacom.datasets.projection",
    "histdatacom.data_quality.autoregressive",
    "histdatacom.data_quality.bounded_payload_contracts",
    "histdatacom.data_quality.calendar",
    "histdatacom.data_quality.calendar_profiles",
    "histdatacom.data_quality.campaign",
    "histdatacom.data_quality.classical_baselines",
    "histdatacom.data_quality.classical_model_comparison",
    "histdatacom.data_quality.classical_model_contracts",
    "histdatacom.data_quality.engine",
    "histdatacom.data_quality.exponential_smoothing",
    "histdatacom.data_quality.fingerprint_contracts",
    "histdatacom.data_quality.fingerprint_discovery",
    "histdatacom.data_quality.fingerprint_next_work",
    "histdatacom.data_quality.fingerprints",
    "histdatacom.data_quality.manifest",
    "histdatacom.data_quality.preflight",
    "histdatacom.data_quality.profiles",
    "histdatacom.data_quality.provenance",
    "histdatacom.data_quality.remediation",
    "histdatacom.data_quality.remediation_audit",
    "histdatacom.data_quality.repair_plan",
    "histdatacom.data_quality.reporting",
    "histdatacom.data_quality.seasonal_exogenous",
    "histdatacom.data_quality.state_space",
    "histdatacom.data_quality.symbols",
    "histdatacom.data_quality.synthetic_constraints",
    "histdatacom.data_quality.synthetic_generation",
    "histdatacom.data_quality.time",
    "histdatacom.data_quality.training_features",
    "histdatacom.data_quality.volatility",
    "histdatacom.data_analytics.feed_epochs",
    "histdatacom.data_analytics.feed_epochs_v2",
    "histdatacom.data_analytics.feed_regimes",
    "histdatacom.market_context.contracts",
    "histdatacom.market_context.corpus",
    "histdatacom.market_context.positioning",
    "histdatacom.orchestration.reconstruction",
    "histdatacom.reconstruction",
    "histdatacom.synthetic.contracts",
    "histdatacom.synthetic.streaming",
    "histdatacom.synthetic.information",
    "histdatacom.synthetic.observation",
    "histdatacom.synthetic.observation_calibration",
    "histdatacom.synthetic.benchmark",
    "histdatacom.synthetic.benchmark_corpus",
    "histdatacom.synthetic.benchmark_gates",
    "histdatacom.synthetic.motifs",
    "histdatacom.synthetic.motif_library",
    "histdatacom.synthetic.generation",
    "histdatacom.synthetic.carving",
    "histdatacom.synthetic.cross_currency",
    "histdatacom.synthetic.ensembles",
    "histdatacom.synthetic.delivery",
    "histdatacom.synthetic.persistence",
    "histdatacom.synthetic.reconstruction_plan",
    "histdatacom.synthetic.reconstruction_handlers",
    "histdatacom.synthetic.certification",
    "histdatacom.synthetic.certification_campaign",
    "histdatacom.synthetic.event_clock",
    "histdatacom.synthetic.marked_hawkes",
    "histdatacom.synthetic.regime_hawkes",
    "histdatacom.synthetic.neural_tpp",
    "histdatacom.synthetic.add_thin",
    "histdatacom.synthetic.schrodinger_bridge",
    "histdatacom.synthetic.activity",
    "histdatacom.synthetic.bars",
    "histdatacom.synthetic.strategy_sensitivity",
)

_REQUIRED_SCHEMA_TOKENS = (
    "source-provider",
    "provider-source-inventory",
    "observed-partition",
    "dataset-catalog",
    "dataset-version",
    "canonical-ascii-tick",
    "ascii-tick-training-features",
    "synthetic-event.v1",
    "reconstruction-event-batch",
    "feed-epoch-definition.v2",
    "observation-operator.v1",
    "market-context-corpus",
    "cftc-positioning-corpus",
    "reverse-degradation-benchmark-manifest",
    "modern-reference-motif-manifest",
    "modern-reference-motif-index",
    "modern-reference-motif-qualification",
    "modern-reference-motif-leakage",
    "reconstruction-ensemble-config",
    "reconstruction-plan-spec.v1",
    "synthetic-infill-plan",
    "reconstruction-checkpoint",
    "reconstruction-product.v2",
)

_RESERVED_CONTRACTS = (
    (
        "histdatacom.reconstruction-evidence-projection.v1",
        "PointInTimeEvidenceProjectionV1",
        "evidence",
        "Reserved for #483 point-in-time quality evidence.",
        ("evidence_window_id", "available_at_ns", "source_partition_id"),
    ),
    (
        "histdatacom.cross-series-constraint-window.v1",
        "CrossSeriesConstraintWindowV1",
        "cross_series",
        "Reserved for #484 synchronized cross-series constraints.",
        ("constraint_window_id", "synchronization_unit_id", "symbols"),
    ),
    (
        "histdatacom.reconstruction-experiment-manifest.v1",
        "ReconstructionExperimentManifestV1",
        "experiment",
        "Reserved for #486 catalog-bound experiment manifests.",
        ("experiment_id", "dataset_version_id", "plan_id"),
    ),
    (
        "histdatacom.proposal-engine-descriptor.v1",
        "ProposalEngineDescriptorV1",
        "proposal",
        "Reserved for #489 proposal-engine portfolio discovery.",
        ("engine_id", "engine_version", "fitted_artifact_schema_version"),
    ),
    (
        PORTFOLIO_PLAN_SCHEMA_VERSION,
        "ReconstructionPlanSpecV2",
        "plan",
        "Reserved for #489 portfolio-bound planning and execution.",
        ("dataset_version_id", "proposal_engines", "information_mode"),
    ),
)

_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "source_root",
        "source_provider_id",
        "provider",
        "dataset_version_id",
        "feed_epoch_definition_path",
        "observation_operator_path",
        "market_context_corpus_path",
        "cftc_positioning_corpus_path",
        "benchmark_manifest_path",
        "motif_manifest_path",
        "motif_index_path",
        "motif_qualification_path",
        "motif_leakage_audit_path",
        "artifact_root",
        "output_root",
        "checkpoint_root",
        "scratch_root",
        "information_mode",
        "start_period",
        "end_period",
        "requested_start_ns",
        "requested_end_ns",
        "window_size_ns",
        "delivery_mode",
        "broker_delivery_artifact",
        "source_format",
        "timeframe",
        "symbols",
        "scientific_nonclaim",
    }
)

_ARTIFACT_SCHEMA_EXPECTATIONS = {
    "feed_epoch_definition_path": ("histdatacom.feed-epoch-definition.v2",),
    "observation_operator_path": ("histdatacom.observation-operator.v1",),
    "market_context_corpus_path": ("histdatacom.market-context-corpus.v1",),
    "cftc_positioning_corpus_path": ("histdatacom.cftc-positioning-corpus.v1",),
    "benchmark_manifest_path": (
        "histdatacom.reverse-degradation-benchmark-manifest.v1",
        "histdatacom.reverse-degradation-corpus.v1",
    ),
    "motif_manifest_path": ("histdatacom.modern-reference-motif-manifest.v1",),
    "motif_index_path": ("histdatacom.reference-motif-index.v1",),
    "motif_qualification_path": (
        "histdatacom.modern-reference-motif-qualification.v1",
    ),
    "motif_leakage_audit_path": (
        "histdatacom.modern-reference-motif-leakage-audit.v1",
    ),
}


@lru_cache(maxsize=1)
def reconstruction_schema_registry() -> ReconstructionSchemaRegistryV1:
    """Build the audited deterministic registry from installed contracts."""
    constants: dict[str, set[str]] = {}
    implementations: dict[str, type[Any]] = {}
    for module_name in _AUDITED_MODULES:
        module = importlib.import_module(module_name)
        for constant_name, value in vars(module).items():
            if constant_name.endswith("SCHEMA_VERSION") and isinstance(
                value, str
            ):
                constants.setdefault(value, set()).add(
                    f"{module_name}.{constant_name}"
                )
        for _, candidate in inspect.getmembers(module, inspect.isclass):
            if candidate.__module__ != module_name or not is_dataclass(
                candidate
            ):
                continue
            schema_version = _dataclass_schema_version(candidate)
            if schema_version:
                implementations.setdefault(schema_version, candidate)

    for version, implemented_type in implementations.items():
        constants.setdefault(version, set()).add(
            f"{implemented_type.__module__}.{implemented_type.__name__}.schema_version"
        )
    fingerprint_contracts = importlib.import_module(
        "histdatacom.data_quality.fingerprint_contracts"
    )
    for fingerprint in fingerprint_contracts.FINGERPRINT_SCHEMA_CONTRACTS:
        constants.setdefault(fingerprint.schema_version, set()).add(
            "histdatacom.data_quality.fingerprint_contracts."
            f"FINGERPRINT_SCHEMA_CONTRACTS[{fingerprint.key}]"
        )

    contracts: dict[str, ReconstructionContractDefinitionV1] = {}
    for version in sorted(constants):
        contract_type = implementations.get(version)
        contracts[version] = _contract_from_audit(
            version,
            contract_type,
            tuple(sorted(constants[version])),
        )
    contracts[LEGACY_HISTDATA_CACHE_SCHEMA_VERSION] = _legacy_cache_contract()
    contracts[TRAINING_SCHEMA_VERSION] = _training_contract(
        tuple(sorted(constants.get(TRAINING_SCHEMA_VERSION, ())))
    )
    for version, name, family, note, field_names in _RESERVED_CONTRACTS:
        contracts[version] = _reserved_contract(
            version, name, family, note, field_names
        )
    return ReconstructionSchemaRegistryV1(tuple(contracts.values()))


def evaluate_reconstruction_compatibility(
    plan: Mapping[str, Any] | Any,
    *,
    inspect_source: bool = True,
    inspect_artifacts: bool = True,
) -> ReconstructionCompatibilityReportV1:
    """Compare one proposed plan with installed HistData-only policy."""
    payload = _plan_payload(plan)
    registry = reconstruction_schema_registry()
    findings: list[ReconstructionCompatibilityFindingV1] = []
    translations: list[str] = []
    cache_schemas: tuple[ReconstructionCacheSchemaV1, ...] = ()
    subject_schema = str(payload.get("schema_version", "")).strip()

    if not subject_schema:
        _finding(
            findings,
            "unversioned_plan",
            ReconstructionCompatibilityStatus.INVALID,
            "schema_version",
            "The proposed plan has no schema version.",
            "Supply a registered reconstruction plan schema version.",
        )
    elif subject_schema == PORTFOLIO_PLAN_SCHEMA_VERSION:
        _finding(
            findings,
            "portfolio_plan_not_executable",
            ReconstructionCompatibilityStatus.RESEARCH_ONLY,
            "schema_version",
            "The v2 portfolio plan is reserved for issue #489 and is not executable.",
            "Use the v1 empirical-motif path until #489 is implemented.",
        )
    elif subject_schema != CURRENT_PLAN_SCHEMA_VERSION:
        _finding(
            findings,
            "unsupported_plan_schema",
            ReconstructionCompatibilityStatus.UNSUPPORTED,
            "schema_version",
            f"Plan schema {subject_schema!r} is not an executable installed schema.",
            "Use the current registered plan schema.",
        )
    else:
        translations.append(
            "v2.3 explicit empirical-motif plan adapted to the current "
            "provider-neutral domain boundary"
        )

    if subject_schema == CURRENT_PLAN_SCHEMA_VERSION:
        _check_unknown_plan_fields(payload, findings)
        _check_current_scope(payload, findings)
        if inspect_source:
            cache_schemas = _inspect_source_root(payload, findings)
        if inspect_artifacts:
            _inspect_artifacts(payload, registry, findings)

    status = _overall_status(findings, translations)
    executable = status in {
        ReconstructionCompatibilityStatus.EXACT,
        ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION,
        ReconstructionCompatibilityStatus.DEPRECATED,
    }
    return ReconstructionCompatibilityReportV1(
        subject_schema_version=subject_schema,
        status=status,
        executable=executable,
        registry_id=registry.registry_id,
        findings=tuple(findings),
        translations=tuple(translations),
        cache_schemas=cache_schemas,
    )


def read_compatibility_plan(path: str | Path) -> Mapping[str, Any]:
    """Read a bounded JSON plan mapping without constructing its typed class."""
    selected = Path(path).expanduser()
    if not selected.is_file():
        raise ValueError(f"compatibility plan does not exist: {selected}")
    if selected.stat().st_size > MAX_ARTIFACT_BYTES:
        raise ValueError("compatibility plan exceeds size limit")
    payload = json.loads(selected.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TypeError("compatibility plan must be a JSON object")
    return payload


def _contract_from_audit(
    version: str,
    candidate: type[Any] | None,
    constant_paths: Sequence[str],
) -> ReconstructionContractDefinitionV1:
    module_name = (
        candidate.__module__
        if candidate is not None
        else constant_paths[0].rsplit(".", 1)[0]
    )
    family = _family(module_name, version)
    stages = _consumer_stages(family)
    status = _contract_status(version, candidate)
    contract_fields = (
        _fields_from_dataclass(version, candidate, family, stages)
        if candidate is not None
        else ()
    )
    name = candidate.__name__ if candidate is not None else version
    implementation = (
        f"{candidate.__module__}.{candidate.__name__}"
        if candidate is not None
        else ", ".join(constant_paths)
    )
    note = (
        (inspect.getdoc(candidate) or "Versioned reconstruction contract.")
        if candidate is not None
        else "Version constant is registered and explicitly audited as internal-only."
    )
    return ReconstructionContractDefinitionV1(
        contract_schema_version=version,
        name=name,
        family=family,
        status=status,
        grain=_grain(version),
        publication_safety=(
            "internal_only"
            if status is ReconstructionContractStatus.INTERNAL_ONLY
            else "bounded_metadata"
        ),
        information_modes=INFORMATION_MODES,
        consumer_stages=stages,
        fields=contract_fields,
        implementation=implementation,
        audit_note=note,
    )


def _fields_from_dataclass(
    version: str,
    candidate: type[Any],
    family: str,
    stages: tuple[str, ...],
) -> tuple[ReconstructionFieldDefinitionV1, ...]:
    definitions = []
    for item in fields(candidate):
        nullable = _nullable(item.type, item.default)
        status = (
            ReconstructionContractStatus.OPTIONAL
            if nullable
            or (
                item.default is not MISSING
                and item.name not in {"schema_version"}
            )
            or item.default_factory is not MISSING
            else ReconstructionContractStatus.REQUIRED
        )
        definitions.append(
            ReconstructionFieldDefinitionV1(
                contract_schema_version=version,
                name=item.name,
                dtype=_type_name(item.type),
                nullable=nullable,
                status=status,
                grain=_field_grain(item.name, _grain(version)),
                identity_role=_identity_role(version, item.name),
                lineage=_lineage(item.name),
                basis=_basis(item.name),
                source_derived_status=_basis(item.name),
                availability=_availability(item.name, family),
                publication_safety=_field_publication_safety(item.name),
                information_modes=INFORMATION_MODES,
                consumer_stages=stages,
                description=_field_description(version, item.name),
            )
        )
    if len(definitions) > MAX_FIELDS_PER_CONTRACT:
        raise ValueError(f"contract {version} exceeds field limit")
    return tuple(definitions)


def _legacy_cache_contract() -> ReconstructionContractDefinitionV1:
    fields_ = tuple(
        _cache_field(
            LEGACY_HISTDATA_CACHE_SCHEMA_VERSION,
            name,
            dtype,
            nullable=False,
            status=(
                ReconstructionContractStatus.OPTIONAL
                if name == "vol"
                else ReconstructionContractStatus.REQUIRED
            ),
            identity_role=(
                "timestamp_component_requires_row_ordinal"
                if name == "datetime"
                else "not_identity"
            ),
            description=description,
        )
        for name, dtype, description in (
            (
                "datetime",
                "Int64 milliseconds",
                "Observed cache timestamp; never sufficient as row identity alone.",
            ),
            ("bid", "Float64", "Immutable observed HistData bid."),
            ("ask", "Float64", "Immutable observed HistData ask."),
            ("vol", "Int32", "Optional source volume placeholder."),
        )
    )
    return ReconstructionContractDefinitionV1(
        contract_schema_version=LEGACY_HISTDATA_CACHE_SCHEMA_VERSION,
        name="HistDataAsciiTickCacheLegacy",
        family="source",
        status=ReconstructionContractStatus.DEPRECATED,
        grain="observed_tick_row",
        publication_safety="local_only_event_data",
        information_modes=INFORMATION_MODES,
        consumer_stages=("source_enrichment",),
        fields=fields_,
        implementation="HistData.com Polars Arrow IPC .data cache",
        audit_note=(
            "Accepted only through a compatible translation that binds path, "
            "source row ordinal, provider, symbol, and period identity."
        ),
    )


def _training_contract(
    constant_paths: Sequence[str],
) -> ReconstructionContractDefinitionV1:
    field_defs = list(_legacy_cache_contract().fields)
    stages = ("source_enrichment", "evidence_qualification", "proposal")
    for item in training_feature_definitions():
        deprecated = item.name in SYNTHETIC_PLACEHOLDER_COLUMNS
        description = item.description
        if deprecated:
            description += (
                " Legacy same-row auxiliary placeholder; it is not the "
                "variable-cardinality reconstruction event product."
            )
        field_defs.append(
            ReconstructionFieldDefinitionV1(
                contract_schema_version=TRAINING_SCHEMA_VERSION,
                name=item.name,
                dtype=item.dtype,
                nullable=item.nullable,
                status=(
                    ReconstructionContractStatus.DEPRECATED
                    if deprecated
                    else (
                        ReconstructionContractStatus.OPTIONAL
                        if item.nullable
                        else ReconstructionContractStatus.REQUIRED
                    )
                ),
                grain=item.grain,
                identity_role=(
                    "composite_source_row_identity"
                    if item.name in IDENTITY_COLUMNS
                    else "not_identity"
                ),
                lineage=item.source,
                basis=(
                    "source"
                    if item.source == "source"
                    else "deterministically_derived"
                ),
                source_derived_status=(
                    "source"
                    if item.source == "source"
                    else "deterministically_derived"
                ),
                availability=_availability(item.name, "training"),
                publication_safety="local_only_event_data",
                information_modes=INFORMATION_MODES,
                consumer_stages=stages,
                description=description,
            )
        )
    return ReconstructionContractDefinitionV1(
        contract_schema_version=TRAINING_SCHEMA_VERSION,
        name="AsciiTickTrainingFeaturesV1",
        family="training",
        status=ReconstructionContractStatus.REQUIRED,
        grain="observed_tick_row",
        publication_safety="local_only_event_data",
        information_modes=INFORMATION_MODES,
        consumer_stages=stages,
        fields=tuple(field_defs),
        implementation=(
            ", ".join(constant_paths)
            or "histdatacom.data_quality.training_features"
        ),
        audit_note=(
            "Row-aligned evidence substrate. Observed datetime/bid/ask and "
            "source-row identity are immutable; synth_* fields are deprecated "
            "auxiliary placeholders only."
        ),
    )


def _reserved_contract(
    version: str,
    name: str,
    family: str,
    note: str,
    field_names: Sequence[str],
) -> ReconstructionContractDefinitionV1:
    stages = _consumer_stages(family)
    return ReconstructionContractDefinitionV1(
        contract_schema_version=version,
        name=name,
        family=family,
        status=ReconstructionContractStatus.RESERVED,
        grain=_grain(version),
        publication_safety="bounded_metadata",
        information_modes=INFORMATION_MODES,
        consumer_stages=stages,
        fields=tuple(
            ReconstructionFieldDefinitionV1(
                contract_schema_version=version,
                name=field_name,
                dtype="reserved",
                nullable=False,
                status=ReconstructionContractStatus.RESERVED,
                grain=_field_grain(field_name, _grain(version)),
                identity_role=_identity_role(version, field_name),
                lineage=_lineage(field_name),
                basis=_basis(field_name),
                source_derived_status=_basis(field_name),
                availability=_availability(field_name, family),
                publication_safety="bounded_metadata",
                information_modes=INFORMATION_MODES,
                consumer_stages=stages,
                description=note,
            )
            for field_name in field_names
        ),
        implementation="reserved; no executable implementation",
        audit_note=note,
    )


def _cache_field(
    version: str,
    name: str,
    dtype: str,
    *,
    nullable: bool,
    status: ReconstructionContractStatus,
    identity_role: str,
    description: str,
) -> ReconstructionFieldDefinitionV1:
    return ReconstructionFieldDefinitionV1(
        contract_schema_version=version,
        name=name,
        dtype=dtype,
        nullable=nullable,
        status=status,
        grain="observed_tick_row",
        identity_role=identity_role,
        lineage="histdata.com source cache",
        basis="source",
        source_derived_status="source",
        availability="source_recorded",
        publication_safety="local_only_event_data",
        information_modes=INFORMATION_MODES,
        consumer_stages=("source_enrichment",),
        description=description,
    )


def _check_unknown_plan_fields(
    payload: Mapping[str, Any],
    findings: list[ReconstructionCompatibilityFindingV1],
) -> None:
    unknown = tuple(sorted(set(payload) - _PLAN_FIELDS))
    for name in unknown[:MAX_COMPATIBILITY_FINDINGS]:
        _finding(
            findings,
            "unknown_plan_field",
            ReconstructionCompatibilityStatus.INVALID,
            name,
            f"Plan field {name!r} is not registered for the v1 contract.",
            "Remove the field or use a future registered plan version.",
        )


def _check_current_scope(
    payload: Mapping[str, Any],
    findings: list[ReconstructionCompatibilityFindingV1],
) -> None:
    provider = (
        str(
            payload.get(
                "source_provider_id",
                payload.get("provider", HISTDATA_PROVIDER_ID),
            )
            or HISTDATA_PROVIDER_ID
        )
        .strip()
        .lower()
    )
    if provider != HISTDATA_PROVIDER_ID:
        _finding(
            findings,
            "alternate_provider_later_milestone",
            ReconstructionCompatibilityStatus.UNSUPPORTED,
            "source_provider_id",
            "v2.4 execution is qualified only for HistData.com data.",
            "Use source_provider_id=histdata.com; defer other providers.",
        )
    source_format = str(payload.get("source_format", "")).strip().lower()
    if source_format != SUPPORTED_SOURCE_FORMAT:
        _finding(
            findings,
            "non_ascii_source",
            ReconstructionCompatibilityStatus.INVALID,
            "source_format",
            "Reconstruction requires HistData ASCII tick input.",
            "Select source_format=ascii.",
        )
    timeframe = str(payload.get("timeframe", "")).strip().upper()
    if timeframe != SUPPORTED_TIMEFRAME:
        _finding(
            findings,
            "non_tick_grain",
            ReconstructionCompatibilityStatus.INVALID,
            "timeframe",
            "M1, OHLC, and other aggregate grains cannot anchor reconstruction.",
            "Select raw tick timeframe T.",
        )
    symbols_value = payload.get("symbols", ())
    symbols = (
        tuple(sorted(str(value).strip().lower() for value in symbols_value))
        if isinstance(symbols_value, Sequence)
        and not isinstance(symbols_value, (str, bytes))
        else ()
    )
    if symbols != SUPPORTED_SYMBOLS:
        _finding(
            findings,
            "unsupported_symbol_group",
            ReconstructionCompatibilityStatus.INVALID,
            "symbols",
            "The current product requires the complete EURGBP/EURUSD/GBPUSD triangle.",
            "Supply exactly EURGBP, EURUSD, and GBPUSD.",
        )
    delivery = (
        str(payload.get("delivery_mode", "modern_reference")).strip().lower()
    )
    if delivery != "modern_reference" or payload.get(
        "broker_delivery_artifact"
    ):
        _finding(
            findings,
            "broker_input_later_milestone",
            ReconstructionCompatibilityStatus.RESEARCH_ONLY,
            "delivery_mode",
            "Broker-conditioned and OANDA inputs are outside the HistData v2.4 milestone.",
            "Use modern_reference delivery; defer broker/OANDA work.",
        )


def _inspect_source_root(
    payload: Mapping[str, Any],
    findings: list[ReconstructionCompatibilityFindingV1],
) -> tuple[ReconstructionCacheSchemaV1, ...]:
    root_text = str(payload.get("source_root", "")).strip()
    root = Path(root_text).expanduser().resolve() if root_text else Path("/")
    if (
        not root_text
        or root.name.upper() != "T"
        or root.parent.name.upper() != "ASCII"
    ):
        _finding(
            findings,
            "invalid_histdata_source_root",
            ReconstructionCompatibilityStatus.INVALID,
            "source_root",
            "HistData source_root must identify the existing ASCII/T directory.",
            "Point source_root at the HistData ASCII/T cache root.",
        )
        return ()
    paths = tuple(sorted(root.glob("*/[0-9]*/[0-9]*/.data")))
    if not paths:
        _finding(
            findings,
            "missing_histdata_partitions",
            ReconstructionCompatibilityStatus.INVALID,
            "source_root",
            "The HistData ASCII/T root contains no cache partitions.",
            "Download or select existing HistData tick cache partitions.",
        )
        return ()
    if len(paths) > MAX_SOURCE_PARTITIONS:
        _finding(
            findings,
            "source_partition_limit",
            ReconstructionCompatibilityStatus.INVALID,
            "source_root",
            "The source inventory exceeds the public compatibility limit.",
            "Use a bounded dataset selection.",
        )
        return ()
    available_symbols = {
        path.resolve().relative_to(root).parts[0].strip().lower()
        for path in paths
    }
    if not set(SUPPORTED_SYMBOLS).issubset(available_symbols):
        _finding(
            findings,
            "incomplete_histdata_triangle",
            ReconstructionCompatibilityStatus.INVALID,
            "source_root",
            "The source inventory lacks one or more required triangle symbols.",
            "Provide HistData caches for EURGBP, EURUSD, and GBPUSD.",
        )
    summaries: Counter[tuple[str, str, tuple[str, ...], str]] = Counter()
    for path in paths:
        try:
            version, status, columns, identity = _inspect_cache(path)
        except (OSError, TypeError, ValueError) as err:
            _finding(
                findings,
                "invalid_tick_cache_schema",
                ReconstructionCompatibilityStatus.INVALID,
                "source_root",
                str(err),
                "Regenerate the partition from HistData ASCII/T source data.",
            )
            continue
        summaries[(version, status.value, columns, identity)] += 1
    result = tuple(
        ReconstructionCacheSchemaV1(
            cache_schema_version=version,
            status=ReconstructionCompatibilityStatus(status),
            columns=columns,
            partition_count=count,
            identity_policy=identity,
        )
        for (version, status, columns, identity), count in sorted(
            summaries.items()
        )
    )
    if any(
        item.status is ReconstructionCompatibilityStatus.STALE
        for item in result
    ):
        _finding(
            findings,
            "stale_training_cache",
            ReconstructionCompatibilityStatus.STALE,
            "source_root",
            "At least one enriched cache uses a stale training schema version.",
            "Rebuild enriched caches with the installed training schema.",
        )
    if any(
        item.status is ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION
        for item in result
    ):
        _finding(
            findings,
            "legacy_cache_translation",
            ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION,
            "source_root",
            "Legacy raw caches require deterministic source-row identity translation.",
            "No mutation is required; the planner binds provider/path/row ordinal.",
        )
    return result


def _inspect_cache(
    path: Path,
) -> tuple[
    str,
    ReconstructionCompatibilityStatus,
    tuple[str, ...],
    str,
]:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        from pyarrow import ipc  # pylint: disable=import-outside-toplevel
    except (
        ImportError
    ) as err:  # pragma: no cover - declared planning dependency
        raise RuntimeError(
            "cache compatibility inspection requires pyarrow"
        ) from err
    try:
        with pa.memory_map(str(path), "r") as source:
            reader = ipc.open_file(source)
            names = tuple(reader.schema.names)
            columns = frozenset(names)
            required = {"datetime", "bid", "ask"}
            if not required.issubset(columns):
                raise ValueError("Tick cache lacks datetime/bid/ask fields.")
            if columns.intersection({"open", "high", "low", "close"}):
                raise ValueError(
                    "Tick cache contains forbidden OHLC aggregate fields."
                )
            if not reader.num_record_batches:
                raise ValueError("Tick cache contains no record batches.")
            if "training_schema_version" not in columns:
                unknown = columns - {"datetime", "bid", "ask", "vol"}
                if unknown:
                    raise ValueError(
                        "Unversioned tick cache contains unknown fields: "
                        + ", ".join(sorted(unknown))
                    )
                return (
                    LEGACY_HISTDATA_CACHE_SCHEMA_VERSION,
                    ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION,
                    tuple(sorted(columns)),
                    "provider+symbol+period+source_row_ordinal",
                )
            allowed = {"datetime", "bid", "ask", "vol"} | {
                item.name for item in training_feature_definitions()
            }
            unknown = columns - allowed
            if unknown:
                raise ValueError(
                    "Enriched tick cache contains unknown fields: "
                    + ", ".join(sorted(unknown))
                )
            required_training = {
                item.name for item in training_feature_definitions()
            }
            if not required_training.issubset(columns):
                missing = ", ".join(sorted(required_training - columns)[:16])
                raise ValueError(
                    "Enriched tick cache is incomplete; missing fields: "
                    + missing
                )
            required_identity = {
                "series_id",
                "period",
                "row_id",
                "source_row_number",
                "event_seq",
                "symbol",
                "format",
                "timeframe",
                "source",
            }
            if not required_identity.issubset(columns):
                missing = ", ".join(sorted(required_identity - columns))
                raise ValueError(
                    "Enriched tick cache lacks composite identity fields: "
                    + missing
                )
            first = reader.get_batch(0)
            last = reader.get_batch(reader.num_record_batches - 1)
            index = reader.schema.get_field_index("training_schema_version")
            versions = {
                str(first.column(index)[0].as_py()),
                str(last.column(index)[-1].as_py()),
            }
            if versions != {TRAINING_SCHEMA_VERSION}:
                return (
                    ",".join(sorted(versions)),
                    ReconstructionCompatibilityStatus.STALE,
                    tuple(sorted(columns)),
                    "series_id+period+row_id+event_seq",
                )
            _require_edge_value(reader, "source", HISTDATA_PROVIDER_ID)
            _require_edge_value(reader, "format", SUPPORTED_SOURCE_FORMAT)
            _require_edge_value(reader, "timeframe", SUPPORTED_TIMEFRAME)
            return (
                TRAINING_SCHEMA_VERSION,
                ReconstructionCompatibilityStatus.EXACT,
                tuple(sorted(columns)),
                "series_id+period+row_id+event_seq",
            )
    except ValueError:
        raise
    except Exception as err:
        raise ValueError("Tick cache is not readable Arrow IPC.") from err


def _inspect_artifacts(
    payload: Mapping[str, Any],
    registry: ReconstructionSchemaRegistryV1,
    findings: list[ReconstructionCompatibilityFindingV1],
) -> None:
    registered = {item.contract_schema_version for item in registry.contracts}
    for field_name, expected in _ARTIFACT_SCHEMA_EXPECTATIONS.items():
        path_text = str(payload.get(field_name, "")).strip()
        if not path_text:
            _finding(
                findings,
                "missing_artifact_reference",
                ReconstructionCompatibilityStatus.INVALID,
                field_name,
                "A required evidence artifact reference is absent.",
                "Supply the required versioned artifact path.",
            )
            continue
        path = Path(path_text).expanduser()
        if not path.is_file():
            _finding(
                findings,
                "missing_artifact",
                ReconstructionCompatibilityStatus.INVALID,
                field_name,
                "The referenced evidence artifact does not exist.",
                "Build or select the required artifact before planning.",
            )
            continue
        try:
            data = _read_artifact_mapping(path)
        except (OSError, TypeError, ValueError) as err:
            _finding(
                findings,
                "invalid_artifact",
                ReconstructionCompatibilityStatus.INVALID,
                field_name,
                str(err),
                "Supply a bounded versioned JSON evidence artifact.",
            )
            continue
        version = str(data.get("schema_version", "")).strip()
        if not version:
            _finding(
                findings,
                "unversioned_artifact",
                ReconstructionCompatibilityStatus.INVALID,
                field_name,
                "The referenced evidence artifact is unversioned.",
                "Regenerate it with an installed versioned writer.",
            )
        elif version not in expected:
            status = (
                ReconstructionCompatibilityStatus.STALE
                if version in registered
                else ReconstructionCompatibilityStatus.UNSUPPORTED
            )
            _finding(
                findings,
                "artifact_schema_mismatch",
                status,
                field_name,
                f"Artifact schema {version!r} does not match {expected!r}.",
                "Regenerate or select the required current artifact version.",
            )


def _read_artifact_mapping(path: Path) -> Mapping[str, Any]:
    if path.stat().st_size > MAX_ARTIFACT_BYTES:
        raise ValueError("Evidence artifact exceeds compatibility size limit.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TypeError("Evidence artifact must be a JSON object.")
    return payload


def _finding(
    findings: list[ReconstructionCompatibilityFindingV1],
    code: str,
    status: ReconstructionCompatibilityStatus,
    location: str,
    message: str,
    remediation: str,
) -> None:
    if len(findings) >= MAX_COMPATIBILITY_FINDINGS:
        return
    findings.append(
        ReconstructionCompatibilityFindingV1(
            code=code,
            status=status,
            location=location,
            message=message,
            remediation=remediation,
        )
    )


def _overall_status(
    findings: Sequence[ReconstructionCompatibilityFindingV1],
    translations: Sequence[str],
) -> ReconstructionCompatibilityStatus:
    order = {
        ReconstructionCompatibilityStatus.EXACT: 0,
        ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION: 1,
        ReconstructionCompatibilityStatus.DEPRECATED: 2,
        ReconstructionCompatibilityStatus.RESEARCH_ONLY: 3,
        ReconstructionCompatibilityStatus.STALE: 4,
        ReconstructionCompatibilityStatus.UNSUPPORTED: 5,
        ReconstructionCompatibilityStatus.INVALID: 6,
    }
    statuses = [item.status for item in findings]
    if translations:
        statuses.append(
            ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION
        )
    return (
        max(statuses, key=order.__getitem__)
        if statuses
        else (ReconstructionCompatibilityStatus.EXACT)
    )


def _plan_payload(plan: Mapping[str, Any] | Any) -> Mapping[str, Any]:
    if isinstance(plan, Mapping):
        return plan
    serializer = getattr(plan, "to_dict", None)
    if not callable(serializer):
        raise TypeError(
            "compatibility input must be a plan mapping or contract"
        )
    payload = serializer()
    if not isinstance(payload, Mapping):
        raise TypeError("plan serializer must return a mapping")
    return payload


def _dataclass_schema_version(candidate: type[Any]) -> str | None:
    for item in fields(candidate):
        if item.name == "schema_version" and isinstance(item.default, str):
            return item.default
    return None


def _contract_status(
    version: str, candidate: type[Any] | None
) -> ReconstructionContractStatus:
    if candidate is None:
        return ReconstructionContractStatus.INTERNAL_ONLY
    if any(token in version for token in _REQUIRED_SCHEMA_TOKENS):
        return ReconstructionContractStatus.REQUIRED
    return ReconstructionContractStatus.OPTIONAL


def _family(module_name: str, version: str) -> str:
    if "dataset" in module_name or "source-" in version:
        return "dataset"
    if "training" in module_name:
        return "training"
    if "feed_epoch" in module_name:
        return "feed_epoch"
    if "market_context" in module_name:
        return "context"
    if "benchmark" in module_name:
        return "benchmark"
    if "motif" in module_name or "generation" in module_name:
        return "proposal"
    if "carving" in module_name:
        return "carving"
    if "cross_currency" in module_name:
        return "cross_series"
    if "ensemble" in module_name:
        return "ensemble"
    if "persistence" in module_name:
        return "product"
    if "certification" in module_name or "strategy_sensitivity" in module_name:
        return "certification"
    if (
        "reconstruction_plan" in module_name
        or module_name == "histdatacom.reconstruction"
    ):
        return "plan"
    if "streaming" in module_name or "orchestration" in module_name:
        return "runtime"
    if "observation" in module_name or "delivery" in module_name:
        return "observation"
    return "proposal"


def _consumer_stages(family: str) -> tuple[str, ...]:
    return {
        "dataset": ("catalog_selection", "source_enrichment"),
        "source": ("source_enrichment",),
        "training": ("source_enrichment", "evidence_qualification", "proposal"),
        "feed_epoch": ("evidence_qualification", "proposal", "validation"),
        "context": ("evidence_qualification", "proposal", "validation"),
        "evidence": ("evidence_qualification", "proposal", "validation"),
        "benchmark": ("evidence_qualification", "certification"),
        "proposal": ("proposal",),
        "carving": ("carving",),
        "cross_series": ("cross_series_reconciliation", "validation"),
        "ensemble": ("proposal", "validation"),
        "observation": ("source_enrichment", "delivery_projection"),
        "plan": ("planning", "preflight"),
        "experiment": ("planning", "certification"),
        "runtime": ("execution", "checkpoint", "recovery"),
        "product": ("persistence", "preview", "replay"),
        "certification": ("validation", "certification"),
    }.get(family, ("internal",))


def _grain(version: str) -> str:
    text = version.lower()
    for token, grain in (
        ("event-batch", "event_batch"),
        ("event", "event"),
        ("partition", "partition"),
        ("window", "window"),
        ("interval", "interval"),
        ("snapshot", "release_snapshot"),
        ("corpus", "corpus"),
        ("catalog", "catalog"),
        ("manifest", "manifest"),
        ("checkpoint", "checkpoint"),
        ("plan", "plan"),
        ("report", "report"),
        ("config", "configuration"),
        ("policy", "policy"),
    ):
        if token in text:
            return grain
    return "contract"


def _field_grain(name: str, default: str) -> str:
    if name in {"event_time_ns", "event_sequence", "bid", "ask"}:
        return "event"
    if "window" in name:
        return "window"
    if "partition" in name:
        return "partition"
    if "artifact" in name:
        return "artifact_reference"
    return default


def _identity_role(version: str, name: str) -> str:
    if name == "schema_version":
        return "schema_identity"
    if name.endswith(("sha256", "digest")):
        return "content_identity"
    if name.endswith("_id") or name in {
        "event_time_ns",
        "event_sequence",
        "period",
        "symbol",
    }:
        return "composite_identity_component"
    if "synthetic-event" in version and name in {"bid", "ask"}:
        return "immutable_evidence_or_generated_value"
    return "not_identity"


def _lineage(name: str) -> str:
    if name.startswith("source") or "parent" in name:
        return "source_lineage"
    if "artifact" in name or name.endswith("_ref"):
        return "strong_artifact_lineage"
    if "generator" in name or "motif" in name:
        return "generation_lineage"
    return "contract_local"


def _basis(name: str) -> str:
    if name.startswith("source") or name in {"bid", "ask", "event_time_ns"}:
        return "source_or_origin_dependent"
    if name.endswith(("_id", "sha256")):
        return "deterministically_derived"
    return "declared_or_derived"


def _availability(name: str, family: str) -> str:
    if "available_at" in name or "as_of" in name or "release" in name:
        return "point_in_time_explicit"
    if name.endswith(("_start_ns", "_end_ns")):
        return "half_open_interval"
    if family in {"context", "evidence", "feed_epoch"}:
        return "artifact_as_of_required"
    if name in {"bid", "ask", "datetime", "event_time_ns"}:
        return "source_recorded_or_generated_by_origin"
    return "available_with_parent_contract"


def _field_publication_safety(name: str) -> str:
    lowered = name.lower()
    if "path" in lowered:
        return "local_reference_only"
    if name in {"bid", "ask", "datetime", "event_time_ns"}:
        return "local_only_event_data"
    return "bounded_metadata"


def _field_description(version: str, name: str) -> str:
    if "synthetic-event" in version and name in {"bid", "ask", "event_time_ns"}:
        return (
            "Immutable for observed origin; generated only for explicitly "
            "synthetic origin with separate lineage."
        )
    if name == "schema_version":
        return "Required immutable wire-contract version."
    if name.endswith("_id"):
        return "Stable identity field defined by the owning contract."
    return "Field declared by the installed owning dataclass contract."


def _nullable(annotation: Any, default: Any) -> bool:
    return default is None or "None" in _type_name(annotation)


def _type_name(annotation: Any) -> str:
    text = annotation if isinstance(annotation, str) else str(annotation)
    return (
        str(text)
        .replace("typing.", "")
        .replace("<class '", "")
        .replace("'>", "")
    )


def _require_edge_value(reader: Any, name: str, expected: str) -> None:
    """Require first and last enriched-cache values to retain current scope."""
    index = reader.schema.get_field_index(name)
    first = reader.get_batch(0).column(index)[0].as_py()
    last_batch = reader.get_batch(reader.num_record_batches - 1)
    last = last_batch.column(index)[-1].as_py()
    actual = {str(first).strip(), str(last).strip()}
    if actual != {expected}:
        raise ValueError(
            f"Enriched tick cache {name} values differ from {expected!r}."
        )


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


__all__ = [
    "CURRENT_PLAN_SCHEMA_VERSION",
    "HISTDATA_PROVIDER_ID",
    "LEGACY_HISTDATA_CACHE_SCHEMA_VERSION",
    "PORTFOLIO_PLAN_SCHEMA_VERSION",
    "ReconstructionCacheSchemaV1",
    "ReconstructionCompatibilityFindingV1",
    "ReconstructionCompatibilityReportV1",
    "ReconstructionCompatibilityStatus",
    "ReconstructionContractDefinitionV1",
    "ReconstructionContractStatus",
    "ReconstructionFieldDefinitionV1",
    "ReconstructionSchemaRegistryV1",
    "evaluate_reconstruction_compatibility",
    "read_compatibility_plan",
    "reconstruction_schema_registry",
]
