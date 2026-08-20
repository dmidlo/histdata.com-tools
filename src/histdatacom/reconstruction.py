"""Typed public facade for first-party reconstruction operations.

The facade keeps operator intent, scientific plan identity, orchestration
control, and product inspection at one supported import boundary.  Tick rows
remain in Arrow/Parquet artifacts; public requests and receipts carry only
bounded metadata and strong references.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import IntEnum
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

from histdatacom.cross_series_constraints import CrossSeriesConstraintPolicyV1
from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    DatasetParentV1,
    DatasetQualificationStatus,
    DatasetVersionManifestV1,
)
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.orchestration.client import (
    OrchestrationJobHandle,
    cancel_job,
    get_job_result,
    inspect_job_status,
    submit_reconstruction_request,
)
from histdatacom.orchestration.queues import OrchestrationWorkerConfig
from histdatacom.orchestration.reconstruction import (
    ReconstructionRunReportV1,
    ReconstructionWorkflowRequestV1,
    artifact_ref_for_file,
    reconcile_reconstruction_report,
    run_reconstruction_request,
    verify_artifact_ref,
    write_reconstruction_report,
)
from histdatacom.orchestration.supervisor import OrchestrationSupervisor
from histdatacom.reconstruction_evidence import (
    CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    ReconstructionEvidencePolicyV1,
)
from histdatacom.reconstruction_experiment import (
    ReconstructionExperimentManifestV1,
    ReconstructionExperimentVerificationV1,
    discover_reconstruction_experiments,
    read_reconstruction_experiment,
    verify_reconstruction_experiment,
)
from histdatacom.reconstruction_schema import (
    ReconstructionCompatibilityReportV1,
    ReconstructionCompatibilityStatus,
    ReconstructionSchemaRegistryV1,
    evaluate_reconstruction_compatibility,
    read_compatibility_plan,
    reconstruction_schema_registry,
)
from histdatacom.reconstruction_science import (
    RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND,
    ReconstructionScientificLedgerV1,
    current_histdata_reconstruction_scientific_ledger,
    read_reconstruction_scientific_ledger,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.certification import (
    ReconstructionCertificationDossierV2,
)
from histdatacom.synthetic.certification_campaign import (
    ModernReferenceCertificationCampaignResultV1,
    ModernReferenceCertificationCampaignSpecV1,
    read_modern_reference_certification_campaign_spec,
    run_modern_reference_certification_campaign,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.diagnostics import (
    DiagnosticPublicationManifestV1,
    DiagnosticPublicationSpecV1,
    diagnostic_publication_listing,
    publish_reconstruction_diagnostics,
)
from histdatacom.synthetic.hawkes_selection import (
    HAWKES_SELECTION_ENGINE_IDS,
    HawkesProductSelectionDossierV1,
    build_hawkes_product_selection_dossier,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_MANIFEST_ARTIFACT_KIND,
    ReconstructionProductManifestV3,
    discover_reconstruction_manifests,
    iter_reconstruction_event_batches,
    load_reconstruction_manifest,
    read_reconstruction_streams,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.proposal_engines import (
    ProposalEnginePortfolioV1,
    ProposalEngineRegistryV1,
    ProposalPortfolioEvaluationV1,
    proposal_engine_registry,
    run_histdata_proposal_portfolio_evaluation,
)
from histdatacom.synthetic.qualification import (
    PoweredQualificationDossierV1,
    powered_qualification_verification_scope,
    qualify_histdata_proposal_portfolio,
)
from histdatacom.synthetic.reconstruction_handlers import (
    register_first_party_reconstruction_handlers,
)
from histdatacom.synthetic.reconstruction_plan import (
    DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS,
    SCIENTIFIC_NONCLAIM,
    ReconstructionDeliveryMode,
    ReconstructionPlanConfigurationV2,
    ReconstructionPlanResourceSummaryV1,
    ReconstructionPlanSourceSupportStatus,
    SyntheticInfillPlanV1,
    build_synthetic_infill_plan,
    read_reconstruction_plan_configuration,
    read_reconstruction_plan_execution_manifest,
    read_reconstruction_source_inventory,
    read_synthetic_infill_plan,
    validate_synthetic_infill_plan_for_execution,
    write_synthetic_infill_plan,
)

RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-spec.v1"
)
RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-spec.v2"
)
RECONSTRUCTION_PLAN_SET_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-set.v1"
)
RECONSTRUCTION_PLAN_SET_PREFLIGHT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-set-preflight.v1"
)
RECONSTRUCTION_PLAN_SUPPORT_WINDOW_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-support-window.v1"
)
RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-support-map.v1"
)
RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-support-map-index.v2"
)
RECONSTRUCTION_PLAN_SHARD_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-shard.v1"
)
RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-execution-request.v1"
)
RECONSTRUCTION_PLAN_SET_EXECUTION_REQUEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-set-execution-request.v1"
)
RECONSTRUCTION_PREFLIGHT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-preflight.v1"
)
RECONSTRUCTION_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-operation-receipt.v1"
)
RECONSTRUCTION_PLAN_SET_RECEIPT_INDEX_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-set-receipt-index.v1"
)
RECONSTRUCTION_CAMPAIGN_PRODUCT_ENTRY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-product-entry.v1"
)
RECONSTRUCTION_CAMPAIGN_PRODUCT_SHARD_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-product-shard.v1"
)
RECONSTRUCTION_CAMPAIGN_PRODUCT_INDEX_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-product-index.v1"
)
RECONSTRUCTION_CAMPAIGN_DATASET_PUBLICATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-dataset-publication.v1"
)
RECONSTRUCTION_OUTPUT_LIST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-output-list.v1"
)
RECONSTRUCTION_PREVIEW_SCHEMA_VERSION = "histdatacom.reconstruction-preview.v1"
RECONSTRUCTION_REPLAY_SCHEMA_VERSION = "histdatacom.reconstruction-replay.v1"

RECONSTRUCTION_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
RECONSTRUCTION_SOURCE_FORMAT = "ascii"
RECONSTRUCTION_TIMEFRAME = "T"
DEFAULT_PREVIEW_LIMIT = 20
MAX_PREVIEW_LIMIT = 100
DEFAULT_PLAN_SET_PERIODS_PER_SHARD = 12
MAX_PLAN_SET_PERIODS_PER_SHARD = 24
MAX_RECONSTRUCTION_PLAN_SHARDS = 4096
MAX_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS = 100_000
MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES = 64 * 1024 * 1024
MAX_MONOLITHIC_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS = 10_000
MAX_RECONSTRUCTION_SUPPORT_INSPECTION_WINDOWS = 1_000
MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES = 64 * 1024 * 1024
MAX_RECONSTRUCTION_CAMPAIGN_ENTRIES_PER_SHARD = 5_000


class ReconstructionExitCode(IntEnum):
    """Stable CLI outcome categories for public reconstruction commands."""

    SUCCESS = 0
    INVALID_PLAN = 2
    REFUSED = 3
    RUNTIME_FAILURE = 4
    VALIDATION_FAILURE = 5


class ReconstructionPublicError(RuntimeError):
    """Base error carrying a stable machine-readable public reason code."""

    reason_code = "reconstruction_error"
    exit_code = ReconstructionExitCode.RUNTIME_FAILURE


class ReconstructionUnsupportedError(ReconstructionPublicError):
    """The requested public source, timeframe, symbol set, or mode is invalid."""

    reason_code = "unsupported_reconstruction_request"
    exit_code = ReconstructionExitCode.INVALID_PLAN


class ReconstructionPlanError(ReconstructionPublicError):
    """The bound plan is missing, changed, malformed, or not executable."""

    reason_code = "invalid_reconstruction_plan"
    exit_code = ReconstructionExitCode.INVALID_PLAN


class ReconstructionRefusedError(ReconstructionPublicError):
    """Declared scientific or resource policy refuses execution."""

    reason_code = "reconstruction_refused"
    exit_code = ReconstructionExitCode.REFUSED


class ReconstructionValidationError(ReconstructionPublicError):
    """Executed output did not reach a fully committed validated state."""

    reason_code = "reconstruction_validation_failed"
    exit_code = ReconstructionExitCode.VALIDATION_FAILURE


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSpecV1:
    """Serializable public inputs for constructing one first-party plan."""

    source_root: str | None
    feed_epoch_definition_path: str
    observation_operator_path: str
    market_context_corpus_path: str
    cftc_positioning_corpus_path: str
    benchmark_manifest_path: str
    motif_manifest_path: str
    motif_index_path: str
    motif_qualification_path: str
    motif_leakage_audit_path: str
    artifact_root: str
    output_root: str
    checkpoint_root: str
    scratch_root: str
    information_mode: InformationMode
    dataset_catalog_path: str | None = None
    dataset_reference: str = "reconstruction-selected"
    start_period: str | None = None
    end_period: str | None = None
    requested_start_ns: int | None = None
    requested_end_ns: int | None = None
    window_size_ns: int = DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS
    delivery_mode: ReconstructionDeliveryMode = (
        ReconstructionDeliveryMode.MODERN_REFERENCE
    )
    evidence_policy: ReconstructionEvidencePolicyV1 = field(
        default_factory=ReconstructionEvidencePolicyV1
    )
    cross_series_constraint_policy: CrossSeriesConstraintPolicyV1 = field(
        default_factory=CrossSeriesConstraintPolicyV1
    )
    broker_delivery_artifact: ArtifactRef | None = None
    source_format: str = RECONSTRUCTION_SOURCE_FORMAT
    timeframe: str = RECONSTRUCTION_TIMEFRAME
    symbols: tuple[str, ...] = RECONSTRUCTION_SYMBOLS
    proposal_engine_ids: tuple[str, ...] = ()
    selected_proposal_engine_ids: tuple[str, ...] = ()
    proposal_evaluation_paths: tuple[str, ...] = ()
    qualification_dossier_path: str | None = None
    hawkes_product_selection_dossier_path: str | None = None
    schema_version: str = RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version not in {
            RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION,
            RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION,
        }:
            raise ReconstructionUnsupportedError(
                "unsupported reconstruction plan-spec schema"
            )
        for name in (
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
        ):
            value = str(Path(_required_text(getattr(self, name))).expanduser())
            object.__setattr__(self, name, value)
        source_root = _optional_text(self.source_root)
        catalog_path = _optional_text(self.dataset_catalog_path)
        if source_root is None and catalog_path is None:
            raise ReconstructionUnsupportedError(
                "supply dataset_catalog_path or the documented v2.3 source_root "
                "translation"
            )
        object.__setattr__(
            self,
            "source_root",
            (
                str(Path(source_root).expanduser())
                if source_root is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "dataset_catalog_path",
            (
                str(Path(catalog_path).expanduser())
                if catalog_path is not None
                else None
            ),
        )
        object.__setattr__(
            self, "dataset_reference", _required_text(self.dataset_reference)
        )
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode.from_value(self.delivery_mode),
        )
        if not isinstance(self.evidence_policy, ReconstructionEvidencePolicyV1):
            raise ReconstructionUnsupportedError(
                "evidence_policy must use the installed v1 contract"
            )
        if self.evidence_policy.supported_provider_ids != (
            CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
        ):
            raise ReconstructionUnsupportedError(
                "the current evidence policy supports only HistData.com"
            )
        if not isinstance(
            self.cross_series_constraint_policy,
            CrossSeriesConstraintPolicyV1,
        ):
            raise ReconstructionUnsupportedError(
                "cross_series_constraint_policy must use the installed v1 contract"
            )
        if self.cross_series_constraint_policy.supported_provider_ids != (
            CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
        ):
            raise ReconstructionUnsupportedError(
                "the current cross-series policy supports only HistData.com"
            )
        if self.cross_series_constraint_policy.required_symbols != (
            RECONSTRUCTION_SYMBOLS
        ):
            raise ReconstructionUnsupportedError(
                "the cross-series policy must cover the complete HistData triangle"
            )
        _validate_public_input_contract(
            source_format=self.source_format,
            timeframe=self.timeframe,
            symbols=self.symbols,
        )
        object.__setattr__(self, "source_format", RECONSTRUCTION_SOURCE_FORMAT)
        object.__setattr__(self, "timeframe", RECONSTRUCTION_TIMEFRAME)
        object.__setattr__(self, "symbols", RECONSTRUCTION_SYMBOLS)
        proposal_engine_ids = tuple(
            _required_text(value) for value in self.proposal_engine_ids
        )
        selected_engine_ids = tuple(
            _required_text(value) for value in self.selected_proposal_engine_ids
        )
        if len(set(proposal_engine_ids)) != len(proposal_engine_ids):
            raise ReconstructionUnsupportedError(
                "proposal_engine_ids cannot contain duplicates"
            )
        if len(set(selected_engine_ids)) != len(selected_engine_ids):
            raise ReconstructionUnsupportedError(
                "selected_proposal_engine_ids cannot contain duplicates"
            )
        evaluation_paths = tuple(
            str(Path(_required_text(value)).expanduser())
            for value in self.proposal_evaluation_paths
        )
        qualification_path = _optional_text(self.qualification_dossier_path)
        if qualification_path is not None:
            qualification_path = str(Path(qualification_path).expanduser())
        hawkes_selection_path = _optional_text(
            self.hawkes_product_selection_dossier_path
        )
        if hawkes_selection_path is not None:
            hawkes_selection_path = str(
                Path(hawkes_selection_path).expanduser()
            )
        if self.schema_version == RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION and (
            proposal_engine_ids
            or selected_engine_ids
            or evaluation_paths
            or qualification_path is not None
            or hawkes_selection_path is not None
        ):
            raise ReconstructionUnsupportedError(
                "v1 plan translation cannot declare proposal portfolio fields"
            )
        object.__setattr__(self, "proposal_engine_ids", proposal_engine_ids)
        object.__setattr__(
            self, "selected_proposal_engine_ids", selected_engine_ids
        )
        object.__setattr__(self, "proposal_evaluation_paths", evaluation_paths)
        object.__setattr__(
            self, "qualification_dossier_path", qualification_path
        )
        object.__setattr__(
            self,
            "hawkes_product_selection_dossier_path",
            hawkes_selection_path,
        )
        requested_start = self.requested_start_ns
        requested_end = self.requested_end_ns
        exact_bounds = (requested_start, requested_end)
        if (requested_start is None) != (requested_end is None):
            raise ReconstructionUnsupportedError(
                "requested_start_ns and requested_end_ns must be supplied together"
            )
        if requested_start is not None and requested_end is not None:
            if any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in exact_bounds
            ):
                raise ReconstructionUnsupportedError(
                    "requested nanosecond bounds must be integers"
                )
            if requested_end <= requested_start:
                raise ReconstructionUnsupportedError(
                    "requested nanosecond interval must be nonempty"
                )
        if (
            isinstance(self.window_size_ns, bool)
            or not isinstance(self.window_size_ns, int)
            or self.window_size_ns <= 0
        ):
            raise ReconstructionUnsupportedError(
                "window_size_ns must be a positive integer"
            )
        if (
            self.delivery_mode is ReconstructionDeliveryMode.BROKER_CONDITIONED
            and self.broker_delivery_artifact is None
        ):
            raise ReconstructionUnsupportedError(
                "broker-conditioned delivery requires broker_delivery_artifact"
            )
        if (
            self.delivery_mode is ReconstructionDeliveryMode.MODERN_REFERENCE
            and self.broker_delivery_artifact is not None
        ):
            raise ReconstructionUnsupportedError(
                "modern-reference delivery rejects broker_delivery_artifact"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return machine-readable planning metadata without row payloads."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "source_root": self.source_root,
            "dataset_catalog_path": self.dataset_catalog_path,
            "dataset_reference": self.dataset_reference,
            "feed_epoch_definition_path": self.feed_epoch_definition_path,
            "observation_operator_path": self.observation_operator_path,
            "market_context_corpus_path": self.market_context_corpus_path,
            "cftc_positioning_corpus_path": self.cftc_positioning_corpus_path,
            "benchmark_manifest_path": self.benchmark_manifest_path,
            "motif_manifest_path": self.motif_manifest_path,
            "motif_index_path": self.motif_index_path,
            "motif_qualification_path": self.motif_qualification_path,
            "motif_leakage_audit_path": self.motif_leakage_audit_path,
            "artifact_root": self.artifact_root,
            "output_root": self.output_root,
            "checkpoint_root": self.checkpoint_root,
            "scratch_root": self.scratch_root,
            "information_mode": self.information_mode.value,
            "start_period": self.start_period,
            "end_period": self.end_period,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "window_size_ns": self.window_size_ns,
            "delivery_mode": self.delivery_mode.value,
            "evidence_policy": self.evidence_policy.to_dict(),
            "cross_series_constraint_policy": (
                self.cross_series_constraint_policy.to_dict()
            ),
            "broker_delivery_artifact": (
                self.broker_delivery_artifact.to_dict()
                if self.broker_delivery_artifact is not None
                else None
            ),
            "source_format": self.source_format,
            "timeframe": self.timeframe,
            "symbols": list(self.symbols),
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }
        if self.schema_version == RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION:
            payload["proposal_engine_ids"] = list(self.proposal_engine_ids)
            payload["selected_proposal_engine_ids"] = list(
                self.selected_proposal_engine_ids
            )
            payload["proposal_evaluation_paths"] = list(
                self.proposal_evaluation_paths
            )
            payload["qualification_dossier_path"] = (
                self.qualification_dossier_path
            )
            payload["hawkes_product_selection_dossier_path"] = (
                self.hawkes_product_selection_dossier_path
            )
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionPlanSpecV1:
        """Restore a strict public plan specification."""
        broker_payload = data.get("broker_delivery_artifact")
        broker_ref = (
            ArtifactRef.from_dict(_mapping(broker_payload))
            if broker_payload is not None
            else None
        )
        evidence_payload = data.get("evidence_policy")
        evidence_policy = (
            ReconstructionEvidencePolicyV1()
            if evidence_payload is None
            else ReconstructionEvidencePolicyV1.from_dict(
                _mapping(evidence_payload)
            )
        )
        cross_series_payload = data.get("cross_series_constraint_policy")
        cross_series_constraint_policy = (
            CrossSeriesConstraintPolicyV1()
            if cross_series_payload is None
            else CrossSeriesConstraintPolicyV1.from_dict(
                _mapping(cross_series_payload)
            )
        )
        return cls(
            source_root=_optional_text(data.get("source_root")),
            feed_epoch_definition_path=str(
                data.get("feed_epoch_definition_path", "")
            ),
            observation_operator_path=str(
                data.get("observation_operator_path", "")
            ),
            market_context_corpus_path=str(
                data.get("market_context_corpus_path", "")
            ),
            cftc_positioning_corpus_path=str(
                data.get("cftc_positioning_corpus_path", "")
            ),
            benchmark_manifest_path=str(
                data.get("benchmark_manifest_path", "")
            ),
            motif_manifest_path=str(data.get("motif_manifest_path", "")),
            motif_index_path=str(data.get("motif_index_path", "")),
            motif_qualification_path=str(
                data.get("motif_qualification_path", "")
            ),
            motif_leakage_audit_path=str(
                data.get("motif_leakage_audit_path", "")
            ),
            artifact_root=str(data.get("artifact_root", "")),
            output_root=str(data.get("output_root", "")),
            checkpoint_root=str(data.get("checkpoint_root", "")),
            scratch_root=str(data.get("scratch_root", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            dataset_catalog_path=_optional_text(
                data.get("dataset_catalog_path")
            ),
            dataset_reference=str(
                data.get("dataset_reference", "reconstruction-selected")
            ),
            start_period=_optional_text(data.get("start_period")),
            end_period=_optional_text(data.get("end_period")),
            requested_start_ns=(
                cast(int, data["requested_start_ns"])
                if data.get("requested_start_ns") is not None
                else None
            ),
            requested_end_ns=(
                cast(int, data["requested_end_ns"])
                if data.get("requested_end_ns") is not None
                else None
            ),
            window_size_ns=int(
                data.get(
                    "window_size_ns",
                    DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS,
                )
            ),
            delivery_mode=ReconstructionDeliveryMode.from_value(
                str(data.get("delivery_mode", "modern_reference"))
            ),
            evidence_policy=evidence_policy,
            cross_series_constraint_policy=cross_series_constraint_policy,
            broker_delivery_artifact=broker_ref,
            source_format=str(data.get("source_format", "ascii")),
            timeframe=str(data.get("timeframe", "T")),
            symbols=tuple(
                str(value)
                for value in _sequence(
                    data.get("symbols", RECONSTRUCTION_SYMBOLS)
                )
            ),
            proposal_engine_ids=tuple(
                str(value)
                for value in _sequence(data.get("proposal_engine_ids", ()))
            ),
            selected_proposal_engine_ids=tuple(
                str(value)
                for value in _sequence(
                    data.get("selected_proposal_engine_ids", ())
                )
            ),
            proposal_evaluation_paths=tuple(
                str(value)
                for value in _sequence(
                    data.get("proposal_evaluation_paths", ())
                )
            ),
            qualification_dossier_path=_optional_text(
                data.get("qualification_dossier_path")
            ),
            hawkes_product_selection_dossier_path=_optional_text(
                data.get("hawkes_product_selection_dossier_path")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSpecV2(ReconstructionPlanSpecV1):
    """Executable HistData plan with bound proposal-portfolio evidence."""

    schema_version: str = RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        ReconstructionPlanSpecV1.__post_init__(self)
        if not self.proposal_engine_ids:
            raise ReconstructionUnsupportedError(
                "v2 plan requires an explicit proposal engine ordering"
            )
        if not self.selected_proposal_engine_ids:
            raise ReconstructionUnsupportedError(
                "v2 plan requires an explicit reconstruction selection"
            )
        if not set(self.selected_proposal_engine_ids).issubset(
            self.proposal_engine_ids
        ):
            raise ReconstructionUnsupportedError(
                "v2 reconstruction selection is absent from its portfolio"
            )
        if not self.proposal_evaluation_paths:
            raise ReconstructionUnsupportedError(
                "v2 plan requires retained proposal evaluation evidence"
            )
        if (
            set(self.selected_proposal_engine_ids)
            & set(HAWKES_SELECTION_ENGINE_IDS)
            and self.hawkes_product_selection_dossier_path is None
        ):
            raise ReconstructionUnsupportedError(
                "v2 marked-Hawkes selection requires its frozen dossier"
            )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanShardV1:
    """One bounded executable plan in a contiguous full-range plan set."""

    start_period: str
    end_period: str
    requested_start_ns: int
    requested_end_ns: int
    plan_id: str
    plan_ref: ArtifactRef
    preflight_status: str
    executable: bool
    refusal_count: int
    resource_summary: Mapping[str, JSONValue]
    empty_window_count: int = 0
    shard_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SHARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_PLAN_SHARD_SCHEMA_VERSION:
            raise ReconstructionPlanError(
                "unsupported reconstruction plan shard"
            )
        start = _period(self.start_period)
        end = _period(self.end_period)
        if start > end:
            raise ReconstructionPlanError("plan shard period range is reversed")
        object.__setattr__(self, "start_period", start)
        object.__setattr__(self, "end_period", end)
        if (
            isinstance(self.requested_start_ns, bool)
            or not isinstance(self.requested_start_ns, int)
            or isinstance(self.requested_end_ns, bool)
            or not isinstance(self.requested_end_ns, int)
            or self.requested_end_ns <= self.requested_start_ns
        ):
            raise ReconstructionPlanError(
                "plan shard nanosecond range is invalid"
            )
        object.__setattr__(self, "plan_id", _required_text(self.plan_id))
        if self.plan_ref.kind != "synthetic_infill_plan_v1":
            raise ReconstructionPlanError("plan shard artifact kind differs")
        status = _required_text(self.preflight_status)
        if status not in {
            "ready",
            "ready_with_empty_windows",
            "ready_with_refusals",
            "ready_with_refusals_and_empty_windows",
            "refused",
        }:
            raise ReconstructionPlanError("plan shard preflight status differs")
        object.__setattr__(self, "preflight_status", status)
        if not isinstance(self.executable, bool):
            raise ReconstructionPlanError(
                "plan shard executable must be boolean"
            )
        if (
            isinstance(self.refusal_count, bool)
            or not isinstance(self.refusal_count, int)
            or self.refusal_count < 0
        ):
            raise ReconstructionPlanError("plan shard refusal count is invalid")
        if (
            isinstance(self.empty_window_count, bool)
            or not isinstance(self.empty_window_count, int)
            or self.empty_window_count < 0
        ):
            raise ReconstructionPlanError(
                "plan shard empty-window count is invalid"
            )
        resources = {
            str(key): value
            for key, value in sorted(self.resource_summary.items())
        }
        object.__setattr__(self, "resource_summary", resources)
        if (
            resources.get("refused_window_count") != self.refusal_count
            or resources.get("empty_window_count", 0) != self.empty_window_count
        ):
            raise ReconstructionPlanError(
                "plan shard terminal counts differ from resources"
            )
        expected_status = (
            "refused"
            if not self.executable
            else _terminal_window_status(
                refusal_count=self.refusal_count,
                empty_window_count=self.empty_window_count,
            )
        )
        if self.preflight_status != expected_status:
            raise ReconstructionPlanError(
                "plan shard status differs from terminal outcomes"
            )
        expected = _stable_id(
            "reconstruction-plan-shard", self.identity_payload()
        )
        if self.shard_id and self.shard_id != expected:
            raise ReconstructionPlanError(
                "reconstruction plan shard identity differs"
            )
        object.__setattr__(self, "shard_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return stable shard content without the derived identity."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "start_period": self.start_period,
            "end_period": self.end_period,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "plan_id": self.plan_id,
            "plan_ref": self.plan_ref.to_dict(),
            "preflight_status": self.preflight_status,
            "executable": self.executable,
            "refusal_count": self.refusal_count,
            "resource_summary": dict(self.resource_summary),
        }
        if self.empty_window_count:
            payload["empty_window_count"] = self.empty_window_count
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded machine-readable shard metadata."""
        return {**self.identity_payload(), "shard_id": self.shard_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionPlanShardV1:
        """Restore and identity-check one plan shard."""
        return cls(
            start_period=str(data.get("start_period", "")),
            end_period=str(data.get("end_period", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            plan_id=str(data.get("plan_id", "")),
            plan_ref=ArtifactRef.from_dict(_mapping(data.get("plan_ref"))),
            preflight_status=str(data.get("preflight_status", "")),
            executable=_strict_bool(data.get("executable"), "executable"),
            refusal_count=_strict_int(
                data.get("refusal_count"), "refusal_count"
            ),
            empty_window_count=_strict_int(
                data.get("empty_window_count", 0), "empty_window_count"
            ),
            resource_summary=_mapping(data.get("resource_summary")),
            shard_id=str(data.get("shard_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSetV1:
    """Content-addressed full-range plan composed of bounded plan shards."""

    source_spec: ReconstructionPlanSpecV1
    shards: tuple[ReconstructionPlanShardV1, ...]
    requested_start_ns: int
    requested_end_ns: int
    resource_summary: Mapping[str, JSONValue]
    status: str
    plan_set_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SET_SCHEMA_VERSION
    _identity_source_spec_payload: Mapping[str, JSONValue] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
        metadata={"reconstruction_schema": False},
    )

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_PLAN_SET_SCHEMA_VERSION:
            raise ReconstructionPlanError("unsupported reconstruction plan set")
        shards = tuple(
            sorted(self.shards, key=lambda item: item.requested_start_ns)
        )
        if not shards or len(shards) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionPlanError(
                "plan set shard count is outside limits"
            )
        if len({item.shard_id for item in shards}) != len(shards):
            raise ReconstructionPlanError("plan set contains duplicate shards")
        for previous, current in pairwise(shards):
            if previous.requested_end_ns != current.requested_start_ns:
                raise ReconstructionPlanError(
                    "plan set shards are not contiguous"
                )
        if (
            self.requested_start_ns != shards[0].requested_start_ns
            or self.requested_end_ns != shards[-1].requested_end_ns
        ):
            raise ReconstructionPlanError(
                "plan set bounds differ from its shards"
            )
        if (
            self.source_spec.start_period != shards[0].start_period
            or self.source_spec.end_period != shards[-1].end_period
        ):
            raise ReconstructionPlanError(
                "plan set periods differ from source spec"
            )
        object.__setattr__(self, "shards", shards)
        resources = {
            str(key): value
            for key, value in sorted(self.resource_summary.items())
        }
        object.__setattr__(self, "resource_summary", resources)
        expected_status = (
            "refused"
            if any(not item.executable for item in shards)
            else _terminal_window_status(
                refusal_count=sum(item.refusal_count for item in shards),
                empty_window_count=sum(
                    item.empty_window_count for item in shards
                ),
            )
        )
        if self.status != expected_status:
            raise ReconstructionPlanError("plan set status differs from shards")
        expected = _stable_id(
            "reconstruction-plan-set", self.identity_payload()
        )
        if self.plan_set_id and self.plan_set_id != expected:
            raise ReconstructionPlanError(
                "reconstruction plan-set identity differs"
            )
        object.__setattr__(self, "plan_set_id", expected)

    @property
    def executable(self) -> bool:
        """Return whether every bounded shard can execute its supported windows."""
        return all(item.executable for item in self.shards)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return stable plan-set content without the derived identity."""
        return {
            "schema_version": self.schema_version,
            "source_spec": (
                dict(self._identity_source_spec_payload)
                if self._identity_source_spec_payload is not None
                else self.source_spec.to_dict()
            ),
            "shards": [item.to_dict() for item in self.shards],
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "resource_summary": dict(self.resource_summary),
            "status": self.status,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded machine-readable full-range planning evidence."""
        return {**self.identity_payload(), "plan_set_id": self.plan_set_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionPlanSetV1:
        """Restore and identity-check one full-range plan set."""
        if data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM:
            raise ReconstructionPlanError(
                "plan set scientific nonclaim differs"
            )
        supplied_id = str(data.get("plan_set_id", ""))
        source_payload = _mapping(data.get("source_spec"))
        source_spec = (
            ReconstructionPlanSpecV2.from_dict(source_payload)
            if source_payload.get("schema_version")
            == RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION
            else ReconstructionPlanSpecV1.from_dict(source_payload)
        )
        restored = cls(
            source_spec=source_spec,
            shards=tuple(
                ReconstructionPlanShardV1.from_dict(_mapping(item))
                for item in _sequence(data.get("shards"))
            ),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            resource_summary=_mapping(data.get("resource_summary")),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if supplied_id == restored.plan_set_id:
            return restored

        # Additive v1 source-spec defaults must not invalidate an immutable
        # plan-set written before those fields existed.  Validate the complete
        # original identity payload first, then retain that exact payload for
        # faithful serialization; no shard, resource, or unknown field may use
        # this compatibility path.
        raw_identity = {
            str(key): cast(JSONValue, value)
            for key, value in data.items()
            if key != "plan_set_id"
        }
        legacy_expected = _stable_id("reconstruction-plan-set", raw_identity)
        normalized_identity = restored.identity_payload()
        raw_source = _mapping(raw_identity.get("source_spec"))
        normalized_source = _mapping(normalized_identity["source_spec"])
        compatible_missing_fields = {
            "dataset_catalog_path",
            "dataset_reference",
            "evidence_policy",
            "cross_series_constraint_policy",
        }
        raw_without_source = dict(raw_identity)
        normalized_without_source = dict(normalized_identity)
        raw_without_source.pop("source_spec", None)
        normalized_without_source.pop("source_spec", None)
        missing_source_fields = set(normalized_source).difference(raw_source)
        source_values_match = all(
            key in normalized_source and value == normalized_source[key]
            for key, value in raw_source.items()
        )
        legacy_compatible = (
            supplied_id == legacy_expected
            and raw_without_source == normalized_without_source
            and bool(missing_source_fields)
            and missing_source_fields.issubset(compatible_missing_fields)
            and source_values_match
        )
        if not legacy_compatible:
            raise ReconstructionPlanError(
                "reconstruction plan-set identity differs"
            )
        object.__setattr__(
            restored,
            "_identity_source_spec_payload",
            {
                str(key): cast(JSONValue, value)
                for key, value in raw_source.items()
            },
        )
        object.__setattr__(restored, "plan_set_id", supplied_id)
        return restored


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSupportWindowV1:
    """One exact plan interval with executable, empty, or refused support."""

    start_ns: int
    end_ns: int
    symbols: tuple[str, ...]
    status: str
    shard_id: str
    plan_id: str
    member_ids: tuple[str, ...] = ()
    resource_estimate: Mapping[str, JSONValue] | None = None
    refusal_code: str | None = None
    refusal_reason: str | None = None
    refusal_id: str | None = None
    source_support_id: str | None = None
    source_status: str | None = None
    core_source_event_counts: Mapping[str, JSONValue] | None = None
    input_source_event_counts: Mapping[str, JSONValue] | None = None
    common_exact_core_timestamp_count: int | None = None
    bounded_nearest_core_timestamp_count: int | None = None
    bounded_nearest_core_stale_timestamp_count: int | None = None
    bounded_nearest_core_maximum_age_ns: int | None = None
    bounded_nearest_core_p95_age_ns: int | None = None
    selected_cross_series_alignment: str | None = None
    recommended_cross_series_event_time_ns: int | None = None
    cross_series_policy_id: str | None = None
    cftc_support_id: str | None = None
    cftc_query_status: str | None = None
    cftc_conditioning_mode: str | None = None
    cftc_reason: str | None = None
    cftc_query_id: str | None = None
    cftc_qualification_id: str | None = None
    empty_code: str | None = None
    empty_reason: str | None = None
    support_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SUPPORT_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_SUPPORT_WINDOW_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction support-window schema"
            )
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ReconstructionPlanError("support window interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        symbols = tuple(sorted(_required_text(value) for value in self.symbols))
        if symbols != RECONSTRUCTION_SYMBOLS:
            raise ReconstructionPlanError(
                "support window must cover the complete HistData triangle"
            )
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "shard_id", _required_text(self.shard_id))
        object.__setattr__(self, "plan_id", _required_text(self.plan_id))
        member_ids = tuple(
            sorted(_required_text(value) for value in self.member_ids)
        )
        if len(member_ids) != len(set(member_ids)):
            raise ReconstructionPlanError(
                "support window contains duplicate ensemble members"
            )
        object.__setattr__(self, "member_ids", member_ids)
        if self.status not in {"executable", "empty", "refused"}:
            raise ReconstructionPlanError("support window status is invalid")
        has_source_support = self.source_support_id is not None
        source_values = (
            self.source_status,
            self.core_source_event_counts,
            self.input_source_event_counts,
            self.common_exact_core_timestamp_count,
        )
        if has_source_support != all(
            value is not None for value in source_values
        ):
            raise ReconstructionPlanError(
                "support window source evidence is incomplete"
            )
        if has_source_support:
            object.__setattr__(
                self,
                "source_support_id",
                _required_text(self.source_support_id),
            )
            if self.source_status not in {"complete", "empty", "incomplete"}:
                raise ReconstructionPlanError(
                    "support window source status is invalid"
                )
            core_counts = _support_source_counts(
                cast(Mapping[str, JSONValue], self.core_source_event_counts),
                symbols=symbols,
                name="core_source_event_counts",
            )
            input_counts = _support_source_counts(
                cast(Mapping[str, JSONValue], self.input_source_event_counts),
                symbols=symbols,
                name="input_source_event_counts",
            )
            if any(
                core_counts[symbol] > input_counts[symbol] for symbol in symbols
            ):
                raise ReconstructionPlanError(
                    "support window core counts exceed input counts"
                )
            common_exact = _strict_int(
                self.common_exact_core_timestamp_count,
                "common_exact_core_timestamp_count",
            )
            if common_exact < 0 or common_exact > min(core_counts.values()):
                raise ReconstructionPlanError(
                    "support window common exact core timestamp count is invalid"
                )
            object.__setattr__(self, "core_source_event_counts", core_counts)
            object.__setattr__(self, "input_source_event_counts", input_counts)
            object.__setattr__(
                self, "common_exact_core_timestamp_count", common_exact
            )
            cross_values = (
                self.bounded_nearest_core_timestamp_count,
                self.bounded_nearest_core_stale_timestamp_count,
                self.bounded_nearest_core_maximum_age_ns,
                self.bounded_nearest_core_p95_age_ns,
                self.selected_cross_series_alignment,
                self.cross_series_policy_id,
            )
            has_cross_support = self.cross_series_policy_id is not None
            if has_cross_support != all(
                value is not None for value in cross_values
            ):
                raise ReconstructionPlanError(
                    "support window cross-series evidence is incomplete"
                )
            if has_cross_support:
                nearest = _strict_int(
                    self.bounded_nearest_core_timestamp_count,
                    "bounded_nearest_core_timestamp_count",
                )
                stale = _strict_int(
                    self.bounded_nearest_core_stale_timestamp_count,
                    "bounded_nearest_core_stale_timestamp_count",
                )
                maximum_age = _strict_int(
                    self.bounded_nearest_core_maximum_age_ns,
                    "bounded_nearest_core_maximum_age_ns",
                )
                p95_age = _strict_int(
                    self.bounded_nearest_core_p95_age_ns,
                    "bounded_nearest_core_p95_age_ns",
                )
                if (
                    nearest < 0
                    or nearest > sum(core_counts.values())
                    or stale < 0
                    or stale > nearest
                    or maximum_age < 0
                    or p95_age < 0
                    or p95_age > maximum_age
                ):
                    raise ReconstructionPlanError(
                        "support window bounded-nearest evidence is invalid"
                    )
                alignment = _required_text(self.selected_cross_series_alignment)
                if alignment not in {
                    "exact_event_sequence",
                    "nearest_prior_bounded",
                    "unavailable",
                }:
                    raise ReconstructionPlanError(
                        "support window selected alignment is invalid"
                    )
                recommended = self.recommended_cross_series_event_time_ns
                if recommended is not None:
                    recommended = _strict_int(
                        recommended,
                        "recommended_cross_series_event_time_ns",
                    )
                    if not start <= recommended < end:
                        raise ReconstructionPlanError(
                            "support window alignment recommendation is outside core"
                        )
                if alignment == "exact_event_sequence" and (
                    not common_exact or recommended is None
                ):
                    raise ReconstructionPlanError(
                        "support window exact alignment lacks support"
                    )
                if alignment == "nearest_prior_bounded" and (
                    not nearest or recommended is None
                ):
                    raise ReconstructionPlanError(
                        "support window bounded alignment lacks support"
                    )
                if alignment == "unavailable" and recommended is not None:
                    raise ReconstructionPlanError(
                        "unavailable alignment recommends an event time"
                    )
                object.__setattr__(
                    self, "bounded_nearest_core_timestamp_count", nearest
                )
                object.__setattr__(
                    self,
                    "bounded_nearest_core_stale_timestamp_count",
                    stale,
                )
                object.__setattr__(
                    self, "bounded_nearest_core_maximum_age_ns", maximum_age
                )
                object.__setattr__(
                    self, "bounded_nearest_core_p95_age_ns", p95_age
                )
                object.__setattr__(
                    self, "selected_cross_series_alignment", alignment
                )
                object.__setattr__(
                    self,
                    "recommended_cross_series_event_time_ns",
                    recommended,
                )
                object.__setattr__(
                    self,
                    "cross_series_policy_id",
                    _required_text(self.cross_series_policy_id),
                )
        has_cftc_support = self.cftc_support_id is not None
        cftc_required = (
            self.cftc_query_status,
            self.cftc_conditioning_mode,
            self.cftc_reason,
        )
        if has_cftc_support != all(
            value is not None for value in cftc_required
        ):
            raise ReconstructionPlanError(
                "support window CFTC evidence is incomplete"
            )
        if has_cftc_support:
            for name in (
                "cftc_support_id",
                "cftc_query_status",
                "cftc_conditioning_mode",
                "cftc_reason",
            ):
                object.__setattr__(
                    self, name, _required_text(getattr(self, name))
                )
            if self.cftc_query_id is not None:
                object.__setattr__(
                    self, "cftc_query_id", _required_text(self.cftc_query_id)
                )
            if self.cftc_qualification_id is not None:
                object.__setattr__(
                    self,
                    "cftc_qualification_id",
                    _required_text(self.cftc_qualification_id),
                )
            if (
                self.cftc_conditioning_mode
                == "cftc-unavailable-explicit-unconditioned-v1"
                and self.cftc_qualification_id is None
            ):
                raise ReconstructionPlanError(
                    "unavailable-CFTC support lacks qualification identity"
                )
        if self.status == "executable":
            if not member_ids or self.resource_estimate is None:
                raise ReconstructionPlanError(
                    "executable support window lacks work metadata"
                )
            if any(
                value is not None
                for value in (
                    self.refusal_code,
                    self.refusal_reason,
                    self.refusal_id,
                )
            ):
                raise ReconstructionPlanError(
                    "executable support window contains refusal metadata"
                )
            if has_source_support and self.source_status != "complete":
                raise ReconstructionPlanError(
                    "executable support window lacks complete source"
                )
            if has_source_support and not (
                self.common_exact_core_timestamp_count
                or self.selected_cross_series_alignment
                in {"exact_event_sequence", "nearest_prior_bounded"}
            ):
                raise ReconstructionPlanError(
                    "executable support window lacks synchronized source evidence"
                )
            if has_cftc_support and self.cftc_conditioning_mode not in {
                "cftc-weekly-state-conditioned-v1",
                "cftc-unavailable-explicit-unconditioned-v1",
            }:
                raise ReconstructionPlanError(
                    "executable support window lacks usable CFTC mode"
                )
            if self.empty_code is not None or self.empty_reason is not None:
                raise ReconstructionPlanError(
                    "executable support window contains empty metadata"
                )
            resource_estimate = {
                str(key): value
                for key, value in sorted(self.resource_estimate.items())
            }
            object.__setattr__(self, "resource_estimate", resource_estimate)
        elif self.status == "refused":
            if member_ids or self.resource_estimate is not None:
                raise ReconstructionPlanError(
                    "refused support window contains executable work metadata"
                )
            object.__setattr__(
                self, "refusal_code", _required_text(self.refusal_code)
            )
            object.__setattr__(
                self, "refusal_reason", _required_text(self.refusal_reason)
            )
            object.__setattr__(
                self, "refusal_id", _required_text(self.refusal_id)
            )
            if self.empty_code is not None or self.empty_reason is not None:
                raise ReconstructionPlanError(
                    "refused support window contains empty metadata"
                )
        else:
            if member_ids or self.resource_estimate is not None:
                raise ReconstructionPlanError(
                    "empty support window contains executable work metadata"
                )
            if any(
                value is not None
                for value in (
                    self.refusal_code,
                    self.refusal_reason,
                    self.refusal_id,
                )
            ):
                raise ReconstructionPlanError(
                    "empty support window contains refusal metadata"
                )
            if not has_source_support or self.source_status != "empty":
                raise ReconstructionPlanError(
                    "empty support window lacks exact empty-source evidence"
                )
            if any(
                cast(Mapping[str, int], self.core_source_event_counts).values()
            ):
                raise ReconstructionPlanError(
                    "empty support window contains core source events"
                )
            object.__setattr__(
                self, "empty_code", _required_text(self.empty_code)
            )
            object.__setattr__(
                self, "empty_reason", _required_text(self.empty_reason)
            )
        expected = _stable_id(
            "reconstruction-plan-support-window", self.identity_payload()
        )
        if self.support_id and self.support_id != expected:
            raise ReconstructionPlanError("support window identity differs")
        object.__setattr__(self, "support_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic per-window support evidence."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "symbols": list(self.symbols),
            "status": self.status,
            "shard_id": self.shard_id,
            "plan_id": self.plan_id,
            "member_ids": list(self.member_ids),
            "resource_estimate": (
                dict(self.resource_estimate)
                if self.resource_estimate is not None
                else None
            ),
            "refusal_code": self.refusal_code,
            "refusal_reason": self.refusal_reason,
            "refusal_id": self.refusal_id,
        }
        if self.source_support_id is not None:
            payload.update(
                {
                    "source_support_id": self.source_support_id,
                    "source_status": self.source_status,
                    "core_source_event_counts": dict(
                        cast(
                            Mapping[str, JSONValue],
                            self.core_source_event_counts,
                        )
                    ),
                    "input_source_event_counts": dict(
                        cast(
                            Mapping[str, JSONValue],
                            self.input_source_event_counts,
                        )
                    ),
                    "common_exact_core_timestamp_count": (
                        self.common_exact_core_timestamp_count
                    ),
                }
            )
            if self.cross_series_policy_id is not None:
                payload.update(
                    {
                        "bounded_nearest_core_timestamp_count": (
                            self.bounded_nearest_core_timestamp_count
                        ),
                        "bounded_nearest_core_stale_timestamp_count": (
                            self.bounded_nearest_core_stale_timestamp_count
                        ),
                        "bounded_nearest_core_maximum_age_ns": (
                            self.bounded_nearest_core_maximum_age_ns
                        ),
                        "bounded_nearest_core_p95_age_ns": (
                            self.bounded_nearest_core_p95_age_ns
                        ),
                        "selected_cross_series_alignment": (
                            self.selected_cross_series_alignment
                        ),
                        "recommended_cross_series_event_time_ns": (
                            self.recommended_cross_series_event_time_ns
                        ),
                        "cross_series_policy_id": self.cross_series_policy_id,
                    }
                )
        if self.cftc_support_id is not None:
            payload.update(
                {
                    "cftc_support_id": self.cftc_support_id,
                    "cftc_query_status": self.cftc_query_status,
                    "cftc_conditioning_mode": self.cftc_conditioning_mode,
                    "cftc_reason": self.cftc_reason,
                    "cftc_query_id": self.cftc_query_id,
                    "cftc_qualification_id": self.cftc_qualification_id,
                }
            )
        if self.status == "empty":
            payload["empty_code"] = self.empty_code
            payload["empty_reason"] = self.empty_reason
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the complete content-addressed support record."""
        return {**self.identity_payload(), "support_id": self.support_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanSupportWindowV1:
        """Restore and identity-check one support record."""
        estimate = data.get("resource_estimate")
        return cls(
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            status=str(data.get("status", "")),
            shard_id=str(data.get("shard_id", "")),
            plan_id=str(data.get("plan_id", "")),
            member_ids=tuple(
                str(value) for value in _sequence(data.get("member_ids", ()))
            ),
            resource_estimate=(
                None
                if estimate is None
                else {
                    str(key): cast(JSONValue, value)
                    for key, value in _mapping(estimate).items()
                }
            ),
            refusal_code=_optional_text(data.get("refusal_code")),
            refusal_reason=_optional_text(data.get("refusal_reason")),
            refusal_id=_optional_text(data.get("refusal_id")),
            source_support_id=_optional_text(data.get("source_support_id")),
            source_status=_optional_text(data.get("source_status")),
            core_source_event_counts=(
                None
                if data.get("core_source_event_counts") is None
                else {
                    str(key): cast(JSONValue, value)
                    for key, value in _mapping(
                        data.get("core_source_event_counts")
                    ).items()
                }
            ),
            input_source_event_counts=(
                None
                if data.get("input_source_event_counts") is None
                else {
                    str(key): cast(JSONValue, value)
                    for key, value in _mapping(
                        data.get("input_source_event_counts")
                    ).items()
                }
            ),
            common_exact_core_timestamp_count=(
                None
                if data.get("common_exact_core_timestamp_count") is None
                else _strict_int(
                    data.get("common_exact_core_timestamp_count"),
                    "common_exact_core_timestamp_count",
                )
            ),
            bounded_nearest_core_timestamp_count=(
                None
                if data.get("bounded_nearest_core_timestamp_count") is None
                else _strict_int(
                    data.get("bounded_nearest_core_timestamp_count"),
                    "bounded_nearest_core_timestamp_count",
                )
            ),
            bounded_nearest_core_stale_timestamp_count=(
                None
                if data.get("bounded_nearest_core_stale_timestamp_count")
                is None
                else _strict_int(
                    data.get("bounded_nearest_core_stale_timestamp_count"),
                    "bounded_nearest_core_stale_timestamp_count",
                )
            ),
            bounded_nearest_core_maximum_age_ns=(
                None
                if data.get("bounded_nearest_core_maximum_age_ns") is None
                else _strict_int(
                    data.get("bounded_nearest_core_maximum_age_ns"),
                    "bounded_nearest_core_maximum_age_ns",
                )
            ),
            bounded_nearest_core_p95_age_ns=(
                None
                if data.get("bounded_nearest_core_p95_age_ns") is None
                else _strict_int(
                    data.get("bounded_nearest_core_p95_age_ns"),
                    "bounded_nearest_core_p95_age_ns",
                )
            ),
            selected_cross_series_alignment=_optional_text(
                data.get("selected_cross_series_alignment")
            ),
            recommended_cross_series_event_time_ns=(
                None
                if data.get("recommended_cross_series_event_time_ns") is None
                else _strict_int(
                    data.get("recommended_cross_series_event_time_ns"),
                    "recommended_cross_series_event_time_ns",
                )
            ),
            cross_series_policy_id=_optional_text(
                data.get("cross_series_policy_id")
            ),
            cftc_support_id=_optional_text(data.get("cftc_support_id")),
            cftc_query_status=_optional_text(data.get("cftc_query_status")),
            cftc_conditioning_mode=_optional_text(
                data.get("cftc_conditioning_mode")
            ),
            cftc_reason=_optional_text(data.get("cftc_reason")),
            cftc_query_id=_optional_text(data.get("cftc_query_id")),
            cftc_qualification_id=_optional_text(
                data.get("cftc_qualification_id")
            ),
            empty_code=_optional_text(data.get("empty_code")),
            empty_reason=_optional_text(data.get("empty_reason")),
            support_id=str(data.get("support_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSupportMapV1:
    """Gap-free, per-window support evidence for one immutable plan set."""

    plan_set_id: str
    source_spec_schema_version: str
    requested_start_ns: int
    requested_end_ns: int
    window_size_ns: int
    symbols: tuple[str, ...]
    selected_proposal_engine_ids: tuple[str, ...]
    windows: tuple[ReconstructionPlanSupportWindowV1, ...]
    resource_summary: Mapping[str, JSONValue]
    status: str
    provider_id: str = CURRENT_EVIDENCE_SOURCE_PROVIDER_ID
    support_map_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction support-map schema"
            )
        object.__setattr__(
            self, "plan_set_id", _required_text(self.plan_set_id)
        )
        if self.source_spec_schema_version not in {
            RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION,
            RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION,
        }:
            raise ReconstructionPlanError(
                "support map source spec is unsupported"
            )
        if self.provider_id != CURRENT_EVIDENCE_SOURCE_PROVIDER_ID:
            raise ReconstructionPlanError(
                "current support maps are HistData.com-only"
            )
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        window_size = _strict_int(self.window_size_ns, "window_size_ns")
        if end <= start or window_size <= 0:
            raise ReconstructionPlanError("support map bounds are invalid")
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        object.__setattr__(self, "window_size_ns", window_size)
        symbols = tuple(sorted(_required_text(value) for value in self.symbols))
        if symbols != RECONSTRUCTION_SYMBOLS:
            raise ReconstructionPlanError(
                "support map must cover the complete HistData triangle"
            )
        object.__setattr__(self, "symbols", symbols)
        engine_ids = tuple(
            _required_text(value) for value in self.selected_proposal_engine_ids
        )
        if len(engine_ids) != len(set(engine_ids)):
            raise ReconstructionPlanError(
                "support map contains duplicate proposal engines"
            )
        if (
            self.source_spec_schema_version
            == RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION
            and not engine_ids
        ):
            raise ReconstructionPlanError(
                "v2 support map lacks its selected proposal engine"
            )
        object.__setattr__(self, "selected_proposal_engine_ids", engine_ids)
        windows = tuple(sorted(self.windows, key=lambda item: item.start_ns))
        if (
            not windows
            or len(windows) > MAX_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS
        ):
            raise ReconstructionPlanError(
                "support map window count is outside limits"
            )
        if len({item.support_id for item in windows}) != len(windows):
            raise ReconstructionPlanError(
                "support map contains duplicate windows"
            )
        if windows[0].start_ns != start or windows[-1].end_ns != end:
            raise ReconstructionPlanError(
                "support map bounds differ from windows"
            )
        for previous, current in pairwise(windows):
            if previous.end_ns != current.start_ns:
                raise ReconstructionPlanError(
                    "support map windows are not exactly contiguous"
                )
        if any(item.symbols != symbols for item in windows):
            raise ReconstructionPlanError("support map symbol scope differs")
        object.__setattr__(self, "windows", windows)
        resources = {
            str(key): value
            for key, value in sorted(self.resource_summary.items())
        }
        planned = _strict_int(
            resources.get("planned_window_count"), "planned_window_count"
        )
        executable = _strict_int(
            resources.get("executable_window_count"), "executable_window_count"
        )
        refused = _strict_int(
            resources.get("refused_window_count"), "refused_window_count"
        )
        empty = _strict_int(
            resources.get("empty_window_count", 0), "empty_window_count"
        )
        observed_executable = sum(
            item.status == "executable" for item in windows
        )
        observed_refused = sum(item.status == "refused" for item in windows)
        observed_empty = sum(item.status == "empty" for item in windows)
        if (
            planned != len(windows)
            or executable != observed_executable
            or refused != observed_refused
            or empty != observed_empty
            or planned != executable + refused + empty
        ):
            raise ReconstructionPlanError(
                "support map windows differ from aggregate resources"
            )
        expected_status = _terminal_window_status(
            refusal_count=observed_refused,
            empty_window_count=observed_empty,
        )
        if self.status != expected_status:
            raise ReconstructionPlanError(
                "support map status differs from windows"
            )
        object.__setattr__(self, "resource_summary", resources)
        expected = _stable_id(
            "reconstruction-plan-support-map", self.identity_payload()
        )
        if self.support_map_id and self.support_map_id != expected:
            raise ReconstructionPlanError("support map identity differs")
        object.__setattr__(self, "support_map_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES
        ):
            raise ReconstructionPlanError(
                "support map exceeds bounded artifact size"
            )

    @property
    def executable_window_count(self) -> int:
        """Return the exact number of supported temporal windows."""
        return sum(item.status == "executable" for item in self.windows)

    @property
    def refused_window_count(self) -> int:
        """Return the exact number of explicitly refused temporal windows."""
        return sum(item.status == "refused" for item in self.windows)

    @property
    def empty_window_count(self) -> int:
        """Return the exact number of explicit source-empty outcomes."""
        return sum(item.status == "empty" for item in self.windows)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic complete-range support evidence."""
        return {
            "schema_version": self.schema_version,
            "plan_set_id": self.plan_set_id,
            "source_spec_schema_version": self.source_spec_schema_version,
            "provider_id": self.provider_id,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "window_size_ns": self.window_size_ns,
            "symbols": list(self.symbols),
            "selected_proposal_engine_ids": list(
                self.selected_proposal_engine_ids
            ),
            "windows": [item.to_dict() for item in self.windows],
            "resource_summary": dict(self.resource_summary),
            "status": self.status,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the complete content-addressed support map."""
        return {
            **self.identity_payload(),
            "support_map_id": self.support_map_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanSupportMapV1:
        """Restore and identity-check one complete-range support map."""
        if data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM:
            raise ReconstructionPlanError(
                "support map scientific nonclaim differs"
            )
        return cls(
            plan_set_id=str(data.get("plan_set_id", "")),
            source_spec_schema_version=str(
                data.get("source_spec_schema_version", "")
            ),
            provider_id=str(data.get("provider_id", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            window_size_ns=_strict_int(
                data.get("window_size_ns"), "window_size_ns"
            ),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            selected_proposal_engine_ids=tuple(
                str(value)
                for value in _sequence(
                    data.get("selected_proposal_engine_ids", ())
                )
            ),
            windows=tuple(
                ReconstructionPlanSupportWindowV1.from_dict(_mapping(item))
                for item in _sequence(data.get("windows"))
            ),
            resource_summary=_mapping(data.get("resource_summary")),
            status=str(data.get("status", "")),
            support_map_id=str(data.get("support_map_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSupportMapIndexV2:
    """Bounded index over contiguous v1 support-map shards."""

    plan_set_id: str
    source_spec_schema_version: str
    requested_start_ns: int
    requested_end_ns: int
    window_size_ns: int
    symbols: tuple[str, ...]
    selected_proposal_engine_ids: tuple[str, ...]
    shard_refs: tuple[ArtifactRef, ...]
    resource_summary: Mapping[str, JSONValue]
    status: str
    provider_id: str = CURRENT_EVIDENCE_SOURCE_PROVIDER_ID
    support_map_index_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction support-map index schema"
            )
        object.__setattr__(
            self, "plan_set_id", _required_text(self.plan_set_id)
        )
        if self.source_spec_schema_version not in {
            RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION,
            RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION,
        }:
            raise ReconstructionPlanError(
                "support-map index source spec is unsupported"
            )
        if self.provider_id != CURRENT_EVIDENCE_SOURCE_PROVIDER_ID:
            raise ReconstructionPlanError(
                "current support-map indexes are HistData.com-only"
            )
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        window_size = _strict_int(self.window_size_ns, "window_size_ns")
        if end <= start or window_size <= 0:
            raise ReconstructionPlanError(
                "support-map index bounds are invalid"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        object.__setattr__(self, "window_size_ns", window_size)
        symbols = tuple(sorted(_required_text(value) for value in self.symbols))
        if symbols != RECONSTRUCTION_SYMBOLS:
            raise ReconstructionPlanError(
                "support-map index must cover the complete HistData triangle"
            )
        object.__setattr__(self, "symbols", symbols)
        engine_ids = tuple(
            _required_text(value) for value in self.selected_proposal_engine_ids
        )
        if len(engine_ids) != len(set(engine_ids)):
            raise ReconstructionPlanError(
                "support-map index contains duplicate proposal engines"
            )
        if (
            self.source_spec_schema_version
            == RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION
            and not engine_ids
        ):
            raise ReconstructionPlanError(
                "v2 support-map index lacks its selected proposal engine"
            )
        object.__setattr__(self, "selected_proposal_engine_ids", engine_ids)
        refs = tuple(
            sorted(
                self.shard_refs,
                key=lambda ref: _strict_int(
                    ref.metadata.get("requested_start_ns"),
                    "support-map shard requested_start_ns",
                ),
            )
        )
        if not refs or len(refs) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionPlanError(
                "support-map index shard count is outside limits"
            )
        if len({ref.sha256 for ref in refs}) != len(refs):
            raise ReconstructionPlanError(
                "support-map index contains duplicate shards"
            )
        shard_counts = {
            "planned_window_count": 0,
            "executable_window_count": 0,
            "refused_window_count": 0,
            "empty_window_count": 0,
        }
        previous_end: int | None = None
        for ref in refs:
            if ref.kind != "reconstruction_plan_support_map_v1":
                raise ReconstructionPlanError(
                    "support-map index shard has the wrong artifact kind"
                )
            metadata = ref.metadata
            if metadata.get("plan_set_id") != self.plan_set_id:
                raise ReconstructionPlanError(
                    "support-map index shard plan-set identity differs"
                )
            shard_start = _strict_int(
                metadata.get("requested_start_ns"),
                "support-map shard requested_start_ns",
            )
            shard_end = _strict_int(
                metadata.get("requested_end_ns"),
                "support-map shard requested_end_ns",
            )
            if shard_end <= shard_start or (
                previous_end is not None and previous_end != shard_start
            ):
                raise ReconstructionPlanError(
                    "support-map index shards are not exactly contiguous"
                )
            previous_end = shard_end
            for name in shard_counts:
                shard_counts[name] += _strict_int(
                    metadata.get(name, 0), f"support-map shard {name}"
                )
        if (
            _strict_int(
                refs[0].metadata.get("requested_start_ns"),
                "support-map first shard start",
            )
            != start
            or previous_end != end
        ):
            raise ReconstructionPlanError(
                "support-map index bounds differ from its shards"
            )
        object.__setattr__(self, "shard_refs", refs)
        resources = {
            str(key): value
            for key, value in sorted(self.resource_summary.items())
        }
        for name, observed in shard_counts.items():
            expected = _strict_int(resources.get(name, 0), name)
            if observed != expected:
                raise ReconstructionPlanError(
                    "support-map index shard counts differ from resources"
                )
        if shard_counts["planned_window_count"] != sum(
            shard_counts[name]
            for name in (
                "executable_window_count",
                "refused_window_count",
                "empty_window_count",
            )
        ):
            raise ReconstructionPlanError(
                "support-map index terminal outcomes do not cover every window"
            )
        expected_status = _terminal_window_status(
            refusal_count=shard_counts["refused_window_count"],
            empty_window_count=shard_counts["empty_window_count"],
        )
        if self.status != expected_status:
            raise ReconstructionPlanError(
                "support-map index status differs from its shards"
            )
        object.__setattr__(self, "resource_summary", resources)
        expected_id = _stable_id(
            "reconstruction-plan-support-map-index", self.identity_payload()
        )
        if (
            self.support_map_index_id
            and self.support_map_index_id != expected_id
        ):
            raise ReconstructionPlanError("support-map index identity differs")
        object.__setattr__(self, "support_map_index_id", expected_id)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES
        ):
            raise ReconstructionPlanError(
                "support-map index exceeds bounded artifact size"
            )

    @property
    def window_count(self) -> int:
        """Return the exact number of indexed terminal windows."""
        return _strict_int(
            self.resource_summary.get("planned_window_count"),
            "planned_window_count",
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_id": self.plan_set_id,
            "source_spec_schema_version": self.source_spec_schema_version,
            "provider_id": self.provider_id,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "window_size_ns": self.window_size_ns,
            "symbols": list(self.symbols),
            "selected_proposal_engine_ids": list(
                self.selected_proposal_engine_ids
            ),
            "shard_refs": [ref.to_dict() for ref in self.shard_refs],
            "resource_summary": dict(self.resource_summary),
            "status": self.status,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "support_map_index_id": self.support_map_index_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanSupportMapIndexV2:
        if data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM:
            raise ReconstructionPlanError(
                "support-map index scientific nonclaim differs"
            )
        return cls(
            plan_set_id=str(data.get("plan_set_id", "")),
            source_spec_schema_version=str(
                data.get("source_spec_schema_version", "")
            ),
            provider_id=str(data.get("provider_id", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            window_size_ns=_strict_int(
                data.get("window_size_ns"), "window_size_ns"
            ),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            selected_proposal_engine_ids=tuple(
                str(value)
                for value in _sequence(
                    data.get("selected_proposal_engine_ids", ())
                )
            ),
            shard_refs=tuple(
                ArtifactRef.from_dict(_mapping(item))
                for item in _sequence(data.get("shard_refs"))
            ),
            resource_summary=_mapping(data.get("resource_summary")),
            status=str(data.get("status", "")),
            support_map_index_id=str(data.get("support_map_index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCampaignProductEntryV1:
    """One terminal support outcome or verified retained-member product."""

    support_id: str
    plan_id: str
    shard_id: str
    start_ns: int
    end_ns: int
    status: str
    ensemble_member_id: str | None = None
    window_id: str | None = None
    product_ref: ArtifactRef | None = None
    observed_event_count: int = 0
    synthetic_event_count: int = 0
    reason_code: str | None = None
    entry_id: str = ""
    schema_version: str = RECONSTRUCTION_CAMPAIGN_PRODUCT_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_CAMPAIGN_PRODUCT_ENTRY_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction campaign-product entry schema"
            )
        for name in ("support_id", "plan_id", "shard_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        start = _strict_int(self.start_ns, "start_ns")
        end = _strict_int(self.end_ns, "end_ns")
        if end <= start:
            raise ReconstructionPlanError("campaign product interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        if self.status not in {
            "verified_product",
            "missing_product",
            "empty",
            "refused",
        }:
            raise ReconstructionPlanError("campaign product status is invalid")
        observed = _strict_int(
            self.observed_event_count, "observed_event_count"
        )
        synthetic = _strict_int(
            self.synthetic_event_count, "synthetic_event_count"
        )
        if observed < 0 or synthetic < 0:
            raise ReconstructionPlanError(
                "campaign product counts are negative"
            )
        object.__setattr__(self, "observed_event_count", observed)
        object.__setattr__(self, "synthetic_event_count", synthetic)
        if self.status == "verified_product":
            member_id = _required_text(self.ensemble_member_id)
            window_id = _required_text(self.window_id)
            if (
                self.product_ref is None
                or self.product_ref.kind
                != RECONSTRUCTION_MANIFEST_ARTIFACT_KIND
            ):
                raise ReconstructionPlanError(
                    "verified campaign product lacks its manifest artifact"
                )
            metadata = self.product_ref.metadata
            if (
                metadata.get("window_id") != window_id
                or metadata.get("ensemble_member_id") != member_id
                or metadata.get("observed_event_count") != observed
                or metadata.get("synthetic_event_count") != synthetic
            ):
                raise ReconstructionPlanError(
                    "campaign product manifest metadata differs from its entry"
                )
            if self.reason_code is not None:
                raise ReconstructionPlanError(
                    "verified campaign product contains a failure reason"
                )
            object.__setattr__(self, "ensemble_member_id", member_id)
            object.__setattr__(self, "window_id", window_id)
        elif self.status == "missing_product":
            object.__setattr__(
                self,
                "ensemble_member_id",
                _required_text(self.ensemble_member_id),
            )
            object.__setattr__(
                self, "window_id", _required_text(self.window_id)
            )
            if self.product_ref is not None or observed or synthetic:
                raise ReconstructionPlanError(
                    "missing campaign product contains committed evidence"
                )
            object.__setattr__(
                self, "reason_code", _required_text(self.reason_code)
            )
        else:
            if (
                self.ensemble_member_id is not None
                or self.window_id is not None
                or self.product_ref is not None
                or observed
                or synthetic
            ):
                raise ReconstructionPlanError(
                    "terminal non-product outcome contains product evidence"
                )
            object.__setattr__(
                self, "reason_code", _required_text(self.reason_code)
            )
        expected = _stable_id(
            "reconstruction-campaign-product-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ReconstructionPlanError(
                "campaign product entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "support_id": self.support_id,
            "plan_id": self.plan_id,
            "shard_id": self.shard_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "status": self.status,
            "ensemble_member_id": self.ensemble_member_id,
            "window_id": self.window_id,
            "product_ref": (
                None if self.product_ref is None else self.product_ref.to_dict()
            ),
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "reason_code": self.reason_code,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionCampaignProductEntryV1:
        product_value = data.get("product_ref")
        return cls(
            support_id=str(data.get("support_id", "")),
            plan_id=str(data.get("plan_id", "")),
            shard_id=str(data.get("shard_id", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            status=str(data.get("status", "")),
            ensemble_member_id=_optional_text(data.get("ensemble_member_id")),
            window_id=_optional_text(data.get("window_id")),
            product_ref=(
                None
                if product_value is None
                else ArtifactRef.from_dict(_mapping(product_value))
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count", 0), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count", 0), "synthetic_event_count"
            ),
            reason_code=_optional_text(data.get("reason_code")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCampaignProductShardV1:
    """Bounded product/outcome entries for one contiguous plan shard."""

    plan_set_id: str
    support_artifact_id: str
    plan_id: str
    shard_id: str
    requested_start_ns: int
    requested_end_ns: int
    entries: tuple[ReconstructionCampaignProductEntryV1, ...]
    status: str
    product_shard_id: str = ""
    schema_version: str = RECONSTRUCTION_CAMPAIGN_PRODUCT_SHARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_CAMPAIGN_PRODUCT_SHARD_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction campaign-product shard schema"
            )
        for name in (
            "plan_set_id",
            "support_artifact_id",
            "plan_id",
            "shard_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise ReconstructionPlanError(
                "campaign product shard bounds are empty"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        entries = tuple(
            sorted(
                self.entries,
                key=lambda item: (
                    item.start_ns,
                    item.end_ns,
                    item.ensemble_member_id or "",
                    item.status,
                ),
            )
        )
        if (
            not entries
            or len(entries) > MAX_RECONSTRUCTION_CAMPAIGN_ENTRIES_PER_SHARD
        ):
            raise ReconstructionPlanError(
                "campaign product shard entry count is outside limits"
            )
        if len({item.entry_id for item in entries}) != len(entries):
            raise ReconstructionPlanError(
                "campaign product shard contains duplicate entries"
            )
        if any(
            item.plan_id != self.plan_id or item.shard_id != self.shard_id
            for item in entries
        ):
            raise ReconstructionPlanError(
                "campaign product entry has a different plan shard"
            )
        groups: list[
            tuple[
                int, int, str, tuple[ReconstructionCampaignProductEntryV1, ...]
            ]
        ] = []
        for entry in entries:
            key = (entry.start_ns, entry.end_ns, entry.support_id)
            if groups and groups[-1][:3] == key:
                previous = groups[-1]
                groups[-1] = (*key, previous[3] + (entry,))
            else:
                groups.append((*key, (entry,)))
        if groups[0][0] != start or groups[-1][1] != end:
            raise ReconstructionPlanError(
                "campaign product shard bounds differ from support outcomes"
            )
        for previous, current in pairwise(groups):
            if previous[1] != current[0]:
                raise ReconstructionPlanError(
                    "campaign product shard support outcomes are not contiguous"
                )
        for _, _, _, group in groups:
            statuses = {item.status for item in group}
            if statuses.issubset({"verified_product", "missing_product"}):
                member_ids = tuple(item.ensemble_member_id for item in group)
                if None in member_ids or len(set(member_ids)) != len(
                    member_ids
                ):
                    raise ReconstructionPlanError(
                        "campaign product support rectangle has duplicate members"
                    )
            elif len(group) != 1 or statuses not in ({"empty"}, {"refused"}):
                raise ReconstructionPlanError(
                    "campaign terminal support outcome is ambiguous"
                )
        object.__setattr__(self, "entries", entries)
        expected_status = (
            "complete"
            if not any(item.status == "missing_product" for item in entries)
            else "incomplete"
        )
        if self.status != expected_status:
            raise ReconstructionPlanError(
                "campaign product shard status differs from its entries"
            )
        expected = _stable_id(
            "reconstruction-campaign-product-shard", self.identity_payload()
        )
        if self.product_shard_id and self.product_shard_id != expected:
            raise ReconstructionPlanError(
                "campaign product shard identity differs"
            )
        object.__setattr__(self, "product_shard_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES
        ):
            raise ReconstructionPlanError(
                "campaign product shard exceeds bounded control size"
            )

    @property
    def support_window_count(self) -> int:
        return len({item.support_id for item in self.entries})

    @property
    def verified_product_count(self) -> int:
        return sum(item.status == "verified_product" for item in self.entries)

    @property
    def missing_product_count(self) -> int:
        return sum(item.status == "missing_product" for item in self.entries)

    @property
    def empty_window_count(self) -> int:
        return sum(item.status == "empty" for item in self.entries)

    @property
    def refused_window_count(self) -> int:
        return sum(item.status == "refused" for item in self.entries)

    @property
    def observed_event_count(self) -> int:
        return sum(item.observed_event_count for item in self.entries)

    @property
    def synthetic_event_count(self) -> int:
        return sum(item.synthetic_event_count for item in self.entries)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_id": self.plan_set_id,
            "support_artifact_id": self.support_artifact_id,
            "plan_id": self.plan_id,
            "shard_id": self.shard_id,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "entries": [item.to_dict() for item in self.entries],
            "support_window_count": self.support_window_count,
            "verified_product_count": self.verified_product_count,
            "missing_product_count": self.missing_product_count,
            "empty_window_count": self.empty_window_count,
            "refused_window_count": self.refused_window_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "status": self.status,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "product_shard_id": self.product_shard_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionCampaignProductShardV1:
        return cls(
            plan_set_id=str(data.get("plan_set_id", "")),
            support_artifact_id=str(data.get("support_artifact_id", "")),
            plan_id=str(data.get("plan_id", "")),
            shard_id=str(data.get("shard_id", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            entries=tuple(
                ReconstructionCampaignProductEntryV1.from_dict(_mapping(value))
                for value in _sequence(data.get("entries"))
            ),
            status=str(data.get("status", "")),
            product_shard_id=str(data.get("product_shard_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCampaignProductIndexV1:
    """Complete bounded index over all support outcomes and member products."""

    plan_set_ref: ArtifactRef
    support_map_ref: ArtifactRef
    observed_dataset_version_id: str
    selected_proposal_engine_ids: tuple[str, ...]
    delivery_profile_id: str
    shard_refs: tuple[ArtifactRef, ...]
    requested_start_ns: int
    requested_end_ns: int
    status: str
    product_index_id: str = ""
    schema_version: str = RECONSTRUCTION_CAMPAIGN_PRODUCT_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_CAMPAIGN_PRODUCT_INDEX_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction campaign-product index schema"
            )
        if self.plan_set_ref.kind != "reconstruction_plan_set_v1":
            raise ReconstructionPlanError(
                "campaign index plan-set kind differs"
            )
        if self.support_map_ref.kind not in {
            "reconstruction_plan_support_map_v1",
            "reconstruction_plan_support_map_index_v2",
        }:
            raise ReconstructionPlanError("campaign index support kind differs")
        plan_set_id = _required_text(
            self.plan_set_ref.metadata.get("plan_set_id")
        )
        if self.support_map_ref.metadata.get("plan_set_id") != plan_set_id:
            raise ReconstructionPlanError(
                "campaign index plan-set/support identities differ"
            )
        dataset_version_id = _required_text(self.observed_dataset_version_id)
        if not dataset_version_id.startswith("dataset-version:sha256:"):
            raise ReconstructionPlanError(
                "campaign index observed parent is not a dataset version"
            )
        object.__setattr__(
            self, "observed_dataset_version_id", dataset_version_id
        )
        engines = tuple(
            _required_text(value) for value in self.selected_proposal_engine_ids
        )
        if not engines or len(set(engines)) != len(engines):
            raise ReconstructionPlanError(
                "campaign product index proposal engines are invalid"
            )
        object.__setattr__(self, "selected_proposal_engine_ids", engines)
        object.__setattr__(
            self,
            "delivery_profile_id",
            _required_text(self.delivery_profile_id),
        )
        start = _strict_int(self.requested_start_ns, "requested_start_ns")
        end = _strict_int(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise ReconstructionPlanError(
                "campaign product index bounds are empty"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        refs = tuple(
            sorted(
                self.shard_refs,
                key=lambda ref: _strict_int(
                    ref.metadata.get("requested_start_ns"),
                    "campaign shard requested_start_ns",
                ),
            )
        )
        if not refs or len(refs) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionPlanError(
                "campaign product index shard count is outside limits"
            )
        previous_end: int | None = None
        missing = 0
        for ref in refs:
            if ref.kind != "reconstruction_campaign_product_shard_v1":
                raise ReconstructionPlanError(
                    "campaign product index shard kind differs"
                )
            metadata = ref.metadata
            if metadata.get("plan_set_id") != plan_set_id:
                raise ReconstructionPlanError(
                    "campaign product shard plan-set identity differs"
                )
            shard_start = _strict_int(
                metadata.get("requested_start_ns"),
                "campaign shard requested_start_ns",
            )
            shard_end = _strict_int(
                metadata.get("requested_end_ns"),
                "campaign shard requested_end_ns",
            )
            if shard_end <= shard_start or (
                previous_end is not None and previous_end != shard_start
            ):
                raise ReconstructionPlanError(
                    "campaign product index shards are not contiguous"
                )
            previous_end = shard_end
            missing += _strict_int(
                metadata.get("missing_product_count"), "missing_product_count"
            )
        if (
            _strict_int(
                refs[0].metadata.get("requested_start_ns"),
                "campaign first shard start",
            )
            != start
            or previous_end != end
        ):
            raise ReconstructionPlanError(
                "campaign product index bounds differ from its shards"
            )
        object.__setattr__(self, "shard_refs", refs)
        expected_status = "complete" if not missing else "incomplete"
        if self.status != expected_status:
            raise ReconstructionPlanError(
                "campaign product index status differs from its shards"
            )
        expected = _stable_id(
            "reconstruction-campaign-product-index", self.identity_payload()
        )
        if self.product_index_id and self.product_index_id != expected:
            raise ReconstructionPlanError(
                "campaign product index identity differs"
            )
        object.__setattr__(self, "product_index_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES
        ):
            raise ReconstructionPlanError(
                "campaign product index exceeds bounded control size"
            )

    @property
    def plan_set_id(self) -> str:
        return _required_text(self.plan_set_ref.metadata.get("plan_set_id"))

    @property
    def support_artifact_id(self) -> str:
        key = (
            "support_map_id"
            if self.support_map_ref.kind == "reconstruction_plan_support_map_v1"
            else "support_map_index_id"
        )
        return _required_text(self.support_map_ref.metadata.get(key))

    def _sum_metadata(self, name: str) -> int:
        return sum(
            _strict_int(ref.metadata.get(name, 0), name)
            for ref in self.shard_refs
        )

    @property
    def support_window_count(self) -> int:
        return self._sum_metadata("support_window_count")

    @property
    def verified_product_count(self) -> int:
        return self._sum_metadata("verified_product_count")

    @property
    def missing_product_count(self) -> int:
        return self._sum_metadata("missing_product_count")

    @property
    def empty_window_count(self) -> int:
        return self._sum_metadata("empty_window_count")

    @property
    def refused_window_count(self) -> int:
        return self._sum_metadata("refused_window_count")

    @property
    def observed_event_count(self) -> int:
        return self._sum_metadata("observed_event_count")

    @property
    def synthetic_event_count(self) -> int:
        return self._sum_metadata("synthetic_event_count")

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_ref": self.plan_set_ref.to_dict(),
            "support_map_ref": self.support_map_ref.to_dict(),
            "observed_dataset_version_id": self.observed_dataset_version_id,
            "selected_proposal_engine_ids": list(
                self.selected_proposal_engine_ids
            ),
            "delivery_profile_id": self.delivery_profile_id,
            "shard_refs": [ref.to_dict() for ref in self.shard_refs],
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "support_window_count": self.support_window_count,
            "verified_product_count": self.verified_product_count,
            "missing_product_count": self.missing_product_count,
            "empty_window_count": self.empty_window_count,
            "refused_window_count": self.refused_window_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "status": self.status,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "product_index_id": self.product_index_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionCampaignProductIndexV1:
        if data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM:
            raise ReconstructionPlanError(
                "campaign product index scientific nonclaim differs"
            )
        return cls(
            plan_set_ref=ArtifactRef.from_dict(
                _mapping(data.get("plan_set_ref"))
            ),
            support_map_ref=ArtifactRef.from_dict(
                _mapping(data.get("support_map_ref"))
            ),
            observed_dataset_version_id=str(
                data.get("observed_dataset_version_id", "")
            ),
            selected_proposal_engine_ids=tuple(
                str(value)
                for value in _sequence(data.get("selected_proposal_engine_ids"))
            ),
            delivery_profile_id=str(data.get("delivery_profile_id", "")),
            shard_refs=tuple(
                ArtifactRef.from_dict(_mapping(value))
                for value in _sequence(data.get("shard_refs"))
            ),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            status=str(data.get("status", "")),
            product_index_id=str(data.get("product_index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCampaignDatasetPublicationV1:
    """Provider-neutral synthetic dataset/catalog publication receipt."""

    product_index_ref: ArtifactRef
    dataset_version_ref: ArtifactRef
    catalog_ref: ArtifactRef
    observed_parent_dataset_version_id: str
    synthetic_dataset_version_id: str
    publication_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_CAMPAIGN_DATASET_PUBLICATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_CAMPAIGN_DATASET_PUBLICATION_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported reconstruction campaign-dataset publication schema"
            )
        if (
            self.product_index_ref.kind
            != "reconstruction_campaign_product_index_v1"
        ):
            raise ReconstructionPlanError(
                "campaign dataset publication product-index kind differs"
            )
        if self.dataset_version_ref.kind != "dataset_version_manifest_v1":
            raise ReconstructionPlanError(
                "campaign dataset publication version kind differs"
            )
        if self.catalog_ref.kind != "dataset_catalog_v1":
            raise ReconstructionPlanError(
                "campaign dataset publication catalog kind differs"
            )
        observed = _required_text(self.observed_parent_dataset_version_id)
        synthetic = _required_text(self.synthetic_dataset_version_id)
        if not observed.startswith(
            "dataset-version:sha256:"
        ) or not synthetic.startswith("dataset-version:sha256:"):
            raise ReconstructionPlanError(
                "campaign dataset publication versions are invalid"
            )
        if (
            self.dataset_version_ref.metadata.get("dataset_version_id")
            != synthetic
        ):
            raise ReconstructionPlanError(
                "campaign dataset version artifact identity differs"
            )
        object.__setattr__(self, "observed_parent_dataset_version_id", observed)
        object.__setattr__(self, "synthetic_dataset_version_id", synthetic)
        expected = _stable_id(
            "reconstruction-campaign-dataset-publication",
            self.identity_payload(),
        )
        if self.publication_id and self.publication_id != expected:
            raise ReconstructionPlanError(
                "campaign dataset publication identity differs"
            )
        object.__setattr__(self, "publication_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "product_index_ref": self.product_index_ref.to_dict(),
            "dataset_version_ref": self.dataset_version_ref.to_dict(),
            "catalog_ref": self.catalog_ref.to_dict(),
            "observed_parent_dataset_version_id": (
                self.observed_parent_dataset_version_id
            ),
            "synthetic_dataset_version_id": self.synthetic_dataset_version_id,
            "status": "qualified",
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionCampaignDatasetPublicationV1:
        if (
            data.get("status") != "qualified"
            or data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM
        ):
            raise ReconstructionPlanError(
                "campaign dataset publication status/nonclaim differs"
            )
        return cls(
            product_index_ref=ArtifactRef.from_dict(
                _mapping(data.get("product_index_ref"))
            ),
            dataset_version_ref=ArtifactRef.from_dict(
                _mapping(data.get("dataset_version_ref"))
            ),
            catalog_ref=ArtifactRef.from_dict(
                _mapping(data.get("catalog_ref"))
            ),
            observed_parent_dataset_version_id=str(
                data.get("observed_parent_dataset_version_id", "")
            ),
            synthetic_dataset_version_id=str(
                data.get("synthetic_dataset_version_id", "")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSetPreflightV1:
    """Fresh public verification of every shard in a plan set."""

    plan_set_id: str
    status: str
    executable: bool
    shard_count: int
    verified_shard_count: int
    refusal_count: int
    resource_summary: Mapping[str, JSONValue]
    shard_preflights: tuple[Mapping[str, JSONValue], ...]
    empty_window_count: int = 0
    schema_version: str = RECONSTRUCTION_PLAN_SET_PREFLIGHT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded public full-range preflight evidence."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "plan_set_id": self.plan_set_id,
            "status": self.status,
            "executable": self.executable,
            "shard_count": self.shard_count,
            "verified_shard_count": self.verified_shard_count,
            "refusal_count": self.refusal_count,
            "resource_summary": dict(self.resource_summary),
            "shard_preflights": [dict(item) for item in self.shard_preflights],
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }
        if self.empty_window_count:
            payload["empty_window_count"] = self.empty_window_count
        return payload


@dataclass(frozen=True, slots=True)
class ReconstructionExecutionRequestV1:
    """Operator intent bound to one immutable reconstruction plan artifact."""

    plan_path: str
    plan_id: str
    information_mode: InformationMode
    scientific_nonclaim_acknowledged: bool
    source_format: str = RECONSTRUCTION_SOURCE_FORMAT
    timeframe: str = RECONSTRUCTION_TIMEFRAME
    symbols: tuple[str, ...] = RECONSTRUCTION_SYMBOLS
    allow_refusals: bool = False
    request_id: str = ""
    schema_version: str = RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION
        ):
            raise ReconstructionUnsupportedError(
                "unsupported reconstruction execution-request schema"
            )
        object.__setattr__(
            self,
            "plan_path",
            str(Path(_required_text(self.plan_path)).expanduser().resolve()),
        )
        object.__setattr__(self, "plan_id", _required_text(self.plan_id))
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        if not self.scientific_nonclaim_acknowledged:
            raise ReconstructionRefusedError(
                "scientific nonclaim acknowledgement is required"
            )
        _validate_public_input_contract(
            source_format=self.source_format,
            timeframe=self.timeframe,
            symbols=self.symbols,
        )
        object.__setattr__(self, "source_format", RECONSTRUCTION_SOURCE_FORMAT)
        object.__setattr__(self, "timeframe", RECONSTRUCTION_TIMEFRAME)
        object.__setattr__(self, "symbols", RECONSTRUCTION_SYMBOLS)
        expected = _stable_id(
            "reconstruction-execution-request", self.identity_payload()
        )
        if self.request_id and self.request_id != expected:
            raise ReconstructionPlanError(
                "reconstruction execution request identity differs"
            )
        object.__setattr__(self, "request_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the exact operator and plan inputs bound by request_id."""
        return {
            "schema_version": self.schema_version,
            "plan_path": self.plan_path,
            "plan_id": self.plan_id,
            "information_mode": self.information_mode.value,
            "source_format": self.source_format,
            "timeframe": self.timeframe,
            "symbols": list(self.symbols),
            "allow_refusals": self.allow_refusals,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
            "scientific_nonclaim_acknowledged": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded machine-readable operator metadata."""
        return {**self.identity_payload(), "request_id": self.request_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExecutionRequestV1:
        """Restore and identity-check an operator execution request."""
        nonclaim = str(data.get("scientific_nonclaim", ""))
        if nonclaim != SCIENTIFIC_NONCLAIM:
            raise ReconstructionRefusedError(
                "execution request scientific nonclaim text differs"
            )
        return cls(
            plan_path=str(data.get("plan_path", "")),
            plan_id=str(data.get("plan_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            scientific_nonclaim_acknowledged=_strict_bool(
                data.get("scientific_nonclaim_acknowledged"),
                "scientific_nonclaim_acknowledged",
            ),
            source_format=str(data.get("source_format", "")),
            timeframe=str(data.get("timeframe", "")),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            allow_refusals=_strict_bool(
                data.get("allow_refusals", False), "allow_refusals"
            ),
            request_id=str(data.get("request_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSetExecutionRequestV1:
    """Durable operator intent for every shard in one verified plan set."""

    plan_set_ref: ArtifactRef
    support_map_ref: ArtifactRef
    requests: tuple[ReconstructionExecutionRequestV1, ...]
    request_set_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_PLAN_SET_EXECUTION_REQUEST_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_SET_EXECUTION_REQUEST_SCHEMA_VERSION
        ):
            raise ReconstructionUnsupportedError(
                "unsupported plan-set execution-request schema"
            )
        if self.plan_set_ref.kind != "reconstruction_plan_set_v1":
            raise ReconstructionPlanError(
                "plan-set execution request has the wrong plan-set artifact"
            )
        if self.support_map_ref.kind not in {
            "reconstruction_plan_support_map_v1",
            "reconstruction_plan_support_map_index_v2",
        }:
            raise ReconstructionPlanError(
                "plan-set execution request has the wrong support artifact"
            )
        plan_set_id = _required_text(
            self.plan_set_ref.metadata.get("plan_set_id")
        )
        if self.support_map_ref.metadata.get("plan_set_id") != plan_set_id:
            raise ReconstructionPlanError(
                "plan-set and support-map identities differ"
            )
        requests = tuple(self.requests)
        if not requests or len(requests) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionUnsupportedError(
                "plan-set execution request count is outside public limits"
            )
        if len({item.request_id for item in requests}) != len(requests):
            raise ReconstructionPlanError(
                "plan-set execution request contains duplicate requests"
            )
        if len({item.plan_id for item in requests}) != len(requests):
            raise ReconstructionPlanError(
                "plan-set execution request contains duplicate plan shards"
            )
        if len({item.information_mode for item in requests}) != 1:
            raise ReconstructionPlanError(
                "plan-set execution requests use different information modes"
            )
        object.__setattr__(self, "requests", requests)
        expected = _stable_id(
            "reconstruction-plan-set-execution-request", self.identity_payload()
        )
        if self.request_set_id and self.request_set_id != expected:
            raise ReconstructionPlanError(
                "plan-set execution request identity differs"
            )
        object.__setattr__(self, "request_set_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES
        ):
            raise ReconstructionPlanError(
                "plan-set execution request exceeds bounded control size"
            )

    @property
    def plan_set_id(self) -> str:
        return _required_text(self.plan_set_ref.metadata.get("plan_set_id"))

    @property
    def information_mode(self) -> InformationMode:
        return self.requests[0].information_mode

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "plan_set_ref": self.plan_set_ref.to_dict(),
            "support_map_ref": self.support_map_ref.to_dict(),
            "requests": [item.to_dict() for item in self.requests],
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "request_set_id": self.request_set_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanSetExecutionRequestV1:
        if data.get("scientific_nonclaim") != SCIENTIFIC_NONCLAIM:
            raise ReconstructionRefusedError(
                "plan-set request scientific nonclaim text differs"
            )
        return cls(
            plan_set_ref=ArtifactRef.from_dict(
                _mapping(data.get("plan_set_ref"))
            ),
            support_map_ref=ArtifactRef.from_dict(
                _mapping(data.get("support_map_ref"))
            ),
            requests=tuple(
                ReconstructionExecutionRequestV1.from_dict(_mapping(value))
                for value in _sequence(data.get("requests"))
            ),
            request_set_id=str(data.get("request_set_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPreflightV1:
    """Bounded readiness, refusal, resource, and evidence decision."""

    request_id: str
    plan_id: str
    status: str
    executable: bool
    plan_status: str
    dry_run: Mapping[str, JSONValue]
    evidence_refs: Mapping[str, ArtifactRef]
    refusal_reasons: tuple[Mapping[str, JSONValue], ...] = ()
    schema_version: str = RECONSTRUCTION_PREFLIGHT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a public preflight report."""
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "plan_id": self.plan_id,
            "status": self.status,
            "executable": self.executable,
            "plan_status": self.plan_status,
            "dry_run": dict(self.dry_run),
            "evidence_refs": {
                name: ref.to_dict() for name, ref in self.evidence_refs.items()
            },
            "refusal_reasons": [dict(value) for value in self.refusal_reasons],
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionOperationReceiptV1:
    """Serializable submission, execution, status, cancel, or resume receipt."""

    operation: str
    request: ReconstructionExecutionRequestV1
    status: str
    handles: tuple[OrchestrationJobHandle, ...] = ()
    status_store_roots: tuple[str, ...] = ()
    execution_attempt_id: str = ""
    job_snapshots: tuple[Mapping[str, JSONValue], ...] = ()
    reports: tuple[ReconstructionRunReportV1, ...] = ()
    report_refs: tuple[ArtifactRef, ...] = ()
    receipt_id: str = ""
    schema_version: str = RECONSTRUCTION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_RECEIPT_SCHEMA_VERSION:
            raise ReconstructionPlanError(
                "unsupported reconstruction operation-receipt schema"
            )
        if len(self.handles) != len(self.status_store_roots):
            raise ReconstructionPlanError(
                "receipt handles and status-store roots differ"
            )
        if self.report_refs and len(self.reports) != len(self.report_refs):
            raise ReconstructionPlanError(
                "receipt reports and report references differ"
            )
        expected = _stable_id(
            "reconstruction-operation-receipt", self.identity_payload()
        )
        if self.receipt_id and self.receipt_id != expected:
            raise ReconstructionPlanError(
                "reconstruction receipt identity differs"
            )
        object.__setattr__(self, "receipt_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return stable receipt content."""
        return {
            "schema_version": self.schema_version,
            "operation": self.operation,
            "request": self.request.to_dict(),
            "status": self.status,
            "handles": [
                cast(dict[str, JSONValue], handle.to_dict())
                for handle in self.handles
            ],
            "status_store_roots": list(self.status_store_roots),
            "execution_attempt_id": self.execution_attempt_id,
            "job_snapshots": [dict(item) for item in self.job_snapshots],
            "reports": [report.to_dict() for report in self.reports],
            "report_refs": [ref.to_dict() for ref in self.report_refs],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return machine-readable receipt content."""
        return {**self.identity_payload(), "receipt_id": self.receipt_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionOperationReceiptV1:
        """Restore an identity-checked public operation receipt."""
        return cls(
            operation=str(data.get("operation", "")),
            request=ReconstructionExecutionRequestV1.from_dict(
                _mapping(data.get("request"))
            ),
            status=str(data.get("status", "")),
            handles=tuple(
                OrchestrationJobHandle(
                    request_id=str(item.get("request_id", "")),
                    workflow_id=str(item.get("workflow_id", "")),
                    run_id=str(item.get("run_id", "")),
                    task_queue=str(item.get("task_queue", "")),
                    namespace=str(item.get("namespace", "")),
                )
                for item in (
                    _mapping(value) for value in _sequence(data.get("handles"))
                )
            ),
            status_store_roots=tuple(
                str(value)
                for value in _sequence(data.get("status_store_roots"))
            ),
            execution_attempt_id=str(data.get("execution_attempt_id", "")),
            job_snapshots=tuple(
                dict(_mapping(value))
                for value in _sequence(data.get("job_snapshots"))
            ),
            reports=tuple(
                ReconstructionRunReportV1.from_dict(_mapping(value))
                for value in _sequence(data.get("reports"))
            ),
            report_refs=tuple(
                ArtifactRef.from_dict(_mapping(value))
                for value in _sequence(data.get("report_refs"))
            ),
            receipt_id=str(data.get("receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanSetReceiptIndexV1:
    """Bounded content-addressed index over per-shard operation receipts."""

    operation: str
    request_set_ref: ArtifactRef
    receipt_refs: tuple[ArtifactRef, ...]
    status_counts: Mapping[str, int]
    status: str
    receipt_index_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_SET_RECEIPT_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_SET_RECEIPT_INDEX_SCHEMA_VERSION
        ):
            raise ReconstructionPlanError(
                "unsupported plan-set receipt-index schema"
            )
        object.__setattr__(self, "operation", _required_text(self.operation))
        if (
            self.request_set_ref.kind
            != "reconstruction_plan_set_execution_request_v1"
        ):
            raise ReconstructionPlanError(
                "receipt index has the wrong plan-set request artifact"
            )
        request_set_id = _required_text(
            self.request_set_ref.metadata.get("request_set_id")
        )
        refs = tuple(self.receipt_refs)
        if not refs or len(refs) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionUnsupportedError(
                "plan-set receipt count is outside public limits"
            )
        if any(
            ref.kind != "reconstruction_operation_receipt_v1" for ref in refs
        ):
            raise ReconstructionPlanError(
                "receipt index contains the wrong artifact kind"
            )
        if any(
            ref.metadata.get("request_set_id") != request_set_id for ref in refs
        ):
            raise ReconstructionPlanError(
                "receipt index contains a different request-set identity"
            )
        request_ids = tuple(
            _required_text(ref.metadata.get("request_id")) for ref in refs
        )
        if len(set(request_ids)) != len(request_ids):
            raise ReconstructionPlanError(
                "receipt index contains duplicate shard requests"
            )
        object.__setattr__(self, "receipt_refs", refs)
        observed = Counter(
            _required_text(ref.metadata.get("status")) for ref in refs
        )
        counts = {
            _required_text(name): _strict_int(value, f"status_counts.{name}")
            for name, value in self.status_counts.items()
        }
        if any(value < 0 for value in counts.values()) or counts != dict(
            observed
        ):
            raise ReconstructionPlanError(
                "receipt-index status counts differ from shard receipts"
            )
        object.__setattr__(self, "status_counts", dict(sorted(counts.items())))
        expected_status = _aggregate_plan_set_operation_status(
            tuple(observed.elements())
        )
        if self.status != expected_status:
            raise ReconstructionPlanError(
                "receipt-index status differs from shard receipts"
            )
        expected = _stable_id(
            "reconstruction-plan-set-receipt-index", self.identity_payload()
        )
        if self.receipt_index_id and self.receipt_index_id != expected:
            raise ReconstructionPlanError(
                "plan-set receipt-index identity differs"
            )
        object.__setattr__(self, "receipt_index_id", expected)
        if len(canonical_contract_json(self.to_dict()).encode("utf-8")) > (
            MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES
        ):
            raise ReconstructionPlanError(
                "plan-set receipt index exceeds bounded control size"
            )

    @property
    def request_set_id(self) -> str:
        return _required_text(
            self.request_set_ref.metadata.get("request_set_id")
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "operation": self.operation,
            "request_set_ref": self.request_set_ref.to_dict(),
            "receipt_refs": [ref.to_dict() for ref in self.receipt_refs],
            "status_counts": dict(self.status_counts),
            "status": self.status,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "receipt_index_id": self.receipt_index_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanSetReceiptIndexV1:
        return cls(
            operation=str(data.get("operation", "")),
            request_set_ref=ArtifactRef.from_dict(
                _mapping(data.get("request_set_ref"))
            ),
            receipt_refs=tuple(
                ArtifactRef.from_dict(_mapping(value))
                for value in _sequence(data.get("receipt_refs"))
            ),
            status_counts={
                str(key): _strict_int(value, f"status_counts.{key}")
                for key, value in _mapping(data.get("status_counts")).items()
            },
            status=str(data.get("status", "")),
            receipt_index_id=str(data.get("receipt_index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


class ReconstructionClient:
    """Supported synchronous and asynchronous reconstruction control facade."""

    def __init__(
        self,
        *,
        config: OrchestrationWorkerConfig | None = None,
        supervisor: OrchestrationSupervisor | None = None,
        temporal_client: Any | None = None,
    ) -> None:
        self.config = config
        self.supervisor = supervisor
        self.temporal_client = temporal_client

    def schemas(self) -> ReconstructionSchemaRegistryV1:
        """Return the installed deterministic reconstruction schema registry."""
        return reconstruction_schema_registry()

    def scientific_ledger(
        self, path: str | Path | None = None
    ) -> ReconstructionScientificLedgerV1:
        """Return the current or one retained identity-checked science ledger."""
        return (
            current_histdata_reconstruction_scientific_ledger()
            if path is None
            else read_reconstruction_scientific_ledger(path)
        )

    def proposal_engines(self) -> ProposalEngineRegistryV1:
        """Return every installed concrete proposal-engine descriptor."""
        return proposal_engine_registry()

    def proposal_portfolio(
        self, plan_path: str | Path
    ) -> ProposalEnginePortfolioV1:
        """Read the exact qualified/refused portfolio bound to one plan."""
        plan = _read_plan(plan_path)
        ref = plan.artifact_graph.get("proposal_engine_portfolio")
        if ref is None:
            raise ReconstructionUnsupportedError(
                "plan has no proposal portfolio artifact"
            )
        verify_artifact_ref(ref)
        payload = _read_json_mapping(ref.path)
        return ProposalEnginePortfolioV1.from_dict(payload)

    def evaluate_proposal_portfolio(
        self,
        benchmark_manifest_path: str | Path,
        source_root: str | Path,
        *,
        output_directory: str | Path,
        engine_ids: Sequence[str] | None = None,
    ) -> ProposalPortfolioEvaluationV1:
        """Execute all HistData benchmark-eligible engines without promotion."""
        try:
            return run_histdata_proposal_portfolio_evaluation(
                benchmark_manifest_path,
                source_root,
                output_directory=output_directory,
                engine_ids=engine_ids,
            )
        except (OSError, TypeError, ValueError, RuntimeError) as err:
            raise ReconstructionValidationError(str(err)) from err

    def qualify_proposal_portfolio(
        self,
        evaluation_path: str | Path,
        experiment_path: str | Path,
        *,
        output_directory: str | Path,
    ) -> PoweredQualificationDossierV1:
        """Produce powered decisions for one exact evaluation and experiment."""
        try:
            return qualify_histdata_proposal_portfolio(
                evaluation_path,
                experiment_path,
                output_directory=output_directory,
            )
        except (OSError, TypeError, ValueError, RuntimeError) as err:
            raise ReconstructionValidationError(str(err)) from err

    def select_hawkes_product(
        self,
        policy_path: str | Path,
        comparison_path: str | Path,
        qualification_path: str | Path,
        *,
        output_directory: str | Path,
    ) -> HawkesProductSelectionDossierV1:
        """Freeze the validation-only diagonal-versus-full product choice."""
        try:
            return build_hawkes_product_selection_dossier(
                policy_path,
                comparison_path,
                qualification_path,
                output_directory=output_directory,
            )
        except (OSError, TypeError, ValueError, RuntimeError) as err:
            raise ReconstructionValidationError(str(err)) from err

    def publish_diagnostics(
        self,
        spec: DiagnosticPublicationSpecV1 | str | Path,
        *,
        output_directory: str | Path,
    ) -> DiagnosticPublicationManifestV1:
        """Publish verified HistData reconstruction diagnostic evidence."""
        try:
            return publish_reconstruction_diagnostics(
                spec, output_directory=output_directory
            )
        except ModuleNotFoundError as err:
            raise ReconstructionUnsupportedError(str(err)) from err
        except (OSError, TypeError, ValueError, RuntimeError) as err:
            raise ReconstructionValidationError(str(err)) from err

    def diagnostics(self, manifest_path: str | Path) -> Mapping[str, JSONValue]:
        """Verify and list one publication-safe diagnostic bundle."""
        try:
            return cast(
                Mapping[str, JSONValue],
                diagnostic_publication_listing(manifest_path),
            )
        except (OSError, TypeError, ValueError, RuntimeError) as err:
            raise ReconstructionValidationError(str(err)) from err

    def experiments(
        self, root: str | Path
    ) -> tuple[Mapping[str, JSONValue], ...]:
        """Discover publication-safe frozen experiment summaries."""
        return tuple(
            read_reconstruction_experiment(path).publication_summary()
            for path in discover_reconstruction_experiments(root)
        )

    def inspect_experiment(
        self, path: str | Path
    ) -> ReconstructionExperimentManifestV1:
        """Read and identity-check one bounded experiment manifest."""
        return read_reconstruction_experiment(path)

    def verify_experiment(
        self, path: str | Path
    ) -> ReconstructionExperimentVerificationV1:
        """Recompute catalog, partition, artifact, split, and code identity."""
        return verify_reconstruction_experiment(
            read_reconstruction_experiment(path)
        )

    def compatibility(
        self,
        plan: ReconstructionPlanSpecV1 | Mapping[str, Any] | str | Path,
        *,
        inspect_source: bool = True,
        inspect_artifacts: bool = True,
    ) -> ReconstructionCompatibilityReportV1:
        """Evaluate one plan without mutating its dataset or artifacts."""
        payload = (
            read_compatibility_plan(plan)
            if isinstance(plan, (str, Path))
            else plan
        )
        return evaluate_reconstruction_compatibility(
            payload,
            inspect_source=inspect_source,
            inspect_artifacts=inspect_artifacts,
        )

    def construct_plan(self, spec: ReconstructionPlanSpecV1) -> ArtifactRef:
        """Build, execution-validate, and persist one content-addressed plan."""
        with powered_qualification_verification_scope():
            plan = self._construct_plan_model(spec)
            return write_synthetic_infill_plan(plan, spec.artifact_root)

    def _construct_plan_model(
        self, spec: ReconstructionPlanSpecV1
    ) -> SyntheticInfillPlanV1:
        """Build one validated plan without a redundant persistence readback."""
        compatibility = self.compatibility(
            spec,
            inspect_source=True,
            # Contract-specific loaders below verify every strong input.  The
            # public compatibility command additionally inspects their wire
            # schema before construction.
            inspect_artifacts=False,
        )
        if not compatibility.executable:
            blocking = next(
                (
                    item
                    for item in compatibility.findings
                    if item.status
                    not in {
                        ReconstructionCompatibilityStatus.EXACT,
                        ReconstructionCompatibilityStatus.COMPATIBLE_TRANSLATION,
                        ReconstructionCompatibilityStatus.DEPRECATED,
                    }
                ),
                None,
            )
            message = (
                f"{blocking.code}: {blocking.message}"
                if blocking is not None
                else "reconstruction compatibility refused execution"
            )
            if (
                compatibility.status
                is ReconstructionCompatibilityStatus.RESEARCH_ONLY
            ):
                raise ReconstructionRefusedError(message)
            raise ReconstructionUnsupportedError(message)
        try:
            plan = build_synthetic_infill_plan(
                spec.source_root,
                feed_epoch_definition_path=spec.feed_epoch_definition_path,
                observation_operator_path=spec.observation_operator_path,
                market_context_corpus_path=spec.market_context_corpus_path,
                cftc_positioning_corpus_path=spec.cftc_positioning_corpus_path,
                benchmark_manifest_path=spec.benchmark_manifest_path,
                motif_manifest_path=spec.motif_manifest_path,
                motif_index_path=spec.motif_index_path,
                motif_qualification_path=spec.motif_qualification_path,
                motif_leakage_audit_path=spec.motif_leakage_audit_path,
                artifact_root=spec.artifact_root,
                output_root=spec.output_root,
                checkpoint_root=spec.checkpoint_root,
                scratch_root=spec.scratch_root,
                symbols=spec.symbols,
                start_period=spec.start_period,
                end_period=spec.end_period,
                requested_start_ns=spec.requested_start_ns,
                requested_end_ns=spec.requested_end_ns,
                window_size_ns=spec.window_size_ns,
                information_mode=spec.information_mode,
                delivery_mode=spec.delivery_mode,
                evidence_policy=spec.evidence_policy,
                cross_series_constraint_policy=(
                    spec.cross_series_constraint_policy
                ),
                broker_delivery_artifact=spec.broker_delivery_artifact,
                dataset_catalog_path=spec.dataset_catalog_path,
                dataset_reference=spec.dataset_reference,
                proposal_engine_ids=spec.proposal_engine_ids,
                selected_proposal_engine_ids=(
                    spec.selected_proposal_engine_ids
                ),
                proposal_evaluation_paths=spec.proposal_evaluation_paths,
                qualification_dossier_path=spec.qualification_dossier_path,
                hawkes_product_selection_dossier_path=(
                    spec.hawkes_product_selection_dossier_path
                ),
            )
            validate_synthetic_infill_plan_for_execution(plan)
            return plan
        except ReconstructionPublicError:
            raise
        except (OSError, TypeError, ValueError) as err:
            raise ReconstructionPlanError(str(err)) from err

    def construct_plan_set(
        self,
        spec: ReconstructionPlanSpecV1,
        *,
        periods_per_shard: int = DEFAULT_PLAN_SET_PERIODS_PER_SHARD,
    ) -> ArtifactRef:
        """Build one full-range plan as bounded contiguous executable shards."""
        if (
            isinstance(periods_per_shard, bool)
            or not isinstance(periods_per_shard, int)
            or not 1 <= periods_per_shard <= MAX_PLAN_SET_PERIODS_PER_SHARD
        ):
            raise ReconstructionUnsupportedError(
                "periods_per_shard is outside public limits"
            )
        exact_start = spec.requested_start_ns
        exact_end = spec.requested_end_ns
        if exact_start is not None and exact_end is not None:
            first_period = _period_for_ns(exact_start)
            last_period = _period_for_ns(exact_end - 1)
            if (
                spec.start_period is not None
                and _period(spec.start_period) != first_period
            ):
                raise ReconstructionUnsupportedError(
                    "plan-set start_period differs from requested_start_ns"
                )
            if (
                spec.end_period is not None
                and _period(spec.end_period) != last_period
            ):
                raise ReconstructionUnsupportedError(
                    "plan-set end_period differs from requested_end_ns"
                )
        else:
            if spec.start_period is None or spec.end_period is None:
                raise ReconstructionUnsupportedError(
                    "plan sets require explicit periods or exact bounds"
                )
            first_period = _period(spec.start_period)
            last_period = _period(spec.end_period)
        ranges = _period_shards(
            first_period,
            last_period,
            periods_per_shard=periods_per_shard,
        )
        if len(ranges) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionUnsupportedError(
                "requested range exceeds the public plan-set shard limit"
            )
        shards: list[ReconstructionPlanShardV1] = []
        resource_summaries: list[ReconstructionPlanResourceSummaryV1] = []
        source_partitions: dict[str, tuple[str, str, int, int]] = {}
        artifact_root = Path(spec.artifact_root).expanduser().resolve()
        output_root = Path(spec.output_root).expanduser().resolve()
        checkpoint_root = Path(spec.checkpoint_root).expanduser().resolve()
        scratch_root = Path(spec.scratch_root).expanduser().resolve()

        def construct_interval(
            requested_start_ns: int, requested_end_ns: int
        ) -> None:
            start_period = _period_for_ns(requested_start_ns)
            end_period = _period_for_ns(requested_end_ns - 1)
            shard_key = (
                f"{start_period}-{end_period}-"
                f"{requested_start_ns}-{requested_end_ns}"
            )
            shard_spec = replace(
                spec,
                start_period=start_period,
                end_period=end_period,
                requested_start_ns=requested_start_ns,
                requested_end_ns=requested_end_ns,
                artifact_root=str(
                    artifact_root / "shards" / shard_key / "artifacts"
                ),
                output_root=str(output_root / "shards" / shard_key),
                checkpoint_root=str(checkpoint_root / "shards" / shard_key),
                scratch_root=str(scratch_root / "shards" / shard_key),
            )
            try:
                plan = self._construct_plan_model(shard_spec)
            except ReconstructionPlanError as error:
                window_count = (
                    requested_end_ns
                    - requested_start_ns
                    + spec.window_size_ns
                    - 1
                ) // spec.window_size_ns
                if not _splittable_plan_error(error) or window_count <= 1:
                    raise
                left_window_count = max(1, window_count // 2)
                split_ns = min(
                    requested_end_ns,
                    requested_start_ns
                    + left_window_count * spec.window_size_ns,
                )
                if (
                    split_ns <= requested_start_ns
                    or split_ns >= requested_end_ns
                ):
                    raise
                construct_interval(requested_start_ns, split_ns)
                construct_interval(split_ns, requested_end_ns)
                return
            plan_ref = write_synthetic_infill_plan(
                plan, shard_spec.artifact_root
            )
            preflight_status = plan.status
            shards.append(
                ReconstructionPlanShardV1(
                    start_period=start_period,
                    end_period=end_period,
                    requested_start_ns=plan.requested_start_ns,
                    requested_end_ns=plan.requested_end_ns,
                    plan_id=plan.plan_id,
                    plan_ref=plan_ref,
                    preflight_status=preflight_status,
                    executable=True,
                    refusal_count=len(plan.refusals),
                    empty_window_count=plan.resources.empty_window_count,
                    resource_summary=plan.resources.to_dict(),
                )
            )
            _accumulate_plan_set_resources(
                plan,
                resource_summaries=resource_summaries,
                source_partitions=source_partitions,
            )
            if len(shards) > MAX_RECONSTRUCTION_PLAN_SHARDS:
                raise ReconstructionUnsupportedError(
                    "resource-safe plan set exceeds the public shard limit"
                )

        with powered_qualification_verification_scope():
            for start_period, end_period in ranges:
                construct_interval(
                    max(
                        _period_start_ns(start_period),
                        (
                            exact_start
                            if exact_start is not None
                            else _period_start_ns(start_period)
                        ),
                    ),
                    min(
                        _period_start_ns(_next_period(end_period)),
                        (
                            exact_end
                            if exact_end is not None
                            else _period_start_ns(_next_period(end_period))
                        ),
                    ),
                )
        resources = _aggregate_plan_set_resources(
            resource_summaries, source_partitions
        )
        source_spec = replace(
            spec,
            start_period=spec.start_period or first_period,
            end_period=spec.end_period or last_period,
        )
        status = (
            "refused"
            if any(not item.executable for item in shards)
            else _terminal_window_status(
                refusal_count=sum(item.refusal_count for item in shards),
                empty_window_count=sum(
                    item.empty_window_count for item in shards
                ),
            )
        )
        plan_set = ReconstructionPlanSetV1(
            source_spec=source_spec,
            shards=tuple(shards),
            requested_start_ns=shards[0].requested_start_ns,
            requested_end_ns=shards[-1].requested_end_ns,
            resource_summary=resources,
            status=status,
        )
        return write_reconstruction_plan_set(plan_set, artifact_root)

    def preflight_plan_set(
        self, plan_set_path: str | Path
    ) -> ReconstructionPlanSetPreflightV1:
        """Re-verify every artifact, identity, resource bound, and refusal."""
        with powered_qualification_verification_scope():
            return self._preflight_plan_set(plan_set_path)

    def _preflight_plan_set(
        self, plan_set_path: str | Path
    ) -> ReconstructionPlanSetPreflightV1:
        """Run one verification-scoped plan-set preflight."""
        plan_set = read_reconstruction_plan_set(plan_set_path)
        shard_preflights: list[Mapping[str, JSONValue]] = []
        resource_summaries: list[ReconstructionPlanResourceSummaryV1] = []
        source_partitions: dict[str, tuple[str, str, int, int]] = {}
        refusal_count = 0
        empty_window_count = 0
        all_executable = True
        verified_refs: set[tuple[str, str, int | None, str]] = set()

        def verify_once(ref: ArtifactRef) -> None:
            key = (ref.kind, ref.path, ref.size_bytes, ref.sha256)
            if key not in verified_refs:
                verify_artifact_ref(ref)
                verified_refs.add(key)

        for shard in plan_set.shards:
            try:
                verify_once(shard.plan_ref)
            except (OSError, TypeError, ValueError) as error:
                raise ReconstructionPlanError(
                    "plan-set shard artifact differs"
                ) from error
            plan = read_synthetic_infill_plan(shard.plan_ref.path)
            if (
                plan.plan_id != shard.plan_id
                or plan.requested_start_ns != shard.requested_start_ns
                or plan.requested_end_ns != shard.requested_end_ns
                or plan.resources.to_dict() != dict(shard.resource_summary)
            ):
                raise ReconstructionPlanError("plan-set shard content differs")
            for ref in plan.artifact_graph.values():
                verify_once(ref)
            for workflow_request in plan.workflow_requests:
                for task in workflow_request.tasks:
                    for command in task.commands:
                        for ref in command.input_manifest_refs:
                            verify_once(ref)
            validate_synthetic_infill_plan_for_execution(
                plan, verify_artifacts=False
            )
            request = self.create_request(
                shard.plan_ref.path,
                information_mode=plan_set.source_spec.information_mode,
                acknowledge_scientific_nonclaim=True,
                allow_refusals=True,
            )
            preflight = self.preflight(request, verify_artifacts=False)
            current_refusals = len(preflight.refusal_reasons)
            refusal_count += current_refusals
            empty_window_count += plan.resources.empty_window_count
            all_executable = all_executable and preflight.executable
            shard_preflights.append(
                {
                    "shard_id": shard.shard_id,
                    "plan_id": shard.plan_id,
                    "start_period": shard.start_period,
                    "end_period": shard.end_period,
                    "status": preflight.status,
                    "executable": preflight.executable,
                    "refusal_count": current_refusals,
                    "empty_window_count": plan.resources.empty_window_count,
                }
            )
            _accumulate_plan_set_resources(
                plan,
                resource_summaries=resource_summaries,
                source_partitions=source_partitions,
            )
        resources = _aggregate_plan_set_resources(
            resource_summaries, source_partitions
        )
        if resources != dict(plan_set.resource_summary):
            raise ReconstructionPlanError("plan-set aggregate resources differ")
        status = (
            "refused"
            if not all_executable
            else _terminal_window_status(
                refusal_count=refusal_count,
                empty_window_count=empty_window_count,
            )
        )
        if status != plan_set.status:
            raise ReconstructionPlanError("plan-set preflight status differs")
        return ReconstructionPlanSetPreflightV1(
            plan_set_id=plan_set.plan_set_id,
            status=status,
            executable=all_executable,
            shard_count=len(plan_set.shards),
            verified_shard_count=len(shard_preflights),
            refusal_count=refusal_count,
            empty_window_count=empty_window_count,
            resource_summary=resources,
            shard_preflights=tuple(shard_preflights),
        )

    def construct_plan_support_map(
        self,
        plan_set_path: str | Path,
        *,
        output_directory: str | Path,
    ) -> ArtifactRef:
        """Verify a plan set and persist its exact gap-free support map."""
        preflight = self.preflight_plan_set(plan_set_path)
        if not preflight.executable:
            raise ReconstructionRefusedError(
                "plan set must preflight before support-map construction"
            )
        plan_set = read_reconstruction_plan_set(plan_set_path)
        planned = _strict_int(
            plan_set.resource_summary.get("planned_window_count"),
            "planned_window_count",
        )
        if planned <= MAX_MONOLITHIC_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS:
            support_map = _build_reconstruction_plan_support_map(plan_set)
            return write_reconstruction_plan_support_map(
                support_map, output_directory
            )
        shard_refs = tuple(
            write_reconstruction_plan_support_map(
                _build_reconstruction_plan_support_map_shard(plan_set, shard),
                Path(output_directory) / "shards",
            )
            for shard in plan_set.shards
        )
        first = read_reconstruction_plan_support_map(shard_refs[0].path)
        support_map_index = ReconstructionPlanSupportMapIndexV2(
            plan_set_id=plan_set.plan_set_id,
            source_spec_schema_version=plan_set.source_spec.schema_version,
            requested_start_ns=plan_set.requested_start_ns,
            requested_end_ns=plan_set.requested_end_ns,
            window_size_ns=plan_set.source_spec.window_size_ns,
            symbols=plan_set.source_spec.symbols,
            selected_proposal_engine_ids=(first.selected_proposal_engine_ids),
            shard_refs=shard_refs,
            resource_summary=plan_set.resource_summary,
            status=plan_set.status,
        )
        return write_reconstruction_plan_support_map_index(
            support_map_index, output_directory
        )

    def inspect_plan_support_map(
        self,
        path: str | Path,
        *,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int = 100,
    ) -> Mapping[str, JSONValue]:
        """Inspect a bounded slice from either monolithic or indexed support."""
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not (1 <= limit <= MAX_RECONSTRUCTION_SUPPORT_INSPECTION_WINDOWS)
        ):
            raise ReconstructionUnsupportedError(
                "support inspection limit is outside public bounds"
            )
        if (start_ns is None) != (end_ns is None):
            raise ReconstructionUnsupportedError(
                "support inspection bounds must be supplied together"
            )
        if start_ns is not None and end_ns is not None:
            start_ns = _strict_int(start_ns, "start_ns")
            end_ns = _strict_int(end_ns, "end_ns")
            if end_ns <= start_ns:
                raise ReconstructionUnsupportedError(
                    "support inspection interval is empty"
                )
        payload = _read_json_mapping(path)
        schema = str(payload.get("schema_version", ""))
        maps: Iterable[ReconstructionPlanSupportMapV1]
        artifact_kind: str
        if schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION:
            support = read_reconstruction_plan_support_map(path)
            maps = (support,)
            artifact_kind = "reconstruction_plan_support_map_v1"
            plan_set_id = support.plan_set_id
            status = support.status
            total_window_count = len(support.windows)
            full_start = support.requested_start_ns
            full_end = support.requested_end_ns
        elif schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION:
            index = read_reconstruction_plan_support_map_index(
                path, verify_shards=False
            )
            selected_refs = tuple(
                ref
                for ref in index.shard_refs
                if start_ns is None
                or (
                    _strict_int(
                        ref.metadata.get("requested_end_ns"),
                        "support-map shard requested_end_ns",
                    )
                    > start_ns
                    and _strict_int(
                        ref.metadata.get("requested_start_ns"),
                        "support-map shard requested_start_ns",
                    )
                    < cast(int, end_ns)
                )
            )
            maps = iter_reconstruction_plan_support_maps(
                index, shard_refs=selected_refs
            )
            artifact_kind = "reconstruction_plan_support_map_index_v2"
            plan_set_id = index.plan_set_id
            status = index.status
            total_window_count = index.window_count
            full_start = index.requested_start_ns
            full_end = index.requested_end_ns
        else:
            raise ReconstructionPlanError(
                "unsupported support-map artifact schema"
            )
        selected_count = 0
        returned: list[dict[str, JSONValue]] = []
        for support_map in maps:
            for window in support_map.windows:
                if (
                    start_ns is not None
                    and end_ns is not None
                    and not (
                        window.end_ns > start_ns and window.start_ns < end_ns
                    )
                ):
                    continue
                selected_count += 1
                if len(returned) < limit:
                    returned.append(window.to_dict())
        return {
            "schema_version": "histdatacom.reconstruction-plan-support-inspection.v1",
            "support_artifact_kind": artifact_kind,
            "plan_set_id": plan_set_id,
            "status": status,
            "requested_start_ns": full_start,
            "requested_end_ns": full_end,
            "window_count": total_window_count,
            "selected_window_count": selected_count,
            "returned_window_count": len(returned),
            "truncated": selected_count > len(returned),
            "windows": cast(list[JSONValue], returned),
        }

    def construct_campaign_product_index(
        self,
        plan_set_path: str | Path,
        support_map_path: str | Path,
        *,
        output_directory: str | Path,
        verify_products: bool = True,
    ) -> ArtifactRef:
        """Reconcile every support outcome with retained-member products."""
        return _build_reconstruction_campaign_product_index(
            plan_set_path,
            support_map_path,
            output_directory=output_directory,
            verify_products=verify_products,
        )

    def publish_campaign_dataset(
        self,
        product_index_path: str | Path,
        *,
        output_directory: str | Path,
        dataset_id: str = "histdata-triangle-modern-reference-synthetic",
    ) -> ArtifactRef:
        """Publish one qualified provider-neutral synthetic dataset version."""
        return _publish_reconstruction_campaign_dataset(
            product_index_path,
            output_directory=output_directory,
            dataset_id=dataset_id,
        )

    def inspect_campaign_products(
        self,
        product_index_path: str | Path,
        *,
        start_ns: int | None = None,
        end_ns: int | None = None,
        limit: int = 100,
    ) -> Mapping[str, JSONValue]:
        """Inspect a bounded product/outcome slice from a campaign index."""
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not (1 <= limit <= MAX_RECONSTRUCTION_SUPPORT_INSPECTION_WINDOWS)
        ):
            raise ReconstructionUnsupportedError(
                "campaign product inspection limit is outside public bounds"
            )
        if (start_ns is None) != (end_ns is None):
            raise ReconstructionUnsupportedError(
                "campaign product inspection bounds must be supplied together"
            )
        if start_ns is not None and end_ns is not None:
            start_ns = _strict_int(start_ns, "start_ns")
            end_ns = _strict_int(end_ns, "end_ns")
            if end_ns <= start_ns:
                raise ReconstructionUnsupportedError(
                    "campaign product inspection interval is empty"
                )
        index = read_reconstruction_campaign_product_index(
            product_index_path, verify_shards=False
        )
        selected_count = 0
        returned: list[dict[str, JSONValue]] = []
        for ref in index.shard_refs:
            shard_start = _strict_int(
                ref.metadata.get("requested_start_ns"), "requested_start_ns"
            )
            shard_end = _strict_int(
                ref.metadata.get("requested_end_ns"), "requested_end_ns"
            )
            if (
                start_ns is not None
                and end_ns is not None
                and not (shard_end > start_ns and shard_start < end_ns)
            ):
                continue
            verify_artifact_ref(ref)
            shard = read_reconstruction_campaign_product_shard(ref.path)
            for entry in shard.entries:
                if (
                    start_ns is not None
                    and end_ns is not None
                    and not (
                        entry.end_ns > start_ns and entry.start_ns < end_ns
                    )
                ):
                    continue
                selected_count += 1
                if len(returned) < limit:
                    returned.append(entry.to_dict())
        return {
            "schema_version": "histdatacom.reconstruction-campaign-product-inspection.v1",
            "product_index_id": index.product_index_id,
            "status": index.status,
            "requested_start_ns": index.requested_start_ns,
            "requested_end_ns": index.requested_end_ns,
            "support_window_count": index.support_window_count,
            "verified_product_count": index.verified_product_count,
            "missing_product_count": index.missing_product_count,
            "empty_window_count": index.empty_window_count,
            "refused_window_count": index.refused_window_count,
            "selected_entry_count": selected_count,
            "returned_entry_count": len(returned),
            "truncated": selected_count > len(returned),
            "entries": cast(list[JSONValue], returned),
        }

    def create_request(
        self,
        plan_path: str | Path,
        *,
        information_mode: InformationMode | str,
        acknowledge_scientific_nonclaim: bool,
        allow_refusals: bool = False,
    ) -> ReconstructionExecutionRequestV1:
        """Bind explicit operator intent to a verified plan identity."""
        plan = _read_plan(plan_path)
        return ReconstructionExecutionRequestV1(
            plan_path=str(Path(plan_path).expanduser().resolve()),
            plan_id=plan.plan_id,
            information_mode=InformationMode.from_value(information_mode),
            scientific_nonclaim_acknowledged=acknowledge_scientific_nonclaim,
            allow_refusals=allow_refusals,
        )

    def create_plan_set_requests(
        self,
        plan_set_path: str | Path,
        *,
        information_mode: InformationMode | str,
        acknowledge_scientific_nonclaim: bool,
        allow_refusals: bool = True,
    ) -> tuple[ReconstructionExecutionRequestV1, ...]:
        """Bind operator intent to every verified shard of one plan set."""
        preflight = self.preflight_plan_set(plan_set_path)
        if not preflight.executable:
            raise ReconstructionRefusedError("plan set does not preflight")
        plan_set = read_reconstruction_plan_set(plan_set_path)
        mode = InformationMode.from_value(information_mode)
        if mode is not plan_set.source_spec.information_mode:
            raise ReconstructionUnsupportedError(
                "plan-set request information mode differs from its source spec"
            )
        # Preflight has already read, identity-checked, and validated every
        # shard. Bind those verified descriptors directly so large plan sets
        # are not parsed again merely to copy plan IDs.
        return tuple(
            ReconstructionExecutionRequestV1(
                plan_path=shard.plan_ref.path,
                plan_id=shard.plan_id,
                information_mode=mode,
                scientific_nonclaim_acknowledged=(
                    acknowledge_scientific_nonclaim
                ),
                allow_refusals=allow_refusals,
            )
            for shard in plan_set.shards
        )

    def create_plan_set_execution_request(
        self,
        plan_set_path: str | Path,
        support_map_path: str | Path,
        *,
        information_mode: InformationMode | str,
        acknowledge_scientific_nonclaim: bool,
        allow_refusals: bool = True,
    ) -> ReconstructionPlanSetExecutionRequestV1:
        """Bind one durable request to all shards and exact terminal support."""
        plan_set_target = Path(plan_set_path).expanduser().resolve()
        support_target = Path(support_map_path).expanduser().resolve()
        plan_set = read_reconstruction_plan_set(plan_set_target)
        requests = self.create_plan_set_requests(
            plan_set_target,
            information_mode=information_mode,
            acknowledge_scientific_nonclaim=acknowledge_scientific_nonclaim,
            allow_refusals=allow_refusals,
        )
        plan_set_ref = artifact_ref_for_file(
            plan_set_target,
            kind="reconstruction_plan_set_v1",
            metadata={
                "plan_set_id": plan_set.plan_set_id,
                "shard_count": len(plan_set.shards),
                "status": plan_set.status,
            },
        )
        support_payload = _read_json_mapping(support_target)
        support_schema = str(support_payload.get("schema_version", ""))
        if support_schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION:
            support_map = read_reconstruction_plan_support_map(support_target)
            support_kind = "reconstruction_plan_support_map_v1"
            support_metadata: dict[str, JSONValue] = {
                "support_map_id": support_map.support_map_id,
                "plan_set_id": support_map.plan_set_id,
                "window_count": len(support_map.windows),
                "status": support_map.status,
            }
        elif (
            support_schema
            == RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION
        ):
            support_index = read_reconstruction_plan_support_map_index(
                support_target
            )
            support_kind = "reconstruction_plan_support_map_index_v2"
            support_metadata = {
                "support_map_index_id": support_index.support_map_index_id,
                "plan_set_id": support_index.plan_set_id,
                "shard_count": len(support_index.shard_refs),
                "window_count": support_index.window_count,
                "status": support_index.status,
            }
        else:
            raise ReconstructionPlanError(
                "unsupported plan-set support artifact schema"
            )
        support_ref = artifact_ref_for_file(
            support_target,
            kind=support_kind,
            metadata=support_metadata,
        )
        request = ReconstructionPlanSetExecutionRequestV1(
            plan_set_ref=plan_set_ref,
            support_map_ref=support_ref,
            requests=requests,
        )
        _validate_plan_set_execution_request_artifacts(
            request, verify_artifacts=True
        )
        return request

    def submit_plan_set(
        self,
        requests: Sequence[ReconstructionExecutionRequestV1],
        *,
        wait: bool = False,
        execution_attempt_id: str = "",
    ) -> tuple[ReconstructionOperationReceiptV1, ...]:
        """Submit every bounded plan shard through the installed Temporal path."""
        selected = tuple(requests)
        if not selected or len(selected) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionUnsupportedError(
                "plan-set request count is outside public limits"
            )
        return tuple(
            self.submit(
                request,
                wait=wait,
                execution_attempt_id=execution_attempt_id,
            )
            for request in selected
        )

    def execute_plan_set_local(
        self,
        requests: Sequence[ReconstructionExecutionRequestV1],
        *,
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> tuple[ReconstructionOperationReceiptV1, ...]:
        """Execute all bounded shards locally through registered handlers."""
        selected = tuple(requests)
        if not selected or len(selected) > MAX_RECONSTRUCTION_PLAN_SHARDS:
            raise ReconstructionUnsupportedError(
                "plan-set request count is outside public limits"
            )
        return tuple(
            self.execute_local(
                request,
                cancellation_requested=cancellation_requested,
            )
            for request in selected
        )

    def run_plan_set_execution_request(
        self,
        request: ReconstructionPlanSetExecutionRequestV1,
        *,
        output_directory: str | Path,
        wait: bool = False,
        local: bool = False,
        execution_attempt_id: str = "",
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> ArtifactRef:
        """Run/submit a durable campaign and index every shard receipt."""
        _validate_plan_set_execution_request_artifacts(
            request, verify_artifacts=True
        )
        receipts = (
            self.execute_plan_set_local(
                request.requests,
                cancellation_requested=cancellation_requested,
            )
            if local
            else self.submit_plan_set(
                request.requests,
                wait=wait,
                execution_attempt_id=execution_attempt_id,
            )
        )
        return _persist_plan_set_operation(
            request,
            receipts,
            output_directory=output_directory,
            operation="execute_local" if local else "submit",
        )

    def inspect_plan_set(
        self,
        receipts: Sequence[ReconstructionOperationReceiptV1],
        *,
        offline: bool = False,
    ) -> tuple[ReconstructionOperationReceiptV1, ...]:
        """Inspect every persisted plan-set operation receipt."""
        return tuple(
            self.inspect(receipt, offline=offline) for receipt in receipts
        )

    def cancel_plan_set(
        self,
        receipts: Sequence[ReconstructionOperationReceiptV1],
        *,
        reason: str = "",
    ) -> tuple[ReconstructionOperationReceiptV1, ...]:
        """Request cancellation for every submitted plan-set shard."""
        return tuple(
            self.cancel(receipt, reason=reason) for receipt in receipts
        )

    def resume_plan_set(
        self,
        receipts: Sequence[ReconstructionOperationReceiptV1],
        *,
        wait: bool = False,
        local: bool = False,
    ) -> tuple[ReconstructionOperationReceiptV1, ...]:
        """Resume every plan-set shard from durable checkpoints."""
        return tuple(
            self.resume(receipt, wait=wait, local=local) for receipt in receipts
        )

    def operate_plan_set_receipt_index(
        self,
        receipt_index_path: str | Path,
        *,
        operation: str,
        output_directory: str | Path,
        offline: bool = False,
        reason: str = "",
        wait: bool = False,
        local: bool = False,
    ) -> ArtifactRef:
        """Status, cancel, or resume every shard from one durable index."""
        index = read_reconstruction_plan_set_receipt_index(receipt_index_path)
        request = read_reconstruction_plan_set_execution_request(
            index.request_set_ref.path
        )
        receipts = tuple(
            read_operation_receipt(ref.path) for ref in index.receipt_refs
        )
        if operation == "status":
            updated = self.inspect_plan_set(receipts, offline=offline)
        elif operation == "cancel":
            updated = self.cancel_plan_set(receipts, reason=reason)
        elif operation == "resume":
            updated = self.resume_plan_set(receipts, wait=wait, local=local)
        else:
            raise ReconstructionUnsupportedError(
                "plan-set operation must be status, cancel, or resume"
            )
        return _persist_plan_set_operation(
            request,
            updated,
            output_directory=output_directory,
            operation=operation,
            request_set_ref=index.request_set_ref,
        )

    def preflight(
        self,
        request: ReconstructionExecutionRequestV1,
        *,
        verify_artifacts: bool = True,
    ) -> ReconstructionPreflightV1:
        """Validate plan identity, artifacts, support, refusals, and resources."""
        plan = _bound_plan(request)
        try:
            validate_synthetic_infill_plan_for_execution(
                plan, verify_artifacts=verify_artifacts
            )
        except (OSError, TypeError, ValueError) as err:
            raise ReconstructionPlanError(str(err)) from err
        refusals = tuple(item.to_dict() for item in plan.refusals)
        executable = not refusals or request.allow_refusals
        status = (
            _terminal_window_status(
                refusal_count=len(refusals),
                empty_window_count=plan.resources.empty_window_count,
            )
            if executable
            else "refused"
        )
        evidence = {
            name: ref
            for name, ref in plan.artifact_graph.items()
            if any(
                token in name
                for token in (
                    "audit",
                    "benchmark",
                    "certification",
                    "information",
                    "policy",
                    "qualification",
                    "scientific",
                    "validation",
                )
            )
        }
        return ReconstructionPreflightV1(
            request_id=request.request_id,
            plan_id=plan.plan_id,
            status=status,
            executable=executable,
            plan_status=plan.status,
            dry_run=plan.dry_run_payload(),
            evidence_refs=evidence,
            refusal_reasons=refusals,
        )

    def submit(
        self,
        request: ReconstructionExecutionRequestV1,
        *,
        wait: bool = False,
        execution_attempt_id: str = "",
    ) -> ReconstructionOperationReceiptV1:
        """Synchronously submit all plan batches and optionally wait."""
        return asyncio.run(
            self.submit_async(
                request,
                wait=wait,
                execution_attempt_id=execution_attempt_id,
            )
        )

    async def submit_async(
        self,
        request: ReconstructionExecutionRequestV1,
        *,
        wait: bool = False,
        execution_attempt_id: str = "",
    ) -> ReconstructionOperationReceiptV1:
        """Submit all plan batches and optionally attach terminal snapshots."""
        plan = self._executable_plan(request)
        handles: list[OrchestrationJobHandle] = []
        roots: list[str] = []
        snapshots: list[Mapping[str, JSONValue]] = []
        for workflow_request in plan.workflow_requests:
            store = ManifestStatusStore(workflow_request.manifest_store_root)
            workflow_id = _attempt_workflow_id(
                workflow_request, execution_attempt_id
            )
            handle = await submit_reconstruction_request(
                workflow_request,
                config=self.config,
                supervisor=self.supervisor,
                client=self.temporal_client,
                status_store=store,
                workflow_id=workflow_id,
                execution_attempt_id=execution_attempt_id,
            )
            handles.append(handle)
            roots.append(workflow_request.manifest_store_root)
            if wait:
                snapshot = await get_job_result(
                    handle.workflow_id,
                    run_id=handle.run_id,
                    config=self.config,
                    supervisor=self.supervisor,
                    client=self.temporal_client,
                    status_store=store,
                )
                snapshots.append(
                    cast(Mapping[str, JSONValue], snapshot.to_dict())
                )
        status = "submitted"
        if wait:
            status = _snapshot_collection_status(snapshots)
        return ReconstructionOperationReceiptV1(
            operation="submit_and_wait" if wait else "submit_only",
            request=request,
            status=status,
            handles=tuple(handles),
            status_store_roots=tuple(roots),
            execution_attempt_id=execution_attempt_id,
            job_snapshots=tuple(snapshots),
        )

    def execute_local(
        self,
        request: ReconstructionExecutionRequestV1,
        *,
        window_id: str = "",
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> ReconstructionOperationReceiptV1:
        """Execute the real registered pipeline in-process for bounded recovery.

        Production submission remains Temporal-backed.  This explicit method is
        for one-process smoke, deterministic parity, and checkpoint recovery;
        it never silently replaces a failed Temporal submission.
        """
        plan = self._executable_plan(request)
        register_first_party_reconstruction_handlers()
        reports: list[ReconstructionRunReportV1] = []
        report_refs: list[ArtifactRef] = []
        matched_window = not window_id
        for workflow_request in plan.workflow_requests:
            selected = _selected_workflow_request(workflow_request, window_id)
            if selected is None:
                continue
            matched_window = True
            states = asyncio.run(
                run_reconstruction_request(
                    selected,
                    cancellation_requested=cancellation_requested,
                )
            )
            report = reconcile_reconstruction_report(selected, states)
            reports.append(report)
            report_refs.append(
                write_reconstruction_report(report, selected.report_root)
            )
        if not matched_window:
            raise ReconstructionPlanError(
                f"window_id is absent from plan: {window_id}"
            )
        status = _report_collection_status(reports)
        return ReconstructionOperationReceiptV1(
            operation="execute_local",
            request=request,
            status=status,
            reports=tuple(reports),
            report_refs=tuple(report_refs),
        )

    def inspect(
        self,
        receipt: ReconstructionOperationReceiptV1,
        *,
        offline: bool = False,
    ) -> ReconstructionOperationReceiptV1:
        """Inspect every submitted handle using its exact persisted store."""
        return asyncio.run(self.inspect_async(receipt, offline=offline))

    async def inspect_async(
        self,
        receipt: ReconstructionOperationReceiptV1,
        *,
        offline: bool = False,
    ) -> ReconstructionOperationReceiptV1:
        """Asynchronously inspect every submitted reconstruction handle."""
        snapshots: list[Mapping[str, JSONValue]] = []
        for handle, root in zip(
            receipt.handles, receipt.status_store_roots, strict=True
        ):
            snapshot = await inspect_job_status(
                handle.workflow_id,
                run_id=handle.run_id,
                config=self.config,
                supervisor=self.supervisor,
                client=self.temporal_client,
                status_store=ManifestStatusStore(root),
                offline=offline,
            )
            snapshots.append(cast(Mapping[str, JSONValue], snapshot.to_dict()))
        return ReconstructionOperationReceiptV1(
            operation="status",
            request=receipt.request,
            status=_snapshot_collection_status(snapshots),
            handles=receipt.handles,
            status_store_roots=receipt.status_store_roots,
            execution_attempt_id=receipt.execution_attempt_id,
            job_snapshots=tuple(snapshots),
        )

    def cancel(
        self,
        receipt: ReconstructionOperationReceiptV1,
        *,
        reason: str = "",
    ) -> ReconstructionOperationReceiptV1:
        """Request live Temporal cancellation for every receipt handle."""
        return asyncio.run(self.cancel_async(receipt, reason=reason))

    async def cancel_async(
        self,
        receipt: ReconstructionOperationReceiptV1,
        *,
        reason: str = "",
    ) -> ReconstructionOperationReceiptV1:
        """Asynchronously request cancellation using aligned status stores."""
        snapshots: list[Mapping[str, JSONValue]] = []
        for handle, root in zip(
            receipt.handles, receipt.status_store_roots, strict=True
        ):
            snapshot = await cancel_job(
                handle.workflow_id,
                run_id=handle.run_id,
                reason=reason,
                config=self.config,
                supervisor=self.supervisor,
                client=self.temporal_client,
                status_store=ManifestStatusStore(root),
            )
            snapshots.append(cast(Mapping[str, JSONValue], snapshot.to_dict()))
        return ReconstructionOperationReceiptV1(
            operation="cancel",
            request=receipt.request,
            status="cancellation_requested",
            handles=receipt.handles,
            status_store_roots=receipt.status_store_roots,
            execution_attempt_id=receipt.execution_attempt_id,
            job_snapshots=tuple(snapshots),
        )

    def resume(
        self,
        receipt: ReconstructionOperationReceiptV1,
        *,
        wait: bool = False,
        local: bool = False,
    ) -> ReconstructionOperationReceiptV1:
        """Resume from durable checkpoints with fresh workflow identities."""
        if local:
            return replace(
                self.execute_local(receipt.request),
                operation="resume_local",
                receipt_id="",
            )
        attempt = _next_resume_attempt(receipt.execution_attempt_id)
        resumed = self.submit(
            receipt.request,
            wait=wait,
            execution_attempt_id=attempt,
        )
        return replace(resumed, operation="resume", receipt_id="")

    def outputs(
        self, request: ReconstructionExecutionRequestV1
    ) -> dict[str, JSONValue]:
        """List compact verified committed product manifests for the plan."""
        plan = _bound_plan(request)
        execution = read_reconstruction_plan_execution_manifest(
            plan.artifact_graph["execution_manifest"].path
        )
        outputs: list[JSONValue] = []
        ignored = 0
        planned_scopes = {
            (task.window.window_id, task.window.ensemble_member_id)
            for workflow_request in plan.workflow_requests
            for task in workflow_request.tasks
        }
        for path in discover_reconstruction_manifests(
            execution.output_root, run_id=plan.run.run_id
        ):
            manifest = verify_reconstruction_publication(path)
            scope = (manifest.window_id, manifest.ensemble_member_id)
            if scope not in planned_scopes:
                ignored += 1
                continue
            outputs.append(_manifest_summary(path, manifest))
        return {
            "schema_version": RECONSTRUCTION_OUTPUT_LIST_SCHEMA_VERSION,
            "request_id": request.request_id,
            "plan_id": plan.plan_id,
            "run_id": plan.run.run_id,
            "output_root": execution.output_root,
            "output_count": len(outputs),
            "ignored_out_of_plan_count": ignored,
            "outputs": outputs,
        }

    def preview(
        self,
        manifest_path: str | Path,
        *,
        limit: int = DEFAULT_PREVIEW_LIMIT,
    ) -> dict[str, JSONValue]:
        """Return bounded rows with origin, lineage, method, and decisions."""
        selected_limit = _preview_limit(limit)
        path = Path(manifest_path).expanduser().resolve()
        manifest = verify_reconstruction_publication(path)
        rows: list[JSONValue] = []
        for batch in iter_reconstruction_event_batches(
            path, batch_size=selected_limit
        ):
            for row in batch.to_pylist():
                rows.append(_preview_row(row))
                if len(rows) >= selected_limit:
                    break
            if len(rows) >= selected_limit:
                break
        return {
            "schema_version": RECONSTRUCTION_PREVIEW_SCHEMA_VERSION,
            "manifest_path": str(path),
            "manifest_id": manifest.manifest_id,
            "publication_id": manifest.publication_id,
            "run_id": manifest.run_id,
            "logical_content_sha256": manifest.replay.logical_content_sha256,
            "validation": manifest.quality.to_dict(),
            "constraints": manifest.constraints.to_dict(),
            "preview_limit": selected_limit,
            "preview_count": len(rows),
            "rows": rows,
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def replay(self, manifest_path: str | Path) -> dict[str, JSONValue]:
        """Integrity-replay a committed output and return compact evidence."""
        path = Path(manifest_path).expanduser().resolve()
        manifest = load_reconstruction_manifest(path)
        streams = read_reconstruction_streams(path)
        event_count = sum(len(stream.events) for stream in streams)
        benchmark_evidence = cast(
            Mapping[str, JSONValue],
            getattr(manifest.quality, "benchmark_evidence", {}),
        )
        return {
            "schema_version": RECONSTRUCTION_REPLAY_SCHEMA_VERSION,
            "manifest_path": str(path),
            "manifest_id": manifest.manifest_id,
            "publication_id": manifest.publication_id,
            "run_id": manifest.run_id,
            "symbols": [stream.symbol for stream in streams],
            "stream_count": len(streams),
            "event_count": event_count,
            "logical_content_sha256": manifest.replay.logical_content_sha256,
            "replay_verified": event_count == manifest.event_count,
            "proposal_engine_registry_id": benchmark_evidence.get(
                "proposal_engine_registry_id"
            ),
            "proposal_portfolio_id": benchmark_evidence.get(
                "proposal_portfolio_id"
            ),
            "proposal_selected_engine_ids": benchmark_evidence.get(
                "proposal_selected_engine_ids", []
            ),
            "proposal_portfolio_diversity_claim": (
                benchmark_evidence.get("proposal_portfolio_diversity_claim")
            ),
            "proposal_eligibility_audit_ids": (
                benchmark_evidence.get("proposal_eligibility_audit_ids", [])
            ),
            "proposal_evidence_ids": benchmark_evidence.get(
                "proposal_evidence_ids", []
            ),
        }

    def certify(
        self,
        spec_path: str | Path,
        *,
        output_directory: str | Path,
    ) -> tuple[
        ReconstructionCertificationDossierV2,
        ModernReferenceCertificationCampaignResultV1,
    ]:
        """Run the public hash-verified modern-reference evidence campaign."""
        result: tuple[
            ReconstructionCertificationDossierV2,
            ModernReferenceCertificationCampaignResultV1,
        ] = run_modern_reference_certification_campaign(
            spec_path, output_directory=output_directory
        )
        return result

    def _executable_plan(
        self, request: ReconstructionExecutionRequestV1
    ) -> SyntheticInfillPlanV1:
        preflight = self.preflight(request)
        if not preflight.executable:
            reasons = "; ".join(
                str(item.get("reason", "refused"))
                for item in preflight.refusal_reasons
            )
            raise ReconstructionRefusedError(reasons or "plan was refused")
        return _bound_plan(request)


def read_plan_spec(
    path: str | Path,
) -> ReconstructionPlanSpecV1 | ReconstructionPlanSpecV2:
    """Read a public plan-spec JSON artifact."""
    payload = _read_json_mapping(path)
    if (
        payload.get("schema_version")
        == RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION
    ):
        return ReconstructionPlanSpecV2.from_dict(payload)
    return ReconstructionPlanSpecV1.from_dict(payload)


def write_reconstruction_plan_set(
    plan_set: ReconstructionPlanSetV1, directory: str | Path
) -> ArtifactRef:
    """Atomically persist one content-addressed bounded plan set."""
    root = Path(directory).expanduser().resolve()
    path = (
        root
        / f"reconstruction-plan-set-{plan_set.plan_set_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, plan_set.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_plan_set_v1",
        metadata={
            "plan_set_id": plan_set.plan_set_id,
            "shard_count": len(plan_set.shards),
            "status": plan_set.status,
        },
    )


def read_reconstruction_plan_set(path: str | Path) -> ReconstructionPlanSetV1:
    """Read and identity-check one bounded plan-set artifact."""
    return ReconstructionPlanSetV1.from_dict(_read_json_mapping(path))


def write_reconstruction_plan_support_map(
    support_map: ReconstructionPlanSupportMapV1, directory: str | Path
) -> ArtifactRef:
    """Atomically persist one content-addressed complete-range support map."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-plan-support-map-"
        f"{support_map.support_map_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, support_map.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_plan_support_map_v1",
        metadata={
            "support_map_id": support_map.support_map_id,
            "plan_set_id": support_map.plan_set_id,
            "window_count": len(support_map.windows),
            "planned_window_count": len(support_map.windows),
            "executable_window_count": support_map.executable_window_count,
            "refused_window_count": support_map.refused_window_count,
            "empty_window_count": support_map.empty_window_count,
            "requested_start_ns": support_map.requested_start_ns,
            "requested_end_ns": support_map.requested_end_ns,
            "selected_proposal_engine_ids": list(
                support_map.selected_proposal_engine_ids
            ),
            "status": support_map.status,
        },
    )


def read_reconstruction_plan_support_map(
    path: str | Path,
) -> ReconstructionPlanSupportMapV1:
    """Read and identity-check one complete-range support map."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES:
        raise ReconstructionPlanError(
            "support map exceeds bounded artifact size"
        )
    return ReconstructionPlanSupportMapV1.from_dict(_read_json_mapping(target))


def write_reconstruction_plan_support_map_index(
    support_map: ReconstructionPlanSupportMapIndexV2,
    directory: str | Path,
) -> ArtifactRef:
    """Persist one bounded full-range index over v1 support-map shards."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-plan-support-map-index-"
        f"{support_map.support_map_index_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, support_map.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_plan_support_map_index_v2",
        metadata={
            "support_map_index_id": support_map.support_map_index_id,
            "plan_set_id": support_map.plan_set_id,
            "shard_count": len(support_map.shard_refs),
            "window_count": support_map.window_count,
            "status": support_map.status,
        },
    )


def read_reconstruction_plan_support_map_index(
    path: str | Path,
    *,
    verify_shards: bool = True,
) -> ReconstructionPlanSupportMapIndexV2:
    """Read and optionally verify a bounded support-map shard index."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES:
        raise ReconstructionPlanError(
            "support-map index exceeds bounded artifact size"
        )
    support_map = ReconstructionPlanSupportMapIndexV2.from_dict(
        _read_json_mapping(target)
    )
    if verify_shards:
        tuple(iter_reconstruction_plan_support_maps(support_map))
    return support_map


def iter_reconstruction_plan_support_maps(
    support_map: ReconstructionPlanSupportMapIndexV2,
    *,
    shard_refs: Sequence[ArtifactRef] | None = None,
) -> Iterable[ReconstructionPlanSupportMapV1]:
    """Verify and stream each bounded support-map shard in index order."""
    selected_refs = (
        support_map.shard_refs if shard_refs is None else tuple(shard_refs)
    )
    indexed_ref_payloads = {
        canonical_contract_json(ref.to_dict()) for ref in support_map.shard_refs
    }
    if not {
        canonical_contract_json(ref.to_dict()) for ref in selected_refs
    }.issubset(indexed_ref_payloads):
        raise ReconstructionPlanError(
            "support-map shard selection is outside its index"
        )
    for ref in selected_refs:
        verify_artifact_ref(ref)
        shard = read_reconstruction_plan_support_map(ref.path)
        metadata = ref.metadata
        if (
            shard.plan_set_id != support_map.plan_set_id
            or shard.requested_start_ns
            != _strict_int(
                metadata.get("requested_start_ns"),
                "support-map shard requested_start_ns",
            )
            or shard.requested_end_ns
            != _strict_int(
                metadata.get("requested_end_ns"),
                "support-map shard requested_end_ns",
            )
            or shard.symbols != support_map.symbols
            or shard.selected_proposal_engine_ids
            != support_map.selected_proposal_engine_ids
            or metadata.get("support_map_id") != shard.support_map_id
        ):
            raise ReconstructionPlanError(
                "support-map index shard content differs from its descriptor"
            )
        yield shard


def write_reconstruction_campaign_product_shard(
    shard: ReconstructionCampaignProductShardV1,
    directory: str | Path,
) -> ArtifactRef:
    """Persist one bounded campaign product/outcome shard."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-campaign-product-shard-"
        f"{shard.product_shard_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, shard.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_campaign_product_shard_v1",
        metadata={
            "product_shard_id": shard.product_shard_id,
            "plan_set_id": shard.plan_set_id,
            "plan_id": shard.plan_id,
            "shard_id": shard.shard_id,
            "requested_start_ns": shard.requested_start_ns,
            "requested_end_ns": shard.requested_end_ns,
            "support_window_count": shard.support_window_count,
            "verified_product_count": shard.verified_product_count,
            "missing_product_count": shard.missing_product_count,
            "empty_window_count": shard.empty_window_count,
            "refused_window_count": shard.refused_window_count,
            "observed_event_count": shard.observed_event_count,
            "synthetic_event_count": shard.synthetic_event_count,
            "status": shard.status,
        },
    )


def read_reconstruction_campaign_product_shard(
    path: str | Path,
) -> ReconstructionCampaignProductShardV1:
    """Read and identity-check one bounded campaign product shard."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES:
        raise ReconstructionPlanError(
            "campaign product shard exceeds bounded control size"
        )
    return ReconstructionCampaignProductShardV1.from_dict(
        _read_json_mapping(target)
    )


def write_reconstruction_campaign_product_index(
    index: ReconstructionCampaignProductIndexV1,
    directory: str | Path,
) -> ArtifactRef:
    """Persist the top-level content-addressed campaign product index."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-campaign-product-index-"
        f"{index.product_index_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, index.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_campaign_product_index_v1",
        metadata={
            "product_index_id": index.product_index_id,
            "plan_set_id": index.plan_set_id,
            "support_artifact_id": index.support_artifact_id,
            "shard_count": len(index.shard_refs),
            "support_window_count": index.support_window_count,
            "verified_product_count": index.verified_product_count,
            "missing_product_count": index.missing_product_count,
            "empty_window_count": index.empty_window_count,
            "refused_window_count": index.refused_window_count,
            "observed_event_count": index.observed_event_count,
            "synthetic_event_count": index.synthetic_event_count,
            "status": index.status,
        },
    )


def read_reconstruction_campaign_product_index(
    path: str | Path,
    *,
    verify_shards: bool = True,
) -> ReconstructionCampaignProductIndexV1:
    """Read and reconcile a campaign index with every bounded shard."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES:
        raise ReconstructionPlanError(
            "campaign product index exceeds bounded control size"
        )
    index = ReconstructionCampaignProductIndexV1.from_dict(
        _read_json_mapping(target)
    )
    if verify_shards:
        for ref in index.shard_refs:
            verify_artifact_ref(ref)
            shard = read_reconstruction_campaign_product_shard(ref.path)
            metadata = ref.metadata
            if (
                metadata.get("product_shard_id") != shard.product_shard_id
                or shard.plan_set_id != index.plan_set_id
                or shard.support_artifact_id != index.support_artifact_id
                or metadata.get("plan_id") != shard.plan_id
                or metadata.get("shard_id") != shard.shard_id
            ):
                raise ReconstructionPlanError(
                    "campaign product shard differs from its index descriptor"
                )
    return index


def write_reconstruction_campaign_dataset_publication(
    publication: ReconstructionCampaignDatasetPublicationV1,
    directory: str | Path,
) -> ArtifactRef:
    """Persist a provider-neutral synthetic dataset publication receipt."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-campaign-dataset-publication-"
        f"{publication.publication_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, publication.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_campaign_dataset_publication_v1",
        metadata={
            "publication_id": publication.publication_id,
            "synthetic_dataset_version_id": (
                publication.synthetic_dataset_version_id
            ),
            "observed_parent_dataset_version_id": (
                publication.observed_parent_dataset_version_id
            ),
            "status": "qualified",
        },
    )


def read_reconstruction_campaign_dataset_publication(
    path: str | Path,
    *,
    verify_artifacts: bool = True,
) -> ReconstructionCampaignDatasetPublicationV1:
    """Read and verify a campaign dataset publication and its graph."""
    publication = ReconstructionCampaignDatasetPublicationV1.from_dict(
        _read_json_mapping(path)
    )
    if verify_artifacts:
        for ref in (
            publication.product_index_ref,
            publication.dataset_version_ref,
            publication.catalog_ref,
        ):
            verify_artifact_ref(ref)
    index = read_reconstruction_campaign_product_index(
        publication.product_index_ref.path,
        verify_shards=verify_artifacts,
    )
    version = DatasetVersionManifestV1.from_dict(
        _read_json_mapping(publication.dataset_version_ref.path)
    )
    catalog = DatasetCatalog.read(publication.catalog_ref.path)
    if (
        index.status != "complete"
        or index.observed_dataset_version_id
        != publication.observed_parent_dataset_version_id
        or version.dataset_version_id
        != publication.synthetic_dataset_version_id
        or tuple(item.parent_dataset_version_id for item in version.parents)
        != (publication.observed_parent_dataset_version_id,)
        or not any(
            item.dataset_version_id == publication.synthetic_dataset_version_id
            for item in catalog.versions
        )
    ):
        raise ReconstructionPlanError(
            "campaign dataset publication graph does not reconcile"
        )
    return publication


def write_execution_request(
    request: ReconstructionExecutionRequestV1, path: str | Path
) -> Path:
    """Atomically write operator request metadata."""
    return _write_json(path, request.to_dict())


def write_reconstruction_plan_set_execution_request(
    request: ReconstructionPlanSetExecutionRequestV1,
    directory: str | Path,
) -> ArtifactRef:
    """Persist one bounded full-campaign operator request."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-plan-set-execution-request-"
        f"{request.request_set_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, request.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_plan_set_execution_request_v1",
        metadata={
            "request_set_id": request.request_set_id,
            "plan_set_id": request.plan_set_id,
            "request_count": len(request.requests),
            "information_mode": request.information_mode.value,
        },
    )


def read_reconstruction_plan_set_execution_request(
    path: str | Path,
    *,
    verify_artifacts: bool = True,
) -> ReconstructionPlanSetExecutionRequestV1:
    """Read and reconcile one plan-set request with plan/support artifacts."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES:
        raise ReconstructionPlanError(
            "plan-set execution request exceeds bounded control size"
        )
    request = ReconstructionPlanSetExecutionRequestV1.from_dict(
        _read_json_mapping(target)
    )
    _validate_plan_set_execution_request_artifacts(
        request, verify_artifacts=verify_artifacts
    )
    return request


def read_execution_request(
    path: str | Path,
) -> ReconstructionExecutionRequestV1:
    """Read and verify operator request metadata."""
    return ReconstructionExecutionRequestV1.from_dict(_read_json_mapping(path))


def write_operation_receipt(
    receipt: ReconstructionOperationReceiptV1, path: str | Path
) -> Path:
    """Atomically write a reconstruction operation receipt."""
    return _write_json(path, receipt.to_dict())


def read_operation_receipt(
    path: str | Path,
) -> ReconstructionOperationReceiptV1:
    """Read and identity-check a reconstruction operation receipt."""
    return ReconstructionOperationReceiptV1.from_dict(_read_json_mapping(path))


def write_reconstruction_plan_set_receipt_index(
    index: ReconstructionPlanSetReceiptIndexV1,
    directory: str | Path,
) -> ArtifactRef:
    """Persist one bounded operation index over durable shard receipts."""
    root = Path(directory).expanduser().resolve()
    path = root / (
        "reconstruction-plan-set-receipt-index-"
        f"{index.receipt_index_id.rsplit(':', 1)[-1]}.json"
    )
    written = _write_json(path, index.to_dict())
    return artifact_ref_for_file(
        written,
        kind="reconstruction_plan_set_receipt_index_v1",
        metadata={
            "receipt_index_id": index.receipt_index_id,
            "request_set_id": index.request_set_id,
            "receipt_count": len(index.receipt_refs),
            "operation": index.operation,
            "status": index.status,
        },
    )


def read_reconstruction_plan_set_receipt_index(
    path: str | Path,
    *,
    verify_artifacts: bool = True,
) -> ReconstructionPlanSetReceiptIndexV1:
    """Read and reconcile a plan-set receipt index and its shard receipts."""
    target = Path(path).expanduser().resolve()
    if target.stat().st_size > MAX_RECONSTRUCTION_PLAN_SET_CONTROL_BYTES:
        raise ReconstructionPlanError(
            "plan-set receipt index exceeds bounded control size"
        )
    index = ReconstructionPlanSetReceiptIndexV1.from_dict(
        _read_json_mapping(target)
    )
    request_set = read_reconstruction_plan_set_execution_request(
        index.request_set_ref.path,
        verify_artifacts=verify_artifacts,
    )
    if request_set.request_set_id != index.request_set_id:
        raise ReconstructionPlanError(
            "receipt index request-set artifact identity differs"
        )
    indexed_requests = {
        item.request_id: item.plan_id for item in request_set.requests
    }
    observed_requests: dict[str, str] = {}
    for ref in index.receipt_refs:
        if verify_artifacts:
            verify_artifact_ref(ref)
        receipt = read_operation_receipt(ref.path)
        if (
            ref.metadata.get("receipt_id") != receipt.receipt_id
            or ref.metadata.get("request_id") != receipt.request.request_id
            or ref.metadata.get("plan_id") != receipt.request.plan_id
            or ref.metadata.get("status") != receipt.status
        ):
            raise ReconstructionPlanError(
                "receipt-index shard metadata differs from its receipt"
            )
        observed_requests[receipt.request.request_id] = receipt.request.plan_id
    if observed_requests != indexed_requests:
        raise ReconstructionPlanError(
            "receipt index does not cover every plan-set request exactly once"
        )
    return index


def _persist_plan_set_operation(
    request: ReconstructionPlanSetExecutionRequestV1,
    receipts: Sequence[ReconstructionOperationReceiptV1],
    *,
    output_directory: str | Path,
    operation: str,
    request_set_ref: ArtifactRef | None = None,
) -> ArtifactRef:
    """Write independently recoverable receipts plus one bounded index."""
    root = Path(output_directory).expanduser().resolve()
    selected = tuple(receipts)
    expected_request_ids = tuple(item.request_id for item in request.requests)
    observed_request_ids = tuple(item.request.request_id for item in selected)
    if observed_request_ids != expected_request_ids:
        raise ReconstructionPlanError(
            "plan-set operation receipts do not cover requests in order"
        )
    bound_request_ref = request_set_ref or (
        write_reconstruction_plan_set_execution_request(
            request, root / "request"
        )
    )
    if (
        bound_request_ref.metadata.get("request_set_id")
        != request.request_set_id
    ):
        raise ReconstructionPlanError(
            "plan-set operation request artifact identity differs"
        )
    receipt_refs: list[ArtifactRef] = []
    for receipt in selected:
        path = (
            root
            / "receipts"
            / (
                f"{receipt.request.request_id.rsplit(':', 1)[-1]}-"
                f"{receipt.receipt_id.rsplit(':', 1)[-1]}.json"
            )
        )
        written = write_operation_receipt(receipt, path)
        receipt_refs.append(
            artifact_ref_for_file(
                written,
                kind="reconstruction_operation_receipt_v1",
                metadata={
                    "request_set_id": request.request_set_id,
                    "request_id": receipt.request.request_id,
                    "plan_id": receipt.request.plan_id,
                    "receipt_id": receipt.receipt_id,
                    "operation": receipt.operation,
                    "status": receipt.status,
                },
            )
        )
    statuses = tuple(receipt.status for receipt in selected)
    index = ReconstructionPlanSetReceiptIndexV1(
        operation=operation,
        request_set_ref=bound_request_ref,
        receipt_refs=tuple(receipt_refs),
        status_counts=dict(Counter(statuses)),
        status=_aggregate_plan_set_operation_status(statuses),
    )
    ref = write_reconstruction_plan_set_receipt_index(index, root)
    read_reconstruction_plan_set_receipt_index(ref.path)
    return ref


def reconstruction_exit_code(
    result: (
        ReconstructionPreflightV1
        | ReconstructionOperationReceiptV1
        | ReconstructionPlanSetReceiptIndexV1
    ),
) -> ReconstructionExitCode:
    """Map a public report or receipt to its stable CLI exit category."""
    if isinstance(result, ReconstructionPreflightV1):
        return (
            ReconstructionExitCode.SUCCESS
            if result.executable
            else ReconstructionExitCode.REFUSED
        )
    if result.status in {
        "cancelled",
        "cancellation_requested",
        "committed",
        "completed",
        "running",
        "submitted",
    }:
        return ReconstructionExitCode.SUCCESS
    if result.status == "refused":
        return ReconstructionExitCode.REFUSED
    if result.status in {"failed", "partial"}:
        return ReconstructionExitCode.VALIDATION_FAILURE
    return ReconstructionExitCode.RUNTIME_FAILURE


def _read_plan(path: str | Path) -> SyntheticInfillPlanV1:
    try:
        return read_synthetic_infill_plan(path)
    except (OSError, TypeError, ValueError) as err:
        raise ReconstructionPlanError(str(err)) from err


def _bound_plan(
    request: ReconstructionExecutionRequestV1,
) -> SyntheticInfillPlanV1:
    plan = _read_plan(request.plan_path)
    if plan.plan_id != request.plan_id:
        raise ReconstructionPlanError("execution request plan_id differs")
    if plan.information_mode is not request.information_mode:
        raise ReconstructionRefusedError(
            "operator information mode differs from the immutable plan"
        )
    if tuple(plan.run.symbols) != RECONSTRUCTION_SYMBOLS:
        raise ReconstructionUnsupportedError(
            "plan does not contain the supported complete EURUSD triangle"
        )
    return plan


def _validate_plan_set_execution_request_artifacts(
    request: ReconstructionPlanSetExecutionRequestV1,
    *,
    verify_artifacts: bool,
) -> None:
    if verify_artifacts:
        verify_artifact_ref(request.plan_set_ref)
        verify_artifact_ref(request.support_map_ref)
    plan_set = read_reconstruction_plan_set(request.plan_set_ref.path)
    if (
        request.plan_set_ref.metadata.get("plan_set_id") != plan_set.plan_set_id
        or request.plan_set_id != plan_set.plan_set_id
    ):
        raise ReconstructionPlanError(
            "plan-set request artifact identity differs from its plan set"
        )
    if request.support_map_ref.kind == "reconstruction_plan_support_map_v1":
        support: (
            ReconstructionPlanSupportMapV1 | ReconstructionPlanSupportMapIndexV2
        )
        support = read_reconstruction_plan_support_map(
            request.support_map_ref.path
        )
        support_id = support.support_map_id
        expected_id = request.support_map_ref.metadata.get("support_map_id")
    else:
        support = read_reconstruction_plan_support_map_index(
            request.support_map_ref.path,
            verify_shards=verify_artifacts,
        )
        support_id = support.support_map_index_id
        expected_id = request.support_map_ref.metadata.get(
            "support_map_index_id"
        )
    if (
        support_id != expected_id
        or support.plan_set_id != plan_set.plan_set_id
        or support.requested_start_ns != plan_set.requested_start_ns
        or support.requested_end_ns != plan_set.requested_end_ns
        or support.status != plan_set.status
    ):
        raise ReconstructionPlanError(
            "plan-set support artifact differs from the execution campaign"
        )
    expected_requests = tuple(
        (
            shard.plan_id,
            str(Path(shard.plan_ref.path).expanduser().resolve()),
        )
        for shard in plan_set.shards
    )
    observed_requests = tuple(
        (item.plan_id, item.plan_path) for item in request.requests
    )
    if observed_requests != expected_requests:
        raise ReconstructionPlanError(
            "plan-set execution requests do not cover its shards in order"
        )
    if any(
        item.information_mode is not plan_set.source_spec.information_mode
        for item in request.requests
    ):
        raise ReconstructionPlanError(
            "plan-set execution request information mode differs"
        )


def _selected_workflow_request(
    request: ReconstructionWorkflowRequestV1, window_id: str
) -> ReconstructionWorkflowRequestV1 | None:
    if not window_id:
        return request
    tasks = tuple(
        task for task in request.tasks if task.window.window_id == window_id
    )
    if not tasks:
        return None
    return replace(
        request,
        tasks=tasks,
        max_parallel_windows=1,
        request_fingerprint="",
    )


def _attempt_workflow_id(
    request: ReconstructionWorkflowRequestV1, execution_attempt_id: str
) -> str:
    if not execution_attempt_id:
        return ""
    digest = hashlib.sha256(
        (
            f"{request.run.run_id}|{request.request_fingerprint}|{execution_attempt_id}"
        ).encode()
    ).hexdigest()[:24]
    return f"histdatacom-reconstruction-{request.request_id}-{digest}"


def _next_resume_attempt(previous: str) -> str:
    prefix = "resume-"
    if previous.startswith(prefix) and previous[len(prefix) :].isdigit():
        ordinal = int(previous[len(prefix) :]) + 1
    else:
        ordinal = 1
    return f"{prefix}{ordinal:03d}"


def _report_collection_status(
    reports: Sequence[ReconstructionRunReportV1],
) -> str:
    statuses = {report.status for report in reports}
    if statuses == {"committed"} and reports:
        return "committed"
    if "failed" in statuses:
        return "failed"
    if "partial" in statuses:
        return "partial"
    if statuses == {"cancelled"}:
        return "cancelled"
    return "failed"


def _snapshot_collection_status(
    snapshots: Sequence[Mapping[str, JSONValue]],
) -> str:
    if not snapshots:
        return "unknown"
    values = {
        str(snapshot.get("status", "")).strip().lower()
        for snapshot in snapshots
    }
    if values.issubset({"completed", "succeeded"}):
        return "completed"
    if "failed" in values:
        return "failed"
    if values.issubset({"cancelled", "canceled"}):
        return "cancelled"
    return "running"


def _manifest_summary(path: Path, manifest: Any) -> dict[str, JSONValue]:
    return {
        "manifest_path": str(path),
        "manifest_id": manifest.manifest_id,
        "publication_id": manifest.publication_id,
        "run_id": manifest.run_id,
        "window_id": manifest.window_id,
        "ensemble_member_id": manifest.ensemble_member_id,
        "symbols": list(manifest.symbols),
        "event_count": manifest.event_count,
        "observed_event_count": manifest.observed_event_count,
        "synthetic_event_count": manifest.synthetic_event_count,
        "logical_content_sha256": manifest.replay.logical_content_sha256,
        "validation_manifest_id": manifest.quality.quality_manifest_id,
        "constraint_manifest_id": (manifest.constraints.constraint_manifest_id),
    }


def _preview_row(row: Mapping[str, Any]) -> dict[str, JSONValue]:
    origin = str(row.get("origin", ""))
    observed = origin == "observed"
    return {
        "event_id": str(row.get("event_id", "")),
        "origin": origin,
        "symbol": str(row.get("symbol", "")),
        "event_time_ns": cast(int, row.get("event_time_ns", 0)),
        "event_sequence": cast(int, row.get("event_sequence", 0)),
        "bid": cast(float, row.get("bid", 0.0)),
        "ask": cast(float, row.get("ask", 0.0)),
        "lineage": {
            "source_version_id": row.get("source_version_id"),
            "source_series_id": row.get("source_series_id"),
            "source_period": row.get("source_period"),
            "source_row_id": row.get("source_row_id"),
            "anchor_interval_id": row.get("anchor_interval_id"),
            "left_anchor_event_id": row.get("left_anchor_event_id"),
            "right_anchor_event_id": row.get("right_anchor_event_id"),
            "immutable_observed_anchor": observed,
        },
        "generation": {
            "method": (
                "immutable_observed_anchor"
                if observed
                else row.get("generator_id")
            ),
            "generator_id": row.get("generator_id"),
            "generator_version": row.get("generator_version"),
            "generator_config_id": row.get("generator_config_id"),
            "reference_id": row.get("reference_id"),
            "motif_id": row.get("motif_id"),
            "feed_epoch_id": row.get("feed_epoch_id"),
            "broker_profile_id": row.get("broker_profile_id"),
            "confidence": row.get("confidence"),
        },
        "constraint_decision": {
            "decision": "immutable_anchor" if observed else "accepted",
            "constraint_set_id": row.get("constraint_set_id"),
        },
    }


def _validate_public_input_contract(
    *, source_format: str, timeframe: str, symbols: Sequence[str]
) -> None:
    if str(source_format).strip().lower() != RECONSTRUCTION_SOURCE_FORMAT:
        raise ReconstructionUnsupportedError(
            "unsupported source format; reconstruction requires ASCII"
        )
    if str(timeframe).strip().upper() != RECONSTRUCTION_TIMEFRAME:
        raise ReconstructionUnsupportedError(
            "unsupported timeframe; reconstruction requires tick timeframe T"
        )
    selected = tuple(sorted(str(value).strip().lower() for value in symbols))
    if selected != RECONSTRUCTION_SYMBOLS:
        raise ReconstructionUnsupportedError(
            "unsupported symbols; reconstruction requires EURGBP/EURUSD/GBPUSD"
        )


def _preview_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReconstructionUnsupportedError("preview limit must be an integer")
    if value < 1 or value > MAX_PREVIEW_LIMIT:
        raise ReconstructionUnsupportedError(
            f"preview limit must be between 1 and {MAX_PREVIEW_LIMIT}"
        )
    return value


def _period(value: Any) -> str:
    selected = str(value or "").strip()
    if (
        len(selected) != 6
        or not selected.isdigit()
        or not 1 <= int(selected[4:]) <= 12
    ):
        raise ReconstructionUnsupportedError(
            "reconstruction period must use YYYYMM"
        )
    return selected


def _next_period(value: str) -> str:
    selected = _period(value)
    year = int(selected[:4])
    month = int(selected[4:])
    if month == 12:
        return f"{year + 1:04d}01"
    return f"{year:04d}{month + 1:02d}"


def _period_start_ns(value: str) -> int:
    selected = _period(value)
    timestamp = datetime(
        int(selected[:4]), int(selected[4:]), 1, tzinfo=timezone.utc
    )
    return int(timestamp.timestamp()) * 1_000_000_000


def _period_for_ns(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReconstructionUnsupportedError("event time must be nanoseconds")
    timestamp = datetime.fromtimestamp(value // 1_000_000_000, tz=timezone.utc)
    return f"{timestamp.year:04d}{timestamp.month:02d}"


def _splittable_plan_error(error: ReconstructionPlanError) -> bool:
    message = str(error).lower()
    return any(
        token in message
        for token in (
            "reconstruction persistence preflight failed",
            "reconstruction resource preflight failed",
            "synthetic infill plan exceeds bounded artifact size",
        )
    )


def _period_shards(
    start_period: str,
    end_period: str,
    *,
    periods_per_shard: int,
) -> tuple[tuple[str, str], ...]:
    start = _period(start_period)
    end = _period(end_period)
    if start > end:
        raise ReconstructionUnsupportedError(
            "plan-set start_period follows end_period"
        )
    periods: list[str] = []
    current = start
    while current <= end:
        periods.append(current)
        current = _next_period(current)
    return tuple(
        (selected[0], selected[-1])
        for offset in range(0, len(periods), periods_per_shard)
        for selected in (periods[offset : offset + periods_per_shard],)
    )


def _build_reconstruction_plan_support_map(
    plan_set: ReconstructionPlanSetV1,
) -> ReconstructionPlanSupportMapV1:
    """Reconcile actual task/refusal rectangles into one temporal support map."""
    support_windows: list[ReconstructionPlanSupportWindowV1] = []
    selected_engine_ids: tuple[str, ...] | None = None
    for shard in plan_set.shards:
        plan = read_synthetic_infill_plan(shard.plan_ref.path)
        if (
            plan.plan_id != shard.plan_id
            or plan.requested_start_ns != shard.requested_start_ns
            or plan.requested_end_ns != shard.requested_end_ns
        ):
            raise ReconstructionPlanError("support-map shard content differs")
        configuration = read_reconstruction_plan_configuration(
            plan.artifact_graph["configuration"].path
        )
        shard_engine_ids = (
            tuple(configuration.proposal_portfolio.selected_engine_ids)
            if isinstance(configuration, ReconstructionPlanConfigurationV2)
            else ()
        )
        if selected_engine_ids is None:
            selected_engine_ids = shard_engine_ids
        elif selected_engine_ids != shard_engine_ids:
            raise ReconstructionPlanError(
                "support-map shards select different proposal engines"
            )
        if isinstance(
            plan_set.source_spec, ReconstructionPlanSpecV2
        ) and shard_engine_ids != tuple(
            plan_set.source_spec.selected_proposal_engine_ids
        ):
            raise ReconstructionPlanError(
                "support-map engine selection differs from its v2 source spec"
            )

        task_entries: dict[
            tuple[int, int], tuple[dict[str, JSONValue], set[str]]
        ] = {}
        for request in plan.workflow_requests:
            for task in request.tasks:
                window = task.window
                if tuple(window.symbols) != RECONSTRUCTION_SYMBOLS:
                    raise ReconstructionPlanError(
                        "support-map task symbol scope differs"
                    )
                boundary = (window.core_start_ns, window.core_end_ns)
                estimate = {
                    str(key): value
                    for key, value in task.resource_estimate.to_dict().items()
                }
                existing = task_entries.get(boundary)
                if existing is None:
                    task_entries[boundary] = (
                        estimate,
                        {window.ensemble_member_id},
                    )
                else:
                    existing_estimate, member_ids = existing
                    if existing_estimate != estimate:
                        raise ReconstructionPlanError(
                            "support-map member resource estimates differ"
                        )
                    if window.ensemble_member_id in member_ids:
                        raise ReconstructionPlanError(
                            "support-map contains duplicate member work"
                        )
                    member_ids.add(window.ensemble_member_id)

        refusal_entries = {
            (item.start_ns, item.end_ns): item for item in plan.refusals
        }
        source_entries = {
            (item.start_ns, item.end_ns): item for item in plan.source_support
        }
        cftc_entries = {
            (item.start_ns, item.end_ns): item for item in plan.cftc_support
        }
        empty_entries = {
            boundary: item
            for boundary, item in source_entries.items()
            if item.status is ReconstructionPlanSourceSupportStatus.EMPTY
        }
        if len(refusal_entries) != len(plan.refusals):
            raise ReconstructionPlanError(
                "support-map contains duplicate refusal intervals"
            )
        if (
            set(task_entries).intersection(refusal_entries)
            or set(task_entries).intersection(empty_entries)
            or set(refusal_entries).intersection(empty_entries)
        ):
            raise ReconstructionPlanError(
                "support-map interval has overlapping terminal outcomes"
            )
        if len(task_entries) != plan.resources.executable_window_count:
            raise ReconstructionPlanError(
                "support-map executable intervals differ from shard resources"
            )
        if len(refusal_entries) != plan.resources.refused_window_count:
            raise ReconstructionPlanError(
                "support-map refusal intervals differ from shard resources"
            )
        if len(empty_entries) != plan.resources.empty_window_count:
            raise ReconstructionPlanError(
                "support-map empty intervals differ from shard resources"
            )
        if cftc_entries and set(cftc_entries) != set(source_entries):
            raise ReconstructionPlanError(
                "support-map CFTC coverage differs from source support"
            )
        valid_member_counts = {plan.resources.retained_member_count}
        if "proposal_engine_portfolio" not in plan.artifact_graph:
            valid_member_counts.add(plan.resources.ensemble_member_count)
        boundaries = set(task_entries).union(refusal_entries, empty_entries)
        if source_entries and boundaries != set(source_entries):
            raise ReconstructionPlanError(
                "support-map terminal outcomes differ from source support"
            )
        for boundary in sorted(boundaries):
            source_support = source_entries.get(boundary)
            source_fields: dict[str, Any] = {}
            if source_support is not None:
                source_fields = {
                    "source_support_id": source_support.support_id,
                    "source_status": source_support.status.value,
                    "core_source_event_counts": (
                        source_support.core_event_counts
                    ),
                    "input_source_event_counts": (
                        source_support.input_event_counts
                    ),
                    "common_exact_core_timestamp_count": (
                        source_support.common_exact_core_timestamp_count
                    ),
                }
                if source_support.cross_series_policy_id:
                    source_fields.update(
                        {
                            "bounded_nearest_core_timestamp_count": (
                                source_support.bounded_nearest_core_timestamp_count
                            ),
                            "bounded_nearest_core_stale_timestamp_count": (
                                source_support.bounded_nearest_core_stale_timestamp_count
                            ),
                            "bounded_nearest_core_maximum_age_ns": (
                                source_support.bounded_nearest_core_maximum_age_ns
                            ),
                            "bounded_nearest_core_p95_age_ns": (
                                source_support.bounded_nearest_core_p95_age_ns
                            ),
                            "selected_cross_series_alignment": (
                                source_support.selected_cross_series_alignment
                            ),
                            "recommended_cross_series_event_time_ns": (
                                source_support.recommended_cross_series_event_time_ns
                            ),
                            "cross_series_policy_id": (
                                source_support.cross_series_policy_id
                            ),
                        }
                    )
            cftc_fields: dict[str, Any] = {}
            cftc_support = cftc_entries.get(boundary)
            if cftc_support is not None:
                if (
                    source_support is None
                    or cftc_support.source_support_id
                    != source_support.support_id
                ):
                    raise ReconstructionPlanError(
                        "support-map CFTC source binding differs"
                    )
                cftc_fields = {
                    "cftc_support_id": cftc_support.support_id,
                    "cftc_query_status": cftc_support.query_status,
                    "cftc_conditioning_mode": (
                        cftc_support.conditioning_mode.value
                    ),
                    "cftc_reason": cftc_support.reason,
                    "cftc_query_id": cftc_support.query_id,
                    "cftc_qualification_id": (cftc_support.qualification_id),
                }
            estimate_and_members = task_entries.get(boundary)
            if estimate_and_members is not None:
                estimate, member_ids = estimate_and_members
                if len(member_ids) not in valid_member_counts:
                    raise ReconstructionPlanError(
                        "support-map executable member rectangle is incomplete"
                    )
                support_windows.append(
                    ReconstructionPlanSupportWindowV1(
                        start_ns=boundary[0],
                        end_ns=boundary[1],
                        symbols=RECONSTRUCTION_SYMBOLS,
                        status="executable",
                        shard_id=shard.shard_id,
                        plan_id=shard.plan_id,
                        member_ids=tuple(member_ids),
                        resource_estimate=estimate,
                        **source_fields,
                        **cftc_fields,
                    )
                )
                continue
            refusal = refusal_entries.get(boundary)
            if refusal is not None:
                support_windows.append(
                    ReconstructionPlanSupportWindowV1(
                        start_ns=refusal.start_ns,
                        end_ns=refusal.end_ns,
                        symbols=refusal.symbols,
                        status="refused",
                        shard_id=shard.shard_id,
                        plan_id=shard.plan_id,
                        refusal_code=refusal.code.value,
                        refusal_reason=refusal.reason,
                        refusal_id=refusal.refusal_id,
                        **source_fields,
                        **cftc_fields,
                    )
                )
                continue
            empty_support = empty_entries[boundary]
            support_windows.append(
                ReconstructionPlanSupportWindowV1(
                    start_ns=empty_support.start_ns,
                    end_ns=empty_support.end_ns,
                    symbols=empty_support.symbols,
                    status="empty",
                    shard_id=shard.shard_id,
                    plan_id=shard.plan_id,
                    empty_code="source_empty_triangle",
                    empty_reason=empty_support.reason,
                    **source_fields,
                    **cftc_fields,
                )
            )

    return ReconstructionPlanSupportMapV1(
        plan_set_id=plan_set.plan_set_id,
        source_spec_schema_version=plan_set.source_spec.schema_version,
        requested_start_ns=plan_set.requested_start_ns,
        requested_end_ns=plan_set.requested_end_ns,
        window_size_ns=plan_set.source_spec.window_size_ns,
        symbols=plan_set.source_spec.symbols,
        selected_proposal_engine_ids=selected_engine_ids or (),
        windows=tuple(support_windows),
        resource_summary=plan_set.resource_summary,
        status=plan_set.status,
    )


def _build_reconstruction_plan_support_map_shard(
    plan_set: ReconstructionPlanSetV1,
    shard: ReconstructionPlanShardV1,
) -> ReconstructionPlanSupportMapV1:
    """Build one bounded map while retaining the parent plan-set identity."""
    shard_spec = replace(
        plan_set.source_spec,
        start_period=shard.start_period,
        end_period=shard.end_period,
        requested_start_ns=shard.requested_start_ns,
        requested_end_ns=shard.requested_end_ns,
    )
    subset = ReconstructionPlanSetV1(
        source_spec=shard_spec,
        shards=(shard,),
        requested_start_ns=shard.requested_start_ns,
        requested_end_ns=shard.requested_end_ns,
        resource_summary=shard.resource_summary,
        status=_terminal_window_status(
            refusal_count=shard.refusal_count,
            empty_window_count=shard.empty_window_count,
        ),
    )
    support_map = _build_reconstruction_plan_support_map(subset)
    return replace(
        support_map,
        plan_set_id=plan_set.plan_set_id,
        support_map_id="",
    )


def _build_reconstruction_campaign_product_index(
    plan_set_path: str | Path,
    support_map_path: str | Path,
    *,
    output_directory: str | Path,
    verify_products: bool,
) -> ArtifactRef:
    """Reconcile planned support with exact committed member products."""
    plan_set_target = Path(plan_set_path).expanduser().resolve()
    support_target = Path(support_map_path).expanduser().resolve()
    plan_set = read_reconstruction_plan_set(plan_set_target)
    plan_set_ref = artifact_ref_for_file(
        plan_set_target,
        kind="reconstruction_plan_set_v1",
        metadata={
            "plan_set_id": plan_set.plan_set_id,
            "shard_count": len(plan_set.shards),
            "status": plan_set.status,
        },
    )
    support_payload = _read_json_mapping(support_target)
    support_schema = str(support_payload.get("schema_version", ""))
    support_maps: tuple[ReconstructionPlanSupportMapV1, ...]
    if support_schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION:
        support_map = read_reconstruction_plan_support_map(support_target)
        support_maps = (support_map,)
        support_artifact_id = support_map.support_map_id
        selected_engine_ids = support_map.selected_proposal_engine_ids
        support_ref = artifact_ref_for_file(
            support_target,
            kind="reconstruction_plan_support_map_v1",
            metadata={
                "support_map_id": support_map.support_map_id,
                "plan_set_id": support_map.plan_set_id,
                "window_count": len(support_map.windows),
                "status": support_map.status,
            },
        )
    elif support_schema == RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION:
        support_index = read_reconstruction_plan_support_map_index(
            support_target
        )
        support_maps = tuple(
            iter_reconstruction_plan_support_maps(support_index)
        )
        support_artifact_id = support_index.support_map_index_id
        selected_engine_ids = support_index.selected_proposal_engine_ids
        support_ref = artifact_ref_for_file(
            support_target,
            kind="reconstruction_plan_support_map_index_v2",
            metadata={
                "support_map_index_id": support_index.support_map_index_id,
                "plan_set_id": support_index.plan_set_id,
                "shard_count": len(support_index.shard_refs),
                "window_count": support_index.window_count,
                "status": support_index.status,
            },
        )
    else:
        raise ReconstructionPlanError(
            "unsupported campaign support artifact schema"
        )
    if any(item.plan_set_id != plan_set.plan_set_id for item in support_maps):
        raise ReconstructionPlanError(
            "campaign support map differs from its plan set"
        )
    support_by_shard: dict[str, list[ReconstructionPlanSupportWindowV1]] = {}
    for support_map in support_maps:
        for window in support_map.windows:
            support_by_shard.setdefault(window.shard_id, []).append(window)
    plans: dict[str, SyntheticInfillPlanV1] = {}
    output_roots: set[str] = set()
    run_ids: set[str] = set()
    source_version_ids: set[str] = set()
    delivery_profile_ids: set[str] = set()
    scientific_ledger_ids: set[str] = set()
    scientific_lineage_by_run: dict[
        str, tuple[ReconstructionScientificLedgerV1, ArtifactRef, str]
    ] = {}
    for shard in plan_set.shards:
        plan = read_synthetic_infill_plan(shard.plan_ref.path)
        if plan.plan_id != shard.plan_id:
            raise ReconstructionPlanError(
                "campaign plan shard content differs from its descriptor"
            )
        plans[shard.plan_id] = plan
        execution = read_reconstruction_plan_execution_manifest(
            plan.artifact_graph["execution_manifest"].path
        )
        output_roots.add(execution.output_root)
        run_ids.add(plan.run.run_id)
        if len(plan.run.source_version_ids) != 1:
            raise ReconstructionPlanError(
                "campaign plan does not bind one observed dataset version"
            )
        source_version_ids.add(plan.run.source_version_ids[0])
        delivery_profile_ids.add("modern-reference:" + plan.configuration_id)
        ledger_ref = plan.artifact_graph.get("scientific_ledger")
        if (
            ledger_ref is None
            or ledger_ref.kind != RECONSTRUCTION_SCIENTIFIC_LEDGER_ARTIFACT_KIND
        ):
            raise ReconstructionPlanError(
                "campaign plan is scientific-ledger-unbound"
            )
        verify_artifact_ref(ledger_ref)
        ledger = read_reconstruction_scientific_ledger(ledger_ref.path)
        experiment = read_reconstruction_experiment(
            plan.artifact_graph["experiment_manifest"].path
        )
        scientific_ledger_ids.add(ledger.ledger_id)
        scientific_lineage_by_run[plan.run.run_id] = (
            ledger,
            ledger_ref,
            experiment.experiment_id,
        )
    if (
        len(source_version_ids) != 1
        or len(delivery_profile_ids) != 1
        or len(scientific_ledger_ids) != 1
    ):
        raise ReconstructionPlanError(
            "campaign plan shards differ in dataset, delivery, or scientific identity"
        )
    observed_dataset_version_id = next(iter(source_version_ids))
    if not observed_dataset_version_id.startswith("dataset-version:sha256:"):
        raise ReconstructionPlanError(
            "campaign source is not a provider-neutral dataset version"
        )
    delivery_profile_id = next(iter(delivery_profile_ids))
    products: dict[
        tuple[str, str, str], tuple[ReconstructionProductManifestV3, Path]
    ] = {}
    for output_root in sorted(output_roots):
        for path in discover_reconstruction_manifests(output_root):
            manifest = (
                verify_reconstruction_publication(path)
                if verify_products
                else load_reconstruction_manifest(path)
            )
            if not isinstance(manifest, ReconstructionProductManifestV3):
                continue
            if manifest.run_id not in run_ids:
                continue
            key = (
                manifest.run_id,
                manifest.window_id,
                manifest.ensemble_member_id,
            )
            previous = products.get(key)
            if (
                previous is not None
                and previous[0].manifest_id != manifest.manifest_id
            ):
                raise ReconstructionPlanError(
                    "campaign contains conflicting committed products"
                )
            products[key] = (manifest, path)
    shard_refs: list[ArtifactRef] = []
    for shard in plan_set.shards:
        plan = plans[shard.plan_id]
        support_windows = tuple(
            sorted(
                support_by_shard.get(shard.shard_id, ()),
                key=lambda item: item.start_ns,
            )
        )
        if (
            not support_windows
            or support_windows[0].start_ns != shard.requested_start_ns
            or support_windows[-1].end_ns != shard.requested_end_ns
        ):
            raise ReconstructionPlanError(
                "campaign support coverage differs from its plan shard"
            )
        task_windows = {
            (
                task.window.core_start_ns,
                task.window.core_end_ns,
                task.window.ensemble_member_id,
            ): task.window
            for request in plan.workflow_requests
            for task in request.tasks
        }
        entries: list[ReconstructionCampaignProductEntryV1] = []
        for support in support_windows:
            if support.status == "executable":
                for member_id in support.member_ids:
                    task_window = task_windows.get(
                        (support.start_ns, support.end_ns, member_id)
                    )
                    if task_window is None:
                        raise ReconstructionPlanError(
                            "campaign support member lacks its execution window"
                        )
                    product = products.get(
                        (plan.run.run_id, task_window.window_id, member_id)
                    )
                    if product is None:
                        entries.append(
                            ReconstructionCampaignProductEntryV1(
                                support_id=support.support_id,
                                plan_id=shard.plan_id,
                                shard_id=shard.shard_id,
                                start_ns=support.start_ns,
                                end_ns=support.end_ns,
                                status="missing_product",
                                ensemble_member_id=member_id,
                                window_id=task_window.window_id,
                                reason_code="missing_committed_product",
                            )
                        )
                        continue
                    manifest, manifest_path = product
                    scientific_ledger, ledger_ref, experiment_id = (
                        scientific_lineage_by_run[manifest.run_id]
                    )
                    if (
                        manifest.delivery_profile_id != delivery_profile_id
                        or manifest.source.source_version_ids
                        != (observed_dataset_version_id,)
                        or tuple(manifest.symbols) != RECONSTRUCTION_SYMBOLS
                        or manifest.quality.final_validation_status != "passed"
                        or manifest.quality.cross_instrument_quality_status
                        != "passed"
                        or manifest.source.experiment_id != experiment_id
                        or manifest.quality.benchmark_evidence.get(
                            "scientific_ledger_id"
                        )
                        != scientific_ledger.ledger_id
                        or ledger_ref.sha256
                        not in manifest.quality.benchmark_artifact_ids
                    ):
                        raise ReconstructionPlanError(
                            "committed campaign product lineage or validation differs"
                        )
                    product_ref = artifact_ref_for_file(
                        manifest_path,
                        kind=RECONSTRUCTION_MANIFEST_ARTIFACT_KIND,
                        metadata={
                            "manifest_id": manifest.manifest_id,
                            "publication_id": manifest.publication_id,
                            "run_id": manifest.run_id,
                            "window_id": manifest.window_id,
                            "ensemble_member_id": manifest.ensemble_member_id,
                            "delivery_profile_id": manifest.delivery_profile_id,
                            "observed_event_count": manifest.observed_event_count,
                            "synthetic_event_count": manifest.synthetic_event_count,
                            "logical_content_sha256": (
                                manifest.replay.logical_content_sha256
                            ),
                        },
                    )
                    entries.append(
                        ReconstructionCampaignProductEntryV1(
                            support_id=support.support_id,
                            plan_id=shard.plan_id,
                            shard_id=shard.shard_id,
                            start_ns=support.start_ns,
                            end_ns=support.end_ns,
                            status="verified_product",
                            ensemble_member_id=member_id,
                            window_id=task_window.window_id,
                            product_ref=product_ref,
                            observed_event_count=manifest.observed_event_count,
                            synthetic_event_count=manifest.synthetic_event_count,
                        )
                    )
            else:
                entries.append(
                    ReconstructionCampaignProductEntryV1(
                        support_id=support.support_id,
                        plan_id=shard.plan_id,
                        shard_id=shard.shard_id,
                        start_ns=support.start_ns,
                        end_ns=support.end_ns,
                        status=support.status,
                        reason_code=(
                            support.empty_code
                            if support.status == "empty"
                            else support.refusal_code
                        ),
                    )
                )
        product_shard = ReconstructionCampaignProductShardV1(
            plan_set_id=plan_set.plan_set_id,
            support_artifact_id=support_artifact_id,
            plan_id=shard.plan_id,
            shard_id=shard.shard_id,
            requested_start_ns=shard.requested_start_ns,
            requested_end_ns=shard.requested_end_ns,
            entries=tuple(entries),
            status=(
                "complete"
                if not any(item.status == "missing_product" for item in entries)
                else "incomplete"
            ),
        )
        shard_refs.append(
            write_reconstruction_campaign_product_shard(
                product_shard,
                Path(output_directory) / "shards",
            )
        )
    index = ReconstructionCampaignProductIndexV1(
        plan_set_ref=plan_set_ref,
        support_map_ref=support_ref,
        observed_dataset_version_id=observed_dataset_version_id,
        selected_proposal_engine_ids=selected_engine_ids,
        delivery_profile_id=delivery_profile_id,
        shard_refs=tuple(shard_refs),
        requested_start_ns=plan_set.requested_start_ns,
        requested_end_ns=plan_set.requested_end_ns,
        status=(
            "complete"
            if not any(
                ref.metadata.get("missing_product_count") for ref in shard_refs
            )
            else "incomplete"
        ),
    )
    ref = write_reconstruction_campaign_product_index(index, output_directory)
    read_reconstruction_campaign_product_index(ref.path)
    return ref


def _publish_reconstruction_campaign_dataset(
    product_index_path: str | Path,
    *,
    output_directory: str | Path,
    dataset_id: str,
) -> ArtifactRef:
    """Bind a complete campaign index into the provider-neutral catalog."""
    index_target = Path(product_index_path).expanduser().resolve()
    index = read_reconstruction_campaign_product_index(index_target)
    if index.status != "complete" or index.missing_product_count:
        raise ReconstructionRefusedError(
            "campaign dataset publication requires every retained product"
        )
    product_index_ref = artifact_ref_for_file(
        index_target,
        kind="reconstruction_campaign_product_index_v1",
        metadata={
            "product_index_id": index.product_index_id,
            "plan_set_id": index.plan_set_id,
            "support_artifact_id": index.support_artifact_id,
            "shard_count": len(index.shard_refs),
            "support_window_count": index.support_window_count,
            "verified_product_count": index.verified_product_count,
            "missing_product_count": index.missing_product_count,
            "empty_window_count": index.empty_window_count,
            "refused_window_count": index.refused_window_count,
            "observed_event_count": index.observed_event_count,
            "synthetic_event_count": index.synthetic_event_count,
            "status": index.status,
        },
    )
    plan_set = read_reconstruction_plan_set(index.plan_set_ref.path)
    first_plan = read_synthetic_infill_plan(plan_set.shards[0].plan_ref.path)
    scientific_ledger_ref = first_plan.artifact_graph.get("scientific_ledger")
    if scientific_ledger_ref is None:
        raise ReconstructionPlanError(
            "campaign dataset cannot publish a scientific-ledger-unbound plan"
        )
    verify_artifact_ref(scientific_ledger_ref)
    scientific_ledger = read_reconstruction_scientific_ledger(
        scientific_ledger_ref.path
    )
    if scientific_ledger != current_histdata_reconstruction_scientific_ledger():
        raise ReconstructionPlanError(
            "campaign dataset scientific ledger differs from installed target"
        )
    for shard in plan_set.shards[1:]:
        shard_plan = read_synthetic_infill_plan(shard.plan_ref.path)
        shard_ledger_ref = shard_plan.artifact_graph.get("scientific_ledger")
        if (
            shard_ledger_ref is None
            or shard_ledger_ref.sha256 != scientific_ledger_ref.sha256
        ):
            raise ReconstructionPlanError(
                "campaign dataset plan shards differ in scientific identity"
            )
        verify_artifact_ref(shard_ledger_ref)
    source_catalog_ref = first_plan.artifact_graph["dataset_catalog"]
    verify_artifact_ref(source_catalog_ref)
    source_catalog = DatasetCatalog.read(source_catalog_ref.path)
    parent = next(
        (
            item
            for item in source_catalog.versions
            if item.dataset_version_id == index.observed_dataset_version_id
        ),
        None,
    )
    if parent is None:
        raise ReconstructionPlanError(
            "campaign observed parent is absent from its source catalog"
        )
    descriptor = DatasetDescriptorV1(
        dataset_id=dataset_id,
        display_name="HistData Triangle Modern-Reference Synthetic",
        description=(
            "Regime-conditioned constrained synthetic infill for the qualified "
            "EURGBP/EURUSD/GBPUSD HistData intersection; not recovered truth "
            "and not broker-conditioned."
        ),
        allowed_origins=(DatasetOrigin.SYNTHETIC,),
    )
    existing_descriptor = next(
        (
            item
            for item in source_catalog.datasets
            if item.dataset_id == descriptor.dataset_id
        ),
        None,
    )
    if existing_descriptor is not None and existing_descriptor != descriptor:
        raise ReconstructionPlanError(
            "campaign synthetic dataset descriptor already differs"
        )
    version = DatasetVersionManifestV1(
        dataset_id=descriptor.dataset_id,
        origin=DatasetOrigin.SYNTHETIC,
        normalization_policy_id="reconstruction-campaign-product-index-v1",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        parents=(
            DatasetParentV1(
                parent_dataset_version_id=parent.dataset_version_id,
                role="immutable-observed-histdata-anchor",
                ordinal=0,
            ),
        ),
        qualification_evidence=(
            scientific_ledger_ref,
            product_index_ref,
            index.plan_set_ref,
            index.support_map_ref,
        ),
        delivery_profile_id=index.delivery_profile_id,
    )
    root = Path(output_directory).expanduser().resolve()
    version_path = root / (
        "dataset-version-"
        + version.dataset_version_id.rsplit(":", 1)[-1]
        + ".json"
    )
    written_version = _write_json(version_path, version.to_dict())
    version_ref = artifact_ref_for_file(
        written_version,
        kind="dataset_version_manifest_v1",
        metadata={
            "dataset_id": version.dataset_id,
            "dataset_version_id": version.dataset_version_id,
            "origin": version.origin.value,
            "qualification_status": version.qualification_status.value,
            "parent_dataset_version_id": parent.dataset_version_id,
            "product_index_id": index.product_index_id,
            "scientific_ledger_id": scientific_ledger.ledger_id,
        },
    )
    existing_version = next(
        (
            item
            for item in source_catalog.versions
            if item.dataset_version_id == version.dataset_version_id
        ),
        None,
    )
    if existing_version is not None and existing_version != version:
        raise ReconstructionPlanError(
            "campaign synthetic dataset version already differs"
        )
    catalog = DatasetCatalog(
        providers=source_catalog.providers,
        adapters=source_catalog.adapters,
        datasets=(
            source_catalog.datasets
            if existing_descriptor is not None
            else (*source_catalog.datasets, descriptor)
        ),
        versions=(
            source_catalog.versions
            if existing_version is not None
            else (*source_catalog.versions, version)
        ),
        aliases=source_catalog.aliases,
    )
    catalog_path = catalog.write(
        root
        / ("dataset-catalog-" + catalog.catalog_id.rsplit(":", 1)[-1] + ".json")
    )
    catalog_ref = artifact_ref_for_file(
        catalog_path,
        kind="dataset_catalog_v1",
        metadata={
            "catalog_id": catalog.catalog_id,
            "dataset_id": version.dataset_id,
            "dataset_version_id": version.dataset_version_id,
        },
    )
    publication = ReconstructionCampaignDatasetPublicationV1(
        product_index_ref=product_index_ref,
        dataset_version_ref=version_ref,
        catalog_ref=catalog_ref,
        observed_parent_dataset_version_id=parent.dataset_version_id,
        synthetic_dataset_version_id=version.dataset_version_id,
    )
    ref = write_reconstruction_campaign_dataset_publication(publication, root)
    read_reconstruction_campaign_dataset_publication(ref.path)
    return ref


def _accumulate_plan_set_resources(
    plan: SyntheticInfillPlanV1,
    *,
    resource_summaries: list[ReconstructionPlanResourceSummaryV1],
    source_partitions: dict[str, tuple[str, str, int, int]],
) -> None:
    """Retain only compact shard resources and unique source identities."""
    inventory = read_reconstruction_source_inventory(
        plan.artifact_graph["source_inventory"].path
    )
    for partition in inventory.partitions:
        identity = (
            partition.period,
            partition.symbol,
            partition.row_count,
            cast(int, partition.artifact.size_bytes),
        )
        existing = source_partitions.setdefault(
            partition.partition_id, identity
        )
        if existing != identity:
            raise ReconstructionPlanError(
                "plan-set source partition identity is inconsistent"
            )
    resource_summaries.append(plan.resources)


def _aggregate_plan_set_resources(
    resources: Sequence[ReconstructionPlanResourceSummaryV1],
    source_partitions: Mapping[str, tuple[str, str, int, int]],
) -> dict[str, JSONValue]:
    if not resources:
        raise ReconstructionPlanError("cannot aggregate an empty plan set")
    input_events = sum(item.estimated_input_event_count for item in resources)
    candidate_events = sum(
        item.estimated_candidate_event_count for item in resources
    )
    payload: dict[str, JSONValue] = {
        "schema_version": "histdatacom.reconstruction-plan-set-resources.v1",
        "plan_shard_count": len(resources),
        "source_partition_count": len(source_partitions),
        "source_event_count": sum(
            item[2] for item in source_partitions.values()
        ),
        "source_size_bytes": sum(
            item[3] for item in source_partitions.values()
        ),
        "planned_window_count": sum(
            item.planned_window_count for item in resources
        ),
        "executable_window_count": sum(
            item.executable_window_count for item in resources
        ),
        "refused_window_count": sum(
            item.refused_window_count for item in resources
        ),
        "ensemble_member_count": max(
            item.ensemble_member_count for item in resources
        ),
        "retained_member_count": max(
            item.retained_member_count for item in resources
        ),
        "workflow_request_count": sum(
            item.workflow_request_count for item in resources
        ),
        "estimated_input_event_count": input_events,
        "estimated_candidate_event_count": candidate_events,
        "estimated_candidate_bytes": sum(
            item.estimated_candidate_bytes for item in resources
        ),
        "estimated_peak_memory_bytes": max(
            item.estimated_peak_memory_bytes for item in resources
        ),
        "estimated_peak_scratch_bytes": max(
            item.estimated_peak_scratch_bytes for item in resources
        ),
        "estimated_output_bytes": sum(
            item.estimated_output_bytes for item in resources
        ),
        "estimated_partition_count": sum(
            item.estimated_partition_count for item in resources
        ),
        "candidate_amplification": (
            candidate_events / input_events if input_events else 0.0
        ),
        "output_basis": "sharded-sum-of-retained-member-compressed-upper-bound-v1",
        "scratch_basis": "maximum-shard-peak-concurrent-window-scratch-v1",
    }
    empty_window_count = sum(item.empty_window_count for item in resources)
    if empty_window_count:
        payload["empty_window_count"] = empty_window_count
    payload["summary_id"] = _stable_id(
        "reconstruction-plan-set-resources", payload
    )
    return payload


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _read_json_mapping(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as err:
        raise ReconstructionPlanError(f"cannot read {target}: {err}") from err
    if not isinstance(payload, Mapping):
        raise ReconstructionPlanError(f"JSON root must be an object: {target}")
    return dict(payload)


def _write_json(path: str | Path, payload: Mapping[str, JSONValue]) -> Path:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.partial")
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    temporary.write_bytes(encoded)
    temporary.replace(target)
    return target


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReconstructionPlanError("expected a JSON object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ReconstructionPlanError("expected a JSON array")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ReconstructionPlanError(f"{name} must be a JSON boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReconstructionPlanError(f"{name} must be a JSON integer")
    return value


def _support_source_counts(
    value: Mapping[str, JSONValue],
    *,
    symbols: tuple[str, ...],
    name: str,
) -> dict[str, int]:
    counts = {
        str(key): _strict_int(item, f"{name}[{key}]")
        for key, item in value.items()
    }
    if set(counts) != set(symbols) or any(item < 0 for item in counts.values()):
        raise ReconstructionPlanError(
            f"{name} does not contain nonnegative complete-triangle counts"
        )
    return {symbol: counts[symbol] for symbol in symbols}


def _terminal_window_status(
    *, refusal_count: int, empty_window_count: int
) -> str:
    if refusal_count and empty_window_count:
        return "ready_with_refusals_and_empty_windows"
    if refusal_count:
        return "ready_with_refusals"
    if empty_window_count:
        return "ready_with_empty_windows"
    return "ready"


def _aggregate_plan_set_operation_status(statuses: Sequence[str]) -> str:
    """Collapse complete per-shard states without hiding partial failures."""
    selected = tuple(_required_text(value) for value in statuses)
    if not selected:
        raise ReconstructionPlanError(
            "plan-set operation has no shard statuses"
        )
    unique = set(selected)
    if len(unique) == 1:
        return selected[0]
    if unique.issubset({"committed", "completed"}):
        return "completed"
    if unique.issubset({"cancelled", "cancellation_requested"}):
        return (
            "cancelled" if unique == {"cancelled"} else "cancellation_requested"
        )
    if unique & {"failed", "partial", "refused"}:
        return "partial"
    if unique & {"running", "submitted"}:
        return "running"
    return "partial"


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ReconstructionPlanError("required reconstruction text is empty")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "DEFAULT_PLAN_SET_PERIODS_PER_SHARD",
    "DEFAULT_PREVIEW_LIMIT",
    "MAX_MONOLITHIC_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS",
    "MAX_PREVIEW_LIMIT",
    "MAX_RECONSTRUCTION_PLAN_SHARDS",
    "MAX_RECONSTRUCTION_PLAN_SUPPORT_MAP_BYTES",
    "MAX_RECONSTRUCTION_PLAN_SUPPORT_WINDOWS",
    "RECONSTRUCTION_CAMPAIGN_DATASET_PUBLICATION_SCHEMA_VERSION",
    "RECONSTRUCTION_CAMPAIGN_PRODUCT_ENTRY_SCHEMA_VERSION",
    "RECONSTRUCTION_CAMPAIGN_PRODUCT_INDEX_SCHEMA_VERSION",
    "RECONSTRUCTION_CAMPAIGN_PRODUCT_SHARD_SCHEMA_VERSION",
    "RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SET_EXECUTION_REQUEST_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SET_PREFLIGHT_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SET_RECEIPT_INDEX_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SET_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SHARD_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SPEC_V2_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SUPPORT_MAP_INDEX_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SUPPORT_MAP_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SUPPORT_WINDOW_SCHEMA_VERSION",
    "RECONSTRUCTION_PREVIEW_SCHEMA_VERSION",
    "RECONSTRUCTION_RECEIPT_SCHEMA_VERSION",
    "RECONSTRUCTION_REPLAY_SCHEMA_VERSION",
    "RECONSTRUCTION_SOURCE_FORMAT",
    "RECONSTRUCTION_SYMBOLS",
    "RECONSTRUCTION_TIMEFRAME",
    "CrossSeriesConstraintPolicyV1",
    "DiagnosticPublicationManifestV1",
    "DiagnosticPublicationSpecV1",
    "InformationMode",
    "ModernReferenceCertificationCampaignResultV1",
    "ModernReferenceCertificationCampaignSpecV1",
    "ReconstructionCampaignDatasetPublicationV1",
    "ReconstructionCampaignProductEntryV1",
    "ReconstructionCampaignProductIndexV1",
    "ReconstructionCampaignProductShardV1",
    "ReconstructionCertificationDossierV2",
    "ReconstructionClient",
    "ReconstructionCompatibilityReportV1",
    "ReconstructionCompatibilityStatus",
    "ReconstructionEvidencePolicyV1",
    "ReconstructionExecutionRequestV1",
    "ReconstructionExitCode",
    "ReconstructionOperationReceiptV1",
    "ReconstructionPlanError",
    "ReconstructionPlanSetExecutionRequestV1",
    "ReconstructionPlanSetPreflightV1",
    "ReconstructionPlanSetReceiptIndexV1",
    "ReconstructionPlanSetV1",
    "ReconstructionPlanShardV1",
    "ReconstructionPlanSpecV1",
    "ReconstructionPlanSpecV2",
    "ReconstructionPlanSupportMapIndexV2",
    "ReconstructionPlanSupportMapV1",
    "ReconstructionPlanSupportWindowV1",
    "ReconstructionPreflightV1",
    "ReconstructionPublicError",
    "ReconstructionRefusedError",
    "ReconstructionSchemaRegistryV1",
    "ReconstructionScientificLedgerV1",
    "ReconstructionUnsupportedError",
    "ReconstructionValidationError",
    "current_histdata_reconstruction_scientific_ledger",
    "iter_reconstruction_plan_support_maps",
    "read_execution_request",
    "read_modern_reference_certification_campaign_spec",
    "read_operation_receipt",
    "read_plan_spec",
    "read_reconstruction_campaign_dataset_publication",
    "read_reconstruction_campaign_product_index",
    "read_reconstruction_campaign_product_shard",
    "read_reconstruction_plan_set",
    "read_reconstruction_plan_set_execution_request",
    "read_reconstruction_plan_set_receipt_index",
    "read_reconstruction_plan_support_map",
    "read_reconstruction_plan_support_map_index",
    "read_reconstruction_scientific_ledger",
    "reconstruction_exit_code",
    "run_modern_reference_certification_campaign",
    "write_execution_request",
    "write_operation_receipt",
    "write_reconstruction_campaign_dataset_publication",
    "write_reconstruction_campaign_product_index",
    "write_reconstruction_campaign_product_shard",
    "write_reconstruction_plan_set",
    "write_reconstruction_plan_set_execution_request",
    "write_reconstruction_plan_set_receipt_index",
    "write_reconstruction_plan_support_map",
    "write_reconstruction_plan_support_map_index",
]
