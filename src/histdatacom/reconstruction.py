"""Typed public facade for first-party reconstruction operations.

The facade keeps operator intent, scientific plan identity, orchestration
control, and product inspection at one supported import boundary.  Tick rows
remain in Arrow/Parquet artifacts; public requests and receipts carry only
bounded metadata and strong references.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import IntEnum
import hashlib
import json
from pathlib import Path
from typing import Any, cast

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
    reconcile_reconstruction_report,
    run_reconstruction_request,
    write_reconstruction_report,
)
from histdatacom.orchestration.supervisor import OrchestrationSupervisor
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    discover_reconstruction_manifests,
    iter_reconstruction_event_batches,
    load_reconstruction_manifest,
    read_reconstruction_streams,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.reconstruction_handlers import (
    register_first_party_reconstruction_handlers,
)
from histdatacom.synthetic.reconstruction_plan import (
    SCIENTIFIC_NONCLAIM,
    ReconstructionDeliveryMode,
    SyntheticInfillPlanV1,
    build_synthetic_infill_plan,
    read_reconstruction_plan_execution_manifest,
    read_synthetic_infill_plan,
    validate_synthetic_infill_plan_for_execution,
    write_synthetic_infill_plan,
)

RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-spec.v1"
)
RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-execution-request.v1"
)
RECONSTRUCTION_PREFLIGHT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-preflight.v1"
)
RECONSTRUCTION_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-operation-receipt.v1"
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

    source_root: str
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
    start_period: str | None = None
    end_period: str | None = None
    delivery_mode: ReconstructionDeliveryMode = (
        ReconstructionDeliveryMode.MODERN_REFERENCE
    )
    broker_delivery_artifact: ArtifactRef | None = None
    source_format: str = RECONSTRUCTION_SOURCE_FORMAT
    timeframe: str = RECONSTRUCTION_TIMEFRAME
    symbols: tuple[str, ...] = RECONSTRUCTION_SYMBOLS
    schema_version: str = RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION:
            raise ReconstructionUnsupportedError(
                "unsupported reconstruction plan-spec schema"
            )
        for name in (
            "source_root",
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
        _validate_public_input_contract(
            source_format=self.source_format,
            timeframe=self.timeframe,
            symbols=self.symbols,
        )
        object.__setattr__(self, "source_format", RECONSTRUCTION_SOURCE_FORMAT)
        object.__setattr__(self, "timeframe", RECONSTRUCTION_TIMEFRAME)
        object.__setattr__(self, "symbols", RECONSTRUCTION_SYMBOLS)
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
        return {
            "schema_version": self.schema_version,
            "source_root": self.source_root,
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
            "delivery_mode": self.delivery_mode.value,
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

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionPlanSpecV1":
        """Restore a strict public plan specification."""
        broker_payload = data.get("broker_delivery_artifact")
        broker_ref = (
            ArtifactRef.from_dict(_mapping(broker_payload))
            if broker_payload is not None
            else None
        )
        return cls(
            source_root=str(data.get("source_root", "")),
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
            start_period=_optional_text(data.get("start_period")),
            end_period=_optional_text(data.get("end_period")),
            delivery_mode=ReconstructionDeliveryMode.from_value(
                str(data.get("delivery_mode", "modern_reference"))
            ),
            broker_delivery_artifact=broker_ref,
            source_format=str(data.get("source_format", "ascii")),
            timeframe=str(data.get("timeframe", "T")),
            symbols=tuple(
                str(value)
                for value in _sequence(
                    data.get("symbols", RECONSTRUCTION_SYMBOLS)
                )
            ),
            schema_version=str(data.get("schema_version", "")),
        )


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
    ) -> "ReconstructionExecutionRequestV1":
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
    ) -> "ReconstructionOperationReceiptV1":
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

    def construct_plan(self, spec: ReconstructionPlanSpecV1) -> ArtifactRef:
        """Build, execution-validate, and persist one content-addressed plan."""
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
                information_mode=spec.information_mode,
                delivery_mode=spec.delivery_mode,
                broker_delivery_artifact=spec.broker_delivery_artifact,
            )
            validate_synthetic_infill_plan_for_execution(plan)
            return write_synthetic_infill_plan(plan, spec.artifact_root)
        except ReconstructionPublicError:
            raise
        except (OSError, TypeError, ValueError) as err:
            raise ReconstructionPlanError(str(err)) from err

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
        status = "ready"
        if refusals:
            status = "ready_with_refusals" if executable else "refused"
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
                    "qualification",
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
        }

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


def read_plan_spec(path: str | Path) -> ReconstructionPlanSpecV1:
    """Read a public plan-spec JSON artifact."""
    return ReconstructionPlanSpecV1.from_dict(_read_json_mapping(path))


def write_execution_request(
    request: ReconstructionExecutionRequestV1, path: str | Path
) -> Path:
    """Atomically write operator request metadata."""
    return _write_json(path, request.to_dict())


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


def reconstruction_exit_code(
    result: ReconstructionPreflightV1 | ReconstructionOperationReceiptV1,
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
            f"{request.run.run_id}|{request.request_fingerprint}|"
            f"{execution_attempt_id}"
        ).encode("utf-8")
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
    "DEFAULT_PREVIEW_LIMIT",
    "MAX_PREVIEW_LIMIT",
    "InformationMode",
    "RECONSTRUCTION_EXECUTION_REQUEST_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_SPEC_SCHEMA_VERSION",
    "RECONSTRUCTION_PREVIEW_SCHEMA_VERSION",
    "RECONSTRUCTION_RECEIPT_SCHEMA_VERSION",
    "RECONSTRUCTION_REPLAY_SCHEMA_VERSION",
    "RECONSTRUCTION_SOURCE_FORMAT",
    "RECONSTRUCTION_SYMBOLS",
    "RECONSTRUCTION_TIMEFRAME",
    "ReconstructionClient",
    "ReconstructionExecutionRequestV1",
    "ReconstructionExitCode",
    "ReconstructionOperationReceiptV1",
    "ReconstructionPlanError",
    "ReconstructionPlanSpecV1",
    "ReconstructionPreflightV1",
    "ReconstructionPublicError",
    "ReconstructionRefusedError",
    "ReconstructionUnsupportedError",
    "ReconstructionValidationError",
    "read_execution_request",
    "read_operation_receipt",
    "read_plan_spec",
    "reconstruction_exit_code",
    "write_execution_request",
    "write_operation_receipt",
]
