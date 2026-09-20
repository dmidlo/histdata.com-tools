"""Bounded diagnostic attempts; no calibration, losses, or implicit real runs.

The coordinator gate is a workflow boundary, not a same-user security sandbox.
Only newly generated synthetic inputs are used to qualify this implementation.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import ClassVar, cast

import polars as pl

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
)
from histdatacom.datasets.contracts import (
    DatasetQualificationStatus,
    DatasetVersionManifestV1,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file

from .training_contracts import (
    TrainingContract,
    TrainingOwnershipV1,
    TrainingSourceV1,
    training_json,
    training_load,
)
from .training_lineage import (
    _ownership,
    read_training_regular,
    verify_training_source,
)
from .training_weight_approval import training_weight_source_fingerprint
from .training_weight_artifacts import _publish as _atomic_publish
from .training_weight_artifacts import _sync
from .training_weight_campaign import _supervise_campaign
from .training_weight_candidates import (
    TrainingWeightFileV1,
    TrainingWeightModelV1,
    TrainingWeightOperationalFailure,
    _supervise_day_worker,
)
from .training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticApprovalV1,
    TrainingWeightDiagnosticEvidenceKind,
    TrainingWeightDiagnosticInputRole,
    TrainingWeightDiagnosticPlanV1,
    diagnostic_json,
    diagnostic_load,
    read_training_weight_diagnostic_protocol,
    require_training_weight_diagnostic_approval,
)
from .training_weight_lineage import (
    TrainingWeightDayBridgeV1,
    TrainingWeightRefusedDayV1,
    TrainingWeightSourcePlanV1,
    WeightEvidenceKind,
    _create_training_weight_degradation_from_verified,
)

MAX_ATTEMPT_SECONDS = 10800.0
MAX_MONTH_SECONDS = 300.0
MAX_DAY_SECONDS = 120.0
MAX_COMPONENTS = 192
MAX_COMPONENT_BYTES = 8 * 1024 * 1024
MAX_TOTAL_COMPONENT_BYTES = 128 * 1024 * 1024
PERIODS = ("201001", "201101", "201102")
SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
MEMBERS = tuple(f"member-{seed}" for seed in range(60701, 60705))
_OPERATIONS = (
    "diagnostic-input-verification",
    "diagnostic-source-census",
    "diagnostic-generator-trace",
)


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticRuntimeV1(TrainingContract):
    KIND: ClassVar[str] = "weight-diagnostic-runtime"
    plan_id: str
    source_fingerprint: str

    def _validate(self) -> None:
        _identity(self.plan_id)
        if re.fullmatch(r"[0-9a-f]{64}", self.source_fingerprint) is None:
            raise ValueError("invalid diagnostic implementation fingerprint")


def _canonical_artifact(
    path: Path, cls: type[TrainingContract]
) -> TrainingContract:
    data = read_training_regular(path, MAX_COMPONENT_BYTES)
    value = cls.from_json(data.decode("ascii"))
    if value.to_json().encode("ascii") != data:
        raise ValueError("diagnostic artifact bytes are not canonical")
    return value


def _runtime(
    root: Path, plan: TrainingWeightDiagnosticPlanV1, *, executing: bool = False
) -> TrainingWeightDiagnosticRuntimeV1:
    value = _canonical_artifact(
        root / "runtime.json", TrainingWeightDiagnosticRuntimeV1
    )
    assert isinstance(value, TrainingWeightDiagnosticRuntimeV1)
    if value.plan_id != plan.artifact_id or (
        executing
        and value.source_fingerprint != training_weight_source_fingerprint()
    ):
        raise ValueError("diagnostic runtime binding differs")
    return value


def _input_paths(plan: TrainingWeightDiagnosticPlanV1) -> None:
    """Resolve declared locators before reads; no filesystem discovery.

    Fixture namespaces must survive resolution unchanged. This rejects ancestor
    alias escapes, not a hostile same-user race during later filesystem access.
    """
    for ref in plan.inputs:
        path = Path(ref.file.path)
        if path.is_symlink() or path.resolve() != path:
            raise ValueError("diagnostic input locator has a symlink alias")
        if (
            plan.evidence_kind is TrainingWeightDiagnosticEvidenceKind.FIXTURE
            and "training-weight-diagnostic-fixture" not in path.resolve().parts
        ):
            raise ValueError("diagnostic fixture locator escapes its namespace")


def _identity(value: str) -> None:
    if re.fullmatch(r"[a-z0-9-]+:sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("invalid diagnostic artifact identity")


@lru_cache(maxsize=1)
def _dates() -> tuple[str, ...]:
    return read_training_weight_diagnostic_protocol().scheduled_dates()


@lru_cache(maxsize=1)
def _selected_dates() -> tuple[str, ...]:
    return read_training_weight_diagnostic_protocol().selected_dates()


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticComponentV1(TrainingContract):
    KIND: ClassVar[str] = "weight-diagnostic-component"
    kind: str
    component_id: str
    file: TrainingWeightFileV1

    def _validate(self) -> None:
        if self.kind not in ("source-shard", "bridge", "month", "model", "day"):
            raise ValueError("unknown diagnostic component kind")
        _identity(self.component_id)
        if (
            self.file.path != f"diagnostic-{self.kind}-{self.file.sha256}.json"
            or self.file.size_bytes > MAX_COMPONENT_BYTES
        ):
            raise ValueError("invalid diagnostic component locator or size")


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticMonthV1(TrainingContract):
    KIND: ClassVar[str] = "weight-diagnostic-month"
    plan_id: str
    source_plan: TrainingWeightSourcePlanV1
    ownership: TrainingOwnershipV1
    source_report_id: str
    source_shards: tuple[TrainingWeightDiagnosticComponentV1, ...]
    subset_source: TrainingSourceV1 | None
    subset_ownership: TrainingOwnershipV1 | None
    bridges: tuple[TrainingWeightDiagnosticComponentV1, ...]
    refusals: tuple[TrainingWeightRefusedDayV1, ...]

    def _validate(self) -> None:
        _identity(self.plan_id)
        _identity(self.source_report_id)
        if self.ownership.dataset_version_id != (
            self.source_plan.parent_source.dataset_version_id
        ):
            raise ValueError("diagnostic month ownership differs")
        if len(self.source_shards) != 3 or any(
            item.kind != "source-shard" for item in self.source_shards
        ):
            raise ValueError("diagnostic month requires three source shards")
        if any(item.kind != "bridge" for item in self.bridges):
            raise ValueError("diagnostic month has non-bridge reference")
        if (
            len(self.bridges) + len(self.refusals)
            != len(self.source_plan.dates)
            or (self.subset_source is None) != (not self.bridges)
            or (self.subset_ownership is None) != (not self.bridges)
        ):
            raise ValueError("diagnostic month has incomplete day inventory")
        if len({r.utc_date for r in self.refusals}) != len(self.refusals):
            raise ValueError("diagnostic month repeats a refused date")
        diagnostic_load(self.to_json())


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticCellInventoryV1(TrainingContract):
    KIND: ClassVar[str] = "weight-diagnostic-cell-inventory"
    stage: str
    utc_date: str
    member_id: str
    symbol: str
    status: str
    reason: str
    component_id: str | None

    def _validate(self) -> None:
        if (
            self.stage not in ("source", "generator")
            or self.symbol not in SYMBOLS
        ):
            raise ValueError("invalid diagnostic cell coordinate")
        if (
            self.utc_date not in _dates()
            or (self.stage == "source" and self.member_id != "")
            or (
                self.stage == "generator"
                and (
                    self.utc_date not in _selected_dates()
                    or self.member_id not in MEMBERS
                )
            )
        ):
            raise ValueError("diagnostic cell is outside frozen inventory")
        if self.status not in (
            "evaluated",
            "deterministic_refused",
            "not_evaluable",
            "diagnostic_failed",
            "not_attempted",
            "attempted_result_unavailable",
        ):
            raise ValueError("unknown diagnostic cell status")
        if len(self.reason) > 256 or any(ord(c) < 32 for c in self.reason):
            raise ValueError("diagnostic reason is not bounded plain text")
        if self.component_id is not None:
            _identity(self.component_id)
        if (
            self.status
            in (
                "evaluated",
                "deterministic_refused",
                "not_evaluable",
            )
            and self.component_id is None
        ):
            raise ValueError(
                "evaluated diagnostic cell needs retained evidence"
            )


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticAttemptV1(TrainingContract):
    KIND: ClassVar[str] = "weight-diagnostic-attempt"
    plan_id: str
    evidence_kind: TrainingWeightDiagnosticEvidenceKind
    status: str
    reason: str
    elapsed_nanoseconds: int
    worker_pid: int | None
    last_stage: str
    inventory: tuple[TrainingWeightDiagnosticCellInventoryV1, ...]
    components: tuple[TrainingWeightDiagnosticComponentV1, ...]
    day_selection_json: str
    runtime_id: str

    def _validate(self) -> None:
        _identity(self.plan_id)
        _identity(self.runtime_id)
        if self.status not in ("started", "completed", "failed"):
            raise ValueError("unknown diagnostic attempt status")
        reasons = {
            "pending",
            "diagnostics_completed_not_qualification",
            "attempt_deadline",
            "attempt_cancelled",
            "source_month_deadline",
            "generator_day_deadline",
            "worker_failed",
            "worker_start_failed",
            "worker_terminal_missing",
            "source_reconciliation_failed",
            "diagnostic_capacity_failed",
        }
        if (
            self.reason not in reasons
            or ((self.status == "started") != (self.reason == "pending"))
            or (
                (self.status == "completed")
                != (self.reason == "diagnostics_completed_not_qualification")
            )
        ):
            raise ValueError("contradictory diagnostic outcome")
        if not 0 <= self.elapsed_nanoseconds < 2**63 or (
            self.worker_pid is not None and not 0 < self.worker_pid < 2**31
        ):
            raise ValueError("invalid diagnostic process clock/PID")
        if self.last_stage not in (
            "preflight",
            "model",
            "source",
            "reconciliation",
            "generator",
            "completed",
        ):
            raise ValueError("unknown diagnostic stage")
        expected = tuple(
            ("source", d, "", s) for d in _dates() for s in SYMBOLS
        ) + tuple(
            ("generator", d, m, s)
            for d in _selected_dates()
            for m in MEMBERS
            for s in SYMBOLS
        )
        if (
            tuple(
                (c.stage, c.utc_date, c.member_id, c.symbol)
                for c in self.inventory
            )
            != expected
        ):
            raise ValueError(
                "diagnostic attempt must account for all 186/48 cells"
            )
        if (
            len(self.components) > MAX_COMPONENTS
            or sum(c.file.size_bytes for c in self.components)
            > MAX_TOTAL_COMPONENT_BYTES
            or len({c.file.path for c in self.components})
            != len(self.components)
        ):
            raise ValueError("diagnostic component inventory exceeds bounds")
        ids = {c.component_id for c in self.components}
        if any(
            c.component_id is not None and c.component_id not in ids
            for c in self.inventory
        ):
            raise ValueError("diagnostic cell references missing evidence")
        selections = diagnostic_load(self.day_selection_json)
        if set(selections) != set(_dates()) or any(
            v
            not in (
                "pending",
                "source_refused",
                "selected",
                "not_selected_by_diagnostic_design",
            )
            for v in selections.values()
        ):
            raise ValueError("diagnostic day selection inventory differs")
        if self.status == "completed" and (
            self.last_stage != "completed"
            or any(
                c.status
                in (
                    "diagnostic_failed",
                    "attempted_result_unavailable",
                    "not_attempted",
                )
                for c in self.inventory
            )
            or "pending" in selections.values()
        ):
            raise ValueError("incomplete diagnostics cannot claim completion")
        diagnostic_load(self.to_json())


def _publish(root: Path, name: str, text: str) -> None:
    diagnostic_load(text)
    path = root / name
    if path.exists() or path.is_symlink():
        raise FileExistsError(path)
    _atomic_publish(path, text.encode("ascii"))


def _component(
    root: Path, kind: str, value: TrainingContract
) -> TrainingWeightDiagnosticComponentV1:
    text = value.to_json()
    diagnostic_load(text)
    digest = hashlib.sha256(text.encode("ascii")).hexdigest()
    name = f"diagnostic-{kind}-{digest}.json"
    _publish(root, name, text)
    return TrainingWeightDiagnosticComponentV1(
        kind, value.artifact_id, TrainingWeightFileV1(name, len(text), digest)
    )


def _read_component(
    root: Path, component: TrainingWeightDiagnosticComponentV1
) -> str:
    data = read_training_regular(
        root / component.file.path, component.file.size_bytes
    )
    if (
        len(data) != component.file.size_bytes
        or hashlib.sha256(data).hexdigest() != component.file.sha256
    ):
        raise ValueError("diagnostic component bytes differ")
    text = data.decode("ascii")
    if diagnostic_json(diagnostic_load(text)) != text:
        raise ValueError("diagnostic component is not canonical")
    return text


def _checkpoint(
    root: Path, stage: str, day: str = "", member: str = "", symbol: str = ""
) -> None:
    name = f"progress-{stage}-{day or 'none'}-{member or 'none'}-{symbol or 'none'}.json"
    _publish(
        root,
        name,
        diagnostic_json(
            {
                "stage": stage,
                "utc_date": day,
                "member_id": member,
                "symbol": symbol,
            }
        ),
    )


def _legacy_kind(plan: TrainingWeightDiagnosticPlanV1) -> WeightEvidenceKind:
    return (
        WeightEvidenceKind.FIXTURE
        if plan.evidence_kind is TrainingWeightDiagnosticEvidenceKind.FIXTURE
        else WeightEvidenceKind.PREREGISTERED
    )


def _gate(
    plan: TrainingWeightDiagnosticPlanV1,
    approval: TrainingWeightDiagnosticApprovalV1 | None,
    operation: str,
) -> None:
    require_training_weight_diagnostic_approval(approval, operation, plan)


def _source_plan(
    root: Path, plan: TrainingWeightDiagnosticPlanV1, period: str
) -> TrainingWeightSourcePlanV1:
    """Read only exact approved paths, with no corpus discovery or normalization."""
    refs = tuple(
        r
        for r in plan.inputs
        if r.role is TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
        and r.period == period
    )
    adapter = HistDataProviderAdapter()
    partitions = []
    for ref in refs:
        payload = ref.file.verify()
        frame = pl.read_ipc(io.BytesIO(payload), memory_map=False)
        if (
            frame.schema
            != {
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            }
            or frame.height != ref.row_count
            or frame.height > 2_000_000
        ):
            raise ValueError(
                "diagnostic raw schema/count differs from input binding"
            )
        if (
            any(frame.null_count().row(0))
            or frame.filter(
                (pl.col("datetime") < 0)
                | (~pl.col("bid").is_finite())
                | (~pl.col("ask").is_finite())
                | (pl.col("bid") <= 0)
                | (pl.col("ask") <= 0)
            ).height
        ):
            raise ValueError(
                "diagnostic raw integrity failed; no quote/order repair"
            )
        path = Path(ref.file.path)
        relative = Path(ref.relative_path)
        if path.parts[-len(relative.parts) :] != relative.parts:
            raise ValueError(
                "diagnostic source locator does not match canonical slot"
            )
        base = path
        for _ in relative.parts:
            base = base.parent
        partition = adapter.inspect_partition(
            base,
            symbol=ref.symbol,
            period=period,
            expected_sha256=ref.file.sha256,
        )
        if (
            partition.row_count != ref.row_count
            or partition.artifact.size_bytes != ref.file.size_bytes
        ):
            raise ValueError("diagnostic adapter inventory differs")
        if ref.file.verify() != payload:
            raise ValueError("diagnostic source changed during raw admission")
        partitions.append(partition)
    if tuple(sorted(p.symbol for p in partitions)) != SYMBOLS:
        raise ValueError("diagnostic source requires all three exact symbols")
    descriptor = DatasetDescriptorV1(
        (
            "training-weight-contract-fixture"
            if _legacy_kind(plan) is WeightEvidenceKind.FIXTURE
            else "training-weight-diagnostic-parent"
        ),
        "Diagnostic parent source",
        "Raw structure/integrity only; not scientific support, authenticity or qualification.",
        (DatasetOrigin.OBSERVED,),
    )
    evidence = root / f"source-admission-{period}.json"
    _publish(
        root,
        evidence.name,
        diagnostic_json(
            {
                "plan_id": plan.artifact_id,
                "period": period,
                "claim": "raw_structure_integrity_only_not_scientific_qualification",
            }
        ),
    )
    version = DatasetVersionManifestV1(
        dataset_id=descriptor.dataset_id,
        origin=DatasetOrigin.OBSERVED,
        normalization_policy_id=f"{adapter.descriptor.adapter_id}@{adapter.descriptor.adapter_version}:{adapter.descriptor.projection_schema_version}",
        qualification_status=DatasetQualificationStatus.QUALIFIED,
        partitions=tuple(sorted(partitions, key=lambda p: p.symbol)),
        qualification_evidence=(
            artifact_ref_for_file(
                evidence, kind="diagnostic-source-integrity-only"
            ),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return TrainingWeightSourcePlanV1(
        TrainingSourceV1(catalog.to_json(), version.dataset_version_id),
        "application" if period == "201102" else "calibration",
        _legacy_kind(plan),
    )


def _model(plan: TrainingWeightDiagnosticPlanV1) -> TrainingWeightModelV1:
    index = next(
        r
        for r in plan.inputs
        if r.role is TrainingWeightDiagnosticInputRole.MODEL_INDEX
    )
    payload = index.file.verify()
    raw = training_load(payload.decode("utf-8"))
    files = tuple(
        sorted(
            (
                r.file
                for r in plan.inputs
                if r.role
                is TrainingWeightDiagnosticInputRole.MODEL_TRAINING_SOURCE
            ),
            key=lambda f: (f.sha256, f.path),
        )
    )
    epoch = next(
        (
            r.file
            for r in plan.inputs
            if r.role is TrainingWeightDiagnosticInputRole.EPOCH
        ),
        None,
    )
    for file in (*files, *((epoch,) if epoch is not None else ())):
        file.verify()
    model = TrainingWeightModelV1(
        _legacy_kind(plan),
        index.file,
        training_json(raw),
        files,
        epoch,
        training_weight_source_fingerprint(),
    )
    if index.file.verify() != payload:
        raise ValueError(
            "diagnostic fixed model changed during input verification"
        )
    return model


def _month_worker(
    plan_json: str, approval_json: str | None, directory: str, period: str
) -> None:
    from .training_weight_source_diagnostics import (
        diagnose_verified_training_weight_source,
        split_training_weight_source_diagnostics,
    )

    root = Path(directory)
    plan = TrainingWeightDiagnosticPlanV1.from_json(plan_json)
    approval = (
        None
        if approval_json is None
        else TrainingWeightDiagnosticApprovalV1.from_json(approval_json)
    )
    _gate(plan, approval, "diagnostic-source-census")
    _gate(plan, approval, "diagnostic-input-verification")
    _runtime(root, plan, executing=True)
    _input_paths(plan)
    source = _source_plan(root, plan, period)
    verified = verify_training_source(source.parent_source)
    ownership = _ownership(verified)
    report = diagnose_verified_training_weight_source(
        source,
        verified,
        ownership,
        on_cell=lambda day, symbol: _checkpoint(
            root, "source", day, "", symbol
        ),
    )
    source_shards = tuple(
        _component(root, "source-shard", shard)
        for shard in split_training_weight_source_diagnostics(report)
    )
    degradation = _create_training_weight_degradation_from_verified(
        source, verified, root / f"subset-{period}"
    )
    if tuple(d.utc_date for d in report.days if d.original_admitted) != tuple(
        b.utc_date for b in degradation.bridges
    ):
        raise ValueError("diagnostic census differs from unchanged degradation")
    bridges = tuple(
        _component(root, "bridge", bridge) for bridge in degradation.bridges
    )
    month = TrainingWeightDiagnosticMonthV1(
        plan.artifact_id,
        source,
        ownership,
        report.artifact_id,
        source_shards,
        degradation.subset_source,
        degradation.subset_ownership,
        bridges,
        degradation.refusals,
    )
    for ref in plan.inputs:
        if (
            ref.role is TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
            and ref.period == period
        ):
            ref.file.verify()
    _component(root, "month", month)


def _day_worker(
    plan_json: str,
    approval_json: str | None,
    directory: str,
    bridge_json: str,
    model_json: str,
) -> None:
    from .training_weight_generator_diagnostics import (
        diagnose_training_weight_generator_day,
    )

    root = Path(directory)
    plan = TrainingWeightDiagnosticPlanV1.from_json(plan_json)
    approval = (
        None
        if approval_json is None
        else TrainingWeightDiagnosticApprovalV1.from_json(approval_json)
    )
    _gate(plan, approval, "diagnostic-generator-trace")
    _runtime(root, plan, executing=True)
    bridge = TrainingWeightDayBridgeV1.from_json(bridge_json)
    model = TrainingWeightModelV1.from_json(model_json)
    if model.adapter_source_fingerprint != training_weight_source_fingerprint():
        raise ValueError("diagnostic worker method fingerprint changed")
    report = diagnose_training_weight_generator_day(
        bridge,
        model,
        model.index,
        deadline=time.monotonic() + MAX_DAY_SECONDS,
        on_cell=lambda member, symbol: _checkpoint(
            root, "generator", bridge.utc_date, member, symbol
        ),
    )
    _component(root, "day", report)


def _load_months(root: Path) -> tuple[TrainingWeightDiagnosticMonthV1, ...]:
    paths = sorted(root.glob("diagnostic-month-*.json"))
    if len(paths) > 3:
        raise ValueError("diagnostic month inventory exceeds frozen scope")
    months = tuple(
        TrainingWeightDiagnosticMonthV1.from_json(
            read_training_regular(p).decode("ascii")
        )
        for p in paths
    )
    return tuple(sorted(months, key=lambda m: m.source_plan.period))


def _worker(root: Path) -> None:
    plan = TrainingWeightDiagnosticPlanV1.from_json(
        read_training_regular(root / "plan.json").decode("ascii")
    )
    approval_path = root / "approval.json"
    approval = (
        TrainingWeightDiagnosticApprovalV1.from_json(
            read_training_regular(approval_path).decode("ascii")
        )
        if approval_path.exists()
        else None
    )
    reason = "worker_failed"
    stage = "preflight"

    def interrupted(signum: int, frame: object) -> None:
        raise TrainingWeightOperationalFailure("campaign_cancelled")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        for operation in _OPERATIONS:
            _gate(plan, approval, operation)
        _runtime(root, plan, executing=True)
        _input_paths(plan)
        stage = "source"
        for period in PERIODS:
            _checkpoint(root, "source", period)
            _supervise_day_worker(
                _month_worker,
                (
                    plan.to_json(),
                    None if approval is None else approval.to_json(),
                    str(root),
                    period,
                ),
                MAX_MONTH_SECONDS,
                period,
            )
        months = _load_months(root)
        if tuple(m.source_plan.period for m in months) != PERIODS:
            raise ValueError("diagnostic months incomplete")
        stage = "reconciliation"
        if (
            plan.evidence_kind
            is TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
            and tuple(len(m.bridges) for m in months) != (17, 8, 6)
        ):
            reason = "source_reconciliation_failed"
            raise ValueError("original source admission inventory changed")
        stage = "model"
        _checkpoint(root, "model")
        _gate(plan, approval, "diagnostic-input-verification")
        model = _model(plan)
        _component(root, "model", model)
        by_date = {}
        for month in months:
            for ref in month.bridges:
                bridge = TrainingWeightDayBridgeV1.from_json(
                    _read_component(root, ref)
                )
                by_date[bridge.utc_date] = bridge
        stage = "generator"
        for day in _selected_dates():
            if day not in by_date:
                if (
                    plan.evidence_kind
                    is TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
                ):
                    reason = "source_reconciliation_failed"
                    raise ValueError(
                        "selected original diagnostic day is unavailable"
                    )
                continue
            _supervise_day_worker(
                _day_worker,
                (
                    plan.to_json(),
                    None if approval is None else approval.to_json(),
                    str(root),
                    by_date[day].to_json(),
                    model.to_json(),
                ),
                MAX_DAY_SECONDS,
                day,
            )
        for input_ref in plan.inputs:
            input_ref.file.verify()
        runtime = _runtime(root, plan, executing=True)
        # Full report reconciliation is inside the hard-supervised process,
        # including its decoding and native-lineage accounting cost.
        cells, components, selections, capacity_failed = _collect(
            root, plan, complete=True
        )
        result = TrainingWeightDiagnosticAttemptV1(
            plan.artifact_id,
            plan.evidence_kind,
            "failed" if capacity_failed else "completed",
            (
                "diagnostic_capacity_failed"
                if capacity_failed
                else "diagnostics_completed_not_qualification"
            ),
            0,
            None,
            "completed",
            cells,
            components,
            selections,
            runtime.artifact_id,
        )
        _publish(root, "worker-result.json", result.to_json())
        stage = "completed"
        reason = result.reason
    except TrainingWeightOperationalFailure as exc:
        reason = {
            "campaign_cancelled": "attempt_cancelled",
            "candidate_day_deadline": (
                "source_month_deadline"
                if stage == "source"
                else "generator_day_deadline"
            ),
        }.get(exc.reason, "worker_failed")
    except (
        Exception
    ):  # noqa: BLE001 - close the attempt without leaking raw input exceptions.
        if reason != "source_reconciliation_failed":
            reason = "worker_failed"
    _publish(
        root,
        "worker-status.json",
        diagnostic_json({"stage": stage, "reason": reason}),
    )


def _empty_inventory() -> tuple[TrainingWeightDiagnosticCellInventoryV1, ...]:
    return tuple(
        TrainingWeightDiagnosticCellInventoryV1(
            stage, day, member, symbol, "not_attempted", "not_started", None
        )
        for stage, day, member, symbol in (
            *(("source", d, "", s) for d in _dates() for s in SYMBOLS),
            *(
                ("generator", d, m, s)
                for d in _selected_dates()
                for m in MEMBERS
                for s in SYMBOLS
            ),
        )
    )


def _progress_inventory(
    root: Path,
) -> tuple[TrainingWeightDiagnosticCellInventoryV1, ...]:
    """Only exact per-cell checkpoints imply work started, never completion."""
    cells = []
    for cell in _empty_inventory():
        name = f"progress-{cell.stage}-{cell.utc_date}-{cell.member_id or 'none'}-{cell.symbol}.json"
        path = root / name
        if path.exists():
            raw = diagnostic_load(
                read_training_regular(path, 4096).decode("ascii")
            )
            if raw != {
                "stage": cell.stage,
                "utc_date": cell.utc_date,
                "member_id": cell.member_id,
                "symbol": cell.symbol,
            }:
                raise ValueError("diagnostic progress coordinate differs")
            cell = replace(
                cell,
                status="attempted_result_unavailable",
                reason="no_finalized_cell_evidence",
            )
        cells.append(cell)
    return tuple(cells)


def _collect(
    root: Path, plan: TrainingWeightDiagnosticPlanV1, *, complete: bool
) -> tuple[
    tuple[TrainingWeightDiagnosticCellInventoryV1, ...],
    tuple[TrainingWeightDiagnosticComponentV1, ...],
    str,
    bool,
]:
    """Replay only bounded retained artifacts, never source/model file locators."""
    from .training_weight_generator_diagnostics import (
        TrainingWeightGeneratorDayDiagnosticV1,
    )
    from .training_weight_source_diagnostics import (
        TrainingWeightSourceDiagnosticShardV1,
        verify_training_weight_source_diagnostic_shards,
    )

    readers: dict[str, type[TrainingContract]] = {
        "source-shard": TrainingWeightSourceDiagnosticShardV1,
        "bridge": TrainingWeightDayBridgeV1,
        "month": TrainingWeightDiagnosticMonthV1,
        "model": TrainingWeightModelV1,
        "day": TrainingWeightGeneratorDayDiagnosticV1,
    }
    runtime = _runtime(root, plan)
    refs: list[TrainingWeightDiagnosticComponentV1] = []
    values = {}
    total = 0
    for count, path in enumerate(root.iterdir(), 1):
        if count > 512:
            raise ValueError("diagnostic directory exceeds inventory bound")
        if not path.name.startswith("diagnostic-"):
            continue
        match = re.fullmatch(
            r"diagnostic-(source-shard|bridge|month|model|day)-([0-9a-f]{64})\.json",
            path.name,
        )
        if match is None or len(refs) >= MAX_COMPONENTS:
            raise ValueError("unknown/excess diagnostic component")
        kind, digest = match.groups()
        payload = read_training_regular(path, MAX_COMPONENT_BYTES)
        total += len(payload)
        if (
            total > MAX_TOTAL_COMPONENT_BYTES
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise ValueError("diagnostic component digest/budget differs")
        value = readers[kind].from_json(payload.decode("ascii"))
        if value.to_json().encode("ascii") != payload:
            raise ValueError("diagnostic component encoding differs")
        diagnostic_load(value.to_json())
        ref = TrainingWeightDiagnosticComponentV1(
            kind,
            value.artifact_id,
            TrainingWeightFileV1(path.name, len(payload), digest),
        )
        if ref.component_id in values:
            raise ValueError("repeated diagnostic component identity")
        refs.append(ref)
        values[ref.component_id] = value
    references = {ref.component_id: ref for ref in refs}
    used: set[str] = set()

    def get(ref: TrainingWeightDiagnosticComponentV1) -> TrainingContract:
        if references.get(ref.component_id) != ref:
            raise ValueError("diagnostic component reference differs")
        used.add(ref.component_id)
        return values[ref.component_id]

    ledger = {
        (c.stage, c.utc_date, c.member_id, c.symbol): c
        for c in _progress_inventory(root)
    }
    selections = dict.fromkeys(_dates(), "pending")
    bridges = {}
    periods = set()
    for ref in refs:
        if ref.kind != "month":
            continue
        month = get(ref)
        assert isinstance(month, TrainingWeightDiagnosticMonthV1)
        period = month.source_plan.period
        if (
            month.plan_id != plan.artifact_id
            or period in periods
            or month.source_plan.evidence_kind != _legacy_kind(plan)
        ):
            raise ValueError("diagnostic month plan/period differs")
        periods.add(period)
        catalog = DatasetCatalog.from_json(
            month.source_plan.parent_source.catalog_json
        )
        version = next(
            v
            for v in catalog.versions
            if v.dataset_version_id
            == month.source_plan.parent_source.dataset_version_id
        )
        admission_path = root / f"source-admission-{period}.json"
        admission_text = diagnostic_json(
            {
                "plan_id": plan.artifact_id,
                "period": period,
                "claim": "raw_structure_integrity_only_not_scientific_qualification",
            }
        )
        if (
            read_training_regular(admission_path, 4096)
            != admission_text.encode("ascii")
            or len(version.qualification_evidence) != 1
        ):
            raise ValueError("diagnostic source admission metadata differs")
        admission_ref = version.qualification_evidence[0]
        if (
            admission_ref.path,
            admission_ref.size_bytes,
            admission_ref.sha256,
            admission_ref.kind,
        ) != (
            str(admission_path.resolve()),
            len(admission_text),
            hashlib.sha256(admission_text.encode("ascii")).hexdigest(),
            "diagnostic-source-integrity-only",
        ):
            raise ValueError("diagnostic source admission reference differs")
        expected_inputs = {
            r.symbol: r
            for r in plan.inputs
            if r.role is TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
            and r.period == period
        }
        for partition in version.partitions:
            expected = expected_inputs[partition.symbol]
            if (
                partition.artifact.path,
                partition.artifact.sha256,
                partition.artifact.size_bytes,
                partition.row_count,
            ) != (
                expected.file.path,
                expected.file.sha256,
                expected.file.size_bytes,
                expected.row_count,
            ):
                raise ValueError("diagnostic month source binding differs")
        untyped_shards = tuple(get(s) for s in month.source_shards)
        if not all(
            isinstance(s, TrainingWeightSourceDiagnosticShardV1)
            for s in untyped_shards
        ):
            raise ValueError("diagnostic source shard has wrong type")
        shards = cast(
            tuple[TrainingWeightSourceDiagnosticShardV1, ...], untyped_shards
        )
        report = verify_training_weight_source_diagnostic_shards(shards)
        if (
            report.artifact_id,
            report.source_plan_id,
            report.source_id,
            report.dataset_version_id,
            report.ownership_id,
            report.evidence_kind,
        ) != (
            month.source_report_id,
            month.source_plan.artifact_id,
            month.source_plan.parent_source.artifact_id,
            month.source_plan.parent_source.dataset_version_id,
            month.ownership.artifact_id,
            plan.evidence_kind,
        ):
            raise ValueError("diagnostic source provenance differs")
        month_bridges = []
        for bridge_ref in month.bridges:
            bridge = get(bridge_ref)
            if (
                not isinstance(bridge, TrainingWeightDayBridgeV1)
                or bridge.utc_date in bridges
                or bridge.source_plan != month.source_plan
                or bridge.subset_source != month.subset_source
                or bridge.parent_unit not in month.ownership.units
                or month.subset_ownership is None
                or bridge.subset_unit not in month.subset_ownership.units
            ):
                raise ValueError("diagnostic bridge lineage differs")
            bridges[bridge.utc_date] = bridge
            month_bridges.append(bridge.utc_date)
        if tuple(month_bridges) != tuple(
            d.utc_date for d in report.days if d.original_admitted
        ) or tuple(
            (r.utc_date, r.reason.value) for r in month.refusals
        ) != tuple(
            (d.utc_date, d.original_reason)
            for d in report.days
            if not d.original_admitted
        ):
            raise ValueError(
                "diagnostic source admission reconciliation differs"
            )
        for shard, shard_ref in zip(shards, month.source_shards):
            for cell in shard.cells:
                key = ("source", cell.utc_date, "", cell.symbol)
                ledger[key] = replace(
                    ledger[key],
                    status=(
                        "not_evaluable"
                        if cell.classification == "not_evaluable"
                        else "evaluated"
                    ),
                    reason=cell.classification,
                    component_id=shard_ref.component_id,
                )
        for source_day in report.days:
            selections[source_day.utc_date] = (
                (
                    "selected"
                    if source_day.utc_date in _selected_dates()
                    else "not_selected_by_diagnostic_design"
                )
                if source_day.original_admitted
                else "source_refused"
            )
            if (
                not source_day.original_admitted
                and source_day.utc_date in _selected_dates()
            ):
                for member in MEMBERS:
                    for symbol in SYMBOLS:
                        key = ("generator", source_day.utc_date, member, symbol)
                        ledger[key] = replace(
                            ledger[key],
                            status="not_evaluable",
                            reason="source_refused_before_generator",
                            component_id=ref.component_id,
                        )
    model_refs = [r for r in refs if r.kind == "model"]
    if len(model_refs) > 1:
        raise ValueError("diagnostic has multiple models")
    model = get(model_refs[0]) if model_refs else None
    if model is not None:
        assert isinstance(model, TrainingWeightModelV1)
        index = next(
            r.file
            for r in plan.inputs
            if r.role is TrainingWeightDiagnosticInputRole.MODEL_INDEX
        )
        files = tuple(
            sorted(
                (
                    r.file
                    for r in plan.inputs
                    if r.role
                    is TrainingWeightDiagnosticInputRole.MODEL_TRAINING_SOURCE
                ),
                key=lambda f: (f.sha256, f.path),
            )
        )
        epoch = next(
            (
                r.file
                for r in plan.inputs
                if r.role is TrainingWeightDiagnosticInputRole.EPOCH
            ),
            None,
        )
        if (
            model.evidence_kind != _legacy_kind(plan)
            or model.index_file != index
            or model.source_files != files
            or model.epoch_file != epoch
            or model.adapter_source_fingerprint != runtime.source_fingerprint
        ):
            raise ValueError("diagnostic model input binding differs")
    days: set[str] = set()
    capacity_failed = False
    for ref in refs:
        if ref.kind != "day":
            continue
        day = get(ref)
        assert isinstance(day, TrainingWeightGeneratorDayDiagnosticV1)
        day_bridge = bridges.get(day.utc_date)
        if (
            day.utc_date not in _selected_dates()
            or day.utc_date in days
            or day_bridge is None
            or model is None
            or (
                day.bridge_id,
                day.model_id,
                day.index_id,
                day.adapter_source_fingerprint,
                day.stochastic_model_id,
                day.evidence_kind,
            )
            != (
                day_bridge.artifact_id,
                model.artifact_id,
                model.index.index_id,
                model.adapter_source_fingerprint,
                model.stochastic_model_id,
                plan.evidence_kind,
            )
        ):
            raise ValueError("diagnostic generator lineage differs")
        days.add(day.utc_date)
        capacity_failed |= day.status != "complete"
        for generator_cell in day.cells:
            key = (
                "generator",
                day.utc_date,
                generator_cell.member_id,
                generator_cell.symbol,
            )
            ledger[key] = replace(
                ledger[key],
                status=(
                    "evaluated"
                    if generator_cell.status == "success"
                    else generator_cell.status
                ),
                reason=generator_cell.reason or "native_generation_succeeded",
                component_id=ref.component_id,
            )
    if complete:
        if (
            periods != set(PERIODS)
            or model is None
            or days != set(_selected_dates()) & set(bridges)
            or used != set(values)
        ):
            raise ValueError("completed diagnostic inventory is incomplete")
        if (
            plan.evidence_kind
            is TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
            and (
                tuple(
                    sum(d.replace("-", "").startswith(p) for d in bridges)
                    for p in PERIODS
                )
                != (17, 8, 6)
                or days != set(_selected_dates())
            )
        ):
            raise ValueError("original source reconciliation differs")
    return (
        tuple(ledger.values()),
        tuple(sorted(refs, key=lambda r: r.file.path)),
        diagnostic_json(selections),
        capacity_failed,
    )


def run_training_weight_diagnostics(
    plan: TrainingWeightDiagnosticPlanV1,
    directory: str | Path,
    *,
    approval: TrainingWeightDiagnosticApprovalV1 | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> TrainingWeightDiagnosticAttemptV1:
    """One fresh bounded attempt; never retry, calibrate, evaluate or publish.

    Fixture execution accepts no approval. Nonfixture reads require the distinct
    plan-bound diagnostic receipt, supplied by an external coordinator. No API
    here creates an approval. Interrupted workers leave a full cell denominator.
    """
    if os.name != "posix":
        raise NotImplementedError("hard-supervised diagnostics require POSIX")
    if type(plan) is not TrainingWeightDiagnosticPlanV1:
        raise TypeError("diagnostics require a complete typed plan")
    start = time.monotonic()
    for operation in _OPERATIONS:
        _gate(plan, approval, operation)
    root = Path(directory)
    if root.exists() or root.is_symlink():
        raise ValueError("diagnostics require a new attempt directory")
    if root.absolute() != root.resolve():
        raise ValueError("diagnostic output locator has a symlink alias")
    root.mkdir(parents=True, exist_ok=False)
    _sync(root.parent)
    _publish(root, "plan.json", plan.to_json())
    runtime = TrainingWeightDiagnosticRuntimeV1(
        plan.artifact_id, training_weight_source_fingerprint()
    )
    _publish(
        root,
        "runtime.json",
        runtime.to_json(),
    )
    if approval is not None:
        _publish(root, "approval.json", approval.to_json())
    pending = diagnostic_json(dict.fromkeys(_dates(), "pending"))
    started = TrainingWeightDiagnosticAttemptV1(
        plan.artifact_id,
        plan.evidence_kind,
        "started",
        "pending",
        0,
        None,
        "preflight",
        _empty_inventory(),
        (),
        pending,
        runtime.artifact_id,
    )
    _publish(root, "started.json", started.to_json())
    pid = None
    stage = "preflight"
    reason = "worker_start_failed"
    cells, selections = started.inventory, pending
    components: tuple[TrainingWeightDiagnosticComponentV1, ...] = ()
    try:
        pid, outcome = _supervise_campaign(
            (sys.executable, "-m", __name__, "--worker", str(root.absolute())),
            max(0.0, MAX_ATTEMPT_SECONDS - (time.monotonic() - start)),
            cancelled,
        )
        reason = {
            "campaign_deadline": "attempt_deadline",
            "campaign_cancelled": "attempt_cancelled",
        }.get(outcome, outcome)
        if outcome == "execution_completed_not_qualification":
            status_path = root / "worker-status.json"
            if not status_path.exists():
                reason = "worker_terminal_missing"
            else:
                text = read_training_regular(status_path, 4096).decode("ascii")
                raw = diagnostic_load(text)
                if (
                    set(raw) != {"stage", "reason"}
                    or diagnostic_json(raw) != text
                ):
                    raise ValueError(
                        "diagnostic worker terminal is not closed/canonical"
                    )
                if not isinstance(raw["stage"], str) or not isinstance(
                    raw["reason"], str
                ):
                    raise ValueError(
                        "diagnostic worker outcome needs string fields"
                    )
                stage, reason = raw["stage"], raw["reason"]
        if reason in (
            "diagnostics_completed_not_qualification",
            "diagnostic_capacity_failed",
        ):
            result = _canonical_artifact(
                root / "worker-result.json", TrainingWeightDiagnosticAttemptV1
            )
            assert isinstance(result, TrainingWeightDiagnosticAttemptV1)
            if (
                result.plan_id,
                result.evidence_kind,
                result.runtime_id,
                result.reason,
                result.last_stage,
            ) != (
                plan.artifact_id,
                plan.evidence_kind,
                runtime.artifact_id,
                reason,
                stage,
            ):
                raise ValueError("diagnostic worker report binding differs")
            # Recheck persisted bytes, without repeating expensive native decode
            # outside the supervised report-construction deadline.
            for component in result.components:
                _read_component(root, component)
            _runtime(root, plan, executing=True)
            cells, components, selections = (
                result.inventory,
                result.components,
                result.day_selection_json,
            )
        else:
            # Failed-attempt recovery is metadata accounting, never continued
            # source/model execution or another candidate attempt.
            cells, components, selections, _ = _collect(
                root, plan, complete=False
            )
        if (
            reason == "diagnostics_completed_not_qualification"
            and time.monotonic() - start >= MAX_ATTEMPT_SECONDS
        ):
            reason = "attempt_deadline"
        TrainingWeightDiagnosticAttemptV1(
            plan.artifact_id,
            plan.evidence_kind,
            (
                "completed"
                if reason == "diagnostics_completed_not_qualification"
                else "failed"
            ),
            reason,
            0,
            pid,
            stage,
            cells,
            components,
            selections,
            runtime.artifact_id,
        )
    except KeyboardInterrupt:
        reason, stage = "attempt_cancelled", "preflight"
    except (
        Exception
    ):  # noqa: BLE001 - every operational failure needs a terminal ledger.
        reason, stage = "worker_failed", "preflight"
    if not components:
        try:
            cells = _progress_inventory(root)
        except (
            Exception
        ):  # noqa: BLE001 - corrupt progress cannot prevent a failed terminal.
            cells = started.inventory
    terminal = TrainingWeightDiagnosticAttemptV1(
        plan.artifact_id,
        plan.evidence_kind,
        (
            "completed"
            if reason == "diagnostics_completed_not_qualification"
            else "failed"
        ),
        reason,
        int((time.monotonic() - start) * 1_000_000_000),
        pid,
        stage,
        cells,
        components,
        selections,
        runtime.artifact_id,
    )
    _publish(root, "terminal.json", terminal.to_json())
    return terminal


def inspect_training_weight_diagnostic_artifacts(
    directory: str | Path,
    *,
    approval: TrainingWeightDiagnosticApprovalV1 | None = None,
) -> TrainingWeightDiagnosticAttemptV1:
    """Artifact-only structural replay, NOT fresh source/generator execution.

    This never follows source or model locators. It confers no scientific
    qualification, authenticity, approval or additional historical evidence.
    """
    root = Path(directory)
    plan = _canonical_artifact(
        root / "plan.json", TrainingWeightDiagnosticPlanV1
    )
    assert isinstance(plan, TrainingWeightDiagnosticPlanV1)
    _gate(plan, approval, "diagnostic-artifact-replay")
    runtime = _runtime(root, plan)
    started = _canonical_artifact(
        root / "started.json", TrainingWeightDiagnosticAttemptV1
    )
    assert isinstance(started, TrainingWeightDiagnosticAttemptV1)
    if (
        started.status != "started"
        or started.plan_id != plan.artifact_id
        or started.evidence_kind != plan.evidence_kind
        or started.inventory != _empty_inventory()
        or started.components
        or started.runtime_id != runtime.artifact_id
    ):
        raise ValueError("diagnostic started inventory differs")
    if plan.evidence_kind is TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC:
        retained = _canonical_artifact(
            root / "approval.json", TrainingWeightDiagnosticApprovalV1
        )
        assert isinstance(retained, TrainingWeightDiagnosticApprovalV1)
        if (
            retained.plan_id != plan.artifact_id
            or retained.source_fingerprint != runtime.source_fingerprint
            or not set(_OPERATIONS) <= set(retained.allowed_operations)
        ):
            raise ValueError("retained execution approval differs")
    elif (root / "approval.json").exists():
        raise ValueError("fixture cannot retain execution approval")
    terminal = _canonical_artifact(
        root / "terminal.json", TrainingWeightDiagnosticAttemptV1
    )
    assert isinstance(terminal, TrainingWeightDiagnosticAttemptV1)
    if terminal.status == "started" or (
        terminal.plan_id,
        terminal.evidence_kind,
    ) != (plan.artifact_id, plan.evidence_kind):
        raise ValueError("diagnostic terminal scope differs")
    if terminal.runtime_id != runtime.artifact_id:
        raise ValueError("diagnostic terminal runtime differs")
    cells, components, selections, capacity_failed = _collect(
        root, plan, complete=terminal.status == "completed"
    )
    if (cells, components, selections) != (
        terminal.inventory,
        terminal.components,
        terminal.day_selection_json,
    ) or (terminal.status == "completed" and capacity_failed):
        raise ValueError("diagnostic terminal differs from retained artifacts")
    return terminal


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit(
            "private diagnostic worker requires an exact plan directory"
        )
    _worker(Path(sys.argv[2]))
