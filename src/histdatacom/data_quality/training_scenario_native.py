"""Closed, freshly executed native evidence for scenario-preserving views.

This adapter does not promote reconstruction engines, authenticate a historical
campaign, or infer scenario meanings from opaque configuration labels. Research
recipes and full campaign traversal are separate native verification routes.
"""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass, replace
from itertools import pairwise
from pathlib import Path
from typing import Any

from .training_contracts import (
    NO_LABEL_SCHEMA,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingRootV1,
    TrainingRowV1,
    TrainingVerificationLevel,
    admitted_modes,
    training_json,
    training_load,
)
from .training_lineage import (
    _VerifiedSource,
    read_training_regular,
    verify_training_ownership,
)
from .training_provider_policy import require_training_derivation
from .training_scenario_contracts import (
    SCENARIO_AXES,
    ScenarioAxis,
    ScenarioAxisState,
    ScenarioMemberStatus,
    TrainingScenarioAxisV1,
    TrainingScenarioMemberV1,
    TrainingScenarioPlanV1,
    TrainingScenarioRequestV1,
)
from .training_scenario_math import MAX_SCENARIO_CELLS
from .training_views import NATIVE_FEATURE_SCHEMA, OBSERVED_FEATURE_SCHEMA

MAX_SCENARIO_PRODUCTS = 32
MAX_SCENARIO_NATIVE_FILES = 8192


@dataclass(frozen=True, slots=True)
class ScenarioNativeInventory:
    """Operation-local observations, never accepted as public authority."""

    members: tuple[TrainingScenarioMemberV1, ...]
    rows: tuple[tuple[str | None, TrainingRowV1], ...]
    roots: tuple[TrainingRootV1, ...]
    files: tuple[tuple[str, int, str], ...]
    verified: _VerifiedSource


def _remember(
    files: dict[str, tuple[int, str]], path: str, size: int, digest: str
) -> None:
    previous = files.get(path)
    if previous is not None and previous != (size, digest):
        raise ValueError("scenario native input changed between observations")
    if previous is None and len(files) >= MAX_SCENARIO_NATIVE_FILES:
        raise ValueError(
            "scenario complete native input inventory exceeds bound"
        )
    files[path] = (size, digest)


def _current_files(files: tuple[tuple[str, int, str], ...]) -> None:
    # Stream native binary partitions instead of allocating a whole extra IPC
    # copy. Byte integrity is not authenticity against a hostile local process.
    def stamp(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    for path, size, digest in files:
        selected = Path(path)
        before = selected.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:
            raise ValueError(
                "scenario input is no longer the exact regular file"
            )
        fd = os.open(
            selected,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        count = 0
        actual = hashlib.sha256()
        with os.fdopen(fd, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if stamp(before) != stamp(opened):
                raise ValueError(
                    "scenario input identity changed before hashing"
                )
            while chunk := stream.read(min(1024 * 1024, size - count + 1)):
                count += len(chunk)
                if count > size:
                    raise ValueError(
                        "scenario native input grew during hashing"
                    )
                actual.update(chunk)
            after = os.fstat(stream.fileno())
        if (
            count != size
            or actual.hexdigest() != digest
            or stamp(opened) != stamp(after)
            or stamp(after) != stamp(selected.lstat())
        ):
            raise ValueError(
                "scenario native input changed during materialization"
            )


def _unsupported_provider(verified: _VerifiedSource) -> None:
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    # A new descendant does not inherit the permission receipt of an old batch.
    # Inspect the complete inventory, including products omitted by the view.
    for item in verified.products:
        if type(item.manifest) is ReconstructionProductManifestV1 or any(
            event.broker_profile_id is not None for event in item.events
        ):
            raise ValueError("broker-derived scenario views are unsupported")
    if verified.contexts or verified.features:
        raise ValueError(
            "scenario v1 accepts quote/product sources, not context panels"
        )
    require_training_derivation(verified)


def _axes(
    runtime: dict[str, Any] | None,
    member: str | None,
    delivery: str | None,
    evidence: tuple[str, ...],
) -> tuple[TrainingScenarioAxisV1, ...]:
    values: dict[ScenarioAxis, str | None] = {
        ScenarioAxis.OBSERVATION_RETENTION: (
            runtime.get("observation_scenario_kind") if runtime else None
        ),
        ScenarioAxis.TRANSITION: (
            runtime.get("transition_scenario_kind") if runtime else None
        ),
        ScenarioAxis.PATH_REALIZATION: member,
        ScenarioAxis.ENGINE: (
            runtime.get("proposal_engine_id") if runtime else None
        ),
        ScenarioAxis.DELIVERY_PROFILE: delivery,
        ScenarioAxis.BROKER: None,
        ScenarioAxis.POPULATION: None,
    }
    result = []
    for axis in SCENARIO_AXES:
        value = values[axis]
        if value is not None:
            state = ScenarioAxisState.KNOWN
        elif axis in (ScenarioAxis.BROKER, ScenarioAxis.POPULATION):
            state = ScenarioAxisState.NOT_APPLICABLE
        elif runtime is None:
            state = ScenarioAxisState.UNAVAILABLE
        else:
            state = ScenarioAxisState.NOT_APPLICABLE
        result.append(TrainingScenarioAxisV1(axis, state, value, evidence))
    return tuple(result)


def _merge_members(
    members: list[TrainingScenarioMemberV1],
) -> tuple[TrainingScenarioMemberV1, ...]:
    """Join adjacent physical pieces, never silently bridge a support hole."""
    groups: dict[str, list[TrainingScenarioMemberV1]] = {}
    for member in members:
        groups.setdefault(member.member_key, []).append(member)
    result = []
    for key in sorted(groups):
        pieces = sorted(
            groups[key], key=lambda item: (item.start_ns, item.end_ns)
        )
        first = pieces[0]
        if any(a.end_ns > b.start_ns for a, b in pairwise(pieces)):
            raise ValueError("scenario member physical supports overlap")
        gap = any(a.end_ns != b.start_ns for a, b in pairwise(pieces))
        statuses = {item.status for item in pieces}
        reasons = {reason for item in pieces for reason in item.reason_codes}
        if gap:
            reasons.add("incomplete_member_support")
        status = (
            ScenarioMemberStatus.REFUSED
            if ScenarioMemberStatus.REFUSED in statuses
            else (
                ScenarioMemberStatus.UNAVAILABLE
                if gap or ScenarioMemberStatus.UNAVAILABLE in statuses
                else (
                    ScenarioMemberStatus.ELIGIBLE
                    if ScenarioMemberStatus.ELIGIBLE in statuses
                    else ScenarioMemberStatus.EMPTY
                )
            )
        )
        axes = tuple(
            replace(
                axis,
                evidence_ids=tuple(
                    sorted(
                        {
                            identity
                            for item in pieces
                            for identity in item.axes[index].evidence_ids
                        }
                    )
                ),
            )
            for index, axis in enumerate(first.axes)
        )
        result.append(
            replace(
                first,
                end_ns=pieces[-1].end_ns,
                run_ids=tuple(
                    sorted({x for item in pieces for x in item.run_ids})
                ),
                product_manifest_ids=tuple(
                    sorted(
                        {
                            x
                            for item in pieces
                            for x in item.product_manifest_ids
                        }
                    )
                ),
                axes=axes,
                status=status,
                reason_codes=tuple(sorted(reasons)),
                native_evidence_ids=tuple(
                    sorted(
                        {x for item in pieces for x in item.native_evidence_ids}
                    )
                ),
            )
        )
    return tuple(result)


def _campaign_members(
    plan: TrainingScenarioPlanV1,
    verified: _VerifiedSource,
) -> tuple[
    tuple[TrainingScenarioMemberV1, ...],
    tuple[TrainingRootV1, ...],
    tuple[tuple[str, int, str], ...],
]:
    from histdatacom.campaign_index_contracts import (
        CampaignProductVerificationV1,
    )
    from histdatacom.campaign_verification import open_campaign_verification
    from histdatacom.reconstruction import (
        ReconstructionCampaignProductIndexV1,
        ReconstructionCampaignProductShardV1,
    )

    binding = plan.campaign_binding
    assert binding is not None
    raw = read_training_regular(Path(binding.index_path))
    payload = training_load(raw.decode())
    index = ReconstructionCampaignProductIndexV1.from_dict(payload)
    if (
        hashlib.sha256(raw).hexdigest() != binding.index_sha256
        or index.product_index_id != binding.index_id
        or training_json(index.to_dict()) != training_json(payload)
    ):
        raise ValueError(
            "scenario independently selected campaign binding differs"
        )
    if (
        index.verified_product_count > MAX_SCENARIO_PRODUCTS
        or len(index.shard_refs) > MAX_SCENARIO_PRODUCTS
        or index.support_window_count > MAX_SCENARIO_CELLS
    ):
        raise ValueError(
            "scenario campaign exceeds prospective native work bound"
        )
    traversal = open_campaign_verification(binding.index_path)
    structural = traversal.inventory.structural
    if (
        structural.index_id != binding.index_id
        or structural.index_ref.sha256 != binding.index_sha256
        or Path(structural.index_ref.path).resolve()
        != Path(binding.index_path).resolve()
    ):
        raise ValueError("scenario campaign identity changed")
    source_paths = {
        str(Path(path).resolve()) for path in plan.source.product_manifest_paths
    }
    campaign_paths = {
        str(Path(item.product_path).resolve())
        for item in traversal.inventory.coordinates
    }
    if source_paths != campaign_paths:
        raise ValueError(
            "scenario source must retain the complete campaign product inventory"
        )
    files: dict[str, tuple[int, str]] = {}
    _remember(
        files,
        str(Path(binding.index_path).resolve()),
        len(raw),
        binding.index_sha256,
    )
    for control in traversal.iter_control_evidence():
        for item in control.files:
            _remember(files, item.path, item.size_bytes, item.sha256)
    evidence_by_path: dict[str, CampaignProductVerificationV1] = {}
    for evidence in traversal:
        product_evidence = evidence.verification
        evidence_by_path[
            str(Path(product_evidence.product_ref.path).resolve())
        ] = product_evidence
        for observation in evidence.files:
            _remember(
                files,
                observation.path,
                observation.size_bytes,
                observation.sha256,
            )
    summary = traversal.finish()
    if summary is None:
        raise ValueError("scenario views require a complete native traversal")
    summary_index = training_load(summary.index_ref_json)
    if (
        summary.index_id != binding.index_id
        or summary_index.get("sha256") != binding.index_sha256
    ):
        raise ValueError("scenario campaign final index binding changed")
    if summary.out_of_plan_json != "[]":
        raise ValueError(
            "scenario campaign contains out-of-plan native products"
        )
    manifests = {
        item.manifest.manifest_id: item.manifest for item in verified.products
    }
    members: list[TrainingScenarioMemberV1] = []
    for ref in index.shard_refs:
        shard_raw = read_training_regular(Path(ref.path))
        actual = (len(shard_raw), hashlib.sha256(shard_raw).hexdigest())
        if (
            files.get(str(Path(ref.path).resolve())) != actual
            and files.get(ref.path) != actual
        ):
            raise ValueError(
                "scenario product shard changed after native traversal"
            )
        shard = ReconstructionCampaignProductShardV1.from_dict(
            training_load(shard_raw.decode())
        )
        for row in shard.entries:
            verification = (
                evidence_by_path[str(Path(row.product_ref.path).resolve())]
                if row.product_ref is not None
                else None
            )
            runtime = (
                training_load(verification.runtime_scope_json)
                if verification is not None
                else None
            )
            manifest = (
                manifests[verification.manifest_id]
                if verification is not None
                else None
            )
            ids = tuple(
                sorted(
                    {summary.artifact_id, row.entry_id}
                    | (
                        {verification.verification_id}
                        if verification is not None
                        else set()
                    )
                )
            )
            status = {
                "verified_product": ScenarioMemberStatus.ELIGIBLE,
                "missing_product": ScenarioMemberStatus.UNAVAILABLE,
                "empty": ScenarioMemberStatus.EMPTY,
                "refused": ScenarioMemberStatus.REFUSED,
            }[row.status]
            for unit in plan.ownership.units:
                start, end = max(unit.start_ns, row.start_ns), min(
                    unit.end_ns, row.end_ns
                )
                if start >= end:
                    continue
                if len(members) >= MAX_SCENARIO_CELLS:
                    raise ValueError(
                        "scenario member inventory exceeds expansion bound"
                    )
                members.append(
                    TrainingScenarioMemberV1(
                        unit.artifact_id,
                        row.ensemble_member_id,
                        start,
                        end,
                        (
                            (verification.generator_config_id,)
                            if verification is not None
                            else ()
                        ),
                        (
                            (verification.run_id,)
                            if verification is not None
                            else ()
                        ),
                        (
                            (verification.manifest_id,)
                            if verification is not None
                            else ()
                        ),
                        _axes(
                            runtime,
                            row.ensemble_member_id,
                            getattr(manifest, "delivery_profile_id", None),
                            ids,
                        ),
                        status,
                        (
                            (row.reason_code,)
                            if row.reason_code is not None
                            else ()
                        ),
                        ids,
                    )
                )
    observations = tuple(
        (path, size, digest) for path, (size, digest) in sorted(files.items())
    )
    _current_files(observations)
    root = TrainingRootV1(
        "fresh-scenario-campaign-native-validation",
        summary.artifact_id,
        hashlib.sha256(summary.to_json().encode("ascii")).hexdigest(),
        TrainingVerificationLevel.PRODUCT_REPLAY,
    )
    return _merge_members(members), (root,), observations


def _row(
    plan: TrainingScenarioPlanV1,
    verified: _VerifiedSource,
    *,
    time: int,
    symbol: str,
    origin: TrainingOrigin,
    key: str,
    value: dict[str, Any],
    run: str | None = None,
    member: str | None = None,
    decision: int | None = None,
    scenarios: tuple[str, ...] = (),
    uncertainty: dict[str, Any] | None = None,
) -> TrainingRowV1:
    unit = next(
        item
        for item in plan.ownership.units
        if item.start_ns <= time < item.end_ns
    )
    info = TrainingInformationMode.EX_POST
    return TrainingRowV1(
        unit.artifact_id,
        plan.ownership.artifact_id,
        origin,
        info,
        time,
        time if decision is None else decision,
        None,
        symbol,
        plan.source.dataset_version_id,
        key,
        run,
        member,
        scenarios,
        training_json(
            uncertainty or {"confidence": None, "status": "unavailable"}
        ),
        OBSERVED_FEATURE_SCHEMA if run is None else NATIVE_FEATURE_SCHEMA,
        NO_LABEL_SCHEMA,
        training_json(value),
        verified.roots,
        admitted_modes(origin, info),
    )


def inspect_scenario_native(
    plan: TrainingScenarioPlanV1, request: TrainingScenarioRequestV1
) -> ScenarioNativeInventory:
    """Execute all native sources before any view-specific selection."""
    verified = verify_training_ownership(plan.source, plan.ownership)
    _unsupported_provider(verified)
    if (
        not set(request.symbols) <= set(verified.graph_symbols)
        or request.start_ns < verified.start_ns
        or request.end_ns > verified.end_ns
    ):
        raise ValueError(
            "scenario request escapes complete native source support"
        )

    def selected(time: int, symbol: str) -> bool:
        return (
            request.start_ns <= time < request.end_ns
            and symbol.upper() in request.symbols
        )

    row_count = sum(
        selected(row.event_time_ns, row.symbol) for row in verified.observed
    )
    row_count += sum(
        selected(event.event_time_ns, event.symbol)
        for product in verified.products
        for event in product.events
    )
    if row_count > MAX_SCENARIO_CELLS:
        raise ValueError("scenario selected native row expansion exceeds bound")
    research = getattr(plan, "research_binding", None)
    if plan.campaign_binding is not None:
        if research is not None:
            raise ValueError(
                "scenario plan cannot mix campaign/research authorities"
            )
        members, extra_roots, files = _campaign_members(plan, verified)
    elif research is not None:
        from .training_scenario_research import (
            replay_training_scenario_research,
        )

        members, extra_roots, files = replay_training_scenario_research(plan)
    elif verified.products:
        raise ValueError(
            "counterfactual scenario sources require a native evidence binding"
        )
    else:
        members, extra_roots, files = (), (), ()
    actual_products = {item.manifest.manifest_id for item in verified.products}
    if {
        identity
        for member in members
        for identity in member.product_manifest_ids
    } != actual_products:
        raise ValueError(
            "scenario native members omit or invent retained products"
        )
    rows: list[tuple[str | None, TrainingRowV1]] = []
    for observed in verified.observed:
        if not selected(observed.event_time_ns, observed.symbol):
            continue
        value = observed.payload()
        value.pop("vol")
        value["volume_state"] = "unavailable_pending_source_semantics"
        rows.append(
            (
                None,
                _row(
                    plan,
                    verified,
                    time=observed.event_time_ns,
                    symbol=observed.symbol,
                    origin=TrainingOrigin.OBSERVED,
                    key=f"{plan.source.dataset_version_id}|{observed.series_id}|{observed.period}|{observed.row_id}",
                    value=value,
                ),
            )
        )
    by_product: dict[str, list[TrainingScenarioMemberV1]] = {}
    for member in members:
        for identity in member.product_manifest_ids:
            by_product.setdefault(identity, []).append(member)
    for product in verified.products:
        manifest = product.manifest
        for event in product.events:
            if not selected(event.event_time_ns, event.symbol):
                continue
            matches = [
                member
                for member in by_product[manifest.manifest_id]
                if member.start_ns <= event.event_time_ns < member.end_ns
            ]
            if len(matches) != 1:
                raise ValueError(
                    "native product event has ambiguous or missing scenario ownership"
                )
            member = matches[0]
            origin = (
                TrainingOrigin.OBSERVED
                if event.origin.value == "observed"
                else TrainingOrigin.SYNTHETIC_RECONSTRUCTION
            )
            rows.append(
                (
                    member.member_key,
                    _row(
                        plan,
                        verified,
                        time=event.event_time_ns,
                        symbol=event.symbol.upper(),
                        origin=origin,
                        key=f"{manifest.manifest_id}|{event.event_id}",
                        value=dict(event.to_dict()),
                        run=event.run_id,
                        member=event.ensemble_member_id,
                        decision=product.dependency_end_ns,
                        scenarios=tuple(
                            sorted(
                                set(
                                    manifest.constraints.generator_config_ids
                                    + manifest.constraints.constraint_set_ids
                                )
                            )
                        ),
                        uncertainty={
                            "confidence": event.confidence,
                            "status": "retained_not_calibrated",
                            "ensemble_manifest_id": manifest.ensemble.ensemble_manifest_id,
                        },
                    ),
                )
            )
    roots = tuple(
        sorted(
            {
                root.artifact_id: root
                for root in (*verified.roots, *extra_roots)
            }.values(),
            key=lambda root: root.artifact_id,
        )
    )
    return ScenarioNativeInventory(
        tuple(sorted(members, key=lambda member: member.member_key)),
        tuple(rows),
        roots,
        files,
        verified,
    )


def assert_scenario_native_current(
    plan: TrainingScenarioPlanV1, observed: ScenarioNativeInventory
) -> None:
    """No process-lifetime verification cache or read-once permission receipt."""
    _current_files(observed.files)
    current = verify_training_ownership(plan.source, plan.ownership)
    _unsupported_provider(current)
    if current != observed.verified:
        raise ValueError(
            "scenario source changed during complete materialization"
        )
