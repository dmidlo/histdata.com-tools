"""First-party data-plane handlers for the reconstruction stage plan.

The Temporal workflow carries only bounded receipts and strong references.
Every event-bearing intermediate is written below the window scratch tree,
while the final publication crosses the durable boundary with one atomic
directory rename.  The validation stage writes a durable transaction
descriptor outside the rename source so a worker crash after promotion can be
recovered without referring to a vanished staging path.
"""

from __future__ import annotations

import asyncio
from bisect import bisect_left
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from histdatacom.data_analytics.feed_epochs_v2 import (
    read_active_time_feed_epoch_definition,
)
from histdatacom.market_context import (
    CftcPositioningQueryV1,
    CftcPositioningQueryStatus,
    CftcReportFamily,
    CftcReportScope,
    MarketContextKind,
    MarketContextQueryV1,
    MarketContextView,
    cftc_positioning_state_label,
    market_context_benchmark_event_state,
    preflight_market_context_corpus,
    query_cftc_positioning_corpus,
    query_market_context_corpus,
    read_cftc_positioning_corpus,
    read_market_context_corpus,
)
from histdatacom.orchestration.reconstruction import (
    ReconstructionStage,
    ReconstructionStageInvocationV1,
    ReconstructionStageOutcomeV1,
    artifact_ref_for_file,
    register_reconstruction_stage_handler,
    registered_reconstruction_stage_handlers,
    verify_artifact_ref,
)
from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark_corpus import (
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.carving import (
    HistoricalCarvedCandidateBatchV1,
    carve_empirical_motif_candidates,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
    read_synthetic_event_stream_parquet,
    write_synthetic_event_stream_parquet,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyConditionV1,
    CrossCurrencyGroupStatus,
    CrossCurrencyReconciledGroupV1,
    CrossCurrencyValidationStage,
    reconcile_cross_currency_window,
    validate_cross_currency_output,
)
from histdatacom.synthetic.delivery import (
    ReconstructionDeliveredGroupV1,
    ReconstructionDeliveryManifestV1,
    ReconstructionDeliveryMode,
    project_modern_reference_delivery,
)
from histdatacom.synthetic.generation import (
    EmpiricalMotifCandidateBatchV1,
    EmpiricalMotifEventLineageV1,
    EmpiricalMotifGeneratorConfigV1,
    EmpiricalMotifTransformationV1,
    MotifGenerationDecision,
    MotifGenerationStatus,
    generate_empirical_motif_candidates,
)
from histdatacom.synthetic.information import (
    InformationAuditReportV1,
    InformationMode,
)
from histdatacom.synthetic.motif_library import (
    read_modern_reference_motif_artifact,
    read_modern_reference_motif_index,
)
from histdatacom.synthetic.motifs import (
    ReferenceMotifConditionV1,
    ReferenceMotifIndexV1,
    ReferenceMotifQueryResultV1,
    ReferenceMotifQueryV1,
    query_reference_motifs,
    reference_motif_condition_from_quotes,
)
from histdatacom.synthetic.observation import read_observation_operator_artifact
from histdatacom.synthetic.persistence import (
    PublishedReconstructionV2,
    ReconstructionProductManifestV2,
    ReconstructionRetentionPlanV1,
    StagedReconstructionPublicationV2,
    commit_delivery_reconstruction_publication,
    stage_delivery_reconstruction_publication,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.reconstruction_plan import (
    FIRST_PARTY_RECONSTRUCTION_HANDLERS,
    ReconstructionStagePlanV1,
    load_reconstruction_stage_plan,
)
from histdatacom.synthetic.streaming import (
    CarryStateV1,
    ReconstructionResourceEstimateV1,
)

RECONSTRUCTION_STAGE_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-stage-artifact.v1"
)
RECONSTRUCTION_STAGING_DESCRIPTOR_SCHEMA_VERSION = (
    "histdatacom.reconstruction-staging-descriptor.v1"
)

SOURCE_STAGE_ARTIFACT_KIND = "reconstruction_source_stage_v1"
PROPOSAL_STAGE_ARTIFACT_KIND = "reconstruction_proposal_stage_v1"
CARVING_STAGE_ARTIFACT_KIND = "reconstruction_carving_stage_v1"
CROSS_STAGE_ARTIFACT_KIND = "reconstruction_cross_stage_v1"
DELIVERY_STAGE_ARTIFACT_KIND = "reconstruction_delivery_stage_v1"
VALIDATION_STAGE_ARTIFACT_KIND = "reconstruction_validation_stage_v1"
STAGING_DESCRIPTOR_ARTIFACT_KIND = "reconstruction_staging_descriptor_v1"

_SOURCE_INPUT_STREAM_KIND = "reconstruction_observed_input_stream_v1"
_SOURCE_CORE_STREAM_KIND = "reconstruction_observed_core_stream_v1"
_CANDIDATE_STREAM_KIND = "reconstruction_candidate_stream_v1"
_CANDIDATE_BATCH_LEDGER_KIND = "reconstruction_candidate_batch_ledger_v1"
_CARVED_STREAM_KIND = "reconstruction_carved_stream_v1"
_CARVED_BATCH_LEDGER_KIND = "reconstruction_carved_batch_ledger_v1"
_CROSS_STREAM_KIND = "reconstruction_cross_reconciled_stream_v1"
_DELIVERED_STREAM_KIND = "reconstruction_delivered_stream_v1"
_MAX_CANDIDATE_BATCH_LEDGER_LINE_BYTES = 1_048_576
_MAX_CARVED_BATCH_LEDGER_LINE_BYTES = 1_048_576
_MAX_SYNCHRONIZATION_TIMESTAMP_PROBES = 4_096

_SourceRow = tuple[int, float, float, str, int, str]

_HANDLERS = {
    ReconstructionStage.SOURCE_ENRICHMENT: "source_enrichment_handler",
    ReconstructionStage.PROPOSAL: "proposal_handler",
    ReconstructionStage.CARVING: "carving_handler",
    ReconstructionStage.CROSS_SERIES_RECONCILIATION: (
        "cross_series_reconciliation_handler"
    ),
    ReconstructionStage.BROKER_TRANSFER: "delivery_projection_handler",
    ReconstructionStage.VALIDATION: "validation_handler",
    ReconstructionStage.ATOMIC_PARTITION_COMMIT: "atomic_commit_handler",
}


def register_first_party_reconstruction_handlers() -> None:
    """Idempotently install every versioned first-party stage adapter."""
    current = registered_reconstruction_stage_handlers()
    namespace = globals()
    for stage, function_name in _HANDLERS.items():
        name = FIRST_PARTY_RECONSTRUCTION_HANDLERS[stage]
        handler = cast(Any, namespace[function_name])
        existing = current.get(name)
        if existing is handler:
            continue
        if existing is not None:
            raise ValueError(
                f"reconstruction stage handler already registered: {name}"
            )
        register_reconstruction_stage_handler(name, handler)


def source_enrichment_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Resolve immutable ASCII anchors and bounded context sidecars."""
    started = time.perf_counter()
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        window = invocation.task.window
        source_events = _read_source_events(invocation, plan)
        if any(len(values) < 2 for values in source_events.values()):
            return invocation.refused(
                "source_corruption",
                message="complete triangle input requires two anchors per symbol",
            )
        core_events = {
            symbol: tuple(
                event
                for event in events
                if window.owns_event_time(event.event_time_ns)
            )
            for symbol, events in source_events.items()
        }
        if any(not values for values in core_events.values()):
            return invocation.refused(
                "source_corruption",
                message="complete triangle core is empty for at least one symbol",
            )
        try:
            context, positioning = _window_context(plan, invocation)
            observation_operator = read_observation_operator_artifact(
                plan.execution_manifest.artifacts["observation_operator"]
            )
            if (
                invocation.task.window.left_halo_ns
                < observation_operator.required_left_halo_ns
            ):
                raise ValueError(
                    "window halo is shorter than observation operator support"
                )
            conditions = _motif_conditions(
                plan,
                invocation,
                source_events,
                context=context,
                positioning=positioning,
            )
            cross_condition = _cross_condition(
                invocation, conditions, context=context
            )
        except (OSError, ValueError, TypeError) as err:
            return invocation.refused(
                "source_context_unsupported",
                message=_bounded_error(err),
            )
        input_refs = _write_streams(
            invocation,
            source_events,
            directory_name="source-input",
            kind=_SOURCE_INPUT_STREAM_KIND,
        )
        core_refs = _write_streams(
            invocation,
            core_events,
            directory_name="source-core",
            kind=_SOURCE_CORE_STREAM_KIND,
        )
        anchor_hash = _events_content_sha256(
            event for values in core_events.values() for event in values
        )
        payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "input_stream_refs": _refs_dict(input_refs),
            "core_stream_refs": _refs_dict(core_refs),
            "market_context": context.to_dict(),
            "cftc_positioning": positioning.to_dict(),
            "observation_operator_id": observation_operator.operator_id,
            "motif_conditions": {
                symbol: condition.to_dict()
                for symbol, condition in sorted(conditions.items())
            },
            "cross_condition": cross_condition.to_dict(),
            "immutable_anchor_content_sha256": anchor_hash,
            "source_row_identity": {
                "inventory_basis": "zero-based-arrow-row-ordinal-v1",
                "event_contract_mapping": "source_row_id=arrow_ordinal+1",
            },
        }
        manifest = _write_json_artifact(
            invocation,
            "source",
            SOURCE_STAGE_ARTIFACT_KIND,
            payload,
            metadata={"immutable_anchor_content_sha256": anchor_hash},
        )
        observed = sum(len(values) for values in core_events.values())
        return _completed(
            invocation,
            manifest,
            started=started,
            observed=observed,
            message="resolved immutable source, feed epoch, calendar, and CFTC context",
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "source_corruption",
            message=_bounded_error(err),
        )


def proposal_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Run deterministic empirical motif retrieval and proposal generation."""
    started = time.perf_counter()
    ledger_stream: Any | None = None
    ledger_temporary: Path | None = None
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        source = _prior_manifest(invocation, SOURCE_STAGE_ARTIFACT_KIND)
        streams = _read_stream_map(source, "input_stream_refs")
        index = read_modern_reference_motif_index(
            plan.execution_manifest.artifacts["motif_index"].path
        )
        conditions = {
            symbol: ReferenceMotifConditionV1.from_dict(_mapping(value))
            for symbol, value in _mapping(source["motif_conditions"]).items()
        }
        synchronization_anchor_symbol = _proposal_synchronization_anchor_symbol(
            streams,
            conditions=conditions,
        )
        synchronization_event_time_ns = _proposal_synchronization_event_time(
            streams,
            conditions=conditions,
            start_ns=invocation.task.window.core_start_ns,
            end_ns=invocation.task.window.core_end_ns,
        )
        ledger_directory = _stage_directory(invocation, "proposal-batches")
        ledger_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".candidate-batches-",
            suffix=".ndjson",
            dir=ledger_directory,
        )
        ledger_temporary = Path(temporary_name)
        ledger_stream = os.fdopen(descriptor, "wb")
        ledger_digest = hashlib.sha256()
        batch_count = 0
        candidate_events: dict[str, list[SyntheticEventV1]] = {
            symbol: [] for symbol in invocation.run.symbols
        }
        generated = 0
        refused = 0
        interval_count = sum(
            max(0, len(stream.events) - 1) for stream in streams.values()
        )
        completed_intervals = 0
        for symbol, stream in sorted(streams.items()):
            condition = conditions[symbol]
            for left, right in zip(stream.events, stream.events[1:]):
                _cancel_if_requested(invocation)
                if (
                    right.event_time_ns <= invocation.task.window.core_start_ns
                    or left.event_time_ns >= invocation.task.window.core_end_ns
                ):
                    completed_intervals += 1
                    continue
                query = ReferenceMotifQueryV1(
                    condition=condition,
                    information_mode=(
                        plan.configuration.information_policy.information_mode
                    ),
                    used_at_ns=right.event_time_ns,
                    as_of_ns=(
                        invocation.task.window.core_start_ns
                        if plan.configuration.information_policy.information_mode.value
                        == "ex_ante_simulation"
                        else None
                    ),
                    max_results=index.config.max_matches,
                )
                result = query_reference_motifs(index, query)
                batch = generate_empirical_motif_candidates(
                    run=invocation.run,
                    window=invocation.task.window,
                    left_anchor=left,
                    right_anchor=right,
                    query_result=result,
                    config=plan.configuration.generator_config,
                    required_event_time_ns=(
                        synchronization_event_time_ns
                        if left.event_time_ns
                        < synchronization_event_time_ns
                        < right.event_time_ns
                        else None
                    ),
                )
                encoded_evidence = (
                    canonical_contract_json(_candidate_evidence(batch)).encode(
                        "utf-8"
                    )
                    + b"\n"
                )
                if (
                    len(encoded_evidence)
                    > _MAX_CANDIDATE_BATCH_LEDGER_LINE_BYTES
                ):
                    raise ValueError("candidate batch ledger row exceeds limit")
                ledger_stream.write(encoded_evidence)
                ledger_digest.update(encoded_evidence)
                batch_count += 1
                candidate_events[symbol].extend(batch.events)
                generated += len(batch.events)
                refused += int(batch.status.value == "refused")
                completed_intervals += 1
                if (
                    completed_intervals
                    % max(
                        1, invocation.run.storage_policy.heartbeat_every_batches
                    )
                    == 0
                ):
                    invocation.heartbeat(
                        sequence=completed_intervals,
                        completed_units=completed_intervals,
                        total_units=interval_count,
                        candidate_event_count=generated,
                        scratch_bytes=_tree_size(
                            invocation.task.scratch_directory
                        ),
                        message="empirical motif proposal",
                    )
        ledger_stream.flush()
        os.fsync(ledger_stream.fileno())
        ledger_stream.close()
        ledger_stream = None
        ledger_sha256 = ledger_digest.hexdigest()
        ledger_target = ledger_directory / (
            f"{_CANDIDATE_BATCH_LEDGER_KIND}-{ledger_sha256}.ndjson"
        )
        if ledger_target.exists():
            if _file_sha256(ledger_target) != ledger_sha256:
                raise ValueError("content-addressed batch ledger collision")
            ledger_temporary.unlink()
        else:
            os.replace(ledger_temporary, ledger_target)
        ledger_temporary = None
        ledger_ref = artifact_ref_for_file(
            ledger_target,
            kind=_CANDIDATE_BATCH_LEDGER_KIND,
            metadata={
                "batch_count": batch_count,
                "format": "canonical-json-lines-v1",
                "large_event_rows_inline": False,
            },
        )
        candidate_refs = _write_streams(
            invocation,
            candidate_events,
            directory_name="proposal-candidates",
            kind=_CANDIDATE_STREAM_KIND,
        )
        payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "batch_ledger_ref": ledger_ref.to_dict(),
            "batches_inline": False,
            "query_conditions": {
                symbol: condition.to_dict()
                for symbol, condition in sorted(conditions.items())
            },
            "generator_config": plan.configuration.generator_config.to_dict(),
            "candidate_stream_refs": _refs_dict(candidate_refs),
            "batch_count": batch_count,
            "generated_event_count": generated,
            "refused_interval_count": refused,
            "synchronization_anchor_symbol": synchronization_anchor_symbol,
            "synchronization_event_time_ns": synchronization_event_time_ns,
            "motif_index_id": index.index_id,
            "immutable_anchor_content_sha256": source[
                "immutable_anchor_content_sha256"
            ],
        }
        manifest = _write_json_artifact(
            invocation,
            "proposal",
            PROPOSAL_STAGE_ARTIFACT_KIND,
            payload,
            metadata={
                "batch_count": batch_count,
                "rejected_event_count": refused,
                "immutable_anchor_content_sha256": source[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        return _completed(
            invocation,
            manifest,
            started=started,
            observed=sum(
                stream.observed_event_count for stream in streams.values()
            ),
            candidates=generated,
            rejected=refused,
            message="generated empirical motif proposals",
            output_bytes=(manifest.size_bytes or 0)
            + (ledger_ref.size_bytes or 0)
            + sum(ref.size_bytes or 0 for ref in candidate_refs.values()),
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "proposal_validation_failed", message=_bounded_error(err)
        )
    finally:
        if ledger_stream is not None:
            ledger_stream.close()
        if ledger_temporary is not None:
            ledger_temporary.unlink(missing_ok=True)


def carving_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Apply historical carving and materialize accepted narrow streams."""
    started = time.perf_counter()
    ledger_stream: Any | None = None
    ledger_temporary: Path | None = None
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        source = _prior_manifest(invocation, SOURCE_STAGE_ARTIFACT_KIND)
        proposal = _prior_manifest(invocation, PROPOSAL_STAGE_ARTIFACT_KIND)
        input_streams = _read_stream_map(source, "input_stream_refs")
        core_streams = _read_stream_map(source, "core_stream_refs")
        context = MarketContextQueryV1.from_dict(
            _mapping(source["market_context"])
        )
        index = read_modern_reference_motif_index(
            plan.execution_manifest.artifacts["motif_index"].path
        )
        batch_count = int(proposal.get("batch_count", 0))
        batches = _restore_candidate_batches(proposal, index=index)
        ledger_directory = _stage_directory(invocation, "carving-batches")
        ledger_directory.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".carved-batches-",
            suffix=".ndjson",
            dir=ledger_directory,
        )
        ledger_temporary = Path(temporary_name)
        ledger_stream = os.fdopen(descriptor, "wb")
        ledger_digest = hashlib.sha256()
        carved_batch_count = 0
        accepted_by_symbol: dict[str, list[SyntheticEventV1]] = {
            symbol: [] for symbol in invocation.run.symbols
        }
        candidates = 0
        accepted = 0
        rejected = 0
        for ordinal, batch in enumerate(batches, start=1):
            _cancel_if_requested(invocation)
            carved = carve_empirical_motif_candidates(
                run=invocation.run,
                window=invocation.task.window,
                candidate_batch=batch,
                observed_events=input_streams[batch.symbol].events,
                market_context=context,
                constraints=plan.configuration.carving_constraints,
                fingerprint_evidence=None,
            )
            accepted_by_symbol[batch.symbol].extend(carved.accepted_events)
            candidates += len(batch.events)
            accepted += len(carved.accepted_events)
            rejected += carved.rejection_summary.rejected_count
            encoded_evidence = (
                canonical_contract_json(_carved_evidence(carved)).encode(
                    "utf-8"
                )
                + b"\n"
            )
            if len(encoded_evidence) > _MAX_CARVED_BATCH_LEDGER_LINE_BYTES:
                raise ValueError("carved batch ledger row exceeds limit")
            ledger_stream.write(encoded_evidence)
            ledger_digest.update(encoded_evidence)
            carved_batch_count += 1
            if (
                ordinal
                % max(1, invocation.run.storage_policy.heartbeat_every_batches)
                == 0
            ):
                invocation.heartbeat(
                    sequence=ordinal,
                    completed_units=ordinal,
                    total_units=batch_count,
                    candidate_event_count=candidates,
                    accepted_event_count=accepted,
                    scratch_bytes=_tree_size(invocation.task.scratch_directory),
                    message="historical candidate carving",
                )
        if carved_batch_count != batch_count:
            raise ValueError("carved batch count differs from proposal")
        ledger_stream.flush()
        os.fsync(ledger_stream.fileno())
        ledger_stream.close()
        ledger_stream = None
        ledger_sha256 = ledger_digest.hexdigest()
        ledger_target = ledger_directory / (
            f"{_CARVED_BATCH_LEDGER_KIND}-{ledger_sha256}.ndjson"
        )
        if ledger_target.exists():
            if _file_sha256(ledger_target) != ledger_sha256:
                raise ValueError("content-addressed carved ledger collision")
            ledger_temporary.unlink()
        else:
            os.replace(ledger_temporary, ledger_target)
        ledger_temporary = None
        ledger_ref = artifact_ref_for_file(
            ledger_target,
            kind=_CARVED_BATCH_LEDGER_KIND,
            metadata={
                "batch_count": carved_batch_count,
                "format": "canonical-json-lines-v1",
                "large_event_rows_inline": False,
            },
        )
        merged = {
            symbol: SyntheticEventStreamV1.merge(
                run_id=invocation.run.run_id,
                ensemble_member_id=invocation.task.window.ensemble_member_id,
                symbol=symbol,
                observed_events=core_streams[symbol].events,
                synthetic_events=accepted_by_symbol[symbol],
                source_version_ids=invocation.run.source_version_ids,
            )
            for symbol in invocation.run.symbols
        }
        stream_refs = _write_streams(
            invocation,
            {symbol: stream.events for symbol, stream in merged.items()},
            directory_name="carved",
            kind=_CARVED_STREAM_KIND,
        )
        payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "stream_refs": _refs_dict(stream_refs),
            "carved_batch_ledger_ref": ledger_ref.to_dict(),
            "carved_batches_inline": False,
            "carved_batch_count": carved_batch_count,
            "candidate_event_count": candidates,
            "accepted_event_count": accepted,
            "rejected_event_count": rejected,
            "immutable_anchor_content_sha256": source[
                "immutable_anchor_content_sha256"
            ],
        }
        manifest = _write_json_artifact(
            invocation,
            "carving",
            CARVING_STAGE_ARTIFACT_KIND,
            payload,
            metadata={
                "rejected_event_count": rejected,
                "immutable_anchor_content_sha256": source[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        return _completed(
            invocation,
            manifest,
            started=started,
            observed=sum(item.observed_event_count for item in merged.values()),
            candidates=candidates,
            accepted=accepted,
            rejected=rejected,
            message="carved candidate events and materialized core streams",
            output_bytes=(manifest.size_bytes or 0)
            + (ledger_ref.size_bytes or 0)
            + sum(ref.size_bytes or 0 for ref in stream_refs.values()),
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "carving_validation_failed", message=_bounded_error(err)
        )
    finally:
        if ledger_stream is not None:
            ledger_stream.close()
        if ledger_temporary is not None:
            ledger_temporary.unlink(missing_ok=True)


def cross_series_reconciliation_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Reconcile the synchronized FX triangle without forward-filled joins."""
    started = time.perf_counter()
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        source = _prior_manifest(invocation, SOURCE_STAGE_ARTIFACT_KIND)
        carving = _prior_manifest(invocation, CARVING_STAGE_ARTIFACT_KIND)
        streams = _read_stream_map(carving, "stream_refs")
        condition = CrossCurrencyConditionV1.from_dict(
            _mapping(source["cross_condition"])
        )
        group = reconcile_cross_currency_window(
            run=invocation.run,
            window=invocation.task.window,
            streams=streams,
            config=plan.configuration.cross_currency_config,
            conditions=(condition,),
        )
        if group.status is not CrossCurrencyGroupStatus.RECONCILED:
            return invocation.refused(
                *_bounded_reasons(
                    group.generation_validation.failure_reasons,
                    fallback="invalid_triangle_group",
                ),
                message="cross-currency reconciliation refused the group",
            )
        stream_refs = _write_streams(
            invocation,
            {stream.symbol: stream.events for stream in group.streams},
            directory_name="cross",
            kind=_CROSS_STREAM_KIND,
        )
        payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "stream_refs": _refs_dict(stream_refs),
            "group": _group_without_streams(group),
            "immutable_anchor_content_sha256": carving[
                "immutable_anchor_content_sha256"
            ],
        }
        manifest = _write_json_artifact(
            invocation,
            "cross",
            CROSS_STAGE_ARTIFACT_KIND,
            payload,
            metadata={
                "projection_count": len(group.projection_lineage),
                "generation_validation_id": (
                    group.generation_validation.validation_id
                ),
                "immutable_anchor_content_sha256": carving[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        observed = sum(item.observed_event_count for item in group.streams)
        synthetic = sum(item.synthetic_event_count for item in group.streams)
        return _completed(
            invocation,
            manifest,
            started=started,
            observed=observed,
            candidates=synthetic,
            accepted=synthetic,
            message="reconciled complete synchronized triangle",
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "invalid_triangle_group", message=_bounded_error(err)
        )


def delivery_projection_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Apply explicit modern-reference identity delivery."""
    started = time.perf_counter()
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        if (
            plan.configuration.delivery_mode
            is not ReconstructionDeliveryMode.MODERN_REFERENCE
        ):
            return invocation.refused(
                "unsupported_delivery_mode",
                message="first-party v2.1 reference handler is identity-only",
            )
        cross = _prior_manifest(invocation, CROSS_STAGE_ARTIFACT_KIND)
        group = _restore_cross_group(cross)
        delivered = project_modern_reference_delivery(
            group,
            delivery_profile_id=(
                "modern-reference:" + plan.configuration.configuration_id
            ),
        )
        stream_refs = _write_streams(
            invocation,
            {stream.symbol: stream.events for stream in delivered.streams},
            directory_name="delivery",
            kind=_DELIVERED_STREAM_KIND,
        )
        payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "stream_refs": _refs_dict(stream_refs),
            "delivery_manifest": delivered.manifest.to_dict(),
            "immutable_anchor_content_sha256": cross[
                "immutable_anchor_content_sha256"
            ],
        }
        manifest = _write_json_artifact(
            invocation,
            "delivery",
            DELIVERY_STAGE_ARTIFACT_KIND,
            payload,
            metadata={
                "delivery_mode": delivered.manifest.delivery_mode.value,
                "identity_event_count": delivered.manifest.identity_event_count,
                "immutable_anchor_content_sha256": cross[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        observed = delivered.manifest.observed_event_count
        synthetic = delivered.manifest.synthetic_event_count
        return _completed(
            invocation,
            manifest,
            started=started,
            observed=observed,
            candidates=synthetic,
            accepted=synthetic,
            message="applied explicit modern-reference identity delivery",
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "delivery_projection_failed", message=_bounded_error(err)
        )


def validation_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Enforce scientific gates, then stage an atomic v2 publication."""
    started = time.perf_counter()
    staged: StagedReconstructionPublicationV2 | None = None
    try:
        _cancel_if_requested(invocation)
        plan = load_reconstruction_stage_plan(invocation.command)
        source = _prior_manifest(invocation, SOURCE_STAGE_ARTIFACT_KIND)
        delivery = _prior_manifest(invocation, DELIVERY_STAGE_ARTIFACT_KIND)
        delivered = _restore_delivered_group(delivery)
        core_streams = _read_stream_map(source, "core_stream_refs")
        anchors = tuple(
            event
            for stream in core_streams.values()
            for event in stream.events
            if event.origin is SyntheticEventOrigin.OBSERVED
        )
        final_validation = validate_cross_currency_output(
            run=invocation.run,
            window=invocation.task.window,
            streams={item.symbol: item for item in delivered.streams},
            config=plan.configuration.cross_currency_config,
            stage=CrossCurrencyValidationStage.POST_BROKER,
            observed_anchors=anchors,
            conditions=(
                CrossCurrencyConditionV1.from_dict(
                    _mapping(source["cross_condition"])
                ),
            ),
        )
        if not final_validation.passed:
            return invocation.refused(
                *_bounded_reasons(
                    final_validation.failure_reasons,
                    fallback="final_validation_failed",
                ),
                message="final cross-instrument validation failed",
            )
        benchmark_evidence = _validate_scientific_evidence(plan)
        retention_ref = plan.execution_manifest.artifacts["retention_plan"]
        retention = ReconstructionRetentionPlanV1.from_dict(
            _mapping(
                json.loads(Path(retention_ref.path).read_text(encoding="utf-8"))
            )
        )
        _cancel_if_requested(invocation)
        staged = stage_delivery_reconstruction_publication(
            plan.execution_manifest.output_root,
            delivered,
            final_validation=final_validation,
            benchmark_artifact_ids=tuple(
                ref.sha256
                for name, ref in sorted(
                    plan.execution_manifest.artifacts.items()
                )
                if name
                in {
                    "benchmark_manifest",
                    "motif_qualification",
                    "motif_leakage_audit",
                    "information_audit",
                }
            ),
            benchmark_evidence=benchmark_evidence,
            immutable_source_anchors=anchors,
            symbol_group_id=invocation.task.window.synchronization_unit_id,
            retention_plan=retention,
            storage_policy=plan.configuration.storage_policy,
            staging_root=_stage_directory(invocation, "publication"),
        )
        _cancel_if_requested(invocation)
        descriptor_payload: dict[str, JSONValue] = {
            **_stage_scope(invocation),
            "schema_version": RECONSTRUCTION_STAGING_DESCRIPTOR_SCHEMA_VERSION,
            "root": str(staged.root),
            "staging_directory": str(staged.staging_directory),
            "committed_directory": str(staged.committed_directory),
            "product_manifest": staged.manifest.to_dict(),
            "final_validation": final_validation.to_dict(),
            "immutable_anchor_content_sha256": source[
                "immutable_anchor_content_sha256"
            ],
        }
        descriptor = _write_json_artifact(
            invocation,
            "validation",
            STAGING_DESCRIPTOR_ARTIFACT_KIND,
            descriptor_payload,
            metadata={
                "publication_id": staged.manifest.publication_id,
                "manifest_id": staged.manifest.manifest_id,
                "immutable_anchor_content_sha256": source[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        staged_manifest_ref = _write_product_manifest_mirror(
            invocation, staged.manifest
        )
        staged_manifest_ref = replace(
            staged_manifest_ref,
            metadata={
                **staged_manifest_ref.metadata,
                "immutable_anchor_content_sha256": source[
                    "immutable_anchor_content_sha256"
                ],
            },
        )
        observed = delivered.manifest.observed_event_count
        synthetic = delivered.manifest.synthetic_event_count
        return _completed(
            invocation,
            staged_manifest_ref,
            started=started,
            observed=observed,
            candidates=synthetic,
            accepted=synthetic,
            message="passed scientific gates and staged atomic publication",
            additional_refs=(descriptor,),
            output_bytes=(
                _tree_size(staged.staging_directory)
                + (descriptor.size_bytes or 0)
                + (staged_manifest_ref.size_bytes or 0)
            ),
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        if staged is not None and staged.staging_directory.exists():
            shutil.rmtree(staged.staging_directory)
        return invocation.refused(
            "final_validation_failed", message=_bounded_error(err)
        )


def atomic_commit_handler(
    invocation: ReconstructionStageInvocationV1,
) -> ReconstructionStageOutcomeV1:
    """Promote or recover one already-promoted atomic publication."""
    started = time.perf_counter()
    try:
        _cancel_if_requested(invocation)
        descriptor_ref = _prior_ref(
            invocation, STAGING_DESCRIPTOR_ARTIFACT_KIND
        )
        descriptor = _read_json_ref(descriptor_ref)
        _require_schema(
            descriptor, RECONSTRUCTION_STAGING_DESCRIPTOR_SCHEMA_VERSION
        )
        manifest = ReconstructionProductManifestV2.from_dict(
            _mapping(descriptor["product_manifest"])
        )
        staged = StagedReconstructionPublicationV2(
            root=Path(str(descriptor["root"])).resolve(),
            staging_directory=Path(
                str(descriptor["staging_directory"])
            ).resolve(),
            committed_directory=Path(
                str(descriptor["committed_directory"])
            ).resolve(),
            manifest=manifest,
        )
        _cancel_if_requested(invocation)
        published = _commit_or_recover(staged)
        committed_ref = replace(
            published.manifest_ref,
            metadata={
                **published.manifest_ref.metadata,
                "commit_phase": "committed",
                "idempotent_retry": published.idempotent_retry,
            },
        )
        return _completed(
            invocation,
            committed_ref,
            started=started,
            observed=published.manifest.observed_event_count,
            candidates=published.manifest.synthetic_event_count,
            accepted=published.manifest.synthetic_event_count,
            message=(
                "recovered already committed atomic publication"
                if published.idempotent_retry
                else "committed atomic Parquet publication"
            ),
            output_bytes=sum(
                item.size_bytes for item in published.manifest.partitions
            )
            + published.manifest_path.stat().st_size,
        )
    except asyncio.CancelledError:
        raise
    except (OSError, ValueError, TypeError, OverflowError) as err:
        return invocation.refused(
            "atomic_commit_failed", message=_bounded_error(err)
        )


def _read_source_events(
    invocation: ReconstructionStageInvocationV1,
    plan: ReconstructionStagePlanV1,
) -> dict[str, tuple[SyntheticEventV1, ...]]:
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as err:  # pragma: no cover - package dependency
        raise RuntimeError(
            "first-party reconstruction requires pyarrow"
        ) from err
    window = invocation.task.window
    raw: dict[str, list[_SourceRow]] = {
        symbol: [] for symbol in invocation.run.symbols
    }
    selected = plan.source_inventory.partitions_for_window(window)
    for partition in selected:
        _cancel_if_requested(invocation)
        verify_artifact_ref(partition.artifact)
        with pa.memory_map(partition.artifact.path, "r") as source:
            reader = ipc.open_file(source)
            dt_index = reader.schema.get_field_index("datetime")
            bid_index = reader.schema.get_field_index("bid")
            ask_index = reader.schema.get_field_index("ask")
            if min(dt_index, bid_index, ask_index) < 0:
                raise ValueError("source partition lacks datetime/bid/ask")
            ordinal = 0
            series_id = (
                f"ascii-tick:{partition.symbol}:{partition.period}:"
                f"sha256:{partition.artifact.sha256}"
            )
            for batch_ordinal in range(reader.num_record_batches):
                batch = reader.get_batch(batch_ordinal)
                times = batch.column(dt_index).to_pylist()
                bids = batch.column(bid_index).to_pylist()
                asks = batch.column(ask_index).to_pylist()
                for timestamp_ms, bid_raw, ask_raw in zip(times, bids, asks):
                    timestamp_ns = int(timestamp_ms) * 1_000_000
                    bid = float(bid_raw)
                    ask = float(ask_raw)
                    if (
                        not math.isfinite(bid)
                        or not math.isfinite(ask)
                        or bid <= 0.0
                        or ask <= 0.0
                        or ask < bid
                    ):
                        raise ValueError(
                            f"invalid source quote {partition.symbol} "
                            f"{partition.period} row {ordinal}"
                        )
                    if window.reads_event_time(timestamp_ns):
                        raw[partition.symbol].append(
                            (
                                timestamp_ns,
                                bid,
                                ask,
                                partition.period,
                                ordinal,
                                series_id,
                            )
                        )
                    ordinal += 1
    result: dict[str, tuple[SyntheticEventV1, ...]] = {}
    source_version = invocation.run.source_version_ids[0]
    for symbol, values in raw.items():
        counters: Counter[int] = Counter()
        events: list[SyntheticEventV1] = []
        for timestamp, bid, ask, period, ordinal, series_id in sorted(
            values, key=_source_row_order_key
        ):
            sequence = counters[timestamp]
            counters[timestamp] += 1
            events.append(
                SyntheticEventV1.observed(
                    symbol=symbol,
                    event_time_ns=timestamp,
                    event_sequence=sequence,
                    bid=bid,
                    ask=ask,
                    run_id=invocation.run.run_id,
                    ensemble_member_id=window.ensemble_member_id,
                    source_version_id=source_version,
                    source_series_id=series_id,
                    source_period=period,
                    source_row_id=ordinal + 1,
                )
            )
        result[symbol] = tuple(events)
    return result


def _source_row_order_key(row: _SourceRow) -> tuple[int, str, int, str]:
    """Preserve immutable partition/Arrow order for equal timestamps."""
    timestamp, _, _, period, ordinal, series_id = row
    return (timestamp, period, ordinal, series_id)


def _proposal_synchronization_event_time(
    streams: Mapping[str, SyntheticEventStreamV1],
    *,
    conditions: Mapping[str, ReferenceMotifConditionV1],
    start_ns: int,
    end_ns: int,
) -> int:
    if set(streams) != set(conditions):
        raise ValueError(
            "proposal synchronization conditions differ from streams"
        )
    if any(not stream.events for stream in streams.values()):
        raise ValueError("triangle stream lacks synchronization anchors")
    lower = max(
        start_ns,
        *(stream.events[0].event_time_ns for stream in streams.values()),
    )
    upper = min(
        end_ns,
        *(stream.events[-1].event_time_ns for stream in streams.values()),
    )
    if lower >= upper:
        raise ValueError(
            "triangle streams have no common synchronization interval"
        )
    probes: set[int] = set()
    probes_per_stream = max(
        1,
        _MAX_SYNCHRONIZATION_TIMESTAMP_PROBES // len(streams),
    )
    anchor_symbol = _proposal_synchronization_anchor_symbol(
        streams,
        conditions=conditions,
    )
    for stream in (streams[anchor_symbol],):
        event_count = len(stream.events)
        sample_count = min(event_count, probes_per_stream)
        indices: tuple[int, ...]
        if sample_count == 1:
            indices = (event_count // 2,)
        else:
            indices = tuple(
                ordinal * (event_count - 1) // (sample_count - 1)
                for ordinal in range(sample_count)
            )
        for index in indices:
            selected = stream.events[index].event_time_ns
            if lower <= selected < upper:
                probes.add(selected)

    center_twice = lower + upper
    best: tuple[tuple[int, int, int, int, int], int] | None = None
    for selected in probes:
        exact_support = 0
        missing_interval_widths: list[int] = []
        for stream in streams.values():
            position = bisect_left(
                stream.events,
                selected,
                key=lambda event: event.event_time_ns,
            )
            if (
                position < len(stream.events)
                and stream.events[position].event_time_ns == selected
            ):
                exact_support += 1
                continue
            if position == 0 or position == len(stream.events):
                break
            missing_interval_widths.append(
                stream.events[position].event_time_ns
                - stream.events[position - 1].event_time_ns
            )
        else:
            if exact_support == len(streams):
                support_penalty = 2
            elif exact_support == 1:
                support_penalty = 0
            else:
                support_penalty = 1
            score = (
                support_penalty,
                max(missing_interval_widths, default=0),
                sum(missing_interval_widths),
                abs(2 * selected - center_twice),
                selected,
            )
            if best is None or score < best[0]:
                best = (score, selected)
    if best is None:
        raise ValueError(
            "triangle streams have no bounded synchronization support"
        )
    return best[1]


def _proposal_synchronization_anchor_symbol(
    streams: Mapping[str, SyntheticEventStreamV1],
    *,
    conditions: Mapping[str, ReferenceMotifConditionV1],
) -> str:
    if set(streams) != set(conditions):
        raise ValueError(
            "proposal synchronization conditions differ from streams"
        )
    return min(
        streams,
        key=lambda symbol: (
            conditions[symbol].metrics.get("tick_intensity", math.inf),
            len(streams[symbol].events),
            symbol,
        ),
    )


def _window_context(
    plan: ReconstructionStagePlanV1,
    invocation: ReconstructionStageInvocationV1,
) -> tuple[MarketContextQueryV1, CftcPositioningQueryV1]:
    window = invocation.task.window
    context_corpus = read_market_context_corpus(
        plan.execution_manifest.artifacts["market_context"].path
    )
    requirements = (
        ("EUR", MarketContextKind.POLICY_RATE_CHANGE),
        ("GBP", MarketContextKind.POLICY_RATE_CHANGE),
        ("USD", MarketContextKind.CENTRAL_BANK_DECISION),
    )
    reasons: list[str] = []
    for currency, kind in requirements:
        decision = preflight_market_context_corpus(
            context_corpus,
            start_ns=window.core_start_ns,
            end_ns=window.core_end_ns,
            currencies=(currency,),
            kinds=(kind,),
        )
        reasons.extend(decision.reasons)
    if reasons:
        raise ValueError("unsupported market context: " + "; ".join(reasons))
    mode = plan.configuration.information_policy.information_mode
    view = (
        MarketContextView.EX_ANTE
        if mode.value == "ex_ante_simulation"
        else MarketContextView.EX_POST
    )
    context = query_market_context_corpus(
        context_corpus,
        start_ns=window.core_start_ns,
        end_ns=window.core_end_ns,
        view=view,
        as_of_ns=(
            window.core_start_ns if view is MarketContextView.EX_ANTE else None
        ),
        symbols=window.symbols,
        include_calendar=True,
        window_id=window.window_id,
        require_supported=False,
    )
    if context.calendar_state is None:
        raise ValueError("market context lacks calendar state")
    cftc_corpus = read_cftc_positioning_corpus(
        plan.execution_manifest.artifacts["cftc_positioning"].path
    )
    positioning = query_cftc_positioning_corpus(
        cftc_corpus,
        start_ns=window.core_start_ns,
        end_ns=window.core_end_ns,
        information_mode=mode,
        as_of_ns=(
            window.core_start_ns if mode.value == "ex_ante_simulation" else None
        ),
        symbols=window.symbols,
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    if positioning.status is not CftcPositioningQueryStatus.READY:
        raise ValueError("unsupported CFTC context: " + positioning.reason)
    return context, positioning


def _motif_conditions(
    plan: ReconstructionStagePlanV1,
    invocation: ReconstructionStageInvocationV1,
    source_events: Mapping[str, Sequence[SyntheticEventV1]],
    *,
    context: MarketContextQueryV1,
    positioning: CftcPositioningQueryV1,
) -> dict[str, ReferenceMotifConditionV1]:
    definition = read_active_time_feed_epoch_definition(
        plan.execution_manifest.artifacts["feed_epochs"].path
    )
    calendar = cast(Any, context.calendar_state)
    event_tags = (
        *calendar.event_tags,
        market_context_benchmark_event_state(context),
        cftc_positioning_state_label(positioning),
    )
    result: dict[str, ReferenceMotifConditionV1] = {}
    for symbol, events in sorted(source_events.items()):
        midpoint_ms = (
            (
                invocation.task.window.core_start_ns
                + invocation.task.window.core_end_ns
            )
            // 2
            // 1_000_000
        )
        assignment = definition.assign(
            symbol=symbol, timestamp_utc_ms=midpoint_ms
        )
        if assignment.assignment_kind == "out_of_scope":
            raise ValueError(f"unsupported feed epoch for {symbol}")
        result[symbol] = reference_motif_condition_from_quotes(
            symbol=symbol,
            feed_epoch_id=assignment.label,
            session_state=calendar.session_state,
            event_times_ns=tuple(event.event_time_ns for event in events),
            bids=tuple(event.bid for event in events),
            asks=tuple(event.ask for event in events),
            event_tags=event_tags,
            active_sessions=calendar.active_sessions,
            overlap_tags=calendar.overlaps,
            special_tags=calendar.special_tags,
            holiday_tags=calendar.holiday_tags,
        )
    return result


def _cross_condition(
    invocation: ReconstructionStageInvocationV1,
    conditions: Mapping[str, ReferenceMotifConditionV1],
    *,
    context: MarketContextQueryV1,
) -> CrossCurrencyConditionV1:
    calendar = cast(Any, context.calendar_state)
    return CrossCurrencyConditionV1(
        start_ns=invocation.task.window.core_start_ns,
        end_ns=invocation.task.window.core_end_ns,
        session_key=calendar.session_state,
        event_key=market_context_benchmark_event_state(context),
        feed_epoch_key="+".join(
            sorted({item.feed_epoch_id for item in conditions.values()})
        ),
    )


def _validate_scientific_evidence(
    plan: ReconstructionStagePlanV1,
) -> dict[str, JSONValue]:
    artifacts = plan.execution_manifest.artifacts
    benchmark = read_reverse_degradation_benchmark_corpus(
        artifacts["benchmark_manifest"].path
    )
    qualification = read_modern_reference_motif_artifact(
        artifacts["motif_qualification"].path, kind="qualification"
    )
    leakage = read_modern_reference_motif_artifact(
        artifacts["motif_leakage_audit"].path, kind="leakage-audit"
    )
    audit = InformationAuditReportV1.from_json(
        Path(artifacts["information_audit"].path).read_text(encoding="utf-8")
    )
    contracts = _mapping(qualification.get("real_window_contracts"))
    failures: list[str] = []
    if qualification.get("candidate_promotion_eligible") is not True:
        failures.append("motif candidate is not promotion eligible")
    if qualification.get("candidate_provisional") is not False:
        failures.append("motif candidate remains provisional")
    if not contracts or not all(value is True for value in contracts.values()):
        failures.append("motif real-window contracts are not all passing")
    for name in (
        "retained_nontrain_fragment_count",
        "retained_holdout_fragment_count",
        "post_exclusion_cross_split_finding_count",
    ):
        if leakage.get(name) != 0:
            failures.append(f"motif leakage evidence {name} is nonzero")
    if not audit.accepted or audit.total_violation_count:
        failures.append("reconstruction information audit is not accepted")
    if failures:
        raise ValueError("; ".join(failures))
    return {
        "benchmark_corpus_id": benchmark.corpus_id,
        "motif_library_id": str(qualification.get("library_id", "")),
        "motif_candidate_report_id": str(
            qualification.get("candidate_report_id", "")
        ),
        "information_audit_id": audit.audit_id,
        "information_violation_count": audit.total_violation_count,
        "leakage_cross_split_finding_count": cast(
            int, leakage["post_exclusion_cross_split_finding_count"]
        ),
    }


def _commit_or_recover(
    staged: StagedReconstructionPublicationV2,
) -> PublishedReconstructionV2:
    if staged.staging_directory.exists():
        return commit_delivery_reconstruction_publication(staged)
    manifest_path = staged.committed_directory / "manifest.json"
    if not manifest_path.exists():
        raise ValueError("neither staged nor committed publication exists")
    manifest = verify_reconstruction_publication(manifest_path)
    if not isinstance(manifest, ReconstructionProductManifestV2):
        raise ValueError("recovered publication is not a v2 product")
    if manifest != staged.manifest:
        raise ValueError("recovered publication differs from staged evidence")
    ref = artifact_ref_for_file(
        manifest_path, kind="reconstruction-product-manifest"
    )
    return PublishedReconstructionV2(
        manifest=manifest,
        manifest_path=manifest_path,
        manifest_ref=replace(
            ref,
            metadata={
                "schema_version": manifest.schema_version,
                "publication_id": manifest.publication_id,
                "manifest_id": manifest.manifest_id,
                "event_count": manifest.event_count,
                "logical_content_sha256": manifest.replay.logical_content_sha256,
            },
        ),
        idempotent_retry=True,
    )


def _restore_cross_group(
    manifest: Mapping[str, Any],
) -> CrossCurrencyReconciledGroupV1:
    payload = dict(_mapping(manifest["group"]))
    payload["streams"] = [
        stream.to_dict()
        for stream in _read_stream_map(manifest, "stream_refs").values()
    ]
    return CrossCurrencyReconciledGroupV1.from_dict(payload)


def _restore_delivered_group(
    manifest: Mapping[str, Any],
) -> ReconstructionDeliveredGroupV1:
    delivery = ReconstructionDeliveryManifestV1.from_dict(
        _mapping(manifest["delivery_manifest"])
    )
    streams = tuple(_read_stream_map(manifest, "stream_refs").values())
    return ReconstructionDeliveredGroupV1(manifest=delivery, streams=streams)


def _group_without_streams(
    group: CrossCurrencyReconciledGroupV1,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = group.to_dict()
    payload["streams"] = []
    return payload


def _carved_evidence(
    batch: HistoricalCarvedCandidateBatchV1,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = batch.metadata()
    return payload


def _candidate_evidence(
    batch: EmpiricalMotifCandidateBatchV1,
) -> dict[str, JSONValue]:
    """Serialize bounded batch lineage while large reusable rows stay external."""
    return {
        "schema_version": batch.schema_version,
        "run_id": batch.run_id,
        "window_id": batch.window_id,
        "ensemble_member_id": batch.ensemble_member_id,
        "symbol": batch.symbol,
        "anchor_interval_id": batch.anchor_interval_id,
        "left_anchor_event_id": batch.left_anchor_event_id,
        "right_anchor_event_id": batch.right_anchor_event_id,
        "generator_config_id": batch.generator_config.config_id,
        "query_evidence": _compact_query_evidence(batch.query_result),
        "status": batch.status.value,
        "decision": batch.decision.value,
        "target_event_count": batch.target_event_count,
        "transformations": [item.to_dict() for item in batch.transformations],
        "event_lineage": [item.to_dict() for item in batch.event_lineage],
        "resource_estimate": batch.resource_estimate.to_dict(),
        "carry_state": batch.carry_state.to_dict(),
        "decision_details": list(batch.decision_details),
        "batch_id": batch.batch_id,
        "candidate_events_inline": False,
    }


def _restore_candidate_batches(
    manifest: Mapping[str, Any],
    *,
    index: ReferenceMotifIndexV1,
) -> Iterable[EmpiricalMotifCandidateBatchV1]:
    """Yield verified batches without retaining motif fragments per interval."""
    streams = _read_stream_map(manifest, "candidate_stream_refs")
    events_by_interval: dict[str, list[SyntheticEventV1]] = {}
    for stream in streams.values():
        for event in stream.events:
            interval_id = event.anchor_interval_id
            if interval_id is None:
                raise ValueError("candidate event lacks anchor interval")
            events_by_interval.setdefault(interval_id, []).append(event)
    available = {
        event.event_id for stream in streams.values() for event in stream.events
    }
    expected_batch_count = int(manifest.get("batch_count", -1))
    ledger_value = manifest.get("batch_ledger_ref")
    if ledger_value is not None:
        if manifest.get("batches_inline") is not False:
            raise ValueError("proposal batch ledger must remain external")
        ledger_ref = ArtifactRef.from_dict(_mapping(ledger_value))
        if ledger_ref.kind != _CANDIDATE_BATCH_LEDGER_KIND:
            raise ValueError("proposal batch ledger kind differs")
        verify_artifact_ref(ledger_ref)
        if ledger_ref.metadata.get("format") != "canonical-json-lines-v1":
            raise ValueError("proposal batch ledger format differs")
        if ledger_ref.metadata.get("batch_count") != expected_batch_count:
            raise ValueError("proposal batch ledger count metadata differs")
        values: Iterable[Any] = _read_candidate_batch_ledger(ledger_ref)
    else:
        inline_values = _sequence(manifest["batches"])
        if expected_batch_count != len(inline_values):
            raise ValueError(
                "candidate batch count differs from proposal manifest"
            )
        values = inline_values
    conditions = {
        str(symbol): ReferenceMotifConditionV1.from_dict(_mapping(value))
        for symbol, value in _mapping(
            manifest.get("query_conditions", {})
        ).items()
    }
    shared_config_value = manifest.get("generator_config")
    shared_config = (
        EmpiricalMotifGeneratorConfigV1.from_dict(_mapping(shared_config_value))
        if shared_config_value is not None
        else None
    )
    if manifest.get("motif_index_id") != index.index_id:
        raise ValueError("proposal motif index differs from current artifact")
    consumed: set[str] = set()
    restored_batch_count = 0
    for value in values:
        restored_batch_count += 1
        data = _mapping(value)
        if data.get("candidate_events_inline") is not False:
            raise ValueError("candidate evidence embeds event rows")
        interval_id = str(data.get("anchor_interval_id", ""))
        events = tuple(events_by_interval.get(interval_id, ()))
        consumed.update(event.event_id for event in events)
        symbol = str(data.get("symbol", ""))
        config = shared_config
        if config is None:
            config = EmpiricalMotifGeneratorConfigV1.from_dict(
                _mapping(data.get("generator_config"))
            )
        if (
            data.get("generator_config_id", config.config_id)
            != config.config_id
        ):
            raise ValueError("candidate generator config identity differs")
        query_result_value = data.get("query_result")
        if query_result_value is not None:
            query_result = ReferenceMotifQueryResultV1.from_dict(
                _mapping(query_result_value)
            )
            if query_result.index_id != index.index_id:
                raise ValueError("candidate query result index differs")
        else:
            condition = conditions.get(symbol)
            if condition is None:
                raise ValueError("candidate query condition is absent")
            query_result = _restore_compact_query_result(
                _mapping(data.get("query_evidence")),
                condition=condition,
                index=index,
            )
        yield EmpiricalMotifCandidateBatchV1(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbol=symbol,
            anchor_interval_id=interval_id,
            left_anchor_event_id=str(data.get("left_anchor_event_id", "")),
            right_anchor_event_id=str(data.get("right_anchor_event_id", "")),
            generator_config=config,
            query_result=query_result,
            status=MotifGenerationStatus(str(data.get("status", ""))),
            decision=MotifGenerationDecision(str(data.get("decision", ""))),
            target_event_count=int(data.get("target_event_count", 0)),
            events=events,
            transformations=tuple(
                EmpiricalMotifTransformationV1.from_dict(_mapping(item))
                for item in _sequence(data.get("transformations"))
            ),
            event_lineage=tuple(
                EmpiricalMotifEventLineageV1.from_dict(_mapping(item))
                for item in _sequence(data.get("event_lineage"))
            ),
            resource_estimate=ReconstructionResourceEstimateV1.from_dict(
                _mapping(data.get("resource_estimate"))
            ),
            carry_state=CarryStateV1.from_dict(
                _mapping(data.get("carry_state"))
            ),
            decision_details=tuple(
                str(item) for item in _sequence(data.get("decision_details"))
            ),
            batch_id=str(data.get("batch_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
    if consumed != available:
        raise ValueError("candidate Parquet rows do not reconcile with batches")
    if restored_batch_count != expected_batch_count:
        raise ValueError("candidate batch ledger row count differs")


def _read_candidate_batch_ledger(
    ref: ArtifactRef,
) -> Iterable[Mapping[str, Any]]:
    with Path(ref.path).open("rb") as stream:
        for line in stream:
            if len(line) > _MAX_CANDIDATE_BATCH_LEDGER_LINE_BYTES:
                raise ValueError("candidate batch ledger row exceeds limit")
            try:
                value = json.loads(line)
            except (UnicodeError, json.JSONDecodeError) as err:
                raise ValueError(
                    "candidate batch ledger row is invalid"
                ) from err
            yield _mapping(value)


def _compact_query_evidence(
    result: ReferenceMotifQueryResultV1,
) -> dict[str, JSONValue]:
    query = result.query
    return {
        "query_schema_version": query.schema_version,
        "information_mode": query.information_mode.value,
        "used_at_ns": query.used_at_ns,
        "as_of_ns": query.as_of_ns,
        "max_results": query.max_results,
        "min_cell_support": query.min_cell_support,
        "max_distance": query.max_distance,
        "excluded_source_window_ids": list(query.excluded_source_window_ids),
        "query_id": query.query_id,
        "index_id": result.index_id,
        "status": result.status.value,
        "result_id": result.result_id,
        "result_schema_version": result.schema_version,
        "retrieval_rows_inline": False,
    }


def _restore_compact_query_result(
    data: Mapping[str, Any],
    *,
    condition: ReferenceMotifConditionV1,
    index: ReferenceMotifIndexV1,
) -> ReferenceMotifQueryResultV1:
    if data.get("retrieval_rows_inline") is not False:
        raise ValueError("compact query evidence must not embed retrieval rows")
    if data.get("index_id") != index.index_id:
        raise ValueError("compact query evidence index differs")
    query = ReferenceMotifQueryV1(
        condition=condition,
        information_mode=InformationMode.from_value(
            str(data.get("information_mode", ""))
        ),
        used_at_ns=int(data.get("used_at_ns", 0)),
        as_of_ns=(
            int(data["as_of_ns"]) if data.get("as_of_ns") is not None else None
        ),
        max_results=int(data.get("max_results", 0)),
        min_cell_support=(
            int(data["min_cell_support"])
            if data.get("min_cell_support") is not None
            else None
        ),
        max_distance=(
            float(data["max_distance"])
            if data.get("max_distance") is not None
            else None
        ),
        excluded_source_window_ids=tuple(
            str(item)
            for item in _sequence(data.get("excluded_source_window_ids"))
        ),
        query_id=str(data.get("query_id", "")),
        schema_version=str(data.get("query_schema_version", "")),
    )
    result = query_reference_motifs(index, query)
    if (
        result.result_id != data.get("result_id")
        or result.status.value != data.get("status")
        or result.schema_version != data.get("result_schema_version")
    ):
        raise ValueError("replayed compact query evidence differs")
    return result


def _write_streams(
    invocation: ReconstructionStageInvocationV1,
    events_by_symbol: Mapping[str, Sequence[SyntheticEventV1]],
    *,
    directory_name: str,
    kind: str,
) -> dict[str, ArtifactRef]:
    refs: dict[str, ArtifactRef] = {}
    directory = _stage_directory(invocation, directory_name)
    directory.mkdir(parents=True, exist_ok=True)
    for symbol, events in sorted(events_by_symbol.items()):
        _cancel_if_requested(invocation)
        stream = SyntheticEventStreamV1(
            run_id=invocation.run.run_id,
            ensemble_member_id=invocation.task.window.ensemble_member_id,
            symbol=symbol,
            events=tuple(events),
            source_version_ids=invocation.run.source_version_ids,
        )
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{symbol.lower()}-", suffix=".parquet", dir=directory
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            write_synthetic_event_stream_parquet(stream, temporary)
            digest = _file_sha256(temporary)
            target = directory / f"{symbol.lower()}-{digest}.parquet"
            if target.exists():
                if _file_sha256(target) != digest:
                    raise ValueError("content-addressed stream collision")
                temporary.unlink()
            else:
                os.replace(temporary, target)
            refs[symbol] = artifact_ref_for_file(
                target,
                kind=kind,
                metadata={
                    "symbol": symbol,
                    "stream_id": stream.stream_id,
                    "event_count": len(stream.events),
                    "observed_event_count": stream.observed_event_count,
                    "synthetic_event_count": stream.synthetic_event_count,
                },
            )
        finally:
            temporary.unlink(missing_ok=True)
    return refs


def _read_stream_map(
    manifest: Mapping[str, Any], key: str
) -> dict[str, SyntheticEventStreamV1]:
    result: dict[str, SyntheticEventStreamV1] = {}
    for symbol, value in _mapping(manifest[key]).items():
        ref = ArtifactRef.from_dict(_mapping(value))
        verify_artifact_ref(ref)
        stream = read_synthetic_event_stream_parquet(ref.path)
        if stream.symbol != symbol:
            raise ValueError("stream reference symbol differs")
        result[symbol] = stream
    return result


def _write_json_artifact(
    invocation: ReconstructionStageInvocationV1,
    directory_name: str,
    kind: str,
    payload: Mapping[str, JSONValue],
    *,
    metadata: Mapping[str, JSONValue] | None = None,
) -> ArtifactRef:
    value: dict[str, JSONValue] = {
        "schema_version": RECONSTRUCTION_STAGE_ARTIFACT_SCHEMA_VERSION,
        **dict(payload),
    }
    encoded = canonical_contract_json(value).encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    directory = _stage_directory(invocation, directory_name)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{kind}-{digest}.json"
    if target.exists():
        if target.read_bytes() != encoded:
            raise ValueError("content-addressed stage artifact collision")
    else:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", dir=directory
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return ArtifactRef(
        kind=kind,
        path=str(target.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(metadata or {}),
    )


def _write_product_manifest_mirror(
    invocation: ReconstructionStageInvocationV1,
    manifest: ReconstructionProductManifestV2,
) -> ArtifactRef:
    """Persist byte-identical staged evidence outside the rename source."""
    encoded = manifest.to_json().encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    directory = _stage_directory(invocation, "validation")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"staged-product-manifest-{digest}.json"
    if target.exists():
        if target.read_bytes() != encoded:
            raise ValueError("staged product manifest mirror collision")
    else:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", dir=directory
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return ArtifactRef(
        kind=VALIDATION_STAGE_ARTIFACT_KIND,
        path=str(target.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "commit_phase": "staged",
            "publication_id": manifest.publication_id,
            "manifest_id": manifest.manifest_id,
            "logical_content_sha256": manifest.replay.logical_content_sha256,
        },
    )


def _prior_manifest(
    invocation: ReconstructionStageInvocationV1, kind: str
) -> Mapping[str, Any]:
    return _read_json_ref(_prior_ref(invocation, kind))


def _prior_ref(
    invocation: ReconstructionStageInvocationV1, kind: str
) -> ArtifactRef:
    matches = tuple(
        ref
        for outcome in invocation.prior_outcomes
        for ref in outcome.output_refs
        if ref.kind == kind
    )
    if len(matches) != 1:
        raise ValueError(f"expected exactly one prior {kind} artifact")
    return matches[0]


def _read_json_ref(ref: ArtifactRef) -> Mapping[str, Any]:
    verify_artifact_ref(ref)
    try:
        value = json.loads(Path(ref.path).read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as err:
        raise ValueError(f"invalid JSON stage artifact: {ref.path}") from err
    return _mapping(value)


def _completed(
    invocation: ReconstructionStageInvocationV1,
    ref: ArtifactRef,
    *,
    started: float,
    observed: int = 0,
    candidates: int = 0,
    accepted: int = 0,
    rejected: int = 0,
    message: str,
    output_bytes: int | None = None,
    additional_refs: Sequence[ArtifactRef] = (),
) -> ReconstructionStageOutcomeV1:
    runtime = max(0.0, time.perf_counter() - started)
    scratch = _tree_size(invocation.task.scratch_directory)
    actual_output = (
        int(output_bytes)
        if output_bytes is not None
        else _artifact_graph_size(ref)
    )
    amplification = candidates / observed if observed else 0.0
    telemetry: dict[str, JSONValue] = {
        "runtime_seconds": round(runtime, 6),
        "peak_rss_bytes": _peak_rss_bytes(),
        "scratch_bytes": scratch,
        "output_bytes": actual_output,
        "observed_event_count": observed,
        "candidate_event_count": candidates,
        "accepted_event_count": accepted,
        "rejected_event_count": rejected,
        "candidate_amplification": round(amplification, 9),
    }
    outputs = tuple(
        replace(item, metadata={**item.metadata, **telemetry})
        for item in (ref, *tuple(additional_refs))
    )
    return invocation.completed(
        output_refs=outputs,
        observed_event_count=observed,
        candidate_event_count=candidates,
        accepted_event_count=accepted,
        scratch_bytes=scratch,
        output_bytes=actual_output,
        message=message,
    )


def _cancel_if_requested(invocation: ReconstructionStageInvocationV1) -> None:
    if not invocation.cancellation_requested:
        return
    shutil.rmtree(invocation.task.scratch_directory, ignore_errors=True)
    raise asyncio.CancelledError


def _stage_directory(
    invocation: ReconstructionStageInvocationV1, name: str
) -> Path:
    root = Path(invocation.task.scratch_directory).expanduser().resolve()
    directory = (root / name).resolve()
    if not directory.is_relative_to(root):
        raise ValueError("stage directory escaped window scratch")
    return directory


def _stage_scope(
    invocation: ReconstructionStageInvocationV1,
) -> dict[str, JSONValue]:
    window = invocation.task.window
    return {
        "run_id": window.run_id,
        "window_id": window.window_id,
        "synchronization_unit_id": window.synchronization_unit_id,
        "ensemble_member_id": window.ensemble_member_id,
        "stage": invocation.command.stage.value,
        "large_event_rows_inline": False,
    }


def _refs_dict(values: Mapping[str, ArtifactRef]) -> dict[str, JSONValue]:
    return {
        symbol: cast(JSONValue, ref.to_dict())
        for symbol, ref in sorted(values.items())
    }


def _events_content_sha256(events: Iterable[SyntheticEventV1]) -> str:
    digest = hashlib.sha256(b"histdatacom-stage-events-v1\n")
    for event in sorted(
        events,
        key=lambda item: (
            item.symbol,
            item.event_time_ns,
            item.event_sequence,
            item.event_id,
        ),
    ):
        digest.update(event.to_json().encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _tree_size(path: str | Path) -> int:
    root = Path(path)
    if not root.exists():
        return 0
    return sum(
        item.stat().st_size
        for item in root.rglob("*")
        if item.is_file() and not item.is_symlink()
    )


def _peak_rss_bytes() -> int:
    return peak_rss_bytes()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_graph_size(ref: ArtifactRef) -> int:
    """Measure a stage manifest and every distinct strong file it names."""
    paths: dict[str, int] = {ref.path: ref.size_bytes or 0}
    if not ref.path.endswith(".json"):
        return sum(paths.values())
    try:
        payload = json.loads(Path(ref.path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return sum(paths.values())

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            path = value.get("path")
            size = value.get("size_bytes")
            digest = value.get("sha256")
            kind = value.get("kind")
            if (
                isinstance(path, str)
                and isinstance(size, int)
                and isinstance(digest, str)
                and isinstance(kind, str)
                and bool(kind.strip())
                and len(digest) == 64
            ):
                paths[path] = size
            for item in value.values():
                visit(item)
        elif isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            for item in value:
                visit(item)

    visit(payload)
    return sum(paths.values())


def _bounded_error(err: BaseException) -> str:
    text = f"{type(err).__name__}: {err}".strip()
    return text[:2_048]


def _bounded_reasons(
    values: Sequence[str], *, fallback: str
) -> tuple[str, ...]:
    selected = tuple(str(value)[:1_024] for value in values[:32] if str(value))
    return selected or (fallback,)


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a JSON object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("expected a JSON sequence")
    return value


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported staging descriptor schema")


__all__ = [
    "CARVING_STAGE_ARTIFACT_KIND",
    "CROSS_STAGE_ARTIFACT_KIND",
    "DELIVERY_STAGE_ARTIFACT_KIND",
    "PROPOSAL_STAGE_ARTIFACT_KIND",
    "RECONSTRUCTION_STAGE_ARTIFACT_SCHEMA_VERSION",
    "RECONSTRUCTION_STAGING_DESCRIPTOR_SCHEMA_VERSION",
    "SOURCE_STAGE_ARTIFACT_KIND",
    "STAGING_DESCRIPTOR_ARTIFACT_KIND",
    "VALIDATION_STAGE_ARTIFACT_KIND",
    "atomic_commit_handler",
    "carving_handler",
    "cross_series_reconciliation_handler",
    "delivery_projection_handler",
    "proposal_handler",
    "register_first_party_reconstruction_handlers",
    "source_enrichment_handler",
    "validation_handler",
]
