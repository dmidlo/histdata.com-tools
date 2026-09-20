"""Closed source dispatch and selection; no public 'verified' receipt escape."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .training_contracts import TrainingRowV1, training_json, training_load
from .training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    JoinState,
    TrainingJoinColumnV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
    TrainingJoinValueV1,
    TrainingJoinRefusalEvidenceV1,
)
from .training_lineage import (
    _VerifiedSource,
    read_training_regular,
    verify_training_ownership,
)
from .training_views import materialize_training_rows

Scalar = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class _JoinRecord:
    """Private normalized output of an executed native adapter."""

    key_time_ns: int
    source_time_ns: int
    end_ns: int
    available_at_ns: int | None
    dependency_start_ns: int
    dependency_end_ns: int
    source_ids: tuple[str, ...]
    parent_ids: tuple[str, ...]
    source_schema: str
    origin: str
    value: Scalar
    state: JoinState = JoinState.AVAILABLE
    reason: str = "native_source_value_replayed"
    valid_until_ns: int | None = None


@dataclass(frozen=True, slots=True)
class _JoinAdapter:
    """Process-local executed adapter; never serializable or caller accepted."""

    binding: TrainingJoinSourceV1
    root_ids: tuple[str, ...]
    select: Callable[[TrainingJoinColumnV1, int], tuple[_JoinRecord, ...]]


def _configuration(
    source: TrainingJoinSourceV1, *, keys: set[str]
) -> dict[str, object]:
    result = training_load(source.configuration_json)
    if set(result) != keys:
        raise ValueError(
            "join adapter configuration keys differ from closed schema"
        )
    return result


def _canonical_restore(
    text: str,
    restore: Callable[[str], object],
    encode: Callable[[object], object],
) -> object:
    result = restore(text)
    if training_json(encode(result)) != text:
        raise ValueError(
            "native join evidence has unknown/coercive wire fields"
        )
    return result


def _bound_file(path: str, *, limit: int = 8 * 1024 * 1024) -> bytes:
    return read_training_regular(Path(path), limit)


def _hash(value: object) -> str:
    return hashlib.sha256(training_json(value).encode()).hexdigest()


def _prefix(column: TrainingJoinColumnV1, prefix: str) -> None:
    if not column.name.startswith(prefix):
        raise ValueError("column namespace differs from native source family")


def _semantics(column: TrainingJoinColumnV1, meaning: JoinMeaning) -> None:
    if column.meaning is not meaning:
        raise ValueError(
            "declared persistence differs from native feature semantics"
        )


def _no_source(
    source: TrainingJoinSourceV1,
) -> _JoinAdapter:
    _configuration(source, keys=set())
    if source.paths or source.evidence_json:
        raise ValueError("unimplemented source cannot smuggle value evidence")

    def select(
        column: TrainingJoinColumnV1, cutoff: int
    ) -> tuple[_JoinRecord, ...]:
        if not column.name.startswith(
            ("synthetic_flow.", "uncertainty.", "lineage.")
        ):
            raise ValueError("implemented namespace requires its real adapter")
        return ()

    return _JoinAdapter(source, (source.artifact_id,), select)


def prepare_training_join_sources(
    plan: TrainingJoinPlanV1,
) -> dict[str, _JoinAdapter]:
    """Replay all sources even if no rows/columns select them.

    The frozen606 API returns a batch, not its private decoded source. A second
    bounded full-source pass supplies quote/product adapters. This fixed cost is
    independent of column/cell count; no repeated source decode occurs per tick.
    """
    from .training_join_context import prepare_context_join
    from .training_join_market import prepare_market_join

    expected = materialize_training_rows(
        plan.spine.source, plan.spine.ownership, plan.spine.request
    )
    if expected != plan.spine:
        raise ValueError(
            "join spine differs from complete canonical source replay"
        )
    verified: _VerifiedSource = verify_training_ownership(
        plan.spine.source, plan.spine.ownership
    )
    result = {}
    market = {
        JoinFamily.TICK,
        JoinFamily.BAR,
        JoinFamily.INDICATOR,
        JoinFamily.TRIANGLE,
        JoinFamily.ACTIVITY,
    }
    for source in plan.sources:
        if source.family is JoinFamily.UNSUPPORTED:
            adapter = _no_source(source)
        elif source.family in market:
            adapter = prepare_market_join(source, verified)
        else:
            adapter = prepare_context_join(source, verified)
        result[source.key] = adapter
    # Validate declarations even for an empty spine/projection. A clock inside
    # the verified domain is used only to check adapter schema, never emitted.
    for column in plan.columns:
        validation_cutoff = plan.spine.request.decision_time_ns
        result[column.source_key].select(
            column,
            (
                plan.spine.request.start_ns
                if validation_cutoff is None
                else validation_cutoff
            ),
        )
    return result


def join_value(
    adapter: _JoinAdapter,
    column: TrainingJoinColumnV1,
    row: TrainingRowV1,
    mode: JoinInformationMode,
) -> TrainingJoinValueV1:
    cutoff = row.decision_time_ns

    def missing(
        state: JoinState, reason: str, records: tuple[_JoinRecord, ...] = ()
    ) -> TrainingJoinValueV1:
        # Refusals retain latest considered coordinates, not future values.
        # Complete inventory is still bound and independently replayed.
        if records:
            latest = max(record.key_time_ns for record in records)
            records = tuple(
                record for record in records if record.key_time_ns == latest
            )
        if len(records) > 32:
            raise ValueError(
                "ambiguous refusal candidate evidence exceeds bound"
            )
        evidence = (
            tuple(
                dict.fromkeys(
                    identity
                    for record in records
                    for identity in record.source_ids
                )
            )
            or adapter.root_ids
        )
        return TrainingJoinValueV1(
            column.name,
            state,
            '{"value":null}',
            reason,
            adapter.binding.artifact_id,
            evidence,
            "histdatacom.training-join-null-state.v1",
            "explicit_null_not_an_observation",
            None,
            None,
            None,
            None,
            (),
            tuple(
                TrainingJoinRefusalEvidenceV1(
                    record.source_ids,
                    record.source_schema,
                    record.origin,
                    record.key_time_ns,
                    record.source_time_ns,
                    record.available_at_ns,
                    record.dependency_start_ns,
                    record.dependency_end_ns,
                    record.parent_ids,
                )
                for record in records
            ),
        )

    if row.symbol != column.entity.symbol:
        return missing(
            JoinState.NOT_APPLICABLE, "row_entity_not_in_column_mapping"
        )
    records = adapter.select(column, cutoff)
    if len(records) > 4096:
        raise ValueError("join selected native record budget exceeded")
    if adapter.binding.family is JoinFamily.UNSUPPORTED:
        return missing(
            JoinState.UNSUPPORTED, "reserved_family_has_no_verified_emitter"
        )
    candidates = []
    expired = []
    for record in records:
        if (
            record.valid_until_ns is not None
            and cutoff >= record.valid_until_ns
        ):
            expired.append(record)
            continue
        if column.direction is JoinDirection.EXACT:
            include = record.key_time_ns == cutoff
        elif column.direction is JoinDirection.INTERVAL:
            include = record.key_time_ns <= cutoff < record.end_ns
        else:
            include = record.key_time_ns <= cutoff
        if include:
            candidates.append(record)
    if not candidates:
        if expired:
            return missing(
                JoinState.STALE,
                "native_state_validity_interval_ended",
                tuple(expired),
            )
        return missing(
            JoinState.UNAVAILABLE,
            "no_source_coordinate_at_declared_join",
            records,
        )
    # Delayed publications are not eligible merely because their event clock is
    # old. Filter first, then choose latest actually available source coordinate.
    if mode is JoinInformationMode.NORMALIZED_AS_OF:
        admitted = [
            r
            for r in candidates
            if (
                r.state is not JoinState.AVAILABLE
                and (r.available_at_ns is None or r.available_at_ns <= cutoff)
                and r.source_time_ns <= cutoff
            )
            or (
                r.available_at_ns is not None
                and r.available_at_ns <= cutoff
                and r.source_time_ns <= cutoff
                and r.dependency_end_ns <= cutoff + 1
            )
        ]
        if not admitted:
            state = (
                JoinState.UNKNOWN_AVAILABILITY
                if any(r.available_at_ns is None for r in candidates)
                else JoinState.UNAVAILABLE
            )
            return missing(
                state, "source_support_not_known_by_decision", tuple(candidates)
            )
        candidates = admitted
    latest = max(r.key_time_ns for r in candidates)
    chosen = [r for r in candidates if r.key_time_ns == latest]
    if len(chosen) != 1:
        raise ValueError("ambiguous duplicate native source coordinate")
    record = chosen[0]
    if cutoff - record.key_time_ns > column.max_age_ns:
        return missing(
            JoinState.STALE, "source_age_exceeds_frozen_bound", (record,)
        )
    return TrainingJoinValueV1(
        column.name,
        record.state,
        training_json({"value": record.value}),
        record.reason,
        adapter.binding.artifact_id,
        record.source_ids,
        record.source_schema,
        record.origin,
        record.source_time_ns,
        record.available_at_ns,
        record.dependency_start_ns,
        record.dependency_end_ns,
        record.parent_ids,
    )
