"""Locked, journal-first collection with honest partial/indeterminate outcomes."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path
import time
from typing import Any, ClassVar, Literal

from .contracts import (
    CollectionOutcomeV1,
    CollectionPlanV1,
    CollectionReceiptV1,
    CollectionTombstoneV1,
)
from .lifecycle_contracts import (
    ApplyStartedV1,
    CollectionInterruptedEvidenceV1,
    PayloadObservationV1,
    UnlinkIntentV1,
    UnlinkObservedV1,
)
from .planning import _derive_plan
from .secure_fs import StoreSession
from .storage import (
    RetentionStoreError,
    _CONTROL_ALLOWANCE,
    _NativeBudget,
    _State,
    _append,
    _control_path,
    _inspection_cost,
    _lifecycle_blockers,
    _load_state,
    _now,
    _require_clean,
    _snapshot,
)


class CollectionInterruptedError(RuntimeError):
    """Some intent/effect may exist; never retry automatically or infer absence."""

    # Keep the wrapper discoverable by the static compatibility inventory;
    # the interruption regression binds this label to the delegated wire.
    schema_version: ClassVar[str] = (
        "histdatacom.retention.collection-interrupted.v1"
    )

    def __init__(
        self,
        *,
        store_id: str,
        plan_id: str,
        outcomes: tuple[CollectionOutcomeV1, ...],
        observed_time_ns: int | None,
        receipt: CollectionReceiptV1 | None = None,
        receipt_persisted: bool = False,
        unaccepted_receipt_id: str | None = None,
        detail: str = "collection interrupted; retained store requires investigation",
    ) -> None:
        super().__init__(detail)
        self.store_id = store_id
        self.plan_id = plan_id
        self.outcomes = outcomes
        self.observed_time_ns = observed_time_ns
        self.receipt = receipt
        self.receipt_persisted = receipt_persisted
        self.unaccepted_receipt_id = unaccepted_receipt_id
        self.detail = detail

    def to_dict(self) -> dict[str, Any]:
        return CollectionInterruptedEvidenceV1(
            self.store_id,
            self.plan_id,
            self.outcomes,
            self.observed_time_ns,
            self.receipt,
            self.receipt_persisted,
            self.detail,
            unaccepted_receipt_id=self.unaccepted_receipt_id,
        ).to_dict()


def _unchanged_prefix(state: _State) -> None:
    """Validate this exact attempt's authorized prefix, not a stale snapshot."""
    actual = _load_state(state.session)
    if (
        actual.observations != state.observations
        or actual.journal != state.journal
        or actual.records != state.records
    ):
        raise RetentionStoreError(
            "managed collection prefix changed outside this attempt"
        )


def apply_artifact_collection(
    root: str | Path, plan: CollectionPlanV1
) -> CollectionReceiptV1:
    """Re-derive a retained plan under lock, then unlink only exact payloads.

    There is no recursive deletion and no automatic resume. A failure after a
    durable intent is indeterminate unless the actual unlink observation and
    tombstone both persist. Filesystem or clock failure can prevent any normal
    receipt; the raised error preserves actual available evidence instead.
    """
    if type(plan) is not CollectionPlanV1:
        raise TypeError("collection requires the exact typed retained plan")
    plan = CollectionPlanV1.from_json(plan.to_json())
    with StoreSession(root) as session:
        state = _load_state(session)
        path, raw = _control_path("plans", plan)
        if (
            plan.store_id != session.marker.store_id
            or path not in state.observations
            or session.read_bytes(path) != raw
        ):
            raise RetentionStoreError(
                "plan was not persisted by this managed store"
            )
        if any(
            item.plan_id == plan.artifact_id
            for item in state.of_type(ApplyStartedV1)
        ):
            raise RetentionStoreError(
                "collection attempt already exists; no implicit resume"
            )
        candidates = tuple(
            item.object_id for item in plan.decisions if item.action == "delete"
        )
        # Include the complete protected/historical-proof replay after effects.
        # Reserving the initial full cost twice is conservative when orphan
        # cache payloads disappear, and never skips their permanent proofs.
        control_files = 4 + 6 * len(candidates)
        budget = _NativeBudget(
            state,
            2 * _inspection_cost(state),
            control_bytes=control_files * _CONTROL_ALLOWANCE,
            control_files=control_files,
        )
        snapshot = _snapshot(state, budget)
        _require_clean(snapshot)
        expected = _derive_plan(
            snapshot, cutoff_ns=plan.cutoff_ns, observed_now_ns=_now(state)
        )
        if expected.to_json() != plan.to_json() or plan.blockers:
            raise RetentionStoreError(
                "retained collection plan is stale or blocked"
            )
        start = ApplyStartedV1(
            session.marker.store_id,
            plan.artifact_id,
            snapshot.artifact_id,
            candidates,
            _now(state),
        )
        start_entry = _append(state, "apply_started", start)
        objects = {item.artifact_id: item for item in state.descriptors}
        outcomes: dict[str, CollectionOutcomeV1] = {}
        raw_clock: int | None = None
        replay: Literal["not_attempted", "verified", "failed"] = "not_attempted"
        replay_started = False
        clock_invalid = False
        interrupted: Exception | None = None
        active_identity: str | None = None
        active_intent_entry = start_entry
        try:
            for identity in candidates:
                _unchanged_prefix(state)
                item = objects[identity]
                observation = state.observations[item.payload_ref.relative_path]
                intent = UnlinkIntentV1(
                    session.marker.store_id,
                    plan.artifact_id,
                    identity,
                    PayloadObservationV1(
                        **{
                            field.name: getattr(observation, field.name)
                            for field in fields(observation)
                        }
                    ),
                    _now(state),
                )
                active_identity = identity
                active_intent_entry = _append(state, "unlink_intent", intent)
                unlink_error: Exception | None = None
                try:
                    session._unlink_payload(observation)
                except Exception as error:
                    unlink_error = error
                raw_clock = time.time_ns()
                if (
                    type(raw_clock) is not int
                    or not state.journal[-1].recorded_ns
                    <= raw_clock
                    <= 2**63 - 1
                ):
                    clock_invalid = True
                    raise RetentionStoreError(
                        "current clock regressed after unlink attempt"
                    )
                observed = UnlinkObservedV1(
                    session.marker.store_id,
                    plan.artifact_id,
                    identity,
                    intent.artifact_id,
                    "indeterminate" if unlink_error else "unlinked",
                    0 if unlink_error else observation.size_bytes,
                    raw_clock,
                    (
                        "filesystem operation may have affected payload"
                        if unlink_error
                        else "exact payload unlinked and directory synchronized"
                    ),
                )
                if unlink_error is None:
                    del state.observations[item.payload_ref.relative_path]
                else:
                    # Record the physical inventory actually left by the failed
                    # operation, without treating absence as proof of success.
                    state.observations = {
                        entry.relative_path: entry
                        for entry in session.inventory()
                    }
                observed_entry = _append(state, "unlink_observed", observed)
                outcome = CollectionOutcomeV1(
                    identity,
                    observed.outcome,
                    observed.size_bytes_unlinked,
                    observed_entry.artifact_id,
                    observed.detail,
                )
                outcomes[identity] = outcome
                if unlink_error is not None:
                    raise unlink_error
                tombstone = CollectionTombstoneV1(
                    identity,
                    plan.artifact_id,
                    observed_entry.artifact_id,
                    observation.sha256,
                    observation.size_bytes,
                    raw_clock,
                )
                _append(state, "tombstone", tombstone)
                active_identity = None
            _unchanged_prefix(state)
            replay_started = True
            after = _snapshot(state, budget)
            if after.blockers != (
                f"incomplete_collection_attempt:{plan.artifact_id}",
            ):
                raise RetentionStoreError(
                    "post-collection current protected replay failed"
                )
            # Pure graph checks include every permanent source/recipe/catalog
            # and historical regeneration; only this exact pending receipt is
            # removed from the locally verified projection.
            from dataclasses import replace

            _require_clean(replace(after, blockers=()))
            replay = "verified"
        except Exception as error:
            interrupted = error
            if active_identity is not None and active_identity not in outcomes:
                outcomes[active_identity] = CollectionOutcomeV1(
                    active_identity,
                    "indeterminate",
                    0,
                    active_intent_entry.artifact_id,
                    "unlink or durable observation could not be established",
                )
            if replay_started:
                replay = "failed"
        for identity in candidates:
            if identity not in outcomes:
                outcomes[identity] = CollectionOutcomeV1(
                    identity,
                    "not_attempted",
                    0,
                    start_entry.artifact_id,
                    "stopped before this candidate",
                )
        ordered = tuple(outcomes[identity] for identity in candidates)
        receipt: CollectionReceiptV1 | None = None
        persisted = False
        unaccepted_receipt_id: str | None = None
        try:
            # Do not normalize/clamp a clock backstep to manufacture a receipt.
            if clock_invalid:
                raise RetentionStoreError(
                    "raw post-unlink clock regression prevents a normal receipt"
                )
            if raw_clock is None:
                raw_clock = time.time_ns()
            finished = _now(state)
            status: Literal["complete", "partial", "indeterminate"] = (
                "complete"
                if interrupted is None
                else (
                    "indeterminate"
                    if any(item.outcome == "indeterminate" for item in ordered)
                    else "partial"
                )
            )
            receipt = CollectionReceiptV1(
                session.marker.store_id,
                plan.artifact_id,
                start.started_ns,
                finished,
                status,
                ordered,
                replay,
            )
            # An outcome with no durable observation cannot become a persisted
            # normal receipt. The exception still returns that raw evidence.
            if any(
                item.outcome == "indeterminate"
                and item.journal_entry_id == active_intent_entry.artifact_id
                for item in ordered
            ):
                raise RetentionStoreError(
                    "normal receipt lacks durable unlink observation"
                )
            tombstoned = {
                item.object_id for item in state.of_type(CollectionTombstoneV1)
            }
            if any(
                item.outcome == "unlinked" and item.object_id not in tombstoned
                for item in ordered
            ):
                raise RetentionStoreError(
                    "normal receipt lacks durable unlink tombstone"
                )
            _append(state, "collection_receipt", receipt)
            persisted = True
            _unchanged_prefix(state)
            if interrupted is None:
                if (
                    _load_state(session)
                    .of_type(CollectionReceiptV1)
                    .count(receipt)
                    != 1
                ):
                    raise RetentionStoreError(
                        "completed collection receipt was not retained"
                    )
                if _lifecycle_blockers(_load_state(session)):
                    raise RetentionStoreError(
                        "completed collection lacks exact terminal linkage"
                    )
                return receipt
        except Exception as error:
            interrupted = error
            if receipt is not None and receipt.status == "complete":
                # A proposed complete object was not successfully sealed and
                # checked. If publication returned successfully, retain that
                # exact observed fact separately from acceptance. Never infer
                # that a later check failure erased the immutable bytes.
                if persisted:
                    unaccepted_receipt_id = receipt.artifact_id
                receipt = None
        raise CollectionInterruptedError(
            store_id=session.marker.store_id,
            plan_id=plan.artifact_id,
            outcomes=ordered,
            observed_time_ns=raw_clock,
            receipt=receipt,
            receipt_persisted=persisted,
            unaccepted_receipt_id=unaccepted_receipt_id,
        ) from interrupted
