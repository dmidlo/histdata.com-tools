"""Fresh policy-source checks at closed host-native operation boundaries."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from _thread import LockType
from dataclasses import dataclass
import os
import threading
import time
from typing import Protocol

from ._wire import canonical_json
from .contracts import (
    BrokerPolicyContextV1,
    BrokerPolicyDecisionV1,
    BrokerPolicyOperation,
    BrokerPolicyRequestV1,
    BrokerPolicySubjectV1,
)
from .decisions import decide_provider_operation


class BrokerPolicySource(Protocol):
    """Current supplied review inventory, not an external legal-status oracle."""

    def read_policy_context(self) -> BrokerPolicyContextV1: ...


class BrokerPolicyError(ValueError):
    def __init__(
        self, reason: str, decision: BrokerPolicyDecisionV1 | None = None
    ) -> None:
        self.reason = reason
        self.decision = decision
        super().__init__("provider policy refused: " + reason)


def _now_ns() -> int:
    return time.time_ns()


def _read(source: BrokerPolicySource) -> BrokerPolicyContextV1:
    try:
        value = source.read_policy_context()
        if type(value) is not BrokerPolicyContextV1:
            raise ValueError
        return BrokerPolicyContextV1.from_json(value.to_json())
    except (Exception, SystemExit):
        raise BrokerPolicyError("invalid_current_policy_source") from None


def _selection(context: BrokerPolicyContextV1) -> str:
    selected = {p.artifact_id: p for p in context.policies}
    return canonical_json(
        {
            "policy_ids": context.selected_policy_ids,
            "bindings": tuple(
                selected[i].binding for i in context.selected_policy_ids
            ),
            "execution": context.execution,
        }
    )


def _inventory(context: BrokerPolicyContextV1) -> frozenset[str]:
    return frozenset(
        item.artifact_id
        for family in (
            context.policies,
            context.evidence,
            context.acknowledgements,
            context.revocations,
        )
        for item in family
    )


@dataclass(slots=True)
class _Scope:
    source: BrokerPolicySource
    selection: str
    process_id: int
    inventory: frozenset[str]
    lock: LockType
    last_clock: int | None = None


_CURRENT: ContextVar[_Scope | None] = ContextVar(
    "broker_provider_policy_scope", default=None
)


@contextmanager
def provider_policy_scope(source: BrokerPolicySource) -> Iterator[None]:
    """Pin a policy selection, rereading the current source at every effect.

    This is cooperative host enforcement, not hostile same-user Python security.
    Child processes must reconstruct the source/scope rather than inherit it.
    Nested scopes are refused so a callback cannot silently replace its host's
    selection while an admitted operation is running.
    """
    if _CURRENT.get() is not None:
        raise BrokerPolicyError("nested_policy_scope")
    context = _read(source)
    token = _CURRENT.set(
        _Scope(
            source,
            _selection(context),
            os.getpid(),
            _inventory(context),
            threading.Lock(),
        )
    )
    try:
        yield
    finally:
        _CURRENT.reset(token)


@contextmanager
def _locked_current_scope() -> Iterator[_Scope]:
    state = _CURRENT.get()
    if state is None or state.process_id != os.getpid():
        raise BrokerPolicyError("current_process_policy_scope_required")
    if not state.lock.acquire(blocking=False):
        raise BrokerPolicyError("concurrent_or_reentrant_policy_check")
    try:
        yield state
    finally:
        state.lock.release()


def _current_clock(state: _Scope) -> int:
    now = _now_ns()
    if type(now) is not int or not 0 <= now < 2**63:
        raise BrokerPolicyError("invalid_current_clock")
    if state.last_clock is not None and now < state.last_clock:
        raise BrokerPolicyError("current_clock_regressed")
    state.last_clock = now
    return now


def _fresh_context(state: _Scope) -> BrokerPolicyContextV1:
    context = _read(state.source)
    if _selection(context) != state.selection:
        raise BrokerPolicyError("selected_policy_or_execution_context_changed")
    inventory = _inventory(context)
    if not state.inventory <= inventory:
        raise BrokerPolicyError("review_inventory_removed_or_rewritten")
    state.inventory = inventory
    _current_clock(state)
    return context


def read_current_provider_policy_context() -> BrokerPolicyContextV1:
    """Fresh supplied ledger for a bounded worker IPC exchange, not a permit.

    Enforce current scope/PID, pinned selection, append-only review inventory
    and clock before returning a detached canonical context. The receiving
    process must independently run its ordinary native operation guard.
    """
    with _locked_current_scope() as state:
        return _fresh_context(state)


def require_provider_operation(
    native_subject: object,
    operation: BrokerPolicyOperation,
    *,
    intended_retention_deadline_ns: int | None = None,
    recipient_scope: str | None = None,
) -> BrokerPolicyDecisionV1:
    """Resolve actual native classes and enforce fresh declared rights.

    The native resolver is a closed root-owned module, never a caller callback
    or a supplied data-class list. Metadata descriptors/pure decisions are not
    native operation subjects.
    """
    if type(operation) is not BrokerPolicyOperation:
        raise BrokerPolicyError("invalid_operation")
    # Never wait on a reentrant resolver/source callback. Concurrent use of a
    # copied context must establish separate scopes rather than race its ledger.
    with _locked_current_scope() as state:
        return _require_current(
            state,
            native_subject,
            operation,
            intended_retention_deadline_ns,
            recipient_scope,
        )


def _require_current(
    state: _Scope,
    native_subject: object,
    operation: BrokerPolicyOperation,
    intended_retention_deadline_ns: int | None,
    recipient_scope: str | None,
) -> BrokerPolicyDecisionV1:
    context = _fresh_context(state)
    if isinstance(
        native_subject,
        (BrokerPolicySubjectV1, BrokerPolicyDecisionV1, BrokerPolicyRequestV1),
    ):
        raise BrokerPolicyError("native_subject_required")
    # Root owns this built-in resolver and all supported native wrappers.
    from .bindings import resolve_provider_subject

    subject = resolve_provider_subject(native_subject)
    if type(subject) is not BrokerPolicySubjectV1:
        raise BrokerPolicyError("invalid_native_subject_resolution")
    now = _current_clock(state)
    request = BrokerPolicyRequestV1(
        subject, operation, now, intended_retention_deadline_ns, recipient_scope
    )
    decision = decide_provider_operation(context, request)
    if not decision.allowed:
        raise BrokerPolicyError("operation_not_allowed", decision)
    return decision
