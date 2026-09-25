"""Exact grant intersection and fresh host-owned resource-use authority."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from typing import Protocol

from ._wire import identifier
from .contracts import (
    BrokerPermissionBindingV1,
    BrokerPermissionContextV1,
    BrokerPermissionDecisionV1,
    BrokerPermissionManifestV1,
    validate_permission_atom,
)
from .contracts import (
    BrokerPermissionReason as Reason,
)


class BrokerPermissionError(ValueError):
    """Bounded host refusal, without echoing configuration or resource data."""

    def __init__(
        self, reason: str, decision: BrokerPermissionDecisionV1 | None = None
    ) -> None:
        self.reason = reason
        self.decision = decision
        super().__init__("broker permission refused: " + reason)


class BrokerPermissionSource(Protocol):
    """Current operator-supplied ledger; not an OS or legal authority oracle."""

    def read_context(self) -> BrokerPermissionContextV1: ...


def decide_broker_permissions(
    manifest: BrokerPermissionManifestV1,
    binding: BrokerPermissionBindingV1,
    grant_id: str,
    context: BrokerPermissionContextV1,
    at_ns: int,
) -> BrokerPermissionDecisionV1:
    """Recompute a declaration decision; returned bytes are not a live permit.

    Extra operator-granted atoms cannot extend the plugin's declared surface.
    Optional denied atoms remain explicit; no missing fields are fabricated.
    """
    if (
        type(manifest) is not BrokerPermissionManifestV1
        or type(binding) is not BrokerPermissionBindingV1
        or type(context) is not BrokerPermissionContextV1
        or type(at_ns) is not int
        or not 0 <= at_ns < 2**63
    ):
        raise ValueError("invalid permission decision input")
    # Fresh restoration catches even object.__setattr__ mutation of a frozen
    # dataclass; it also charges complete nested trees before any decision.
    manifest = BrokerPermissionManifestV1.from_json(manifest.to_json())
    binding = BrokerPermissionBindingV1.from_json(binding.to_json())
    context = BrokerPermissionContextV1.from_json(context.to_json())
    identifier(grant_id, "grant")
    grants = {item.artifact_id: item for item in context.grants}
    grant = grants.get(grant_id)
    granted = set() if grant is None else set(grant.granted_atoms)
    missing = tuple(sorted(set(manifest.required_atoms) - granted))
    denied = tuple(sorted(set(manifest.optional_atoms) - granted))
    reason = Reason.ADMITTED
    if (
        binding.candidate_id != manifest.candidate_id
        or binding.manifest_id != manifest.artifact_id
        or binding.sdk_version != manifest.sdk_version
        or binding.provider_id not in manifest.provider_ids
        or (grant is not None and grant.binding != binding)
    ):
        reason = Reason.BINDING
    elif grant is None:
        reason = Reason.MISSING_GRANT
    elif at_ns < grant.issued_at_ns:
        reason = Reason.NOT_YET_VALID
    elif at_ns >= grant.expires_at_ns:
        reason = Reason.EXPIRED
    elif any(
        item.grant_id == grant_id and item.revoked_at_ns <= at_ns
        for item in context.revocations
    ):
        reason = Reason.REVOKED
    elif missing:
        reason = Reason.REQUIRED_DENIED
    admitted = reason is Reason.ADMITTED
    effective = (
        tuple(sorted(set(manifest.declared_atoms) & granted))
        if admitted
        else ()
    )
    return BrokerPermissionDecisionV1(
        binding,
        grant_id,
        context.artifact_id,
        at_ns,
        admitted,
        reason,
        effective,
        denied,
        missing,
    )


def _read(source: BrokerPermissionSource) -> BrokerPermissionContextV1:
    try:
        result = source.read_context()
        if type(result) is not BrokerPermissionContextV1:
            raise ValueError
        return BrokerPermissionContextV1.from_json(result.to_json())
    # The supplied callback may fail with private input in its exception.
    except (Exception, SystemExit):  # noqa: BLE001
        raise BrokerPermissionError(
            "invalid_current_permission_source"
        ) from None


def _inventory(context: BrokerPermissionContextV1) -> frozenset[str]:
    return frozenset(
        item.artifact_id
        for group in (context.grants, context.revocations)
        for item in group
    )


class BrokerPermissionAuthorityV1:
    """One process-local pinned selection, freshly checked before each effect.

    This cooperating host boundary cannot confine hostile trusted in-process
    Python. The isolated runtime must separately enforce its qualified kernel
    profile. A grant never implies that a requested backend is supported.
    """

    __slots__ = (
        "_binding",
        "_clock",
        "_context_id",
        "_grant_id",
        "_inventory",
        "_last_clock",
        "_lock",
        "_manifest",
        "_pid",
        "_revision",
        "_source",
    )

    def __init__(
        self,
        manifest: BrokerPermissionManifestV1,
        binding: BrokerPermissionBindingV1,
        grant_id: str,
        source: BrokerPermissionSource,
        clock: Callable[[], int] = time.time_ns,
    ) -> None:
        if (
            type(manifest) is not BrokerPermissionManifestV1
            or type(binding) is not BrokerPermissionBindingV1
        ):
            raise BrokerPermissionError("invalid_permission_authority_binding")
        identifier(grant_id, "grant")
        self._manifest = BrokerPermissionManifestV1.from_json(
            manifest.to_json()
        )
        self._binding = BrokerPermissionBindingV1.from_json(binding.to_json())
        self._grant_id = grant_id
        self._source = source
        self._clock = clock
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._inventory: frozenset[str] = frozenset()
        self._revision: int | None = None
        self._context_id: str | None = None
        self._last_clock: int | None = None

    @property
    def manifest(self) -> BrokerPermissionManifestV1:
        return BrokerPermissionManifestV1.from_json(self._manifest.to_json())

    @property
    def binding(self) -> BrokerPermissionBindingV1:
        return BrokerPermissionBindingV1.from_json(self._binding.to_json())

    @property
    def grant_id(self) -> str:
        return self._grant_id

    def _fresh(self) -> tuple[BrokerPermissionContextV1, int]:
        if self._pid != os.getpid():
            raise BrokerPermissionError(
                "current_process_permission_authority_required"
            )
        context = _read(self._source)
        inventory = _inventory(context)
        if not self._inventory <= inventory:
            raise BrokerPermissionError(
                "permission_ledger_removed_or_rewritten"
            )
        if self._revision is not None and (
            context.revision < self._revision
            or (
                context.revision == self._revision
                and context.artifact_id != self._context_id
            )
        ):
            raise BrokerPermissionError("permission_ledger_revision_regressed")
        # Record a revocation as soon as it is observed, even when a subsequent
        # clock check fails; a later source read may not roll it back.
        self._inventory = inventory
        self._revision = context.revision
        self._context_id = context.artifact_id
        try:
            now = self._clock()
        except (Exception, SystemExit):  # noqa: BLE001
            # Private host callback errors are never public evidence.
            raise BrokerPermissionError(
                "invalid_current_permission_clock"
            ) from None
        if type(now) is not int or not 0 <= now < 2**63:
            raise BrokerPermissionError("invalid_current_permission_clock")
        if self._last_clock is not None and now < self._last_clock:
            raise BrokerPermissionError("current_permission_clock_regressed")
        self._last_clock = now
        return context, now

    def read_context(self) -> BrokerPermissionContextV1:
        """Fresh detached ledger for worker IPC, never a reusable grant token."""
        if not self._lock.acquire(blocking=False):
            raise BrokerPermissionError(
                "concurrent_or_reentrant_permission_check"
            )
        try:
            context, _ = self._fresh()
            return context
        finally:
            self._lock.release()

    def _require(self, atom: str | None) -> BrokerPermissionDecisionV1:
        if atom is not None:
            try:
                validate_permission_atom(atom)
            except ValueError:
                raise BrokerPermissionError(
                    "unknown_resource_permission"
                ) from None
        if not self._lock.acquire(blocking=False):
            raise BrokerPermissionError(
                "concurrent_or_reentrant_permission_check"
            )
        try:
            context, now = self._fresh()
            result = decide_broker_permissions(
                self._manifest, self._binding, self._grant_id, context, now
            )
            if not result.admitted:
                raise BrokerPermissionError(result.reason.value, result)
            if atom is not None and atom not in result.effective_atoms:
                raise BrokerPermissionError(
                    Reason.RESOURCE_DENIED.value, result
                )
            return result
        finally:
            self._lock.release()

    def require_admission(self) -> BrokerPermissionDecisionV1:
        """Fresh required-subset admission before import/factory/session effects."""
        return self._require(None)

    def snapshot_admission(
        self,
    ) -> tuple[BrokerPermissionContextV1, BrokerPermissionDecisionV1]:
        """One fresh read and its exact historical decision under one lock.

        Useful for durable native provenance: a second source read could observe
        a different revision and must not be mislabeled as the decision's input.
        Neither returned value is a reusable resource-use permit.
        """
        if not self._lock.acquire(blocking=False):
            raise BrokerPermissionError(
                "concurrent_or_reentrant_permission_check"
            )
        try:
            context, now = self._fresh()
            result = decide_broker_permissions(
                self._manifest, self._binding, self._grant_id, context, now
            )
            if not result.admitted:
                raise BrokerPermissionError(result.reason.value, result)
            return context, result
        finally:
            self._lock.release()

    def require(self, atom: str) -> BrokerPermissionDecisionV1:
        """Fresh admission plus exact declared/granted atom before a resource use."""
        return self._require(atom)
