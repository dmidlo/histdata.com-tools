"""Runtime-selection policy for the orchestration-only V1 surface."""

from __future__ import annotations

from histdatacom.sidecar.cutover import (
    DEFAULT_CUTOVER_POLICY,
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
    RuntimeCutoverPolicy,
    cutover_policy_payload,
    selected_runtime,
    should_submit_to_sidecar,
)

should_submit_to_orchestration = should_submit_to_sidecar

__all__ = [
    "DEFAULT_CUTOVER_POLICY",
    "FOREGROUND_RUNTIME_REMOVED_MESSAGE",
    "RuntimeCutoverPolicy",
    "cutover_policy_payload",
    "selected_runtime",
    "should_submit_to_orchestration",
]
