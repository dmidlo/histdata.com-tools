"""Runtime-selection policy for the orchestration-only V1 surface."""

from __future__ import annotations

from histdatacom.sidecar.cutover import (
    DEFAULT_CUTOVER_POLICY,
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
    ORCHESTRATION_RUNTIME,
    RuntimeCutoverPolicy,
    cutover_policy_payload,
    selected_runtime,
    should_submit_to_orchestration,
)

__all__ = [
    "DEFAULT_CUTOVER_POLICY",
    "FOREGROUND_RUNTIME_REMOVED_MESSAGE",
    "ORCHESTRATION_RUNTIME",
    "RuntimeCutoverPolicy",
    "cutover_policy_payload",
    "selected_runtime",
    "should_submit_to_orchestration",
]
