"""Compatibility warnings for legacy helper side-effect entrypoints."""

from __future__ import annotations

import warnings


class LegacyHelperSideEffectWarning(RuntimeWarning):
    """Warn when direct helper use bypasses sidecar orchestration."""


LEGACY_HELPER_WARNING = (
    "Direct use of {surface} bypasses the Temporal sidecar runtime. "
    "GUI and automation callers should submit histdatacom.Options or "
    "RunRequest payloads through histdatacom.main(...), histdatacom(...), or "
    "histdatacom.sidecar client/job-control APIs so work has durable status, "
    "cancellation, retry/resume, and worker-lane routing. Activity-stage "
    "helpers remain supported for Temporal activities and tests."
)


def warn_legacy_side_effect(surface: str) -> None:
    """Emit a visible compatibility warning for direct side-effect helpers."""
    warnings.warn(
        LEGACY_HELPER_WARNING.format(surface=surface),
        LegacyHelperSideEffectWarning,
        stacklevel=3,
    )
