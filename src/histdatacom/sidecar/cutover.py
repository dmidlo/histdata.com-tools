"""Temporal sidecar runtime cutover policy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

FOREGROUND_RUNTIME = "foreground"
SIDECAR_RUNTIME = "sidecar"


@dataclass(frozen=True, slots=True)
class RuntimeCutoverPolicy:
    """Document the current production runtime selection contract."""

    default_runtime: str
    sidecar_activation: str
    foreground_lifecycle: str
    config_globals_lifecycle: str
    unavailable_sidecar_behavior: str
    unsupported_artifact_behavior: str

    def to_dict(self) -> dict[str, str]:
        """Return a JSON/documentation friendly representation."""
        return asdict(self)


DEFAULT_CUTOVER_POLICY = RuntimeCutoverPolicy(
    default_runtime=SIDECAR_RUNTIME,
    sidecar_activation=(
        "default CLI/API runtime; --sidecar remains accepted as a "
        "compatibility alias"
    ),
    foreground_lifecycle=(
        "supported compatibility runtime through --foreground or "
        "Options.use_sidecar = False while downstream callers migrate"
    ),
    config_globals_lifecycle=(
        "config.ARGS is limited to explicit foreground and legacy "
        "compatibility paths; default sidecar requests use resolved runtime "
        "context and RunRequest"
    ),
    unavailable_sidecar_behavior=(
        "default sidecar requests start the bundled local sidecar when "
        "needed; startup or connection failures raise SidecarUnavailableError "
        "or exit nonzero with a clear message"
    ),
    unsupported_artifact_behavior=(
        "metadata-only wheels and unsupported platforms require an operator "
        "provided Temporal executable for sidecar startup, or explicit "
        "foreground opt-out for compatibility execution"
    ),
)


def should_submit_to_sidecar(args: Mapping[str, object]) -> bool:
    """Return whether parsed CLI/API args request the sidecar runtime."""
    return bool(
        args.get(
            "use_sidecar",
            DEFAULT_CUTOVER_POLICY.default_runtime == SIDECAR_RUNTIME,
        )
    )


def selected_runtime(args: Mapping[str, object]) -> str:
    """Return the selected runtime for parsed CLI/API args."""
    if should_submit_to_sidecar(args):
        return SIDECAR_RUNTIME
    return FOREGROUND_RUNTIME


def cutover_policy_payload() -> dict[str, str]:
    """Return the current runtime cutover policy as plain data."""
    return DEFAULT_CUTOVER_POLICY.to_dict()
