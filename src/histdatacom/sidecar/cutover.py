"""Temporal sidecar runtime cutover policy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

SIDECAR_RUNTIME = "sidecar"
FOREGROUND_RUNTIME_REMOVED_MESSAGE = (
    "The foreground compatibility runtime has been removed. Use the default "
    "Temporal sidecar runtime instead."
)


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
        "removed after the documented release window; --foreground is no "
        "longer accepted and Options.use_sidecar = False is rejected"
    ),
    config_globals_lifecycle=(
        "CLI/API runtime selection uses resolved runtime context and "
        "RunRequest payloads; legacy helper surfaces accept explicit "
        "argument mappings instead of ambient parser state"
    ),
    unavailable_sidecar_behavior=(
        "default sidecar requests start the bundled local sidecar when "
        "needed; startup or connection failures raise SidecarUnavailableError "
        "or exit nonzero with a clear message"
    ),
    unsupported_artifact_behavior=(
        "metadata-only wheels and unsupported platforms require an operator "
        "provided Temporal executable for sidecar startup"
    ),
)


def should_submit_to_sidecar(args: Mapping[str, object]) -> bool:
    """Return whether parsed CLI/API args request the sidecar runtime."""
    if args.get("use_sidecar") is False:
        raise ValueError(FOREGROUND_RUNTIME_REMOVED_MESSAGE)
    return True


def selected_runtime(args: Mapping[str, object]) -> str:
    """Return the selected runtime for parsed CLI/API args."""
    should_submit_to_sidecar(args)
    return SIDECAR_RUNTIME


def cutover_policy_payload() -> dict[str, str]:
    """Return the current runtime cutover policy as plain data."""
    return DEFAULT_CUTOVER_POLICY.to_dict()
