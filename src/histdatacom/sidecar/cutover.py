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
    default_runtime=FOREGROUND_RUNTIME,
    sidecar_activation="explicit --sidecar or Options.use_sidecar opt-in",
    foreground_lifecycle=(
        "supported compatibility runtime until a later issue flips the "
        "production default"
    ),
    config_globals_lifecycle=(
        "config.ARGS remains a compatibility adapter for foreground and "
        "legacy API behavior while sidecar requests use RunRequest"
    ),
    unavailable_sidecar_behavior=(
        "explicit sidecar requests fail clearly; non-sidecar requests keep "
        "using foreground"
    ),
    unsupported_artifact_behavior=(
        "metadata-only wheels and unsupported platforms do not auto-select "
        "sidecar; explicit start requests require a bundled or operator "
        "provided Temporal executable"
    ),
)


def should_submit_to_sidecar(args: Mapping[str, object]) -> bool:
    """Return whether parsed CLI/API args request the sidecar runtime."""
    return bool(args.get("use_sidecar"))


def selected_runtime(args: Mapping[str, object]) -> str:
    """Return the selected runtime for parsed CLI/API args."""
    if should_submit_to_sidecar(args):
        return SIDECAR_RUNTIME
    return DEFAULT_CUTOVER_POLICY.default_runtime


def cutover_policy_payload() -> dict[str, str]:
    """Return the current runtime cutover policy as plain data."""
    return DEFAULT_CUTOVER_POLICY.to_dict()
