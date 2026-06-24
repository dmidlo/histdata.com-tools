"""Temporal orchestration runtime cutover policy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

ORCHESTRATION_RUNTIME = "orchestration"
FOREGROUND_RUNTIME_REMOVED_MESSAGE = (
    "The foreground compatibility runtime has been removed. Use the default "
    "Temporal orchestration runtime instead."
)


@dataclass(frozen=True, slots=True)
class RuntimeCutoverPolicy:
    """Document the current production runtime selection contract."""

    default_runtime: str
    orchestration_activation: str
    foreground_lifecycle: str
    config_globals_lifecycle: str
    unavailable_runtime_behavior: str
    unsupported_artifact_behavior: str

    def to_dict(self) -> dict[str, str]:
        """Return a JSON/documentation friendly representation."""
        return asdict(self)


DEFAULT_CUTOVER_POLICY = RuntimeCutoverPolicy(
    default_runtime=ORCHESTRATION_RUNTIME,
    orchestration_activation=(
        "default CLI/API runtime; work is submitted through Temporal "
        "orchestration"
    ),
    foreground_lifecycle=(
        "removed after the documented release window; --foreground is no "
        "longer accepted and Options.use_orchestration = False is rejected"
    ),
    config_globals_lifecycle=(
        "CLI/API runtime selection uses resolved runtime context and "
        "RunRequest payloads; legacy helper surfaces accept explicit "
        "argument mappings instead of ambient parser state"
    ),
    unavailable_runtime_behavior=(
        "default orchestration requests start the local runtime when needed; "
        "startup or connection failures raise OrchestrationUnavailableError "
        "or exit nonzero with a clear message"
    ),
    unsupported_artifact_behavior=(
        "metadata-only wheels and unsupported platforms require an operator "
        "provided Temporal executable for orchestration startup"
    ),
)


def should_submit_to_orchestration(args: Mapping[str, object]) -> bool:
    """Return whether parsed CLI/API args request orchestration."""
    if args.get("use_orchestration") is False:
        raise ValueError(FOREGROUND_RUNTIME_REMOVED_MESSAGE)
    return True


def selected_runtime(args: Mapping[str, object]) -> str:
    """Return the selected runtime for parsed CLI/API args."""
    should_submit_to_orchestration(args)
    return ORCHESTRATION_RUNTIME


def cutover_policy_payload() -> dict[str, str]:
    """Return the current runtime cutover policy as plain data."""
    return DEFAULT_CUTOVER_POLICY.to_dict()
