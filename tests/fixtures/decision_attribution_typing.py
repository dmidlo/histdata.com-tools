"""Installed public attribution consumer without implementation imports."""

from pathlib import Path

from histdatacom.attribution import (
    AttributionRegistryV1,
    DecisionAttributionV1,
    ExplanationPolicyRegistryV1,
    read_attribution_artifact,
    write_attribution_artifact,
)


def load_registry(path: Path) -> AttributionRegistryV1:
    return read_attribution_artifact(path, AttributionRegistryV1)


def persist_registry(registry: AttributionRegistryV1, directory: Path) -> Path:
    return write_attribution_artifact(registry, directory)


def decisions(
    registry: AttributionRegistryV1,
) -> tuple[DecisionAttributionV1, ...]:
    return registry.attributions


def policies(registry: AttributionRegistryV1) -> ExplanationPolicyRegistryV1:
    return registry.policy_registry
