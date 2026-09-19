"""Deterministic selection of declarations, never activation or admission."""

from __future__ import annotations

import re

from .contracts import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
    _ATOM,
    _identifier,
    semver_key,
)


def _constraints(expression: str) -> tuple[tuple[str, str], ...]:
    if type(expression) is not str or len(expression) > 1024:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_SELECTION
        )
    if not expression:
        return ()
    parts = expression.split(",")
    if len(parts) > 8:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_SELECTION
        )
    result = []
    for part in parts:
        match = re.fullmatch(r"\s*(==|>=|<=|>|<)\s*(\S+)\s*", part)
        if match is None:
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVALID_SELECTION
            )
        operator, version = match.groups()
        semver_key(version)
        result.append((operator, version))
    return tuple(result)


def _matches(version: str, constraints: tuple[tuple[str, str], ...]) -> bool:
    for operator, expected in constraints:
        left, right = semver_key(version), semver_key(expected)
        accepted = {
            "==": version == expected,
            ">=": left >= right,
            "<=": left <= right,
            ">": left > right,
            "<": left < right,
        }[operator]
        if not accepted:
            return False
    return True


def _matching_candidates(
    inventory: BrokerPluginInventoryV1,
    *,
    plugin_id: str | None = None,
    provider_id: str | None = None,
    version_constraint: str = "",
) -> tuple[tuple[BrokerPluginCandidateV1, ...], tuple[tuple[str, str], ...]]:
    if type(inventory) is not BrokerPluginInventoryV1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_INVENTORY
        )
    diagnostic_ids = tuple(
        candidate.artifact_id for candidate in inventory.candidates
    )
    try:
        if plugin_id is None and provider_id is None:
            raise ValueError
        if plugin_id is not None:
            _identifier(plugin_id)
        if provider_id is not None and (
            type(provider_id) is not str
            or len(provider_id) > 128
            or not _ATOM.fullmatch(provider_id)
        ):
            raise ValueError
        constraints = _constraints(version_constraint)
    except (ValueError, TypeError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_SELECTION, diagnostic_ids
        ) from None
    matching = tuple(
        candidate
        for candidate in inventory.candidates
        if (plugin_id is None or candidate.registration.plugin_id == plugin_id)
        and (
            provider_id is None
            or provider_id in candidate.registration.provider_ids
        )
        and _matches(candidate.registration.plugin_version, constraints)
    )
    if not matching:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.UNKNOWN_PLUGIN, diagnostic_ids
        )
    return matching, constraints


def inspect_broker_plugins(
    inventory: BrokerPluginInventoryV1,
    *,
    plugin_id: str | None = None,
    provider_id: str | None = None,
    version_constraint: str = "",
) -> tuple[BrokerPluginCandidateV1, ...]:
    """Inspect all matching declarations, including incompatible/duplicate ones."""
    matching, _ = _matching_candidates(
        inventory,
        plugin_id=plugin_id,
        provider_id=provider_id,
        version_constraint=version_constraint,
    )
    return matching


def select_broker_plugin(
    inventory: BrokerPluginInventoryV1,
    *,
    plugin_id: str | None = None,
    provider_id: str | None = None,
    version_constraint: str = "",
) -> BrokerPluginCandidateV1:
    """Require one compatible declaration; never activate or pick latest.

    Comparators use plugin SemVer, not package versions. Exact equality includes
    the build label; ranges use SemVer precedence (ignoring build labels).
    """
    if type(inventory) is not BrokerPluginInventoryV1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_INVENTORY
        )
    if inventory.diagnostics:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INCOMPLETE_INVENTORY,
            tuple(c.artifact_id for c in inventory.candidates),
        )
    matching, constraints = _matching_candidates(
        inventory,
        plugin_id=plugin_id,
        provider_id=provider_id,
        version_constraint=version_constraint,
    )
    compatible = tuple(
        candidate
        for candidate in matching
        if candidate.registration.supports_sdk(inventory.sdk_version)
    )
    if not compatible:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INCOMPATIBLE_SDK,
            tuple(c.artifact_id for c in matching),
        )
    if len(compatible) != 1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.AMBIGUOUS_SELECTION,
            tuple(c.artifact_id for c in compatible),
        )
    # A provider filter must not accidentally select one of two active builds
    # advertising the same stable ID. Only an explicit version filter resolves.
    chosen = compatible[0]
    duplicates = tuple(
        candidate
        for candidate in inventory.candidates
        if candidate.registration.plugin_id == chosen.registration.plugin_id
        and candidate.registration.supports_sdk(inventory.sdk_version)
        and _matches(candidate.registration.plugin_version, constraints)
    )
    if len(duplicates) != 1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.AMBIGUOUS_SELECTION,
            tuple(c.artifact_id for c in duplicates),
        )
    return chosen
