"""Offline permission declarations from fresh, RECORD-bound installed wheels."""

from __future__ import annotations

from importlib import metadata
from pathlib import PurePosixPath

from histdatacom.broker_plugin_registry import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    BrokerPluginCandidateV1,
    discover_broker_plugins,
    normalized_distribution_name,
    registration_resource_path,
)
from histdatacom.broker_plugin_registry.discovery import (
    _candidate,
    _read_owned_resource,
)

from ._wire import MAX_BYTES
from .contracts import BrokerPermissionManifestV1
from .decisions import BrokerPermissionError


def permission_resource_path(plugin_id: str, entry_point: str) -> str:
    """Sibling package-data declaration; lookup never executes plugin code."""
    try:
        registration = PurePosixPath(
            registration_resource_path(plugin_id, entry_point)
        )
        return str(
            registration.parent.parent
            / "_histdatacom_broker_permissions"
            / registration.name
        )
    except (ValueError, TypeError):
        raise BrokerPermissionError(
            "invalid_permission_resource_location"
        ) from None


def _fresh_candidate(candidate: BrokerPluginCandidateV1) -> str:
    inventory = discover_broker_plugins()
    if (
        inventory.diagnostics
        or sum(item == candidate for item in inventory.candidates) != 1
    ):
        raise BrokerPermissionError("permission_installed_selection_changed")
    version = inventory.sdk_version
    if type(version) is not str:
        raise BrokerPermissionError("invalid_installed_sdk_version")
    return version


def read_installed_broker_permissions(
    candidate: BrokerPluginCandidateV1,
) -> BrokerPermissionManifestV1:
    """Read the exact declaration for one freshly verified installed candidate.

    Trust boundary matches metadata-only broker discovery: ordinary unpacked
    wheels, exact registration and entrypoint implementation hashes, unique
    entry ownership and SHA256/size-matching RECORD resources. No imports,
    entrypoint loading, configuration reads, operator grants or network calls.
    Fresh reads detect removal/substitution, not hostile same-user atomicity.
    """
    if type(candidate) is not BrokerPluginCandidateV1:
        raise BrokerPermissionError("invalid_permission_candidate")
    try:
        candidate = BrokerPluginCandidateV1.from_json(candidate.to_json())
        sdk_version = _fresh_candidate(candidate)
        registration = candidate.registration
        matches: list[tuple[metadata.PathDistribution, metadata.EntryPoint]] = (
            []
        )
        for distribution in metadata.distributions():
            if type(distribution) is not metadata.PathDistribution:
                continue
            for entry in distribution.entry_points:
                if (
                    entry.group == BROKER_PLUGIN_ENTRY_POINT_GROUP
                    and entry.name == registration.plugin_id
                    and entry.value == registration.entry_point
                    and normalized_distribution_name(
                        distribution.metadata.get("Name", "")
                    )
                    == registration.distribution_name
                    and distribution.version
                    == registration.distribution_version
                ):
                    matches.append((distribution, entry))
                    if len(matches) > 1:
                        raise BrokerPermissionError(
                            "ambiguous_permission_resource_owner"
                        )
        if len(matches) != 1:
            raise BrokerPermissionError("permission_resource_owner_missing")
        distribution, entry = matches[0]
        if _candidate(distribution, entry) != candidate:
            raise BrokerPermissionError(
                "permission_installed_selection_changed"
            )
        data = _read_owned_resource(
            distribution,
            permission_resource_path(
                registration.plugin_id, registration.entry_point
            ),
            MAX_BYTES,
        )
        manifest = BrokerPermissionManifestV1.from_json(data.decode("ascii"))
        if (
            manifest.candidate_id != candidate.artifact_id
            or manifest.distribution_name != registration.distribution_name
            or manifest.distribution_version
            != registration.distribution_version
            or manifest.sdk_version != sdk_version
            or not registration.supports_sdk(manifest.sdk_version)
            or manifest.provider_ids != registration.provider_ids
        ):
            raise BrokerPermissionError(
                "permission_manifest_candidate_mismatch"
            )
        if (
            _candidate(distribution, entry) != candidate
            or _fresh_candidate(candidate) != sdk_version
        ):
            raise BrokerPermissionError(
                "permission_installed_selection_changed"
            )
        # Re-read the declaration after native candidate verification. The
        # authority pins these exact bytes/ID; no path alone becomes provenance.
        if (
            _read_owned_resource(
                distribution,
                permission_resource_path(
                    registration.plugin_id, registration.entry_point
                ),
                MAX_BYTES,
            )
            != data
        ):
            raise BrokerPermissionError(
                "permission_manifest_changed_during_read"
            )
        return manifest
    except BrokerPermissionError:
        raise
    # Keep arbitrary third-party metadata exception content out of diagnostics.
    except (Exception, SystemExit):  # noqa: BLE001
        raise BrokerPermissionError(
            "invalid_installed_permission_manifest"
        ) from None
