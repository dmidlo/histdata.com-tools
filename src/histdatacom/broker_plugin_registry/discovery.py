"""Read installed wheel metadata and RECORD-bound resources, never imports."""

from __future__ import annotations

import base64
import hashlib
from importlib import metadata
from pathlib import Path, PurePosixPath

from .contracts import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    MAX_PLUGIN_REGISTRATIONS,
    MAX_REGISTRATION_BYTES,
    BrokerPluginCandidateV1,
    BrokerPluginDiscoveryDiagnosticV1,
    BrokerPluginInventoryV1,
    BrokerPluginRegistrationV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
    _ENTRY,
    _identifier,
    _text,
    normalized_distribution_name,
    parse_registry_json,
)
from .storage import _read_regular_bytes

MAX_IMPLEMENTATION_BYTES = 8 * 1024 * 1024
MAX_DISTRIBUTION_FILES = 16_384


def registration_resource_path(plugin_id: str, entry_point: str) -> str:
    """Wheel package-data convention, independent of executable imports."""
    _identifier(plugin_id)
    if not _ENTRY.fullmatch(_text(entry_point, 256)):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_REGISTRATION
        )
    module = entry_point.split(":", 1)[0]
    parts = module.split(".")
    if len(parts) < 2 or not all(
        part.isidentifier() and part.isascii() for part in parts
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_REGISTRATION
        )
    return str(
        PurePosixPath(
            *parts[:-1], "_histdatacom_broker_plugins", plugin_id + ".json"
        )
    )


def _read_owned_resource(
    distribution: metadata.PathDistribution,
    path: str,
    limit: int,
) -> bytes:
    files = distribution.files
    if files is None or len(files) > MAX_DISTRIBUTION_FILES:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_UNAVAILABLE
        )
    matches = [item for item in files if item.as_posix() == path]
    if len(matches) != 1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.PACKAGE_MISMATCH
        )
    record = matches[0]
    root = distribution.locate_file("")
    located = distribution.locate_file(record)
    if not isinstance(root, Path) or not isinstance(located, Path):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_UNAVAILABLE
        )
    expected = root.resolve() / path
    if located.resolve() != expected or not expected.is_relative_to(
        root.resolve()
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.PACKAGE_MISMATCH
        )
    if (
        record.hash is None
        or record.hash.mode != "sha256"
        or record.size is None
        or not 0 <= record.size <= limit
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_INTEGRITY
        )
    data = _read_regular_bytes(located, limit)
    digest = (
        base64.urlsafe_b64encode(hashlib.sha256(data).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    if (
        len(data) != record.size
        or len(data) > limit
        or record.hash.value != digest
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_INTEGRITY
        )
    return data


def _candidate(
    distribution: metadata.PathDistribution,
    entry: metadata.EntryPoint,
) -> BrokerPluginCandidateV1:
    path = registration_resource_path(entry.name, entry.value)
    raw = _read_owned_resource(distribution, path, MAX_REGISTRATION_BYTES)
    registration = BrokerPluginRegistrationV1.from_dict(
        parse_registry_json(raw.decode("utf-8"), MAX_REGISTRATION_BYTES)
    )
    if (
        registration.plugin_id != entry.name
        or registration.entry_point != entry.value
        or registration.distribution_name
        != normalized_distribution_name(distribution.metadata.get("Name", ""))
        or registration.distribution_version != distribution.version
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.PACKAGE_MISMATCH
        )
    module = entry.value.split(":", 1)[0].replace(".", "/")
    files = distribution.files
    if files is None:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_UNAVAILABLE
        )
    implementations = [
        path
        for path in (module + ".py", module + "/__init__.py")
        if any(item.as_posix() == path for item in files)
    ]
    if len(implementations) != 1:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.PACKAGE_MISMATCH
        )
    implementation = _read_owned_resource(
        distribution, implementations[0], MAX_IMPLEMENTATION_BYTES
    )
    return BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(raw).hexdigest(),
        hashlib.sha256(implementation).hexdigest(),
    )


def discover_broker_plugins() -> BrokerPluginInventoryV1:
    """Fresh installed inventory; malformed relevant distributions fail closed.

    The standard importlib metadata provider is trusted host infrastructure.
    Only ordinary unpacked wheel PathDistributions/RECORD resources qualify;
    this function never resolves/imports an entry point or reads configuration.
    """
    candidates: list[BrokerPluginCandidateV1] = []
    diagnostics: list[BrokerPluginDiscoveryDiagnosticV1] = []
    entries: list[
        tuple[str, str, metadata.PathDistribution, metadata.EntryPoint]
    ] = []

    def check_capacity(extra: int = 0) -> None:
        if len(entries) + len(diagnostics) + extra > MAX_PLUGIN_REGISTRATIONS:
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.INVENTORY_LIMIT
            )

    try:
        for distribution in metadata.distributions():
            if type(distribution) is not metadata.PathDistribution:
                diagnostics.append(
                    BrokerPluginDiscoveryDiagnosticV1(
                        BrokerPluginRegistryReason.METADATA_UNAVAILABLE
                    )
                )
                check_capacity()
                continue
            relevant: list[metadata.EntryPoint] = []
            for entry in distribution.entry_points:
                if entry.group == BROKER_PLUGIN_ENTRY_POINT_GROUP:
                    check_capacity(len(relevant) + 1)
                    relevant.append(entry)
            if not relevant:
                continue
            try:
                name = normalized_distribution_name(
                    distribution.metadata.get("Name", "")
                )
                version = distribution.version
                if not isinstance(version, str) or len(version) > 128:
                    raise ValueError
            except (ValueError, TypeError, KeyError):
                diagnostics.append(
                    BrokerPluginDiscoveryDiagnosticV1(
                        BrokerPluginRegistryReason.METADATA_UNAVAILABLE
                    )
                )
                check_capacity()
                continue
            for entry in relevant:
                try:
                    _identifier(entry.name)
                    if type(entry.value) is not str or len(entry.value) > 256:
                        raise ValueError
                    entries.append((name, version, distribution, entry))
                except (ValueError, TypeError):
                    diagnostics.append(
                        BrokerPluginDiscoveryDiagnosticV1(
                            BrokerPluginRegistryReason.INVALID_REGISTRATION,
                            name,
                        )
                    )
                check_capacity()
    except BrokerPluginRegistryError:
        raise
    except Exception:
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.METADATA_UNAVAILABLE
        ) from None
    for name, _version, distribution, entry in sorted(
        entries,
        key=lambda item: (item[3].name, item[1], item[0], item[3].value),
    ):
        plugin_id = ""
        try:
            _identifier(entry.name)
            plugin_id = entry.name
            candidates.append(_candidate(distribution, entry))
        except BrokerPluginRegistryError as error:
            diagnostics.append(
                BrokerPluginDiscoveryDiagnosticV1(error.reason, name, plugin_id)
            )
        except Exception:
            diagnostics.append(
                BrokerPluginDiscoveryDiagnosticV1(
                    BrokerPluginRegistryReason.RESOURCE_UNAVAILABLE,
                    name,
                    plugin_id,
                )
            )
    return BrokerPluginInventoryV1(tuple(candidates), tuple(diagnostics))
