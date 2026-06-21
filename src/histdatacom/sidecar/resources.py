"""Locate packaged Temporal sidecar resources."""

from __future__ import annotations

import json
import os
import platform
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Mapping

ASSET_PACKAGE = "histdatacom.sidecar"
ASSET_ROOT = "assets"
MANIFEST_FILENAME = "manifest.json"


class SidecarResourceError(RuntimeError):
    """Base class for sidecar resource lookup errors."""


class UnsupportedSidecarPlatform(SidecarResourceError):
    """Raised when no sidecar resource is declared for a platform."""


class SidecarExecutableUnavailable(SidecarResourceError):
    """Raised when a platform resource has no bundled executable yet."""


@dataclass(frozen=True, slots=True)
class SidecarPlatformResource:
    """Manifest entry for a platform-specific Temporal sidecar payload."""

    key: str
    bundled: bool
    executable: str
    wheel_tags: tuple[str, ...]
    notes: str = ""

    @classmethod
    def from_dict(
        cls, key: str, data: Mapping[str, Any]
    ) -> "SidecarPlatformResource":
        """Create a platform resource from manifest data."""
        return cls(
            key=key,
            bundled=bool(data.get("bundled", False)),
            executable=str(data.get("executable", "")),
            wheel_tags=tuple(str(tag) for tag in data.get("wheel_tags", [])),
            notes=str(data.get("notes", "")),
        )


@dataclass(frozen=True, slots=True)
class SidecarManifest:
    """Packaged Temporal sidecar distribution manifest."""

    schema_version: int
    sidecar: str
    distribution_strategy: str
    embedded_binary: bool
    resource_files: tuple[str, ...]
    platforms: dict[str, SidecarPlatformResource]
    sdist_fallback: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SidecarManifest":
        """Create a manifest from JSON-compatible data."""
        platforms = {
            str(key): SidecarPlatformResource.from_dict(str(key), value)
            for key, value in dict(data.get("platforms") or {}).items()
        }
        return cls(
            schema_version=int(data.get("schema_version", 0)),
            sidecar=str(data.get("sidecar", "")),
            distribution_strategy=str(data.get("distribution_strategy", "")),
            embedded_binary=bool(data.get("embedded_binary", False)),
            resource_files=tuple(
                str(item) for item in data.get("resource_files", [])
            ),
            platforms=platforms,
            sdist_fallback=str(data.get("sdist_fallback", "")),
        )


def _asset_root() -> Any:
    """Return the importlib resource root for sidecar assets."""
    return resources.files(ASSET_PACKAGE).joinpath(ASSET_ROOT)


def sidecar_asset(relative_path: str) -> Any:
    """Return a packaged sidecar asset by POSIX-style relative path."""
    path = PurePosixPath(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(
            f"sidecar asset path must be relative: {relative_path}"
        )

    asset = _asset_root()
    for part in path.parts:
        asset = asset.joinpath(part)

    if not asset.is_file():
        raise FileNotFoundError(f"sidecar asset not found: {relative_path}")
    return asset


def read_sidecar_asset_text(relative_path: str) -> str:
    """Read a sidecar asset as UTF-8 text."""
    return str(sidecar_asset(relative_path).read_text(encoding="utf-8"))


def load_sidecar_manifest() -> SidecarManifest:
    """Load the packaged sidecar manifest."""
    return SidecarManifest.from_dict(
        json.loads(read_sidecar_asset_text(MANIFEST_FILENAME))
    )


def current_platform_key(
    system: str | None = None,
    machine: str | None = None,
) -> str:
    """Return the manifest platform key for the current or provided machine."""
    normalized_system = (system or platform.system()).strip().lower()
    normalized_machine = (machine or platform.machine()).strip().lower()

    system_aliases = {
        "darwin": "macos",
        "mac": "macos",
        "macos": "macos",
        "linux": "linux",
        "windows": "windows",
    }
    machine_aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "x86-64": "x86_64",
        "aarch64": "arm64",
    }
    platform_system = system_aliases.get(normalized_system, normalized_system)
    platform_machine = machine_aliases.get(
        normalized_machine, normalized_machine
    )
    return f"{platform_system}-{platform_machine}"


def sidecar_platform_resource(
    platform_key: str | None = None,
    manifest: SidecarManifest | None = None,
) -> SidecarPlatformResource:
    """Return the declared sidecar resource for a platform."""
    loaded_manifest = manifest or load_sidecar_manifest()
    resolved_key = platform_key or current_platform_key()
    resource = loaded_manifest.platforms.get(resolved_key)
    if resource is None:
        supported = ", ".join(sorted(loaded_manifest.platforms))
        raise UnsupportedSidecarPlatform(
            "No packaged Temporal sidecar is declared for platform "
            f"{resolved_key!r}. Supported platform keys: {supported}."
        )
    return resource


@contextmanager
def sidecar_executable_path(
    platform_key: str | None = None,
    manifest: SidecarManifest | None = None,
) -> Iterator[Path]:
    """Yield the packaged Temporal executable path for a supported platform."""
    loaded_manifest = manifest or load_sidecar_manifest()
    resource = sidecar_platform_resource(platform_key, loaded_manifest)
    if not resource.bundled:
        raise SidecarExecutableUnavailable(
            "Temporal sidecar executable for platform "
            f"{resource.key!r} is not bundled in this distribution. "
            f"Strategy: {loaded_manifest.distribution_strategy}. "
            f"Sdist fallback: {loaded_manifest.sdist_fallback}. "
            f"Expected executable resource: {resource.executable}."
        )
    if not resource.executable:
        raise SidecarExecutableUnavailable(
            f"Temporal sidecar platform {resource.key!r} has no executable path."
        )

    asset = sidecar_asset(resource.executable)
    with resources.as_file(asset) as executable:
        if not executable.is_file():
            raise SidecarExecutableUnavailable(
                f"Temporal sidecar executable is missing: {executable}"
            )
        if os.name != "nt" and not os.access(executable, os.X_OK):
            raise SidecarExecutableUnavailable(
                f"Temporal sidecar executable is not executable: {executable}"
            )
        yield executable
