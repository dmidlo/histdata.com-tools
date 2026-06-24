"""Locate packaged Temporal orchestration resources."""

from __future__ import annotations

import json
import os
import platform
import hashlib
import shutil
import tarfile
import time
import urllib.request
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import IO, Any, Callable, Iterator, Mapping

ASSET_PACKAGE = "histdatacom.orchestration"
ASSET_ROOT = "assets"
MANIFEST_FILENAME = "manifest.json"
TEMPORAL_EXECUTABLE_ENV = "HISTDATACOM_TEMPORAL_EXECUTABLE"
TEMPORAL_CACHE_DIR_ENV = "HISTDATACOM_TEMPORAL_CACHE_DIR"
TEMPORAL_OFFLINE_ENV = "HISTDATACOM_TEMPORAL_OFFLINE"
DEFAULT_TEMPORAL_RUNTIME_DOWNLOAD_TIMEOUT_SECONDS = 120.0
DEFAULT_TEMPORAL_RUNTIME_LOCK_TIMEOUT_SECONDS = 300.0
DEFAULT_TEMPORAL_RUNTIME_LOCK_POLL_SECONDS = 0.05

DownloadArchive = Callable[["TemporalRuntimeArtifact", Path, float], None]


class OrchestrationResourceError(RuntimeError):
    """Base class for orchestration resource lookup errors."""


class UnsupportedOrchestrationPlatform(OrchestrationResourceError):
    """Raised when no orchestration resource is declared for a platform."""


class OrchestrationExecutableUnavailable(OrchestrationResourceError):
    """Raised when a platform resource has no bundled executable yet."""


class TemporalRuntimeProvisioningError(OrchestrationResourceError):
    """Raised when the Temporal executable cannot be provisioned."""


class TemporalRuntimeChecksumError(TemporalRuntimeProvisioningError):
    """Raised when a downloaded or cached Temporal artifact fails verification."""


class TemporalRuntimeOfflineError(TemporalRuntimeProvisioningError):
    """Raised when offline policy prevents first-run provisioning."""


@dataclass(frozen=True, slots=True)
class OrchestrationPlatformResource:
    """Manifest entry for a platform-specific Temporal runtime payload."""

    key: str
    bundled: bool
    executable: str
    wheel_tags: tuple[str, ...]
    provenance: str = ""
    notes: str = ""

    @classmethod
    def from_dict(
        cls, key: str, data: Mapping[str, Any]
    ) -> "OrchestrationPlatformResource":
        """Create a platform resource from manifest data."""
        return cls(
            key=key,
            bundled=bool(data.get("bundled", False)),
            executable=str(data.get("executable", "")),
            wheel_tags=tuple(str(tag) for tag in data.get("wheel_tags", [])),
            provenance=str(data.get("provenance", "")),
            notes=str(data.get("notes", "")),
        )


@dataclass(frozen=True, slots=True)
class OrchestrationManifest:
    """Packaged Temporal runtime distribution manifest."""

    schema_version: int
    runtime: str
    distribution_strategy: str
    embedded_binary: bool
    resource_files: tuple[str, ...]
    runtime_artifact_index: str
    platforms: dict[str, OrchestrationPlatformResource]
    sdist_fallback: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OrchestrationManifest":
        """Create a manifest from JSON-compatible data."""
        platforms = {
            str(key): OrchestrationPlatformResource.from_dict(str(key), value)
            for key, value in dict(data.get("platforms") or {}).items()
        }
        return cls(
            schema_version=int(data.get("schema_version", 0)),
            runtime=str(data.get("runtime", data.get("orchestration", ""))),
            distribution_strategy=str(data.get("distribution_strategy", "")),
            embedded_binary=bool(data.get("embedded_binary", False)),
            resource_files=tuple(
                str(item) for item in data.get("resource_files", [])
            ),
            runtime_artifact_index=str(data.get("runtime_artifact_index", "")),
            platforms=platforms,
            sdist_fallback=str(data.get("sdist_fallback", "")),
        )


@dataclass(frozen=True, slots=True)
class TemporalRuntimeArtifact:
    """Pinned Temporal runtime artifact for one platform."""

    platform_key: str
    system: str
    machine: str
    archive_name: str
    archive_format: str
    archive_url: str
    archive_sha256: str
    archive_size_bytes: int
    executable_name: str
    license: str
    upstream_repository: str

    @classmethod
    def from_dict(
        cls,
        platform_key: str,
        data: Mapping[str, Any],
    ) -> "TemporalRuntimeArtifact":
        """Create artifact metadata from packaged index data."""
        size = int(data.get("archive_size_bytes", 0) or 0)
        if size <= 0:
            raise TemporalRuntimeProvisioningError(
                "Temporal runtime index entry "
                f"{platform_key!r} must define a positive archive size."
            )
        return cls(
            platform_key=platform_key,
            system=str(data.get("system", "")),
            machine=str(data.get("machine", "")),
            archive_name=str(data.get("archive_name", "")),
            archive_format=str(data.get("archive_format", "")),
            archive_url=str(data.get("archive_url", "")),
            archive_sha256=str(data.get("archive_sha256", "")).lower(),
            archive_size_bytes=size,
            executable_name=str(data.get("executable_name", "")),
            license=str(data.get("license", "")),
            upstream_repository=str(data.get("upstream_repository", "")),
        )


@dataclass(frozen=True, slots=True)
class TemporalRuntimeIndex:
    """Packaged index of Temporal runtime artifacts."""

    schema_version: int
    component: str
    version: str
    release_base_url: str
    platforms: dict[str, TemporalRuntimeArtifact]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TemporalRuntimeIndex":
        """Create a Temporal runtime index from package data."""
        platforms = {
            str(key): TemporalRuntimeArtifact.from_dict(str(key), value)
            for key, value in dict(data.get("platforms") or {}).items()
        }
        return cls(
            schema_version=int(data.get("schema_version", 0) or 0),
            component=str(data.get("component", "")),
            version=str(data.get("version", "")),
            release_base_url=str(data.get("release_base_url", "")),
            platforms=platforms,
        )


@dataclass(frozen=True, slots=True)
class TemporalRuntimeResolution:
    """Resolved Temporal executable path and provenance."""

    executable: Path
    source: str
    platform_key: str
    version: str
    archive_sha256: str = ""
    cache_entry: Path | None = None
    provenance_path: Path | None = None
    network_fetch: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible resolution payload."""
        return {
            "executable": str(self.executable),
            "source": self.source,
            "platform": self.platform_key,
            "version": self.version,
            "archive_sha256": self.archive_sha256,
            "cache_entry": str(self.cache_entry or ""),
            "provenance_path": str(self.provenance_path or ""),
            "network_fetch": self.network_fetch,
        }


@dataclass(frozen=True, slots=True)
class TemporalRuntimeCacheEntry:
    """Inspectable cache entry for a provisioned Temporal executable."""

    path: Path
    executable: Path
    provenance_path: Path
    platform_key: str
    version: str
    archive_sha256: str
    valid: bool
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible cache-entry payload."""
        return {
            "path": str(self.path),
            "executable": str(self.executable),
            "provenance_path": str(self.provenance_path),
            "platform": self.platform_key,
            "version": self.version,
            "archive_sha256": self.archive_sha256,
            "valid": self.valid,
            "reason": self.reason,
        }


def _asset_root() -> Any:
    """Return the importlib resource root for orchestration assets."""
    return resources.files(ASSET_PACKAGE).joinpath(ASSET_ROOT)


def orchestration_asset(relative_path: str) -> Any:
    """Return a packaged orchestration asset by POSIX-style relative path."""
    path = PurePosixPath(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(
            f"orchestration asset path must be relative: {relative_path}"
        )

    asset = _asset_root()
    for part in path.parts:
        asset = asset.joinpath(part)

    if not asset.is_file():
        raise FileNotFoundError(
            f"orchestration asset not found: {relative_path}"
        )
    return asset


def read_orchestration_asset_text(relative_path: str) -> str:
    """Read an orchestration asset as UTF-8 text."""
    return str(orchestration_asset(relative_path).read_text(encoding="utf-8"))


def load_orchestration_manifest() -> OrchestrationManifest:
    """Load the packaged orchestration manifest."""
    return OrchestrationManifest.from_dict(
        json.loads(read_orchestration_asset_text(MANIFEST_FILENAME))
    )


def load_temporal_runtime_index(
    manifest: OrchestrationManifest | None = None,
) -> TemporalRuntimeIndex:
    """Load the packaged Temporal runtime artifact index."""
    loaded_manifest = manifest or load_orchestration_manifest()
    if not loaded_manifest.runtime_artifact_index:
        raise TemporalRuntimeProvisioningError(
            "Orchestration manifest does not declare a Temporal runtime artifact index."
        )
    return TemporalRuntimeIndex.from_dict(
        json.loads(
            read_orchestration_asset_text(
                loaded_manifest.runtime_artifact_index
            )
        )
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


def temporal_runtime_artifact(
    platform_key: str | None = None,
    index: TemporalRuntimeIndex | None = None,
) -> TemporalRuntimeArtifact:
    """Return pinned Temporal runtime artifact metadata for a platform."""
    loaded_index = index or load_temporal_runtime_index()
    resolved_key = platform_key or current_platform_key()
    artifact = loaded_index.platforms.get(resolved_key)
    if artifact is None:
        supported = ", ".join(sorted(loaded_index.platforms))
        raise UnsupportedOrchestrationPlatform(
            "No pinned Temporal runtime artifact is declared for platform "
            f"{resolved_key!r}. Supported platform keys: {supported}."
        )
    return artifact


def default_temporal_runtime_cache_dir(
    *,
    environ: Mapping[str, str] | None = None,
    platform_name: str | None = None,
    home: Path | str | None = None,
) -> Path:
    """Return the per-user Temporal executable cache directory."""
    env = environ if environ is not None else os.environ
    override = env.get(TEMPORAL_CACHE_DIR_ENV)
    if override:
        return Path(override).expanduser()

    user_home = Path(home).expanduser() if home is not None else Path.home()
    system = platform_name or platform.system()
    if system == "Darwin":
        return user_home / "Library" / "Caches" / "histdatacom" / "temporal-cli"
    if system == "Windows":
        local_app_data = env.get("LOCALAPPDATA")
        if local_app_data:
            return (
                Path(local_app_data).expanduser()
                / "histdatacom"
                / "Cache"
                / "temporal-cli"
            )
        return (
            user_home
            / "AppData"
            / "Local"
            / "histdatacom"
            / "Cache"
            / "temporal-cli"
        )

    xdg_cache_home = env.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        return (
            Path(xdg_cache_home).expanduser() / "histdatacom" / "temporal-cli"
        )
    return user_home / ".cache" / "histdatacom" / "temporal-cli"


def temporal_runtime_cache_entry_dir(
    artifact: TemporalRuntimeArtifact,
    *,
    version: str,
    cache_dir: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Return the cache directory for a pinned Temporal artifact."""
    root = (
        Path(cache_dir).expanduser()
        if cache_dir is not None
        else default_temporal_runtime_cache_dir(environ=environ)
    )
    return (
        root / f"v{version}" / artifact.platform_key / artifact.archive_sha256
    )


def orchestration_platform_resource(
    platform_key: str | None = None,
    manifest: OrchestrationManifest | None = None,
) -> OrchestrationPlatformResource:
    """Return the declared orchestration resource for a platform."""
    loaded_manifest = manifest or load_orchestration_manifest()
    resolved_key = platform_key or current_platform_key()
    resource = loaded_manifest.platforms.get(resolved_key)
    if resource is None:
        supported = ", ".join(sorted(loaded_manifest.platforms))
        raise UnsupportedOrchestrationPlatform(
            "No packaged Temporal orchestration is declared for platform "
            f"{resolved_key!r}. Supported platform keys: {supported}."
        )
    return resource


@contextmanager
def orchestration_executable_path(
    platform_key: str | None = None,
    manifest: OrchestrationManifest | None = None,
) -> Iterator[Path]:
    """Yield the packaged Temporal executable path for a supported platform."""
    loaded_manifest = manifest or load_orchestration_manifest()
    resource = orchestration_platform_resource(platform_key, loaded_manifest)
    if not resource.bundled:
        raise OrchestrationExecutableUnavailable(
            "Temporal orchestration executable for platform "
            f"{resource.key!r} is not bundled in this distribution. "
            f"Strategy: {loaded_manifest.distribution_strategy}. "
            f"Sdist fallback: {loaded_manifest.sdist_fallback}. "
            f"Expected executable resource: {resource.executable}."
        )
    if not resource.executable:
        raise OrchestrationExecutableUnavailable(
            f"Temporal orchestration platform {resource.key!r} has no executable path."
        )

    asset = orchestration_asset(resource.executable)
    with resources.as_file(asset) as executable:
        if not executable.is_file():
            raise OrchestrationExecutableUnavailable(
                f"Temporal orchestration executable is missing: {executable}"
            )
        if os.name != "nt" and not os.access(executable, os.X_OK):
            raise OrchestrationExecutableUnavailable(
                f"Temporal orchestration executable is not executable: {executable}"
            )
        yield executable


@contextmanager
def temporal_runtime_executable_path(
    *,
    explicit_executable: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    cache_dir: Path | str | None = None,
    platform_key: str | None = None,
    manifest: OrchestrationManifest | None = None,
    index: TemporalRuntimeIndex | None = None,
    allow_download: bool = True,
    download_archive: DownloadArchive | None = None,
    download_timeout: float = DEFAULT_TEMPORAL_RUNTIME_DOWNLOAD_TIMEOUT_SECONDS,
    lock_timeout: float = DEFAULT_TEMPORAL_RUNTIME_LOCK_TIMEOUT_SECONDS,
) -> Iterator[TemporalRuntimeResolution]:
    """Yield a verified Temporal executable from override, bundle, or cache."""
    env = environ if environ is not None else os.environ
    loaded_manifest = manifest or load_orchestration_manifest()
    loaded_index = index or load_temporal_runtime_index(loaded_manifest)
    resolved_key = platform_key or current_platform_key()

    explicit = _explicit_temporal_executable(explicit_executable, env)
    if explicit is not None:
        yield TemporalRuntimeResolution(
            executable=_validated_executable(explicit, source="explicit"),
            source=(
                "environment" if explicit_executable is None else "explicit"
            ),
            platform_key=resolved_key,
            version=loaded_index.version,
        )
        return

    try:
        with orchestration_executable_path(
            resolved_key, loaded_manifest
        ) as executable:
            artifact = temporal_runtime_artifact(resolved_key, loaded_index)
            resource = orchestration_platform_resource(
                resolved_key, loaded_manifest
            )
            _validate_packaged_temporal_runtime(
                resource,
                executable,
                artifact,
                loaded_index,
            )
            yield TemporalRuntimeResolution(
                executable=executable,
                source="packaged",
                platform_key=resolved_key,
                version=loaded_index.version,
                archive_sha256=artifact.archive_sha256,
            )
            return
    except OrchestrationExecutableUnavailable:
        pass

    artifact = temporal_runtime_artifact(resolved_key, loaded_index)
    cache_entry = temporal_runtime_cache_entry_dir(
        artifact,
        version=loaded_index.version,
        cache_dir=cache_dir,
        environ=env,
    )
    cached = _cache_resolution(cache_entry, artifact, loaded_index)
    if cached is not None:
        yield cached
        return

    if _truthy(env.get(TEMPORAL_OFFLINE_ENV, "")) or not allow_download:
        raise TemporalRuntimeOfflineError(
            "Temporal executable is not available from an explicit path, "
            "packaged offline/private wheel, or verified cache entry, and "
            "runtime provisioning is offline or disabled. Provision the cache "
            "on a connected machine, install an offline/private bundled wheel, "
            "or pass --executable."
        )

    provisioned = _provision_temporal_runtime(
        cache_entry,
        artifact,
        loaded_index,
        download_archive=download_archive or _download_temporal_archive,
        download_timeout=download_timeout,
        lock_timeout=lock_timeout,
    )
    yield provisioned


def resolve_temporal_runtime_executable(
    **kwargs: Any,
) -> TemporalRuntimeResolution:
    """Resolve a Temporal executable for non-packaged-resource call sites."""
    with temporal_runtime_executable_path(**kwargs) as resolution:
        if resolution.source == "packaged":
            raise TemporalRuntimeProvisioningError(
                "Packaged Temporal executable resolutions must be consumed "
                "through temporal_runtime_executable_path()."
            )
        return resolution


def inspect_temporal_runtime_cache(
    *,
    cache_dir: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
) -> tuple[TemporalRuntimeCacheEntry, ...]:
    """Return all known Temporal runtime cache entries."""
    root = (
        Path(cache_dir).expanduser()
        if cache_dir is not None
        else default_temporal_runtime_cache_dir(environ=environ)
    )
    if not root.exists():
        return ()

    entries: list[TemporalRuntimeCacheEntry] = []
    for provenance_path in sorted(root.glob("v*/*/*/provenance.json")):
        entry_dir = provenance_path.parent
        try:
            payload = json.loads(provenance_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as err:
            entries.append(
                TemporalRuntimeCacheEntry(
                    path=entry_dir,
                    executable=entry_dir / "bin" / "temporal",
                    provenance_path=provenance_path,
                    platform_key="",
                    version="",
                    archive_sha256="",
                    valid=False,
                    reason=f"unreadable provenance: {err}",
                )
            )
            continue
        executable = entry_dir / "bin" / str(payload.get("executable_name", ""))
        version = str(payload.get("version", ""))
        platform_value = str(payload.get("platform", ""))
        archive_sha256 = str(payload.get("archive_sha256", ""))
        valid, reason = _validate_cached_executable(entry_dir, payload)
        entries.append(
            TemporalRuntimeCacheEntry(
                path=entry_dir,
                executable=executable,
                provenance_path=provenance_path,
                platform_key=platform_value,
                version=version,
                archive_sha256=archive_sha256,
                valid=valid,
                reason=reason,
            )
        )
    return tuple(entries)


def prune_temporal_runtime_cache(
    *,
    cache_dir: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
    keep_current: bool = True,
    platform_key: str | None = None,
    index: TemporalRuntimeIndex | None = None,
) -> dict[str, Any]:
    """Prune cached Temporal runtimes and return a JSON-ready summary."""
    env = environ if environ is not None else os.environ
    root = (
        Path(cache_dir).expanduser()
        if cache_dir is not None
        else default_temporal_runtime_cache_dir(environ=env)
    )
    current_path: Path | None = None
    if keep_current:
        loaded_index = index or load_temporal_runtime_index()
        artifact = temporal_runtime_artifact(platform_key, loaded_index)
        current_path = temporal_runtime_cache_entry_dir(
            artifact,
            version=loaded_index.version,
            cache_dir=root,
            environ=env,
        )

    deleted: list[str] = []
    kept: list[str] = []
    for entry in inspect_temporal_runtime_cache(cache_dir=root, environ=env):
        if current_path is not None and entry.path == current_path:
            kept.append(str(entry.path))
            continue
        shutil.rmtree(entry.path, ignore_errors=True)
        deleted.append(str(entry.path))
    return {
        "cache_dir": str(root),
        "deleted": deleted,
        "kept": kept,
    }


def _explicit_temporal_executable(
    explicit_executable: Path | str | None,
    environ: Mapping[str, str],
) -> Path | None:
    if explicit_executable is not None and str(explicit_executable).strip():
        return Path(explicit_executable).expanduser()
    env_executable = environ.get(TEMPORAL_EXECUTABLE_ENV, "")
    if env_executable.strip():
        return Path(env_executable).expanduser()
    return None


def _validated_executable(path: Path, *, source: str) -> Path:
    if not path.is_file():
        raise TemporalRuntimeProvisioningError(
            f"Temporal executable from {source} does not exist: {path}"
        )
    if os.name != "nt" and not os.access(path, os.X_OK):
        raise TemporalRuntimeProvisioningError(
            f"Temporal executable from {source} is not executable: {path}"
        )
    return path


def _cache_resolution(
    entry_dir: Path,
    artifact: TemporalRuntimeArtifact,
    index: TemporalRuntimeIndex,
) -> TemporalRuntimeResolution | None:
    provenance_path = entry_dir / "provenance.json"
    if not provenance_path.is_file():
        return None
    try:
        payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    valid, _reason = _validate_cached_executable(entry_dir, payload)
    if not valid:
        return None
    if (
        str(payload.get("version", "")) != index.version
        or str(payload.get("platform", "")) != artifact.platform_key
        or str(payload.get("archive_sha256", "")).lower()
        != artifact.archive_sha256
        or str(payload.get("executable_name", "")) != artifact.executable_name
    ):
        return None
    executable = _validated_executable(
        entry_dir / "bin" / artifact.executable_name,
        source="cache",
    )
    return TemporalRuntimeResolution(
        executable=executable,
        source="cache",
        platform_key=artifact.platform_key,
        version=index.version,
        archive_sha256=artifact.archive_sha256,
        cache_entry=entry_dir,
        provenance_path=provenance_path,
        network_fetch=False,
    )


def _validate_packaged_temporal_runtime(
    resource: OrchestrationPlatformResource,
    executable: Path,
    artifact: TemporalRuntimeArtifact,
    index: TemporalRuntimeIndex,
) -> None:
    """Validate a bundled executable against packaged provenance and the index."""
    if not resource.provenance:
        raise TemporalRuntimeProvisioningError(
            "Bundled Temporal executable is missing packaged provenance for "
            f"{resource.key!r}."
        )
    try:
        provenance = json.loads(
            read_orchestration_asset_text(resource.provenance)
        )
    except (FileNotFoundError, json.JSONDecodeError) as err:
        raise TemporalRuntimeProvisioningError(
            "Bundled Temporal executable provenance is unreadable: "
            f"{resource.provenance}"
        ) from err
    if not isinstance(provenance, dict):
        raise TemporalRuntimeProvisioningError(
            "Bundled Temporal executable provenance must be a JSON object: "
            f"{resource.provenance}"
        )

    release_asset = provenance.get("release_asset")
    executable_payload = provenance.get("executable")
    if not isinstance(release_asset, dict) or not isinstance(
        executable_payload, dict
    ):
        raise TemporalRuntimeProvisioningError(
            "Bundled Temporal executable provenance is missing release_asset "
            "or executable metadata."
        )
    mismatches: list[str] = []
    if provenance.get("schema_version") != 1:
        mismatches.append("schema_version")
    if provenance.get("component") != index.component:
        mismatches.append("component")
    if provenance.get("bundled") is not True:
        mismatches.append("bundled")
    if provenance.get("platform") != artifact.platform_key:
        mismatches.append("platform")
    if provenance.get("version") != index.version:
        mismatches.append("version")
    if release_asset.get("name") != artifact.archive_name:
        mismatches.append("release_asset.name")
    if release_asset.get("url") != artifact.archive_url:
        mismatches.append("release_asset.url")
    if (
        str(release_asset.get("sha256_expected", "")).lower()
        != artifact.archive_sha256
    ):
        mismatches.append("release_asset.sha256_expected")
    if (
        str(release_asset.get("sha256_actual", "")).lower()
        != artifact.archive_sha256
    ):
        mismatches.append("release_asset.sha256_actual")
    if release_asset.get("sha256_verified") is not True:
        mismatches.append("release_asset.sha256_verified")
    if executable_payload.get("resource_path") != resource.executable:
        mismatches.append("executable.resource_path")
    actual_executable_sha256 = _sha256_file(executable)
    if (
        str(executable_payload.get("sha256", "")).lower()
        != actual_executable_sha256
    ):
        mismatches.append("executable.sha256")
    if (
        int(executable_payload.get("size_bytes", -1) or -1)
        != executable.stat().st_size
    ):
        mismatches.append("executable.size_bytes")

    if mismatches:
        raise TemporalRuntimeProvisioningError(
            "Bundled Temporal executable provenance does not match the pinned "
            "runtime index or executable: " + ", ".join(mismatches)
        )


def _validate_cached_executable(
    entry_dir: Path,
    payload: Mapping[str, Any],
) -> tuple[bool, str]:
    executable_name = str(payload.get("executable_name", ""))
    executable = entry_dir / "bin" / executable_name
    expected_sha256 = str(payload.get("executable_sha256", "")).lower()
    if not executable_name:
        return False, "provenance is missing executable_name"
    if not executable.is_file():
        return False, f"cached executable is missing: {executable}"
    if os.name != "nt" and not os.access(executable, os.X_OK):
        return False, f"cached executable is not executable: {executable}"
    if not expected_sha256:
        return False, "provenance is missing executable_sha256"
    actual_sha256 = _sha256_file(executable)
    if actual_sha256.lower() != expected_sha256:
        return (
            False,
            "cached executable checksum mismatch: "
            f"expected {expected_sha256}, got {actual_sha256}",
        )
    return True, ""


def _provision_temporal_runtime(
    entry_dir: Path,
    artifact: TemporalRuntimeArtifact,
    index: TemporalRuntimeIndex,
    *,
    download_archive: DownloadArchive,
    download_timeout: float,
    lock_timeout: float,
) -> TemporalRuntimeResolution:
    entry_dir.parent.mkdir(parents=True, exist_ok=True)
    lock_dir = entry_dir.with_name(f"{entry_dir.name}.lock")
    with _DirectoryLock(lock_dir, timeout=lock_timeout):
        cached = _cache_resolution(entry_dir, artifact, index)
        if cached is not None:
            return cached

        if entry_dir.exists():
            shutil.rmtree(entry_dir)

        temporary_dir = entry_dir.with_name(
            f".{entry_dir.name}.{os.getpid()}.{time.monotonic_ns()}.tmp"
        )
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)
        try:
            archive_dir = temporary_dir / "archive"
            bin_dir = temporary_dir / "bin"
            archive_dir.mkdir(parents=True, exist_ok=True)
            bin_dir.mkdir(parents=True, exist_ok=True)
            archive_path = archive_dir / artifact.archive_name
            download_archive(artifact, archive_path, download_timeout)
            _verify_archive(archive_path, artifact)
            executable = _extract_temporal_executable(
                archive_path,
                artifact=artifact,
                destination_dir=bin_dir,
            )
            executable_sha256 = _sha256_file(executable)
            provenance_path = temporary_dir / "provenance.json"
            provenance_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "component": index.component,
                        "version": index.version,
                        "platform": artifact.platform_key,
                        "archive_name": artifact.archive_name,
                        "archive_url": artifact.archive_url,
                        "archive_sha256": artifact.archive_sha256,
                        "archive_size_bytes": artifact.archive_size_bytes,
                        "executable_name": artifact.executable_name,
                        "executable_sha256": executable_sha256,
                        "license": artifact.license,
                        "upstream_repository": artifact.upstream_repository,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            temporary_dir.rename(entry_dir)
        except Exception:
            shutil.rmtree(temporary_dir, ignore_errors=True)
            raise

    cached = _cache_resolution(entry_dir, artifact, index)
    if cached is None:
        raise TemporalRuntimeProvisioningError(
            "Temporal runtime cache entry was provisioned but could not be "
            f"validated: {entry_dir}"
        )
    return TemporalRuntimeResolution(
        executable=cached.executable,
        source="download",
        platform_key=cached.platform_key,
        version=cached.version,
        archive_sha256=cached.archive_sha256,
        cache_entry=cached.cache_entry,
        provenance_path=cached.provenance_path,
        network_fetch=True,
    )


class _DirectoryLock:
    """Small cross-platform directory lock for runtime cache population."""

    def __init__(self, path: Path, *, timeout: float) -> None:
        self.path = path
        self.timeout = timeout

    def __enter__(self) -> "_DirectoryLock":
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.path.mkdir()
                (self.path / "owner.json").write_text(
                    json.dumps(
                        {"pid": os.getpid(), "created_at": time.time()},
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return self
            except FileExistsError as err:
                if time.monotonic() >= deadline:
                    raise TemporalRuntimeProvisioningError(
                        "Timed out waiting for Temporal runtime cache lock: "
                        f"{self.path}"
                    ) from err
                time.sleep(DEFAULT_TEMPORAL_RUNTIME_LOCK_POLL_SECONDS)

    def __exit__(self, *_exc_info: object) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


def _download_temporal_archive(
    artifact: TemporalRuntimeArtifact,
    destination: Path,
    timeout: float,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(
            artifact.archive_url, timeout=timeout
        ) as response:
            with destination.open("wb") as output:
                shutil.copyfileobj(response, output)
    except OSError as err:
        raise TemporalRuntimeProvisioningError(
            f"Temporal runtime download failed from {artifact.archive_url}: {err}"
        ) from err


def _verify_archive(
    archive_path: Path,
    artifact: TemporalRuntimeArtifact,
) -> None:
    if not archive_path.is_file():
        raise TemporalRuntimeProvisioningError(
            f"Temporal runtime archive was not downloaded: {archive_path}"
        )
    actual_size = archive_path.stat().st_size
    if actual_size != artifact.archive_size_bytes:
        raise TemporalRuntimeChecksumError(
            f"Temporal runtime archive size mismatch for {archive_path}: "
            f"expected {artifact.archive_size_bytes}, got {actual_size}"
        )
    actual_sha256 = _sha256_file(archive_path)
    if actual_sha256.lower() != artifact.archive_sha256.lower():
        raise TemporalRuntimeChecksumError(
            f"Temporal runtime archive checksum mismatch for {archive_path}: "
            f"expected {artifact.archive_sha256}, got {actual_sha256}"
        )


def _extract_temporal_executable(
    archive_path: Path,
    *,
    artifact: TemporalRuntimeArtifact,
    destination_dir: Path,
) -> Path:
    destination = destination_dir / artifact.executable_name
    if artifact.archive_format == "zip" or archive_path.name.endswith(".zip"):
        _extract_zip_executable(
            archive_path,
            executable_name=artifact.executable_name,
            destination=destination,
        )
    elif artifact.archive_format in {
        "tar.gz",
        "tgz",
    } or archive_path.name.endswith(".tar.gz"):
        _extract_tar_executable(
            archive_path,
            executable_name=artifact.executable_name,
            destination=destination,
        )
    else:
        raise TemporalRuntimeProvisioningError(
            f"Unsupported Temporal runtime archive format: {archive_path}"
        )
    if not artifact.executable_name.endswith(".exe"):
        destination.chmod(destination.stat().st_mode | 0o755)
    return _validated_executable(destination, source="download")


def _extract_zip_executable(
    archive_path: Path,
    *,
    executable_name: str,
    destination: Path,
) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        matches = [
            name
            for name in archive.namelist()
            if PurePosixPath(name).name == executable_name
            and not name.endswith("/")
            and _safe_archive_member(name)
        ]
        if len(matches) != 1:
            raise TemporalRuntimeProvisioningError(
                f"expected exactly one {executable_name} in {archive_path}, "
                f"found {matches}"
            )
        with archive.open(matches[0], "r") as source:
            _copy_executable(source, destination)


def _extract_tar_executable(
    archive_path: Path,
    *,
    executable_name: str,
    destination: Path,
) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        matches = [
            member
            for member in archive.getmembers()
            if member.isfile()
            and PurePosixPath(member.name).name == executable_name
            and _safe_archive_member(member.name)
        ]
        if len(matches) != 1:
            names = [member.name for member in matches]
            raise TemporalRuntimeProvisioningError(
                f"expected exactly one {executable_name} in {archive_path}, "
                f"found {names}"
            )
        source = archive.extractfile(matches[0])
        if source is None:
            raise TemporalRuntimeProvisioningError(
                f"could not extract {matches[0].name} from {archive_path}"
            )
        with source:
            _copy_executable(source, destination)


def _safe_archive_member(name: str) -> bool:
    path = PurePosixPath(name)
    return not path.is_absolute() and ".." not in path.parts


def _copy_executable(source: IO[bytes], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as output:
        shutil.copyfileobj(source, output)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


RuntimeManifest = OrchestrationManifest
RuntimePlatformResource = OrchestrationPlatformResource
TemporalExecutableUnavailable = OrchestrationExecutableUnavailable
UnsupportedRuntimePlatform = UnsupportedOrchestrationPlatform
runtime_asset = orchestration_asset
read_runtime_asset_text = read_orchestration_asset_text
load_runtime_manifest = load_orchestration_manifest
runtime_platform_resource = orchestration_platform_resource
packaged_temporal_executable_path = orchestration_executable_path
