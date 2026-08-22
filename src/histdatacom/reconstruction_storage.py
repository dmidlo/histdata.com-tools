"""Fail-closed storage identity guards for reconstruction output roots.

The reconstruction data plane intentionally uses absolute durable paths.  A
detached mount can expose a same-named directory on the underlying filesystem,
so path existence alone is not evidence that a write still targets the
planned storage.  This module binds output and scratch roots to their planning
device and to an immutable marker that is verified before runtime writes.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue

RECONSTRUCTION_STORAGE_ROOT_GUARD_SCHEMA_VERSION = (
    "histdatacom.reconstruction-storage-root-guard.v1"
)
RECONSTRUCTION_STORAGE_ROOT_GUARD_ARTIFACT_KIND = (
    "reconstruction_storage_root_guard_v1"
)
RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND = (
    "reconstruction_plan_execution_manifest_v1"
)
RECONSTRUCTION_STORAGE_ROOT_GUARD_ROLE = "storage_root_guard"
RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER = (
    ".histdatacom-reconstruction-storage-root-guard.json"
)


class ReconstructionStorageRootError(ValueError):
    """Planned reconstruction storage is absent or changed identity."""


@dataclass(frozen=True, slots=True)
class ReconstructionStorageRootGuardV1:
    """Immutable device and marker binding for output and scratch roots."""

    output_root: str
    output_device_id: int
    scratch_root: str
    scratch_device_id: int
    guard_id: str = ""
    schema_version: str = RECONSTRUCTION_STORAGE_ROOT_GUARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_STORAGE_ROOT_GUARD_SCHEMA_VERSION
        ):
            raise ReconstructionStorageRootError(
                "unsupported reconstruction storage-root guard schema"
            )
        output = _resolved_root(self.output_root, "output_root")
        scratch = _resolved_root(self.scratch_root, "scratch_root")
        if (
            output == scratch
            or output.is_relative_to(scratch)
            or scratch.is_relative_to(output)
        ):
            raise ReconstructionStorageRootError(
                "guarded output and scratch roots overlap"
            )
        output_device = _device_id(self.output_device_id, "output_device_id")
        scratch_device = _device_id(self.scratch_device_id, "scratch_device_id")
        if output_device != scratch_device:
            raise ReconstructionStorageRootError(
                "guarded output and scratch roots are on different filesystems"
            )
        object.__setattr__(self, "output_root", str(output))
        object.__setattr__(self, "scratch_root", str(scratch))
        object.__setattr__(self, "output_device_id", output_device)
        object.__setattr__(self, "scratch_device_id", scratch_device)
        expected = _stable_id(
            "reconstruction-storage-root-guard", self.identity_payload()
        )
        if self.guard_id and self.guard_id != expected:
            raise ReconstructionStorageRootError(
                "reconstruction storage-root guard identity differs"
            )
        object.__setattr__(self, "guard_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the immutable storage identity payload."""
        return {
            "schema_version": self.schema_version,
            "output_root": self.output_root,
            "output_device_id": self.output_device_id,
            "scratch_root": self.scratch_root,
            "scratch_device_id": self.scratch_device_id,
            "marker_filename": RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the canonical JSON contract mapping."""
        return {**self.identity_payload(), "guard_id": self.guard_id}

    def to_json(self) -> str:
        """Serialize the canonical guard contract."""
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionStorageRootGuardV1:
        """Restore and validate a guard contract."""
        if data.get("marker_filename") != (
            RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER
        ):
            raise ReconstructionStorageRootError(
                "reconstruction storage-root marker filename differs"
            )
        return cls(
            output_root=str(data.get("output_root", "")),
            output_device_id=_strict_int(
                data.get("output_device_id"), "output_device_id"
            ),
            scratch_root=str(data.get("scratch_root", "")),
            scratch_device_id=_strict_int(
                data.get("scratch_device_id"), "scratch_device_id"
            ),
            guard_id=str(data.get("guard_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def create_reconstruction_storage_root_guard(
    *,
    output_root: str | Path,
    scratch_root: str | Path,
    artifact_root: str | Path,
) -> tuple[ReconstructionStorageRootGuardV1, ArtifactRef]:
    """Create roots, bind their device, and persist identical root markers."""
    output = _resolved_root(output_root, "output_root")
    scratch = _resolved_root(scratch_root, "scratch_root")
    for root in (output, scratch):
        root.mkdir(parents=True, exist_ok=True)
        _require_plain_directory(root)
    guard = ReconstructionStorageRootGuardV1(
        output_root=str(output),
        output_device_id=output.stat().st_dev,
        scratch_root=str(scratch),
        scratch_device_id=scratch.stat().st_dev,
    )
    encoded = guard.to_json().encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    artifacts = _resolved_root(artifact_root, "artifact_root")
    artifacts.mkdir(parents=True, exist_ok=True)
    _require_plain_directory(artifacts)
    artifact_path = (
        artifacts / f"reconstruction-storage-root-guard-{digest}.json"
    )
    _write_exact_file(artifact_path, encoded, replace_existing=False)
    ref = ArtifactRef(
        kind=RECONSTRUCTION_STORAGE_ROOT_GUARD_ARTIFACT_KIND,
        path=str(artifact_path),
        size_bytes=len(encoded),
        sha256=digest,
        metadata={
            "guard_id": guard.guard_id,
            "output_device_id": guard.output_device_id,
            "scratch_device_id": guard.scratch_device_id,
        },
    )
    for root in (output, scratch):
        if root.stat().st_dev != guard.output_device_id:
            raise ReconstructionStorageRootError(
                f"guarded storage changed before marker write: {root}"
            )
        _write_exact_file(
            root / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER,
            encoded,
            replace_existing=True,
        )
        if root.stat().st_dev != guard.output_device_id:
            raise ReconstructionStorageRootError(
                f"guarded storage changed during marker write: {root}"
            )
    verify_reconstruction_storage_root_guard(ref)
    return guard, ref


def verify_reconstruction_storage_root_guard(
    ref: ArtifactRef,
    *,
    expected_output_root: str | Path | None = None,
    expected_scratch_root: str | Path | None = None,
) -> ReconstructionStorageRootGuardV1:
    """Verify the strong guard artifact, roots, devices, and root markers."""
    if ref.kind != RECONSTRUCTION_STORAGE_ROOT_GUARD_ARTIFACT_KIND:
        raise ReconstructionStorageRootError(
            "reconstruction storage-root guard artifact kind differs"
        )
    encoded = _verified_ref_bytes(ref)
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ReconstructionStorageRootError(
            "reconstruction storage-root guard is not valid JSON"
        ) from err
    if not isinstance(payload, dict):
        raise ReconstructionStorageRootError(
            "reconstruction storage-root guard is not an object"
        )
    guard = ReconstructionStorageRootGuardV1.from_dict(payload)
    if ref.metadata.get("guard_id") != guard.guard_id:
        raise ReconstructionStorageRootError(
            "reconstruction storage-root guard metadata differs"
        )
    if expected_output_root is not None and _resolved_root(
        expected_output_root, "expected_output_root"
    ) != Path(guard.output_root):
        raise ReconstructionStorageRootError(
            "execution output root differs from its storage guard"
        )
    if expected_scratch_root is not None and _resolved_root(
        expected_scratch_root, "expected_scratch_root"
    ) != Path(guard.scratch_root):
        raise ReconstructionStorageRootError(
            "execution scratch root differs from its storage guard"
        )
    for role, root_text, expected_device in (
        ("output", guard.output_root, guard.output_device_id),
        ("scratch", guard.scratch_root, guard.scratch_device_id),
    ):
        root = Path(root_text)
        _require_plain_directory(root)
        if root.stat().st_dev != expected_device:
            raise ReconstructionStorageRootError(
                f"guarded {role} root changed filesystem identity: {root}"
            )
        marker = root / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER
        try:
            marker_status = marker.lstat()
            marker_bytes = marker.read_bytes()
        except OSError as err:
            raise ReconstructionStorageRootError(
                f"guarded {role} root marker is unavailable: {marker}"
            ) from err
        if not stat.S_ISREG(marker_status.st_mode) or marker.is_symlink():
            raise ReconstructionStorageRootError(
                f"guarded {role} root marker is not a plain file: {marker}"
            )
        if marker_status.st_dev != expected_device or marker_bytes != encoded:
            raise ReconstructionStorageRootError(
                f"guarded {role} root marker differs: {marker}"
            )
    return guard


def verify_reconstruction_storage_for_execution(
    execution_ref: ArtifactRef,
) -> tuple[ArtifactRef, ReconstructionStorageRootGuardV1] | None:
    """Verify storage for a first-party execution manifest, if applicable."""
    if (
        execution_ref.kind
        != RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND
    ):
        return None
    encoded = _verified_ref_bytes(execution_ref)
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ReconstructionStorageRootError(
            "reconstruction execution manifest is not valid JSON"
        ) from err
    if not isinstance(payload, dict):
        raise ReconstructionStorageRootError(
            "reconstruction execution manifest is not an object"
        )
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ReconstructionStorageRootError(
            "reconstruction execution artifact graph is absent"
        )
    guard_payload = artifacts.get(RECONSTRUCTION_STORAGE_ROOT_GUARD_ROLE)
    if not isinstance(guard_payload, dict):
        raise ReconstructionStorageRootError(
            "reconstruction execution storage-root guard is absent"
        )
    guard_ref = ArtifactRef.from_dict(guard_payload)
    guard = verify_reconstruction_storage_root_guard(
        guard_ref,
        expected_output_root=str(payload.get("output_root", "")),
        expected_scratch_root=str(payload.get("scratch_root", "")),
    )
    return guard_ref, guard


def require_guarded_storage_path(
    guard: ReconstructionStorageRootGuardV1,
    path: str | Path,
    *,
    role: str,
    allow_descendant: bool,
) -> Path:
    """Require a path to equal or remain below its guarded durable root."""
    if role == "output":
        root = Path(guard.output_root)
    elif role == "scratch":
        root = Path(guard.scratch_root)
    else:
        raise ReconstructionStorageRootError(
            f"unsupported guarded storage role: {role!r}"
        )
    target = Path(path).expanduser().resolve()
    valid = target == root or (allow_descendant and target.is_relative_to(root))
    if not valid:
        raise ReconstructionStorageRootError(
            f"{role} path is outside its guarded root: {target}"
        )
    return target


def _verified_ref_bytes(ref: ArtifactRef) -> bytes:
    path = Path(str(ref.path)).expanduser().resolve()
    if not ref.sha256 or ref.size_bytes is None:
        raise ReconstructionStorageRootError(
            "reconstruction storage evidence requires a strong artifact ref"
        )
    try:
        encoded = path.read_bytes()
    except OSError as err:
        raise ReconstructionStorageRootError(
            f"reconstruction storage evidence is unavailable: {path}"
        ) from err
    if len(encoded) != ref.size_bytes:
        raise ReconstructionStorageRootError(
            f"reconstruction storage evidence size differs: {path}"
        )
    if hashlib.sha256(encoded).hexdigest() != ref.sha256:
        raise ReconstructionStorageRootError(
            f"reconstruction storage evidence digest differs: {path}"
        )
    return encoded


def _write_exact_file(
    path: Path, payload: bytes, *, replace_existing: bool
) -> None:
    if path.exists() and not replace_existing:
        if path.read_bytes() != payload:
            raise ReconstructionStorageRootError(
                f"content-addressed storage artifact collision: {path}"
            )
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _require_plain_directory(path: Path) -> None:
    try:
        status = path.lstat()
    except OSError as err:
        raise ReconstructionStorageRootError(
            f"guarded storage root is unavailable: {path}"
        ) from err
    if not stat.S_ISDIR(status.st_mode) or path.is_symlink():
        raise ReconstructionStorageRootError(
            f"guarded storage root is not a plain directory: {path}"
        )


def _resolved_root(value: str | Path, name: str) -> Path:
    text = str(value).strip()
    if not text:
        raise ReconstructionStorageRootError(f"{name} is empty")
    return Path(text).expanduser().resolve()


def _device_id(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ReconstructionStorageRootError(f"{name} is negative")
    return result


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReconstructionStorageRootError(f"{name} is not an integer")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = _canonical_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _canonical_json(value: Mapping[str, JSONValue]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


__all__ = [
    "RECONSTRUCTION_STORAGE_ROOT_GUARD_ARTIFACT_KIND",
    "RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER",
    "RECONSTRUCTION_STORAGE_ROOT_GUARD_ROLE",
    "RECONSTRUCTION_STORAGE_ROOT_GUARD_SCHEMA_VERSION",
    "ReconstructionStorageRootError",
    "ReconstructionStorageRootGuardV1",
    "create_reconstruction_storage_root_guard",
    "require_guarded_storage_path",
    "verify_reconstruction_storage_for_execution",
    "verify_reconstruction_storage_root_guard",
]
