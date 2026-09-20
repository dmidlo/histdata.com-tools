"""Explicit coordinator workflow gate, not a security or source attestation.

There is no default approval. A coordinator records an external receipt only
after reviewing the final integrated package. Construction is structural;
``require_training_weight_approval`` freshly checks the exact policy, package
bytes and runtime before each controlled operation. This is not protection
against a hostile same-user Python process or an atomic filesystem snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from importlib import metadata
import os
from pathlib import Path
import platform
import re
import stat
import sys
from typing import ClassVar

from .training_contracts import TrainingContract, training_json
from .training_weight_contracts import (
    PREREGISTRATION_FILE_SHA256,
    _array,
    _object,
    _text,
    read_training_weight_preregistration,
)

TRAINING_WEIGHT_POLICY_COMMIT = "4e1eadf6862a192906d2c8f69efd356578252044"
TRAINING_WEIGHT_ALLOWED_OPERATIONS = tuple(
    sorted(
        (
            "source-window-decoding",
            "degraded-subset-materialization",
            "candidate-generation",
            "calibration-fit",
            "application-outcome-inspection",
            "policy-comparison",
        )
    )
)
MAX_APPROVAL_BYTES = 64 * 1024
MAX_FINGERPRINT_FILES = 2048
MAX_FINGERPRINT_ENTRIES = 8192
MAX_FINGERPRINT_FILE_BYTES = 16 * 1024 * 1024
MAX_FINGERPRINT_TOTAL_BYTES = 256 * 1024 * 1024
MAX_FINGERPRINT_PATH_CHARS = 512
_SUFFIXES = frozenset((".py", ".json", ".b64"))
_IGNORED_CACHES = frozenset(
    (
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
    )
)
_DEPENDENCIES = ("numpy", "polars", "pyarrow", "scipy")


@dataclass(frozen=True, slots=True)
class TrainingWeightExecutionApprovalV1(TrainingContract):
    """Coordinator declaration; no signature, source proof or default grant."""

    KIND: ClassVar[str] = "weight-execution-approval"
    protocol_commit: str
    protocol_file_sha256: str
    source_fingerprint: str
    allowed_operations: tuple[str, ...]
    coordinator_label: str
    approved_at_utc: str

    def _validate(self) -> None:
        if (
            self.protocol_commit != TRAINING_WEIGHT_POLICY_COMMIT
            or self.protocol_file_sha256 != PREREGISTRATION_FILE_SHA256
            or re.fullmatch(r"[0-9a-f]{64}", self.source_fingerprint) is None
        ):
            raise ValueError(
                "approval differs from frozen policy or fingerprint syntax"
            )
        if (
            not self.allowed_operations
            or self.allowed_operations
            != tuple(sorted(set(self.allowed_operations)))
            or not set(self.allowed_operations)
            <= set(TRAINING_WEIGHT_ALLOWED_OPERATIONS)
        ):
            raise ValueError(
                "approval operations must be exact sorted known names"
            )
        if (
            re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._ -]{0,127}", self.coordinator_label
            )
            is None
            or self.coordinator_label != self.coordinator_label.strip()
        ):
            raise ValueError(
                "approval coordinator label is not bounded canonical text"
            )
        if (
            re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
                self.approved_at_utc,
            )
            is None
        ):
            raise ValueError("approval time must be canonical UTC seconds")
        try:
            parsed = datetime.strptime(
                self.approved_at_utc, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError(
                "approval time is not a valid UTC instant"
            ) from None
        if (
            parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
            != self.approved_at_utc
        ):
            raise ValueError("approval time is not canonical")
        if len(self.to_json()) > MAX_APPROVAL_BYTES:
            raise ValueError("approval exceeds byte bound")


def _stamp(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _scan_package(root: Path) -> dict[str, tuple[int, int, int, int, int]]:
    if not stat.S_ISDIR(root.lstat().st_mode):
        raise ValueError("fingerprint root must be a real directory")
    pending = [root]
    files: dict[str, tuple[int, int, int, int, int]] = {}
    entries = 0
    total = 0
    while pending:
        directory = pending.pop()
        if not stat.S_ISDIR(directory.lstat().st_mode):
            raise ValueError(
                "fingerprint directory changed or became a symlink"
            )
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                if entries > MAX_FINGERPRINT_ENTRIES:
                    raise ValueError(
                        "fingerprint directory inventory exceeds bound"
                    )
                if child.name in _IGNORED_CACHES:
                    continue
                if child.is_symlink():
                    raise ValueError("fingerprint package contains a symlink")
                if child.is_dir(follow_symlinks=False):
                    pending.append(Path(child.path))
                    continue
                path = Path(child.path)
                if path.suffix not in _SUFFIXES:
                    continue
                relative = path.relative_to(root).as_posix()
                if len(relative) > MAX_FINGERPRINT_PATH_CHARS or any(
                    part in ("", ".", "..")
                    for part in path.relative_to(root).parts
                ):
                    raise ValueError("fingerprint relative path exceeds bounds")
                info = child.stat(follow_symlinks=False)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or not 0 <= info.st_size <= MAX_FINGERPRINT_FILE_BYTES
                ):
                    raise ValueError(
                        "fingerprint input is not a bounded regular file"
                    )
                total += info.st_size
                if (
                    len(files) >= MAX_FINGERPRINT_FILES
                    or total > MAX_FINGERPRINT_TOTAL_BYTES
                ):
                    raise ValueError(
                        "fingerprint aggregate input exceeds bounds"
                    )
                files[relative] = _stamp(info)
    if not files:
        raise ValueError("fingerprint package inventory is empty")
    return files


def _read_digest(path: Path, expected: tuple[int, int, int, int, int]) -> str:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    count = 0
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or _stamp(opened) != expected:
            raise ValueError("fingerprint input changed before open")
        while True:
            block = stream.read(min(1_048_576, expected[2] - count + 1))
            if not block:
                break
            count += len(block)
            if count > expected[2]:
                raise ValueError("fingerprint input grew beyond declared bound")
            digest.update(block)
        if (
            count != expected[2]
            or _stamp(os.fstat(stream.fileno())) != expected
        ):
            raise ValueError("fingerprint input changed during read")
    if _stamp(path.lstat()) != expected or path.is_symlink():
        raise ValueError("fingerprint input changed after read")
    return digest.hexdigest()


def _runtime_identity() -> dict[str, object]:
    dependencies: dict[str, object] = {}
    for name in _DEPENDENCIES:
        try:
            version = metadata.version(name)
        except metadata.PackageNotFoundError:
            dependencies[name] = None
        else:
            if type(version) is not str or not 1 <= len(version) <= 128:
                raise ValueError("dependency version exceeds identity bounds")
            dependencies[name] = version
    return {
        "implementation": sys.implementation.name,
        "implementation_version": list(sys.implementation.version),
        "python_version": sys.version,
        "platform": sys.platform,
        "os_name": os.name,
        "machine": platform.machine(),
        "system": platform.system(),
        "release": platform.release(),
        "platform_version": platform.version(),
        "dependencies": dependencies,
    }


def _package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def training_weight_source_fingerprint() -> str:
    """Fresh whole-package code/assets plus runtime identity, no cached trust.

    Relative names make an unchanged package relocatable. All package .py,
    .json and .b64 files are included; known interpreter/tool cache directories
    are excluded. Reads are bounded and no-follow, and an inventory recheck
    detects ordinary concurrent additions/removals/replacements. This does not
    authenticate dependency code or defend against hostile filesystem races.
    """
    root = _package_root()
    before = _scan_package(root)
    runtime = _runtime_identity()
    inventory = [
        {
            "path": name,
            "size_bytes": stamp[2],
            "sha256": _read_digest(root / name, stamp),
        }
        for name, stamp in sorted(before.items())
    ]
    if _scan_package(root) != before or _runtime_identity() != runtime:
        raise ValueError("package or runtime changed during fingerprinting")
    payload = {
        "schema_version": "histdatacom.training-weight-source-fingerprint.v1",
        "files": inventory,
        "runtime": runtime,
    }
    return hashlib.sha256(training_json(payload).encode("ascii")).hexdigest()


def read_training_weight_execution_approval(
    path: str | Path,
) -> TrainingWeightExecutionApprovalV1:
    """Read an explicitly supplied external, exact canonical approval file."""
    target = Path(path)
    info = target.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or not 0 < info.st_size <= MAX_APPROVAL_BYTES
    ):
        raise ValueError("approval is not a bounded regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(target, flags)
    with os.fdopen(descriptor, "rb") as stream:
        if _stamp(os.fstat(stream.fileno())) != _stamp(info):
            raise ValueError("approval changed before read")
        data = stream.read(MAX_APPROVAL_BYTES + 1)
        if _stamp(os.fstat(stream.fileno())) != _stamp(info):
            raise ValueError("approval changed during read")
    if _stamp(target.lstat()) != _stamp(info) or target.is_symlink():
        raise ValueError("approval changed after read")
    if len(data) != info.st_size:
        raise ValueError("approval exceeds exact byte count")
    result: TrainingWeightExecutionApprovalV1 = (
        TrainingWeightExecutionApprovalV1.from_json(data.decode("ascii"))
    )
    if data != result.to_json().encode("ascii"):
        raise ValueError("approval file is not exact canonical JSON")
    return result


def require_training_weight_approval(
    receipt: TrainingWeightExecutionApprovalV1 | None, operation: str
) -> None:
    """Fail closed before a controlled operation; never changes policy state."""
    if type(receipt) is not TrainingWeightExecutionApprovalV1:
        raise ValueError("explicit coordinator approval is required")
    if (
        type(operation) is not str
        or operation not in TRAINING_WEIGHT_ALLOWED_OPERATIONS
    ):
        raise ValueError("unsupported controlled weighting operation")
    TrainingWeightExecutionApprovalV1.from_json(receipt.to_json())
    policy = read_training_weight_preregistration()
    gate = _object(policy.to_dict()["execution_gate"])
    operations = tuple(sorted(_text(item) for item in _array(gate["before"])))
    if (
        policy.execution_approved
        or operations != TRAINING_WEIGHT_ALLOWED_OPERATIONS
    ):
        raise ValueError(
            "approval operation catalog differs from frozen policy"
        )
    if operation not in receipt.allowed_operations:
        raise ValueError("operation is outside explicit coordinator approval")
    if training_weight_source_fingerprint() != receipt.source_fingerprint:
        raise ValueError("approval package or runtime fingerprint is stale")
