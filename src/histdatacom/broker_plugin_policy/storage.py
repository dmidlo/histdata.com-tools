"""Bounded policy review inputs and native-byte-bound admission sidecars.

Receipt readers report historical declared decisions, never current permission.
A sidecar does not prove that its native file was successfully published; verify
both. A native file and its sidecar are separate atomic writes, not a two-file
transaction. Readers must refuse a missing or mismatched pair.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from ._wire import MAX_BYTES, Artifact
from .contracts import (
    BrokerPolicyContextV1,
    BrokerPolicyDecisionV1,
    BrokerPolicyOperation,
    BrokerPolicyReferenceV1,
)
from .scope import require_provider_operation


def _path(value: Path) -> Path:
    if type(value) is not Path and not isinstance(value, Path):
        raise ValueError("provider policy storage requires an explicit Path")
    if not value.is_absolute() or value.name in ("", ".", ".."):
        raise ValueError(
            "provider policy storage requires an absolute file path"
        )
    if value.parent.resolve(strict=True) != value.parent:
        raise ValueError("provider policy storage refuses parent aliases")
    return value


def _read(path: Path) -> bytes:
    path = _path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or not 0 < before.st_size <= MAX_BYTES
        ):
            raise ValueError(
                "provider policy input must be a bounded regular file"
            )
        chunks: list[bytes] = []
        size = 0
        while size <= MAX_BYTES:
            block = os.read(descriptor, min(65536, MAX_BYTES + 1 - size))
            if not block:
                break
            chunks.append(block)
            size += len(block)
        after = os.fstat(descriptor)
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (
            size != before.st_size
            or size > MAX_BYTES
            or any(getattr(before, f) != getattr(after, f) for f in fields)
        ):
            raise ValueError("provider policy input changed while being read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _sync(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class BrokerPolicyFileSourceV1:
    """Reread one operator-owned canonical review file on every host guard.

    The source is not a signer or legal authority. Operator-controlled atomic
    replacement can publish a newer complete ledger; an active scope detects
    inventory rollback and selection changes. Path aliases are not followed.
    """

    path: Path

    def read_policy_context(self) -> BrokerPolicyContextV1:
        data = _read(self.path)
        context = BrokerPolicyContextV1.from_json(data.decode("ascii"))
        if context.to_json().encode("ascii") != data:
            raise ValueError(
                "policy review file must retain exact canonical bytes"
            )
        return context


def write_broker_policy_context(
    path: Path, context: BrokerPolicyContextV1
) -> None:
    """Publish an immutable review snapshot, not an execution approval shortcut."""
    path = _path(path)
    if type(context) is not BrokerPolicyContextV1:
        raise ValueError("exact provider review context required")
    context = BrokerPolicyContextV1.from_json(context.to_json())
    encoded = context.to_json().encode("ascii")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".provider-review-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if _read(path) != encoded:
                raise ValueError(
                    "provider review snapshot already exists with different bytes"
                ) from None
        _sync(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class BrokerPolicyArtifactReceiptV1(Artifact):
    """Exact native-file identity and as-of declared operation/retention checks."""

    KIND: ClassVar[str] = "artifact-receipt"
    native_artifact_name: str
    native_file: BrokerPolicyReferenceV1
    operation_admission: BrokerPolicyDecisionV1
    retention_admission: BrokerPolicyDecisionV1

    def _validate(self) -> None:
        name = self.native_artifact_name
        if (
            not name
            or name in (".", "..")
            or "/" in name
            or "\\" in name
            or len(name) > 255
            or name != name.strip()
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in name
            )
        ):
            raise ValueError(
                "native artifact receipt requires a bounded basename"
            )
        operation = self.operation_admission
        retention = self.retention_admission
        if not operation.allowed or not retention.allowed:
            raise ValueError(
                "artifact receipt cannot claim a refused operation"
            )
        if (
            retention.request.operation
            is not BrokerPolicyOperation.RETAIN_LOCAL
        ):
            raise ValueError(
                "durable receipt requires explicit local retention admission"
            )
        if operation.request.subject != retention.request.subject:
            raise ValueError(
                "receipt operation and retention native subjects differ"
            )
        if (
            self.native_file.native_id
            != operation.request.subject.native_ref.native_id
        ):
            raise ValueError("native file is not bound to the admitted subject")
        if self.native_file.kind != "native-provider-artifact-file-v1":
            raise ValueError("unsupported native file reference kind")


def _file_reference(
    native_subject: object, data: bytes
) -> BrokerPolicyReferenceV1:
    from .bindings import _resolve_native

    if type(data) is not bytes or not 0 < len(data) <= MAX_BYTES:
        raise ValueError(
            "native provider file exceeds the bounded sidecar contract"
        )
    # Match bytes and identity from one detached native snapshot. This avoids
    # repeating the same bounded native replay within this single check; it
    # does not cache a decision or a snapshot across effects.
    snapshot = _resolve_native(native_subject)
    if snapshot.artifact_json is None:
        raise ValueError("provider invocation has no storable native artifact")
    canonical = snapshot.artifact_json.encode("utf-8")
    if data not in (canonical, canonical + b"\n"):
        raise ValueError(
            "native file bytes differ from the closed native subject"
        )
    subject = snapshot.subject
    return BrokerPolicyReferenceV1(
        "native-provider-artifact-file-v1",
        subject.native_ref.native_id,
        hashlib.sha256(data).hexdigest(),
        len(data),
    )


def read_broker_policy_receipt(path: Path) -> BrokerPolicyArtifactReceiptV1:
    """Read a bounded historical declaration; do not grant a new operation."""
    return BrokerPolicyArtifactReceiptV1.from_json(_read(path).decode("ascii"))


def verify_broker_policy_receipt(
    receipt: BrokerPolicyArtifactReceiptV1,
    native_subject: object,
    native_path: Path,
) -> None:
    """Match actual native bytes and classification, without authorizing reuse."""
    from .bindings import resolve_provider_subject

    if type(receipt) is not BrokerPolicyArtifactReceiptV1:
        raise ValueError("exact provider policy receipt required")
    receipt = BrokerPolicyArtifactReceiptV1.from_json(receipt.to_json())
    native_path = _path(native_path)
    if native_path.name != receipt.native_artifact_name:
        raise ValueError("provider policy receipt native file name differs")
    if (
        _file_reference(native_subject, _read(native_path))
        != receipt.native_file
    ):
        raise ValueError("provider policy receipt native file identity differs")
    if (
        resolve_provider_subject(native_subject)
        != receipt.operation_admission.request.subject
    ):
        raise ValueError(
            "provider policy receipt native classification differs"
        )


def write_broker_policy_receipt(
    path: Path,
    native_subject: object,
    *,
    native_artifact_name: str,
    native_file_bytes: bytes,
    operation: BrokerPolicyOperation = BrokerPolicyOperation.RETAIN_LOCAL,
    recipient_scope: str | None = None,
    replace_existing: bool = False,
    maximum_bytes: int = MAX_BYTES,
    before_persist: Callable[[str], None] | None = None,
) -> BrokerPolicyArtifactReceiptV1:
    """Persist exact declared admissions beside an unchanged native artifact.

    The caller publishes the matched native file separately. A successful
    sidecar write alone is never capture completion. Updating an owned mutable
    manifest sidecar requires explicit replacement and a valid prior receipt
    for the same native basename; unrelated files are never overwritten.
    A trusted host may add a refusal-only private-input check over the complete
    canonical receipt. It is repeated before promotion and grants no rights.
    """
    operation_admission = require_provider_operation(
        native_subject, operation, recipient_scope=recipient_scope
    )
    retention = require_provider_operation(
        native_subject, BrokerPolicyOperation.RETAIN_LOCAL
    )
    native_file = _file_reference(native_subject, native_file_bytes)
    receipt = BrokerPolicyArtifactReceiptV1(
        native_artifact_name, native_file, operation_admission, retention
    )
    path = _path(path)
    if not path.name.endswith(".provider-policy.json"):
        raise ValueError(
            "provider policy sidecar requires its dedicated file suffix"
        )
    if type(replace_existing) is not bool:
        raise ValueError("sidecar replacement must be explicitly boolean")
    if before_persist is not None and not callable(before_persist):
        raise ValueError("sidecar private-input check must be callable")
    previous: bytes | None = None
    if path.exists() or path.is_symlink():
        previous = _read(path)
        old = BrokerPolicyArtifactReceiptV1.from_json(previous.decode("ascii"))
        if (
            not replace_existing
            or old.native_artifact_name != native_artifact_name
        ):
            raise ValueError(
                "provider policy sidecar already exists or names a different artifact"
            )
    encoded = receipt.to_json().encode("ascii")
    if type(maximum_bytes) is not int or not 0 <= maximum_bytes <= MAX_BYTES:
        raise ValueError(
            "provider policy sidecar requires an explicit bounded byte budget"
        )
    if len(encoded) > maximum_bytes:
        raise ValueError(
            "provider policy sidecar exceeds the remaining native storage budget"
        )

    def check_current() -> None:
        current = require_provider_operation(
            native_subject, operation, recipient_scope=recipient_scope
        )
        retained = require_provider_operation(
            native_subject, BrokerPolicyOperation.RETAIN_LOCAL
        )
        if (
            current.request.subject
            != receipt.operation_admission.request.subject
            or retained.request.subject
            != receipt.retention_admission.request.subject
        ):
            raise ValueError(
                "native provider subject changed during receipt publication"
            )
        if _file_reference(native_subject, native_file_bytes) != native_file:
            raise ValueError(
                "native provider file changed during receipt publication"
            )
        if before_persist is not None:
            try:
                before_persist(encoded.decode("ascii"))
            except Exception:  # noqa: BLE001 - suppress private detector text
                # A private-input detector may put its matched secret in an
                # exception. Never include that diagnostic in host failures.
                raise ValueError(
                    "provider policy receipt refused by private-input check"
                ) from None

    check_current()
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".provider-admission-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        check_current()
        if previous is not None:
            if _read(path) != previous:
                raise ValueError(
                    "provider policy sidecar changed before publication"
                )
            os.replace(temporary, path)
        else:
            os.link(temporary, path, follow_symlinks=False)
        _sync(path.parent)
        return receipt
    finally:
        temporary.unlink(missing_ok=True)
