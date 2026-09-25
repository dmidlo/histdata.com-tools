"""Canonical immutable security receipt persistence, separate from capture V1."""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
import stat
import tempfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

from .contracts import (
    MAX_SECURITY_BYTES,
    BrokerSecurityReceiptV1,
    BrokerSecurityReason as Reason,
    refuse,
)


def _read(path: Path) -> bytes:
    if path.is_symlink():
        refuse(Reason.INTEGRITY)
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SECURITY_BYTES:
            refuse(Reason.INTEGRITY)
        data = stream.read(MAX_SECURITY_BYTES + 1)
        if len(data) > MAX_SECURITY_BYTES:
            refuse(Reason.INTEGRITY)
        return data


def write_security_receipt(
    receipt: BrokerSecurityReceiptV1,
    path: Path,
    *,
    provider_request: BrokerSDKInvocationV1,
    manifest: BrokerLifecycleManifestV1,
    before_persist: Callable[[str], None] | None = None,
) -> None:
    """Create once without clobbering; caller owns the separate output path."""
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKSecurityV1
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyOperation
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.storage import (
        read_broker_policy_receipt,
        verify_broker_policy_receipt,
        write_broker_policy_receipt,
    )

    subject = BrokerSDKSecurityV1(provider_request, receipt, manifest)
    require_provider_operation(subject, BrokerPolicyOperation.RETAIN_LOCAL)
    temporary: Path | None = None
    try:
        payload = receipt.to_json().encode("ascii")
        BrokerSecurityReceiptV1.from_json(payload.decode("ascii"))
        if not path.parent.is_dir() or path.parent.is_symlink():
            refuse(Reason.INTEGRITY)
        # Preserve the existing native parent checks, then normalize host
        # aliases/relative parents consistently for both halves of the pair.
        # Resolving the final leaf would hide a forbidden evidence symlink.
        path = path.parent.resolve(strict=True) / path.name
        sidecar = path.with_name(path.name + ".provider-policy.json")
        if (
            path.exists()
            or path.is_symlink()
            or sidecar.exists()
            or sidecar.is_symlink()
        ):
            # Retry never rewrites the immutable admission's clock/context.
            # Both halves must already match; an orphan is not a completed
            # write and cannot be silently repaired or adopted here.
            if _read(path) != payload:
                refuse(Reason.INTEGRITY)
            if sidecar.lstat().st_size > MAX_SECURITY_BYTES - len(payload):
                refuse(Reason.INTEGRITY)
            retained = read_broker_policy_receipt(sidecar)
            retained_json = retained.to_json()
            if len(retained_json) + len(payload) > MAX_SECURITY_BYTES:
                refuse(Reason.INTEGRITY)
            verify_broker_policy_receipt(retained, subject, path)
            if before_persist is not None:
                before_persist(payload.decode("ascii"))
                before_persist(retained_json)
            require_provider_operation(
                subject, BrokerPolicyOperation.RETAIN_LOCAL
            )
            if os.name == "posix":
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            return
        write_broker_policy_receipt(
            sidecar,
            subject,
            native_artifact_name=path.name,
            native_file_bytes=payload,
            maximum_bytes=MAX_SECURITY_BYTES - len(payload),
            before_persist=before_persist,
        )
        if before_persist is not None:
            before_persist(payload.decode("ascii"))
        descriptor, name = tempfile.mkstemp(
            prefix=".broker-security-", dir=path.parent
        )
        temporary = Path(name)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            require_provider_operation(
                subject, BrokerPolicyOperation.RETAIN_LOCAL
            )
            if before_persist is not None:
                before_persist(payload.decode("ascii"))
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if _read(path) != payload:
                refuse(Reason.INTEGRITY)
        if os.name == "posix":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except BrokerPolicyError:
        raise
    except BaseException:
        refuse(Reason.INTEGRITY)
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def read_security_receipt(path: Path) -> BrokerSecurityReceiptV1:
    """Verify exact receipt bytes; verify_security_capture binds native evidence."""
    try:
        payload = _read(path)
        receipt = BrokerSecurityReceiptV1.from_json(payload.decode("ascii"))
        if receipt.to_json().encode("ascii") != payload:
            refuse(Reason.INTEGRITY)
        return receipt
    except BaseException:
        refuse(Reason.INTEGRITY)
