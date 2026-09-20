"""Canonical immutable security receipt persistence, separate from capture V1."""

from __future__ import annotations

import os
from pathlib import Path
import stat
import tempfile

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
    receipt: BrokerSecurityReceiptV1, path: Path
) -> None:
    """Create once without clobbering; caller owns the separate output path."""
    temporary: Path | None = None
    try:
        payload = receipt.to_json().encode("ascii")
        BrokerSecurityReceiptV1.from_json(payload.decode("ascii"))
        if not path.parent.is_dir() or path.parent.is_symlink():
            refuse(Reason.INTEGRITY)
        descriptor, name = tempfile.mkstemp(
            prefix=".broker-security-", dir=path.parent
        )
        temporary = Path(name)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
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
