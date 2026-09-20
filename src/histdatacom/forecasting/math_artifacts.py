"""Atomic, no-clobber, bounded storage for executable math evidence."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path

from .feature_artifacts import _sync_directory
from .math_reports import (
    ForecastMathCheckedReportV1,
    ForecastPairedLossReportV1,
)
from .math_verification import (
    ForecastMathVerificationV1,
    MAX_MATH_ARTIFACT_BYTES,
    math_load,
)

ForecastMathArtifact = (
    ForecastMathVerificationV1
    | ForecastMathCheckedReportV1
    | ForecastPairedLossReportV1
)


def _restore(text: str) -> ForecastMathArtifact:
    data = math_load(text)
    kind = data.get("contract_type")
    if kind == "forecast-math-verification":
        return ForecastMathVerificationV1.from_dict(data)
    if kind == "forecast-math-checked-report":
        return ForecastMathCheckedReportV1.from_dict(data)
    if kind == "forecast-paired-loss-report":
        return ForecastPairedLossReportV1.from_dict(data)
    raise ValueError("not a complete supported forecast math artifact")


def _read(path: Path) -> bytes:
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_size > MAX_MATH_ARTIFACT_BYTES
    ):
        raise ValueError("math artifact must be a bounded regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        after = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino)
            or opened.st_size > MAX_MATH_ARTIFACT_BYTES
        ):
            raise ValueError("math artifact source changed or is not regular")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            payload = stream.read(MAX_MATH_ARTIFACT_BYTES + 1)
        if len(payload) > MAX_MATH_ARTIFACT_BYTES:
            raise ValueError("math artifact exceeds byte bounds")
        return payload
    finally:
        os.close(fd)


def write_math_artifact(
    artifact: ForecastMathArtifact, directory: str | Path
) -> Path:
    if type(artifact) not in (
        ForecastMathVerificationV1,
        ForecastMathCheckedReportV1,
        ForecastPairedLossReportV1,
    ):
        raise TypeError("math persistence requires complete typed evidence")
    text = artifact.to_json()
    _restore(text)
    payload = text.encode()
    digest = hashlib.sha256(payload).hexdigest()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    target = root / f'{artifact.to_dict()["contract_type"]}-{digest}.json'
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".forecast-math-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _read(target) != payload:
                raise ValueError(
                    "existing math artifact differs from sealed bytes"
                )
        else:
            _sync_directory(root)
    finally:
        if temporary is not None:
            temporary.unlink()
    return target


def read_math_artifact(path: str | Path) -> ForecastMathArtifact:
    source = Path(path)
    payload = _read(source)
    digest = hashlib.sha256(payload).hexdigest()
    result = _restore(payload.decode())
    if (
        source.name != f'{result.to_dict()["contract_type"]}-{digest}.json'
        or result.to_json().encode() != payload
    ):
        raise ValueError("math artifact canonical bytes/filename differ")
    return result
