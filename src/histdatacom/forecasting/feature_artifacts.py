"""Content-addressed persistence of complete feature-aware replay artifacts."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from pathlib import Path

from .contracts import MAX_FORECAST_ARTIFACT_BYTES
from .feature_forecasts import ForecastFeatureScoreV1, ForecastFeatureSnapshotV1
from .feature_store import FeatureMatrixSnapshotV1

FeatureArtifact = (
    FeatureMatrixSnapshotV1 | ForecastFeatureSnapshotV1 | ForecastFeatureScoreV1
)


def _read_regular(path: Path) -> bytes:
    """Bounded, nonblocking, no-follow final-component reads with inode check."""
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("feature artifact must be a regular non-symlink file")
    if before.st_size > MAX_FORECAST_ARTIFACT_BYTES:
        raise ValueError("feature artifact exceeds byte bound")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or (
            before.st_dev,
            before.st_ino,
        ) != (opened.st_dev, opened.st_ino):
            raise ValueError(
                "feature artifact changed during regular-file open"
            )
        payload = stream.read(MAX_FORECAST_ARTIFACT_BYTES + 1)
    if len(payload) > MAX_FORECAST_ARTIFACT_BYTES:
        raise ValueError("feature artifact exceeds byte bound")
    return payload


def _sync_directory(root: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_feature_artifact(
    artifact: FeatureArtifact, directory: str | Path
) -> Path:
    """Write once or compare existing exact bytes; reject calendar projections."""
    if not isinstance(
        artifact,
        (
            FeatureMatrixSnapshotV1,
            ForecastFeatureSnapshotV1,
            ForecastFeatureScoreV1,
        ),
    ):
        raise TypeError(
            "feature persistence requires a complete feature artifact"
        )
    payload = artifact.to_json().encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{artifact.to_dict()["contract_type"]}-{digest}.json'
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".feature-artifact-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if _read_regular(path) != payload:
                raise ValueError(
                    "existing feature artifact differs from sealed bytes"
                )
        else:
            _sync_directory(root)
    finally:
        if temporary is not None:
            temporary.unlink()
    return path


def read_feature_artifact(path: str | Path) -> FeatureArtifact:
    """Validate complete envelope, content filename and every derived result."""
    source = Path(path)
    payload = _read_regular(source)
    digest = hashlib.sha256(payload).hexdigest()
    readers = (
        ("feature-matrix-snapshot", FeatureMatrixSnapshotV1.from_json),
        ("forecast-feature-snapshot", ForecastFeatureSnapshotV1.from_json),
        ("forecast-feature-score", ForecastFeatureScoreV1.from_json),
    )
    for kind, reader in readers:
        if source.name == f"{kind}-{digest}.json":
            result = reader(payload.decode("utf-8"))
            if result.to_json().encode("utf-8") != payload:
                raise ValueError("feature artifact is not canonical JSON")
            return result
    raise ValueError("feature artifact filename/content digest mismatch")
