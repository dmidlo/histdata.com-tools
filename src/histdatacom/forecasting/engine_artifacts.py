"""Atomic no-clobber storage of complete executable registry envelopes."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .contracts import _load
from .engine_contracts import ForecastEngineRegistryV1, ForecastRegistryChangeV1
from .engine_runner import (
    ForecastEngineComparisonV1,
    ForecastEngineModelV1,
    ForecastEngineScoreV1,
    ForecastEngineSnapshotV1,
)
from .feature_artifacts import _read_regular, _sync_directory

ForecastEngineArtifact = (
    ForecastEngineRegistryV1
    | ForecastRegistryChangeV1
    | ForecastEngineModelV1
    | ForecastEngineSnapshotV1
    | ForecastEngineScoreV1
    | ForecastEngineComparisonV1
)


def _restore(text: str) -> ForecastEngineArtifact:
    data = _load(text)
    kind = data.get("contract_type")
    if kind == "forecast-engine-registry":
        return ForecastEngineRegistryV1.from_dict(data)
    if kind == "forecast-registry-change":
        return ForecastRegistryChangeV1.from_dict(data)
    if kind == "forecast-engine-model":
        return ForecastEngineModelV1.from_dict(data)
    if kind == "forecast-engine-snapshot":
        return ForecastEngineSnapshotV1.from_dict(data)
    if kind == "forecast-engine-score":
        return ForecastEngineScoreV1.from_dict(data)
    if kind == "forecast-engine-comparison":
        return ForecastEngineComparisonV1.from_dict(data)
    raise ValueError("not a complete supported engine artifact")


def write_engine_artifact(
    artifact: ForecastEngineArtifact, directory: str | Path
) -> Path:
    if not isinstance(
        artifact,
        (
            ForecastEngineRegistryV1,
            ForecastRegistryChangeV1,
            ForecastEngineModelV1,
            ForecastEngineSnapshotV1,
            ForecastEngineScoreV1,
            ForecastEngineComparisonV1,
        ),
    ):
        raise TypeError(
            "engine persistence refuses bare feature/calendar artifacts"
        )
    text = artifact.to_json()
    # Replay before publishing: a code change cannot publish old evidence under
    # today's executable identity merely because hashes remain self-consistent.
    _restore(text)
    payload = text.encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    target = root / f'{artifact.to_dict()["contract_type"]}-{digest}.json'
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".engine-artifact-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _read_regular(target) != payload:
                raise ValueError(
                    "existing engine artifact differs from sealed bytes"
                )
        else:
            _sync_directory(root)
    finally:
        if temporary is not None:
            temporary.unlink()
    return target


def read_engine_artifact(path: str | Path) -> ForecastEngineArtifact:
    source = Path(path)
    payload = _read_regular(source)
    digest = hashlib.sha256(payload).hexdigest()
    result = _restore(payload.decode("utf-8"))
    if (
        source.name != f'{result.to_dict()["contract_type"]}-{digest}.json'
        or result.to_json().encode("utf-8") != payload
    ):
        raise ValueError("engine artifact filename/canonical content mismatch")
    return result
