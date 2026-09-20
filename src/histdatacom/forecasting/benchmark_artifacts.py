"""Bounded atomic publication with mandatory benchmark re-execution."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .benchmark_contracts import ForecastBenchmarkSuiteV1
from .benchmark_runner import ForecastBenchmarkRunV1
from .feature_artifacts import _sync_directory
from .math_artifacts import _read
from .math_verification import math_load

ForecastBenchmarkArtifact = ForecastBenchmarkSuiteV1 | ForecastBenchmarkRunV1


def _restore(text: str) -> ForecastBenchmarkArtifact:
    data = math_load(text)
    if data.get("contract_type") == "forecast-benchmark-suite":
        return ForecastBenchmarkSuiteV1.from_dict(data)
    if data.get("contract_type") == "forecast-benchmark-run":
        return ForecastBenchmarkRunV1.from_dict(data)
    raise ValueError("not a complete benchmark artifact")


def write_benchmark_artifact(
    artifact: ForecastBenchmarkArtifact, directory: str | Path
) -> Path:
    if type(artifact) not in (ForecastBenchmarkSuiteV1, ForecastBenchmarkRunV1):
        raise TypeError(
            "benchmark writer requires a full suite or executed run"
        )
    text = artifact.to_json()
    _restore(text)
    payload = text.encode("utf-8")
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    target = (
        root
        / f'{artifact.to_dict()["contract_type"]}-{hashlib.sha256(payload).hexdigest()}.json'
    )
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".benchmark-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if _read(target) != payload:
                raise ValueError("existing benchmark artifact differs")
        else:
            _sync_directory(root)
    finally:
        if temporary is not None:
            temporary.unlink()
    return target


def read_benchmark_artifact(path: str | Path) -> ForecastBenchmarkArtifact:
    source = Path(path)
    payload = _read(source)
    # Reject a false filename before expensive re-execution.
    kind = math_load(payload.decode("utf-8")).get("contract_type")
    if source.name != f"{kind}-{hashlib.sha256(payload).hexdigest()}.json":
        raise ValueError("benchmark filename/content digest differs")
    result = _restore(payload.decode("utf-8"))
    if result.to_json().encode("utf-8") != payload:
        raise ValueError("benchmark artifact is not canonical")
    return result
