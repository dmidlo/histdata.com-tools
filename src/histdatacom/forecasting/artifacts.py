"""Content-addressed local persistence for replayable forecast artifacts."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .contracts import MAX_FORECAST_ARTIFACT_BYTES, ForecastSnapshotV1
from .scoring import ForecastScoreReportV1, ForecastScoreV1

ForecastArtifact = ForecastSnapshotV1 | ForecastScoreV1 | ForecastScoreReportV1


def write_forecast_artifact(
    artifact: ForecastArtifact, directory: str | Path
) -> Path:
    """Write once, or verify identical bytes; never overwrite an artifact."""
    payload = artifact.to_json().encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    kind = str(artifact.to_dict()["contract_type"])
    path = root / f"{kind}-{digest}.json"
    try:
        with path.open("xb") as stream:
            stream.write(payload)
    except FileExistsError:
        if (
            path.is_symlink()
            or path.stat().st_size != len(payload)
            or path.read_bytes() != payload
        ):
            raise ValueError(
                "existing forecast artifact differs from sealed bytes"
            )
    return path


def read_forecast_artifact(path: str | Path) -> ForecastArtifact:
    """Verify filename, byte bound, canonical bytes, nested identities and scores."""
    source = Path(path)
    if source.is_symlink():
        raise ValueError("forecast artifact cannot be a symlink")
    with source.open("rb") as stream:
        payload = stream.read(MAX_FORECAST_ARTIFACT_BYTES + 1)
    if len(payload) > MAX_FORECAST_ARTIFACT_BYTES:
        raise ValueError("forecast artifact exceeds byte bound")
    digest = hashlib.sha256(payload).hexdigest()
    readers = (
        ("forecast-snapshot", ForecastSnapshotV1.from_json),
        ("forecast-score", ForecastScoreV1.from_json),
        ("forecast-score-report", ForecastScoreReportV1.from_json),
    )
    for kind, reader in readers:
        if source.name == f"{kind}-{digest}.json":
            result = reader(payload.decode("utf-8"))
            if result.to_json().encode("utf-8") != payload:
                raise ValueError("forecast artifact is not canonical JSON")
            return result
    raise ValueError("forecast artifact filename/content digest mismatch")
