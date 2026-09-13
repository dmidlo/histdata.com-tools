"""Canonical, lineage-bound source projections for benchmark campaigns.

The reverse-degradation benchmark consumes only the canonical quote columns
from monthly Arrow caches.  This module projects those columns without
reordering or coercing rows and seals both parent and projected identities in
a row-free manifest.  Projection files are named by their physical SHA-256;
the separate values digest binds the exact primitive values independent of
Arrow record-batch boundaries.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from histdatacom.histdata_ascii import (
    MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS,
    MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

BENCHMARK_SOURCE_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.benchmark-source-projection.v1"
)
BENCHMARK_SOURCE_PROJECTION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.benchmark-source-projection-manifest.v1"
)
CANONICAL_BENCHMARK_PROJECTION_COLUMNS = (
    "datetime",
    "bid",
    "ask",
    "vol",
)
MAX_PROJECTION_PARTITIONS = 96
MAX_PARENT_BYTES = 16 * 1024**3
MAX_PROJECTION_BYTES = 4 * 1024**3
MAX_MANIFEST_BYTES = 1024 * 1024
_BATCH_SIZE = 131_072
_PERIOD = re.compile(r"^[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PROJECTION_NAME = re.compile(r"^source-projection-([0-9a-f]{64})\.arrow$")


@dataclass(frozen=True, slots=True)
class BenchmarkSourceProjectionV1:
    """One canonical Arrow projection and its immutable parent lineage."""

    symbol: str
    period: str
    parent_relative_path: str
    parent_size_bytes: int
    parent_row_count: int
    parent_sha256: str
    projected_relative_path: str
    projected_size_bytes: int
    projected_row_count: int
    projected_sha256: str
    values_sha256: str
    first_timestamp_ms: int
    last_timestamp_ms: int
    timestamp_regression_count: int
    maximum_timestamp_regression_ms: int
    projection_id: str = ""
    schema_version: str = BENCHMARK_SOURCE_PROJECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BENCHMARK_SOURCE_PROJECTION_SCHEMA_VERSION:
            raise ValueError("benchmark source projection schema differs")
        symbol = _symbol(self.symbol)
        object.__setattr__(self, "symbol", symbol)
        if not _PERIOD.fullmatch(self.period):
            raise ValueError("projection period must use YYYYMM")
        parent_relative = _relative_path(
            self.parent_relative_path, "parent projection"
        )
        projected_relative = _relative_path(
            self.projected_relative_path, "projected source"
        )
        object.__setattr__(self, "parent_relative_path", parent_relative)
        object.__setattr__(self, "projected_relative_path", projected_relative)
        _bounded_int(self.parent_size_bytes, "parent size", 1, MAX_PARENT_BYTES)
        _bounded_int(
            self.projected_size_bytes,
            "projected size",
            1,
            MAX_PROJECTION_BYTES,
        )
        _bounded_int(self.parent_row_count, "parent rows", 2, 2**63 - 1)
        _bounded_int(self.projected_row_count, "projected rows", 2, 2**63 - 1)
        if self.parent_row_count != self.projected_row_count:
            raise ValueError("projection row count differs from parent")
        for name in ("parent_sha256", "projected_sha256", "values_sha256"):
            _sha256(str(getattr(self, name)), name)
        projected_name = Path(projected_relative).name
        match = _PROJECTION_NAME.fullmatch(projected_name)
        if match is None or match.group(1) != self.projected_sha256:
            raise ValueError("projection path is not content addressed")
        _bounded_int(
            self.first_timestamp_ms,
            "first timestamp",
            -(2**63),
            2**63 - 1,
        )
        _bounded_int(
            self.last_timestamp_ms,
            "last timestamp",
            -(2**63),
            2**63 - 1,
        )
        _bounded_int(
            self.timestamp_regression_count,
            "timestamp regression count",
            0,
            MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION,
        )
        _bounded_int(
            self.maximum_timestamp_regression_ms,
            "maximum timestamp regression",
            0,
            MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS,
        )
        expected = _stable_id("benchmark-source-projection", self.payload())
        if self.projection_id and self.projection_id != expected:
            raise ValueError("benchmark source projection identity differs")
        object.__setattr__(self, "projection_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "period": self.period,
            "parent_relative_path": self.parent_relative_path,
            "parent_size_bytes": self.parent_size_bytes,
            "parent_row_count": self.parent_row_count,
            "parent_sha256": self.parent_sha256,
            "projected_relative_path": self.projected_relative_path,
            "projected_size_bytes": self.projected_size_bytes,
            "projected_row_count": self.projected_row_count,
            "projected_sha256": self.projected_sha256,
            "values_sha256": self.values_sha256,
            "first_timestamp_ms": self.first_timestamp_ms,
            "last_timestamp_ms": self.last_timestamp_ms,
            "timestamp_regression_count": self.timestamp_regression_count,
            "maximum_timestamp_regression_ms": (
                self.maximum_timestamp_regression_ms
            ),
            "source_order_preserved": True,
            "numeric_values_preserved": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "projection_id": self.projection_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BenchmarkSourceProjectionV1":
        if data.get("source_order_preserved") is not True:
            raise ValueError("projection does not preserve source order")
        if data.get("numeric_values_preserved") is not True:
            raise ValueError("projection does not preserve numeric values")
        return cls(
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            parent_relative_path=str(data.get("parent_relative_path", "")),
            parent_size_bytes=_strict_int(data.get("parent_size_bytes")),
            parent_row_count=_strict_int(data.get("parent_row_count")),
            parent_sha256=str(data.get("parent_sha256", "")),
            projected_relative_path=str(
                data.get("projected_relative_path", "")
            ),
            projected_size_bytes=_strict_int(data.get("projected_size_bytes")),
            projected_row_count=_strict_int(data.get("projected_row_count")),
            projected_sha256=str(data.get("projected_sha256", "")),
            values_sha256=str(data.get("values_sha256", "")),
            first_timestamp_ms=_strict_int(data.get("first_timestamp_ms")),
            last_timestamp_ms=_strict_int(data.get("last_timestamp_ms")),
            timestamp_regression_count=_strict_int(
                data.get("timestamp_regression_count")
            ),
            maximum_timestamp_regression_ms=_strict_int(
                data.get("maximum_timestamp_regression_ms")
            ),
            projection_id=str(data.get("projection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkSourceProjectionManifestV1:
    """Row-free, content-addressed set of canonical source projections."""

    projections: tuple[BenchmarkSourceProjectionV1, ...]
    frozen_at_utc: str
    columns: tuple[str, ...] = CANONICAL_BENCHMARK_PROJECTION_COLUMNS
    manifest_id: str = ""
    schema_version: str = BENCHMARK_SOURCE_PROJECTION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BENCHMARK_SOURCE_PROJECTION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "benchmark source projection manifest schema differs"
            )
        if tuple(self.columns) != CANONICAL_BENCHMARK_PROJECTION_COLUMNS:
            raise ValueError("benchmark source projection columns differ")
        projections = tuple(
            sorted(
                self.projections, key=lambda item: (item.period, item.symbol)
            )
        )
        if not projections or len(projections) > MAX_PROJECTION_PARTITIONS:
            raise ValueError("benchmark source projection count is invalid")
        if len({(item.period, item.symbol) for item in projections}) != len(
            projections
        ):
            raise ValueError("benchmark source projection axis is duplicated")
        if len({item.projected_relative_path for item in projections}) != len(
            projections
        ):
            raise ValueError("benchmark source projection path is duplicated")
        object.__setattr__(self, "projections", projections)
        _utc_timestamp(self.frozen_at_utc)
        expected = _stable_id(
            "benchmark-source-projection-manifest", self.payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError(
                "benchmark source projection manifest identity differs"
            )
        object.__setattr__(self, "manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "columns": list(self.columns),
            "projections": [item.to_dict() for item in self.projections],
            "frozen_at_utc": self.frozen_at_utc,
            "event_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BenchmarkSourceProjectionManifestV1":
        if data.get("event_rows_embedded") is not False:
            raise ValueError("benchmark source projection manifest embeds rows")
        columns = data.get("columns")
        projections = data.get("projections")
        if not isinstance(columns, Sequence) or isinstance(
            columns, (str, bytes)
        ):
            raise ValueError("benchmark source projection columns are invalid")
        if not isinstance(projections, Sequence) or isinstance(
            projections, (str, bytes)
        ):
            raise ValueError("benchmark source projections are invalid")
        return cls(
            projections=tuple(
                BenchmarkSourceProjectionV1.from_dict(_mapping(item))
                for item in projections
            ),
            frozen_at_utc=str(data.get("frozen_at_utc", "")),
            columns=tuple(str(item) for item in columns),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BenchmarkSourceProjectionManifestV1":
        if len(text.encode("utf-8")) > MAX_MANIFEST_BYTES:
            raise ValueError(
                "benchmark source projection manifest exceeds bound"
            )
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "benchmark source projection manifest is invalid"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class _ProjectionScan:
    row_count: int
    first_timestamp_ms: int
    last_timestamp_ms: int
    timestamp_regression_count: int
    maximum_timestamp_regression_ms: int
    values_sha256: str


def build_benchmark_source_projection_manifest(
    parent_root: str | Path,
    projection_root: str | Path,
    *,
    expected_parent_sha256: Mapping[tuple[str, str], str],
    frozen_at_utc: str,
) -> BenchmarkSourceProjectionManifestV1:
    """Create and verify canonical projections for explicitly pinned parents."""
    source_root = Path(parent_root).expanduser().resolve()
    target_root = Path(projection_root).expanduser().resolve()
    if not source_root.is_dir():
        raise ValueError("benchmark projection parent root is not a directory")
    if not expected_parent_sha256:
        raise ValueError("benchmark projection requires pinned parent hashes")
    if len(expected_parent_sha256) > MAX_PROJECTION_PARTITIONS:
        raise ValueError("benchmark projection partition count exceeds bound")
    _utc_timestamp(frozen_at_utc)
    pinned: dict[tuple[str, str], str] = {}
    for raw_axis, expected_sha256 in expected_parent_sha256.items():
        if not isinstance(raw_axis, tuple) or len(raw_axis) != 2:
            raise TypeError(
                "benchmark projection axis must be (symbol, period)"
            )
        axis = (_symbol(str(raw_axis[0])), str(raw_axis[1]))
        if not _PERIOD.fullmatch(axis[1]):
            raise ValueError("projection period must use YYYYMM")
        _sha256(expected_sha256, "expected parent sha256")
        if axis in pinned:
            raise ValueError("benchmark projection axis is duplicated")
        pinned[axis] = expected_sha256
    target_root.mkdir(parents=True, exist_ok=True)
    projections: list[BenchmarkSourceProjectionV1] = []
    for (symbol, period), expected_sha256 in sorted(pinned.items()):
        parent_relative = (
            Path(symbol.lower())
            / str(int(period[:4]))
            / str(int(period[4:]))
            / ".data"
        )
        parent_path = source_root / parent_relative
        if not parent_path.is_file():
            raise ValueError(f"projection parent is missing: {parent_relative}")
        parent_identity = _file_identity(parent_path)
        parent_size = parent_identity[0]
        _bounded_int(parent_size, "parent size", 1, MAX_PARENT_BYTES)
        parent_sha256 = _file_sha256(parent_path)
        if _file_identity(parent_path) != parent_identity:
            raise ValueError("projection parent changed while hashing")
        if parent_sha256 != expected_sha256:
            raise ValueError(
                f"projection parent hash differs: {parent_relative.as_posix()}"
            )

        output_directory = (
            target_root
            / symbol.lower()
            / str(int(period[:4]))
            / str(int(period[4:]))
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        temporary = output_directory / f".source-projection.{os.getpid()}.tmp"
        temporary.unlink(missing_ok=True)
        try:
            parent_scan = _write_projection(parent_path, temporary)
            if _file_identity(parent_path) != parent_identity:
                raise ValueError("projection parent changed while projecting")
            projected_sha256 = _file_sha256(temporary)
            target = output_directory / (
                f"source-projection-{projected_sha256}.arrow"
            )
            if target.exists():
                if _file_sha256(target) != projected_sha256:
                    raise ValueError("existing source projection hash differs")
                temporary.unlink()
            else:
                os.replace(temporary, target)
            projected_size = target.stat().st_size
            _bounded_int(
                projected_size,
                "projected size",
                1,
                MAX_PROJECTION_BYTES,
            )
            projected_scan = inspect_benchmark_source_projection(target)
            if projected_scan != parent_scan:
                raise ValueError("projected source values differ from parent")
            projections.append(
                BenchmarkSourceProjectionV1(
                    symbol=symbol,
                    period=period,
                    parent_relative_path=parent_relative.as_posix(),
                    parent_size_bytes=parent_size,
                    parent_row_count=parent_scan.row_count,
                    parent_sha256=parent_sha256,
                    projected_relative_path=target.relative_to(
                        target_root
                    ).as_posix(),
                    projected_size_bytes=projected_size,
                    projected_row_count=projected_scan.row_count,
                    projected_sha256=projected_sha256,
                    values_sha256=projected_scan.values_sha256,
                    first_timestamp_ms=projected_scan.first_timestamp_ms,
                    last_timestamp_ms=projected_scan.last_timestamp_ms,
                    timestamp_regression_count=(
                        projected_scan.timestamp_regression_count
                    ),
                    maximum_timestamp_regression_ms=(
                        projected_scan.maximum_timestamp_regression_ms
                    ),
                )
            )
        finally:
            temporary.unlink(missing_ok=True)
    return BenchmarkSourceProjectionManifestV1(
        projections=tuple(projections), frozen_at_utc=frozen_at_utc
    )


def inspect_benchmark_source_projection(path: str | Path) -> _ProjectionScan:
    """Return bounded row/order/value evidence for one canonical projection."""
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise ValueError("benchmark source projection is not a file")
    _pa, _ipc, dataset_module = _arrow_modules()
    dataset = dataset_module.dataset(str(source), format="ipc")
    _validate_projection_schema(dataset.schema)
    batches = dataset.scanner(
        columns=list(CANONICAL_BENCHMARK_PROJECTION_COLUMNS),
        batch_size=_BATCH_SIZE,
        use_threads=False,
    ).to_batches()
    return _scan_projection_batches(dataset.schema, batches)


def write_benchmark_source_projection_manifest(
    manifest: BenchmarkSourceProjectionManifestV1,
    artifact_directory: str | Path,
) -> ArtifactRef:
    """Write a content-addressed, row-free projection manifest."""
    if not isinstance(manifest, BenchmarkSourceProjectionManifestV1):
        raise TypeError("projection manifest must use the v1 contract")
    content = manifest.to_json().encode("utf-8") + b"\n"
    if len(content) > MAX_MANIFEST_BYTES:
        raise ValueError("benchmark source projection manifest exceeds bound")
    root = Path(artifact_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(content).hexdigest()
    target = root / f"benchmark-source-projection-manifest-{digest}.json"
    _write_once(target, content)
    return ArtifactRef(
        kind="benchmark_source_projection_manifest_v1",
        path=str(target),
        size_bytes=len(content),
        sha256=digest,
        metadata={"manifest_id": manifest.manifest_id},
    )


def read_benchmark_source_projection_manifest(
    path: str | Path,
) -> BenchmarkSourceProjectionManifestV1:
    """Read and verify one content-addressed projection manifest."""
    source = Path(path).expanduser().resolve()
    match = re.fullmatch(
        r"benchmark-source-projection-manifest-([0-9a-f]{64})\.json",
        source.name,
    )
    if match is None:
        raise ValueError("projection manifest name is not content addressed")
    content = source.read_bytes()
    if len(content) > MAX_MANIFEST_BYTES:
        raise ValueError("benchmark source projection manifest exceeds bound")
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError("projection manifest content hash differs")
    return BenchmarkSourceProjectionManifestV1.from_json(
        content.decode("utf-8")
    )


def validate_benchmark_source_projections(
    manifest: BenchmarkSourceProjectionManifestV1,
    projection_root: str | Path,
) -> None:
    """Fail closed unless every declared projection matches its manifest."""
    root = Path(projection_root).expanduser().resolve()
    for projection in manifest.projections:
        path = root / projection.projected_relative_path
        if not path.is_file():
            raise ValueError(
                f"benchmark source projection is missing: "
                f"{projection.projected_relative_path}"
            )
        if path.stat().st_size != projection.projected_size_bytes:
            raise ValueError("benchmark source projection size differs")
        if _file_sha256(path) != projection.projected_sha256:
            raise ValueError("benchmark source projection hash differs")
        scanned = inspect_benchmark_source_projection(path)
        expected = _ProjectionScan(
            row_count=projection.projected_row_count,
            first_timestamp_ms=projection.first_timestamp_ms,
            last_timestamp_ms=projection.last_timestamp_ms,
            timestamp_regression_count=projection.timestamp_regression_count,
            maximum_timestamp_regression_ms=(
                projection.maximum_timestamp_regression_ms
            ),
            values_sha256=projection.values_sha256,
        )
        if scanned != expected:
            raise ValueError("benchmark source projection evidence differs")


def _write_projection(parent: Path, temporary: Path) -> _ProjectionScan:
    pa, ipc, dataset_module = _arrow_modules()
    dataset = dataset_module.dataset(str(parent), format="ipc")
    _validate_projection_schema(dataset.schema)
    schema = pa.schema(
        [
            dataset.schema.field(name)
            for name in CANONICAL_BENCHMARK_PROJECTION_COLUMNS
        ],
        metadata=dataset.schema.metadata,
    )
    scanner = dataset.scanner(
        columns=list(CANONICAL_BENCHMARK_PROJECTION_COLUMNS),
        batch_size=_BATCH_SIZE,
        use_threads=False,
    )
    with pa.OSFile(str(temporary), "wb") as sink:
        with ipc.new_file(sink, schema) as writer:

            def written_batches() -> Any:
                for batch in scanner.to_batches():
                    writer.write_batch(batch)
                    yield batch

            scanned = _scan_projection_batches(schema, written_batches())
    # Windows maps fsync to the writable-handle-only CRT commit operation.
    # Reopen without truncation but retain write access across every platform.
    with temporary.open("rb+") as handle:
        os.fsync(handle.fileno())
    return scanned


def _scan_projection_batches(schema: Any, batches: Any) -> _ProjectionScan:
    _validate_projection_schema(schema)
    field_hashes = {
        name: hashlib.sha256()
        for name in CANONICAL_BENCHMARK_PROJECTION_COLUMNS
    }
    row_count = 0
    first_timestamp: int | None = None
    last_timestamp: int | None = None
    prior_timestamp: int | None = None
    regression_count = 0
    maximum_regression = 0
    for batch in batches:
        if tuple(batch.schema.names) != CANONICAL_BENCHMARK_PROJECTION_COLUMNS:
            raise ValueError("projected Arrow batch columns differ")
        count = batch.num_rows
        if count == 0:
            continue
        timestamps = batch.column(0)
        first = int(timestamps[0].as_py())
        if first_timestamp is None:
            first_timestamp = first
        for index in range(count):
            timestamp = int(timestamps[index].as_py())
            if prior_timestamp is not None and timestamp < prior_timestamp:
                regression_count += 1
                maximum_regression = max(
                    maximum_regression, prior_timestamp - timestamp
                )
            prior_timestamp = timestamp
        last_timestamp = prior_timestamp
        for index, name in enumerate(CANONICAL_BENCHMARK_PROJECTION_COLUMNS):
            array = batch.column(index)
            if array.null_count:
                raise ValueError(
                    "canonical benchmark projection contains nulls"
                )
            width = schema.field(name).type.bit_width // 8
            buffer = array.buffers()[1]
            if buffer is None:
                raise ValueError("canonical benchmark projection lacks values")
            start = array.offset * width
            field_hashes[name].update(
                memoryview(buffer)[start : start + count * width]
            )
        row_count += count
    if row_count < 2 or first_timestamp is None or last_timestamp is None:
        raise ValueError("canonical benchmark projection has too few rows")
    if (
        regression_count > MAX_HISTDATA_SOURCE_ORDER_REGRESSIONS_PER_PARTITION
        or maximum_regression > MAX_HISTDATA_SOURCE_ORDER_REGRESSION_MS
    ):
        raise ValueError("canonical benchmark projection exceeds order policy")
    digest_payload = {
        "columns": [
            {
                "name": name,
                "type": str(schema.field(name).type),
                "sha256": field_hashes[name].hexdigest(),
            }
            for name in CANONICAL_BENCHMARK_PROJECTION_COLUMNS
        ],
        "row_count": row_count,
    }
    return _ProjectionScan(
        row_count=row_count,
        first_timestamp_ms=first_timestamp,
        last_timestamp_ms=last_timestamp,
        timestamp_regression_count=regression_count,
        maximum_timestamp_regression_ms=maximum_regression,
        values_sha256=hashlib.sha256(
            canonical_contract_json(digest_payload).encode("utf-8")
        ).hexdigest(),
    )


def _validate_projection_schema(schema: Any) -> None:
    pa, _ipc, _dataset = _arrow_modules()
    expected = {
        "datetime": pa.int64(),
        "bid": pa.float64(),
        "ask": pa.float64(),
        "vol": pa.int32(),
    }
    if not set(expected).issubset(schema.names):
        raise ValueError("Arrow parent lacks canonical projection columns")
    if any(schema.field(name).type != kind for name, kind in expected.items()):
        raise ValueError("Arrow parent canonical projection types differ")


def _arrow_modules() -> tuple[Any, Any, Any]:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        import pyarrow.dataset as ds  # pylint: disable=import-outside-toplevel
        import pyarrow.ipc as ipc  # pylint: disable=import-outside-toplevel
    except ImportError as exc:
        raise RuntimeError(
            "benchmark source projection requires histdatacom[arrow]"
        ) from exc
    return pa, ipc, ds


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not re.fullmatch(r"[A-Z]{6}", symbol):
        raise ValueError("projection symbol must be a six-letter FX pair")
    return symbol


def _relative_path(value: str, name: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{name} path must be relative and contained")
    return path.as_posix()


def _bounded_int(value: int, name: str, minimum: int, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _strict_int(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("projection integer field is invalid")
    return value


def _sha256(value: str, name: str) -> str:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


def _utc_timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("projection frozen timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(
        parsed
    ):
        raise ValueError("projection frozen timestamp must be UTC")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a projection mapping")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _file_identity(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return (
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
        stat.st_dev,
        stat.st_ino,
    )


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("content-addressed projection artifact differs")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "BENCHMARK_SOURCE_PROJECTION_MANIFEST_SCHEMA_VERSION",
    "BENCHMARK_SOURCE_PROJECTION_SCHEMA_VERSION",
    "CANONICAL_BENCHMARK_PROJECTION_COLUMNS",
    "BenchmarkSourceProjectionManifestV1",
    "BenchmarkSourceProjectionV1",
    "build_benchmark_source_projection_manifest",
    "inspect_benchmark_source_projection",
    "read_benchmark_source_projection_manifest",
    "validate_benchmark_source_projections",
    "write_benchmark_source_projection_manifest",
]
