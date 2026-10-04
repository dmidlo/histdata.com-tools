"""Closed native retention proofs, not dataset or release qualification.

Native producers run only in fresh, unmanaged scratch. Storage owns publication
and writes exact bytes at final immutable paths; this module never writes into
the managed namespace. A locally constructed execution value is not authority:
managed admissions repeat the concrete producer and compare its actual output.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import stat
import sys
from typing import Any

from histdatacom.artifact_retention.canonical import _lexical_bound, sha256
from histdatacom.artifact_retention.contracts import (
    AsciiTickRecipeV1,
    CacheRegenerationV1,
    DependencyRelation,
    DependencyV1,
    ManagedPayloadRefV1,
    NativeObjectV1,
    RetentionClass,
)

ASCII_SOURCE_ADAPTER = "ascii-tick-source.v1"
ASCII_RECIPE_ADAPTER = "ascii-tick-recipe.v1"
CACHE_ADAPTER = "histdata-arrow-cache.v1"
CATALOG_ADAPTER = "dataset-catalog.v1"
REGENERATION_ADAPTER = "cache-regeneration.v1"
MAX_ASCII_BYTES = 2 * 1024 * 1024
MAX_ASCII_ROWS = 4096
MAX_CACHE_BYTES = 64 * 1024 * 1024
MAX_NATIVE_JSON_BYTES = 16 * 1024 * 1024
MAX_PARTITIONS = 16
MAX_INVENTORY = 1024
_MARKER = ".histdatacom-retention.json"
_SOURCE_ID = re.compile(
    r"retention-ascii-source:([A-Z]{6}):([0-9]{6}):sha256:([0-9a-f]{64})\Z"
)
_IMPLEMENTATION_FILES = (
    "activity_stages.py",
    "artifact_retention/native.py",
    "data_quality/contracts.py",
    "data_quality/training_features.py",
    "datasets/adapters.py",
    "datasets/catalog.py",
    "datasets/contracts.py",
    "histdata_ascii.py",
    "reconstruction_evidence.py",
    "records.py",
    "runtime_contracts.py",
    "utils.py",
)


@dataclass(frozen=True, slots=True)
class AsciiSourceInspection:
    """Local result of actual bounded native CSV parsing, not a wire proof."""

    sha256: str
    size_bytes: int
    row_count: int
    symbol: str
    period: str


@dataclass(frozen=True, slots=True)
class NativeCacheExecution:
    """Unpublished local output; managed admission never trusts it alone."""

    cache_bytes: bytes
    partition_json: str
    native_readback_sha256: str
    source_sha256: str
    source_size_bytes: int
    implementation_sha256: str
    backend_id: str
    decision: str
    cache_created: bool
    reused_existing: bool


@dataclass(frozen=True, slots=True)
class NativeAdmission:
    """Closed extraction result for the caller's already verified inventory."""

    native_schema: str
    native_id: str
    live_dependencies: tuple[DependencyV1, ...]
    historical_subject_ids: tuple[str, ...] = ()
    native_readback_sha256: str = ""


@dataclass(frozen=True, slots=True)
class NativeCatalogBuild:
    """Exact newly located wire for storage to publish, not a native writer."""

    canonical_bytes: bytes
    admission: NativeAdmission


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def current_cache_implementation_sha256() -> str:
    """Bind selected recipe/readback sources, not a full transitive SBOM."""
    package = Path(__file__).resolve().parents[1]
    return sha256(
        _json(
            {
                name: sha256((package / name).read_bytes())
                for name in _IMPLEMENTATION_FILES
            }
        ).encode("ascii")
    )


def current_cache_backend_id() -> str:
    """Exact selected runtime profile; never a claim of cross-version bytes."""
    value = {
        "python": sys.version,
        "implementation": sys.implementation.name,
        "machine": platform.machine(),
        "byteorder": sys.byteorder,
        "distributions": {
            name: importlib.metadata.version(name) for name in ("polars",)
        },
    }
    return "retention-cache-backend:sha256:" + sha256(
        _json(value).encode("ascii")
    )


def inspect_ascii_source(
    raw: bytes, *, symbol: str, period: str
) -> AsciiSourceInspection:
    """Parse a bounded native ASCII/T CSV without accepting ZIP or a path."""
    from histdatacom.datasets.contracts import (
        normalize_period,
        normalize_symbol,
    )
    from histdatacom.histdata_ascii import (
        EST_NO_DST_OFFSET_MS,
        parse_ascii_lines,
    )

    if type(raw) is not bytes or not 0 < len(raw) <= MAX_ASCII_BYTES:
        raise ValueError("ASCII source bytes exceed the closed recipe bound")
    if (
        type(symbol) is not str
        or len(symbol) != 6
        or normalize_symbol(symbol) != symbol
        or type(period) is not str
        or len(period) != 6
        or normalize_period(period) != period
    ):
        raise ValueError("ASCII source dimensions must be canonical")
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("ASCII source must contain ASCII bytes") from exc
    # Bound rows and individual fields before invoking the native parser.
    if raw.count(b"\n") + 1 > MAX_ASCII_ROWS + 1:
        raise ValueError("ASCII source row count exceeds the recipe bound")
    lines = text.splitlines()
    if not lines or len(lines) > MAX_ASCII_ROWS:
        raise ValueError("ASCII source row count exceeds the recipe bound")
    if any(not line or len(line) > 512 for line in lines):
        raise ValueError("ASCII source rows must be nonempty and bounded")
    parsed = parse_ascii_lines("T", lines)
    if len(parsed.rows) != len(lines):
        raise ValueError("ASCII source contains ignored rows")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    for timestamp_ms, bid, ask, volume in parsed.rows:
        source_clock = epoch + timedelta(
            milliseconds=timestamp_ms - EST_NO_DST_OFFSET_MS
        )
        if source_clock.strftime("%Y%m") != period:
            raise ValueError(
                "ASCII source timestamps differ from declared period"
            )
        if (
            not math.isfinite(bid)
            or not math.isfinite(ask)
            or not -(2**31) <= volume < 2**31
        ):
            raise ValueError("ASCII source numeric values exceed native bounds")
    return AsciiSourceInspection(
        sha256(raw), len(raw), len(parsed.rows), symbol, period
    )


def _fresh_scratch(path: Path) -> Path:
    if type(path) is not Path:
        # PosixPath is Path's normal concrete implementation.
        if not isinstance(path, Path):
            raise TypeError("scratch directory must be a Path")
    absolute = path.absolute()
    if absolute != absolute.resolve():
        raise ValueError("scratch ancestry cannot contain a symlink")
    for ancestor in (absolute, *absolute.parents):
        if os.path.lexists(ancestor / _MARKER):
            raise ValueError("native producer cannot write in a managed store")
    if absolute.exists():
        raise ValueError("native recipe requires a fresh nonexistent directory")
    absolute.mkdir()
    return absolute


def _frame_digest(frame: Any) -> str:
    if not 0 < frame.height <= MAX_ASCII_ROWS:
        raise ValueError("native readback exceeds admitted source rows")
    return sha256(
        _json(
            {
                "schema": {
                    name: str(dtype) for name, dtype in frame.schema.items()
                },
                "columns": frame.to_dict(as_series=False),
            }
        ).encode("ascii")
    )


def execute_ascii_cache_recipe(
    raw: bytes,
    recipe: AsciiTickRecipeV1,
    *,
    scratch_directory: Path,
) -> NativeCacheExecution:
    """Actually build one fresh cache, without download or source deletion.

    The scratch and its native status database are retained for the owning
    transaction. No recursive cleanup or native writer inside a store occurs.
    """
    from histdatacom.activity_stages import build_cache_work_item
    from histdatacom.datasets.adapters import (
        HistDataProviderAdapter,
        histdata_cache_path,
    )
    from histdatacom.records import Record
    from histdatacom.runtime_contracts import WorkItem, WorkStatus

    if type(recipe) is not AsciiTickRecipeV1:
        raise TypeError("native cache execution requires the exact recipe")
    recipe = AsciiTickRecipeV1.from_json(recipe.to_json())
    source = inspect_ascii_source(
        raw, symbol=recipe.symbol, period=recipe.period
    )
    implementation = current_cache_implementation_sha256()
    backend = current_cache_backend_id()
    if (
        recipe.implementation_sha256 != implementation
        or recipe.backend_id != backend
    ):
        raise ValueError("cache recipe implementation or backend is stale")
    scratch = _fresh_scratch(scratch_directory)
    source_root = scratch / "ASCII/T"
    cache = histdata_cache_path(source_root, source.symbol, source.period)
    cache.parent.mkdir(parents=True)
    csv = cache.parent / (f"DAT_ASCII_{source.symbol}_T_{source.period}.csv")
    with csv.open("xb") as stream:
        stream.write(raw)
    record = Record(
        url="retention:ascii-tick:" + source.sha256,
        data_dir=str(cache.parent) + os.sep,
        csv_filename=csv.name,
        zip_filename="",
        data_format="ascii",
        data_timeframe="T",
        data_fxpair=source.symbol.lower(),
        data_year=source.period[:4],
        data_month=source.period[4:],
        data_date=source.period,
        data_datemonth=source.period,
        status=WorkStatus.CSV_FILE,
    )
    if cache.exists():
        raise ValueError("fresh native cache output already exists")
    output = build_cache_work_item(
        WorkItem.from_record(record),
        args={
            "default_download_dir": str(scratch) + os.sep,
            "delete_after_cache": False,
            "delete_after_influx": False,
        },
        download_file=None,
    )
    metrics = output.result.metrics
    if (
        output.result.status is not WorkStatus.CACHE_READY
        or metrics.get("decision") != "built"
        or metrics.get("cache_created") is not True
        or metrics.get("reused_existing") is not False
        or metrics.get("source_artifacts_deleted") != []
    ):
        raise ValueError("native result is not a fresh regeneration")
    if csv.read_bytes() != raw:
        raise ValueError("native source bytes changed during regeneration")
    cache_bytes = _read_regular(cache, MAX_CACHE_BYTES)
    adapter = HistDataProviderAdapter()
    partition = adapter.inspect_partition(
        source_root,
        symbol=source.symbol,
        period=source.period,
        expected_sha256=sha256(cache_bytes),
    )
    frame = adapter.read_partition(partition)
    if frame.height != source.row_count:
        raise ValueError("native cache readback lost source rows")
    readback = _frame_digest(frame)
    if (
        implementation != current_cache_implementation_sha256()
        or backend != current_cache_backend_id()
        or csv.read_bytes() != raw
        or _read_regular(cache, MAX_CACHE_BYTES) != cache_bytes
    ):
        raise ValueError("native execution inputs changed during readback")
    return NativeCacheExecution(
        cache_bytes,
        _json(partition.to_dict()),
        readback,
        source.sha256,
        source.size_bytes,
        implementation,
        backend,
        "built",
        True,
        False,
    )


def verify_regeneration_match(
    execution: NativeCacheExecution, exact_cache_bytes: bytes
) -> None:
    """Reject reused, foreign or non-byte-identical output, not just its ID."""
    if type(execution) is not NativeCacheExecution:
        raise TypeError("regeneration requires the exact local execution")
    if (
        type(exact_cache_bytes) is not bytes
        or not 0 < len(exact_cache_bytes) <= MAX_CACHE_BYTES
        or execution.decision != "built"
        or execution.cache_created is not True
        or execution.reused_existing is not False
        or type(execution.cache_bytes) is not bytes
        or execution.cache_bytes != exact_cache_bytes
    ):
        raise ValueError("cache does not match fresh native regeneration")


def _read_regular(path: Path, maximum: int) -> bytes:
    # Native readers dereference literal paths, so preflight every ancestor and
    # recheck the complete file identity afterward. Storage additionally locks.
    absolute = path.absolute()
    if absolute != absolute.resolve():
        raise ValueError("native artifact path traverses a symlink")
    flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
    before = absolute.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not 0 < before.st_size <= maximum
    ):
        raise ValueError("native artifact must be a bounded single-link file")
    fd = os.open(absolute, flags)
    try:
        current = os.fstat(fd)
        if _stat_identity(current) != _stat_identity(before):
            raise ValueError("native artifact identity changed before read")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(maximum + 1)
        if len(raw) != before.st_size or _stat_identity(
            os.fstat(fd)
        ) != _stat_identity(before):
            raise ValueError("native artifact changed during read")
    finally:
        os.close(fd)
    if _stat_identity(absolute.lstat()) != _stat_identity(before):
        raise ValueError("native artifact path changed after read")
    return raw


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def read_managed_payload(
    root: Path,
    ref: ManagedPayloadRefV1,
    *,
    maximum: int = MAX_CACHE_BYTES,
) -> bytes:
    """Read the exact bound regular payload; never resolve an external ref."""
    if type(ref) is not ManagedPayloadRefV1:
        raise TypeError("native payload requires an exact managed reference")
    ref = ManagedPayloadRefV1.from_json(ref.to_json())
    if (
        type(maximum) is not int
        or not 0 < maximum <= MAX_CACHE_BYTES
        or ref.size_bytes > maximum
    ):
        raise ValueError("native payload exceeds its admitted family bound")
    if root.absolute() != root.resolve():
        raise ValueError("managed root must have canonical physical ancestry")
    path = root / ref.relative_path
    if not path.is_relative_to(root):
        raise ValueError("native payload escapes the managed root")
    raw = _read_regular(path, maximum)
    if len(raw) != ref.size_bytes or sha256(raw) != ref.sha256:
        raise ValueError("managed payload bytes differ from their reference")
    return raw


def _load_native(raw: bytes) -> dict[str, Any]:
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_NATIVE_JSON_BYTES:
        raise ValueError("native JSON bytes exceed bounds")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, value in items:
            if name in result:
                raise ValueError("duplicate native JSON field")
            result[name] = value
        return result

    try:
        text = raw.decode("ascii")
        _lexical_bound(text)
        value = json.loads(text, object_pairs_hook=pairs)
        if type(value) is not dict or _json(value) != text:
            raise ValueError("native JSON is not exact canonical wire")
    except (UnicodeError, RecursionError, OverflowError) as exc:
        raise ValueError("native JSON is not bounded canonical ASCII") from exc
    return value


def _dependencies(
    values: tuple[tuple[str, DependencyRelation], ...],
) -> tuple[DependencyV1, ...]:
    return tuple(
        DependencyV1(identity, DependencyRelation(relation))
        for identity, relation in sorted(
            {(identity, relation.value) for identity, relation in values}
        )
    )


def _inventory(
    inventory: tuple[NativeObjectV1, ...],
) -> dict[str, NativeObjectV1]:
    if type(inventory) is not tuple or len(inventory) > MAX_INVENTORY:
        raise ValueError("native inventory must be an exact bounded tuple")
    result = {}
    paths = set()
    for item in inventory:
        if type(item) is not NativeObjectV1:
            raise TypeError("native inventory requires exact descriptors")
        admitted = NativeObjectV1.from_json(item.to_json())
        if (
            admitted.artifact_id in result
            or admitted.payload_ref.relative_path in paths
        ):
            raise ValueError("native inventory repeats an object or path")
        paths.add(admitted.payload_ref.relative_path)
        result[admitted.artifact_id] = admitted
    return result


def _require_object(
    objects: dict[str, NativeObjectV1], identity: str, adapter: str
) -> NativeObjectV1:
    item = objects.get(identity)
    if item is None or item.adapter_id != adapter:
        raise ValueError("missing or unsupported native dependency")
    return item


def _match_descriptor(
    item: NativeObjectV1,
    expected: NativeAdmission,
    retention_class: RetentionClass,
) -> NativeAdmission:
    if (
        item.native_schema != expected.native_schema
        or item.native_id != expected.native_id
        or item.live_dependencies != expected.live_dependencies
        or item.historical_subject_ids != expected.historical_subject_ids
        or item.retention_class is not retention_class
    ):
        raise ValueError("native descriptor identity, class or edges differ")
    return expected


def source_admission(
    raw: bytes, *, symbol: str, period: str
) -> NativeAdmission:
    """Bind parsed CSV dimensions and bytes, without claiming a native wire."""
    inspected = inspect_ascii_source(raw, symbol=symbol, period=period)
    return NativeAdmission(
        "histdata-ascii-tick-csv.v1",
        f"retention-ascii-source:{symbol}:{period}:sha256:{inspected.sha256}",
        (),
    )


def _verify_source(root: Path, source: NativeObjectV1) -> bytes:
    match = _SOURCE_ID.fullmatch(source.native_id)
    if match is None or not source.payload_ref.relative_path.endswith(".csv"):
        raise ValueError("ASCII source identity or payload type is invalid")
    symbol, period, _ = match.groups()
    raw = read_managed_payload(
        root, source.payload_ref, maximum=MAX_ASCII_BYTES
    )
    _match_descriptor(
        source,
        source_admission(raw, symbol=symbol, period=period),
        RetentionClass.IMMUTABLE_SOURCE,
    )
    return raw


def _recipe(
    root: Path,
    item: NativeObjectV1,
    objects: dict[str, NativeObjectV1],
) -> tuple[AsciiTickRecipeV1, NativeObjectV1, bytes]:
    if not item.payload_ref.relative_path.endswith(".json"):
        raise ValueError("native recipe requires a JSON payload")
    raw = read_managed_payload(
        root, item.payload_ref, maximum=MAX_NATIVE_JSON_BYTES
    )
    recipe = AsciiTickRecipeV1.from_json(raw.decode("ascii"))
    source = _require_object(
        objects, recipe.raw_source_object_id, ASCII_SOURCE_ADAPTER
    )
    source_bytes = _verify_source(root, source)
    expected_source = source_admission(
        source_bytes, symbol=recipe.symbol, period=recipe.period
    )
    if expected_source.native_id != source.native_id:
        raise ValueError("recipe dimensions differ from the admitted source")
    _match_descriptor(
        item,
        NativeAdmission(
            recipe.SCHEMA,
            recipe.artifact_id,
            _dependencies(((source.artifact_id, DependencyRelation.SOURCE),)),
        ),
        RetentionClass.REPRODUCIBILITY_DEPENDENCY,
    )
    return recipe, source, source_bytes


def _cache_inputs(
    root: Path,
    cache: NativeObjectV1,
    objects: dict[str, NativeObjectV1],
) -> tuple[NativeObjectV1, AsciiTickRecipeV1, NativeObjectV1, bytes]:
    recipe_ids = tuple(
        edge.target_object_id
        for edge in cache.live_dependencies
        if edge.relation is DependencyRelation.RECIPE
    )
    if len(recipe_ids) != 1:
        raise ValueError("native cache requires one closed recipe")
    recipe_object = _require_object(
        objects, recipe_ids[0], ASCII_RECIPE_ADAPTER
    )
    recipe, source, raw = _recipe(root, recipe_object, objects)
    if cache.live_dependencies != _dependencies(
        (
            (recipe_object.artifact_id, DependencyRelation.RECIPE),
            (source.artifact_id, DependencyRelation.SOURCE),
        )
    ):
        raise ValueError("cache source/recipe dependencies are incomplete")
    if not cache.payload_ref.relative_path.endswith(".data"):
        raise ValueError("native cache requires a data payload")
    return recipe_object, recipe, source, raw


def _located_partition(
    root: Path,
    cache: NativeObjectV1,
    execution: NativeCacheExecution,
) -> Any:
    from histdatacom.datasets.adapters import HistDataProviderAdapter
    from histdatacom.datasets.contracts import CanonicalObservedPartitionV2
    from histdatacom.runtime_contracts import ArtifactRef

    raw = read_managed_payload(root, cache.payload_ref)
    verify_regeneration_match(execution, raw)
    generated = _load_native(execution.partition_json.encode("ascii"))
    source_partition = CanonicalObservedPartitionV2.from_dict(generated)
    if _json(source_partition.to_dict()) != execution.partition_json:
        raise ValueError("fresh native partition is not exact canonical wire")
    # This is a NEW located native object derived from the fresh execution,
    # never an in-place rewrite of an imported or published immutable object.
    generated["artifact"] = ArtifactRef(
        kind=source_partition.artifact.kind,
        path=str(root / cache.payload_ref.relative_path),
        size_bytes=cache.payload_ref.size_bytes,
        sha256=cache.payload_ref.sha256,
        metadata=dict(source_partition.artifact.metadata),
    ).to_dict()
    located = CanonicalObservedPartitionV2.from_dict(generated)
    actual = HistDataProviderAdapter().read_partition(located)
    if _frame_digest(actual) != execution.native_readback_sha256:
        raise ValueError("managed native readback differs from regeneration")
    if read_managed_payload(root, cache.payload_ref) != raw:
        raise ValueError("managed cache changed during native readback")
    return located


def _verify_cache(
    root: Path,
    cache: NativeObjectV1,
    objects: dict[str, NativeObjectV1],
    scratch_directory: Path,
) -> tuple[NativeAdmission, NativeCacheExecution, Any]:
    recipe_object, recipe, source, source_bytes = _cache_inputs(
        root, cache, objects
    )
    execution = execute_ascii_cache_recipe(
        source_bytes, recipe, scratch_directory=scratch_directory
    )
    partition = _located_partition(root, cache, execution)
    admission = _match_descriptor(
        cache,
        NativeAdmission(
            partition.schema_version,
            partition.partition_id,
            _dependencies(
                (
                    (recipe_object.artifact_id, DependencyRelation.RECIPE),
                    (source.artifact_id, DependencyRelation.SOURCE),
                )
            ),
            native_readback_sha256=execution.native_readback_sha256,
        ),
        RetentionClass.REPLACEABLE_CACHE,
    )
    return admission, execution, partition


def prove_cache_regeneration(
    root: Path,
    cache_object_id: str,
    inventory: tuple[NativeObjectV1, ...],
    *,
    scratch_directory: Path,
) -> CacheRegenerationV1:
    """Actually regenerate a live cache; a historical receipt cannot do so."""
    objects = _inventory(inventory)
    cache = _require_object(objects, cache_object_id, CACHE_ADAPTER)
    _, execution, partition = _verify_cache(
        root, cache, objects, scratch_directory
    )
    recipe_object, recipe, source, _ = _cache_inputs(root, cache, objects)
    return CacheRegenerationV1(
        recipe_object.artifact_id,
        source.artifact_id,
        cache.artifact_id,
        cache.payload_ref.sha256,
        cache.payload_ref.size_bytes,
        sha256(execution.cache_bytes),
        len(execution.cache_bytes),
        partition.partition_id,
        execution.native_readback_sha256,
        recipe.implementation_sha256,
        recipe.backend_id,
    )


def _verify_historical_regeneration(
    root: Path,
    item: NativeObjectV1,
    objects: dict[str, NativeObjectV1],
    scratch_directory: Path,
) -> NativeAdmission:
    proof = CacheRegenerationV1.from_json(
        read_managed_payload(root, item.payload_ref).decode("ascii")
    )
    cache = _require_object(objects, proof.cache_object_id, CACHE_ADAPTER)
    recipe_object, recipe, source, raw = _cache_inputs(root, cache, objects)
    execution = execute_ascii_cache_recipe(
        raw, recipe, scratch_directory=scratch_directory
    )
    partition = _load_native(execution.partition_json.encode("ascii"))
    _match_descriptor(
        cache,
        NativeAdmission(
            partition["schema_version"],
            partition["partition_id"],
            _dependencies(
                (
                    (recipe_object.artifact_id, DependencyRelation.RECIPE),
                    (source.artifact_id, DependencyRelation.SOURCE),
                )
            ),
        ),
        RetentionClass.REPLACEABLE_CACHE,
    )
    # Do not open the historical output path. Its source/recipe remain live;
    # a removed output is not a dangling required-bytes edge of this proof.
    expected = CacheRegenerationV1(
        recipe_object.artifact_id,
        source.artifact_id,
        cache.artifact_id,
        cache.payload_ref.sha256,
        cache.payload_ref.size_bytes,
        sha256(execution.cache_bytes),
        len(execution.cache_bytes),
        partition["partition_id"],
        execution.native_readback_sha256,
        execution.implementation_sha256,
        execution.backend_id,
    )
    if proof.to_json() != expected.to_json():
        raise ValueError("historical regeneration differs from fresh replay")
    return _match_descriptor(
        item,
        NativeAdmission(
            proof.SCHEMA,
            proof.artifact_id,
            _dependencies(
                (
                    (source.artifact_id, DependencyRelation.SOURCE),
                    (recipe_object.artifact_id, DependencyRelation.RECIPE),
                )
            ),
            (cache.artifact_id,),
            execution.native_readback_sha256,
        ),
        RetentionClass.REPRODUCIBILITY_DEPENDENCY,
    )


def build_native_catalog(
    root: Path,
    cache_object_ids: tuple[str, ...],
    inventory: tuple[NativeObjectV1, ...],
    *,
    dataset_id: str,
    scratch_directory: Path,
) -> NativeCatalogBuild:
    """Construct a NEW unqualified managed catalog after actual regeneration."""
    from histdatacom.datasets import (
        DatasetCatalog,
        DatasetDescriptorV1,
        DatasetOrigin,
        HistDataProviderAdapter,
    )
    from histdatacom.datasets.contracts import (
        DatasetQualificationStatus,
        DatasetVersionManifestV1,
        normalize_dataset_id,
    )

    if (
        type(dataset_id) is not str
        or not 0 < len(dataset_id) <= 256
        or normalize_dataset_id(dataset_id) != dataset_id
    ):
        raise ValueError("catalog dataset ID must be bounded and canonical")
    if (
        type(cache_object_ids) is not tuple
        or not 0 < len(cache_object_ids) <= MAX_PARTITIONS
        or any(
            type(identity) is not str
            or re.fullmatch(r"retention-object:sha256:[0-9a-f]{64}", identity)
            is None
            for identity in cache_object_ids
        )
        or len(set(cache_object_ids)) != len(cache_object_ids)
    ):
        raise ValueError("catalog requires a bounded unique cache selection")
    objects = _inventory(inventory)
    caches = tuple(
        _require_object(objects, identity, CACHE_ADAPTER)
        for identity in cache_object_ids
    )
    scratch = _fresh_scratch(scratch_directory)
    partitions = []
    edges: list[tuple[str, DependencyRelation]] = []
    readbacks = {}
    for ordinal, cache in enumerate(caches):
        _, execution, partition = _verify_cache(
            root, cache, objects, scratch / f"partition-{ordinal}"
        )
        partitions.append(partition)
        _, _, source, _ = _cache_inputs(root, cache, objects)
        edges.extend(
            (
                (cache.artifact_id, DependencyRelation.NATIVE_ARTIFACT),
                (source.artifact_id, DependencyRelation.SOURCE),
            )
        )
        readbacks[partition.partition_id] = execution.native_readback_sha256
    adapter = HistDataProviderAdapter()
    descriptor = DatasetDescriptorV1(
        dataset_id=dataset_id,
        display_name="Managed ASCII/T cache publication",
        description=(
            "Physically verified native cache publication; "
            "not scientific dataset qualification."
        ),
        allowed_origins=(DatasetOrigin.OBSERVED,),
    )
    version = DatasetVersionManifestV1(
        dataset_id=descriptor.dataset_id,
        origin=DatasetOrigin.OBSERVED,
        normalization_policy_id=(
            f"{adapter.descriptor.adapter_id}@"
            f"{adapter.descriptor.adapter_version}:"
            f"{adapter.descriptor.projection_schema_version}"
        ),
        qualification_status=DatasetQualificationStatus.UNQUALIFIED,
        partitions=tuple(partitions),
    )
    catalog = DatasetCatalog(
        providers=(adapter.provider,),
        adapters=(adapter.descriptor,),
        datasets=(descriptor,),
        versions=(version,),
    )
    return NativeCatalogBuild(
        catalog.to_json().encode("ascii"),
        NativeAdmission(
            catalog.schema_version,
            catalog.catalog_id,
            _dependencies(tuple(edges)),
            native_readback_sha256=sha256(_json(readbacks).encode("ascii")),
        ),
    )


def verify_native_catalog(
    root: Path,
    item: NativeObjectV1,
    inventory: tuple[NativeObjectV1, ...],
    *,
    scratch_directory: Path,
) -> NativeAdmission:
    """Extract every actual supported reference, then repeat native replay.

    Qualified/derived/composed versions, aliases, evidence, foreign providers
    and metadata outside this one closed producer are unsupported, not empty
    outgoing graphs. No qualified resolution receipt is manufactured.
    """
    from histdatacom.datasets import DatasetCatalog

    if type(item) is not NativeObjectV1 or item.adapter_id != CATALOG_ADAPTER:
        raise TypeError("catalog verification requires the closed descriptor")
    item = NativeObjectV1.from_json(item.to_json())
    objects = _inventory(inventory)
    if objects.get(item.artifact_id) != item:
        raise ValueError("catalog is absent from exact managed inventory")
    raw = read_managed_payload(
        root, item.payload_ref, maximum=MAX_NATIVE_JSON_BYTES
    )
    payload = _load_native(raw)
    catalog = DatasetCatalog.from_dict(payload)
    if catalog.to_json().encode("ascii") != raw:
        raise ValueError("native catalog fields or canonical types differ")
    if (
        len(catalog.versions) != 1
        or len(catalog.datasets) != 1
        or catalog.aliases
        or catalog.versions[0].parents
        or catalog.versions[0].qualification_evidence
        or catalog.versions[0].delivery_profile_id is not None
        or not 0 < len(catalog.versions[0].partitions) <= MAX_PARTITIONS
    ):
        raise ValueError("native catalog family or references are unsupported")
    by_path = {
        str(root / obj.payload_ref.relative_path): obj
        for obj in objects.values()
    }
    cache_ids = []
    for partition in catalog.versions[0].partitions:
        cache = by_path.get(partition.artifact.path)
        if cache is None or cache.adapter_id != CACHE_ADAPTER:
            raise ValueError(
                "native catalog has an unmanaged/unknown reference"
            )
        if (
            partition.artifact.sha256 != cache.payload_ref.sha256
            or partition.artifact.size_bytes != cache.payload_ref.size_bytes
            or partition.source_artifact_sha256 != cache.payload_ref.sha256
        ):
            raise ValueError("native artifact or hash-only source differs")
        cache_ids.append(cache.artifact_id)
    expected = build_native_catalog(
        root,
        tuple(cache_ids),
        inventory,
        dataset_id=catalog.datasets[0].dataset_id,
        scratch_directory=scratch_directory,
    )
    if expected.canonical_bytes != raw:
        raise ValueError(
            "native catalog differs from actual managed publication"
        )
    if read_managed_payload(root, item.payload_ref) != raw:
        raise ValueError("native catalog changed during replay")
    return _match_descriptor(
        item, expected.admission, RetentionClass.PUBLISHED_DERIVED
    )


def admit_native_object(
    root: Path,
    item: NativeObjectV1,
    inventory: tuple[NativeObjectV1, ...],
    *,
    scratch_directory: Path,
) -> NativeAdmission:
    """Closed actual native dispatch; unknown families never have zero edges."""
    objects = _inventory(inventory)
    if type(item) is not NativeObjectV1:
        raise TypeError("native admission requires an exact descriptor")
    item = NativeObjectV1.from_json(item.to_json())
    if objects.get(item.artifact_id) != item:
        raise ValueError("native object is absent from exact inventory")
    if item.adapter_id == ASCII_SOURCE_ADAPTER:
        raw = _verify_source(root, item)
        match = _SOURCE_ID.fullmatch(item.native_id)
        assert match is not None
        return source_admission(raw, symbol=match[1], period=match[2])
    if item.adapter_id == ASCII_RECIPE_ADAPTER:
        recipe, source, _ = _recipe(root, item, objects)
        return NativeAdmission(
            recipe.SCHEMA,
            recipe.artifact_id,
            _dependencies(((source.artifact_id, DependencyRelation.SOURCE),)),
        )
    if item.adapter_id == CACHE_ADAPTER:
        return _verify_cache(root, item, objects, scratch_directory)[0]
    if item.adapter_id == CATALOG_ADAPTER:
        return verify_native_catalog(
            root, item, inventory, scratch_directory=scratch_directory
        )
    if item.adapter_id == REGENERATION_ADAPTER:
        return _verify_historical_regeneration(
            root, item, objects, scratch_directory
        )
    raise ValueError("unsupported native retention adapter or opaque metadata")
