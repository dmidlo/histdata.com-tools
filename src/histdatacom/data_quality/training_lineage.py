"""Closed source verification and immutable dependency-closed evidence units."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from histdatacom.datasets.adapters import (
    FixtureProviderAdapter,
    HistDataProviderAdapter,
)
from histdatacom.datasets.catalog import DatasetCatalog
from histdatacom.datasets.contracts import DatasetOrigin
from histdatacom.forecasting.feature_artifacts import (
    FeatureArtifact,
    read_feature_artifact,
)
from histdatacom.forecasting.feature_forecasts import ForecastFeatureSnapshotV1
from histdatacom.forecasting.feature_store import FeatureMatrixSnapshotV1
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV1,
    ReconstructionProductManifestV2,
    ReconstructionProductManifestV3,
    load_reconstruction_manifest,
    read_reconstruction_streams,
)

from .training_contracts import (
    DAY_NS,
    MAX_TRAINING_BYTES,
    MAX_TRAINING_ITEMS,
    MAX_TRAINING_SOURCE_BYTES,
    MAX_TRAINING_SOURCE_ROWS,
    TrainingEvidenceUnitV1,
    TrainingOwnershipV1,
    TrainingRootV1,
    TrainingSourceV1,
    TrainingVerificationLevel,
    training_json,
    training_load,
)

Product = (
    ReconstructionProductManifestV1
    | ReconstructionProductManifestV2
    | ReconstructionProductManifestV3
)


def read_training_regular(path: Path, limit: int = MAX_TRAINING_BYTES) -> bytes:
    """Bounded no-follow regular-file read, including inode replacement check."""
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
        raise ValueError("training input is not a bounded regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or (
            opened.st_dev,
            opened.st_ino,
        ) != (before.st_dev, before.st_ino):
            raise ValueError("training input changed during open")
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("training input exceeds byte bound")
    return data


def _sha(value: object) -> str:
    return hashlib.sha256(training_json(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class _ObservedRow:
    symbol: str
    series_id: str
    period: str
    row_id: int
    event_time_ns: int
    bid: float
    ask: float
    vol: int
    quote_projection: bool

    @property
    def key(self) -> tuple[str, str, int]:
        return self.series_id, self.period, self.row_id

    def payload(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "source_series_id": self.series_id,
            "source_period": self.period,
            "source_row_id": self.row_id,
            "event_time_ns": self.event_time_ns,
            "bid": self.bid,
            "ask": self.ask,
            "vol": self.vol,
        }


@dataclass(frozen=True, slots=True)
class _VerifiedProduct:
    manifest: Product
    events: tuple[SyntheticEventV1, ...]
    root: TrainingRootV1
    dependency_start_ns: int
    dependency_end_ns: int


@dataclass(frozen=True, slots=True)
class _VerifiedContext:
    root: TrainingRootV1
    start_ns: int
    end_ns: int


@dataclass(frozen=True, slots=True)
class _VerifiedFeature:
    root: TrainingRootV1
    artifact: FeatureArtifact
    start_ns: int
    end_ns: int


@dataclass(frozen=True, slots=True)
class _VerifiedSource:
    source: TrainingSourceV1
    observed: tuple[_ObservedRow, ...]
    products: tuple[_VerifiedProduct, ...]
    contexts: tuple[_VerifiedContext, ...]
    features: tuple[_VerifiedFeature, ...]
    roots: tuple[TrainingRootV1, ...]
    graph_symbols: tuple[str, ...]
    start_ns: int
    end_ns: int


def _sorted_roots(
    roots: tuple[TrainingRootV1, ...],
) -> tuple[TrainingRootV1, ...]:
    by_id = {root.artifact_id: root for root in roots}
    if len(by_id) != len(roots):
        raise ValueError("ambiguous duplicate verification root")
    return tuple(by_id[key] for key in sorted(by_id))


def _matrix_span(matrix: FeatureMatrixSnapshotV1) -> tuple[int, int]:
    clocks = [p.start_ns for p in matrix.request.periods]
    ends = [p.end_ns for p in matrix.request.periods]
    ends += [matrix.request.cutoff_at_ns + 1]
    clocks += [o.available_at_ns for o in matrix.observations]
    clocks += [s.known_at_ns for s in matrix.schedules]
    return min(clocks), max(ends + [clock + 1 for clock in clocks])


def _feature_span(artifact: FeatureArtifact) -> tuple[int, int]:
    if isinstance(artifact, FeatureMatrixSnapshotV1):
        return _matrix_span(artifact)
    if not isinstance(artifact, ForecastFeatureSnapshotV1):
        raise ValueError("scores/labels are not training feature-row inputs")
    spans = [
        _matrix_span(artifact.inputs.features),
        _matrix_span(artifact.model.training_inputs.features),
    ]
    for inputs in (
        artifact.inputs.calendar_inputs,
        artifact.model.training_inputs.calendar_inputs,
    ):
        corpus = inputs.calendar
        spans.append((corpus.coverage_start_ns, corpus.coverage_end_ns))
    if artifact.predicted_consensus is not None:
        spans.append(_feature_span(artifact.predicted_consensus))
    return min(s[0] for s in spans), max(
        [s[1] for s in spans] + [artifact.generated_at_ns + 1]
    )


def verify_training_source(source: TrainingSourceV1) -> _VerifiedSource:
    """Verify actual bytes, closed readers and every committed observed anchor.

    This intentionally does not accept arbitrary ProviderAdapter protocols or
    caller-supplied verification receipts. Total declared work is checked before
    catalog hashing, frame reads or expensive committed-product replay.
    """
    if type(source) is not TrainingSourceV1:
        raise TypeError("training verification requires its typed source")
    raw_catalog = training_load(source.catalog_json)
    catalog = DatasetCatalog.from_dict(raw_catalog)
    if training_json(catalog.to_dict()) != training_json(raw_catalog):
        raise ValueError("catalog contains unknown or coercive wire fields")
    resolution = catalog.resolve(source.dataset_version_id)
    if resolution.dataset_version_id != source.dataset_version_id:
        raise ValueError("training requires an immutable dataset version")
    version = next(
        v
        for v in catalog.versions
        if v.dataset_version_id == source.dataset_version_id
    )
    if version.origin is not DatasetOrigin.OBSERVED or not version.partitions:
        raise ValueError("training anchor source must be an observed dataset")
    declared_rows = sum(p.row_count for p in version.partitions)
    declared_bytes = sum(p.artifact.size_bytes or 0 for p in version.partitions)
    declared_bytes += sum(
        a.size_bytes or 0 for a in version.qualification_evidence
    )
    manifests: list[tuple[str, Product, bytes]] = []
    for name in source.product_manifest_paths:
        data = read_training_regular(Path(name))
        manifest = load_reconstruction_manifest(name)
        if training_json(training_load(data.decode())) != training_json(
            manifest.to_dict()
        ):
            raise ValueError(
                "product manifest contains unknown or coercive wire fields"
            )
        declared_rows += manifest.event_count
        declared_bytes += sum(p.size_bytes for p in manifest.partitions)
        if isinstance(manifest, ReconstructionProductManifestV3):
            declared_bytes += sum(
                s.source_artifact.size_bytes or 0
                for s in manifest.observed_anchor_segments
            )
        manifests.append((name, manifest, data))
    for name in source.context_artifact_paths + source.derived_artifact_paths:
        declared_bytes += len(read_training_regular(Path(name)))
    if (
        declared_rows > MAX_TRAINING_SOURCE_ROWS
        or declared_bytes > MAX_TRAINING_SOURCE_BYTES
    ):
        raise ValueError(
            "total source verification exceeds training work budget"
        )
    if any(a.size_bytes is None for a in version.qualification_evidence) or any(
        p.artifact.size_bytes is None for p in version.partitions
    ):
        raise ValueError("all source artifacts require declared byte sizes")
    start = (
        min(p.coverage_start_ns for p in version.partitions) // DAY_NS * DAY_NS
    )
    end = (
        (max(p.coverage_end_ns for p in version.partitions) + DAY_NS - 1)
        // DAY_NS
        * DAY_NS
    )
    if (end - start) // DAY_NS > MAX_TRAINING_ITEMS:
        raise ValueError("evidence base-interval count exceeds bound")
    verification = catalog.verify(source.dataset_version_id)
    roots = [
        TrainingRootV1(
            "observed-dataset",
            version.dataset_version_id,
            _sha(verification.to_dict()),
            TrainingVerificationLevel.SOURCE_BYTES,
        )
    ]
    observed: list[_ObservedRow] = []
    for partition in version.partitions:
        adapter: HistDataProviderAdapter | FixtureProviderAdapter
        if partition.source_provider_id == "histdata.com":
            adapter = HistDataProviderAdapter()
        else:
            adapter = FixtureProviderAdapter(
                source_provider_id=partition.source_provider_id
            )
        if adapter.descriptor not in catalog.adapters:
            raise ValueError(
                "source adapter is unsupported or not exactly bound"
            )
        frame = adapter.read_partition(partition)
        if frame.height != partition.row_count:
            raise ValueError("observed source row count changed")
        projection = (
            partition.artifact.metadata.get("quote_order_projection_policy")
            == "rowwise-min-bid-max-ask-preserve-raw-v1"
        )
        for ordinal, row in enumerate(
            frame.select("datetime", "bid", "ask", "vol").iter_rows(), 1
        ):
            time, bid, ask, vol = row
            observed.append(
                _ObservedRow(
                    partition.symbol,
                    partition.series_id,
                    partition.period,
                    ordinal,
                    time * 1_000_000,
                    bid,
                    ask,
                    vol,
                    projection,
                )
            )
    observed.sort(
        key=lambda row: (row.symbol, row.event_time_ns, row.period, row.row_id)
    )
    start = min(
        start, min(row.event_time_ns for row in observed) // DAY_NS * DAY_NS
    )
    end = max(
        end, (max(row.event_time_ns for row in observed) // DAY_NS + 1) * DAY_NS
    )
    if (end - start) // DAY_NS > MAX_TRAINING_ITEMS:
        raise ValueError("actual evidence base-interval count exceeds bound")
    by_key = {row.key: row for row in observed}
    if len(by_key) != len(observed):
        raise ValueError("ambiguous observed row ownership")
    graph = tuple(sorted({p.symbol for p in version.partitions}))
    products: list[_VerifiedProduct] = []
    seen_products: set[str] = set()
    for name, manifest, original in manifests:
        if manifest.source.source_version_ids != (version.dataset_version_id,):
            raise ValueError(
                "product parent is not the verified observed version"
            )
        if not set(s.upper() for s in manifest.symbols) <= set(graph):
            raise ValueError("product graph is outside verified observed graph")
        if manifest.manifest_id in seen_products:
            raise ValueError(
                "duplicate product identity under different locators"
            )
        seen_products.add(manifest.manifest_id)
        streams = read_reconstruction_streams(name)
        if read_training_regular(Path(name)) != original:
            raise ValueError("product manifest changed during replay")
        events = tuple(e for stream in streams for e in stream.events)
        if len(events) != manifest.event_count or not events:
            raise ValueError("product row count differs or is empty")
        anchors: dict[str, SyntheticEventV1] = {}
        for event in events:
            if event.source_version_id != version.dataset_version_id:
                raise ValueError("event parent source differs")
            if event.origin.value != "observed":
                continue
            key = (
                event.source_series_id,
                event.source_period,
                event.source_row_id,
            )
            parent = by_key.get(key)  # type: ignore[arg-type]
            if parent is None:
                raise ValueError(
                    "product observed anchor has no exact source row"
                )
            bid, ask = (
                (min(parent.bid, parent.ask), max(parent.bid, parent.ask))
                if parent.quote_projection
                else (parent.bid, parent.ask)
            )
            if (
                event.symbol.upper(),
                event.event_time_ns,
                event.bid,
                event.ask,
            ) != (parent.symbol, parent.event_time_ns, bid, ask):
                raise ValueError(
                    "product observed anchor differs from source values"
                )
            if event.event_id in anchors:
                raise ValueError("product repeats an observed anchor")
            anchors[event.event_id] = event
        if not anchors:
            raise ValueError("reconstruction lacks verified observed anchors")
        for event in events:
            if event.origin.value == "observed":
                continue
            left = anchors.get(event.left_anchor_event_id or "")
            right = anchors.get(event.right_anchor_event_id or "")
            if (
                left is None
                or right is None
                or not (
                    left.symbol == event.symbol == right.symbol
                    and left.event_time_ns
                    < event.event_time_ns
                    < right.event_time_ns
                )
            ):
                raise ValueError(
                    "generated row lacks exact enclosing observed anchors"
                )
        lo, hi = (
            min(e.event_time_ns for e in events),
            max(e.event_time_ns for e in events) + 1,
        )
        if lo < start or hi > end:
            raise ValueError(
                "product dependency support escapes observed coverage"
            )
        root = TrainingRootV1(
            "committed-reconstruction",
            manifest.manifest_id,
            _sha(manifest.to_dict()),
            TrainingVerificationLevel.PRODUCT_REPLAY,
        )
        roots.append(root)
        products.append(_VerifiedProduct(manifest, events, root, lo, hi))
    contexts: list[_VerifiedContext] = []
    features: list[_VerifiedFeature] = []
    for name in source.context_artifact_paths + source.derived_artifact_paths:
        original = read_training_regular(Path(name))
        artifact = read_feature_artifact(name)
        if original != artifact.to_json().encode():
            raise ValueError(
                "derived feature artifact changed during verification"
            )
        artifact_data = artifact.to_dict()
        root = TrainingRootV1(
            str(artifact_data["contract_type"]),
            str(artifact_data["id"]),
            hashlib.sha256(original).hexdigest(),
            TrainingVerificationLevel.DERIVED_REPLAY,
        )
        roots.append(root)
        lo, hi = _feature_span(artifact)
        if lo < start or hi > end:
            raise ValueError(
                "feature dependency scope escapes observed coverage"
            )
        features.append(_VerifiedFeature(root, artifact, lo, hi))
        if name in source.context_artifact_paths:
            if not isinstance(artifact, FeatureMatrixSnapshotV1):
                raise ValueError(
                    "context ownership requires a complete feature matrix"
                )
            # Reference periods AND retained observation/schedule knowledge are
            # dependencies. No claim these normalized records verify raw releases.
            contexts.append(_VerifiedContext(root, lo, hi))
    # Adapters hash before parsing. Detect a source replacement during/after
    # those reads before returning values under the earlier verification root.
    if catalog.verify(source.dataset_version_id) != verification:
        raise ValueError("observed catalog verification changed during read")
    return _VerifiedSource(
        source,
        tuple(observed),
        tuple(products),
        tuple(contexts),
        tuple(features),
        _sorted_roots(tuple(roots)),
        graph,
        start,
        end,
    )


def _ownership(verified: _VerifiedSource) -> TrainingOwnershipV1:
    boundaries = set(range(verified.start_ns, verified.end_ns + 1, DAY_NS))
    for lo, hi in [
        (p.dependency_start_ns, p.dependency_end_ns) for p in verified.products
    ] + [(c.start_ns, c.end_ns) for c in verified.features]:
        # Remove every internal day boundary touched by a whole dependency.
        boundaries.difference_update(
            b for b in tuple(boundaries) if lo < b < hi
        )
    points = sorted(boundaries)
    intervals = list(zip(points, points[1:]))
    digests = [hashlib.sha256() for _ in intervals]
    import bisect  # pylint: disable=import-outside-toplevel

    for row in verified.observed:
        index = bisect.bisect_right(points, row.event_time_ns) - 1
        if not 0 <= index < len(intervals):
            raise ValueError("observed row is outside evidence coverage")
        digests[index].update(training_json(row.payload()).encode() + b"\n")
    units = tuple(
        TrainingEvidenceUnitV1(
            verified.source.dataset_version_id,
            lo,
            hi,
            verified.graph_symbols,
            digests[index].hexdigest(),
            tuple(
                sorted(
                    c.root.artifact_id
                    for c in verified.contexts
                    if lo <= c.start_ns and c.end_ns <= hi
                )
            ),
        )
        for index, (lo, hi) in enumerate(intervals)
    )
    # Derived adjuncts change row/batch lineage, never historical-unit ownership.
    return TrainingOwnershipV1(
        verified.source.dataset_version_id, units, verified.roots
    )


def build_training_ownership(source: TrainingSourceV1) -> TrainingOwnershipV1:
    """Freeze full-graph dependency closure before choosing rows or members."""
    return _ownership(verify_training_source(source))


def verify_training_ownership(
    source: TrainingSourceV1, ownership: TrainingOwnershipV1
) -> _VerifiedSource:
    """Reject later inventories that would silently redefine frozen ownership."""
    verified = verify_training_source(source)
    if _ownership(verified) != ownership:
        raise ValueError("source dependencies differ from frozen ownership map")
    return verified
