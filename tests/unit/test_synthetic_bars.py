"""Derived candlestick contracts and atomic publication regression tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from histdatacom.histdata_ascii import columns_for_timeframe
from histdatacom.synthetic import (
    DERIVED_BAR_ARROW_COLUMNS,
    STANDARD_DERIVED_BAR_INTERVALS,
    ActivitySliceScope,
    DerivedBarIntervalV1,
    DerivedBarPersistenceError,
    DerivedBarPolicyV1,
    DerivedBarProductManifestV1,
    DerivedBarV1,
    commit_derived_bar_publication,
    derive_reconstruction_bars,
    discover_derived_bar_manifests,
    iter_derived_bar_batches,
    publish_derived_bars,
    scan_derived_bars_polars,
    stage_derived_bar_publication,
    verify_derived_bar_publication,
)
from histdatacom.synthetic.contracts import (
    SYNTHETIC_EVENT_ARROW_COLUMNS,
    SyntheticEventStreamV1,
)
from histdatacom.synthetic.persistence import (
    PublishedReconstructionV1,
    publish_reconstruction_group,
)
from tests.unit.test_synthetic_contracts import (
    BASE_TIME_NS,
    _generated,
    _observed,
)
from tests.unit.test_synthetic_persistence import _publication_inputs

MINUTE_NS = STANDARD_DERIVED_BAR_INTERVALS["1m"]
ALIGNED_NS = (BASE_TIME_NS // MINUTE_NS) * MINUTE_NS


def _boundary_stream() -> SyntheticEventStreamV1:
    """Return observed and generated rows spanning an intentionally empty bin."""
    left = _observed(
        101,
        event_time_ns=ALIGNED_NS + 10_000_000_000,
        bid=1.1000,
    )
    right = _observed(
        102,
        event_time_ns=ALIGNED_NS + 180_000_000_000,
        bid=1.1020,
    )
    generated = replace(
        _generated(left, right),
        event_time_ns=ALIGNED_NS + 20_000_000_000,
        bid=1.1010,
        ask=1.1012,
        event_id="",
    )
    return SyntheticEventStreamV1.merge(
        run_id=left.run_id,
        ensemble_member_id=left.ensemble_member_id,
        symbol=left.symbol,
        observed_events=(right, left),
        synthetic_events=(generated,),
    )


def _all_scope_policy(*, max_bars: int = 100) -> DerivedBarPolicyV1:
    return DerivedBarPolicyV1(
        intervals=("1m",),
        scopes=("observed", "synthetic", "merged"),
        max_bars=max_bars,
    )


def _derive_boundary_bars(
    *,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> tuple[DerivedBarV1, ...]:
    stream = _boundary_stream()
    return derive_reconstruction_bars(
        stream.events,
        source_product_manifest_id="product-manifest:sha256:boundary",
        run_id=stream.run_id,
        ensemble_member_id=stream.ensemble_member_id,
        policy=_all_scope_policy(),
        start_ns=start_ns,
        end_ns=end_ns,
    )


def test_interval_policy_and_bar_contracts_round_trip() -> None:
    """Intervals, policy identity, and rows serialize without claim drift."""
    assert tuple(STANDARD_DERIVED_BAR_INTERVALS) == (
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1d",
    )
    interval = DerivedBarIntervalV1("5m")
    assert DerivedBarIntervalV1.from_dict(interval.to_dict()) == interval
    policy = DerivedBarPolicyV1(
        intervals=("1d", "1m", "1h"),
        scopes=("merged", "observed"),
    )
    assert policy.intervals == ("1m", "1h", "1d")
    assert DerivedBarPolicyV1.from_dict(policy.to_dict()) == policy
    assert policy.to_dict()["raw_m1_input"] is False
    assert policy.to_dict()["centralized_traded_volume_claim"] is False

    bar = _derive_boundary_bars()[0]
    assert DerivedBarV1.from_json(bar.to_json()) == bar
    assert tuple(bar.to_dict()) == DERIVED_BAR_ARROW_COLUMNS
    assert bar.to_dict()["volume"] is None
    assert bar.to_dict()["volume_state"] == "unavailable"
    assert bar.policy_id == _all_scope_policy().policy_id
    assert bar.rounding_digits == _all_scope_policy().rounding_digits

    stream = _boundary_stream()
    low_precision = derive_reconstruction_bars(
        stream.events,
        source_product_manifest_id="product-manifest:sha256:rounded",
        run_id=stream.run_id,
        ensemble_member_id=stream.ensemble_member_id,
        policy=DerivedBarPolicyV1(
            intervals=("1m",),
            rounding_digits=0,
        ),
    )
    assert low_precision
    assert all(item.rounding_digits == 0 for item in low_precision)

    with pytest.raises(ValueError, match="unsupported derived bar interval"):
        DerivedBarIntervalV1("2m")
    with pytest.raises(ValueError, match="epoch alignment"):
        DerivedBarIntervalV1("1m", alignment_epoch_ns=1)


def test_bar_contract_rejects_derived_claim_drift() -> None:
    """Derived arithmetic and boolean fields cannot be independently forged."""
    bar = next(
        item
        for item in _derive_boundary_bars()
        if item.scope is ActivitySliceScope.MERGED
        and item.bar_start_ns == ALIGNED_NS
    )
    with pytest.raises(ValueError, match="mid_open differs from bid/ask"):
        replace(bar, mid_open=bar.mid_open + 0.00001, bar_id="")
    assert bar.tick_intensity_per_second is not None
    with pytest.raises(ValueError, match="tick intensity differs"):
        replace(
            bar,
            tick_intensity_per_second=bar.tick_intensity_per_second * 2,
            bar_id="",
        )
    with pytest.raises(ValueError, match="stale rate differs"):
        replace(bar, stale_quote_rate=0.5, bar_id="")
    with pytest.raises(ValueError, match="transition support is incomplete"):
        replace(
            bar,
            transition_count=0,
            price_change_count=0,
            stale_quote_count=0,
            stale_quote_rate=None,
            bar_id="",
        )
    with pytest.raises(ValueError, match="endpoint identities"):
        replace(bar, last_event_id=bar.first_event_id, bar_id="")

    payload = bar.to_dict()
    payload["is_partial_start"] = 1
    payload["bar_id"] = ""
    with pytest.raises(ValueError, match="partial flags must be boolean"):
        DerivedBarV1.from_dict(payload)


def test_every_standard_interval_uses_half_open_utc_bins() -> None:
    """An event on an interval edge starts the next UTC-aligned bar."""
    for ordinal, (interval_code, duration) in enumerate(
        STANDARD_DERIVED_BAR_INTERVALS.items()
    ):
        start = (BASE_TIME_NS // duration) * duration
        left = _observed(301 + ordinal * 2, event_time_ns=start + duration - 1)
        right = _observed(302 + ordinal * 2, event_time_ns=start + duration)

        bars = derive_reconstruction_bars(
            (left, right),
            source_product_manifest_id="product-manifest:sha256:intervals",
            run_id=left.run_id,
            ensemble_member_id=left.ensemble_member_id,
            policy=DerivedBarPolicyV1(intervals=(interval_code,)),
        )

        assert tuple(item.bar_start_ns for item in bars) == (
            start,
            start + duration,
        )
        assert all(
            item.bar_end_ns - item.bar_start_ns == duration for item in bars
        )


def test_scopes_ohlc_empty_bins_and_boundary_carry_are_explicit() -> None:
    """Bars preserve scopes, omit empty bins, and carry transitions forward."""
    bars = _derive_boundary_bars()
    keyed = {(bar.scope, bar.bar_start_ns): bar for bar in bars}

    first_merged = keyed[(ActivitySliceScope.MERGED, ALIGNED_NS)]
    assert first_merged.event_count == 2
    assert first_merged.observed_event_count == 1
    assert first_merged.synthetic_event_count == 1
    assert first_merged.bid_open == 1.1
    assert first_merged.bid_high == 1.101
    assert first_merged.bid_close == 1.101
    assert first_merged.ask_close == 1.1012
    assert first_merged.mid_close == 1.1011
    assert first_merged.spread_close == pytest.approx(0.0002)
    assert first_merged.transition_count == 1
    assert first_merged.price_change_count == 1

    last_merged = keyed[(ActivitySliceScope.MERGED, ALIGNED_NS + 3 * MINUTE_NS)]
    assert last_merged.event_count == 1
    assert last_merged.transition_count == 1
    assert last_merged.price_change_count == 1
    assert last_merged.activity_duration_ns == 0
    assert last_merged.tick_intensity_per_second is None
    assert last_merged.stale_quote_rate == 0.0

    merged_starts = {
        bar.bar_start_ns
        for bar in bars
        if bar.scope is ActivitySliceScope.MERGED
    }
    assert merged_starts == {ALIGNED_NS, ALIGNED_NS + 3 * MINUTE_NS}
    assert (ActivitySliceScope.SYNTHETIC, ALIGNED_NS) in keyed
    assert (
        ActivitySliceScope.SYNTHETIC,
        ALIGNED_NS + 3 * MINUTE_NS,
    ) not in keyed


def test_query_boundaries_flag_partial_bars_without_filling() -> None:
    """Explicit range cuts flag edge bars and never synthesize empty rows."""
    bars = _derive_boundary_bars(
        start_ns=ALIGNED_NS + 15_000_000_000,
        end_ns=ALIGNED_NS + 185_000_000_000,
    )
    merged = [bar for bar in bars if bar.scope is ActivitySliceScope.MERGED]

    assert len(merged) == 2
    assert merged[0].is_partial_start
    assert not merged[0].is_partial_end
    assert not merged[1].is_partial_start
    assert merged[1].is_partial_end
    assert merged[0].first_event_time_ns == ALIGNED_NS + 20_000_000_000
    assert merged[0].transition_count == 0


def test_duplicate_timestamps_use_sequence_and_unordered_rows_fail() -> None:
    """Within-time ordering is stable and reversed positions fail closed."""
    first = _observed(
        201,
        event_time_ns=ALIGNED_NS + 1,
        event_sequence=0,
        bid=1.2,
    )
    second = _observed(
        202,
        event_time_ns=ALIGNED_NS + 1,
        event_sequence=1,
        bid=1.3,
    )
    common = {
        "source_product_manifest_id": "product-manifest:sha256:sequence",
        "run_id": first.run_id,
        "ensemble_member_id": first.ensemble_member_id,
        "policy": DerivedBarPolicyV1(intervals=("1m",)),
    }

    bar = derive_reconstruction_bars((first, second), **common)[0]
    assert bar.bid_open == 1.2
    assert bar.bid_close == 1.3
    assert bar.transition_count == 1

    with pytest.raises(ValueError, match="strictly ordered"):
        derive_reconstruction_bars((second, first), **common)


def test_resource_and_provenance_limits_refuse() -> None:
    """Bar count and lineage state are explicitly bounded."""
    stream = _boundary_stream()
    with pytest.raises(ValueError, match="output exceeds policy"):
        derive_reconstruction_bars(
            stream.events,
            source_product_manifest_id="product-manifest:sha256:limit",
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            policy=_all_scope_policy(max_bars=1),
        )

    second_source = replace(
        stream.events[1],
        source_version_id="source-artifact:sha256:second",
        event_id="",
    )
    events = (stream.events[0], second_source)
    with pytest.raises(ValueError, match="source_version_ids exceeds"):
        derive_reconstruction_bars(
            events,
            source_product_manifest_id="product-manifest:sha256:lineage",
            run_id=stream.run_id,
            ensemble_member_id=stream.ensemble_member_id,
            policy=DerivedBarPolicyV1(
                intervals=("1m",),
                max_provenance_values=1,
            ),
        )


def test_atomic_publication_round_trip_and_idempotent_commit(
    tmp_path: Path,
) -> None:
    """Scratch is invisible and one rename publishes exact narrow bars."""
    source = _publish_source(tmp_path)
    root = tmp_path / "bars"
    policy = DerivedBarPolicyV1(
        intervals=("1m", "5m"),
        scopes=("observed", "synthetic", "merged"),
    )

    staged = stage_derived_bar_publication(
        root,
        source.manifest_path,
        policy=policy,
        start_ns=source.manifest.min_event_time_ns,
        end_ns=source.manifest.max_event_time_ns + 1,
        batch_size=1,
        row_group_size=2,
        write_buffer_rows=1,
    )
    assert not discover_derived_bar_manifests(root)
    committed = commit_derived_bar_publication(staged)
    manifest = verify_derived_bar_publication(committed.manifest_path)

    assert DerivedBarProductManifestV1.from_json(manifest.to_json()) == manifest
    assert manifest.source_product_manifest_id == source.manifest.manifest_id
    assert manifest.query_start_ns == source.manifest.min_event_time_ns
    assert manifest.query_end_ns == source.manifest.max_event_time_ns + 1
    assert manifest.bar_count == sum(
        partition.row_count for partition in manifest.partitions
    )
    assert manifest.to_dict()["event_rows_inline"] is False
    assert manifest.to_dict()["analytical_frame_columns_inline"] is False
    assert discover_derived_bar_manifests(root) == (committed.manifest_path,)
    for partition in manifest.partitions:
        table = pq.read_table(
            committed.manifest_path.parent / partition.relative_path
        )
        assert table.schema.names == list(DERIVED_BAR_ARROW_COLUMNS)
        assert table.num_columns == 64
        assert table.num_columns < 521
    assert len(SYNTHETIC_EVENT_ARROW_COLUMNS) == 26
    assert "volume" not in SYNTHETIC_EVENT_ARROW_COLUMNS

    retry = commit_derived_bar_publication(staged)
    assert retry.idempotent_retry
    assert retry.manifest == manifest


def test_batch_and_buffer_boundaries_preserve_logical_bars(
    tmp_path: Path,
) -> None:
    """Input and output chunk sizes cannot change logical bar identities."""
    source = _publish_source(tmp_path)
    policy = DerivedBarPolicyV1(
        intervals=("1m", "5m"),
        scopes=("merged",),
    )
    first = publish_derived_bars(
        tmp_path / "bars-a",
        source.manifest_path,
        policy=policy,
        batch_size=1,
        row_group_size=2,
        write_buffer_rows=1,
    )
    second = publish_derived_bars(
        tmp_path / "bars-b",
        source.manifest_path,
        policy=policy,
        batch_size=3,
        row_group_size=3,
        write_buffer_rows=3,
    )

    assert first.manifest.logical_content_sha256 == (
        second.manifest.logical_content_sha256
    )
    assert _bar_ids(first.manifest_path) == _bar_ids(second.manifest_path)
    assert first.manifest.publication_id == second.manifest.publication_id


def test_projection_scans_and_raw_m1_remains_rejected(tmp_path: Path) -> None:
    """Readers prune the 64-column product while raw M1 stays unsupported."""
    source = _publish_source(tmp_path)
    bars = publish_derived_bars(
        tmp_path / "bars",
        source.manifest_path,
        policy=DerivedBarPolicyV1(intervals=("1m",)),
        batch_size=1,
    )
    batches = tuple(
        iter_derived_bar_batches(
            bars.manifest_path,
            columns=("symbol", "bar_start_ns", "mid_close"),
            symbols=("eurusd",),
            intervals=("1m",),
            batch_size=1,
        )
    )
    assert batches
    assert all(
        batch.schema.names == ["symbol", "bar_start_ns", "mid_close"]
        for batch in batches
    )
    lazy = scan_derived_bars_polars(
        bars.manifest_path,
        columns=("symbol", "bar_start_ns", "mid_close"),
        symbols=("eurusd",),
    )
    assert lazy.collect().columns == ["symbol", "bar_start_ns", "mid_close"]
    assert "PROJECT 3/64 COLUMNS" in lazy.explain(optimized=True)

    eurusd_partition = next(
        item
        for item in bars.manifest.partitions
        if item.symbol == "eurusd" and item.interval_code == "1m"
    )
    overlap_start = eurusd_partition.min_bar_start_ns + 1
    overlap = tuple(
        iter_derived_bar_batches(
            bars.manifest_path,
            columns=("bar_start_ns", "bar_end_ns"),
            symbols=("eurusd",),
            intervals=("1m",),
            start_ns=overlap_start,
            end_ns=overlap_start + 1,
        )
    )
    assert overlap
    assert overlap[0].to_pylist()[0]["bar_start_ns"] == (
        eurusd_partition.min_bar_start_ns
    )

    with pytest.raises(ValueError, match="symbols are outside"):
        tuple(iter_derived_bar_batches(bars.manifest_path, symbols=("xauusd",)))
    with pytest.raises(ValueError, match="scopes are outside"):
        scan_derived_bars_polars(
            bars.manifest_path,
            scopes=(ActivitySliceScope.OBSERVED,),
        )
    with pytest.raises(ValueError, match="intervals are outside"):
        tuple(iter_derived_bar_batches(bars.manifest_path, intervals=("5m",)))

    with pytest.raises(ValueError, match="unsupported ASCII timeframe"):
        columns_for_timeframe("M1")


def test_failed_stage_releases_writers_and_removes_scratch(
    tmp_path: Path,
) -> None:
    """A bounded aggregation failure leaves no open or discoverable product."""
    source = _publish_source(tmp_path)
    root = tmp_path / "bars"

    with pytest.raises(ValueError, match="output exceeds policy"):
        stage_derived_bar_publication(
            root,
            source.manifest_path,
            policy=DerivedBarPolicyV1(intervals=("1m",), max_bars=1),
            batch_size=1,
            write_buffer_rows=1,
        )

    assert not discover_derived_bar_manifests(root)
    assert not tuple(root.rglob("publication.tmp-*"))


def test_partition_and_manifest_tampering_fail_closed(tmp_path: Path) -> None:
    """Truncated Parquet and serialized claim drift cannot pass verification."""
    source = _publish_source(tmp_path)
    bars = publish_derived_bars(
        tmp_path / "bars",
        source.manifest_path,
        policy=DerivedBarPolicyV1(intervals=("1m",)),
    )
    manifest = bars.manifest
    payload = manifest.to_dict()
    payload["raw_m1_input"] = True
    with pytest.raises(ValueError, match="raw_m1_input differs"):
        DerivedBarProductManifestV1.from_dict(payload)
    payload = manifest.to_dict()
    payload["query_start_ns"] = True
    with pytest.raises(ValueError, match="must be an integer"):
        DerivedBarProductManifestV1.from_dict(payload)
    with pytest.raises(ValueError, match="duplicate partitions"):
        replace(
            manifest,
            partitions=manifest.partitions + (manifest.partitions[0],),
            publication_id="",
            manifest_id="",
        )
    with pytest.raises(ValueError, match="cannot be empty"):
        replace(manifest.partitions[0], size_bytes=0, partition_id="")

    partition = manifest.partitions[0]
    path = bars.manifest_path.parent / partition.relative_path
    path.write_bytes(path.read_bytes()[:32])
    with pytest.raises(DerivedBarPersistenceError, match="size differs"):
        verify_derived_bar_publication(bars.manifest_path)


def test_unexpected_artifacts_are_not_accepted(tmp_path: Path) -> None:
    """Committed directories are confined to manifest-declared artifacts."""
    source = _publish_source(tmp_path)
    bars = publish_derived_bars(
        tmp_path / "bars",
        source.manifest_path,
        policy=DerivedBarPolicyV1(intervals=("1m",)),
    )
    empty = bars.manifest_path.parent / "unexpected-directory"
    empty.mkdir()
    with pytest.raises(
        DerivedBarPersistenceError, match="artifact set differs"
    ):
        verify_derived_bar_publication(bars.manifest_path)
    empty.rmdir()

    extra = bars.manifest_path.parent / "unexpected.csv"
    extra.write_text("not part of the product", encoding="utf-8")

    with pytest.raises(
        DerivedBarPersistenceError, match="artifact set differs"
    ):
        verify_derived_bar_publication(bars.manifest_path)


def test_symlink_artifacts_are_not_accepted(tmp_path: Path) -> None:
    """Publication verification refuses even undeclared readable symlinks."""
    source = _publish_source(tmp_path)
    bars = publish_derived_bars(
        tmp_path / "bars",
        source.manifest_path,
        policy=DerivedBarPolicyV1(intervals=("1m",)),
    )
    target = tmp_path / "outside.txt"
    target.write_text("outside product", encoding="utf-8")
    link = bars.manifest_path.parent / "linked.txt"
    try:
        link.symlink_to(target)
    except OSError as err:  # pragma: no cover - platform policy
        pytest.skip(f"symlink creation is unavailable: {err}")

    with pytest.raises(DerivedBarPersistenceError, match="unsafe symlink"):
        verify_derived_bar_publication(bars.manifest_path)


def test_derived_bar_contract_is_documented() -> None:
    """README and detailed docs preserve the product-boundary decisions."""
    root = Path(__file__).parents[2]
    readme = (root / "README.md").read_text(encoding="utf-8")
    contract = (root / "docs/derived-bar-contracts.md").read_text(
        encoding="utf-8"
    )

    assert "Derived Reconstruction Candlesticks" in readme
    assert "publish_derived_bars()" in readme
    assert "verified" in readme
    assert "committed reconstruction manifest" in readme
    assert "Empty bins emit no rows" in contract
    assert "centralized_traded_volume_claim=false" in contract
    assert "exact 64-column Arrow schema" in contract
    assert "Raw `ascii/T`" in contract
    assert "caches" in contract


def _publish_source(tmp_path: Path) -> PublishedReconstructionV1:
    rendered, anchors, storage, retention = _publication_inputs(tmp_path)
    return publish_reconstruction_group(
        tmp_path / "events",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="eurusd-triangle",
        retention_plan=retention,
        storage_policy=storage,
        row_group_size=2,
    )


def _bar_ids(manifest_path: Path) -> tuple[str, ...]:
    return tuple(
        str(row["bar_id"])
        for batch in iter_derived_bar_batches(
            manifest_path,
            columns=("bar_id",),
        )
        for row in batch.to_pylist()
    )
