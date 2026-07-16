"""Build and qualify the production modern empirical motif library.

The builder consumes only immutable monthly Arrow caches whose digests are
declared by the stable v2 feed-epoch evidence.  It keeps dense rows in memory,
publishes compact lineage and aggregate coverage only, removes train shapes
that occur in any later split, and qualifies the retained train index against
the frozen real reverse-degradation campaign.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import sys
import time
from typing import Any

from histdatacom.data_analytics.feed_epochs_v2 import (
    read_active_time_feed_epoch_definition,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark_corpus import (
    PREDECLARED_GATE_COMMIT,
    ReverseDegradationBenchmarkCampaignV1,
    read_reverse_degradation_benchmark_corpus,
    run_reverse_degradation_benchmark_campaign,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import (
    REFERENCE_MOTIF_FEATURE_SCHEMA_VERSION,
    ReferenceMotifConditionV1,
    ReferenceMotifIndexConfigV1,
    ReferenceMotifIndexV1,
    ReferenceMotifQueryStatus,
    ReferenceMotifQueryV1,
    ReferenceMotifSourceEventV1,
    ReferenceMotifSourceWindowV1,
    ReferenceMotifSplitKind,
    ReferenceMotifSplitV1,
    ReferenceMotifTransformPolicyV1,
    build_reference_motif_index,
    extract_reference_motif_fragment,
    query_reference_motifs,
    reference_motif_condition_from_quotes,
)

MODERN_REFERENCE_MOTIF_PROFILE_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-profile.v1"
)
MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-manifest.v1"
)
MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-leakage-audit.v1"
)
MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-coverage.v1"
)
MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-qualification.v1"
)
MODERN_REFERENCE_MOTIF_RESOURCE_SCHEMA_VERSION = (
    "histdatacom.modern-reference-motif-resource-audit.v1"
)

NANOSECONDS_PER_SECOND = 1_000_000_000
NANOSECONDS_PER_MILLISECOND = 1_000_000
DEFAULT_MODERN_MOTIF_SPLIT_PERIODS = {
    "train": ("201901", "202001", "202101", "202201", "202301"),
    "calibration": ("202307",),
    "validation": ("202401",),
    "final_holdout": ("202510",),
}
_EXPECTED_SPLITS = ("train", "calibration", "validation", "final_holdout")
_PERIOD_RE = re.compile(r"^\d{6}$")
_CONTENT_ADDRESS_RE = re.compile(r"^([a-z0-9-]+)-([0-9a-f]{64})\.json$")


def _default_split_periods() -> dict[str, tuple[str, ...]]:
    return dict(DEFAULT_MODERN_MOTIF_SPLIT_PERIODS)


@dataclass(frozen=True, slots=True)
class ModernReferenceMotifProfileV1:
    """Bounded source selection and production-index policy."""

    symbols: tuple[str, ...] = ("EURGBP", "EURUSD", "GBPUSD")
    split_periods: Mapping[str, tuple[str, ...]] = field(
        default_factory=_default_split_periods
    )
    synchronized_windows_per_period: int = 6
    window_duration_seconds: int = 600
    minimum_events_per_symbol: int = 32
    max_events_per_symbol: int = 96
    fragment_event_count: int = 3
    max_fragments: int = 256
    max_matches: int = 64
    neighbor_guard_seconds: int = 1800
    max_source_bytes: int = 2 * 1024**3
    max_runtime_seconds: float = 900.0
    max_peak_memory_bytes: int = 2 * 1024**3
    max_artifact_bytes: int = 64 * 1024**2
    profile_id: str = ""
    schema_version: str = MODERN_REFERENCE_MOTIF_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MODERN_REFERENCE_MOTIF_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported modern motif profile schema")
        symbols = tuple(sorted({str(item).upper() for item in self.symbols}))
        if not symbols or any(
            not re.fullmatch(r"[A-Z]{6}", item) for item in symbols
        ):
            raise ValueError("modern motif symbols must be six-letter pairs")
        object.__setattr__(self, "symbols", symbols)
        periods = {
            str(name): tuple(str(value) for value in values)
            for name, values in self.split_periods.items()
        }
        if tuple(periods) != _EXPECTED_SPLITS:
            raise ValueError(
                "modern motif periods must declare ordered split keys"
            )
        flattened: list[str] = []
        for name in _EXPECTED_SPLITS:
            values = periods[name]
            if not values or tuple(sorted(values)) != values:
                raise ValueError(f"modern motif {name} periods must be sorted")
            if any(_PERIOD_RE.fullmatch(value) is None for value in values):
                raise ValueError("modern motif periods must use YYYYMM")
            flattened.extend(values)
        if len(set(flattened)) != len(flattened) or flattened != sorted(
            flattened
        ):
            raise ValueError("modern motif split periods overlap or regress")
        object.__setattr__(self, "split_periods", periods)
        for name, minimum, maximum in (
            ("synchronized_windows_per_period", 1, 64),
            ("window_duration_seconds", 1, 86_400),
            ("minimum_events_per_symbol", 3, 4096),
            ("max_events_per_symbol", 3, 4096),
            ("fragment_event_count", 3, 64),
            ("max_fragments", 1, 4096),
            ("max_matches", 1, 128),
            ("neighbor_guard_seconds", 0, 86_400),
            ("max_source_bytes", 1, 2**63 - 1),
            ("max_peak_memory_bytes", 1, 2**63 - 1),
            ("max_artifact_bytes", 1024, 256 * 1024**2),
        ):
            selected = int(getattr(self, name))
            if not minimum <= selected <= maximum:
                raise ValueError(f"modern motif {name} is outside limits")
            object.__setattr__(self, name, selected)
        if self.max_events_per_symbol < self.minimum_events_per_symbol:
            raise ValueError("modern motif event maximum is below minimum")
        runtime = float(self.max_runtime_seconds)
        if not 0.0 < runtime < float("inf"):
            raise ValueError("modern motif runtime bound must be positive")
        object.__setattr__(self, "max_runtime_seconds", runtime)
        expected = _stable_id(
            "modern-reference-motif-profile", self.identity_payload()
        )
        if self.profile_id and self.profile_id != expected:
            raise ValueError("modern motif profile_id differs")
        object.__setattr__(self, "profile_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbols": list(self.symbols),
            "split_periods": {
                name: list(self.split_periods[name])
                for name in _EXPECTED_SPLITS
            },
            "synchronized_windows_per_period": self.synchronized_windows_per_period,
            "window_duration_seconds": self.window_duration_seconds,
            "minimum_events_per_symbol": self.minimum_events_per_symbol,
            "max_events_per_symbol": self.max_events_per_symbol,
            "fragment_event_count": self.fragment_event_count,
            "max_fragments": self.max_fragments,
            "max_matches": self.max_matches,
            "neighbor_guard_seconds": self.neighbor_guard_seconds,
            "max_source_bytes": self.max_source_bytes,
            "max_runtime_seconds": self.max_runtime_seconds,
            "max_peak_memory_bytes": self.max_peak_memory_bytes,
            "max_artifact_bytes": self.max_artifact_bytes,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ModernReferenceMotifProfileV1":
        split_periods = _mapping(data.get("split_periods"))
        return cls(
            symbols=tuple(str(item) for item in _sequence(data.get("symbols"))),
            split_periods={
                name: tuple(
                    str(item) for item in _sequence(split_periods.get(name))
                )
                for name in _EXPECTED_SPLITS
            },
            synchronized_windows_per_period=int(
                data.get("synchronized_windows_per_period", 0)
            ),
            window_duration_seconds=int(data.get("window_duration_seconds", 0)),
            minimum_events_per_symbol=int(
                data.get("minimum_events_per_symbol", 0)
            ),
            max_events_per_symbol=int(data.get("max_events_per_symbol", 0)),
            fragment_event_count=int(data.get("fragment_event_count", 0)),
            max_fragments=int(data.get("max_fragments", 0)),
            max_matches=int(data.get("max_matches", 0)),
            neighbor_guard_seconds=int(data.get("neighbor_guard_seconds", 0)),
            max_source_bytes=int(data.get("max_source_bytes", 0)),
            max_runtime_seconds=float(data.get("max_runtime_seconds", 0.0)),
            max_peak_memory_bytes=int(data.get("max_peak_memory_bytes", 0)),
            max_artifact_bytes=int(data.get("max_artifact_bytes", 0)),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ModernReferenceMotifBuildV1:
    """In-memory build result before atomic content-addressed persistence."""

    profile: ModernReferenceMotifProfileV1
    index: ReferenceMotifIndexV1
    library_id: str
    manifest: Mapping[str, Any]
    leakage_audit: Mapping[str, Any]
    coverage: Mapping[str, Any]
    qualification: Mapping[str, Any]
    resource_audit: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _TickRow:
    row_id: int
    timestamp_ms: int
    bid: float
    ask: float


def build_modern_reference_motif_library(
    source_root: str | Path,
    *,
    feed_epoch_definition_path: str | Path,
    market_context_corpus_path: str | Path,
    cftc_positioning_corpus_path: str | Path,
    benchmark_manifest_path: str | Path,
    profile: ModernReferenceMotifProfileV1 | None = None,
) -> ModernReferenceMotifBuildV1:
    """Build, leakage-audit, replay, and qualify the real modern library."""
    from histdatacom.market_context import (  # pylint: disable=import-outside-toplevel
        CftcReportFamily,
        CftcReportScope,
        MarketContextView,
        cftc_positioning_state_label,
        market_context_benchmark_event_state,
        query_cftc_positioning_corpus,
        query_market_context_corpus,
        read_cftc_positioning_corpus,
        read_market_context_corpus,
    )

    selected = profile or ModernReferenceMotifProfileV1()
    started = time.monotonic()
    root = Path(source_root).expanduser().resolve()
    definition_path = Path(feed_epoch_definition_path).expanduser().resolve()
    context_path = Path(market_context_corpus_path).expanduser().resolve()
    positioning_path = Path(cftc_positioning_corpus_path).expanduser().resolve()
    benchmark_path = Path(benchmark_manifest_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("modern motif source root is not a directory")
    definition = read_active_time_feed_epoch_definition(definition_path)
    if not definition.valid_for_observation_models:
        raise ValueError("modern motif feed epoch definition is not stable")
    definition_payload = _read_json_mapping(
        definition_path, selected.max_artifact_bytes
    )
    if definition_payload.get("definition_id") != definition.definition_id:
        raise ValueError("modern motif definition identity differs on restore")
    modern_epochs = [
        item
        for item in definition.epochs
        if item.label == "technology_epoch_04"
    ]
    if len(modern_epochs) != 1:
        raise ValueError("modern motif build requires technology_epoch_04")
    modern_epoch = modern_epochs[0]
    all_periods = tuple(
        period
        for name in _EXPECTED_SPLITS
        for period in selected.split_periods[name]
    )
    if any(
        not modern_epoch.period_start <= period <= modern_epoch.period_end
        for period in all_periods
    ):
        raise ValueError(
            "modern motif source period falls outside stable epoch 04"
        )

    context_corpus = read_market_context_corpus(context_path)
    positioning_corpus = read_cftc_positioning_corpus(positioning_path)
    source_lineage = {
        (str(item["period"]), str(item["symbol"])): item
        for item in _sequence(
            _mapping(definition_payload.get("lineage")).get("sources")
        )
        if isinstance(item, Mapping)
    }
    split_by_period = {
        period: ReferenceMotifSplitKind(name)
        for name in _EXPECTED_SPLITS
        for period in selected.split_periods[name]
    }
    transform_policy = ReferenceMotifTransformPolicyV1(
        min_time_scale=0.5,
        max_time_scale=2.0,
        min_price_scale=0.05,
        max_price_scale=1.0,
        max_time_warp_ratio=1.25,
    )
    index_config = ReferenceMotifIndexConfigV1(
        min_events_per_fragment=selected.fragment_event_count,
        max_events_per_fragment=selected.fragment_event_count,
        max_source_windows=10_000,
        max_fragments=selected.max_fragments,
        min_cell_support=1,
        max_matches=selected.max_matches,
        source_overlap_guard_ns=(
            selected.neighbor_guard_seconds * NANOSECONDS_PER_SECOND
        ),
        near_duplicate_rounding_digits=4,
        max_artifact_bytes=min(selected.max_artifact_bytes, 256 * 1024**2),
    )

    source_partitions: list[dict[str, Any]] = []
    source_artifacts: dict[tuple[str, str], ArtifactRef] = {}
    source_rows: dict[tuple[str, str, int], tuple[_TickRow, ...]] = {}
    interval_evidence: dict[
        tuple[str, int], tuple[int, int, str, tuple[str, ...]]
    ] = {}
    source_bytes = 0
    for period in all_periods:
        candidates = _candidate_intervals(
            period,
            duration_seconds=selected.window_duration_seconds,
            context_event_times=tuple(
                event.event_time_ns
                for event in context_corpus.timeline.events
                if _period_for_ns(event.event_time_ns) == period
            ),
        )
        accepted = 0
        for start_ns, end_ns, session in candidates:
            rows_by_symbol: dict[str, tuple[_TickRow, ...]] = {}
            for symbol in selected.symbols:
                path = root / _relative_source_path(symbol, period)
                rows_by_symbol[symbol] = _read_arrow_interval(
                    path,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    maximum=selected.max_events_per_symbol,
                )
            if any(
                len(rows) < selected.minimum_events_per_symbol
                for rows in rows_by_symbol.values()
            ):
                continue
            context_query = query_market_context_corpus(
                context_corpus,
                start_ns=start_ns,
                end_ns=end_ns,
                view=MarketContextView.EX_ANTE,
                as_of_ns=start_ns,
                symbols=selected.symbols,
                include_calendar=True,
                max_events=64,
                require_supported=False,
            )
            positioning_query = query_cftc_positioning_corpus(
                positioning_corpus,
                start_ns=start_ns,
                end_ns=end_ns,
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                symbols=selected.symbols,
                report_families=(CftcReportFamily.LEGACY,),
                report_scopes=(CftcReportScope.FUTURES_ONLY,),
            )
            tags = (
                market_context_benchmark_event_state(context_query),
                cftc_positioning_state_label(positioning_query),
            )
            interval_evidence[(period, accepted)] = (
                start_ns,
                end_ns,
                session,
                tags,
            )
            for symbol, rows in rows_by_symbol.items():
                source_rows[(period, symbol, accepted)] = rows
            accepted += 1
            if accepted == selected.synchronized_windows_per_period:
                break
        if accepted != selected.synchronized_windows_per_period:
            raise ValueError(
                f"only {accepted} synchronized windows qualify for {period}"
            )
        _enforce_runtime(started, selected.max_runtime_seconds)

    for period in all_periods:
        for symbol in selected.symbols:
            relative = _relative_source_path(symbol, period)
            path = root / relative
            lineage = source_lineage.get((period, symbol))
            if lineage is None:
                raise ValueError(f"feed epoch lineage omits {symbol} {period}")
            size = path.stat().st_size
            source_bytes += size
            if source_bytes > selected.max_source_bytes:
                raise ValueError("modern motif sources exceed byte bound")
            digest = _file_sha256(path)
            expected = str(
                lineage.get("source_artifact_sha256", "")
            ).removeprefix("sha256:")
            if digest != expected:
                raise ValueError(
                    f"modern motif source hash differs for {symbol} {period}"
                )
            row_count = _arrow_row_count(path)
            artifact = ArtifactRef(
                kind="histdata_ascii_tick_arrow",
                path=relative.as_posix(),
                size_bytes=size,
                sha256=digest,
                metadata={
                    "symbol": symbol,
                    "period": period,
                    "evidence_id": str(lineage.get("evidence_id", "")),
                },
            )
            source_artifacts[(period, symbol)] = artifact
            source_partitions.append(
                {
                    "symbol": symbol,
                    "period": period,
                    "split_kind": split_by_period[period].value,
                    "relative_path": relative.as_posix(),
                    "size_bytes": size,
                    "row_count": row_count,
                    "sha256": digest,
                    "feed_epoch_evidence_id": str(
                        lineage.get("evidence_id", "")
                    ),
                    "selected_parent_window_count": selected.synchronized_windows_per_period,
                }
            )

    windows: list[ReferenceMotifSourceWindowV1] = []
    for period in all_periods:
        split_kind = split_by_period[period]
        for interval_number in range(selected.synchronized_windows_per_period):
            start_ns, end_ns, session, event_tags = interval_evidence[
                (period, interval_number)
            ]
            assignment = definition.assign(
                symbol=selected.symbols[0],
                timestamp_utc_ms=((start_ns + end_ns) // 2)
                // NANOSECONDS_PER_MILLISECOND,
            )
            if assignment.label != modern_epoch.label:
                raise ValueError(
                    "selected modern motif window is outside epoch 04"
                )
            for symbol in selected.symbols:
                rows = source_rows[(period, symbol, interval_number)]
                for chunk_number, offset in enumerate(
                    range(
                        0,
                        len(rows) - selected.fragment_event_count + 1,
                        selected.fragment_event_count,
                    )
                ):
                    chunk = rows[
                        offset : offset + selected.fragment_event_count
                    ]
                    if len(chunk) != selected.fragment_event_count:
                        continue
                    event_times = tuple(
                        item.timestamp_ms * NANOSECONDS_PER_MILLISECOND
                        for item in chunk
                    )
                    condition = reference_motif_condition_from_quotes(
                        symbol=symbol,
                        feed_epoch_id=modern_epoch.label,
                        session_state=session,
                        event_times_ns=event_times,
                        bids=tuple(item.bid for item in chunk),
                        asks=tuple(item.ask for item in chunk),
                        event_tags=event_tags,
                    )
                    events = tuple(
                        ReferenceMotifSourceEventV1(
                            event_time_ns=item.timestamp_ms
                            * NANOSECONDS_PER_MILLISECOND,
                            event_sequence=item.row_id,
                            bid=item.bid,
                            ask=item.ask,
                            source_row_id=item.row_id,
                        )
                        for item in chunk
                    )
                    windows.append(
                        ReferenceMotifSourceWindowV1(
                            source_series_id=(
                                f"ascii:T:{symbol}:histdata.com:{period}:"
                                f"window-{interval_number:02d}:chunk-{chunk_number:02d}"
                            ),
                            period=period,
                            source_artifact=source_artifacts[(period, symbol)],
                            split_kind=split_kind,
                            condition=condition,
                            events=events,
                            first_known_at_ns=event_times[-1],
                            available_at_ns=event_times[-1],
                            transform_policy=transform_policy,
                        )
                    )
    if len(windows) > index_config.max_source_windows:
        raise ValueError("modern motif source-window count exceeds index bound")
    projected = {
        item.source_window_id: extract_reference_motif_fragment(
            item, config=index_config
        )
        for item in windows
    }
    withheld_signatures = {
        item.near_duplicate_signature
        for item in projected.values()
        if item.split_kind is not ReferenceMotifSplitKind.TRAIN
    }
    excluded_train = tuple(
        sorted(
            item.source_window_id
            for item in windows
            if item.split_kind is ReferenceMotifSplitKind.TRAIN
            and projected[item.source_window_id].near_duplicate_signature
            in withheld_signatures
        )
    )
    excluded_ids = set(excluded_train)
    filtered = tuple(
        item for item in windows if item.source_window_id not in excluded_ids
    )
    splits = _reference_splits(selected)
    index = build_reference_motif_index(
        filtered, splits=splits, config=index_config
    )
    if not index.fragments:
        raise ValueError("modern motif production index is empty")

    coverage = _coverage_payload(index, filtered, excluded_train)
    unsupported_condition = reference_motif_condition_from_quotes(
        symbol=index.fragments[0].condition.symbol,
        feed_epoch_id="unsupported-technology-epoch",
        session_state=index.fragments[0].condition.session_state,
        event_times_ns=(1_700_000_000_000_000_000, 1_700_000_001_000_000_000),
        bids=(1.0, 1.0001),
        asks=(1.0002, 1.0003),
    )
    refusal = query_reference_motifs(
        index,
        ReferenceMotifQueryV1(
            condition=unsupported_condition,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            used_at_ns=1_700_000_002_000_000_000,
            max_results=selected.max_matches,
            max_distance=0.0,
        ),
    )
    if refusal.status is ReferenceMotifQueryStatus.MATCHED:
        raise ValueError("unsupported modern motif condition did not refuse")

    benchmark_corpus = read_reverse_degradation_benchmark_corpus(benchmark_path)
    campaign, returned_index = run_reverse_degradation_benchmark_campaign(
        benchmark_corpus,
        root,
        motif_index=index,
        motif_candidate_provisional=False,
    )
    replay, replay_index = run_reverse_degradation_benchmark_campaign(
        benchmark_corpus,
        root,
        motif_index=index,
        motif_candidate_provisional=False,
    )
    if (
        returned_index.index_id != index.index_id
        or replay_index.index_id != index.index_id
    ):
        raise ValueError("benchmark returned a different modern motif index")
    candidate = _candidate_report(campaign)
    replay_candidate = _candidate_report(replay)
    deterministic = candidate.to_dict() == replay_candidate.to_dict()
    controls = {
        item.method_name: {
            "role": item.role,
            "promotion_eligible": item.gate_decision.promotion_eligible,
            "report_id": item.report_id,
        }
        for item in campaign.candidate_reports
        if item.method_name != "empirical_motif"
    }
    real_contracts = {
        "immutable_raw_anchors_pass": (
            candidate.metrics["immutable_anchor_violation_count"] == 0
        ),
        "variable_cardinality_pass": (
            float(candidate.metrics["max_event_count_relative_error"]) <= 0.5
            and candidate.refusal_count == 0
        ),
        "deterministic_seed_replay_pass": deterministic,
        "boundary_continuity_pass": candidate.failure_count == 0,
        "unsupported_context_refusal_pass": (
            refusal.status is ReferenceMotifQueryStatus.NO_SUPPORTED_CELL
            and not refusal.matches
        ),
    }
    dependencies = {
        "feed_epoch_definition": _artifact_ref(
            definition_path, "feed_epoch_definition_v2"
        ),
        "market_context_corpus": _artifact_ref(
            context_path, "market_context_corpus_v1"
        ),
        "cftc_positioning_corpus": _artifact_ref(
            positioning_path, "cftc_positioning_corpus_v1"
        ),
        "benchmark_manifest": _artifact_ref(
            benchmark_path, "reverse_degradation_manifest_v1"
        ),
    }
    library_id = _stable_id(
        "modern-reference-motif-library",
        {
            "profile_id": selected.profile_id,
            "index_id": index.index_id,
            "dependency_sha256": {
                name: item.sha256 for name, item in sorted(dependencies.items())
            },
            "candidate_report_id": candidate.report_id,
        },
    )
    split_source_counts = Counter(item.split_kind.value for item in filtered)
    manifest: dict[str, Any] = {
        "schema_version": MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION,
        "library_id": library_id,
        "profile": selected.to_dict(),
        "stable_feed_epoch": modern_epoch.to_dict(),
        "dependencies": {
            name: item.to_dict() for name, item in sorted(dependencies.items())
        },
        "source_partitions": sorted(
            source_partitions, key=lambda item: (item["period"], item["symbol"])
        ),
        "feature_schema": _feature_schema(),
        "split_assignment": {
            name: list(selected.split_periods[name])
            for name in _EXPECTED_SPLITS
        },
        "support": {
            "source_window_count_before_exclusion": len(windows),
            "source_window_count_after_exclusion": len(filtered),
            "source_windows_by_split": dict(
                sorted(split_source_counts.items())
            ),
            "retained_fragment_count": len(index.fragments),
            "selection_omitted_count": index.selection_omitted_count,
            "near_duplicate_train_exclusion_count": len(excluded_train),
        },
        "index": {
            "index_id": index.index_id,
            "config_id": index.config.config_id,
            "payload_layout": "compact-offsets-and-deltas-v1",
            "raw_rows_embedded": False,
        },
        "artifact_contract": {
            "content_addressed": True,
            "dense_rows_embedded": False,
            "holdout_rows_embedded": False,
            "installed_reader_required": True,
        },
    }
    leakage_audit: dict[str, Any] = {
        "schema_version": MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION,
        "library_id": library_id,
        "policy": "prefilter-train-near-duplicates-then-fail-closed-index-build",
        "neighbor_guard_seconds": selected.neighbor_guard_seconds,
        "train_near_duplicate_exclusion_count": len(excluded_train),
        "train_near_duplicate_excluded_source_window_ids": list(excluded_train),
        "retained_nontrain_fragment_count": 0,
        "retained_holdout_fragment_count": 0,
        "post_exclusion_cross_split_finding_count": 0,
        "post_exclusion_comparison_count": index.leakage_comparison_count,
        "indexed_splits": [ReferenceMotifSplitKind.TRAIN.value],
    }
    qualification: dict[str, Any] = {
        "schema_version": MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION,
        "library_id": library_id,
        "frozen_gate_policy_commit": PREDECLARED_GATE_COMMIT,
        "campaign": campaign.to_dict(),
        "candidate_report_id": candidate.report_id,
        "candidate_promotion_eligible": candidate.gate_decision.promotion_eligible,
        "candidate_provisional": candidate.provisional,
        "candidate_replay_report_id": replay_candidate.report_id,
        "transparent_controls": controls,
        "real_window_contracts": real_contracts,
    }
    runtime = time.monotonic() - started
    peak_memory = _peak_memory_bytes()
    resource_audit: dict[str, Any] = {
        "schema_version": MODERN_REFERENCE_MOTIF_RESOURCE_SCHEMA_VERSION,
        "library_id": library_id,
        "runtime_seconds": round(runtime, 6),
        "peak_memory_bytes": peak_memory,
        "source_partition_count": len(source_partitions),
        "source_bytes": source_bytes,
        "source_row_count": sum(
            int(item["row_count"]) for item in source_partitions
        ),
        "scratch_bytes": 0,
        "index_json_bytes": len(index.to_json().encode("utf-8") + b"\n"),
        "compact_artifact_bytes": 0,
        "profile_bounds": {
            "max_runtime_seconds": selected.max_runtime_seconds,
            "max_peak_memory_bytes": selected.max_peak_memory_bytes,
            "max_source_bytes": selected.max_source_bytes,
            "max_artifact_bytes": selected.max_artifact_bytes,
        },
    }
    if runtime > selected.max_runtime_seconds:
        raise RuntimeError("modern motif campaign exceeded runtime bound")
    if peak_memory > selected.max_peak_memory_bytes:
        raise RuntimeError("modern motif campaign exceeded peak-memory bound")
    return ModernReferenceMotifBuildV1(
        profile=selected,
        index=index,
        library_id=library_id,
        manifest=manifest,
        leakage_audit=leakage_audit,
        coverage=coverage,
        qualification=qualification,
        resource_audit=resource_audit,
    )


def write_modern_reference_motif_artifacts(
    build: ModernReferenceMotifBuildV1,
    artifact_directory: str | Path,
) -> Mapping[str, ArtifactRef]:
    """Write the compact library and qualification evidence exactly once."""
    if not isinstance(build, ModernReferenceMotifBuildV1):
        raise ValueError("modern motif writer requires a v1 build")
    root = Path(artifact_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    index_encoded = build.index.to_json().encode("utf-8") + b"\n"
    index_digest = hashlib.sha256(index_encoded).hexdigest()
    index_path = root / f"modern-reference-motif-index-{index_digest}.json"
    index_ref = ArtifactRef(
        kind="modern_reference_motif_index_v1",
        path=str(index_path),
        size_bytes=len(index_encoded),
        sha256=index_digest,
        metadata={
            "library_id": build.library_id,
            "index_id": build.index.index_id,
        },
    )
    manifest = {**build.manifest, "index_artifact": index_ref.to_dict()}
    resource_audit = dict(build.resource_audit)
    base_payloads: dict[str, tuple[str, Mapping[str, Any]]] = {
        "index": ("modern-reference-motif-index", build.index.to_dict()),
        "manifest": ("modern-reference-motif-manifest", manifest),
        "leakage_audit": (
            "modern-reference-motif-leakage-audit",
            build.leakage_audit,
        ),
        "coverage": ("modern-reference-motif-coverage", build.coverage),
        "qualification": (
            "modern-reference-motif-qualification",
            build.qualification,
        ),
        "resource_audit": (
            "modern-reference-motif-resource-audit",
            resource_audit,
        ),
    }
    total = 0
    for _ in range(8):
        resource_audit["compact_artifact_bytes"] = total
        total_next = sum(
            len(canonical_contract_json(payload).encode("utf-8") + b"\n")
            for _, payload in base_payloads.values()
        )
        if total_next == total:
            break
        total = total_next
    else:
        raise RuntimeError(
            "modern motif artifact byte measurement did not converge"
        )
    if total > build.profile.max_artifact_bytes:
        raise ValueError("modern motif artifact set exceeds configured bound")
    artifacts: dict[str, ArtifactRef] = {}
    measured = 0
    for name, (prefix, payload) in base_payloads.items():
        encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
        digest = hashlib.sha256(encoded).hexdigest()
        path = root / f"{prefix}-{digest}.json"
        _write_once(path, encoded)
        measured += len(encoded)
        artifacts[name] = ArtifactRef(
            kind=f"modern_reference_motif_{name}_v1",
            path=str(path),
            size_bytes=len(encoded),
            sha256=digest,
            metadata={"library_id": build.library_id},
        )
    if measured != total:
        raise ValueError(
            "modern motif artifact bytes differ from resource evidence"
        )
    return artifacts


def read_modern_reference_motif_index(
    path: str | Path,
) -> ReferenceMotifIndexV1:
    """Hash-verify and restore a content-addressed production index."""
    payload = _read_content_addressed_json(path, "modern-reference-motif-index")
    return ReferenceMotifIndexV1.from_dict(payload)


def read_modern_reference_motif_artifact(
    path: str | Path,
    *,
    kind: str,
) -> Mapping[str, Any]:
    """Hash-verify one compact manifest, audit, or qualification artifact."""
    schemas = {
        "manifest": MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION,
        "leakage-audit": MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION,
        "coverage": MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION,
        "qualification": MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION,
        "resource-audit": MODERN_REFERENCE_MOTIF_RESOURCE_SCHEMA_VERSION,
    }
    try:
        schema = schemas[kind]
    except KeyError as exc:
        raise ValueError("unsupported modern motif artifact kind") from exc
    payload = _read_content_addressed_json(
        path, f"modern-reference-motif-{kind}"
    )
    if payload.get("schema_version") != schema:
        raise ValueError("unsupported modern motif artifact schema")
    return payload


def _coverage_payload(
    index: ReferenceMotifIndexV1,
    windows: Sequence[ReferenceMotifSourceWindowV1],
    excluded_train: Sequence[str],
) -> dict[str, Any]:
    retained_axes: Counter[str] = Counter()
    for fragment in index.fragments:
        values = _coverage_axes(fragment.condition)
        retained_axes.update(
            f"{name}={value}" for name, value in values.items()
        )
    backoff: Counter[str] = Counter()
    status: Counter[str] = Counter()
    axis_queries: dict[str, Counter[str]] = {}
    scanned = 0
    for window in windows:
        if window.split_kind is ReferenceMotifSplitKind.TRAIN:
            continue
        result = query_reference_motifs(
            index,
            ReferenceMotifQueryV1(
                condition=window.condition,
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                used_at_ns=window.end_ns + 1,
                max_results=index.config.max_matches,
            ),
        )
        status[result.status.value] += 1
        scanned += result.scanned_fragment_count
        query_axis_counts: list[Counter[str]] = []
        for name, value in _coverage_axes(window.condition).items():
            counts = axis_queries.setdefault(f"{name}={value}", Counter())
            counts["query_count"] += 1
            counts[f"status:{result.status.value}"] += 1
            query_axis_counts.append(counts)
        if result.matches:
            level = result.matches[0].backoff_level
            backoff[level] += 1
            for counts in query_axis_counts:
                counts[f"backoff:{level}"] += 1
    input_axes = Counter(
        f"split={window.split_kind.value}" for window in windows
    )
    event_windows = sum(
        any(
            tag.startswith("market_context:")
            and not tag.startswith("market_context:none:")
            for tag in window.condition.event_tags
        )
        for window in windows
    )
    return {
        "schema_version": MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION,
        "index_id": index.index_id,
        "retained_support_by_axis": dict(sorted(retained_axes.items())),
        "source_windows_by_split": dict(sorted(input_axes.items())),
        "withheld_query_count": sum(status.values()),
        "withheld_query_status_counts": dict(sorted(status.items())),
        "withheld_backoff_counts": dict(sorted(backoff.items())),
        "withheld_backoff_by_axis": {
            axis: _coverage_axis_rates(counts)
            for axis, counts in sorted(axis_queries.items())
        },
        "withheld_exact_match_rate": (
            backoff["exact"] / max(1, sum(status.values()))
        ),
        "withheld_mean_scanned_fragment_count": (
            scanned / max(1, sum(status.values()))
        ),
        "event_conditioned_source_window_count": event_windows,
        "near_duplicate_train_exclusion_count": len(excluded_train),
        "cross_symbol_balance": {
            symbol: sum(
                item.condition.symbol == symbol for item in index.fragments
            )
            for symbol in sorted(
                {item.condition.symbol for item in index.fragments}
            )
        },
    }


def _coverage_axes(
    condition: ReferenceMotifConditionV1,
) -> dict[str, str]:
    return {
        "symbol": condition.symbol,
        "session": condition.session_state,
        "epoch": condition.feed_epoch_id,
        "event_state": "+".join(condition.event_tags) or "none",
        "volatility": condition.volatility_regime,
        "activity": condition.activity_regime,
        "spread": condition.spread_regime,
        "weekday": next(
            (
                item
                for item in condition.special_tags
                if item.startswith("weekday:")
            ),
            "weekday:unknown",
        ),
    }


def _coverage_axis_rates(counts: Mapping[str, int]) -> dict[str, Any]:
    query_count = int(counts.get("query_count", 0))
    status_counts = {
        key.removeprefix("status:"): int(value)
        for key, value in sorted(counts.items())
        if key.startswith("status:")
    }
    backoff_counts = {
        key.removeprefix("backoff:"): int(value)
        for key, value in sorted(counts.items())
        if key.startswith("backoff:")
    }
    return {
        "query_count": query_count,
        "status_counts": status_counts,
        "backoff_counts": backoff_counts,
        "backoff_rates": {
            level: count / max(1, query_count)
            for level, count in backoff_counts.items()
        },
    }


def _feature_schema() -> dict[str, Any]:
    return {
        "schema_version": REFERENCE_MOTIF_FEATURE_SCHEMA_VERSION,
        "categorical_features": [
            "symbol",
            "session_state",
            "weekday",
            "event/news state",
            "feed_epoch_id",
            "return_regime",
            "range_regime",
            "volatility_regime",
            "spread_regime",
            "activity_regime",
            "interarrival_regime",
            "timestamp_precision",
            "price_precision",
            "source_quality_state",
        ],
        "numeric_features": [
            "return_value",
            "range_value",
            "volatility",
            "spread",
            "tick_intensity",
            "interarrival_ns",
            "timestamp_precision_ns",
            "price_precision_digits",
            "source_quality_score",
        ],
        "fixed_thresholds": {
            "return_abs": [1e-5, 5e-5],
            "range": [2e-5, 1e-4],
            "volatility": [5e-6, 2.5e-5],
            "relative_spread": [5e-5, 2e-4],
            "tick_intensity_per_second": [1.0, 5.0],
            "interarrival_ns": [200_000_000, 2_000_000_000],
        },
        "weekday_encoding": "special_tags:weekday:<lowercase-name>",
        "fitted_on_holdout": False,
    }


def _reference_splits(
    profile: ModernReferenceMotifProfileV1,
) -> tuple[ReferenceMotifSplitV1, ...]:
    return tuple(
        ReferenceMotifSplitV1(
            kind=ReferenceMotifSplitKind(name),
            start_ns=_month_start_ns(profile.split_periods[name][0]),
            end_ns=_month_start_ns(
                _next_period(profile.split_periods[name][-1])
            ),
        )
        for name in _EXPECTED_SPLITS
    )


def _candidate_intervals(
    period: str,
    *,
    duration_seconds: int,
    context_event_times: Sequence[int] = (),
) -> tuple[tuple[int, int, str], ...]:
    year, month = int(period[:4]), int(period[4:])
    names = {0: "asia", 8: "london", 14: "new_york"}
    ordinary: list[tuple[int, int, str]] = []
    for day in range(3, 27):
        value = datetime(year, month, day, tzinfo=timezone.utc)
        if value.weekday() >= 5:
            continue
        for hour in (0, 8, 14):
            start = datetime(year, month, day, hour, tzinfo=timezone.utc)
            start_ns = int(start.timestamp() * NANOSECONDS_PER_SECOND)
            ordinary.append(
                (
                    start_ns,
                    start_ns + duration_seconds * NANOSECONDS_PER_SECOND,
                    names[hour],
                )
            )
    baseline = tuple(
        next(item for item in ordinary if item[2] == session)
        for session in ("asia", "london", "new_york")
    )
    contexts: list[tuple[int, int, str]] = []
    for start_ns in sorted(set(context_event_times)):
        if _period_for_ns(start_ns) != period:
            continue
        end_ns = start_ns + duration_seconds * NANOSECONDS_PER_SECOND
        candidate = (start_ns, end_ns, _session_for_ns(start_ns))
        if any(
            left < end_ns and start_ns < right
            for left, right, _ in (*baseline, *contexts)
        ):
            continue
        contexts.append(candidate)
        if len(contexts) == 3:
            break
    used = {(start, end) for start, end, _ in (*baseline, *contexts)}
    remainder = tuple(
        item for item in ordinary if (item[0], item[1]) not in used
    )
    return (*baseline, *contexts, *remainder)


def _read_arrow_interval(
    path: Path,
    *,
    start_ns: int,
    end_ns: int,
    maximum: int,
) -> tuple[_TickRow, ...]:
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.ipc as ipc  # pylint: disable=import-outside-toplevel

    start_ms = start_ns // NANOSECONDS_PER_MILLISECOND
    end_ms = (end_ns - 1) // NANOSECONDS_PER_MILLISECOND + 1
    rows: list[_TickRow] = []
    row_offset = 0
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        if not {"datetime", "bid", "ask"}.issubset(reader.schema.names):
            raise ValueError("modern motif Arrow cache lacks quote columns")
        for batch_index in range(reader.num_record_batches):
            batch = reader.get_batch(batch_index)
            count = batch.num_rows
            if count == 0:
                continue
            timestamps = batch.column(batch.schema.get_field_index("datetime"))
            first = int(timestamps[0].as_py())
            last = int(timestamps[count - 1].as_py())
            if last < start_ms:
                row_offset += count
                continue
            if first >= end_ms:
                break
            bids = batch.column(batch.schema.get_field_index("bid"))
            asks = batch.column(batch.schema.get_field_index("ask"))
            for index in range(count):
                timestamp = int(timestamps[index].as_py())
                if start_ms <= timestamp < end_ms:
                    rows.append(
                        _TickRow(
                            row_id=row_offset + index,
                            timestamp_ms=timestamp,
                            bid=float(bids[index].as_py()),
                            ask=float(asks[index].as_py()),
                        )
                    )
                    if len(rows) == maximum:
                        return tuple(rows)
            row_offset += count
    return tuple(rows)


def _arrow_row_count(path: Path) -> int:
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.ipc as ipc  # pylint: disable=import-outside-toplevel

    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        return sum(
            reader.get_batch(index).num_rows
            for index in range(reader.num_record_batches)
        )


def _candidate_report(campaign: ReverseDegradationBenchmarkCampaignV1) -> Any:
    candidates = [
        item
        for item in campaign.candidate_reports
        if item.method_name == "empirical_motif"
    ]
    if len(candidates) != 1:
        raise ValueError(
            "modern motif qualification lacks one candidate report"
        )
    return candidates[0]


def _relative_source_path(symbol: str, period: str) -> Path:
    return (
        Path(symbol.lower())
        / str(int(period[:4]))
        / str(int(period[4:]))
        / ".data"
    )


def _month_start_ns(period: str) -> int:
    return int(
        datetime(
            int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc
        ).timestamp()
        * NANOSECONDS_PER_SECOND
    )


def _next_period(period: str) -> str:
    year, month = int(period[:4]), int(period[4:])
    if month == 12:
        return f"{year + 1:04d}01"
    return f"{year:04d}{month + 1:02d}"


def _period_for_ns(value: int) -> str:
    return datetime.fromtimestamp(
        value / NANOSECONDS_PER_SECOND, tz=timezone.utc
    ).strftime("%Y%m")


def _session_for_ns(value: int) -> str:
    hour = datetime.fromtimestamp(
        value / NANOSECONDS_PER_SECOND, tz=timezone.utc
    ).hour
    if hour < 7:
        return "asia"
    if hour < 12:
        return "london"
    return "new_york"


def _artifact_ref(path: Path, kind: str) -> ArtifactRef:
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=_file_sha256(path),
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _stable_id(namespace: str, payload: Mapping[str, Any]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{namespace}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _peak_memory_bytes() -> int:
    maximum = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return maximum if sys.platform == "darwin" else maximum * 1024


def _enforce_runtime(started: float, maximum: float) -> None:
    if time.monotonic() - started > maximum:
        raise RuntimeError("modern motif build exceeded runtime bound")


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("content-addressed modern motif artifact differs")
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


def _read_content_addressed_json(
    path: str | Path, prefix: str
) -> Mapping[str, Any]:
    source = Path(path).expanduser().resolve()
    match = _CONTENT_ADDRESS_RE.fullmatch(source.name)
    if match is None or match.group(1) != prefix:
        raise ValueError("modern motif artifact name is not content addressed")
    content = source.read_bytes()
    if len(content) > 256 * 1024**2:
        raise ValueError("modern motif artifact exceeds size bound")
    if hashlib.sha256(content).hexdigest() != match.group(2):
        raise ValueError("modern motif artifact content hash differs from name")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("modern motif artifact is invalid JSON") from exc
    return _mapping(payload)


def _read_json_mapping(path: Path, maximum: int) -> Mapping[str, Any]:
    if path.stat().st_size > maximum:
        raise ValueError("modern motif dependency exceeds size bound")
    return _mapping(json.loads(path.read_text(encoding="utf-8")))


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("modern motif JSON value must be an object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("modern motif JSON value must be an array")
    return value


__all__ = [
    "DEFAULT_MODERN_MOTIF_SPLIT_PERIODS",
    "MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION",
    "MODERN_REFERENCE_MOTIF_LEAKAGE_SCHEMA_VERSION",
    "MODERN_REFERENCE_MOTIF_MANIFEST_SCHEMA_VERSION",
    "MODERN_REFERENCE_MOTIF_PROFILE_SCHEMA_VERSION",
    "MODERN_REFERENCE_MOTIF_QUALIFICATION_SCHEMA_VERSION",
    "MODERN_REFERENCE_MOTIF_RESOURCE_SCHEMA_VERSION",
    "ModernReferenceMotifBuildV1",
    "ModernReferenceMotifProfileV1",
    "build_modern_reference_motif_library",
    "read_modern_reference_motif_artifact",
    "read_modern_reference_motif_index",
    "write_modern_reference_motif_artifacts",
]
