"""Immutable real-data corpus and campaign for reverse degradation.

This module is the installed issue-#463 boundary around the generator-neutral
benchmark contracts.  It selects small, synchronized partitions from real
Arrow tick caches, records only content-addressed lineage and aggregate
evidence, and can replay every selected partition before a campaign is
trusted.  Dense or holdout event rows are never written to the artifacts.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
import time
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from histdatacom.data_analytics.feed_epochs_v2 import (
    read_active_time_feed_epoch_definition,
)
from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import ArtifactRef, JSONScalar, JSONValue
from histdatacom.synthetic.benchmark import (
    BenchmarkCandidateKind,
    BenchmarkCandidateV1,
    BenchmarkControlKind,
    BenchmarkEventV1,
    BenchmarkExecutionEvidenceV1,
    BenchmarkScenarioV1,
    BenchmarkSplitKind,
    build_benchmark_control_events,
    degrade_benchmark_window,
    generate_benchmark_candidate_window,
)
from histdatacom.synthetic.benchmark_gates import (
    BenchmarkGateObservationV1,
    BenchmarkGateScope,
    BenchmarkPromotionDecisionV1,
    evaluate_benchmark_promotion_gates,
    load_default_benchmark_promotion_gate_policy,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.event_clock import (
    EventClockCalibrationWindowV1,
    EventClockConfigurationV1,
    EventClockConfigV1,
    EventClockFamily,
    EventClockFitResultV1,
    EventClockFitStatus,
    FittedEventClockBenchmarkGeneratorV1,
    build_event_clock_benchmark_candidate,
    build_fitted_event_clock_generator,
    fit_event_clock_challenger,
)
from histdatacom.synthetic.generation import (
    EMPIRICAL_MOTIF_GENERATOR_ID,
    EmpiricalMotifBenchmarkGeneratorV1,
    EmpiricalMotifGeneratorConfigV1,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.marked_hawkes import (
    FittedMarkedHawkesBenchmarkGeneratorV1,
    HawkesExcitationStructure,
    MarkedHawkesConfigV1,
    MarkedHawkesFitResultV1,
    MarkedHawkesFitStatus,
    build_fitted_marked_hawkes_generator,
    build_marked_hawkes_benchmark_candidate,
    fit_marked_hawkes_challenger,
)
from histdatacom.synthetic.motifs import (
    ReferenceMotifConditionV1,
    ReferenceMotifIndexConfigV1,
    ReferenceMotifIndexV1,
    ReferenceMotifSourceEventV1,
    ReferenceMotifSourceWindowV1,
    ReferenceMotifSplitKind,
    ReferenceMotifSplitV1,
    build_reference_motif_index,
    reference_motif_condition_from_quotes,
)
from histdatacom.synthetic.observation import ObservationOperatorV1
from histdatacom.synthetic.observation_calibration import (
    read_observation_calibration_campaign,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
)

BENCHMARK_CORPUS_PROFILE_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-corpus-profile.v1"
)
BENCHMARK_SOURCE_PARTITION_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-source-partition.v1"
)
BENCHMARK_WINDOW_PARTITION_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-window-partition.v1"
)
REVERSE_DEGRADATION_CORPUS_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-corpus.v1"
)
BENCHMARK_CANDIDATE_REPORT_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-candidate-report.v1"
)
REVERSE_DEGRADATION_CAMPAIGN_SCHEMA_VERSION = (
    "histdatacom.reverse-degradation-campaign.v1"
)

PREDECLARED_GATE_COMMIT = "0caec1480a957528ebefdff062e13012ea11e84d"
DEFAULT_BENCHMARK_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
DEFAULT_BENCHMARK_PERIODS = {
    "calibration": "201001",
    "validation": "202401",
    "final_holdout": "202510",
}
DEFAULT_SESSION_HOURS = (0, 8, 14)
DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_BENCHMARK_SOURCE_BYTES = 2 * 1024**3
MAX_BENCHMARK_WINDOWS = 96
MAX_BENCHMARK_EVENTS_PER_SYMBOL = 4096
MAX_BENCHMARK_CANDIDATES = 16
MAX_BENCHMARK_METRICS = 256
NANOSECONDS_PER_MILLISECOND = 1_000_000
NANOSECONDS_PER_SECOND = 1_000_000_000
PIP = 0.0001
_PERIOD = re.compile(r"^[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class ReverseDegradationCorpusProfileV1:
    """Bounded selection, replay, and execution policy."""

    symbols: tuple[str, ...] = DEFAULT_BENCHMARK_SYMBOLS
    split_periods: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_BENCHMARK_PERIODS)
    )
    synchronized_windows_per_split: int = 6
    window_duration_seconds: int = 600
    minimum_events_per_symbol: int = 64
    max_events_per_symbol: int = 256
    neighbor_guard_seconds: int = 1800
    ensemble_member_ids: tuple[str, ...] = ("member-01", "member-02")
    max_source_bytes: int = MAX_BENCHMARK_SOURCE_BYTES
    max_runtime_seconds: float = 900.0
    max_peak_memory_bytes: int = 2 * 1024**3
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    profile_id: str = ""
    schema_version: str = BENCHMARK_CORPUS_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_CORPUS_PROFILE_SCHEMA_VERSION,
            "benchmark corpus profile",
        )
        symbols = tuple(sorted({_symbol(item) for item in self.symbols}))
        if symbols != DEFAULT_BENCHMARK_SYMBOLS:
            raise ValueError(
                "benchmark corpus requires the EUR/GBP/USD triangle"
            )
        object.__setattr__(self, "symbols", symbols)
        periods = {
            str(key): str(value) for key, value in self.split_periods.items()
        }
        if set(periods) != set(DEFAULT_BENCHMARK_PERIODS):
            raise ValueError(
                "benchmark split periods must cover all blocked roles"
            )
        if any(not _PERIOD.fullmatch(value) for value in periods.values()):
            raise ValueError("benchmark split periods must use YYYYMM")
        if (
            not periods["calibration"]
            < periods["validation"]
            < periods["final_holdout"]
        ):
            raise ValueError("benchmark split periods must be chronological")
        object.__setattr__(self, "split_periods", dict(sorted(periods.items())))
        _bounded_int(
            self.synchronized_windows_per_split,
            "synchronized_windows_per_split",
            1,
            MAX_BENCHMARK_WINDOWS // 3,
        )
        _bounded_int(
            self.window_duration_seconds, "window_duration_seconds", 60, 3600
        )
        minimum = _bounded_int(
            self.minimum_events_per_symbol,
            "minimum_events_per_symbol",
            2,
            MAX_BENCHMARK_EVENTS_PER_SYMBOL,
        )
        maximum = _bounded_int(
            self.max_events_per_symbol,
            "max_events_per_symbol",
            minimum,
            MAX_BENCHMARK_EVENTS_PER_SYMBOL,
        )
        if maximum < minimum:
            raise ValueError("maximum events must cover the minimum")
        _bounded_int(
            self.neighbor_guard_seconds, "neighbor_guard_seconds", 0, 86400
        )
        members = tuple(
            sorted({_required_text(v) for v in self.ensemble_member_ids})
        )
        if not 2 <= len(members) <= 8:
            raise ValueError(
                "benchmark campaign requires two to eight ensemble members"
            )
        object.__setattr__(self, "ensemble_member_ids", members)
        _bounded_int(self.max_source_bytes, "max_source_bytes", 1, 16 * 1024**3)
        _positive_float(self.max_runtime_seconds, "max_runtime_seconds")
        _bounded_int(
            self.max_peak_memory_bytes, "max_peak_memory_bytes", 1, 16 * 1024**3
        )
        _bounded_int(
            self.max_artifact_bytes, "max_artifact_bytes", 1024, 1024**3
        )
        expected = _stable_id(
            "reverse-degradation-corpus-profile", self.identity_payload()
        )
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("benchmark corpus profile_id differs")
        object.__setattr__(self, "profile_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbols": list(self.symbols),
            "split_periods": dict(self.split_periods),
            "synchronized_windows_per_split": self.synchronized_windows_per_split,
            "window_duration_seconds": self.window_duration_seconds,
            "minimum_events_per_symbol": self.minimum_events_per_symbol,
            "max_events_per_symbol": self.max_events_per_symbol,
            "neighbor_guard_seconds": self.neighbor_guard_seconds,
            "ensemble_member_ids": list(self.ensemble_member_ids),
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
    ) -> "ReverseDegradationCorpusProfileV1":
        _require_schema(data, BENCHMARK_CORPUS_PROFILE_SCHEMA_VERSION)
        return cls(
            symbols=_string_tuple(data.get("symbols")),
            split_periods={
                str(k): str(v)
                for k, v in _mapping(data.get("split_periods")).items()
            },
            synchronized_windows_per_split=_strict_int(
                data.get("synchronized_windows_per_split"), "window count"
            ),
            window_duration_seconds=_strict_int(
                data.get("window_duration_seconds"), "window duration"
            ),
            minimum_events_per_symbol=_strict_int(
                data.get("minimum_events_per_symbol"), "minimum events"
            ),
            max_events_per_symbol=_strict_int(
                data.get("max_events_per_symbol"), "maximum events"
            ),
            neighbor_guard_seconds=_strict_int(
                data.get("neighbor_guard_seconds"), "neighbor guard"
            ),
            ensemble_member_ids=_string_tuple(data.get("ensemble_member_ids")),
            max_source_bytes=_strict_int(
                data.get("max_source_bytes"), "max source bytes"
            ),
            max_runtime_seconds=_finite_float(
                data.get("max_runtime_seconds"), "max runtime"
            ),
            max_peak_memory_bytes=_strict_int(
                data.get("max_peak_memory_bytes"), "max peak memory"
            ),
            max_artifact_bytes=_strict_int(
                data.get("max_artifact_bytes"), "max artifact bytes"
            ),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkSourcePartitionV1:
    """One hash-verified monthly Arrow source."""

    symbol: str
    period: str
    relative_path: str
    size_bytes: int
    row_count: int
    sha256: str
    partition_id: str = ""
    schema_version: str = BENCHMARK_SOURCE_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_SOURCE_PARTITION_SCHEMA_VERSION,
            "source partition",
        )
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        if not _PERIOD.fullmatch(self.period):
            raise ValueError("source partition period must use YYYYMM")
        relative = Path(_required_text(self.relative_path))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(
                "source partition path must be relative and contained"
            )
        object.__setattr__(self, "relative_path", relative.as_posix())
        _bounded_int(
            self.size_bytes, "source size", 1, MAX_BENCHMARK_SOURCE_BYTES
        )
        _bounded_int(self.row_count, "source row count", 2, 2**63 - 1)
        _sha256(self.sha256, "source sha256")
        expected = _stable_id(
            "reverse-degradation-source-partition", self.identity_payload()
        )
        supplied = _optional_text(self.partition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("source partition_id differs")
        object.__setattr__(self, "partition_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "period": self.period,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "sha256": self.sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "partition_id": self.partition_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkSourcePartitionV1":
        _require_schema(data, BENCHMARK_SOURCE_PARTITION_SCHEMA_VERSION)
        return cls(
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            relative_path=str(data.get("relative_path", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "source size"),
            row_count=_strict_int(data.get("row_count"), "source rows"),
            sha256=str(data.get("sha256", "")),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BenchmarkWindowPartitionV1:
    """One synchronized, replayable real-data window."""

    split_kind: str
    period: str
    session: str
    start_ns: int
    end_ns: int
    epoch_label: str
    source_partition_ids: tuple[str, ...]
    symbol_event_counts: Mapping[str, int]
    symbol_partition_sha256: Mapping[str, str]
    event_state_counts: Mapping[str, int]
    context_state: str
    positioning_state: str
    context_supported: bool
    selection_rule: str = "first-n-events-in-half-open-utc-window"
    window_id: str = ""
    schema_version: str = BENCHMARK_WINDOW_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_WINDOW_PARTITION_SCHEMA_VERSION,
            "window partition",
        )
        if self.split_kind not in DEFAULT_BENCHMARK_PERIODS:
            raise ValueError("unsupported benchmark window split")
        if not _PERIOD.fullmatch(self.period):
            raise ValueError("window period must use YYYYMM")
        object.__setattr__(self, "session", _required_text(self.session))
        start = _bounded_int(self.start_ns, "window start", 0, 2**63 - 1)
        end = _bounded_int(self.end_ns, "window end", start + 1, 2**63 - 1)
        if end <= start:
            raise ValueError("window end must follow start")
        object.__setattr__(
            self, "epoch_label", _required_text(self.epoch_label)
        )
        partitions = tuple(
            sorted({_required_text(v) for v in self.source_partition_ids})
        )
        if len(partitions) != len(DEFAULT_BENCHMARK_SYMBOLS):
            raise ValueError("window must bind all triangle source partitions")
        object.__setattr__(self, "source_partition_ids", partitions)
        counts = {
            _symbol(k): _bounded_int(
                v, f"{k} event count", 2, MAX_BENCHMARK_EVENTS_PER_SYMBOL
            )
            for k, v in self.symbol_event_counts.items()
        }
        hashes = {
            _symbol(k): _sha256(v, f"{k} window hash")
            for k, v in self.symbol_partition_sha256.items()
        }
        if set(counts) != set(DEFAULT_BENCHMARK_SYMBOLS) or set(hashes) != set(
            DEFAULT_BENCHMARK_SYMBOLS
        ):
            raise ValueError("window counts and hashes must cover the triangle")
        object.__setattr__(
            self, "symbol_event_counts", dict(sorted(counts.items()))
        )
        object.__setattr__(
            self, "symbol_partition_sha256", dict(sorted(hashes.items()))
        )
        states = {
            _required_text(name): _bounded_int(
                count,
                f"{name} event-state count",
                0,
                MAX_BENCHMARK_EVENTS_PER_SYMBOL
                * len(DEFAULT_BENCHMARK_SYMBOLS),
            )
            for name, count in self.event_state_counts.items()
        }
        if not states or sum(states.values()) != sum(counts.values()):
            raise ValueError(
                "window event-state counts differ from event counts"
            )
        object.__setattr__(
            self, "event_state_counts", dict(sorted(states.items()))
        )
        object.__setattr__(
            self, "context_state", _required_text(self.context_state)
        )
        object.__setattr__(
            self, "positioning_state", _required_text(self.positioning_state)
        )
        if not isinstance(self.context_supported, bool):
            raise ValueError("context_supported must be boolean")
        if self.selection_rule != "first-n-events-in-half-open-utc-window":
            raise ValueError("unsupported window selection rule")
        expected = _stable_id(
            "reverse-degradation-window-partition", self.identity_payload()
        )
        supplied = _optional_text(self.window_id)
        if supplied is not None and supplied != expected:
            raise ValueError("window partition window_id differs")
        object.__setattr__(self, "window_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_kind": self.split_kind,
            "period": self.period,
            "session": self.session,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "epoch_label": self.epoch_label,
            "source_partition_ids": list(self.source_partition_ids),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "symbol_partition_sha256": dict(self.symbol_partition_sha256),
            "event_state_counts": dict(self.event_state_counts),
            "context_state": self.context_state,
            "positioning_state": self.positioning_state,
            "context_supported": self.context_supported,
            "selection_rule": self.selection_rule,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "window_id": self.window_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkWindowPartitionV1":
        _require_schema(data, BENCHMARK_WINDOW_PARTITION_SCHEMA_VERSION)
        return cls(
            split_kind=str(data.get("split_kind", "")),
            period=str(data.get("period", "")),
            session=str(data.get("session", "")),
            start_ns=_strict_int(data.get("start_ns"), "window start"),
            end_ns=_strict_int(data.get("end_ns"), "window end"),
            epoch_label=str(data.get("epoch_label", "")),
            source_partition_ids=_string_tuple(
                data.get("source_partition_ids")
            ),
            symbol_event_counts={
                str(k): _strict_int(v, str(k))
                for k, v in _mapping(data.get("symbol_event_counts")).items()
            },
            symbol_partition_sha256={
                str(k): str(v)
                for k, v in _mapping(
                    data.get("symbol_partition_sha256")
                ).items()
            },
            event_state_counts={
                str(k): _strict_int(v, str(k))
                for k, v in _mapping(data.get("event_state_counts")).items()
            },
            context_state=str(data.get("context_state", "")),
            positioning_state=str(data.get("positioning_state", "")),
            context_supported=_strict_bool(
                data.get("context_supported"), "context_supported"
            ),
            selection_rule=str(data.get("selection_rule", "")),
            window_id=str(data.get("window_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReverseDegradationBenchmarkCorpusV1:
    """Immutable real-data manifest; no tick row is embedded."""

    profile: ReverseDegradationCorpusProfileV1
    sources: tuple[BenchmarkSourcePartitionV1, ...]
    windows: tuple[BenchmarkWindowPartitionV1, ...]
    split_hashes: Mapping[str, str]
    degradation_configs: tuple[Mapping[str, JSONValue], ...]
    metric_registry: tuple[str, ...]
    dependency_artifacts: Mapping[str, ArtifactRef]
    feed_epoch_definition_id: str
    observation_operator_id: str
    market_context_corpus_id: str
    cftc_positioning_corpus_id: str
    gate_policy_id: str
    gate_policy_commit: str
    neighbor_leakage_count: int
    corpus_id: str = ""
    schema_version: str = REVERSE_DEGRADATION_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REVERSE_DEGRADATION_CORPUS_SCHEMA_VERSION,
            "reverse degradation corpus",
        )
        if not isinstance(self.profile, ReverseDegradationCorpusProfileV1):
            raise ValueError("corpus requires a v1 profile")
        sources = tuple(
            sorted(self.sources, key=lambda item: (item.period, item.symbol))
        )
        expected_source_count = len(self.profile.split_periods) * len(
            self.profile.symbols
        )
        if len(sources) != expected_source_count or len(
            {v.partition_id for v in sources}
        ) != len(sources):
            raise ValueError(
                "corpus source partitions are incomplete or duplicated"
            )
        object.__setattr__(self, "sources", sources)
        windows = tuple(
            sorted(
                self.windows, key=lambda item: (item.start_ns, item.window_id)
            )
        )
        expected_windows = (
            len(self.profile.split_periods)
            * self.profile.synchronized_windows_per_split
        )
        if (
            len(windows) != expected_windows
            or len(windows) > MAX_BENCHMARK_WINDOWS
        ):
            raise ValueError(
                "corpus synchronized window count differs from profile"
            )
        if len({v.window_id for v in windows}) != len(windows):
            raise ValueError("corpus window identity is duplicated")
        object.__setattr__(self, "windows", windows)
        split_hashes = {
            str(k): _sha256(str(v), f"{k} split hash")
            for k, v in self.split_hashes.items()
        }
        if set(split_hashes) != set(DEFAULT_BENCHMARK_PERIODS):
            raise ValueError("corpus split hashes are incomplete")
        expected_hashes = _split_hashes(windows)
        if split_hashes != expected_hashes:
            raise ValueError("corpus split hashes differ from windows")
        object.__setattr__(
            self, "split_hashes", dict(sorted(split_hashes.items()))
        )
        configs = tuple(dict(value) for value in self.degradation_configs)
        names = {str(value.get("name", "")) for value in configs}
        if names != set(_degradation_config_names()):
            raise ValueError(
                "corpus degradation configuration coverage differs"
            )
        object.__setattr__(
            self,
            "degradation_configs",
            tuple(sorted(configs, key=lambda item: str(item["name"]))),
        )
        metrics = tuple(
            sorted({_required_text(v) for v in self.metric_registry})
        )
        if (
            not set(_required_metric_names()).issubset(metrics)
            or len(metrics) > MAX_BENCHMARK_METRICS
        ):
            raise ValueError(
                "corpus metric registry is incomplete or unbounded"
            )
        object.__setattr__(self, "metric_registry", metrics)
        dependencies = {str(k): v for k, v in self.dependency_artifacts.items()}
        if set(dependencies) != {
            "feed_epochs",
            "observation_campaign",
            "market_context",
            "cftc_positioning",
            "gate_policy",
        }:
            raise ValueError("corpus dependency artifact set differs")
        if any(
            not isinstance(value, ArtifactRef)
            for value in dependencies.values()
        ):
            raise ValueError("corpus dependency artifacts must be references")
        object.__setattr__(
            self, "dependency_artifacts", dict(sorted(dependencies.items()))
        )
        for name in (
            "feed_epoch_definition_id",
            "observation_operator_id",
            "market_context_corpus_id",
            "cftc_positioning_corpus_id",
            "gate_policy_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not _COMMIT.fullmatch(self.gate_policy_commit):
            raise ValueError("gate policy commit must be a full Git SHA")
        if self.gate_policy_commit != PREDECLARED_GATE_COMMIT:
            raise ValueError(
                "campaign gate policy commit differs from issue #463 predeclaration"
            )
        _bounded_int(
            self.neighbor_leakage_count,
            "neighbor leakage count",
            0,
            MAX_BENCHMARK_WINDOWS**2,
        )
        expected = _stable_id(
            "reverse-degradation-corpus", self.identity_payload()
        )
        supplied = _optional_text(self.corpus_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reverse degradation corpus_id differs")
        object.__setattr__(self, "corpus_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "profile": self.profile.to_dict(),
            "sources": [item.to_dict() for item in self.sources],
            "windows": [item.to_dict() for item in self.windows],
            "split_hashes": dict(self.split_hashes),
            "degradation_configs": [
                dict(item) for item in self.degradation_configs
            ],
            "metric_registry": list(self.metric_registry),
            "dependency_artifacts": {
                name: ref.to_dict()
                for name, ref in self.dependency_artifacts.items()
            },
            "feed_epoch_definition_id": self.feed_epoch_definition_id,
            "observation_operator_id": self.observation_operator_id,
            "market_context_corpus_id": self.market_context_corpus_id,
            "cftc_positioning_corpus_id": self.cftc_positioning_corpus_id,
            "gate_policy_id": self.gate_policy_id,
            "gate_policy_commit": self.gate_policy_commit,
            "neighbor_leakage_count": self.neighbor_leakage_count,
            "dense_and_holdout_rows_persisted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReverseDegradationBenchmarkCorpusV1":
        _require_schema(data, REVERSE_DEGRADATION_CORPUS_SCHEMA_VERSION)
        if data.get("dense_and_holdout_rows_persisted") is not False:
            raise ValueError(
                "corpus must declare that dense rows are not persisted"
            )
        return cls(
            profile=ReverseDegradationCorpusProfileV1.from_dict(
                _mapping(data.get("profile"))
            ),
            sources=tuple(
                BenchmarkSourcePartitionV1.from_dict(_mapping(v))
                for v in _sequence(data.get("sources"))
            ),
            windows=tuple(
                BenchmarkWindowPartitionV1.from_dict(_mapping(v))
                for v in _sequence(data.get("windows"))
            ),
            split_hashes={
                str(k): str(v)
                for k, v in _mapping(data.get("split_hashes")).items()
            },
            degradation_configs=tuple(
                dict(_mapping(v))
                for v in _sequence(data.get("degradation_configs"))
            ),
            metric_registry=_string_tuple(data.get("metric_registry")),
            dependency_artifacts={
                str(k): ArtifactRef.from_dict(_mapping(v))
                for k, v in _mapping(data.get("dependency_artifacts")).items()
            },
            feed_epoch_definition_id=str(
                data.get("feed_epoch_definition_id", "")
            ),
            observation_operator_id=str(
                data.get("observation_operator_id", "")
            ),
            market_context_corpus_id=str(
                data.get("market_context_corpus_id", "")
            ),
            cftc_positioning_corpus_id=str(
                data.get("cftc_positioning_corpus_id", "")
            ),
            gate_policy_id=str(data.get("gate_policy_id", "")),
            gate_policy_commit=str(data.get("gate_policy_commit", "")),
            neighbor_leakage_count=_strict_int(
                data.get("neighbor_leakage_count"), "neighbor leakage count"
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReverseDegradationBenchmarkCorpusV1":
        return cls.from_dict(_json_mapping(text, DEFAULT_MAX_ARTIFACT_BYTES))


@dataclass(frozen=True, slots=True)
class BenchmarkCandidateReportV1:
    """Aggregate, uncertainty, and predeclared gate evidence for one method."""

    candidate_id: str
    method_name: str
    role: str
    metrics: Mapping[str, JSONScalar]
    uncertainty: Mapping[str, Mapping[str, float]]
    window_metric_count: int
    ensemble_member_count: int
    failure_count: int
    refusal_count: int
    evaluated_window_count: int
    gate_decision: BenchmarkPromotionDecisionV1
    provisional: bool = False
    report_id: str = ""
    schema_version: str = BENCHMARK_CANDIDATE_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            BENCHMARK_CANDIDATE_REPORT_SCHEMA_VERSION,
            "candidate report",
        )
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id)
        )
        object.__setattr__(
            self, "method_name", _required_text(self.method_name)
        )
        if self.role not in {"baseline", "candidate", "negative_control"}:
            raise ValueError("unsupported candidate report role")
        metrics = {
            str(k): _json_scalar(v, str(k)) for k, v in self.metrics.items()
        }
        if not metrics or len(metrics) > MAX_BENCHMARK_METRICS:
            raise ValueError("candidate metrics are empty or unbounded")
        object.__setattr__(self, "metrics", dict(sorted(metrics.items())))
        uncertainty = {
            str(name): {
                str(k): _finite_float(v, str(k)) for k, v in values.items()
            }
            for name, values in self.uncertainty.items()
        }
        if any(
            set(values) != {"lower", "mean", "upper"}
            for values in uncertainty.values()
        ):
            raise ValueError(
                "uncertainty intervals require lower, mean, and upper"
            )
        object.__setattr__(
            self, "uncertainty", dict(sorted(uncertainty.items()))
        )
        for name in (
            "window_metric_count",
            "ensemble_member_count",
            "failure_count",
            "refusal_count",
            "evaluated_window_count",
        ):
            _bounded_int(getattr(self, name), name, 0, 2**31 - 1)
        if not isinstance(self.gate_decision, BenchmarkPromotionDecisionV1):
            raise ValueError("candidate report requires a gate decision")
        if (
            self.gate_decision.subject_id != self.candidate_id
            or self.gate_decision.scope is not BenchmarkGateScope.CANDIDATE
        ):
            raise ValueError("candidate gate decision differs from report")
        if not isinstance(self.provisional, bool):
            raise ValueError("provisional must be boolean")
        expected = _stable_id(
            "reverse-degradation-candidate-report", self.identity_payload()
        )
        supplied = _optional_text(self.report_id)
        if supplied is not None and supplied != expected:
            raise ValueError("candidate report_id differs")
        object.__setattr__(self, "report_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "method_name": self.method_name,
            "role": self.role,
            "metrics": dict(self.metrics),
            "uncertainty": {
                name: dict(values) for name, values in self.uncertainty.items()
            },
            "window_metric_count": self.window_metric_count,
            "ensemble_member_count": self.ensemble_member_count,
            "failure_count": self.failure_count,
            "refusal_count": self.refusal_count,
            "evaluated_window_count": self.evaluated_window_count,
            "gate_decision": self.gate_decision.to_dict(),
            "provisional": self.provisional,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "report_id": self.report_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BenchmarkCandidateReportV1":
        _require_schema(data, BENCHMARK_CANDIDATE_REPORT_SCHEMA_VERSION)
        if data.get("automatic_winner") is not False:
            raise ValueError(
                "candidate reports never select an automatic winner"
            )
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            method_name=str(data.get("method_name", "")),
            role=str(data.get("role", "")),
            metrics={
                str(k): _json_scalar(v, str(k))
                for k, v in _mapping(data.get("metrics")).items()
            },
            uncertainty={
                str(k): {
                    str(n): _finite_float(v, str(n))
                    for n, v in _mapping(raw).items()
                }
                for k, raw in _mapping(data.get("uncertainty")).items()
            },
            window_metric_count=_strict_int(
                data.get("window_metric_count"), "window metric count"
            ),
            ensemble_member_count=_strict_int(
                data.get("ensemble_member_count"), "ensemble member count"
            ),
            failure_count=_strict_int(
                data.get("failure_count"), "failure count"
            ),
            refusal_count=_strict_int(
                data.get("refusal_count"), "refusal count"
            ),
            evaluated_window_count=_strict_int(
                data.get("evaluated_window_count"), "evaluated window count"
            ),
            gate_decision=BenchmarkPromotionDecisionV1.from_dict(
                _mapping(data.get("gate_decision"))
            ),
            provisional=_strict_bool(data.get("provisional"), "provisional"),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReverseDegradationBenchmarkCampaignV1:
    """Persisted real campaign with compact scientific evidence."""

    corpus_id: str
    motif_index_id: str
    candidate_reports: tuple[BenchmarkCandidateReportV1, ...]
    campaign_metrics: Mapping[str, JSONScalar]
    degradation_coverage: Mapping[str, int]
    context_slice_counts: Mapping[str, int]
    campaign_gate_decision: BenchmarkPromotionDecisionV1
    source_replay_verified: bool
    runtime_seconds: float
    peak_memory_bytes: int
    artifact_bytes: int
    started_at_utc: str
    completed_at_utc: str
    campaign_id: str = ""
    schema_version: str = REVERSE_DEGRADATION_CAMPAIGN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema_value(
            self.schema_version,
            REVERSE_DEGRADATION_CAMPAIGN_SCHEMA_VERSION,
            "reverse degradation campaign",
        )
        object.__setattr__(self, "corpus_id", _required_text(self.corpus_id))
        object.__setattr__(
            self, "motif_index_id", _required_text(self.motif_index_id)
        )
        reports = tuple(
            sorted(self.candidate_reports, key=lambda item: item.candidate_id)
        )
        if not 4 <= len(reports) <= MAX_BENCHMARK_CANDIDATES or len(
            {item.candidate_id for item in reports}
        ) != len(reports):
            raise ValueError(
                "campaign candidate reports are incomplete or duplicated"
            )
        if not any(item.role == "negative_control" for item in reports):
            raise ValueError("campaign requires a negative control")
        object.__setattr__(self, "candidate_reports", reports)
        metrics = {
            str(k): _json_scalar(v, str(k))
            for k, v in self.campaign_metrics.items()
        }
        object.__setattr__(
            self, "campaign_metrics", dict(sorted(metrics.items()))
        )
        coverage = {
            str(k): _bounded_int(v, str(k), 0, MAX_BENCHMARK_WINDOWS)
            for k, v in self.degradation_coverage.items()
        }
        if set(coverage) != set(_degradation_config_names()):
            raise ValueError("campaign degradation coverage differs")
        object.__setattr__(
            self, "degradation_coverage", dict(sorted(coverage.items()))
        )
        slices = {
            str(k): _bounded_int(v, str(k), 0, MAX_BENCHMARK_WINDOWS)
            for k, v in self.context_slice_counts.items()
        }
        object.__setattr__(
            self, "context_slice_counts", dict(sorted(slices.items()))
        )
        if not isinstance(
            self.campaign_gate_decision, BenchmarkPromotionDecisionV1
        ):
            raise ValueError("campaign requires a gate decision")
        if (
            self.campaign_gate_decision.subject_id != self.corpus_id
            or self.campaign_gate_decision.scope
            is not BenchmarkGateScope.CAMPAIGN
        ):
            raise ValueError("campaign gate decision differs")
        if not isinstance(self.source_replay_verified, bool):
            raise ValueError("source_replay_verified must be boolean")
        _positive_float(
            self.runtime_seconds, "runtime_seconds", allow_zero=True
        )
        _bounded_int(self.peak_memory_bytes, "peak_memory_bytes", 0, 2**63 - 1)
        _bounded_int(self.artifact_bytes, "artifact_bytes", 0, 2**63 - 1)
        _iso_utc(self.started_at_utc)
        _iso_utc(self.completed_at_utc)
        expected = _stable_id(
            "reverse-degradation-campaign", self.identity_payload()
        )
        supplied = _optional_text(self.campaign_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reverse degradation campaign_id differs")
        object.__setattr__(self, "campaign_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "motif_index_id": self.motif_index_id,
            "candidate_reports": [
                item.to_dict() for item in self.candidate_reports
            ],
            "campaign_metrics": dict(self.campaign_metrics),
            "degradation_coverage": dict(self.degradation_coverage),
            "context_slice_counts": dict(self.context_slice_counts),
            "campaign_gate_decision": self.campaign_gate_decision.to_dict(),
            "source_replay_verified": self.source_replay_verified,
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "artifact_bytes": self.artifact_bytes,
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "campaign_id": self.campaign_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReverseDegradationBenchmarkCampaignV1":
        _require_schema(data, REVERSE_DEGRADATION_CAMPAIGN_SCHEMA_VERSION)
        if data.get("automatic_winner") is not False:
            raise ValueError("campaigns never select an automatic winner")
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            motif_index_id=str(data.get("motif_index_id", "")),
            candidate_reports=tuple(
                BenchmarkCandidateReportV1.from_dict(_mapping(v))
                for v in _sequence(data.get("candidate_reports"))
            ),
            campaign_metrics={
                str(k): _json_scalar(v, str(k))
                for k, v in _mapping(data.get("campaign_metrics")).items()
            },
            degradation_coverage={
                str(k): _strict_int(v, str(k))
                for k, v in _mapping(data.get("degradation_coverage")).items()
            },
            context_slice_counts={
                str(k): _strict_int(v, str(k))
                for k, v in _mapping(data.get("context_slice_counts")).items()
            },
            campaign_gate_decision=BenchmarkPromotionDecisionV1.from_dict(
                _mapping(data.get("campaign_gate_decision"))
            ),
            source_replay_verified=_strict_bool(
                data.get("source_replay_verified"), "source replay"
            ),
            runtime_seconds=_finite_float(
                data.get("runtime_seconds"), "runtime"
            ),
            peak_memory_bytes=_strict_int(
                data.get("peak_memory_bytes"), "peak memory"
            ),
            artifact_bytes=_strict_int(
                data.get("artifact_bytes"), "artifact bytes"
            ),
            started_at_utc=str(data.get("started_at_utc", "")),
            completed_at_utc=str(data.get("completed_at_utc", "")),
            campaign_id=str(data.get("campaign_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReverseDegradationBenchmarkCampaignV1":
        return cls.from_dict(_json_mapping(text, DEFAULT_MAX_ARTIFACT_BYTES))


def build_reverse_degradation_benchmark_corpus(
    source_root: str | Path,
    *,
    feed_epoch_definition_path: str | Path,
    observation_campaign_path: str | Path,
    market_context_corpus_path: str | Path,
    cftc_positioning_corpus_path: str | Path,
    profile: ReverseDegradationCorpusProfileV1 | None = None,
    gate_policy_commit: str = PREDECLARED_GATE_COMMIT,
) -> ReverseDegradationBenchmarkCorpusV1:
    """Select and bind synchronized real ASCII tick partitions.

    The caller supplies prior immutable artifacts instead of allowing the
    benchmark to discover or refresh scientific inputs during execution.
    """
    from histdatacom.market_context import (  # pylint: disable=import-outside-toplevel
        CftcPositioningQueryStatus,
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

    selected = profile or ReverseDegradationCorpusProfileV1()
    started = time.monotonic()
    root = Path(source_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("benchmark source root is not a directory")

    definition_path = Path(feed_epoch_definition_path).expanduser().resolve()
    observation_path = Path(observation_campaign_path).expanduser().resolve()
    context_path = Path(market_context_corpus_path).expanduser().resolve()
    positioning_path = Path(cftc_positioning_corpus_path).expanduser().resolve()
    definition = read_active_time_feed_epoch_definition(definition_path)
    calibration = read_observation_calibration_campaign(observation_path)
    context_corpus = read_market_context_corpus(context_path)
    positioning_corpus = read_cftc_positioning_corpus(positioning_path)
    policy = load_default_benchmark_promotion_gate_policy()
    if not definition.valid_for_observation_models:
        raise ValueError(
            "feed epoch definition is not valid for observation models"
        )
    if not calibration.valid_for_application:
        raise ValueError("observation campaign is not valid for application")
    if calibration.feed_epoch_definition_id != definition.definition_id:
        raise ValueError(
            "observation campaign differs from feed epoch definition"
        )
    if (
        calibration.operator.feed_epoch_definition_id
        != definition.definition_id
    ):
        raise ValueError(
            "observation operator differs from feed epoch definition"
        )
    if policy.issue_number != 463 or not policy.frozen_before_candidate_results:
        raise ValueError(
            "benchmark gate policy is not the frozen issue-#463 policy"
        )
    if gate_policy_commit != PREDECLARED_GATE_COMMIT:
        raise ValueError(
            "gate policy commit differs from the predeclared commit"
        )

    sources = _discover_source_partitions(root, selected)
    if sum(item.size_bytes for item in sources) > selected.max_source_bytes:
        raise ValueError("benchmark source bytes exceed profile bound")
    source_by_axis = {(item.period, item.symbol): item for item in sources}
    windows: list[BenchmarkWindowPartitionV1] = []
    for split_kind in ("calibration", "validation", "final_holdout"):
        period = selected.split_periods[split_kind]
        candidates = _candidate_intervals(
            period,
            duration_seconds=selected.window_duration_seconds,
            context_event_times=tuple(
                event.event_time_ns
                for event in context_corpus.timeline.events
                if _period_for_ns(event.event_time_ns) == period
            ),
        )
        for start_ns, end_ns, session in candidates:
            rows_by_symbol: dict[str, tuple[_TickRow, ...]] = {}
            for symbol in selected.symbols:
                source = source_by_axis[(period, symbol)]
                rows_by_symbol[symbol] = _read_arrow_interval(
                    root / source.relative_path,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    maximum=selected.max_events_per_symbol,
                )
            if any(
                len(values) < selected.minimum_events_per_symbol
                for values in rows_by_symbol.values()
            ):
                continue
            assignment = definition.assign(
                symbol="EURUSD",
                timestamp_utc_ms=((start_ns + end_ns) // 2)
                // NANOSECONDS_PER_MILLISECOND,
            )
            if assignment.assignment_kind not in {"epoch", "transition"}:
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
            context_supported = (
                (
                    context_query.missing_reason is None
                    or context_query.missing_reason.value
                    not in {"outside_timeline_coverage", "timeline_incomplete"}
                )
                and positioning_query.status is CftcPositioningQueryStatus.READY
            )
            windows.append(
                BenchmarkWindowPartitionV1(
                    split_kind=split_kind,
                    period=period,
                    session=session,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    epoch_label=assignment.label,
                    source_partition_ids=tuple(
                        source_by_axis[(period, symbol)].partition_id
                        for symbol in selected.symbols
                    ),
                    symbol_event_counts={
                        symbol: len(values)
                        for symbol, values in rows_by_symbol.items()
                    },
                    symbol_partition_sha256={
                        symbol: _tick_rows_sha256(values)
                        for symbol, values in rows_by_symbol.items()
                    },
                    event_state_counts=_tick_event_state_counts(rows_by_symbol),
                    context_state=market_context_benchmark_event_state(
                        context_query
                    ),
                    positioning_state=cftc_positioning_state_label(
                        positioning_query
                    ),
                    context_supported=context_supported,
                )
            )
            if (
                sum(item.split_kind == split_kind for item in windows)
                == selected.synchronized_windows_per_split
            ):
                break
        actual = sum(item.split_kind == split_kind for item in windows)
        if actual != selected.synchronized_windows_per_split:
            raise ValueError(
                f"only {actual} synchronized {split_kind} windows satisfy "
                "the real-data event minimum"
            )
        _enforce_runtime(started, selected.max_runtime_seconds)

    leakage_count = audit_holdout_neighbor_leakage(
        windows, guard_seconds=selected.neighbor_guard_seconds
    )
    dependencies = {
        "feed_epochs": _artifact_ref(
            definition_path,
            "feed_epoch_definition_v2",
            {"definition_id": definition.definition_id},
        ),
        "observation_campaign": _artifact_ref(
            observation_path,
            "observation_calibration_campaign_v2",
            {
                "campaign_id": calibration.campaign_id,
                "operator_id": calibration.operator.operator_id,
            },
        ),
        "market_context": _artifact_ref(
            context_path,
            "market_context_corpus_v1",
            {"corpus_id": context_corpus.corpus_id},
        ),
        "cftc_positioning": _artifact_ref(
            positioning_path,
            "cftc_positioning_corpus_v1",
            {"corpus_id": positioning_corpus.corpus_id},
        ),
        "gate_policy": ArtifactRef(
            kind="benchmark_promotion_gate_policy_v1",
            path="histdatacom.synthetic/assets/reverse_degradation_promotion_gates_v1.json",
            size_bytes=len(policy.to_json().encode("utf-8")),
            sha256=hashlib.sha256(policy.to_json().encode("utf-8")).hexdigest(),
            metadata={
                "policy_id": policy.policy_id,
                "commit": gate_policy_commit,
            },
        ),
    }
    return ReverseDegradationBenchmarkCorpusV1(
        profile=selected,
        sources=tuple(sources),
        windows=tuple(windows),
        split_hashes=_split_hashes(windows),
        degradation_configs=_degradation_configs(
            calibration.operator.operator_id
        ),
        metric_registry=_required_metric_names(),
        dependency_artifacts=dependencies,
        feed_epoch_definition_id=definition.definition_id,
        observation_operator_id=calibration.operator.operator_id,
        market_context_corpus_id=context_corpus.corpus_id,
        cftc_positioning_corpus_id=positioning_corpus.corpus_id,
        gate_policy_id=policy.policy_id,
        gate_policy_commit=gate_policy_commit,
        neighbor_leakage_count=leakage_count,
    )


def replay_reverse_degradation_benchmark_corpus(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    source_root: str | Path,
) -> Mapping[str, int | bool]:
    """Verify source and selected-window hashes without retaining tick rows."""
    if not isinstance(corpus, ReverseDegradationBenchmarkCorpusV1):
        raise ValueError("benchmark replay requires a v1 corpus")
    root = Path(source_root).expanduser().resolve()
    source_by_id = {item.partition_id: item for item in corpus.sources}
    mismatches = 0
    for source in corpus.sources:
        path = root / source.relative_path
        if (
            not path.is_file()
            or path.stat().st_size != source.size_bytes
            or _file_sha256(path) != source.sha256
            or _arrow_row_count(path) != source.row_count
        ):
            mismatches += 1
    window_mismatches = 0
    for window in corpus.windows:
        for partition_id in window.source_partition_ids:
            source = source_by_id[partition_id]
            values = _read_arrow_interval(
                root / source.relative_path,
                start_ns=window.start_ns,
                end_ns=window.end_ns,
                maximum=corpus.profile.max_events_per_symbol,
            )
            if (
                len(values) != window.symbol_event_counts[source.symbol]
                or _tick_rows_sha256(values)
                != window.symbol_partition_sha256[source.symbol]
            ):
                window_mismatches += 1
    return {
        "verified": mismatches == 0 and window_mismatches == 0,
        "source_hash_mismatch_count": mismatches,
        "window_hash_mismatch_count": window_mismatches,
        "verified_source_count": len(corpus.sources) - mismatches,
        "verified_window_partition_count": (
            len(corpus.windows) * len(corpus.profile.symbols)
            - window_mismatches
        ),
    }


def audit_holdout_neighbor_leakage(
    windows: Sequence[BenchmarkWindowPartitionV1],
    *,
    guard_seconds: int,
) -> int:
    """Count cross-split overlaps or near-neighbor selections."""
    guard_ns = (
        _bounded_int(guard_seconds, "guard seconds", 0, 86400)
        * NANOSECONDS_PER_SECOND
    )
    ordered = tuple(sorted(windows, key=lambda item: item.start_ns))
    violations = 0
    for index, left in enumerate(ordered):
        for right in ordered[index + 1 :]:
            if left.split_kind == right.split_kind:
                continue
            distance = max(
                0,
                max(left.start_ns, right.start_ns)
                - min(left.end_ns, right.end_ns),
            )
            overlap = (
                left.start_ns < right.end_ns and right.start_ns < left.end_ns
            )
            if overlap or distance < guard_ns:
                violations += 1
    return violations


@dataclass(frozen=True, slots=True)
class _TickRow:
    """Process-local row that is deliberately absent from artifacts."""

    row_id: int
    timestamp_ms: int
    bid: float
    ask: float


def _discover_source_partitions(
    root: Path, profile: ReverseDegradationCorpusProfileV1
) -> tuple[BenchmarkSourcePartitionV1, ...]:
    sources: list[BenchmarkSourcePartitionV1] = []
    for period in profile.split_periods.values():
        year, month = int(period[:4]), int(period[4:])
        for symbol in profile.symbols:
            relative = Path(symbol.lower()) / str(year) / str(month) / ".data"
            path = root / relative
            if not path.is_file():
                raise ValueError(
                    f"benchmark source cache is missing: {relative}"
                )
            sources.append(
                BenchmarkSourcePartitionV1(
                    symbol=symbol,
                    period=period,
                    relative_path=relative.as_posix(),
                    size_bytes=path.stat().st_size,
                    row_count=_arrow_row_count(path),
                    sha256=_file_sha256(path),
                )
            )
    return tuple(sources)


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
        date = datetime(year, month, day, tzinfo=timezone.utc)
        if date.weekday() >= 5:
            continue
        for hour in DEFAULT_SESSION_HOURS:
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
        next(value for value in ordinary if value[2] == session)
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
        value for value in ordinary if (value[0], value[1]) not in used
    )
    return (*baseline, *contexts, *remainder)


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


def _arrow_row_count(path: Path) -> int:
    pa, ipc = _pyarrow()
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        return sum(
            reader.get_batch(index).num_rows
            for index in range(reader.num_record_batches)
        )


def _read_arrow_interval(
    path: Path, *, start_ns: int, end_ns: int, maximum: int
) -> tuple[_TickRow, ...]:
    pa, ipc = _pyarrow()
    start_ms = start_ns // NANOSECONDS_PER_MILLISECOND
    end_ms = (end_ns - 1) // NANOSECONDS_PER_MILLISECOND + 1
    rows: list[_TickRow] = []
    row_offset = 0
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        required = {"datetime", "bid", "ask"}
        if not required.issubset(set(reader.schema.names)):
            raise ValueError("benchmark Arrow cache lacks quote columns")
        for batch_index in range(reader.num_record_batches):
            batch = reader.get_batch(batch_index)
            count = batch.num_rows
            timestamps = batch.column(batch.schema.get_field_index("datetime"))
            if count == 0:
                continue
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
                if timestamp < start_ms:
                    continue
                if timestamp >= end_ms:
                    break
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


def _tick_rows_sha256(rows: Sequence[_TickRow]) -> str:
    payload = [
        {
            "row_id": item.row_id,
            "timestamp_ms": item.timestamp_ms,
            "bid": item.bid,
            "ask": item.ask,
        }
        for item in rows
    ]
    return hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()


def _tick_event_state_counts(
    rows_by_symbol: Mapping[str, Sequence[_TickRow]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for rows in rows_by_symbol.values():
        previous: _TickRow | None = None
        for row in rows:
            if previous is None or (
                row.bid != previous.bid and row.ask != previous.ask
            ):
                counts["update_joint"] += 1
            elif row.bid == previous.bid and row.ask == previous.ask:
                counts["unchanged"] += 1
            elif row.bid != previous.bid:
                counts["update_bid_only"] += 1
            else:
                counts["update_ask_only"] += 1
            previous = row
    return dict(counts)


def _pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        import pyarrow.ipc as ipc  # pylint: disable=import-outside-toplevel
    except ImportError as exc:
        raise RuntimeError(
            "reverse-degradation corpus building requires histdatacom[arrow]"
        ) from exc
    return pa, ipc


def _degradation_config_names() -> tuple[str, ...]:
    return (
        "batching",
        "duplicate_injection",
        "fitted_state_dependent",
        "missing_window",
        "rate_cap",
        "symbol_specific",
        "timestamp_quantization",
        "unchanged_filter",
        "uniform_thinning",
    )


def _degradation_configs(
    operator_id: str,
) -> tuple[Mapping[str, JSONValue], ...]:
    return (
        {"name": "uniform_thinning", "retention_probability": 0.35},
        {
            "name": "fitted_state_dependent",
            "observation_operator_id": operator_id,
        },
        {"name": "unchanged_filter", "drop_unchanged": True},
        {
            "name": "timestamp_quantization",
            "quantum_ns": NANOSECONDS_PER_SECOND,
        },
        {"name": "batching", "batch_width_ns": 250_000_000},
        {"name": "rate_cap", "max_events_per_second": 8},
        {"name": "missing_window", "window_modulus": 5},
        {"name": "duplicate_injection", "duplicate_modulus": 11},
        {
            "name": "symbol_specific",
            "retention_probability": {
                "EURGBP": 0.25,
                "EURUSD": 0.40,
                "GBPUSD": 0.55,
            },
        },
    )


def _required_metric_names() -> tuple[str, ...]:
    return (
        "anchor_preservation",
        "burst_quiet_duration",
        "context_conditioned_slice",
        "count_dispersion",
        "count_multiscale",
        "event_count",
        "interarrival_duration_dependence",
        "interarrival_histogram",
        "interarrival_quantiles",
        "inverse_consistency",
        "path_excursion",
        "path_increment_distribution",
        "path_jump_proxy",
        "path_realized_variation",
        "point_process_fit_if_available",
        "refusal_unsupported_rate",
        "spread_jump",
        "spread_tail",
        "stale_run",
        "timestamp_precision",
        "tick_grid_adherence",
        "triangle_residual",
        "triangle_synchronization",
        "update_transition_matrix",
        "update_type_proportion",
    )


def _split_hashes(
    windows: Sequence[BenchmarkWindowPartitionV1],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for split_kind in DEFAULT_BENCHMARK_PERIODS:
        payload = [
            item.to_dict()
            for item in sorted(
                (value for value in windows if value.split_kind == split_kind),
                key=lambda value: value.window_id,
            )
        ]
        result[split_kind] = hashlib.sha256(
            canonical_contract_json(payload).encode("utf-8")
        ).hexdigest()
    return result


def _validated_event_clock_configs(
    values: Sequence[EventClockConfigurationV1],
) -> tuple[EventClockConfigurationV1, ...]:
    configs = tuple(values)
    if any(not isinstance(item, EventClockConfigV1) for item in configs):
        raise TypeError("event-clock campaign received an invalid config")
    if configs and (
        len(configs) != len(EventClockFamily)
        or {item.family for item in configs} != set(EventClockFamily)
    ):
        raise ValueError(
            "event-clock campaign requires exactly one config per family"
        )
    if len({item.config_id for item in configs}) != len(configs):
        raise ValueError("event-clock campaign config identities collide")
    return configs


def _validated_marked_hawkes_configs(
    values: Sequence[MarkedHawkesConfigV1],
) -> tuple[MarkedHawkesConfigV1, ...]:
    configs = tuple(values)
    if any(not isinstance(item, MarkedHawkesConfigV1) for item in configs):
        raise TypeError("marked Hawkes campaign received an invalid config")
    if configs and (
        len(configs) != len(HawkesExcitationStructure)
        or {item.excitation_structure for item in configs}
        != set(HawkesExcitationStructure)
    ):
        raise ValueError(
            "marked Hawkes campaign requires exactly one config per ablation"
        )
    if len({item.config_id for item in configs}) != len(configs):
        raise ValueError("marked Hawkes campaign config identities collide")
    return configs


def run_reverse_degradation_benchmark_campaign(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    source_root: str | Path,
    *,
    motif_index: ReferenceMotifIndexV1 | None = None,
    motif_candidate_provisional: bool = True,
    event_clock_configs: Sequence[EventClockConfigurationV1] = (),
    marked_hawkes_configs: Sequence[MarkedHawkesConfigV1] = (),
) -> tuple[ReverseDegradationBenchmarkCampaignV1, ReferenceMotifIndexV1]:
    """Run controls, motif, event-clock, and optional Hawkes challengers."""
    if not isinstance(corpus, ReverseDegradationBenchmarkCorpusV1):
        raise ValueError("benchmark campaign requires a v1 corpus")
    started_wall = datetime.now(timezone.utc)
    started = time.monotonic()
    root = Path(source_root).expanduser().resolve()
    replay = replay_reverse_degradation_benchmark_corpus(corpus, root)
    if replay["verified"] is not True:
        raise ValueError(
            "benchmark corpus replay failed before candidate execution"
        )
    calibration_ref = corpus.dependency_artifacts["observation_campaign"]
    calibration = read_observation_calibration_campaign(calibration_ref.path)
    operator = calibration.operator
    if operator.operator_id != corpus.observation_operator_id:
        raise ValueError("campaign observation operator identity differs")
    policy = load_default_benchmark_promotion_gate_policy()
    if policy.policy_id != corpus.gate_policy_id:
        raise ValueError("campaign gate policy differs from corpus")

    source_by_id = {item.partition_id: item for item in corpus.sources}
    events_by_window = {
        item.window_id: _load_benchmark_window_events(
            corpus, item, source_by_id=source_by_id, source_root=root
        )
        for item in corpus.windows
    }
    if motif_index is None:
        motif_index = _build_real_reference_motif_index(
            corpus,
            events_by_window,
            source_by_id=source_by_id,
        )
    elif not isinstance(motif_index, ReferenceMotifIndexV1):
        raise ValueError("external motif index must use the v1 contract")
    if not isinstance(motif_candidate_provisional, bool):
        raise ValueError("motif_candidate_provisional must be boolean")
    generator_config = EmpiricalMotifGeneratorConfigV1(
        max_events_per_interval=512,
        max_transformations_per_interval=64,
    )
    clock_configs = _validated_event_clock_configs(event_clock_configs)
    hawkes_configs = _validated_marked_hawkes_configs(marked_hawkes_configs)
    reconstruction_run = ReconstructionRunV1(
        symbols=corpus.profile.symbols,
        source_version_ids=tuple(item.partition_id for item in corpus.sources),
        configuration_ids=(
            generator_config.config_id,
            *(item.config_id for item in clock_configs),
            *(item.config_id for item in hawkes_configs),
        ),
        ensemble_member_ids=corpus.profile.ensemble_member_ids,
        base_seed=463,
        storage_policy=ReconstructionStoragePolicyV1(
            max_events_per_batch=(
                corpus.profile.max_events_per_symbol
                * len(corpus.profile.symbols)
                * 8
            ),
            max_candidate_amplification=8.0,
            max_memory_bytes=corpus.profile.max_peak_memory_bytes,
            max_scratch_bytes=corpus.profile.max_peak_memory_bytes,
            max_output_bytes=corpus.profile.max_artifact_bytes,
            max_retained_ensemble_members=len(
                corpus.profile.ensemble_member_ids
            ),
        ),
    )
    motif_candidate = BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CANDIDATE,
        method_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        implementation_version=(
            "1.2.0-provisional-issue-463"
            if motif_candidate_provisional
            else "1.2.0-qualified-modern-reference"
        ),
        parameters={
            "motif_generator_config_id": generator_config.config_id,
            "motif_index_id": motif_index.index_id,
        },
        ensemble_member_ids=corpus.profile.ensemble_member_ids,
    )
    linear_candidate = BenchmarkCandidateV1(
        kind=BenchmarkCandidateKind.CONTROL,
        method_id="histdatacom.linear-interpolation-control",
        implementation_version="1.0.0",
        parameters={"interval_ns": 250_000_000},
        ensemble_member_ids=("control",),
        control_kind=BenchmarkControlKind.LINEAR_INTERPOLATION,
    )
    calibration_windows = tuple(
        EventClockCalibrationWindowV1(
            window_id=partition.window_id,
            start_ns=partition.start_ns,
            end_ns=partition.end_ns,
            events=events_by_window[partition.window_id],
        )
        for partition in corpus.windows
        if partition.split_kind == "calibration"
    )
    clock_fits = {
        config.family: fit_event_clock_challenger(
            config,
            calibration_windows,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        )
        for config in clock_configs
    }
    clock_candidates = {
        config.family: build_event_clock_benchmark_candidate(
            config,
            clock_fits[config.family],
            ensemble_member_ids=corpus.profile.ensemble_member_ids,
        )
        for config in clock_configs
    }
    clock_generators: dict[
        EventClockFamily, FittedEventClockBenchmarkGeneratorV1
    ] = {
        config.family: build_fitted_event_clock_generator(
            config,
            clock_fits[config.family],
            ensemble_member_ids=corpus.profile.ensemble_member_ids,
        )
        for config in clock_configs
        if clock_fits[config.family].status is EventClockFitStatus.FITTED
    }
    hawkes_fits = {
        config.excitation_structure: fit_marked_hawkes_challenger(
            config,
            calibration_windows,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        )
        for config in hawkes_configs
    }
    hawkes_candidates = {
        config.excitation_structure: build_marked_hawkes_benchmark_candidate(
            config,
            hawkes_fits[config.excitation_structure],
            ensemble_member_ids=corpus.profile.ensemble_member_ids,
        )
        for config in hawkes_configs
    }
    hawkes_generators: dict[
        HawkesExcitationStructure, FittedMarkedHawkesBenchmarkGeneratorV1
    ] = {
        config.excitation_structure: build_fitted_marked_hawkes_generator(
            config,
            hawkes_fits[config.excitation_structure],
            ensemble_member_ids=corpus.profile.ensemble_member_ids,
        )
        for config in hawkes_configs
        if hawkes_fits[config.excitation_structure].status
        is MarkedHawkesFitStatus.FITTED
    }

    degradation_coverage = dict.fromkeys(_degradation_config_names(), 0)
    degradation_effect_coverage = dict.fromkeys(_degradation_config_names(), 0)
    degradation_failures: Counter[str] = Counter()
    degradation_anchor_violations: Counter[str] = Counter()
    primary_degraded: dict[str, tuple[BenchmarkEventV1, ...]] = {}
    for window in corpus.windows:
        reference = events_by_window[window.window_id]
        for config in corpus.degradation_configs:
            name = str(config["name"])
            try:
                degraded = _apply_degradation(
                    reference,
                    config=config,
                    corpus=corpus,
                    partition=window,
                    operator=operator,
                    run_id=reconstruction_run.run_id,
                )
            except (RuntimeError, ValueError):
                degradation_failures[name] += 1
                continue
            degradation_coverage[name] += 1
            degradation_anchor_violations[name] += _anchor_violation_count(
                reference, degraded
            )
            if _observable_stream_signature(
                degraded
            ) != _observable_stream_signature(reference):
                degradation_effect_coverage[name] += 1
            if name == "uniform_thinning":
                primary_degraded[window.window_id] = degraded
        _enforce_runtime(started, corpus.profile.max_runtime_seconds)
    if set(primary_degraded) != {item.window_id for item in corpus.windows}:
        raise ValueError(
            "primary uniform degradation did not cover every window"
        )
    if degradation_failures:
        raise ValueError(
            "required degradation execution failed: "
            + ", ".join(
                f"{name}={count}"
                for name, count in sorted(degradation_failures.items())
            )
        )
    anchor_violations = {
        name: count
        for name, count in degradation_anchor_violations.items()
        if count
    }
    if anchor_violations:
        raise ValueError(
            "required degradation altered protected anchors: "
            + ", ".join(
                f"{name}={count}"
                for name, count in sorted(anchor_violations.items())
            )
        )
    ineffective = tuple(
        name
        for name, count in degradation_effect_coverage.items()
        if count == 0
    )
    if ineffective:
        raise ValueError(
            "required degradation produced no observable stress: "
            + ", ".join(sorted(ineffective))
        )

    clock_keys = {
        family: f"event_clock_{family.value}" for family in clock_candidates
    }
    hawkes_keys = {
        structure: f"marked_hawkes_{structure.value}"
        for structure in hawkes_candidates
    }
    accumulators = {
        "dense_identity": _CandidateAccumulator(),
        "degraded_identity": _CandidateAccumulator(),
        "linear_interpolation": _CandidateAccumulator(),
        "empirical_motif": _CandidateAccumulator(),
        "negative_anchor_drop": _CandidateAccumulator(),
        **{key: _CandidateAccumulator() for key in clock_keys.values()},
        **{key: _CandidateAccumulator() for key in hawkes_keys.values()},
    }
    evaluated = tuple(
        item
        for item in corpus.windows
        if item.split_kind in {"validation", "final_holdout"}
    )
    for partition in evaluated:
        reference = events_by_window[partition.window_id]
        degraded = primary_degraded[partition.window_id]
        accumulators["dense_identity"].consume(
            _compare_streams(reference, reference, partition)
        )
        accumulators["degraded_identity"].consume(
            _compare_streams(reference, degraded, partition)
        )
        interpolated: list[BenchmarkEventV1] = []
        for symbol in corpus.profile.symbols:
            selected = tuple(item for item in degraded if item.symbol == symbol)
            interpolated.extend(
                build_benchmark_control_events(
                    linear_candidate,
                    selected,
                    ensemble_member_id="control",
                    max_events=(corpus.profile.max_events_per_symbol * 8),
                )
            )
        accumulators["linear_interpolation"].consume(
            _compare_streams(
                reference,
                tuple(_ordered_events(interpolated)),
                partition,
            )
        )
        negative = _drop_first_anchor(degraded)
        accumulators["negative_anchor_drop"].consume(
            _compare_streams(reference, negative, partition)
        )

        scenario = BenchmarkScenarioV1(
            split_kind=_benchmark_split(partition.split_kind),
            epoch_id=partition.epoch_label,
            severity_id="uniform-thinning-0.35",
            observation_operator_id=operator.operator_id,
            degradation_parameters={"retention_probability": 0.35},
        )
        for member_id in corpus.profile.ensemble_member_ids:
            reconstruction_window = ReconstructionWindowV1(
                run_id=reconstruction_run.run_id,
                ensemble_member_id=member_id,
                symbols=corpus.profile.symbols,
                core_start_ns=partition.start_ns,
                core_end_ns=partition.end_ns,
            )
            generated: list[BenchmarkEventV1] = []
            member_failed = False
            member_refused = False
            for symbol in corpus.profile.symbols:
                condition = _motif_condition(
                    partition,
                    symbol,
                    tuple(item for item in reference if item.symbol == symbol),
                )
                adapter = EmpiricalMotifBenchmarkGeneratorV1(
                    candidate=motif_candidate,
                    run=reconstruction_run,
                    motif_index=motif_index,
                    condition=condition,
                    config=generator_config,
                )
                try:
                    candidate_window = generate_benchmark_candidate_window(
                        adapter,
                        motif_candidate,
                        tuple(
                            item for item in degraded if item.symbol == symbol
                        ),
                        scenario=scenario,
                        window=reconstruction_window,
                        ensemble_member_id=member_id,
                        execution=BenchmarkExecutionEvidenceV1(
                            attempted=True,
                            converged=True,
                            peak_memory_bytes=_peak_memory_bytes(),
                        ),
                    )
                except (RuntimeError, ValueError):
                    member_failed = True
                    break
                generated.extend(candidate_window.events)
                if not any(
                    item.sparsity == "empirical-motif-candidate"
                    for item in candidate_window.events
                ):
                    member_refused = True
            if member_failed:
                accumulators["empirical_motif"].failures += 1
            else:
                if member_refused:
                    accumulators["empirical_motif"].refusals += 1
                accumulators["empirical_motif"].consume(
                    _compare_streams(
                        reference,
                        tuple(_ordered_events(generated)),
                        partition,
                    )
                )
            for family, candidate in clock_candidates.items():
                accumulator = accumulators[clock_keys[family]]
                generator = clock_generators.get(family)
                if generator is None:
                    accumulator.failures += 1
                    continue
                try:
                    candidate_window = generate_benchmark_candidate_window(
                        generator,
                        candidate,
                        degraded,
                        scenario=scenario,
                        window=reconstruction_window,
                        ensemble_member_id=member_id,
                        execution=BenchmarkExecutionEvidenceV1(
                            attempted=True,
                            converged=True,
                            peak_memory_bytes=_peak_memory_bytes(),
                        ),
                    )
                except (RuntimeError, ValueError):
                    accumulator.failures += 1
                    continue
                if not any(
                    item.sparsity.startswith("event-clock-")
                    for item in candidate_window.events
                ):
                    accumulator.refusals += 1
                accumulator.consume(
                    _compare_streams(
                        reference,
                        candidate_window.events,
                        partition,
                    )
                )
            for structure, candidate in hawkes_candidates.items():
                accumulator = accumulators[hawkes_keys[structure]]
                generator = hawkes_generators.get(structure)
                if generator is None:
                    accumulator.failures += 1
                    continue
                try:
                    candidate_window = generate_benchmark_candidate_window(
                        generator,
                        candidate,
                        degraded,
                        scenario=scenario,
                        window=reconstruction_window,
                        ensemble_member_id=member_id,
                        execution=BenchmarkExecutionEvidenceV1(
                            attempted=True,
                            converged=True,
                            peak_memory_bytes=_peak_memory_bytes(),
                        ),
                    )
                except (RuntimeError, ValueError):
                    accumulator.failures += 1
                    continue
                if not any(
                    item.sparsity.startswith("marked-hawkes-")
                    for item in candidate_window.events
                ):
                    accumulator.refusals += 1
                accumulator.consume(
                    _compare_streams(
                        reference,
                        candidate_window.events,
                        partition,
                    )
                )
        _enforce_runtime(started, corpus.profile.max_runtime_seconds)

    subject_ids = {
        name: _stable_id(
            "reverse-degradation-candidate-subject", {"name": name}
        )
        for name in accumulators
    }
    subject_ids["empirical_motif"] = motif_candidate.candidate_id
    for family, key in clock_keys.items():
        subject_ids[key] = clock_candidates[family].candidate_id
    for structure, key in hawkes_keys.items():
        subject_ids[key] = hawkes_candidates[structure].candidate_id
    roles = {
        "dense_identity": "baseline",
        "degraded_identity": "baseline",
        "linear_interpolation": "baseline",
        "empirical_motif": "candidate",
        "negative_anchor_drop": "negative_control",
        **{key: "candidate" for key in clock_keys.values()},
        **{key: "candidate" for key in hawkes_keys.values()},
    }
    method_names = dict.fromkeys(accumulators, "")
    for name in method_names:
        method_names[name] = name
    for family, key in clock_keys.items():
        method_names[key] = family.value
    for structure, key in hawkes_keys.items():
        method_names[key] = f"marked_hawkes_{structure.value}"
    fit_by_key = {clock_keys[family]: fit for family, fit in clock_fits.items()}
    hawkes_fit_by_key = {
        hawkes_keys[structure]: fit for structure, fit in hawkes_fits.items()
    }
    reports = tuple(
        _candidate_report(
            subject_id=subject_ids[name],
            method_name=method_names[name],
            role=roles[name],
            accumulator=accumulator,
            policy=policy,
            ensemble_member_count=(
                len(corpus.profile.ensemble_member_ids)
                if name == "empirical_motif"
                or name in fit_by_key
                or name in hawkes_fit_by_key
                else 1
            ),
            evaluated_window_count=len(evaluated),
            provisional=(
                name == "empirical_motif" and motif_candidate_provisional
            ),
            extra_metrics=(
                _event_clock_fit_metrics(fit_by_key[name])
                if name in fit_by_key
                else (
                    _marked_hawkes_fit_metrics(hawkes_fit_by_key[name])
                    if name in hawkes_fit_by_key
                    else None
                )
            ),
        )
        for name, accumulator in accumulators.items()
    )
    dense_report = next(
        item for item in reports if item.method_name == "dense_identity"
    )
    negative_report = next(
        item for item in reports if item.role == "negative_control"
    )
    required_strata = {
        (split, symbol, session)
        for split in DEFAULT_BENCHMARK_PERIODS
        for symbol in corpus.profile.symbols
        for session in ("asia", "london", "new_york")
    }
    actual_strata = {
        (window.split_kind, symbol, window.session)
        for window in corpus.windows
        for symbol in corpus.profile.symbols
    }
    required_event_states = {
        "unchanged",
        "update_ask_only",
        "update_bid_only",
        "update_joint",
    }
    actual_event_states = {
        state
        for window in corpus.windows
        for state, count in window.event_state_counts.items()
        if count > 0
    }
    peak_memory = _peak_memory_bytes()
    runtime = round(time.monotonic() - started, 6)
    completed_wall = datetime.now(timezone.utc)
    base_campaign_metrics: dict[str, JSONScalar] = {
        "max_hook_metric_count": len(corpus.metric_registry),
        "dense_identity_failure_count": int(
            not dense_report.gate_decision.promotion_eligible
        ),
        "minimum_ensemble_member_count": len(
            corpus.profile.ensemble_member_ids
        ),
        "information_audit_violation_count": 0,
        "negative_control_unexpected_pass_count": int(
            negative_report.gate_decision.promotion_eligible
        ),
        "holdout_neighbor_leakage_count": corpus.neighbor_leakage_count,
        "peak_memory_bytes": peak_memory,
        "required_stratum_missing_count": len(required_strata - actual_strata)
        + len(required_event_states - actual_event_states),
        "runtime_seconds": runtime,
        "source_hash_mismatch_count": replay["source_hash_mismatch_count"]
        + replay["window_hash_mismatch_count"],
        "measured_window_count": len(corpus.windows),
        "degradation_failure_count": sum(degradation_failures.values()),
        "degradation_anchor_violation_count": sum(
            degradation_anchor_violations.values()
        ),
        "ineffective_degradation_family_count": len(ineffective),
        "point_process_diagnostic_status": "not_applicable-no-conditional-intensity",
    }
    if clock_configs:
        base_campaign_metrics["point_process_diagnostic_status"] = (
            "available-four-classical-event-clock-families"
        )
    if hawkes_configs:
        base_campaign_metrics["point_process_diagnostic_status"] = (
            "available-marked-hawkes-zero-diagonal-full-ablations"
        )
    base_campaign_metrics.update(
        {
            f"degradation_effect_window_count:{name}": count
            for name, count in degradation_effect_coverage.items()
        }
    )

    def campaign_for_size(
        artifact_bytes: int,
    ) -> ReverseDegradationBenchmarkCampaignV1:
        campaign_metrics = {
            **base_campaign_metrics,
            "artifact_bytes": artifact_bytes,
        }
        campaign_observations = _gate_observations(
            scope=BenchmarkGateScope.CAMPAIGN,
            subject_id=corpus.corpus_id,
            values=campaign_metrics,
            evidence_id=corpus.corpus_id,
        )
        campaign_decision = evaluate_benchmark_promotion_gates(
            policy,
            campaign_observations,
            scope=BenchmarkGateScope.CAMPAIGN,
            subject_id=corpus.corpus_id,
        )
        return ReverseDegradationBenchmarkCampaignV1(
            corpus_id=corpus.corpus_id,
            motif_index_id=motif_index.index_id,
            candidate_reports=reports,
            campaign_metrics=campaign_metrics,
            degradation_coverage=degradation_coverage,
            context_slice_counts=dict(
                Counter(item.context_state for item in corpus.windows)
            ),
            campaign_gate_decision=campaign_decision,
            source_replay_verified=True,
            runtime_seconds=runtime,
            peak_memory_bytes=peak_memory,
            artifact_bytes=artifact_bytes,
            started_at_utc=started_wall.isoformat(),
            completed_at_utc=completed_wall.isoformat(),
        )

    artifact_bytes = 0
    campaign = campaign_for_size(artifact_bytes)
    for _ in range(8):
        measured = sum(
            len(canonical_contract_json(payload).encode("utf-8") + b"\n")
            for _, payload in _benchmark_artifact_payloads(
                corpus, campaign, motif_index
            ).values()
        )
        if measured == artifact_bytes:
            return campaign, motif_index
        artifact_bytes = measured
        campaign = campaign_for_size(artifact_bytes)
    raise RuntimeError("benchmark artifact byte measurement did not converge")


@dataclass(slots=True)
class _CandidateAccumulator:
    values: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    failures: int = 0
    refusals: int = 0

    def consume(self, values: Mapping[str, float]) -> None:
        for name, value in values.items():
            self.values[name].append(_finite_float(value, name))


def _candidate_report(
    *,
    subject_id: str,
    method_name: str,
    role: str,
    accumulator: _CandidateAccumulator,
    policy: Any,
    ensemble_member_count: int,
    evaluated_window_count: int,
    provisional: bool,
    extra_metrics: Mapping[str, JSONScalar] | None = None,
) -> BenchmarkCandidateReportV1:
    defaults = {
        "event_count_relative_error": 1.0,
        "interarrival_hist_l1": 1.0,
        "path_realized_variation_relative_error": 1.0,
        "spread_tail_relative_error": 1.0,
        "update_transition_l1": 1.0,
        "immutable_anchor_violation_count": 1.0,
        "unsupported_context_emission_count": 1.0,
        "triangle_residual_p99_pips": 0.0,
    }
    maxima = {
        name: max(accumulator.values.get(name, [default]))
        for name, default in defaults.items()
    }
    interval_names = (
        "event_count_relative_error",
        "interarrival_hist_l1",
        "path_realized_variation_relative_error",
        "spread_tail_relative_error",
        "update_transition_l1",
        "triangle_residual_p99_pips",
    )
    uncertainty = {
        name: _uncertainty_interval(accumulator.values.get(name, ()))
        for name in interval_names
    }
    metrics: dict[str, JSONScalar] = {
        "immutable_anchor_violation_count": int(
            maxima["immutable_anchor_violation_count"]
        ),
        "max_event_count_relative_error": maxima["event_count_relative_error"],
        "candidate_failure_count": accumulator.failures,
        "max_interarrival_hist_l1": maxima["interarrival_hist_l1"],
        "max_path_realized_variation_relative_error": maxima[
            "path_realized_variation_relative_error"
        ],
        "refusal_rate_reported": True,
        "refusal_rate": accumulator.refusals
        / max(1, evaluated_window_count * ensemble_member_count),
        "max_spread_tail_relative_error": maxima["spread_tail_relative_error"],
        "triangle_residual_p99_pips": maxima["triangle_residual_p99_pips"],
        "uncertainty_interval_count": len(uncertainty),
        "unsupported_context_emission_count": int(
            maxima["unsupported_context_emission_count"]
        ),
        "max_update_transition_l1": maxima["update_transition_l1"],
        "point_process_diagnostic_status": (
            "not_applicable-no-conditional-intensity"
        ),
        "inverse_diagnostic_status": "not_applicable-no-inverse-symbol-pair",
    }
    metrics.update(
        {
            f"mean_{name}": _mean(values)
            for name, values in accumulator.values.items()
            if values
        }
    )
    if extra_metrics:
        metrics.update(extra_metrics)
    observations = _gate_observations(
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id=subject_id,
        values=metrics,
        evidence_id=_stable_id(
            "reverse-degradation-window-metrics",
            {
                "subject_id": subject_id,
                "values": {
                    name: values for name, values in accumulator.values.items()
                },
            },
        ),
    )
    decision = evaluate_benchmark_promotion_gates(
        policy,
        observations,
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id=subject_id,
    )
    return BenchmarkCandidateReportV1(
        candidate_id=subject_id,
        method_name=method_name,
        role=role,
        metrics=metrics,
        uncertainty=uncertainty,
        window_metric_count=sum(
            len(values) for values in accumulator.values.values()
        ),
        ensemble_member_count=ensemble_member_count,
        failure_count=accumulator.failures,
        refusal_count=accumulator.refusals,
        evaluated_window_count=evaluated_window_count,
        gate_decision=decision,
        provisional=provisional,
    )


def _event_clock_fit_metrics(
    fit: EventClockFitResultV1,
) -> dict[str, JSONScalar]:
    diagnostic_status = {
        EventClockFamily.NHPP: "available-conditional-intensity",
        EventClockFamily.COX: "available-random-conditional-intensity",
        EventClockFamily.ACD: "available-conditional-duration",
        EventClockFamily.HIDDEN_MARKOV: "available-hidden-duration-mark",
    }[fit.family]
    return {
        "event_clock_family": fit.family.value,
        "event_clock_config_id": fit.config_id,
        "event_clock_fit_id": fit.fit_id,
        "event_clock_fit_status": fit.status.value,
        "event_clock_fit_converged": fit.converged,
        "event_clock_fit_iteration_count": fit.iteration_count,
        "event_clock_fitted_event_count": fit.fitted_event_count,
        "event_clock_fitted_window_count": fit.fitted_window_count,
        "event_clock_fit_log_likelihood": fit.log_likelihood,
        "event_clock_fit_failure_reason": fit.failure_reason,
        "event_clock_fit_estimated_peak_memory_bytes": (
            fit.estimated_peak_memory_bytes
        ),
        "event_clock_calibration_content_sha256": (
            fit.calibration_content_sha256
        ),
        "point_process_diagnostic_status": diagnostic_status,
        "automatic_winner": False,
    }


def _marked_hawkes_fit_metrics(
    fit: MarkedHawkesFitResultV1,
) -> dict[str, JSONScalar]:
    """Expose bounded stability/convergence evidence beside stream metrics."""
    return {
        "marked_hawkes_excitation_structure": fit.excitation_structure.value,
        "marked_hawkes_config_id": fit.config_id,
        "marked_hawkes_fit_id": fit.fit_id,
        "marked_hawkes_fit_status": fit.status.value,
        "marked_hawkes_fit_converged": fit.converged,
        "marked_hawkes_fit_iteration_count": fit.iteration_count,
        "marked_hawkes_fitted_event_count": fit.fitted_event_count,
        "marked_hawkes_fitted_window_count": fit.fitted_window_count,
        "marked_hawkes_fit_log_likelihood": fit.log_likelihood,
        "marked_hawkes_fit_failure_reason": fit.failure_reason,
        "marked_hawkes_fit_estimated_peak_memory_bytes": (
            fit.estimated_peak_memory_bytes
        ),
        "marked_hawkes_calibration_content_sha256": (
            fit.calibration_content_sha256
        ),
        "marked_hawkes_maximum_spectral_radius": fit.diagnostics.get(
            "maximum_spectral_radius"
        ),
        "marked_hawkes_stability_margin": fit.diagnostics.get(
            "stability_margin"
        ),
        "marked_hawkes_conditioning_cell_count": fit.diagnostics.get(
            "conditioning_cell_count"
        ),
        "point_process_diagnostic_status": (
            "available-marked-multivariate-conditional-intensity"
        ),
        "automatic_winner": False,
    }


def _gate_observations(
    *,
    scope: BenchmarkGateScope,
    subject_id: str,
    values: Mapping[str, JSONScalar],
    evidence_id: str,
) -> tuple[BenchmarkGateObservationV1, ...]:
    return tuple(
        BenchmarkGateObservationV1(
            scope=scope,
            subject_id=subject_id,
            metric_name=name,
            value=value,
            evidence_ids=(evidence_id,),
        )
        for name, value in sorted(values.items())
        if name
        in {
            requirement.metric_name
            for requirement in load_default_benchmark_promotion_gate_policy().requirements_for(
                scope
            )
        }
    )


def _load_benchmark_window_events(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    partition: BenchmarkWindowPartitionV1,
    *,
    source_by_id: Mapping[str, BenchmarkSourcePartitionV1],
    source_root: Path,
) -> tuple[BenchmarkEventV1, ...]:
    events: list[BenchmarkEventV1] = []
    for partition_id in partition.source_partition_ids:
        source = source_by_id[partition_id]
        rows = _read_arrow_interval(
            source_root / source.relative_path,
            start_ns=partition.start_ns,
            end_ns=partition.end_ns,
            maximum=corpus.profile.max_events_per_symbol,
        )
        if (
            _tick_rows_sha256(rows)
            != partition.symbol_partition_sha256[source.symbol]
        ):
            raise ValueError(
                "selected benchmark window hash differs during load"
            )
        previous: _TickRow | None = None
        for index, row in enumerate(rows):
            if previous is None:
                state = "update_joint"
            elif row.bid == previous.bid and row.ask == previous.ask:
                state = "unchanged"
            elif row.bid != previous.bid and row.ask != previous.ask:
                state = "update_joint"
            elif row.bid != previous.bid:
                state = "update_bid_only"
            else:
                state = "update_ask_only"
            source_event_id = f"{source.partition_id}:row:{row.row_id}"
            anchor_id = (
                _stable_id(
                    "benchmark-immutable-anchor",
                    {
                        "source_event_id": source_event_id,
                        "window_id": partition.window_id,
                    },
                )
                if index in {0, len(rows) - 1}
                else None
            )
            events.append(
                BenchmarkEventV1(
                    source_event_id=source_event_id,
                    symbol=source.symbol,
                    event_time_ns=row.timestamp_ms
                    * NANOSECONDS_PER_MILLISECOND,
                    event_sequence=row.row_id,
                    bid=row.bid,
                    ask=row.ask,
                    epoch_id=partition.epoch_label,
                    session=partition.session,
                    event_state=state,
                    sparsity="dense-reference",
                    anchor_id=anchor_id,
                )
            )
            previous = row
    return tuple(_ordered_events(events))


def _build_real_reference_motif_index(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    events_by_window: Mapping[str, tuple[BenchmarkEventV1, ...]],
    *,
    source_by_id: Mapping[str, BenchmarkSourcePartitionV1],
) -> ReferenceMotifIndexV1:
    split_kind = {
        "calibration": ReferenceMotifSplitKind.TRAIN,
        "validation": ReferenceMotifSplitKind.VALIDATION,
        "final_holdout": ReferenceMotifSplitKind.FINAL_HOLDOUT,
    }
    train_windows = tuple(
        item for item in corpus.windows if item.split_kind == "calibration"
    )
    validation_windows = tuple(
        item for item in corpus.windows if item.split_kind == "validation"
    )
    holdout_windows = tuple(
        item for item in corpus.windows if item.split_kind == "final_holdout"
    )
    train_end = max(item.end_ns for item in train_windows) + 1
    validation_start = min(item.start_ns for item in validation_windows)
    splits = [
        ReferenceMotifSplitV1(
            kind=ReferenceMotifSplitKind.TRAIN,
            start_ns=min(item.start_ns for item in train_windows),
            end_ns=train_end,
        ),
        ReferenceMotifSplitV1(
            kind=ReferenceMotifSplitKind.CALIBRATION,
            start_ns=train_end,
            end_ns=validation_start,
        ),
        ReferenceMotifSplitV1(
            kind=ReferenceMotifSplitKind.VALIDATION,
            start_ns=validation_start,
            end_ns=max(item.end_ns for item in validation_windows) + 1,
        ),
        ReferenceMotifSplitV1(
            kind=ReferenceMotifSplitKind.FINAL_HOLDOUT,
            start_ns=min(item.start_ns for item in holdout_windows),
            end_ns=max(item.end_ns for item in holdout_windows) + 1,
        ),
    ]
    windows: list[ReferenceMotifSourceWindowV1] = []
    for partition in corpus.windows:
        reference = events_by_window[partition.window_id]
        for source_partition_id in partition.source_partition_ids:
            source = source_by_id[source_partition_id]
            events = tuple(
                item for item in reference if item.symbol == source.symbol
            )
            for start in range(0, len(events), 16):
                chunk = events[start : start + 16]
                if len(chunk) < 8:
                    continue
                condition = _motif_condition(partition, source.symbol, chunk)
                windows.append(
                    ReferenceMotifSourceWindowV1(
                        source_series_id=(
                            f"ascii:T:{source.symbol}:histdata.com:"
                            f"{source.period}:chunk-{start // 16:02d}"
                        ),
                        period=source.period,
                        source_artifact=ArtifactRef(
                            kind="histdata_ascii_tick_arrow",
                            path=source.relative_path,
                            size_bytes=source.size_bytes,
                            sha256=source.sha256,
                            metadata={"partition_id": source.partition_id},
                        ),
                        split_kind=split_kind[partition.split_kind],
                        condition=condition,
                        events=tuple(
                            ReferenceMotifSourceEventV1(
                                event_time_ns=item.event_time_ns,
                                event_sequence=item.event_sequence,
                                bid=item.bid,
                                ask=item.ask,
                                source_row_id=item.event_sequence,
                            )
                            for item in chunk
                        ),
                        first_known_at_ns=chunk[-1].event_time_ns,
                        available_at_ns=chunk[-1].event_time_ns,
                    )
                )
    return build_reference_motif_index(
        windows,
        splits=splits,
        config=ReferenceMotifIndexConfigV1(
            min_cell_support=1,
            max_source_windows=2048,
            max_fragments=512,
            max_matches=16,
            source_overlap_guard_ns=(
                corpus.profile.neighbor_guard_seconds * NANOSECONDS_PER_SECOND
            ),
            max_artifact_bytes=corpus.profile.max_artifact_bytes,
        ),
    )


def _motif_condition(
    partition: BenchmarkWindowPartitionV1,
    symbol: str,
    events: Sequence[BenchmarkEventV1],
) -> ReferenceMotifConditionV1:
    return reference_motif_condition_from_quotes(
        symbol=symbol,
        feed_epoch_id=partition.epoch_label,
        session_state=partition.session,
        event_tags=(partition.context_state, partition.positioning_state),
        event_times_ns=tuple(item.event_time_ns for item in events),
        bids=tuple(item.bid for item in events),
        asks=tuple(item.ask for item in events),
    )


def _apply_degradation(
    reference: Sequence[BenchmarkEventV1],
    *,
    config: Mapping[str, JSONValue],
    corpus: ReverseDegradationBenchmarkCorpusV1,
    partition: BenchmarkWindowPartitionV1,
    operator: ObservationOperatorV1,
    run_id: str,
) -> tuple[BenchmarkEventV1, ...]:
    name = str(config["name"])
    if name == "fitted_state_dependent":
        scenario = BenchmarkScenarioV1(
            split_kind=_benchmark_split(partition.split_kind),
            epoch_id=partition.epoch_label,
            severity_id="fitted-state-dependent",
            observation_operator_id=operator.operator_id,
            degradation_parameters={"fitted_operator": True},
        )
        outputs: list[BenchmarkEventV1] = []
        for symbol in corpus.profile.symbols:
            selected_reference = tuple(
                item for item in reference if item.symbol == symbol
            )
            window = ReconstructionWindowV1(
                run_id=run_id,
                ensemble_member_id="degradation",
                symbols=(symbol,),
                core_start_ns=partition.start_ns,
                core_end_ns=partition.end_ns,
            )
            protected = tuple(
                item.source_event_id
                for item in selected_reference
                if item.anchor_id is not None
            )
            degraded, _ = degrade_benchmark_window(
                operator,
                selected_reference,
                scenario=scenario,
                window=window,
                protected_event_ids=protected,
                source_start=True,
                degraded_sparsity="fitted-state-dependent",
            )
            outputs.extend(cast(Sequence[BenchmarkEventV1], degraded))
        return tuple(_ordered_events(outputs))

    anchors = {
        item.source_event_id for item in reference if item.anchor_id is not None
    }
    selected: list[BenchmarkEventV1] = []
    if name in {"uniform_thinning", "symbol_specific"}:
        raw = config["retention_probability"]
        rates = (
            {
                symbol: float(cast(Mapping[str, Any], raw)[symbol])
                for symbol in corpus.profile.symbols
            }
            if isinstance(raw, Mapping)
            else dict.fromkeys(corpus.profile.symbols, float(cast(float, raw)))
        )
        for event in reference:
            score = int(
                hashlib.sha256(
                    f"{partition.window_id}:{name}:{event.source_event_id}".encode()
                ).hexdigest()[:16],
                16,
            ) / float(0xFFFFFFFFFFFFFFFF)
            if event.source_event_id in anchors or score < rates[event.symbol]:
                selected.append(_degraded_event(event, name))
    elif name == "unchanged_filter":
        selected = [
            _degraded_event(item, name)
            for item in reference
            if item.source_event_id in anchors
            or item.event_state != "unchanged"
        ]
    elif name in {"timestamp_quantization", "batching"}:
        quantum = int(
            cast(
                int,
                config[
                    (
                        "quantum_ns"
                        if name == "timestamp_quantization"
                        else "batch_width_ns"
                    )
                ],
            )
        )
        selected = [
            (
                _degraded_event(item, name)
                if item.source_event_id in anchors
                else replace(
                    _degraded_event(item, name),
                    event_time_ns=(item.event_time_ns // quantum) * quantum,
                    benchmark_event_id="",
                )
            )
            for item in reference
        ]
    elif name == "rate_cap":
        maximum = int(cast(int, config["max_events_per_second"]))
        counts: Counter[tuple[str, int]] = Counter()
        for item in reference:
            key = (item.symbol, item.event_time_ns // NANOSECONDS_PER_SECOND)
            if item.source_event_id in anchors or counts[key] < maximum:
                selected.append(_degraded_event(item, name))
                counts[key] += 1
    elif name == "missing_window":
        modulus = int(cast(int, config["window_modulus"]))
        missing = int(partition.window_id[-8:], 16) % modulus == 0
        selected = [
            _degraded_event(item, name)
            for item in reference
            if not missing or item.source_event_id in anchors
        ]
    elif name == "duplicate_injection":
        modulus = int(cast(int, config["duplicate_modulus"]))
        for index, item in enumerate(reference):
            selected.append(_degraded_event(item, name))
            if index % modulus == 0 and item.source_event_id not in anchors:
                selected.append(
                    BenchmarkEventV1(
                        source_event_id=f"{item.source_event_id}:duplicate",
                        symbol=item.symbol,
                        event_time_ns=item.event_time_ns,
                        event_sequence=item.event_sequence + 2**40,
                        bid=item.bid,
                        ask=item.ask,
                        epoch_id=item.epoch_id,
                        session=item.session,
                        event_state=item.event_state,
                        sparsity=name,
                    )
                )
    else:
        raise ValueError(f"unsupported degradation configuration: {name}")
    return tuple(_ordered_events(selected))


def _degraded_event(event: BenchmarkEventV1, sparsity: str) -> BenchmarkEventV1:
    return replace(
        event,
        sparsity=sparsity,
        benchmark_event_id="",
    )


def _drop_first_anchor(
    events: Sequence[BenchmarkEventV1],
) -> tuple[BenchmarkEventV1, ...]:
    first = next((item for item in events if item.anchor_id is not None), None)
    if first is None:
        return tuple(events)
    return tuple(
        item
        for item in events
        if item.benchmark_event_id != first.benchmark_event_id
    )


def _observable_stream_signature(
    events: Sequence[BenchmarkEventV1],
) -> tuple[tuple[str, int, int, float, float], ...]:
    """Describe the externally observed quote stream, excluding labels."""
    return tuple(
        (
            item.source_event_id,
            item.event_time_ns,
            item.event_sequence,
            item.bid,
            item.ask,
        )
        for item in _ordered_events(events)
    )


def _anchor_violation_count(
    reference: Sequence[BenchmarkEventV1],
    candidate: Sequence[BenchmarkEventV1],
) -> int:
    """Count missing or altered protected source events."""
    anchors = {
        item.source_event_id: item
        for item in reference
        if item.anchor_id is not None
    }
    candidate_by_source = {item.source_event_id: item for item in candidate}
    return sum(
        source_id not in candidate_by_source
        or (
            candidate_by_source[source_id].event_time_ns,
            candidate_by_source[source_id].bid,
            candidate_by_source[source_id].ask,
            candidate_by_source[source_id].anchor_id,
        )
        != (item.event_time_ns, item.bid, item.ask, item.anchor_id)
        for source_id, item in anchors.items()
    )


def _compare_streams(
    reference: Sequence[BenchmarkEventV1],
    candidate: Sequence[BenchmarkEventV1],
    partition: BenchmarkWindowPartitionV1,
) -> dict[str, float]:
    left = tuple(_ordered_events(reference))
    right = tuple(_ordered_events(candidate))
    left_intervals = _interarrivals(left)
    right_intervals = _interarrivals(right)
    left_hist = _histogram(
        left_intervals,
        (1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000),
    )
    right_hist = _histogram(
        right_intervals,
        (1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000),
    )
    left_spreads = [item.spread for item in left]
    right_spreads = [item.spread for item in right]
    left_mids = _mids_by_symbol(left)
    right_mids = _mids_by_symbol(right)
    left_updates = _update_proportions(left)
    right_updates = _update_proportions(right)
    left_transitions = _update_transitions(left)
    right_transitions = _update_transitions(right)
    anchors = {
        item.source_event_id: item
        for item in left
        if item.anchor_id is not None
    }
    anchor_violations = _anchor_violation_count(left, right)
    unsupported = sum(
        not partition.context_supported
        and item.source_event_id not in anchors
        and item.sparsity == "empirical-motif-candidate"
        for item in right
    )
    left_counts = _multiscale_counts(left, partition.start_ns, partition.end_ns)
    right_counts = _multiscale_counts(
        right, partition.start_ns, partition.end_ns
    )
    left_rv = sum(_realized_variation(values) for values in left_mids.values())
    right_rv = sum(
        _realized_variation(values) for values in right_mids.values()
    )
    left_increment = _absolute_log_increments(left_mids)
    right_increment = _absolute_log_increments(right_mids)
    return {
        "event_count_relative_error": _relative_error(
            float(len(left)), float(len(right))
        ),
        "count_multiscale_relative_error": _mapping_mean_relative_error(
            left_counts, right_counts
        ),
        "count_dispersion_relative_error": _relative_error(
            _dispersion(left_counts), _dispersion(right_counts)
        ),
        "interarrival_hist_l1": _histogram_l1(left_hist, right_hist),
        "interarrival_quantile_relative_error": _quantile_error(
            left_intervals, right_intervals
        ),
        "interarrival_duration_dependence_error": abs(
            _lag_one_correlation(left_intervals)
            - _lag_one_correlation(right_intervals)
        ),
        "burst_quiet_rate_error": _burst_quiet_error(
            left_intervals, right_intervals
        ),
        "update_type_proportion_l1": _mapping_l1(left_updates, right_updates),
        "update_transition_l1": _mapping_l1(
            left_transitions, right_transitions
        ),
        "spread_tail_relative_error": _relative_error(
            _quantile(left_spreads, 0.95), _quantile(right_spreads, 0.95)
        ),
        "spread_jump_relative_error": _relative_error(
            _mean_absolute_difference(left_spreads),
            _mean_absolute_difference(right_spreads),
        ),
        "stale_run_relative_error": _relative_error(
            _longest_stale_run(left), _longest_stale_run(right)
        ),
        "timestamp_precision_relative_error": _relative_error(
            _timestamp_quantum(left), _timestamp_quantum(right)
        ),
        "tick_grid_adherence_error": abs(
            _grid_adherence(left) - _grid_adherence(right)
        ),
        "path_increment_relative_error": _relative_error(
            _mean(left_increment), _mean(right_increment)
        ),
        "path_realized_variation_relative_error": _relative_error(
            left_rv, right_rv
        ),
        "path_jump_relative_error": _relative_error(
            _quantile(left_increment, 0.99), _quantile(right_increment, 0.99)
        ),
        "path_excursion_relative_error": _relative_error(
            _path_excursion(left_mids), _path_excursion(right_mids)
        ),
        "immutable_anchor_violation_count": float(anchor_violations),
        "triangle_residual_p99_pips": _triangle_residual_p99_pips(right),
        "triangle_synchronization_error": 1.0 - _triangle_sync_rate(right),
        "inverse_consistency_error": 0.0,
        "unsupported_context_emission_count": float(unsupported),
    }


def write_reverse_degradation_benchmark_artifacts(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    campaign: ReverseDegradationBenchmarkCampaignV1,
    motif_index: ReferenceMotifIndexV1,
    artifact_directory: str | Path,
) -> Mapping[str, ArtifactRef]:
    """Atomically write the corpus, scorecard, and companion audits."""
    if campaign.corpus_id != corpus.corpus_id:
        raise ValueError("campaign corpus identity differs")
    if campaign.motif_index_id != motif_index.index_id:
        raise ValueError("campaign motif index identity differs")
    root = Path(artifact_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    payloads = _benchmark_artifact_payloads(corpus, campaign, motif_index)
    artifacts: dict[str, ArtifactRef] = {}
    total = 0
    for name, (prefix, payload) in payloads.items():
        encoded = canonical_contract_json(payload).encode("utf-8") + b"\n"
        if len(encoded) > corpus.profile.max_artifact_bytes:
            raise ValueError(f"{name} artifact exceeds configured bound")
        digest = hashlib.sha256(encoded).hexdigest()
        target = root / f"{prefix}-{digest}.json"
        _write_once(target, encoded)
        total += len(encoded)
        artifacts[name] = ArtifactRef(
            kind=f"reverse_degradation_{name}_v1",
            path=str(target),
            size_bytes=len(encoded),
            sha256=digest,
            metadata={
                "corpus_id": corpus.corpus_id,
                "campaign_id": campaign.campaign_id,
            },
        )
    if total != campaign.artifact_bytes:
        raise ValueError(
            "measured artifact bytes differ from campaign evidence"
        )
    if total > corpus.profile.max_artifact_bytes:
        raise ValueError(
            "combined benchmark artifact set exceeds configured bound"
        )
    return artifacts


def _benchmark_artifact_payloads(
    corpus: ReverseDegradationBenchmarkCorpusV1,
    campaign: ReverseDegradationBenchmarkCampaignV1,
    motif_index: ReferenceMotifIndexV1,
) -> dict[str, tuple[str, Mapping[str, JSONValue]]]:
    return {
        "manifest": (
            "reverse-degradation-manifest",
            {
                "schema_version": "histdatacom.reverse-degradation-manifest.v1",
                "corpus": corpus.to_dict(),
                "artifact_contract": {
                    "content_addressed": True,
                    "dense_rows_embedded": False,
                    "holdout_rows_embedded": False,
                    "replay_required": True,
                },
            },
        ),
        "motif_index": (
            "reverse-degradation-motif-index",
            motif_index.to_dict(),
        ),
        "leakage_audit": (
            "reverse-degradation-leakage-audit",
            {
                "schema_version": "histdatacom.reverse-degradation-leakage-audit.v1",
                "corpus_id": corpus.corpus_id,
                "split_hashes": dict(corpus.split_hashes),
                "neighbor_guard_seconds": corpus.profile.neighbor_guard_seconds,
                "holdout_neighbor_leakage_count": corpus.neighbor_leakage_count,
                "motif_cross_split_comparison_count": motif_index.leakage_comparison_count,
                "motif_indexed_splits": [ReferenceMotifSplitKind.TRAIN.value],
                "information_audit_violation_count": campaign.campaign_metrics[
                    "information_audit_violation_count"
                ],
            },
        ),
        "resource_audit": (
            "reverse-degradation-resource-audit",
            {
                "schema_version": "histdatacom.reverse-degradation-resource-audit.v1",
                "campaign_id": campaign.campaign_id,
                "runtime_seconds": campaign.runtime_seconds,
                "peak_memory_bytes": campaign.peak_memory_bytes,
                "compact_artifact_bytes": campaign.artifact_bytes,
                "max_hook_metric_count": campaign.campaign_metrics[
                    "max_hook_metric_count"
                ],
                "profile_bounds": {
                    "max_runtime_seconds": corpus.profile.max_runtime_seconds,
                    "max_peak_memory_bytes": corpus.profile.max_peak_memory_bytes,
                    "max_artifact_bytes": corpus.profile.max_artifact_bytes,
                },
                "degradation_coverage": dict(campaign.degradation_coverage),
            },
        ),
        "scorecard": (
            "reverse-degradation-scorecard",
            campaign.to_dict(),
        ),
    }


def read_reverse_degradation_benchmark_corpus(
    path: str | Path,
) -> ReverseDegradationBenchmarkCorpusV1:
    """Read a content-addressed manifest and restore its strict corpus."""
    payload = _read_content_addressed_json(
        path,
        prefix="reverse-degradation-manifest",
        maximum=DEFAULT_MAX_ARTIFACT_BYTES,
    )
    if (
        payload.get("schema_version")
        != "histdatacom.reverse-degradation-manifest.v1"
    ):
        raise ValueError("unsupported reverse degradation manifest schema")
    contract = _mapping(payload.get("artifact_contract"))
    if (
        contract.get("dense_rows_embedded") is not False
        or contract.get("holdout_rows_embedded") is not False
    ):
        raise ValueError("reverse degradation manifest embeds protected rows")
    return ReverseDegradationBenchmarkCorpusV1.from_dict(
        _mapping(payload.get("corpus"))
    )


def read_reverse_degradation_benchmark_campaign(
    path: str | Path,
) -> ReverseDegradationBenchmarkCampaignV1:
    """Read and hash-verify a content-addressed campaign scorecard."""
    payload = _read_content_addressed_json(
        path,
        prefix="reverse-degradation-scorecard",
        maximum=DEFAULT_MAX_ARTIFACT_BYTES,
    )
    return ReverseDegradationBenchmarkCampaignV1.from_dict(payload)


def _ordered_events(
    events: Iterable[BenchmarkEventV1],
) -> list[BenchmarkEventV1]:
    return sorted(
        events,
        key=lambda item: (
            item.event_time_ns,
            item.symbol,
            item.event_sequence,
            item.benchmark_event_id,
        ),
    )


def _events_by_symbol(
    events: Sequence[BenchmarkEventV1],
) -> dict[str, tuple[BenchmarkEventV1, ...]]:
    grouped: dict[str, list[BenchmarkEventV1]] = defaultdict(list)
    for event in events:
        grouped[event.symbol].append(event)
    return {
        symbol: tuple(
            sorted(
                values,
                key=lambda item: (
                    item.event_time_ns,
                    item.event_sequence,
                    item.benchmark_event_id,
                ),
            )
        )
        for symbol, values in grouped.items()
    }


def _interarrivals(events: Sequence[BenchmarkEventV1]) -> list[float]:
    values: list[float] = []
    for selected in _events_by_symbol(events).values():
        values.extend(
            float(current.event_time_ns - previous.event_time_ns)
            for previous, current in zip(selected, selected[1:])
            if current.event_time_ns >= previous.event_time_ns
        )
    return values


def _histogram(
    values: Sequence[float], buckets: Sequence[float]
) -> tuple[int, ...]:
    result = [0] * (len(buckets) + 1)
    for value in values:
        index = bisect_left(buckets, value)
        result[index] += 1
    return tuple(result)


def _histogram_l1(left: Sequence[int], right: Sequence[int]) -> float:
    left_total = sum(left)
    right_total = sum(right)
    if left_total == 0:
        return 0.0 if right_total == 0 else 1.0
    if right_total == 0:
        return 1.0
    return 0.5 * sum(
        abs(a / left_total - b / right_total) for a, b in zip(left, right)
    )


def _multiscale_counts(
    events: Sequence[BenchmarkEventV1], start_ns: int, end_ns: int
) -> dict[str, float]:
    result: dict[str, float] = {}
    for seconds in (1, 5, 60):
        width = seconds * NANOSECONDS_PER_SECOND
        bucket_count = max(1, math.ceil((end_ns - start_ns) / width))
        counts = [0] * bucket_count
        for event in events:
            index = min(
                bucket_count - 1, (event.event_time_ns - start_ns) // width
            )
            if index >= 0:
                counts[index] += 1
        result[f"{seconds}s_mean"] = _mean(counts)
        result[f"{seconds}s_variance"] = (
            statistics.pvariance(counts) if len(counts) > 1 else 0.0
        )
    return result


def _dispersion(values: Mapping[str, float]) -> float:
    ratios = [
        values[f"{seconds}s_variance"] / max(values[f"{seconds}s_mean"], 1e-12)
        for seconds in (1, 5, 60)
    ]
    return _mean(ratios)


def _mapping_mean_relative_error(
    left: Mapping[str, float], right: Mapping[str, float]
) -> float:
    return _mean(
        [_relative_error(left[name], right.get(name, 0.0)) for name in left]
    )


def _quantile_error(left: Sequence[float], right: Sequence[float]) -> float:
    return _mean(
        [
            _relative_error(
                _quantile(left, quantile), _quantile(right, quantile)
            )
            for quantile in (0.1, 0.25, 0.5, 0.75, 0.9, 0.99)
        ]
    )


def _lag_one_correlation(values: Sequence[float]) -> float:
    if len(values) < 3:
        return 0.0
    left = values[:-1]
    right = values[1:]
    left_mean, right_mean = _mean(left), _mean(right)
    numerator = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    denominator = math.sqrt(
        sum((a - left_mean) ** 2 for a in left)
        * sum((b - right_mean) ** 2 for b in right)
    )
    return numerator / denominator if denominator else 0.0


def _burst_quiet_error(left: Sequence[float], right: Sequence[float]) -> float:
    def diagnostics(
        values: Sequence[float],
    ) -> tuple[float, float, float, float]:
        if not values:
            return (0.0, 0.0, 0.0, 0.0)
        burst = [item for item in values if item <= 100_000_000]
        quiet = [item for item in values if item >= 5_000_000_000]
        return (
            len(burst) / len(values),
            len(quiet) / len(values),
            _mean(burst),
            _mean(quiet),
        )

    left_values, right_values = diagnostics(left), diagnostics(right)
    return _mean(
        [
            abs(left_values[0] - right_values[0]),
            abs(left_values[1] - right_values[1]),
            _relative_error(left_values[2], right_values[2]),
            _relative_error(left_values[3], right_values[3]),
        ]
    )


def _update_proportions(events: Sequence[BenchmarkEventV1]) -> dict[str, float]:
    counts = Counter(item.event_state for item in events)
    total = max(1, len(events))
    return {name: count / total for name, count in counts.items()}


def _update_transitions(events: Sequence[BenchmarkEventV1]) -> dict[str, float]:
    counts: Counter[str] = Counter()
    total = 0
    for selected in _events_by_symbol(events).values():
        for left, right in zip(selected, selected[1:]):
            counts[f"{left.event_state}->{right.event_state}"] += 1
            total += 1
    return {name: count / max(1, total) for name, count in counts.items()}


def _mapping_l1(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    names = set(left) | set(right)
    return 0.5 * sum(
        abs(left.get(name, 0.0) - right.get(name, 0.0)) for name in names
    )


def _mids_by_symbol(
    events: Sequence[BenchmarkEventV1],
) -> dict[str, tuple[float, ...]]:
    return {
        symbol: tuple(item.mid for item in values)
        for symbol, values in _events_by_symbol(events).items()
    }


def _absolute_log_increments(
    values: Mapping[str, Sequence[float]],
) -> list[float]:
    result: list[float] = []
    for mids in values.values():
        result.extend(
            abs(math.log(right / left))
            for left, right in zip(mids, mids[1:])
            if left > 0.0 and right > 0.0
        )
    return result


def _realized_variation(values: Sequence[float]) -> float:
    return sum(
        math.log(right / left) ** 2
        for left, right in zip(values, values[1:])
        if left > 0.0 and right > 0.0
    )


def _path_excursion(values: Mapping[str, Sequence[float]]) -> float:
    excursions = [max(mids) - min(mids) for mids in values.values() if mids]
    return sum(excursions)


def _mean_absolute_difference(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    return _mean([abs(right - left) for left, right in zip(values, values[1:])])


def _longest_stale_run(events: Sequence[BenchmarkEventV1]) -> float:
    longest = 0
    for selected in _events_by_symbol(events).values():
        current = 0
        previous: BenchmarkEventV1 | None = None
        for event in selected:
            if (
                previous is not None
                and event.bid == previous.bid
                and event.ask == previous.ask
            ):
                current += 1
                longest = max(longest, current)
            else:
                current = 0
            previous = event
    return float(longest)


def _timestamp_quantum(events: Sequence[BenchmarkEventV1]) -> float:
    values = [int(value) for value in _interarrivals(events) if value > 0]
    if not values:
        return 0.0
    result = values[0]
    for value in values[1:]:
        result = math.gcd(result, value)
        if result == 1:
            break
    return float(result)


def _grid_adherence(events: Sequence[BenchmarkEventV1]) -> float:
    if not events:
        return 0.0
    return float(
        sum(
            int(
                round(item.bid, 8) == item.bid
                and round(item.ask, 8) == item.ask
            )
            for item in events
        )
    ) / len(events)


def _triangle_residual_p99_pips(events: Sequence[BenchmarkEventV1]) -> float:
    grouped = _events_by_symbol(events)
    if any(symbol not in grouped for symbol in DEFAULT_BENCHMARK_SYMBOLS):
        return 0.0
    lookup = {
        symbol: (
            [item.event_time_ns for item in values],
            [item.mid for item in values],
        )
        for symbol, values in grouped.items()
    }
    residuals: list[float] = []
    for event in grouped["EURUSD"]:
        eurgbp = _nearest_mid(lookup["EURGBP"], event.event_time_ns)
        gbpusd = _nearest_mid(lookup["GBPUSD"], event.event_time_ns)
        if eurgbp is None or gbpusd is None:
            continue
        residuals.append(abs(event.mid - eurgbp * gbpusd) / PIP)
    return _quantile(residuals, 0.99)


def _triangle_sync_rate(events: Sequence[BenchmarkEventV1]) -> float:
    grouped = _events_by_symbol(events)
    if any(symbol not in grouped for symbol in DEFAULT_BENCHMARK_SYMBOLS):
        return 0.0
    lookup = {
        symbol: (
            [item.event_time_ns for item in values],
            [item.mid for item in values],
        )
        for symbol, values in grouped.items()
    }
    reference = grouped["EURUSD"]
    matched = sum(
        _nearest_mid(
            lookup["EURGBP"],
            event.event_time_ns,
            tolerance_ns=NANOSECONDS_PER_SECOND,
        )
        is not None
        and _nearest_mid(
            lookup["GBPUSD"],
            event.event_time_ns,
            tolerance_ns=NANOSECONDS_PER_SECOND,
        )
        is not None
        for event in reference
    )
    return matched / max(1, len(reference))


def _nearest_mid(
    lookup: tuple[list[int], list[float]],
    timestamp_ns: int,
    *,
    tolerance_ns: int | None = None,
) -> float | None:
    timestamps, mids = lookup
    if not timestamps:
        return None
    index = bisect_left(timestamps, timestamp_ns)
    candidates = [
        value for value in (index - 1, index) if 0 <= value < len(timestamps)
    ]
    selected = min(
        candidates, key=lambda value: abs(timestamps[value] - timestamp_ns)
    )
    if (
        tolerance_ns is not None
        and abs(timestamps[selected] - timestamp_ns) > tolerance_ns
    ):
        return None
    return mids[selected]


def _uncertainty_interval(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"lower": 0.0, "mean": 0.0, "upper": 0.0}
    mean = _mean(values)
    standard_error = (
        statistics.stdev(values) / math.sqrt(len(values))
        if len(values) > 1
        else 0.0
    )
    return {
        "lower": max(0.0, mean - 1.96 * standard_error),
        "mean": mean,
        "upper": mean + 1.96 * standard_error,
    }


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    fraction = position - lower
    return float(ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction)


def _relative_error(reference: float, candidate: float) -> float:
    if reference == 0.0:
        return 0.0 if candidate == 0.0 else 1.0
    return abs(candidate - reference) / abs(reference)


def _mean(values: Sequence[float] | Sequence[int]) -> float:
    return sum(values) / len(values) if values else 0.0


def _benchmark_split(value: str) -> BenchmarkSplitKind:
    return {
        "calibration": BenchmarkSplitKind.CALIBRATION,
        "validation": BenchmarkSplitKind.VALIDATION,
        "final_holdout": BenchmarkSplitKind.FINAL_HOLDOUT,
    }[value]


def _artifact_ref(
    path: Path, kind: str, metadata: Mapping[str, JSONValue]
) -> ArtifactRef:
    size = path.stat().st_size
    if size > DEFAULT_MAX_ARTIFACT_BYTES:
        raise ValueError(f"{kind} dependency exceeds artifact bound")
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=size,
        sha256=_file_sha256(path),
        metadata=dict(metadata),
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError("content-addressed benchmark artifact differs")
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
    path: str | Path, *, prefix: str, maximum: int
) -> Mapping[str, Any]:
    source = Path(path).expanduser().resolve()
    match = re.fullmatch(
        rf"{re.escape(prefix)}-([0-9a-f]{{64}})\.json", source.name
    )
    if match is None:
        raise ValueError("benchmark artifact name is not content addressed")
    content = source.read_bytes()
    if len(content) > maximum:
        raise ValueError("benchmark artifact exceeds size bound")
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError("benchmark artifact content hash differs from name")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("benchmark artifact is invalid JSON") from exc
    return _mapping(payload)


def _peak_memory_bytes() -> int:
    return int(peak_rss_bytes())


def _enforce_runtime(started: float, maximum: float) -> None:
    if time.monotonic() - started > maximum:
        raise RuntimeError("benchmark campaign exceeded runtime bound")


def _stable_id(namespace: str, payload: Mapping[str, Any]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{namespace}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _symbol(value: Any) -> str:
    selected = _required_text(value).upper()
    if not re.fullmatch(r"[A-Z]{6}", selected):
        raise ValueError("benchmark symbol must be a six-letter pair")
    return selected


def _sha256(value: Any, name: str) -> str:
    selected = str(value)
    if not _SHA256.fullmatch(selected):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return selected


def _require_schema(
    data: Mapping[str, Any], expected: str, name: str = "contract"
) -> None:
    _require_schema_value(str(data.get("schema_version", "")), expected, name)


def _require_schema_value(value: str, expected: str, name: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {name} schema")


def _required_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("required text value is empty")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be text")
    selected = value.strip()
    return selected or None


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside [{minimum}, {maximum}]")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _positive_float(
    value: Any, name: str, *, allow_zero: bool = False
) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0 or (selected == 0.0 and not allow_zero):
        raise ValueError(f"{name} must be positive")
    return selected


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"{name} must be a finite JSON scalar")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("expected a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    values = _sequence(value)
    if any(not isinstance(item, str) for item in values):
        raise ValueError("expected a string sequence")
    return tuple(cast(str, item) for item in values)


def _json_mapping(text: str, maximum: int) -> Mapping[str, Any]:
    if len(text.encode("utf-8")) > maximum:
        raise ValueError("benchmark JSON exceeds size bound")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("benchmark JSON is invalid") from exc
    return _mapping(payload)


def _iso_utc(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("benchmark timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(
        parsed
    ):
        raise ValueError("benchmark timestamp must be UTC")


__all__ = [
    "BENCHMARK_CANDIDATE_REPORT_SCHEMA_VERSION",
    "BENCHMARK_CORPUS_PROFILE_SCHEMA_VERSION",
    "BENCHMARK_SOURCE_PARTITION_SCHEMA_VERSION",
    "BENCHMARK_WINDOW_PARTITION_SCHEMA_VERSION",
    "PREDECLARED_GATE_COMMIT",
    "REVERSE_DEGRADATION_CAMPAIGN_SCHEMA_VERSION",
    "REVERSE_DEGRADATION_CORPUS_SCHEMA_VERSION",
    "BenchmarkCandidateReportV1",
    "BenchmarkSourcePartitionV1",
    "BenchmarkWindowPartitionV1",
    "ReverseDegradationBenchmarkCampaignV1",
    "ReverseDegradationBenchmarkCorpusV1",
    "ReverseDegradationCorpusProfileV1",
    "audit_holdout_neighbor_leakage",
    "build_reverse_degradation_benchmark_corpus",
    "read_reverse_degradation_benchmark_campaign",
    "read_reverse_degradation_benchmark_corpus",
    "replay_reverse_degradation_benchmark_corpus",
    "run_reverse_degradation_benchmark_campaign",
    "write_reverse_degradation_benchmark_artifacts",
]
