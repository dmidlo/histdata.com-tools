"""Deterministic derived candlestick products from committed reconstruction.

Bars are an optional export projection of the final narrow event product.  The
module never reads raw HistData M1 rows or the enriched analytical frame, never
widens ``SyntheticEventV1``, and never invents volume.  Publication mirrors the
reconstruction transaction boundary: projected event batches are aggregated in
bounded state below hidden scratch, verified, and promoted with one rename.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, cast
from urllib.parse import quote

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.activity import (
    ActivitySliceScope,
    ActivityVolumeState,
)
from histdatacom.synthetic.contracts import (
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    SyntheticEventOrigin,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_PRODUCT_SCHEMA_VERSION,
    iter_reconstruction_event_batches,
    verify_reconstruction_publication,
)

DERIVED_BAR_INTERVAL_SCHEMA_VERSION = "histdatacom.derived-bar-interval.v1"
DERIVED_BAR_POLICY_SCHEMA_VERSION = "histdatacom.derived-bar-policy.v1"
DERIVED_BAR_SCHEMA_VERSION = "histdatacom.derived-bar.v1"
DERIVED_BAR_PARTITION_SCHEMA_VERSION = (
    "histdatacom.derived-bar-product-partition.v1"
)
DERIVED_BAR_PRODUCT_SCHEMA_VERSION = "histdatacom.derived-bar-product.v1"

DERIVED_BAR_PRODUCT_DIRECTORY = "derived-bar-products"
DERIVED_BAR_MANIFEST_FILENAME = "manifest.json"
DERIVED_BAR_MANIFEST_ARTIFACT_KIND = "derived-bar-product-manifest"
DERIVED_BAR_COMPRESSION = "zstd"
DERIVED_BAR_WRITER_ID = "histdatacom.pyarrow-parquet-zstd.v1"
DERIVED_BAR_LOGICAL_HASH_ALGORITHM = "sha256-canonical-bar-json-lines-v1"
DERIVED_BAR_BYTE_HASH_ALGORITHM = "sha256-path-byte-digests-v1"
DEFAULT_DERIVED_BAR_BATCH_SIZE = 65_536
DEFAULT_DERIVED_BAR_ROW_GROUP_SIZE = 16_384
DEFAULT_DERIVED_BAR_WRITE_BUFFER_ROWS = 1_024
DEFAULT_DERIVED_BAR_MAX_BARS = 100_000_000
DEFAULT_DERIVED_BAR_MAX_PROVENANCE_VALUES = 256
DEFAULT_DERIVED_BAR_MAX_SYMBOLS = 64
DEFAULT_DERIVED_BAR_ROUNDING_DIGITS = 12
MAX_DERIVED_BAR_MANIFEST_BYTES = 8 * 1024**2
MAX_DERIVED_BAR_PARTITIONS = 100_000
MAX_DERIVED_BAR_TEXT = 1_024

NANOSECONDS_PER_SECOND = 1_000_000_000
STANDARD_DERIVED_BAR_INTERVALS: dict[str, int] = {
    "1m": 60 * NANOSECONDS_PER_SECOND,
    "5m": 5 * 60 * NANOSECONDS_PER_SECOND,
    "15m": 15 * 60 * NANOSECONDS_PER_SECOND,
    "30m": 30 * 60 * NANOSECONDS_PER_SECOND,
    "1h": 60 * 60 * NANOSECONDS_PER_SECOND,
    "4h": 4 * 60 * 60 * NANOSECONDS_PER_SECOND,
    "1d": 24 * 60 * 60 * NANOSECONDS_PER_SECOND,
}

DERIVED_BAR_EVENT_COLUMNS = (
    "event_id",
    "origin",
    "symbol",
    "event_time_ns",
    "event_sequence",
    "bid",
    "ask",
    "run_id",
    "ensemble_member_id",
    "source_version_id",
    "generator_id",
    "generator_version",
    "generator_config_id",
    "reference_id",
    "motif_id",
    "feed_epoch_id",
    "broker_profile_id",
    "constraint_set_id",
    "confidence",
)

DERIVED_BAR_ARROW_COLUMNS = (
    "schema_version",
    "event_schema_version",
    "bar_id",
    "source_product_manifest_id",
    "policy_id",
    "rounding_digits",
    "run_id",
    "ensemble_member_id",
    "symbol",
    "scope",
    "interval_code",
    "interval_ns",
    "bar_start_ns",
    "bar_end_ns",
    "first_event_id",
    "last_event_id",
    "first_event_time_ns",
    "last_event_time_ns",
    "event_count",
    "observed_event_count",
    "synthetic_event_count",
    "quote_update_count",
    "transition_count",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "mid_open",
    "mid_high",
    "mid_low",
    "mid_close",
    "spread_open",
    "spread_high",
    "spread_low",
    "spread_close",
    "mean_spread",
    "activity_duration_ns",
    "tick_intensity_per_second",
    "price_change_count",
    "stale_quote_count",
    "stale_quote_rate",
    "mean_event_confidence",
    "confidence_support_count",
    "volume_state",
    "volume",
    "is_partial_start",
    "is_partial_end",
    "source_version_ids",
    "generator_ids",
    "generator_versions",
    "generator_config_ids",
    "reference_ids",
    "motif_ids",
    "feed_epoch_ids",
    "broker_profile_ids",
    "constraint_set_ids",
    "event_content_sha256",
    "event_schema_augmented",
    "raw_m1_input",
    "centralized_traded_volume_claim",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BAR_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


class DerivedBarPersistenceError(ValueError):
    """A derived-bar product is malformed or unsafe to publish."""


@dataclass(frozen=True, slots=True)
class DerivedBarIntervalV1:
    """One supported, UTC epoch-aligned half-open bar interval."""

    code: str
    duration_ns: int = 0
    alignment_epoch_ns: int = 0
    schema_version: str = DERIVED_BAR_INTERVAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            DERIVED_BAR_INTERVAL_SCHEMA_VERSION,
            "derived bar interval",
        )
        code = _required_text(self.code).lower()
        if code not in STANDARD_DERIVED_BAR_INTERVALS:
            raise ValueError("unsupported derived bar interval")
        expected = STANDARD_DERIVED_BAR_INTERVALS[code]
        supplied = _strict_int(self.duration_ns, "duration_ns")
        if supplied not in (0, expected):
            raise ValueError("derived bar interval duration differs")
        alignment = _strict_int(self.alignment_epoch_ns, "alignment_epoch_ns")
        if alignment != 0:
            raise ValueError("v1 derived bars require UTC Unix-epoch alignment")
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "duration_ns", expected)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the exact interval contract."""
        return {
            "schema_version": self.schema_version,
            "code": self.code,
            "duration_ns": self.duration_ns,
            "alignment_epoch_ns": self.alignment_epoch_ns,
            "timezone": "UTC",
            "bin_semantics": "[start,end)",
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DerivedBarIntervalV1":
        """Restore and verify one interval contract."""
        _require_schema(data, DERIVED_BAR_INTERVAL_SCHEMA_VERSION)
        _require_derived(data, "timezone", "UTC")
        _require_derived(data, "bin_semantics", "[start,end)")
        return cls(
            code=str(data.get("code", "")),
            duration_ns=_strict_int(data.get("duration_ns"), "duration_ns"),
            alignment_epoch_ns=_strict_int(
                data.get("alignment_epoch_ns"), "alignment_epoch_ns"
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DerivedBarPolicyV1:
    """Versioned scope, interval, empty-bin, and resource semantics."""

    intervals: tuple[str, ...] = tuple(STANDARD_DERIVED_BAR_INTERVALS)
    scopes: tuple[ActivitySliceScope, ...] = (ActivitySliceScope.MERGED,)
    max_bars: int = DEFAULT_DERIVED_BAR_MAX_BARS
    max_symbols: int = DEFAULT_DERIVED_BAR_MAX_SYMBOLS
    max_provenance_values: int = DEFAULT_DERIVED_BAR_MAX_PROVENANCE_VALUES
    rounding_digits: int = DEFAULT_DERIVED_BAR_ROUNDING_DIGITS
    policy_id: str = ""
    schema_version: str = DERIVED_BAR_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            DERIVED_BAR_POLICY_SCHEMA_VERSION,
            "derived bar policy",
        )
        requested = {_required_text(value).lower() for value in self.intervals}
        if not requested:
            raise ValueError("derived bar policy requires intervals")
        unknown = requested.difference(STANDARD_DERIVED_BAR_INTERVALS)
        if unknown:
            raise ValueError(
                f"unsupported derived bar intervals: {sorted(unknown)}"
            )
        intervals = tuple(
            code for code in STANDARD_DERIVED_BAR_INTERVALS if code in requested
        )
        object.__setattr__(self, "intervals", intervals)
        scopes = tuple(
            scope
            for scope in ActivitySliceScope
            if scope in {ActivitySliceScope(value) for value in self.scopes}
        )
        if not scopes:
            raise ValueError("derived bar policy requires scopes")
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(
            self,
            "max_bars",
            _bounded_int(self.max_bars, "max_bars", 1, 1_000_000_000),
        )
        object.__setattr__(
            self,
            "max_symbols",
            _bounded_int(self.max_symbols, "max_symbols", 1, 10_000),
        )
        object.__setattr__(
            self,
            "max_provenance_values",
            _bounded_int(
                self.max_provenance_values,
                "max_provenance_values",
                1,
                10_000,
            ),
        )
        object.__setattr__(
            self,
            "rounding_digits",
            _bounded_int(self.rounding_digits, "rounding_digits", 0, 15),
        )
        expected = _stable_id("derived-bar-policy", self.identity_payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("derived bar policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    @property
    def interval_contracts(self) -> tuple[DerivedBarIntervalV1, ...]:
        """Return canonical interval contracts in increasing duration order."""
        return tuple(DerivedBarIntervalV1(code) for code in self.intervals)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return policy fields participating in stable identity."""
        return {
            "schema_version": self.schema_version,
            "intervals": [item.to_dict() for item in self.interval_contracts],
            "scopes": [item.value for item in self.scopes],
            "max_bars": self.max_bars,
            "max_symbols": self.max_symbols,
            "max_provenance_values": self.max_provenance_values,
            "rounding_digits": self.rounding_digits,
            "empty_bin_policy": "omit_without_fill",
            "market_closure_policy": "omit_empty_without_liquidity",
            "partial_bin_policy": "emit_and_flag_query_boundary_overlap",
            "duplicate_timestamp_policy": (
                "order_by_event_time_sequence_and_event_id"
            ),
            "transition_boundary_policy": "carry_previous_quote_into_next_bar",
            "volume_state": ActivityVolumeState.UNAVAILABLE.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the strict policy contract."""
        return {
            **self.identity_payload(),
            "policy_id": self.policy_id,
            "raw_m1_input": False,
            "event_schema_augmented": False,
            "centralized_traded_volume_claim": False,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DerivedBarPolicyV1":
        """Restore and verify a derived-bar policy."""
        _require_schema(data, DERIVED_BAR_POLICY_SCHEMA_VERSION)
        for name, expected in (
            ("empty_bin_policy", "omit_without_fill"),
            ("market_closure_policy", "omit_empty_without_liquidity"),
            (
                "partial_bin_policy",
                "emit_and_flag_query_boundary_overlap",
            ),
            (
                "duplicate_timestamp_policy",
                "order_by_event_time_sequence_and_event_id",
            ),
            (
                "transition_boundary_policy",
                "carry_previous_quote_into_next_bar",
            ),
            ("volume_state", ActivityVolumeState.UNAVAILABLE.value),
            ("raw_m1_input", False),
            ("event_schema_augmented", False),
            ("centralized_traded_volume_claim", False),
        ):
            _require_derived(data, name, expected)
        interval_rows = _mapping_sequence(data.get("intervals"), "intervals")
        intervals = tuple(
            DerivedBarIntervalV1.from_dict(item).code for item in interval_rows
        )
        return cls(
            intervals=intervals,
            scopes=tuple(
                ActivitySliceScope(str(item))
                for item in _sequence(data.get("scopes"), "scopes")
            ),
            max_bars=_strict_int(data.get("max_bars"), "max_bars"),
            max_symbols=_strict_int(data.get("max_symbols"), "max_symbols"),
            max_provenance_values=_strict_int(
                data.get("max_provenance_values"), "max_provenance_values"
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DerivedBarV1:
    """One deterministic OHLC/activity projection of ordered event rows."""

    source_product_manifest_id: str
    policy_id: str
    rounding_digits: int
    run_id: str
    ensemble_member_id: str
    symbol: str
    scope: ActivitySliceScope
    interval_code: str
    interval_ns: int
    bar_start_ns: int
    bar_end_ns: int
    first_event_id: str
    last_event_id: str
    first_event_time_ns: int
    last_event_time_ns: int
    event_count: int
    observed_event_count: int
    synthetic_event_count: int
    quote_update_count: int
    transition_count: int
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    mid_open: float
    mid_high: float
    mid_low: float
    mid_close: float
    spread_open: float
    spread_high: float
    spread_low: float
    spread_close: float
    mean_spread: float
    activity_duration_ns: int
    tick_intensity_per_second: float | None
    price_change_count: int
    stale_quote_count: int
    stale_quote_rate: float | None
    mean_event_confidence: float | None
    confidence_support_count: int
    is_partial_start: bool
    is_partial_end: bool
    source_version_ids: tuple[str, ...]
    generator_ids: tuple[str, ...] = ()
    generator_versions: tuple[str, ...] = ()
    generator_config_ids: tuple[str, ...] = ()
    reference_ids: tuple[str, ...] = ()
    motif_ids: tuple[str, ...] = ()
    feed_epoch_ids: tuple[str, ...] = ()
    broker_profile_ids: tuple[str, ...] = ()
    constraint_set_ids: tuple[str, ...] = ()
    event_content_sha256: str = ""
    bar_id: str = ""
    schema_version: str = DERIVED_BAR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version, DERIVED_BAR_SCHEMA_VERSION, "derived bar"
        )
        for name in (
            "source_product_manifest_id",
            "policy_id",
            "run_id",
            "ensemble_member_id",
            "first_event_id",
            "last_event_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "rounding_digits",
            _bounded_int(self.rounding_digits, "rounding_digits", 0, 15),
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "scope", ActivitySliceScope(self.scope))
        interval = DerivedBarIntervalV1(
            self.interval_code, duration_ns=self.interval_ns
        )
        object.__setattr__(self, "interval_code", interval.code)
        object.__setattr__(self, "interval_ns", interval.duration_ns)
        start = _strict_int(self.bar_start_ns, "bar_start_ns")
        end = _strict_int(self.bar_end_ns, "bar_end_ns")
        if start % interval.duration_ns or end != start + interval.duration_ns:
            raise ValueError(
                "derived bar bounds differ from interval alignment"
            )
        object.__setattr__(self, "bar_start_ns", start)
        object.__setattr__(self, "bar_end_ns", end)
        first = _strict_int(self.first_event_time_ns, "first_event_time_ns")
        last = _strict_int(self.last_event_time_ns, "last_event_time_ns")
        if not start <= first <= last < end:
            raise ValueError("derived bar event bounds fall outside the bin")
        object.__setattr__(self, "first_event_time_ns", first)
        object.__setattr__(self, "last_event_time_ns", last)
        for name in (
            "event_count",
            "observed_event_count",
            "synthetic_event_count",
            "quote_update_count",
            "transition_count",
            "activity_duration_ns",
            "price_change_count",
            "stale_quote_count",
            "confidence_support_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.event_count < 1:
            raise ValueError("derived bars cannot be empty")
        if (
            self.event_count
            != self.observed_event_count + self.synthetic_event_count
        ):
            raise ValueError("derived bar origin counts do not reconcile")
        if self.quote_update_count != self.event_count:
            raise ValueError("derived bar quote-update count differs")
        if (
            self.transition_count
            != self.price_change_count + self.stale_quote_count
        ):
            raise ValueError("derived bar transition counts do not reconcile")
        if self.transition_count > self.event_count:
            raise ValueError("derived bar transition support is impossible")
        if self.transition_count not in {
            self.event_count - 1,
            self.event_count,
        }:
            raise ValueError("derived bar transition support is incomplete")
        if self.activity_duration_ns != last - first:
            raise ValueError("derived bar activity duration differs")
        if (self.event_count == 1) != (
            self.first_event_id == self.last_event_id
        ):
            raise ValueError("derived bar endpoint identities do not reconcile")
        if self.confidence_support_count > self.event_count:
            raise ValueError("derived bar confidence support exceeds rows")
        if (
            self.scope is ActivitySliceScope.OBSERVED
            and self.synthetic_event_count
        ):
            raise ValueError("observed-only bar contains synthetic support")
        if (
            self.scope is ActivitySliceScope.SYNTHETIC
            and self.observed_event_count
        ):
            raise ValueError("synthetic-only bar contains observed support")
        for prefix in ("bid", "ask", "mid", "spread"):
            values = tuple(
                _finite_float(
                    getattr(self, f"{prefix}_{suffix}"), f"{prefix}_{suffix}"
                )
                for suffix in ("open", "high", "low", "close")
            )
            if values[1] < max(values[0], values[3]) or values[2] > min(
                values[0], values[3]
            ):
                raise ValueError(f"derived bar {prefix} OHLC is inconsistent")
            if values[1] < values[2]:
                raise ValueError(f"derived bar {prefix} range is reversed")
            for suffix, value in zip(("open", "high", "low", "close"), values):
                object.__setattr__(self, f"{prefix}_{suffix}", value)
        if min(self.bid_low, self.ask_low, self.mid_low) <= 0:
            raise ValueError("derived bar prices must be positive")
        for suffix in ("open", "high", "low", "close"):
            bid = getattr(self, f"bid_{suffix}")
            ask = getattr(self, f"ask_{suffix}")
            if ask < bid:
                raise ValueError(
                    f"derived bar ask_{suffix} is below bid_{suffix}"
                )
        if not (
            self.bid_low <= self.mid_low <= self.ask_low
            and self.bid_high <= self.mid_high <= self.ask_high
        ):
            raise ValueError("derived bar midpoint extrema are inconsistent")
        for suffix in ("open", "close"):
            bid = getattr(self, f"bid_{suffix}")
            ask = getattr(self, f"ask_{suffix}")
            midpoint = getattr(self, f"mid_{suffix}")
            spread = getattr(self, f"spread_{suffix}")
            if not _matches_rounded_value(
                midpoint, (bid + ask) / 2, self.rounding_digits
            ):
                raise ValueError(
                    f"derived bar mid_{suffix} differs from bid/ask"
                )
            if not _matches_rounded_value(
                spread, ask - bid, self.rounding_digits
            ):
                raise ValueError(
                    f"derived bar spread_{suffix} differs from bid/ask"
                )
        if (
            min(
                self.spread_open,
                self.spread_high,
                self.spread_low,
                self.spread_close,
            )
            < 0
        ):
            raise ValueError("derived bar spread cannot be negative")
        mean_spread = _nonnegative_float(self.mean_spread, "mean_spread")
        if not self.spread_low <= mean_spread <= self.spread_high:
            raise ValueError("derived bar mean spread is outside its range")
        object.__setattr__(self, "mean_spread", mean_spread)
        intensity = _optional_nonnegative_float(
            self.tick_intensity_per_second, "tick_intensity_per_second"
        )
        if self.activity_duration_ns == 0 and intensity is not None:
            raise ValueError("zero-duration bar cannot claim tick intensity")
        if self.activity_duration_ns > 0 and intensity is None:
            raise ValueError("positive-duration bar requires tick intensity")
        if intensity is not None:
            expected_intensity = (
                self.event_count
                * NANOSECONDS_PER_SECOND
                / self.activity_duration_ns
            )
            if not _matches_rounded_value(
                intensity, expected_intensity, self.rounding_digits
            ):
                raise ValueError("derived bar tick intensity differs")
        object.__setattr__(self, "tick_intensity_per_second", intensity)
        stale_rate = _optional_ratio(self.stale_quote_rate, "stale_quote_rate")
        if self.transition_count == 0 and stale_rate is not None:
            raise ValueError("transition-free bar cannot claim stale rate")
        if self.transition_count > 0 and stale_rate is None:
            raise ValueError("derived bar transitions require stale rate")
        if stale_rate is not None and not _matches_rounded_value(
            stale_rate,
            self.stale_quote_count / self.transition_count,
            self.rounding_digits,
        ):
            raise ValueError("derived bar stale rate differs")
        object.__setattr__(self, "stale_quote_rate", stale_rate)
        mean_confidence = _optional_ratio(
            self.mean_event_confidence, "mean_event_confidence"
        )
        if (self.confidence_support_count == 0) != (mean_confidence is None):
            raise ValueError("derived bar confidence support differs")
        object.__setattr__(self, "mean_event_confidence", mean_confidence)
        object.__setattr__(
            self, "is_partial_start", _strict_bool(self.is_partial_start)
        )
        object.__setattr__(
            self, "is_partial_end", _strict_bool(self.is_partial_end)
        )
        for name in (
            "source_version_ids",
            "generator_ids",
            "generator_versions",
            "generator_config_ids",
            "reference_ids",
            "motif_ids",
            "feed_epoch_ids",
            "broker_profile_ids",
            "constraint_set_ids",
        ):
            object.__setattr__(
                self, name, _normalized_text_tuple(getattr(self, name))
            )
        if not self.source_version_ids:
            raise ValueError("derived bar requires source-version lineage")
        object.__setattr__(
            self,
            "event_content_sha256",
            _required_sha256(self.event_content_sha256, "event_content_sha256"),
        )
        expected = _stable_id("derived-bar", self.identity_payload())
        supplied = _optional_text(self.bar_id)
        if supplied is not None and supplied != expected:
            raise ValueError("derived bar_id differs")
        object.__setattr__(self, "bar_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return all logical bar fields except the derived bar identifier."""
        return {
            key: value
            for key, value in self.to_dict(include_bar_id=False).items()
            if key != "bar_id"
        }

    def to_dict(self, *, include_bar_id: bool = True) -> dict[str, JSONValue]:
        """Return the exact narrow Arrow/JSON row."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "source_product_manifest_id": self.source_product_manifest_id,
            "policy_id": self.policy_id,
            "rounding_digits": self.rounding_digits,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "scope": self.scope.value,
            "interval_code": self.interval_code,
            "interval_ns": self.interval_ns,
            "bar_start_ns": self.bar_start_ns,
            "bar_end_ns": self.bar_end_ns,
            "first_event_id": self.first_event_id,
            "last_event_id": self.last_event_id,
            "first_event_time_ns": self.first_event_time_ns,
            "last_event_time_ns": self.last_event_time_ns,
            "event_count": self.event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "quote_update_count": self.quote_update_count,
            "transition_count": self.transition_count,
            "bid_open": self.bid_open,
            "bid_high": self.bid_high,
            "bid_low": self.bid_low,
            "bid_close": self.bid_close,
            "ask_open": self.ask_open,
            "ask_high": self.ask_high,
            "ask_low": self.ask_low,
            "ask_close": self.ask_close,
            "mid_open": self.mid_open,
            "mid_high": self.mid_high,
            "mid_low": self.mid_low,
            "mid_close": self.mid_close,
            "spread_open": self.spread_open,
            "spread_high": self.spread_high,
            "spread_low": self.spread_low,
            "spread_close": self.spread_close,
            "mean_spread": self.mean_spread,
            "activity_duration_ns": self.activity_duration_ns,
            "tick_intensity_per_second": self.tick_intensity_per_second,
            "price_change_count": self.price_change_count,
            "stale_quote_count": self.stale_quote_count,
            "stale_quote_rate": self.stale_quote_rate,
            "mean_event_confidence": self.mean_event_confidence,
            "confidence_support_count": self.confidence_support_count,
            "volume_state": ActivityVolumeState.UNAVAILABLE.value,
            "volume": None,
            "is_partial_start": self.is_partial_start,
            "is_partial_end": self.is_partial_end,
            "source_version_ids": list(self.source_version_ids),
            "generator_ids": list(self.generator_ids),
            "generator_versions": list(self.generator_versions),
            "generator_config_ids": list(self.generator_config_ids),
            "reference_ids": list(self.reference_ids),
            "motif_ids": list(self.motif_ids),
            "feed_epoch_ids": list(self.feed_epoch_ids),
            "broker_profile_ids": list(self.broker_profile_ids),
            "constraint_set_ids": list(self.constraint_set_ids),
            "event_content_sha256": self.event_content_sha256,
            "event_schema_augmented": False,
            "raw_m1_input": False,
            "centralized_traded_volume_claim": False,
        }
        if include_bar_id:
            payload["bar_id"] = self.bar_id
            return {name: payload[name] for name in DERIVED_BAR_ARROW_COLUMNS}
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DerivedBarV1":
        """Restore and verify one derived bar row."""
        _require_schema(data, DERIVED_BAR_SCHEMA_VERSION)
        for name, expected in (
            ("event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION),
            ("volume_state", ActivityVolumeState.UNAVAILABLE.value),
            ("volume", None),
            ("event_schema_augmented", False),
            ("raw_m1_input", False),
            ("centralized_traded_volume_claim", False),
        ):
            _require_derived(data, name, expected)
        kwargs: dict[str, Any] = {
            name: data.get(name)
            for name in DERIVED_BAR_ARROW_COLUMNS
            if name
            not in {
                "event_schema_version",
                "volume_state",
                "volume",
                "event_schema_augmented",
                "raw_m1_input",
                "centralized_traded_volume_claim",
            }
        }
        for name in (
            "source_version_ids",
            "generator_ids",
            "generator_versions",
            "generator_config_ids",
            "reference_ids",
            "motif_ids",
            "feed_epoch_ids",
            "broker_profile_ids",
            "constraint_set_ids",
        ):
            kwargs[name] = _string_tuple(data.get(name), name)
        kwargs["scope"] = ActivitySliceScope(str(data.get("scope", "")))
        return cls(**kwargs)

    def to_json(self) -> str:
        """Serialize one bar deterministically."""
        serialized: str = canonical_contract_json(self.to_dict())
        return serialized

    @classmethod
    def from_json(cls, text: str) -> "DerivedBarV1":
        """Restore one bar from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class DerivedBarPartitionV1:
    """Physical and logical evidence for one monthly bar partition."""

    relative_path: str
    symbol: str
    scope: ActivitySliceScope
    interval_code: str
    bar_month: str
    row_count: int
    min_bar_start_ns: int
    max_bar_start_ns: int
    logical_content_sha256: str
    byte_sha256: str
    size_bytes: int
    row_group_count: int
    partition_id: str = ""
    schema_version: str = DERIVED_BAR_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            DERIVED_BAR_PARTITION_SCHEMA_VERSION,
            "derived bar partition",
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "scope", ActivitySliceScope(self.scope))
        interval = DerivedBarIntervalV1(self.interval_code)
        object.__setattr__(self, "interval_code", interval.code)
        month = _required_bar_month(self.bar_month)
        object.__setattr__(self, "bar_month", month)
        relative = _safe_relative_path(self.relative_path)
        expected_path = _bar_partition_relative_path(
            self.symbol, self.scope, interval.code, month
        )
        if relative != expected_path:
            raise ValueError("derived bar partition path differs from layout")
        object.__setattr__(self, "relative_path", relative)
        for name in ("row_count", "size_bytes", "row_group_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if (
            self.row_count < 1
            or self.size_bytes < 1
            or self.row_group_count < 1
            or self.row_group_count > self.row_count
        ):
            raise ValueError("derived bar partitions cannot be empty")
        minimum = _strict_int(self.min_bar_start_ns, "min_bar_start_ns")
        maximum = _strict_int(self.max_bar_start_ns, "max_bar_start_ns")
        if maximum < minimum:
            raise ValueError("derived bar partition bounds are reversed")
        if _bar_month(minimum) != month or _bar_month(maximum) != month:
            raise ValueError("derived bar partition crosses its month")
        object.__setattr__(self, "min_bar_start_ns", minimum)
        object.__setattr__(self, "max_bar_start_ns", maximum)
        for name in ("logical_content_sha256", "byte_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        expected = _stable_id("derived-bar-partition", self.payload())
        supplied = _optional_text(self.partition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("derived bar partition_id differs")
        object.__setattr__(self, "partition_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return deterministic physical and logical partition evidence."""
        return {
            "schema_version": self.schema_version,
            "bar_schema_version": DERIVED_BAR_SCHEMA_VERSION,
            "relative_path": self.relative_path,
            "symbol": self.symbol,
            "scope": self.scope.value,
            "interval_code": self.interval_code,
            "bar_month": self.bar_month,
            "row_count": self.row_count,
            "min_bar_start_ns": self.min_bar_start_ns,
            "max_bar_start_ns": self.max_bar_start_ns,
            "logical_content_sha256": self.logical_content_sha256,
            "byte_sha256": self.byte_sha256,
            "size_bytes": self.size_bytes,
            "row_group_count": self.row_group_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact partition evidence."""
        return {**self.payload(), "partition_id": self.partition_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DerivedBarPartitionV1":
        """Restore and verify partition evidence."""
        _require_schema(data, DERIVED_BAR_PARTITION_SCHEMA_VERSION)
        _require_derived(data, "bar_schema_version", DERIVED_BAR_SCHEMA_VERSION)
        return cls(
            relative_path=str(data.get("relative_path", "")),
            symbol=str(data.get("symbol", "")),
            scope=ActivitySliceScope(str(data.get("scope", ""))),
            interval_code=str(data.get("interval_code", "")),
            bar_month=str(data.get("bar_month", "")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            min_bar_start_ns=_strict_int(
                data.get("min_bar_start_ns"), "min_bar_start_ns"
            ),
            max_bar_start_ns=_strict_int(
                data.get("max_bar_start_ns"), "max_bar_start_ns"
            ),
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            byte_sha256=str(data.get("byte_sha256", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "size_bytes"),
            row_group_count=_strict_int(
                data.get("row_group_count"), "row_group_count"
            ),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DerivedBarProductManifestV1:
    """Compact identity and replay evidence for one atomic bar product."""

    source_product_manifest_id: str
    source_product_publication_id: str
    source_product_logical_sha256: str
    run_id: str
    ensemble_member_id: str
    query_start_ns: int | None
    query_end_ns: int | None
    policy: DerivedBarPolicyV1
    symbols: tuple[str, ...]
    symbol_bar_counts: Mapping[str, int]
    partitions: tuple[DerivedBarPartitionV1, ...]
    logical_content_sha256: str
    writer_id: str
    writer_library_version: str
    python_runtime: str
    compression: str
    row_group_size: int
    publication_id: str = ""
    manifest_id: str = ""
    schema_version: str = DERIVED_BAR_PRODUCT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            DERIVED_BAR_PRODUCT_SCHEMA_VERSION,
            "derived bar product",
        )
        for name in (
            "source_product_manifest_id",
            "source_product_publication_id",
            "run_id",
            "ensemble_member_id",
            "writer_id",
            "writer_library_version",
            "python_runtime",
            "compression",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.policy, DerivedBarPolicyV1):
            raise TypeError("derived bar manifest requires v1 policy")
        query_start, query_end = _query_bounds(
            self.query_start_ns, self.query_end_ns
        )
        object.__setattr__(self, "query_start_ns", query_start)
        object.__setattr__(self, "query_end_ns", query_end)
        object.__setattr__(
            self,
            "source_product_logical_sha256",
            _required_sha256(
                self.source_product_logical_sha256,
                "source_product_logical_sha256",
            ),
        )
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if not symbols:
            raise ValueError("derived bar product requires symbols")
        object.__setattr__(self, "symbols", symbols)
        counts = {
            _normalized_symbol(symbol): _nonnegative_int(
                count, f"symbol_bar_counts.{symbol}"
            )
            for symbol, count in self.symbol_bar_counts.items()
        }
        if set(counts) != set(symbols) or any(
            not value for value in counts.values()
        ):
            raise ValueError("derived bar symbol counts do not reconcile")
        object.__setattr__(
            self, "symbol_bar_counts", dict(sorted(counts.items()))
        )
        partitions = tuple(
            sorted(
                self.partitions,
                key=lambda item: (
                    item.symbol,
                    item.scope.value,
                    STANDARD_DERIVED_BAR_INTERVALS[item.interval_code],
                    item.bar_month,
                ),
            )
        )
        if not partitions or len(partitions) > MAX_DERIVED_BAR_PARTITIONS:
            raise ValueError(
                "derived bar partition count is empty or unbounded"
            )
        if len({item.relative_path for item in partitions}) != len(partitions):
            raise ValueError(
                "derived bar product contains duplicate partitions"
            )
        partition_counts = dict.fromkeys(symbols, 0)
        for partition in partitions:
            partition_counts[partition.symbol] += partition.row_count
        if partition_counts != counts:
            raise ValueError("derived bar partition rows do not reconcile")
        if {item.symbol for item in partitions} != set(symbols):
            raise ValueError("derived bar partitions do not cover symbols")
        if any(item.scope not in self.policy.scopes for item in partitions):
            raise ValueError("derived bar partition scope differs from policy")
        if any(
            item.interval_code not in self.policy.intervals
            for item in partitions
        ):
            raise ValueError(
                "derived bar partition interval differs from policy"
            )
        object.__setattr__(self, "partitions", partitions)
        logical = _required_sha256(
            self.logical_content_sha256, "logical_content_sha256"
        )
        if logical != _bar_product_logical_sha256(partitions):
            raise ValueError("derived bar logical content hash differs")
        object.__setattr__(self, "logical_content_sha256", logical)
        if self.writer_id != DERIVED_BAR_WRITER_ID:
            raise ValueError("unsupported derived bar writer")
        if self.compression != DERIVED_BAR_COMPRESSION:
            raise ValueError("unsupported derived bar compression")
        object.__setattr__(
            self,
            "row_group_size",
            _bounded_int(self.row_group_size, "row_group_size", 1, 1_000_000),
        )
        expected_publication = _stable_id(
            "derived-bar-publication", self.publication_payload()
        )
        supplied_publication = _optional_text(self.publication_id)
        if (
            supplied_publication is not None
            and supplied_publication != expected_publication
        ):
            raise ValueError("derived bar publication_id differs")
        object.__setattr__(self, "publication_id", expected_publication)
        expected_manifest = _stable_id("derived-bar-manifest", self.payload())
        supplied_manifest = _optional_text(self.manifest_id)
        if (
            supplied_manifest is not None
            and supplied_manifest != expected_manifest
        ):
            raise ValueError("derived bar manifest_id differs")
        object.__setattr__(self, "manifest_id", expected_manifest)
        encoded = self.to_json().encode("utf-8")
        if len(encoded) > MAX_DERIVED_BAR_MANIFEST_BYTES:
            raise ValueError("derived bar manifest exceeds size limit")

    @property
    def bar_count(self) -> int:
        """Return total durable bar rows."""
        return sum(self.symbol_bar_counts.values())

    def publication_payload(self) -> dict[str, JSONValue]:
        """Return logical fields defining the commit directory identity."""
        return {
            "schema_version": self.schema_version,
            "source_product_manifest_id": self.source_product_manifest_id,
            "policy_id": self.policy.policy_id,
            "query_start_ns": self.query_start_ns,
            "query_end_ns": self.query_end_ns,
            "logical_content_sha256": self.logical_content_sha256,
        }

    def payload(self) -> dict[str, JSONValue]:
        """Return full compact manifest evidence except manifest identity."""
        return {
            "schema_version": self.schema_version,
            "source_product_schema_version": (
                RECONSTRUCTION_PRODUCT_SCHEMA_VERSION
            ),
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "bar_schema_version": DERIVED_BAR_SCHEMA_VERSION,
            "source_product_manifest_id": self.source_product_manifest_id,
            "source_product_publication_id": self.source_product_publication_id,
            "source_product_logical_sha256": self.source_product_logical_sha256,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "query_start_ns": self.query_start_ns,
            "query_end_ns": self.query_end_ns,
            "policy": self.policy.to_dict(),
            "symbols": list(self.symbols),
            "symbol_bar_counts": dict(self.symbol_bar_counts),
            "partitions": [item.to_dict() for item in self.partitions],
            "logical_content_sha256": self.logical_content_sha256,
            "logical_hash_algorithm": DERIVED_BAR_LOGICAL_HASH_ALGORITHM,
            "byte_hash_algorithm": DERIVED_BAR_BYTE_HASH_ALGORITHM,
            "writer_id": self.writer_id,
            "writer_library": "pyarrow",
            "writer_library_version": self.writer_library_version,
            "python_runtime": self.python_runtime,
            "compression": self.compression,
            "row_group_size": self.row_group_size,
            "publication_id": self.publication_id,
            "bar_count": self.bar_count,
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
            "raw_m1_input": False,
            "event_schema_augmented": False,
            "centralized_traded_volume_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the complete manifest."""
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Serialize the manifest deterministically."""
        serialized: str = canonical_contract_json(self.to_dict())
        return serialized

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "DerivedBarProductManifestV1":
        """Restore and verify a product manifest."""
        _require_schema(data, DERIVED_BAR_PRODUCT_SCHEMA_VERSION)
        for name, expected in (
            (
                "source_product_schema_version",
                RECONSTRUCTION_PRODUCT_SCHEMA_VERSION,
            ),
            ("event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION),
            ("bar_schema_version", DERIVED_BAR_SCHEMA_VERSION),
            ("logical_hash_algorithm", DERIVED_BAR_LOGICAL_HASH_ALGORITHM),
            ("byte_hash_algorithm", DERIVED_BAR_BYTE_HASH_ALGORITHM),
            ("writer_library", "pyarrow"),
            ("event_rows_inline", False),
            ("analytical_frame_columns_inline", False),
            ("raw_m1_input", False),
            ("event_schema_augmented", False),
            ("centralized_traded_volume_claim", False),
        ):
            _require_derived(data, name, expected)
        result = cls(
            source_product_manifest_id=str(
                data.get("source_product_manifest_id", "")
            ),
            source_product_publication_id=str(
                data.get("source_product_publication_id", "")
            ),
            source_product_logical_sha256=str(
                data.get("source_product_logical_sha256", "")
            ),
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            query_start_ns=_optional_int(
                data.get("query_start_ns"), "query_start_ns"
            ),
            query_end_ns=_optional_int(
                data.get("query_end_ns"), "query_end_ns"
            ),
            policy=DerivedBarPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            symbol_bar_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("symbol_bar_counts"), "symbol_bar_counts"
                ).items()
            },
            partitions=tuple(
                DerivedBarPartitionV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("partitions"), "partitions"
                )
            ),
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            writer_id=str(data.get("writer_id", "")),
            writer_library_version=str(data.get("writer_library_version", "")),
            python_runtime=str(data.get("python_runtime", "")),
            compression=str(data.get("compression", "")),
            row_group_size=_strict_int(
                data.get("row_group_size"), "row_group_size"
            ),
            publication_id=str(data.get("publication_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "bar_count", result.bar_count)
        return result

    @classmethod
    def from_json(cls, text: str) -> "DerivedBarProductManifestV1":
        """Restore a product manifest from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class StagedDerivedBarPublicationV1:
    """Validated but undiscoverable derived-bar publication."""

    root: Path
    staging_directory: Path
    committed_directory: Path
    manifest: DerivedBarProductManifestV1

    @property
    def manifest_path(self) -> Path:
        """Return the staged manifest path."""
        return self.staging_directory / DERIVED_BAR_MANIFEST_FILENAME

    @property
    def manifest_ref(self) -> ArtifactRef:
        """Return a staged manifest reference."""
        return _artifact_ref_for_manifest(self.manifest_path, self.manifest)


@dataclass(frozen=True, slots=True)
class PublishedDerivedBarsV1:
    """One verified committed derived-bar product."""

    manifest: DerivedBarProductManifestV1
    manifest_path: Path
    manifest_ref: ArtifactRef
    idempotent_retry: bool = False


@dataclass(frozen=True, slots=True)
class _BarEventView:
    """Minimal projected source-event fields required by bar aggregation."""

    event_id: str
    origin: SyntheticEventOrigin
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    run_id: str
    ensemble_member_id: str
    source_version_id: str
    generator_id: str | None
    generator_version: str | None
    generator_config_id: str | None
    reference_id: str | None
    motif_id: str | None
    feed_epoch_id: str | None
    broker_profile_id: str | None
    constraint_set_id: str | None
    confidence: float | None


@dataclass(slots=True)
class _BarState:
    """Bounded price, activity, and lineage state for one non-empty bin."""

    source_product_manifest_id: str
    run_id: str
    ensemble_member_id: str
    symbol: str
    scope: ActivitySliceScope
    interval: DerivedBarIntervalV1
    bar_start_ns: int
    policy: DerivedBarPolicyV1
    query_start_ns: int | None
    query_end_ns: int | None
    previous_bid: float | None
    previous_ask: float | None
    first_event: _BarEventView | None = None
    last_event: _BarEventView | None = None
    event_count: int = 0
    observed_event_count: int = 0
    synthetic_event_count: int = 0
    transition_count: int = 0
    price_change_count: int = 0
    stale_quote_count: int = 0
    confidence_total: float = 0.0
    confidence_count: int = 0
    spread_total: float = 0.0
    bid_open: float = 0.0
    bid_high: float = -math.inf
    bid_low: float = math.inf
    bid_close: float = 0.0
    ask_open: float = 0.0
    ask_high: float = -math.inf
    ask_low: float = math.inf
    ask_close: float = 0.0
    mid_open: float = 0.0
    mid_high: float = -math.inf
    mid_low: float = math.inf
    mid_close: float = 0.0
    spread_open: float = 0.0
    spread_high: float = -math.inf
    spread_low: float = math.inf
    spread_close: float = 0.0
    source_version_ids: set[str] = field(default_factory=set)
    generator_ids: set[str] = field(default_factory=set)
    generator_versions: set[str] = field(default_factory=set)
    generator_config_ids: set[str] = field(default_factory=set)
    reference_ids: set[str] = field(default_factory=set)
    motif_ids: set[str] = field(default_factory=set)
    feed_epoch_ids: set[str] = field(default_factory=set)
    broker_profile_ids: set[str] = field(default_factory=set)
    constraint_set_ids: set[str] = field(default_factory=set)
    digest: Any = field(
        default_factory=lambda: hashlib.sha256(b"derived-bar-events-v1\n")
    )

    def add(self, event: _BarEventView) -> None:
        """Consume one event in canonical order."""
        if self.first_event is None:
            self.first_event = event
            self.bid_open = event.bid
            self.ask_open = event.ask
            self.mid_open = (event.bid + event.ask) / 2.0
            self.spread_open = event.ask - event.bid
            previous = (
                (self.previous_bid, self.previous_ask)
                if self.previous_bid is not None
                and self.previous_ask is not None
                else None
            )
        else:
            assert self.last_event is not None
            previous = (self.last_event.bid, self.last_event.ask)
        if previous is not None:
            self.transition_count += 1
            if previous == (event.bid, event.ask):
                self.stale_quote_count += 1
            else:
                self.price_change_count += 1
        self.last_event = event
        self.event_count += 1
        if event.origin is SyntheticEventOrigin.OBSERVED:
            self.observed_event_count += 1
        else:
            self.synthetic_event_count += 1
        mid = (event.bid + event.ask) / 2.0
        spread = event.ask - event.bid
        self.bid_high = max(self.bid_high, event.bid)
        self.bid_low = min(self.bid_low, event.bid)
        self.bid_close = event.bid
        self.ask_high = max(self.ask_high, event.ask)
        self.ask_low = min(self.ask_low, event.ask)
        self.ask_close = event.ask
        self.mid_high = max(self.mid_high, mid)
        self.mid_low = min(self.mid_low, mid)
        self.mid_close = mid
        self.spread_high = max(self.spread_high, spread)
        self.spread_low = min(self.spread_low, spread)
        self.spread_close = spread
        self.spread_total += spread
        if event.confidence is not None:
            self.confidence_total += event.confidence
            self.confidence_count += 1
        self._add_lineage("source_version_ids", event.source_version_id)
        for name in (
            "generator_id",
            "generator_version",
            "generator_config_id",
            "reference_id",
            "motif_id",
            "feed_epoch_id",
            "broker_profile_id",
            "constraint_set_id",
        ):
            value = getattr(event, name)
            if value is not None:
                self._add_lineage(name + "s", value)
        self.digest.update(
            canonical_contract_json(_bar_event_payload(event)).encode("utf-8")
        )
        self.digest.update(b"\n")

    def _add_lineage(self, name: str, value: str) -> None:
        values = cast(set[str], getattr(self, name))
        values.add(_required_text(value))
        if len(values) > self.policy.max_provenance_values:
            raise ValueError(f"derived bar {name} exceeds provenance limit")

    def finalize(self) -> DerivedBarV1:
        """Freeze the non-empty state into one strict bar row."""
        if (
            self.first_event is None
            or self.last_event is None
            or not self.event_count
        ):
            raise ValueError("cannot finalize an empty derived bar")
        duration = (
            self.last_event.event_time_ns - self.first_event.event_time_ns
        )
        return DerivedBarV1(
            source_product_manifest_id=self.source_product_manifest_id,
            policy_id=self.policy.policy_id,
            rounding_digits=self.policy.rounding_digits,
            run_id=self.run_id,
            ensemble_member_id=self.ensemble_member_id,
            symbol=self.symbol,
            scope=self.scope,
            interval_code=self.interval.code,
            interval_ns=self.interval.duration_ns,
            bar_start_ns=self.bar_start_ns,
            bar_end_ns=self.bar_start_ns + self.interval.duration_ns,
            first_event_id=self.first_event.event_id,
            last_event_id=self.last_event.event_id,
            first_event_time_ns=self.first_event.event_time_ns,
            last_event_time_ns=self.last_event.event_time_ns,
            event_count=self.event_count,
            observed_event_count=self.observed_event_count,
            synthetic_event_count=self.synthetic_event_count,
            quote_update_count=self.event_count,
            transition_count=self.transition_count,
            bid_open=_rounded(self.bid_open, self.policy.rounding_digits),
            bid_high=_rounded(self.bid_high, self.policy.rounding_digits),
            bid_low=_rounded(self.bid_low, self.policy.rounding_digits),
            bid_close=_rounded(self.bid_close, self.policy.rounding_digits),
            ask_open=_rounded(self.ask_open, self.policy.rounding_digits),
            ask_high=_rounded(self.ask_high, self.policy.rounding_digits),
            ask_low=_rounded(self.ask_low, self.policy.rounding_digits),
            ask_close=_rounded(self.ask_close, self.policy.rounding_digits),
            mid_open=_rounded(self.mid_open, self.policy.rounding_digits),
            mid_high=_rounded(self.mid_high, self.policy.rounding_digits),
            mid_low=_rounded(self.mid_low, self.policy.rounding_digits),
            mid_close=_rounded(self.mid_close, self.policy.rounding_digits),
            spread_open=_rounded(self.spread_open, self.policy.rounding_digits),
            spread_high=_rounded(self.spread_high, self.policy.rounding_digits),
            spread_low=_rounded(self.spread_low, self.policy.rounding_digits),
            spread_close=_rounded(
                self.spread_close, self.policy.rounding_digits
            ),
            mean_spread=_rounded(
                self.spread_total / self.event_count,
                self.policy.rounding_digits,
            ),
            activity_duration_ns=duration,
            tick_intensity_per_second=(
                _rounded(
                    self.event_count * NANOSECONDS_PER_SECOND / duration,
                    self.policy.rounding_digits,
                )
                if duration
                else None
            ),
            price_change_count=self.price_change_count,
            stale_quote_count=self.stale_quote_count,
            stale_quote_rate=(
                _rounded(
                    self.stale_quote_count / self.transition_count,
                    self.policy.rounding_digits,
                )
                if self.transition_count
                else None
            ),
            mean_event_confidence=(
                _rounded(
                    self.confidence_total / self.confidence_count,
                    self.policy.rounding_digits,
                )
                if self.confidence_count
                else None
            ),
            confidence_support_count=self.confidence_count,
            is_partial_start=(
                self.query_start_ns is not None
                and self.bar_start_ns < self.query_start_ns
            ),
            is_partial_end=(
                self.query_end_ns is not None
                and self.bar_start_ns + self.interval.duration_ns
                > self.query_end_ns
            ),
            source_version_ids=tuple(self.source_version_ids),
            generator_ids=tuple(self.generator_ids),
            generator_versions=tuple(self.generator_versions),
            generator_config_ids=tuple(self.generator_config_ids),
            reference_ids=tuple(self.reference_ids),
            motif_ids=tuple(self.motif_ids),
            feed_epoch_ids=tuple(self.feed_epoch_ids),
            broker_profile_ids=tuple(self.broker_profile_ids),
            constraint_set_ids=tuple(self.constraint_set_ids),
            event_content_sha256=self.digest.hexdigest(),
        )


class _BarAccumulator:
    """Route ordered events to bounded symbol/scope/interval bar states."""

    def __init__(
        self,
        *,
        source_product_manifest_id: str,
        run_id: str,
        ensemble_member_id: str,
        policy: DerivedBarPolicyV1,
        query_start_ns: int | None,
        query_end_ns: int | None,
        grouped_symbols: bool = False,
    ) -> None:
        self.source_product_manifest_id = _required_text(
            source_product_manifest_id
        )
        self.run_id = _required_text(run_id)
        self.ensemble_member_id = _required_text(ensemble_member_id)
        self.policy = policy
        self.query_start_ns = query_start_ns
        self.query_end_ns = query_end_ns
        self.grouped_symbols = grouped_symbols
        self.current_symbol: str | None = None
        self.states: dict[tuple[str, ActivitySliceScope, str], _BarState] = {}
        self.previous_quotes: dict[
            tuple[str, ActivitySliceScope, str], tuple[float, float]
        ] = {}
        self.last_positions: dict[str, tuple[int, int, str]] = {}
        self.symbols: set[str] = set()
        self.bar_count = 0

    def add(self, event: _BarEventView) -> tuple[DerivedBarV1, ...]:
        """Consume one event and emit bars closed by its interval position."""
        if event.run_id != self.run_id:
            raise ValueError("derived bar event run_id differs")
        if event.ensemble_member_id != self.ensemble_member_id:
            raise ValueError("derived bar event ensemble member differs")
        symbol = _normalized_symbol(event.symbol)
        emitted: list[DerivedBarV1] = []
        if self.grouped_symbols and self.current_symbol != symbol:
            if self.current_symbol is not None:
                if symbol <= self.current_symbol:
                    raise ValueError(
                        "derived bar grouped symbols are not strictly ordered"
                    )
                emitted.extend(self._finish_symbol(self.current_symbol))
            self.current_symbol = symbol
        position = (event.event_time_ns, event.event_sequence, event.event_id)
        previous_position = self.last_positions.get(symbol)
        if previous_position is not None and position <= previous_position:
            raise ValueError(
                "derived bar events are not strictly ordered per symbol"
            )
        self.last_positions[symbol] = position
        self.symbols.add(symbol)
        if len(self.symbols) > self.policy.max_symbols:
            raise ValueError("derived bar symbols exceed policy limit")
        origin_scope = (
            ActivitySliceScope.OBSERVED
            if event.origin is SyntheticEventOrigin.OBSERVED
            else ActivitySliceScope.SYNTHETIC
        )
        scopes = tuple(
            scope
            for scope in (origin_scope, ActivitySliceScope.MERGED)
            if scope in self.policy.scopes
        )
        for scope in scopes:
            for interval in self.policy.interval_contracts:
                key = (symbol, scope, interval.code)
                start = _bar_start(event.event_time_ns, interval.duration_ns)
                state = self.states.get(key)
                if state is not None and state.bar_start_ns != start:
                    emitted.append(self._finalize(key, state))
                    state = None
                if state is None:
                    previous_quote = self.previous_quotes.get(key)
                    state = _BarState(
                        source_product_manifest_id=(
                            self.source_product_manifest_id
                        ),
                        run_id=self.run_id,
                        ensemble_member_id=self.ensemble_member_id,
                        symbol=symbol,
                        scope=scope,
                        interval=interval,
                        bar_start_ns=start,
                        policy=self.policy,
                        query_start_ns=self.query_start_ns,
                        query_end_ns=self.query_end_ns,
                        previous_bid=(
                            previous_quote[0] if previous_quote else None
                        ),
                        previous_ask=(
                            previous_quote[1] if previous_quote else None
                        ),
                    )
                    self.states[key] = state
                state.add(event)
        return tuple(emitted)

    def finish(self) -> tuple[DerivedBarV1, ...]:
        """Emit all final non-empty interval states."""
        bars = tuple(
            self._finalize(key, state)
            for key, state in sorted(
                self.states.items(), key=lambda item: item[0]
            )
        )
        self.states.clear()
        return bars

    def _finish_symbol(self, symbol: str) -> tuple[DerivedBarV1, ...]:
        keys = sorted(key for key in self.states if key[0] == symbol)
        return tuple(self._finalize(key, self.states[key]) for key in keys)

    def _finalize(
        self,
        key: tuple[str, ActivitySliceScope, str],
        state: _BarState,
    ) -> DerivedBarV1:
        bar = state.finalize()
        assert state.last_event is not None
        self.previous_quotes[key] = (state.last_event.bid, state.last_event.ask)
        self.states.pop(key, None)
        self.bar_count += 1
        if self.bar_count > self.policy.max_bars:
            raise ValueError("derived bar output exceeds policy limit")
        return bar


def derive_reconstruction_bars(
    events: Iterable[SyntheticEventV1],
    *,
    source_product_manifest_id: str,
    run_id: str,
    ensemble_member_id: str,
    policy: DerivedBarPolicyV1 | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> tuple[DerivedBarV1, ...]:
    """Derive bounded deterministic bars from ordered narrow events."""
    selected = policy or DerivedBarPolicyV1()
    start, end = _query_bounds(start_ns, end_ns)
    accumulator = _BarAccumulator(
        source_product_manifest_id=source_product_manifest_id,
        run_id=run_id,
        ensemble_member_id=ensemble_member_id,
        policy=selected,
        query_start_ns=start,
        query_end_ns=end,
    )
    bars: list[DerivedBarV1] = []
    for event in events:
        if not isinstance(event, SyntheticEventV1):
            raise TypeError("derived bars require SyntheticEventV1 rows")
        if start is not None and event.event_time_ns < start:
            continue
        if end is not None and event.event_time_ns >= end:
            continue
        bars.extend(accumulator.add(_bar_event_view(event)))
    bars.extend(accumulator.finish())
    return tuple(sorted(bars, key=_bar_order_key))


def iter_committed_reconstruction_bars(
    manifest_path: str | Path,
    *,
    policy: DerivedBarPolicyV1 | None = None,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
    batch_size: int = DEFAULT_DERIVED_BAR_BATCH_SIZE,
) -> Iterator[DerivedBarV1]:
    """Stream bars from verified projected final-event Parquet batches."""
    product = verify_reconstruction_publication(manifest_path)
    selected = policy or DerivedBarPolicyV1()
    start, end = _query_bounds(start_ns, end_ns)
    size = _bounded_int(batch_size, "batch_size", 1, 1_000_000)
    selected_symbols = tuple(_normalized_symbol(value) for value in symbols)
    accumulator = _BarAccumulator(
        source_product_manifest_id=product.manifest_id,
        run_id=product.run_id,
        ensemble_member_id=product.ensemble_member_id,
        policy=selected,
        query_start_ns=start,
        query_end_ns=end,
        grouped_symbols=True,
    )
    for batch in iter_reconstruction_event_batches(
        manifest_path,
        columns=DERIVED_BAR_EVENT_COLUMNS,
        symbols=selected_symbols,
        start_ns=start,
        end_ns=end,
        batch_size=size,
    ):
        for row in batch.to_pylist():
            mapping = _mapping(row, "derived bar event row")
            yield from accumulator.add(_bar_event_view_from_mapping(mapping))
    yield from accumulator.finish()


class _PartitionWriter:
    """Bounded buffered writer and digest for one physical partition."""

    def __init__(
        self,
        staging_directory: Path,
        first: DerivedBarV1,
        *,
        row_group_size: int,
        buffer_rows: int,
    ) -> None:
        self.symbol = first.symbol
        self.scope = first.scope
        self.interval_code = first.interval_code
        self.bar_month = _bar_month(first.bar_start_ns)
        self.relative_path = _bar_partition_relative_path(
            self.symbol, self.scope, self.interval_code, self.bar_month
        )
        self.path = staging_directory / self.relative_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _, pq = _arrow_modules()
        self.writer = pq.ParquetWriter(
            self.path,
            derived_bar_arrow_schema(),
            compression=DERIVED_BAR_COMPRESSION,
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
            data_page_version="2.0",
            write_page_checksum=True,
        )
        self.row_group_size = row_group_size
        self.buffer_rows = buffer_rows
        self.buffer: list[DerivedBarV1] = []
        self.row_count = 0
        self.minimum: int | None = None
        self.maximum: int | None = None
        self.last_order: tuple[int, str] | None = None
        self.digest = hashlib.sha256(
            (DERIVED_BAR_LOGICAL_HASH_ALGORITHM + "\n").encode("ascii")
        )

    def add(self, bar: DerivedBarV1) -> None:
        """Append one compatible ordered bar."""
        if (
            bar.symbol,
            bar.scope,
            bar.interval_code,
            _bar_month(bar.bar_start_ns),
        ) != (self.symbol, self.scope, self.interval_code, self.bar_month):
            raise ValueError("derived bar writer axis differs")
        order = (bar.bar_start_ns, bar.bar_id)
        if self.last_order is not None and order <= self.last_order:
            raise ValueError("derived bar partition order differs")
        self.last_order = order
        self.minimum = (
            bar.bar_start_ns
            if self.minimum is None
            else min(self.minimum, bar.bar_start_ns)
        )
        self.maximum = (
            bar.bar_start_ns
            if self.maximum is None
            else max(self.maximum, bar.bar_start_ns)
        )
        self.row_count += 1
        self.digest.update(bar.to_json().encode("utf-8"))
        self.digest.update(b"\n")
        self.buffer.append(bar)
        if len(self.buffer) >= self.buffer_rows:
            self.flush()

    def flush(self) -> None:
        """Write buffered rows as bounded row groups."""
        if not self.buffer:
            return
        table = _bars_to_arrow(self.buffer)
        self.writer.write_table(table, row_group_size=self.row_group_size)
        self.buffer.clear()

    def close(self) -> DerivedBarPartitionV1:
        """Close, fsync, and freeze partition evidence."""
        self.flush()
        self.writer.close()
        _fsync_file(self.path)
        _fsync_directory(self.path.parent)
        if self.minimum is None or self.maximum is None or not self.row_count:
            raise DerivedBarPersistenceError(
                "derived bar writer produced no rows"
            )
        _, pq = _arrow_modules()
        parquet = pq.ParquetFile(self.path)
        return DerivedBarPartitionV1(
            relative_path=self.relative_path,
            symbol=self.symbol,
            scope=self.scope,
            interval_code=self.interval_code,
            bar_month=self.bar_month,
            row_count=self.row_count,
            min_bar_start_ns=self.minimum,
            max_bar_start_ns=self.maximum,
            logical_content_sha256=self.digest.hexdigest(),
            byte_sha256=_file_sha256(self.path),
            size_bytes=self.path.stat().st_size,
            row_group_count=parquet.num_row_groups,
        )

    def abort(self) -> None:
        """Release the Parquet handle without producing evidence."""
        self.writer.close()


class _PartitionManager:
    """Keep at most one active month writer per symbol/scope/interval."""

    def __init__(
        self,
        staging_directory: Path,
        *,
        row_group_size: int,
        buffer_rows: int,
    ) -> None:
        self.staging_directory = staging_directory
        self.row_group_size = row_group_size
        self.buffer_rows = buffer_rows
        self.active: dict[
            tuple[str, ActivitySliceScope, str], _PartitionWriter
        ] = {}
        self.partitions: list[DerivedBarPartitionV1] = []
        self.current_symbol: str | None = None

    def add(self, bar: DerivedBarV1) -> None:
        """Route one bar to its monthly partition."""
        if self.current_symbol != bar.symbol:
            if self.current_symbol is not None:
                if bar.symbol <= self.current_symbol:
                    raise DerivedBarPersistenceError(
                        "derived bar output symbols are not strictly ordered"
                    )
                self._close_symbol(self.current_symbol)
            self.current_symbol = bar.symbol
        axis = (bar.symbol, bar.scope, bar.interval_code)
        writer = self.active.get(axis)
        if writer is not None and writer.bar_month != _bar_month(
            bar.bar_start_ns
        ):
            self._close(axis, writer)
            writer = None
        if writer is None:
            writer = _PartitionWriter(
                self.staging_directory,
                bar,
                row_group_size=self.row_group_size,
                buffer_rows=self.buffer_rows,
            )
            self.active[axis] = writer
        writer.add(bar)

    def finish(self) -> tuple[DerivedBarPartitionV1, ...]:
        """Close all active writers and return stable partition evidence."""
        for axis, writer in tuple(self.active.items()):
            self._close(axis, writer)
        if not self.partitions:
            raise DerivedBarPersistenceError(
                "derived bar publication contains no rows"
            )
        return tuple(self.partitions)

    def abort(self) -> None:
        """Release all open partition handles after a failed transaction."""
        for writer in tuple(self.active.values()):
            try:
                writer.abort()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        self.active.clear()

    def _close_symbol(self, symbol: str) -> None:
        for axis, writer in tuple(self.active.items()):
            if axis[0] == symbol:
                self._close(axis, writer)

    def _close(
        self,
        axis: tuple[str, ActivitySliceScope, str],
        writer: _PartitionWriter,
    ) -> None:
        self.partitions.append(writer.close())
        self.active.pop(axis, None)
        if len(self.partitions) > MAX_DERIVED_BAR_PARTITIONS:
            raise DerivedBarPersistenceError(
                "derived bar partitions exceed limit"
            )


def stage_derived_bar_publication(
    root: str | Path,
    source_manifest_path: str | Path,
    *,
    policy: DerivedBarPolicyV1 | None = None,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
    batch_size: int = DEFAULT_DERIVED_BAR_BATCH_SIZE,
    row_group_size: int = DEFAULT_DERIVED_BAR_ROW_GROUP_SIZE,
    write_buffer_rows: int = DEFAULT_DERIVED_BAR_WRITE_BUFFER_ROWS,
) -> StagedDerivedBarPublicationV1:
    """Aggregate and validate one bar product below hidden scratch."""
    source_path = Path(source_manifest_path).expanduser().resolve()
    source = verify_reconstruction_publication(source_path)
    selected = policy or DerivedBarPolicyV1()
    selected_symbols = tuple(_normalized_symbol(value) for value in symbols)
    if selected_symbols and not set(selected_symbols).issubset(source.symbols):
        raise ValueError("derived bar symbols are outside the source product")
    row_group = _bounded_int(row_group_size, "row_group_size", 1, 1_000_000)
    buffer_rows = _bounded_int(
        write_buffer_rows, "write_buffer_rows", 1, row_group
    )
    size = _bounded_int(batch_size, "batch_size", 1, 1_000_000)
    query_start, query_end = _query_bounds(start_ns, end_ns)
    root_path = Path(root).expanduser().resolve()
    axis = _bar_axis_directory(
        root_path, source.manifest_id, selected.policy_id
    )
    scratch = axis / ".scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="publication.tmp-", dir=scratch))
    manager: _PartitionManager | None = None
    try:
        manager = _PartitionManager(
            staging,
            row_group_size=row_group,
            buffer_rows=buffer_rows,
        )
        for bar in iter_committed_reconstruction_bars(
            source_path,
            policy=selected,
            symbols=selected_symbols,
            start_ns=query_start,
            end_ns=query_end,
            batch_size=size,
        ):
            manager.add(bar)
        partitions = manager.finish()
        counts: dict[str, int] = {}
        for partition in partitions:
            counts[partition.symbol] = (
                counts.get(partition.symbol, 0) + partition.row_count
            )
        pa, _ = _arrow_modules()
        manifest = DerivedBarProductManifestV1(
            source_product_manifest_id=source.manifest_id,
            source_product_publication_id=source.publication_id,
            source_product_logical_sha256=source.replay.logical_content_sha256,
            run_id=source.run_id,
            ensemble_member_id=source.ensemble_member_id,
            query_start_ns=query_start,
            query_end_ns=query_end,
            policy=selected,
            symbols=tuple(counts),
            symbol_bar_counts=counts,
            partitions=partitions,
            logical_content_sha256=_bar_product_logical_sha256(partitions),
            writer_id=DERIVED_BAR_WRITER_ID,
            writer_library_version=str(pa.__version__),
            python_runtime=(
                f"{sys.version_info.major}.{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            ),
            compression=DERIVED_BAR_COMPRESSION,
            row_group_size=row_group,
        )
        manifest_bytes = manifest.to_json().encode("utf-8")
        _atomic_write_bytes(
            staging / DERIVED_BAR_MANIFEST_FILENAME, manifest_bytes
        )
        committed = axis / "commits" / _path_component(manifest.publication_id)
        staged = StagedDerivedBarPublicationV1(
            root=root_path,
            staging_directory=staging,
            committed_directory=committed,
            manifest=manifest,
        )
        _verify_derived_bar_directory(staging, manifest, committed=False)
        return staged
    except Exception:
        if manager is not None:
            manager.abort()
        _remove_bar_scratch(staging, root_path)
        raise


def commit_derived_bar_publication(
    staged: StagedDerivedBarPublicationV1,
) -> PublishedDerivedBarsV1:
    """Atomically promote one validated derived-bar product."""
    if not isinstance(staged, StagedDerivedBarPublicationV1):
        raise TypeError("derived bar commit requires staged publication")
    final = staged.committed_directory
    manifest = staged.manifest
    if final.exists():
        existing_path = final / DERIVED_BAR_MANIFEST_FILENAME
        existing = verify_derived_bar_publication(existing_path)
        if existing != manifest:
            raise DerivedBarPersistenceError(
                "derived bar publication identity contains different evidence"
            )
        if staged.staging_directory.exists():
            _remove_bar_scratch(staged.staging_directory, staged.root)
        return PublishedDerivedBarsV1(
            manifest=existing,
            manifest_path=existing_path,
            manifest_ref=_artifact_ref_for_manifest(existing_path, existing),
            idempotent_retry=True,
        )
    _verify_derived_bar_directory(
        staged.staging_directory, manifest, committed=False
    )
    final.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(staged.staging_directory, final)
    except OSError as err:
        if not final.exists():
            raise
        existing_path = final / DERIVED_BAR_MANIFEST_FILENAME
        existing = verify_derived_bar_publication(existing_path)
        if existing != manifest:
            raise DerivedBarPersistenceError(
                "concurrent derived bar publication differs"
            ) from err
        if staged.staging_directory.exists():
            _remove_bar_scratch(staged.staging_directory, staged.root)
        return PublishedDerivedBarsV1(
            manifest=existing,
            manifest_path=existing_path,
            manifest_ref=_artifact_ref_for_manifest(existing_path, existing),
            idempotent_retry=True,
        )
    _fsync_directory(final.parent)
    manifest_path = final / DERIVED_BAR_MANIFEST_FILENAME
    verified = verify_derived_bar_publication(manifest_path)
    return PublishedDerivedBarsV1(
        manifest=verified,
        manifest_path=manifest_path,
        manifest_ref=_artifact_ref_for_manifest(manifest_path, verified),
    )


def publish_derived_bars(
    root: str | Path,
    source_manifest_path: str | Path,
    *,
    policy: DerivedBarPolicyV1 | None = None,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
    batch_size: int = DEFAULT_DERIVED_BAR_BATCH_SIZE,
    row_group_size: int = DEFAULT_DERIVED_BAR_ROW_GROUP_SIZE,
    write_buffer_rows: int = DEFAULT_DERIVED_BAR_WRITE_BUFFER_ROWS,
) -> PublishedDerivedBarsV1:
    """Stage, validate, and atomically commit a derived-bar product."""
    staged = stage_derived_bar_publication(
        root,
        source_manifest_path,
        policy=policy,
        symbols=symbols,
        start_ns=start_ns,
        end_ns=end_ns,
        batch_size=batch_size,
        row_group_size=row_group_size,
        write_buffer_rows=write_buffer_rows,
    )
    try:
        return commit_derived_bar_publication(staged)
    except Exception:
        if staged.staging_directory.exists():
            _remove_bar_scratch(staged.staging_directory, staged.root)
        raise


def load_derived_bar_manifest(
    path: str | Path,
) -> DerivedBarProductManifestV1:
    """Load and verify a compact manifest without reading Parquet."""
    payload = Path(path).read_bytes()
    if len(payload) > MAX_DERIVED_BAR_MANIFEST_BYTES:
        raise DerivedBarPersistenceError(
            "derived bar manifest exceeds size limit"
        )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as err:
        raise DerivedBarPersistenceError(
            "derived bar manifest is not UTF-8"
        ) from err
    return DerivedBarProductManifestV1.from_json(text)


def verify_derived_bar_publication(
    manifest_path: str | Path,
) -> DerivedBarProductManifestV1:
    """Fail closed unless every committed bar artifact reconciles."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = load_derived_bar_manifest(path)
    _validate_committed_bar_location(path, manifest)
    _verify_derived_bar_directory(path.parent, manifest, committed=True)
    return manifest


def discover_derived_bar_manifests(root: str | Path) -> tuple[Path, ...]:
    """Discover only fully committed, verified derived-bar products."""
    product_root = (
        Path(root).expanduser().resolve() / DERIVED_BAR_PRODUCT_DIRECTORY
    )
    if not product_root.exists():
        return ()
    matches: list[Path] = []
    for path in sorted(product_root.glob("**/commits/*/manifest.json")):
        verify_derived_bar_publication(path)
        matches.append(path.resolve())
    return tuple(matches)


def iter_derived_bar_batches(
    manifest_path: str | Path,
    *,
    columns: Iterable[str] = DERIVED_BAR_ARROW_COLUMNS,
    symbols: Iterable[str] = (),
    scopes: Iterable[ActivitySliceScope | str] = (),
    intervals: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
    batch_size: int = DEFAULT_DERIVED_BAR_BATCH_SIZE,
) -> Iterator[Any]:
    """Stream projected committed bar batches with partition pruning."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = verify_derived_bar_publication(path)
    requested = tuple(columns)
    if not requested:
        raise ValueError("derived bar scan requires columns")
    unknown = set(requested).difference(DERIVED_BAR_ARROW_COLUMNS)
    if unknown:
        raise ValueError(f"unknown derived bar columns: {sorted(unknown)}")
    selected_symbols, selected_scopes, selected_intervals = (
        _validate_bar_scan_filters(manifest, symbols, scopes, intervals)
    )
    start, end = _query_bounds(start_ns, end_ns)
    size = _bounded_int(batch_size, "batch_size", 1, 1_000_000)
    _, _, ds = _arrow_dataset_modules()
    expression = None
    if start is not None:
        expression = ds.field("bar_end_ns") > start
    if end is not None:
        upper = ds.field("bar_start_ns") < end
        expression = upper if expression is None else expression & upper
    for partition in manifest.partitions:
        if selected_symbols and partition.symbol not in selected_symbols:
            continue
        if selected_scopes and partition.scope not in selected_scopes:
            continue
        if (
            selected_intervals
            and partition.interval_code not in selected_intervals
        ):
            continue
        if (
            start is not None
            and partition.max_bar_start_ns
            + STANDARD_DERIVED_BAR_INTERVALS[partition.interval_code]
            <= start
        ):
            continue
        if end is not None and partition.min_bar_start_ns >= end:
            continue
        dataset = ds.dataset(
            path.parent / partition.relative_path, format="parquet"
        )
        scanner = dataset.scanner(
            columns=list(requested),
            filter=expression,
            batch_size=size,
            use_threads=False,
        )
        yield from scanner.to_batches()


def scan_derived_bars_polars(
    manifest_path: str | Path,
    *,
    columns: Iterable[str] = DERIVED_BAR_ARROW_COLUMNS,
    symbols: Iterable[str] = (),
    scopes: Iterable[ActivitySliceScope | str] = (),
    intervals: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Any:
    """Return a predicate-pushed lazy Polars bar scan."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = verify_derived_bar_publication(path)
    requested = tuple(columns)
    unknown = set(requested).difference(DERIVED_BAR_ARROW_COLUMNS)
    if not requested or unknown:
        raise ValueError("derived bar scan columns are empty or unknown")
    selected_symbols, selected_scopes, selected_intervals = (
        _validate_bar_scan_filters(manifest, symbols, scopes, intervals)
    )
    start, end = _query_bounds(start_ns, end_ns)
    paths = [
        path.parent / item.relative_path
        for item in manifest.partitions
        if (not selected_symbols or item.symbol in selected_symbols)
        and (not selected_scopes or item.scope in selected_scopes)
        and (not selected_intervals or item.interval_code in selected_intervals)
        and (
            start is None
            or item.max_bar_start_ns
            + STANDARD_DERIVED_BAR_INTERVALS[item.interval_code]
            > start
        )
        and (end is None or item.min_bar_start_ns < end)
    ]
    pl = _polars_module()
    if not paths:
        return (
            pl.from_arrow(derived_bar_arrow_schema().empty_table())
            .lazy()
            .select(list(requested))
        )
    lazy = pl.scan_parquet(
        [str(item) for item in paths],
        hive_partitioning=False,
        use_statistics=True,
    )
    if start is not None:
        lazy = lazy.filter(pl.col("bar_end_ns") > start)
    if end is not None:
        lazy = lazy.filter(pl.col("bar_start_ns") < end)
    return lazy.select(list(requested))


def derived_bar_arrow_schema() -> Any:
    """Return the exact narrow Arrow schema for derived bar rows."""
    pa, _ = _arrow_modules()

    def text(name: str) -> Any:
        return pa.field(name, pa.string(), nullable=False)

    def integer(name: str) -> Any:
        return pa.field(name, pa.int64(), nullable=False)

    def number(name: str, *, nullable: bool = False) -> Any:
        return pa.field(name, pa.float64(), nullable=nullable)

    fields = [
        text("schema_version"),
        text("event_schema_version"),
        text("bar_id"),
        text("source_product_manifest_id"),
        text("policy_id"),
        integer("rounding_digits"),
        text("run_id"),
        text("ensemble_member_id"),
        text("symbol"),
        text("scope"),
        text("interval_code"),
        integer("interval_ns"),
        integer("bar_start_ns"),
        integer("bar_end_ns"),
        text("first_event_id"),
        text("last_event_id"),
        integer("first_event_time_ns"),
        integer("last_event_time_ns"),
        integer("event_count"),
        integer("observed_event_count"),
        integer("synthetic_event_count"),
        integer("quote_update_count"),
        integer("transition_count"),
    ]
    fields.extend(
        number(name)
        for name in (
            "bid_open",
            "bid_high",
            "bid_low",
            "bid_close",
            "ask_open",
            "ask_high",
            "ask_low",
            "ask_close",
            "mid_open",
            "mid_high",
            "mid_low",
            "mid_close",
            "spread_open",
            "spread_high",
            "spread_low",
            "spread_close",
            "mean_spread",
        )
    )
    fields.extend(
        [
            integer("activity_duration_ns"),
            number("tick_intensity_per_second", nullable=True),
            integer("price_change_count"),
            integer("stale_quote_count"),
            number("stale_quote_rate", nullable=True),
            number("mean_event_confidence", nullable=True),
            integer("confidence_support_count"),
            text("volume_state"),
            number("volume", nullable=True),
            pa.field("is_partial_start", pa.bool_(), nullable=False),
            pa.field("is_partial_end", pa.bool_(), nullable=False),
        ]
    )
    fields.extend(
        pa.field(name, pa.list_(pa.string()), nullable=False)
        for name in (
            "source_version_ids",
            "generator_ids",
            "generator_versions",
            "generator_config_ids",
            "reference_ids",
            "motif_ids",
            "feed_epoch_ids",
            "broker_profile_ids",
            "constraint_set_ids",
        )
    )
    fields.extend(
        [
            text("event_content_sha256"),
            pa.field("event_schema_augmented", pa.bool_(), nullable=False),
            pa.field("raw_m1_input", pa.bool_(), nullable=False),
            pa.field(
                "centralized_traded_volume_claim", pa.bool_(), nullable=False
            ),
        ]
    )
    schema = pa.schema(fields)
    if tuple(schema.names) != DERIVED_BAR_ARROW_COLUMNS:
        raise RuntimeError("derived bar Arrow schema order drifted")
    return schema


def _bars_to_arrow(bars: Iterable[DerivedBarV1]) -> Any:
    pa, _ = _arrow_modules()
    return pa.Table.from_pylist(
        [bar.to_dict() for bar in bars], schema=derived_bar_arrow_schema()
    )


def _verify_derived_bar_directory(
    directory: Path,
    manifest: DerivedBarProductManifestV1,
    *,
    committed: bool,
) -> None:
    if not directory.is_dir() or directory.is_symlink():
        raise DerivedBarPersistenceError(
            "derived bar directory is missing or unsafe"
        )
    if committed:
        _validate_committed_bar_location(
            directory / DERIVED_BAR_MANIFEST_FILENAME, manifest
        )
    expected_files = {DERIVED_BAR_MANIFEST_FILENAME}
    expected_files.update(item.relative_path for item in manifest.partitions)
    expected_directories = {
        parent.as_posix()
        for relative_path in expected_files
        for parent in PurePosixPath(relative_path).parents
        if parent != PurePosixPath(".")
    }
    if any(item.is_symlink() for item in directory.rglob("*")):
        raise DerivedBarPersistenceError(
            "derived bar publication contains unsafe symlink"
        )
    actual_files = {
        item.relative_to(directory).as_posix()
        for item in directory.rglob("*")
        if item.is_file()
    }
    actual_directories = {
        item.relative_to(directory).as_posix()
        for item in directory.rglob("*")
        if item.is_dir()
    }
    if (
        actual_files != expected_files
        or actual_directories != expected_directories
    ):
        raise DerivedBarPersistenceError("derived bar artifact set differs")
    for partition in manifest.partitions:
        _validate_bar_partition_file(
            directory / partition.relative_path,
            partition,
            manifest,
            manifest.row_group_size,
        )
    if (
        _bar_product_logical_sha256(manifest.partitions)
        != manifest.logical_content_sha256
    ):
        raise DerivedBarPersistenceError("derived bar replay hash differs")


def _validate_bar_partition_file(
    path: Path,
    expected: DerivedBarPartitionV1,
    manifest: DerivedBarProductManifestV1,
    row_group_size: int,
) -> None:
    if not path.is_file() or path.is_symlink():
        raise DerivedBarPersistenceError(
            "derived bar partition is missing or unsafe"
        )
    if path.stat().st_size != expected.size_bytes:
        raise DerivedBarPersistenceError("derived bar partition size differs")
    if _file_sha256(path) != expected.byte_sha256:
        raise DerivedBarPersistenceError(
            "derived bar partition byte hash differs"
        )
    _, pq = _arrow_modules()
    try:
        parquet = pq.ParquetFile(path)
    except Exception as err:
        raise DerivedBarPersistenceError(
            "derived bar footer is unreadable"
        ) from err
    if not parquet.schema_arrow.remove_metadata().equals(
        derived_bar_arrow_schema().remove_metadata()
    ):
        raise DerivedBarPersistenceError("derived bar partition schema differs")
    if parquet.num_row_groups != expected.row_group_count:
        raise DerivedBarPersistenceError("derived bar row-group count differs")
    digest = hashlib.sha256(
        (DERIVED_BAR_LOGICAL_HASH_ALGORITHM + "\n").encode("ascii")
    )
    count = 0
    minimum: int | None = None
    maximum: int | None = None
    last_order: tuple[int, str] | None = None
    for ordinal in range(parquet.num_row_groups):
        group_rows = parquet.metadata.row_group(ordinal).num_rows
        if group_rows < 1 or group_rows > row_group_size:
            raise DerivedBarPersistenceError(
                "derived bar row group exceeds bound"
            )
        for row in parquet.read_row_group(ordinal).to_pylist():
            bar = DerivedBarV1.from_dict(_mapping(row, "derived bar row"))
            if (
                bar.symbol != expected.symbol
                or bar.scope is not expected.scope
                or bar.interval_code != expected.interval_code
                or _bar_month(bar.bar_start_ns) != expected.bar_month
            ):
                raise DerivedBarPersistenceError(
                    "derived bar partition axis differs"
                )
            if (
                bar.source_product_manifest_id
                != manifest.source_product_manifest_id
                or bar.run_id != manifest.run_id
                or bar.ensemble_member_id != manifest.ensemble_member_id
                or bar.policy_id != manifest.policy.policy_id
                or bar.rounding_digits != manifest.policy.rounding_digits
            ):
                raise DerivedBarPersistenceError(
                    "derived bar row provenance differs from manifest"
                )
            if (
                manifest.query_start_ns is not None
                and bar.first_event_time_ns < manifest.query_start_ns
            ) or (
                manifest.query_end_ns is not None
                and bar.last_event_time_ns >= manifest.query_end_ns
            ):
                raise DerivedBarPersistenceError(
                    "derived bar event bounds escape the manifest query"
                )
            expected_partial_start = (
                manifest.query_start_ns is not None
                and bar.bar_start_ns < manifest.query_start_ns
            )
            expected_partial_end = (
                manifest.query_end_ns is not None
                and bar.bar_end_ns > manifest.query_end_ns
            )
            if (
                bar.is_partial_start != expected_partial_start
                or bar.is_partial_end != expected_partial_end
            ):
                raise DerivedBarPersistenceError(
                    "derived bar partial flags differ from manifest query"
                )
            order = (bar.bar_start_ns, bar.bar_id)
            if last_order is not None and order <= last_order:
                raise DerivedBarPersistenceError(
                    "derived bar partition order differs"
                )
            last_order = order
            count += 1
            minimum = (
                bar.bar_start_ns
                if minimum is None
                else min(minimum, bar.bar_start_ns)
            )
            maximum = (
                bar.bar_start_ns
                if maximum is None
                else max(maximum, bar.bar_start_ns)
            )
            digest.update(bar.to_json().encode("utf-8"))
            digest.update(b"\n")
    if (
        count != expected.row_count
        or minimum != expected.min_bar_start_ns
        or maximum != expected.max_bar_start_ns
        or digest.hexdigest() != expected.logical_content_sha256
    ):
        raise DerivedBarPersistenceError(
            "derived bar partition content differs"
        )


def _validate_committed_bar_location(
    manifest_path: Path,
    manifest: DerivedBarProductManifestV1,
) -> None:
    expected_suffix = (
        PurePosixPath(
            _bar_axis_directory(
                Path("."),
                manifest.source_product_manifest_id,
                manifest.policy.policy_id,
            ).as_posix()
        )
        / "commits"
        / _path_component(manifest.publication_id)
        / DERIVED_BAR_MANIFEST_FILENAME
    )
    actual = PurePosixPath(manifest_path.as_posix())
    if (
        len(actual.parts) < len(expected_suffix.parts)
        or actual.parts[-len(expected_suffix.parts) :] != expected_suffix.parts
    ):
        raise DerivedBarPersistenceError(
            "derived bar manifest location differs"
        )


def _bar_axis_directory(
    root: Path, source_manifest_id: str, policy_id: str
) -> Path:
    return (
        root
        / DERIVED_BAR_PRODUCT_DIRECTORY
        / f"schema={_path_component(DERIVED_BAR_PRODUCT_SCHEMA_VERSION)}"
        / f"source={_path_component(source_manifest_id)}"
        / f"policy={_path_component(policy_id)}"
    )


def _bar_partition_relative_path(
    symbol: str,
    scope: ActivitySliceScope,
    interval_code: str,
    bar_month: str,
) -> str:
    return (
        f"interval={_path_component(interval_code)}/"
        f"scope={_path_component(scope.value)}/"
        f"symbol={_path_component(symbol)}/"
        f"bar_month={_required_bar_month(bar_month)}/part-00000.parquet"
    )


def _bar_product_logical_sha256(
    partitions: Iterable[DerivedBarPartitionV1],
) -> str:
    payload: list[dict[str, JSONValue]] = [
        {
            "relative_path": item.relative_path,
            "row_count": item.row_count,
            "logical_content_sha256": item.logical_content_sha256,
        }
        for item in sorted(partitions, key=lambda value: value.relative_path)
    ]
    return _content_sha256(
        {"algorithm": DERIVED_BAR_LOGICAL_HASH_ALGORITHM, "partitions": payload}
    )


def _bar_event_view(event: SyntheticEventV1) -> _BarEventView:
    return _BarEventView(
        event_id=event.event_id,
        origin=event.origin,
        symbol=event.symbol,
        event_time_ns=event.event_time_ns,
        event_sequence=event.event_sequence,
        bid=event.bid,
        ask=event.ask,
        run_id=event.run_id,
        ensemble_member_id=event.ensemble_member_id,
        source_version_id=event.source_version_id,
        generator_id=event.generator_id,
        generator_version=event.generator_version,
        generator_config_id=event.generator_config_id,
        reference_id=event.reference_id,
        motif_id=event.motif_id,
        feed_epoch_id=event.feed_epoch_id,
        broker_profile_id=event.broker_profile_id,
        constraint_set_id=event.constraint_set_id,
        confidence=event.confidence,
    )


def _bar_event_view_from_mapping(data: Mapping[str, Any]) -> _BarEventView:
    return _BarEventView(
        event_id=_required_text(data.get("event_id")),
        origin=SyntheticEventOrigin(str(data.get("origin", ""))),
        symbol=_normalized_symbol(str(data.get("symbol", ""))),
        event_time_ns=_strict_int(data.get("event_time_ns"), "event_time_ns"),
        event_sequence=_nonnegative_int(
            data.get("event_sequence"), "event_sequence"
        ),
        bid=_positive_float(data.get("bid"), "bid"),
        ask=_positive_float(data.get("ask"), "ask"),
        run_id=_required_text(data.get("run_id")),
        ensemble_member_id=_required_text(data.get("ensemble_member_id")),
        source_version_id=_required_text(data.get("source_version_id")),
        generator_id=_mapping_optional_text(data, "generator_id"),
        generator_version=_mapping_optional_text(data, "generator_version"),
        generator_config_id=_mapping_optional_text(data, "generator_config_id"),
        reference_id=_mapping_optional_text(data, "reference_id"),
        motif_id=_mapping_optional_text(data, "motif_id"),
        feed_epoch_id=_mapping_optional_text(data, "feed_epoch_id"),
        broker_profile_id=_mapping_optional_text(data, "broker_profile_id"),
        constraint_set_id=_mapping_optional_text(data, "constraint_set_id"),
        confidence=_optional_ratio(data.get("confidence"), "confidence"),
    )


def _bar_event_payload(event: _BarEventView) -> dict[str, JSONValue]:
    return {
        "event_id": event.event_id,
        "origin": event.origin.value,
        "symbol": event.symbol,
        "event_time_ns": event.event_time_ns,
        "event_sequence": event.event_sequence,
        "bid": event.bid,
        "ask": event.ask,
        "run_id": event.run_id,
        "ensemble_member_id": event.ensemble_member_id,
        "source_version_id": event.source_version_id,
        "generator_id": event.generator_id,
        "generator_version": event.generator_version,
        "generator_config_id": event.generator_config_id,
        "reference_id": event.reference_id,
        "motif_id": event.motif_id,
        "feed_epoch_id": event.feed_epoch_id,
        "broker_profile_id": event.broker_profile_id,
        "constraint_set_id": event.constraint_set_id,
        "confidence": event.confidence,
    }


def _bar_order_key(bar: DerivedBarV1) -> tuple[str, str, int, int, str]:
    return (
        bar.symbol,
        bar.scope.value,
        bar.interval_ns,
        bar.bar_start_ns,
        bar.bar_id,
    )


def _bar_start(event_time_ns: int, duration_ns: int) -> int:
    return (event_time_ns // duration_ns) * duration_ns


def _bar_month(bar_start_ns: int) -> str:
    seconds = (
        _strict_int(bar_start_ns, "bar_start_ns") // NANOSECONDS_PER_SECOND
    )
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m")


def _required_bar_month(value: str) -> str:
    month = _required_text(value)
    if not _BAR_MONTH_RE.fullmatch(month):
        raise ValueError("bar_month must be YYYY-MM")
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError as err:
        raise ValueError("bar_month is invalid") from err
    return month


def _query_bounds(
    start_ns: int | None, end_ns: int | None
) -> tuple[int | None, int | None]:
    start = _optional_int(start_ns, "start_ns")
    end = _optional_int(end_ns, "end_ns")
    if start is not None and end is not None and end <= start:
        raise ValueError("derived bar end_ns must exceed start_ns")
    return start, end


def _validate_bar_scan_filters(
    manifest: DerivedBarProductManifestV1,
    symbols: Iterable[str],
    scopes: Iterable[ActivitySliceScope | str],
    intervals: Iterable[str],
) -> tuple[set[str], set[ActivitySliceScope], set[str]]:
    selected_symbols = {_normalized_symbol(value) for value in symbols}
    selected_scopes = {ActivitySliceScope(value) for value in scopes}
    selected_intervals = {_required_text(value).lower() for value in intervals}
    if selected_symbols and not selected_symbols.issubset(manifest.symbols):
        raise ValueError("derived bar symbols are outside the product manifest")
    if selected_scopes and not selected_scopes.issubset(
        set(manifest.policy.scopes)
    ):
        raise ValueError("derived bar scopes are outside the product policy")
    if selected_intervals and not selected_intervals.issubset(
        manifest.policy.intervals
    ):
        raise ValueError("derived bar intervals are outside the product policy")
    return selected_symbols, selected_scopes, selected_intervals


def _artifact_ref_for_manifest(
    path: Path, manifest: DerivedBarProductManifestV1
) -> ArtifactRef:
    payload = path.read_bytes()
    return ArtifactRef(
        kind=DERIVED_BAR_MANIFEST_ARTIFACT_KIND,
        path=str(path),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        metadata={
            "schema_version": manifest.schema_version,
            "manifest_id": manifest.manifest_id,
            "publication_id": manifest.publication_id,
            "source_product_manifest_id": manifest.source_product_manifest_id,
            "bar_count": manifest.bar_count,
            "logical_content_sha256": manifest.logical_content_sha256,
        },
    )


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".tmp-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_bar_scratch(path: Path, root: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink():
        path.unlink(missing_ok=True)
        return
    expected_root = root.resolve() / DERIVED_BAR_PRODUCT_DIRECTORY
    try:
        path.resolve().relative_to(expected_root)
    except ValueError as err:
        raise DerivedBarPersistenceError(
            "derived bar scratch cleanup escaped product root"
        ) from err
    shutil.rmtree(path)


def _fsync_file(path: Path) -> None:
    # Windows maps fsync to the writable-handle-only CRT commit operation.
    with path.open("rb+") as stream:
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            digest.update(chunk)
    return digest.hexdigest()


def _path_component(value: str) -> str:
    return quote(_required_text(value), safe="-_.")


def _safe_relative_path(value: str) -> str:
    text = _required_text(value)
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or "\\" in text:
        raise ValueError("derived bar artifact path must be safe and relative")
    return path.as_posix()


def _arrow_modules() -> tuple[Any, Any]:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError(
            "derived bar persistence requires histdatacom[arrow]"
        ) from err
    return pa, pq


def _arrow_dataset_modules() -> tuple[Any, Any, Any]:
    pa, pq = _arrow_modules()
    try:
        import pyarrow.dataset as ds  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError("derived bar scans require pyarrow.dataset") from err
    return pa, pq, ds


def _polars_module() -> Any:
    try:
        import polars as pl  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError("derived bar scans require polars") from err
    return pl


def _content_sha256(value: Any) -> str:
    return hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(payload)}"


def _rounded(value: float, digits: int) -> float:
    return round(_finite_float(value, "derived value"), digits)


def _matches_rounded_value(actual: float, expected: float, digits: int) -> bool:
    unit = 10.0**-digits
    tolerance = 1.5 * unit + 4 * math.ulp(float(expected))
    return math.isclose(actual, expected, rel_tol=0.0, abs_tol=tolerance)


def _required_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > MAX_DERIVED_BAR_TEXT:
        raise ValueError("required derived bar text is empty or unbounded")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return _required_text(normalized) if normalized else None


def _normalized_symbol(value: str) -> str:
    symbol = _required_text(value).lower()
    if not symbol.isalnum():
        raise ValueError("derived bar symbols must be alphanumeric")
    return symbol


def _normalized_text_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _strict_bool(value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError("derived bar partial flags must be boolean")
    return value


def _optional_int(value: Any, name: str) -> int | None:
    return None if value is None else _strict_int(value, name)


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside its bounded range")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _optional_nonnegative_float(value: Any, name: str) -> float | None:
    return None if value is None else _nonnegative_float(value, name)


def _optional_ratio(value: Any, name: str) -> float | None:
    if value is None:
        return None
    result = _finite_float(value, name)
    if not 0 <= result <= 1:
        raise ValueError(f"{name} must be between zero and one")
    return result


def _required_sha256(value: Any, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return normalized


def _require_version(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema version")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    _require_version(
        str(data.get("schema_version", "")), expected, "derived bar"
    )


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived bar field {name} differs")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item, name) for item in _sequence(value, name))


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    return tuple(_required_text(item) for item in _sequence(value, name))


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    return _optional_text(data.get(name))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(text), "derived bar JSON")
    except json.JSONDecodeError as err:
        raise ValueError("derived bar JSON is invalid") from err


__all__ = [
    "DEFAULT_DERIVED_BAR_BATCH_SIZE",
    "DEFAULT_DERIVED_BAR_MAX_BARS",
    "DEFAULT_DERIVED_BAR_MAX_PROVENANCE_VALUES",
    "DEFAULT_DERIVED_BAR_MAX_SYMBOLS",
    "DEFAULT_DERIVED_BAR_ROUNDING_DIGITS",
    "DEFAULT_DERIVED_BAR_ROW_GROUP_SIZE",
    "DEFAULT_DERIVED_BAR_WRITE_BUFFER_ROWS",
    "DERIVED_BAR_ARROW_COLUMNS",
    "DERIVED_BAR_EVENT_COLUMNS",
    "DERIVED_BAR_INTERVAL_SCHEMA_VERSION",
    "DERIVED_BAR_MANIFEST_ARTIFACT_KIND",
    "DERIVED_BAR_PARTITION_SCHEMA_VERSION",
    "DERIVED_BAR_POLICY_SCHEMA_VERSION",
    "DERIVED_BAR_PRODUCT_DIRECTORY",
    "DERIVED_BAR_PRODUCT_SCHEMA_VERSION",
    "DERIVED_BAR_SCHEMA_VERSION",
    "STANDARD_DERIVED_BAR_INTERVALS",
    "DerivedBarIntervalV1",
    "DerivedBarPartitionV1",
    "DerivedBarPersistenceError",
    "DerivedBarPolicyV1",
    "DerivedBarProductManifestV1",
    "DerivedBarV1",
    "PublishedDerivedBarsV1",
    "StagedDerivedBarPublicationV1",
    "commit_derived_bar_publication",
    "derive_reconstruction_bars",
    "derived_bar_arrow_schema",
    "discover_derived_bar_manifests",
    "iter_committed_reconstruction_bars",
    "iter_derived_bar_batches",
    "load_derived_bar_manifest",
    "publish_derived_bars",
    "scan_derived_bars_polars",
    "stage_derived_bar_publication",
    "verify_derived_bar_publication",
]
