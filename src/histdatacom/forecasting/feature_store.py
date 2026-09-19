"""Bounded ragged-edge assembly; no network or current-database shortcuts."""

from __future__ import annotations

import statistics
import threading
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from histdatacom.runtime_contracts import JSONValue

from .contracts import (
    MAX_FORECAST_ARTIFACT_BYTES,
    _array,
    _finite,
    _json,
    _load,
    _mapping,
    _ns,
    _seal,
    _text,
    _verify,
)
from .feature_contracts import (
    MAX_FEATURE_RECORDS,
    FeatureCellStatus,
    FeatureColumnV1,
    FeatureObservationV1,
    FeaturePeriodV1,
    FeatureRequestV1,
    FeatureScheduleV1,
    FeatureSourceMode,
    FeatureTransformKind,
    _count,
)

_ADMISSIBLE = {FeatureSourceMode.HISTORICAL_VINTAGE, FeatureSourceMode.OBSERVED}
MAX_FEATURE_LINEAGE_REFERENCES = 200_000


def _validate_evidence(
    observations: tuple[FeatureObservationV1, ...],
    schedules: tuple[FeatureScheduleV1, ...],
) -> None:
    _count(
        len(observations) + len(schedules),
        "source records",
        MAX_FEATURE_RECORDS,
    )
    chains: dict[tuple[str, FeaturePeriodV1], list[FeatureObservationV1]] = {}
    retained_bytes = 0
    for item in observations:
        if not isinstance(item, FeatureObservationV1):
            raise TypeError("store requires immutable feature observations")
        if item.evidence.mode not in _ADMISSIBLE:
            raise ValueError(
                "ex-ante store refuses current-revised/ex-post/synthetic inputs"
            )
        retained_bytes += len(_json(item.to_dict()).encode("utf-8"))
        if retained_bytes > MAX_FORECAST_ARTIFACT_BYTES:
            raise ValueError("feature source inventory exceeds byte bound")
        chains.setdefault(
            (item.definition.feature_key, item.period), []
        ).append(item)
    for chain in chains.values():
        chain.sort(key=lambda item: item.vintage_sequence)
        for sequence, item in enumerate(chain):
            if item.vintage_sequence != sequence:
                raise ValueError("duplicate or missing vintage sequence")
            if sequence:
                previous = chain[sequence - 1]
                if item.supersedes_id != previous.observation_id:
                    raise ValueError(
                        "vintage predecessor differs from exact retained record"
                    )
                if item.available_at_ns < previous.available_at_ns:
                    raise ValueError("revision availability moves backwards")
    seen: set[tuple[str, FeaturePeriodV1, int]] = set()
    for schedule in schedules:
        if not isinstance(schedule, FeatureScheduleV1):
            raise TypeError("store requires immutable feature schedules")
        if schedule.evidence.mode not in _ADMISSIBLE:
            raise ValueError("ex-ante store refuses ex-post schedule evidence")
        retained_bytes += len(_json(schedule.to_dict()).encode("utf-8"))
        if retained_bytes > MAX_FORECAST_ARTIFACT_BYTES:
            raise ValueError("feature source inventory exceeds byte bound")
        key = (schedule.feature_key, schedule.period, schedule.known_at_ns)
        if key in seen:
            raise ValueError("ambiguous duplicate schedule knowledge timestamp")
        seen.add(key)


def _observation_order(
    item: FeatureObservationV1,
) -> tuple[str, int, int, str, int]:
    return (
        item.definition.feature_key,
        item.period.start_ns,
        item.period.end_ns,
        item.period.label,
        item.vintage_sequence,
    )


def _schedule_order(item: FeatureScheduleV1) -> tuple[str, int, int, str, int]:
    return (
        item.feature_key,
        item.period.start_ns,
        item.period.end_ns,
        item.period.label,
        item.known_at_ns,
    )


@dataclass(frozen=True, slots=True)
class FeatureCellV1:
    """Derived audit view; executable snapshots always recompute these cells."""

    column: str
    period: FeaturePeriodV1
    value: float | None
    status: FeatureCellStatus
    unit: str | None
    semantic_id: str | None
    observation_ids: tuple[str, ...]
    schedule_id: str | None
    available_at_ns: int | None
    value_age_ns: int | None
    reference_age_ns: int
    availability_lag_ns: int | None
    revision_news: float | None
    revision_news_available_at_ns: int | None
    revision_observation_ids: tuple[str, ...]
    transform_support: int
    fit_cutoff_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "column", _text(self.column, "cell column"))
        if not isinstance(self.period, FeaturePeriodV1):
            raise TypeError("cell requires typed reference period")
        object.__setattr__(self, "status", FeatureCellStatus(self.status))
        if self.value is not None:
            object.__setattr__(self, "value", _finite(self.value, "cell value"))
        if (self.value is not None) != (
            self.status is FeatureCellStatus.AVAILABLE
        ):
            raise ValueError("cell availability/value mismatch")
        for name in ("unit", "semantic_id", "schedule_id"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _text(value, name))
        _ns(self.fit_cutoff_at_ns, "cell fit cutoff")
        if self.available_at_ns is not None:
            _ns(self.available_at_ns, "cell availability")
            if (
                type(self.value_age_ns) is not int
                or self.available_at_ns > self.fit_cutoff_at_ns
                or self.value_age_ns
                != self.fit_cutoff_at_ns - self.available_at_ns
            ):
                raise ValueError("cell availability/age mismatch")
        elif self.value_age_ns is not None:
            raise ValueError("unavailable cell has no vintage age")
        if (
            type(self.reference_age_ns) is not int
            or self.reference_age_ns
            != self.fit_cutoff_at_ns - self.period.end_ns
        ):
            raise ValueError("cell reference age mismatch")
        if (
            self.availability_lag_ns is not None
            and type(self.availability_lag_ns) is not int
        ):
            raise ValueError("availability lag must be an integer duration")
        for name in ("observation_ids", "revision_observation_ids"):
            values = tuple(_text(item, name) for item in getattr(self, name))
            _count(len(values), name, MAX_FEATURE_RECORDS)
            if len(set(values)) != len(values):
                raise ValueError("duplicate cell lineage identity")
            object.__setattr__(self, name, values)
        _count(self.transform_support, "transform support", MAX_FEATURE_RECORDS)
        if self.value is not None and (
            not self.observation_ids
            or self.unit is None
            or self.semantic_id is None
            or self.available_at_ns is None
        ):
            raise ValueError(
                "available cell requires semantic and source lineage"
            )
        if self.revision_news is not None:
            object.__setattr__(
                self,
                "revision_news",
                _finite(self.revision_news, "cell revision news"),
            )
            if (
                len(self.revision_observation_ids) != 2
                or self.revision_news_available_at_ns is None
            ):
                raise ValueError(
                    "revision news requires two exact source vintages"
                )
            _ns(
                self.revision_news_available_at_ns, "revision news availability"
            )
            if self.revision_news_available_at_ns > self.fit_cutoff_at_ns:
                raise ValueError("revision news follows cell cutoff")
        elif (
            self.revision_observation_ids
            or self.revision_news_available_at_ns is not None
        ):
            raise ValueError("unexpected revision-news evidence")
        self.to_dict()

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-cell",
            {
                "column": self.column,
                "period": self.period.to_dict(),
                "value": self.value,
                "status": self.status.value,
                "unit": self.unit,
                "semantic_id": self.semantic_id,
                "observation_ids": list(self.observation_ids),
                "schedule_id": self.schedule_id,
                "available_at_ns": self.available_at_ns,
                "value_age_ns": self.value_age_ns,
                "reference_age_ns": self.reference_age_ns,
                "availability_lag_ns": self.availability_lag_ns,
                "revision_news": self.revision_news,
                "revision_news_available_at_ns": self.revision_news_available_at_ns,
                "revision_observation_ids": list(self.revision_observation_ids),
                "transform_support": self.transform_support,
                "fit_cutoff_at_ns": self.fit_cutoff_at_ns,
            },
        )


def _base_cell(
    column: FeatureColumnV1,
    period: FeaturePeriodV1,
    chain: tuple[FeatureObservationV1, ...],
    schedules: tuple[FeatureScheduleV1, ...],
    cutoff: int,
) -> FeatureCellV1:
    schedule = schedules[-1] if schedules else None
    schedule_id = None if schedule is None else str(schedule.to_dict()["id"])
    latest = chain[-1] if chain else None
    age = None if latest is None else cutoff - latest.available_at_ns
    status = FeatureCellStatus.UNAVAILABLE
    value = None
    if latest is not None:
        status = FeatureCellStatus.AVAILABLE
        value = latest.value
        if value is None:
            status = FeatureCellStatus.SOURCE_MISSING
        elif (
            column.max_age_ns is not None
            and age is not None
            and age > column.max_age_ns
        ):
            status = FeatureCellStatus.STALE
            value = None
    revision_news = None
    if len(chain) > 1:
        previous, current = chain[-2:]
        if (
            previous.value is not None
            and current.value is not None
            and previous.definition.semantic_id
            == current.definition.semantic_id
        ):
            revision_news = _finite(
                current.value - previous.value, "revision news"
            )
    return FeatureCellV1(
        column.name,
        period,
        value,
        status,
        None if latest is None else latest.definition.unit,
        None if latest is None else latest.definition.semantic_id,
        tuple(item.observation_id for item in chain),
        schedule_id,
        None if latest is None else latest.available_at_ns,
        age,
        cutoff - period.end_ns,
        (
            None
            if latest is None or schedule is None
            else latest.available_at_ns - schedule.scheduled_at_ns
        ),
        revision_news,
        (
            None
            if revision_news is None or latest is None
            else latest.available_at_ns
        ),
        (
            ()
            if revision_news is None
            else tuple(item.observation_id for item in chain[-2:])
        ),
        int(value is not None),
        cutoff,
    )


def _transform_cells(
    base: tuple[FeatureCellV1, ...], column: FeatureColumnV1
) -> tuple[FeatureCellV1, ...]:
    """All windows end at the row; unavailable positions are never skipped."""
    spec = column.transform
    if spec.kind is FeatureTransformKind.LEVEL:
        return base
    result = []
    lineage_references = 0
    for index, current in enumerate(base):
        start = (
            0
            if spec.kind is FeatureTransformKind.EXPANDING_MEAN
            else max(0, index - spec.window + 1)
        )
        support = base[start : index + 1]
        warmup = False
        if spec.kind in {
            FeatureTransformKind.LAG,
            FeatureTransformKind.DIFFERENCE,
        }:
            warmup = index < spec.window
            support = (
                ()
                if warmup
                else (
                    (base[index - spec.window],)
                    if spec.kind is FeatureTransformKind.LAG
                    else (base[index - spec.window], current)
                )
            )
        else:
            warmup = len(support) < spec.min_support
        status = (
            FeatureCellStatus.WARMUP if warmup else FeatureCellStatus.AVAILABLE
        )
        if not warmup and any(
            item.status is not FeatureCellStatus.AVAILABLE for item in support
        ):
            status = FeatureCellStatus.UNAVAILABLE
        semantic_ids = {
            item.semantic_id for item in support if item.semantic_id is not None
        }
        if not warmup and len(semantic_ids) > 1:
            status = FeatureCellStatus.SEMANTIC_BREAK
        values = [item.value for item in support if item.value is not None]
        value = None
        if status is FeatureCellStatus.AVAILABLE:
            if spec.kind is FeatureTransformKind.LAG:
                value = values[0]
            elif spec.kind is FeatureTransformKind.DIFFERENCE:
                value = values[1] - values[0]
            elif spec.kind is FeatureTransformKind.ROLLING_ZSCORE:
                mean = _finite(statistics.mean(values), "rolling mean")
                try:
                    scale = _finite(
                        statistics.stdev(values), "rolling sample scale"
                    )
                except OverflowError as exc:
                    raise ValueError(
                        "rolling sample scale must be finite"
                    ) from exc
                if scale == 0:
                    status = FeatureCellStatus.ZERO_SCALE
                else:
                    value = (values[-1] - mean) / scale
            else:
                value = statistics.mean(values)
            if value is not None:
                value = _finite(value, "transformed feature")
        lineage_references += sum(len(item.observation_ids) for item in support)
        if lineage_references > MAX_FEATURE_LINEAGE_REFERENCES:
            raise ValueError(
                "transformed feature lineage exceeds reference bound"
            )
        lineage = tuple(
            dict.fromkeys(
                identity
                for item in support
                for identity in item.observation_ids
            )
        )
        available = max(
            (
                item.available_at_ns
                for item in support
                if item.available_at_ns is not None
            ),
            default=None,
        )
        result.append(
            FeatureCellV1(
                column.name,
                current.period,
                value,
                status,
                (
                    "standard-deviations"
                    if spec.kind is FeatureTransformKind.ROLLING_ZSCORE
                    else (support[-1].unit if support else None)
                ),
                next(iter(semantic_ids)) if len(semantic_ids) == 1 else None,
                lineage,
                current.schedule_id,
                available,
                (
                    None
                    if available is None
                    else current.fit_cutoff_at_ns - available
                ),
                current.reference_age_ns,
                None,
                current.revision_news,
                current.revision_news_available_at_ns,
                current.revision_observation_ids,
                len(values),
                current.fit_cutoff_at_ns,
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class FeatureMatrixSnapshotV1:
    """Exact selected vintage chains; derived cells are verified on restoration."""

    request: FeatureRequestV1
    observations: tuple[FeatureObservationV1, ...]
    schedules: tuple[FeatureScheduleV1, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.request, FeatureRequestV1):
            raise TypeError("feature snapshot requires its typed request")
        _validate_evidence(tuple(self.observations), tuple(self.schedules))
        observations = tuple(sorted(self.observations, key=_observation_order))
        schedules = tuple(sorted(self.schedules, key=_schedule_order))
        keys = {item.feature_key for item in self.request.columns}
        periods = set(self.request.periods)
        cutoff = self.request.cutoff_at_ns
        if any(
            item.definition.feature_key not in keys
            or item.period not in periods
            or item.available_at_ns > cutoff
            for item in observations
        ):
            raise ValueError(
                "feature vintage is outside the requested information set"
            )
        if any(
            item.feature_key not in keys
            or item.period not in periods
            or item.known_at_ns > cutoff
            for item in schedules
        ):
            raise ValueError(
                "schedule evidence is outside the requested information set"
            )
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "schedules", schedules)
        self.to_json()

    @property
    def snapshot_id(self) -> str:
        return str(self.to_dict()["id"])

    @property
    def cells(self) -> tuple[FeatureCellV1, ...]:
        by_period: dict[
            tuple[str, FeaturePeriodV1], list[FeatureObservationV1]
        ] = {}
        schedules: dict[
            tuple[str, FeaturePeriodV1], list[FeatureScheduleV1]
        ] = {}
        for item in self.observations:
            by_period.setdefault(
                (item.definition.feature_key, item.period), []
            ).append(item)
        for schedule in self.schedules:
            schedules.setdefault(
                (schedule.feature_key, schedule.period), []
            ).append(schedule)
        columns = []
        lineage_references = 0
        for column in self.request.columns:
            base = tuple(
                _base_cell(
                    column,
                    period,
                    tuple(by_period.get((column.feature_key, period), ())),
                    tuple(schedules.get((column.feature_key, period), ())),
                    self.request.cutoff_at_ns,
                )
                for period in self.request.periods
            )
            transformed = _transform_cells(base, column)
            lineage_references += sum(
                len(item.observation_ids) + len(item.revision_observation_ids)
                for item in transformed
            )
            if lineage_references > MAX_FEATURE_LINEAGE_REFERENCES:
                raise ValueError(
                    "feature matrix lineage exceeds reference bound"
                )
            columns.append(transformed)
        return tuple(
            column[row]
            for row in range(len(self.request.periods))
            for column in columns
        )

    def column(self, name: str) -> tuple[FeatureCellV1, ...]:
        if name not in {item.name for item in self.request.columns}:
            raise ValueError("feature column is not in the sealed request")
        return tuple(item for item in self.cells if item.column == name)

    def verify_against(self, store: VintageFeatureStoreV1) -> None:
        if not isinstance(store, VintageFeatureStoreV1):
            raise TypeError("replay requires an ex-ante vintage store")
        if store.snapshot(self.request).to_json() != self.to_json():
            raise ValueError(
                "historical feature information set is missing or changed"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return _seal(
            "feature-matrix-snapshot",
            {
                "request": self.request.to_dict(),
                "observations": [item.to_dict() for item in self.observations],
                "schedules": [item.to_dict() for item in self.schedules],
                "cells": [item.to_dict() for item in self.cells],
            },
        )

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeatureMatrixSnapshotV1:
        result = cls(
            FeatureRequestV1.from_dict(_mapping(data["request"])),
            tuple(
                FeatureObservationV1.from_dict(_mapping(item))
                for item in _array(data["observations"])
            ),
            tuple(
                FeatureScheduleV1.from_dict(_mapping(item))
                for item in _array(data["schedules"])
            ),
        )
        _verify(data, result.to_dict())
        return result

    @classmethod
    def from_json(cls, text: str) -> FeatureMatrixSnapshotV1:
        return cls.from_dict(_load(text))


class VintageFeatureStoreV1:
    """Immutable source inventory with a byte/entry-bounded local LRU.

    Construct a new store to append records. Cache keys are local to that
    immutable inventory; no global store identity enters historical snapshots.
    """

    def __init__(
        self,
        observations: tuple[FeatureObservationV1, ...],
        schedules: tuple[FeatureScheduleV1, ...] = (),
        *,
        cache_entries: int = 8,
        cache_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        _count(cache_entries, "cache entries", 128)
        _count(cache_bytes, "cache bytes", 32 * 1024 * 1024)
        _validate_evidence(tuple(observations), tuple(schedules))
        self._observations = tuple(sorted(observations, key=_observation_order))
        self._schedules = tuple(sorted(schedules, key=_schedule_order))
        self._cache_entries = cache_entries
        self._cache_bytes = cache_bytes
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._cache_size = 0
        self._lock = threading.RLock()

    @property
    def cache_usage(self) -> tuple[int, int]:
        with self._lock:
            return len(self._cache), self._cache_size

    def snapshot(self, request: FeatureRequestV1) -> FeatureMatrixSnapshotV1:
        with self._lock:
            return self._snapshot(request)

    def _snapshot(self, request: FeatureRequestV1) -> FeatureMatrixSnapshotV1:
        if not isinstance(request, FeatureRequestV1):
            raise TypeError("store accepts only a typed as-of request")
        key = request.request_id
        if key in self._cache:
            self._cache.move_to_end(key)
            return FeatureMatrixSnapshotV1.from_json(self._cache[key])
        keys = {item.feature_key for item in request.columns}
        periods = set(request.periods)
        result = FeatureMatrixSnapshotV1(
            request,
            tuple(
                item
                for item in self._observations
                if item.definition.feature_key in keys
                and item.period in periods
                and item.available_at_ns <= request.cutoff_at_ns
            ),
            tuple(
                item
                for item in self._schedules
                if item.feature_key in keys
                and item.period in periods
                and item.known_at_ns <= request.cutoff_at_ns
            ),
        )
        payload = result.to_json()
        size = len(payload.encode("utf-8"))
        if self._cache_entries and size <= self._cache_bytes:
            while self._cache and (
                len(self._cache) >= self._cache_entries
                or self._cache_size + size > self._cache_bytes
            ):
                _, evicted = self._cache.popitem(last=False)
                self._cache_size -= len(evicted.encode("utf-8"))
            self._cache[key] = payload
            self._cache_size += size
        return result
