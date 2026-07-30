"""Narrow, versioned contracts for reconstructed market-event streams.

The contracts in this module deliberately contain no generator, carving,
workflow, or final partitioning behavior.  They freeze the portable event
identity and serialization boundary that those later stages share.

Version-one schemas are immutable.  A semantic change to a required field,
identity derivation, ordering rule, or Arrow type requires a new schema
version and a new contract class.  Readers reject other schema versions.
Optional fields may be absent from JSON input and unknown JSON keys are
ignored, but the version-one Arrow schema is exact so persisted columns cannot
drift silently.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, cast

from histdatacom.runtime_contracts import JSONValue

SYNTHETIC_EVENT_SCHEMA_VERSION = "histdatacom.synthetic-event.v1"
SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION = "histdatacom.synthetic-event-stream.v1"
SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.synthetic-ensemble-manifest.v1"
)

EVENT_SCHEMA_METADATA_KEY = b"histdatacom.synthetic_event_schema"
EVENT_TIME_UNIT_METADATA_KEY = b"histdatacom.event_time_unit"
STREAM_METADATA_KEY = b"histdatacom.synthetic_event_stream"

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

SYNTHETIC_EVENT_ARROW_COLUMNS = (
    "schema_version",
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
    "source_series_id",
    "source_period",
    "source_row_id",
    "anchor_interval_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
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


class SyntheticEventOrigin(str, Enum):
    """Whether a product row is immutable evidence or generated infill."""

    OBSERVED = "observed"
    SYNTHETIC = "synthetic"

    @classmethod
    def from_value(
        cls, value: str | "SyntheticEventOrigin"
    ) -> "SyntheticEventOrigin":
        """Return a strict normalized event origin."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported synthetic event origin") from err


@dataclass(frozen=True, slots=True)
class SyntheticEventV1:
    """One observed or generated bid/ask event at nanosecond resolution."""

    origin: SyntheticEventOrigin
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    run_id: str
    ensemble_member_id: str
    source_version_id: str
    source_series_id: str | None = None
    source_period: str | None = None
    source_row_id: int | None = None
    anchor_interval_id: str | None = None
    left_anchor_event_id: str | None = None
    right_anchor_event_id: str | None = None
    generator_id: str | None = None
    generator_version: str | None = None
    generator_config_id: str | None = None
    reference_id: str | None = None
    motif_id: str | None = None
    feed_epoch_id: str | None = None
    broker_profile_id: str | None = None
    constraint_set_id: str | None = None
    confidence: float | None = None
    event_id: str = ""
    schema_version: str = SYNTHETIC_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Normalize values and enforce origin-specific lineage."""
        if self.schema_version != SYNTHETIC_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported synthetic event schema version")
        object.__setattr__(
            self,
            "origin",
            SyntheticEventOrigin.from_value(self.origin),
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _required_text(self.ensemble_member_id),
        )
        object.__setattr__(
            self,
            "source_version_id",
            _required_text(self.source_version_id),
        )
        for name in _OPTIONAL_EVENT_TEXT_FIELDS:
            object.__setattr__(self, name, _optional_text(getattr(self, name)))

        object.__setattr__(
            self,
            "event_time_ns",
            _bounded_int(self.event_time_ns, "event_time_ns"),
        )
        sequence = _bounded_int(self.event_sequence, "event_sequence")
        if sequence < 0:
            raise ValueError("event_sequence must be non-negative")
        object.__setattr__(self, "event_sequence", sequence)
        bid = _finite_float(self.bid, "bid")
        ask = _finite_float(self.ask, "ask")
        if bid <= 0.0 or ask <= 0.0:
            raise ValueError("bid and ask must be positive")
        if ask < bid:
            raise ValueError("ask must be greater than or equal to bid")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        if self.confidence is not None:
            confidence = _finite_float(self.confidence, "confidence")
            if not 0.0 <= confidence <= 1.0:
                raise ValueError("confidence must be between zero and one")
            object.__setattr__(self, "confidence", confidence)

        self._validate_lineage()
        expected = _stable_id("event", self.identity_payload())
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id does not match deterministic identity")
        object.__setattr__(self, "event_id", expected)

    @classmethod
    def observed(
        cls,
        *,
        symbol: str,
        event_time_ns: int,
        event_sequence: int,
        bid: float,
        ask: float,
        run_id: str,
        ensemble_member_id: str,
        source_version_id: str,
        source_series_id: str,
        source_period: str,
        source_row_id: int,
    ) -> "SyntheticEventV1":
        """Construct one immutable observed event."""
        return cls(
            origin=SyntheticEventOrigin.OBSERVED,
            symbol=symbol,
            event_time_ns=event_time_ns,
            event_sequence=event_sequence,
            bid=bid,
            ask=ask,
            run_id=run_id,
            ensemble_member_id=ensemble_member_id,
            source_version_id=source_version_id,
            source_series_id=source_series_id,
            source_period=source_period,
            source_row_id=source_row_id,
        )

    @classmethod
    def generated(
        cls,
        *,
        symbol: str,
        event_time_ns: int,
        event_sequence: int,
        bid: float,
        ask: float,
        run_id: str,
        ensemble_member_id: str,
        source_version_id: str,
        left_anchor_event_id: str,
        right_anchor_event_id: str,
        generator_id: str,
        generator_version: str,
        generator_config_id: str,
        constraint_set_id: str,
        confidence: float | None = None,
        anchor_interval_id: str | None = None,
        reference_id: str | None = None,
        motif_id: str | None = None,
        feed_epoch_id: str | None = None,
        broker_profile_id: str | None = None,
    ) -> "SyntheticEventV1":
        """Construct one generated event with reproducible lineage."""
        interval_id = anchor_interval_id or derive_anchor_interval_id(
            left_anchor_event_id,
            right_anchor_event_id,
        )
        return cls(
            origin=SyntheticEventOrigin.SYNTHETIC,
            symbol=symbol,
            event_time_ns=event_time_ns,
            event_sequence=event_sequence,
            bid=bid,
            ask=ask,
            run_id=run_id,
            ensemble_member_id=ensemble_member_id,
            source_version_id=source_version_id,
            anchor_interval_id=interval_id,
            left_anchor_event_id=left_anchor_event_id,
            right_anchor_event_id=right_anchor_event_id,
            generator_id=generator_id,
            generator_version=generator_version,
            generator_config_id=generator_config_id,
            reference_id=reference_id,
            motif_id=motif_id,
            feed_epoch_id=feed_epoch_id,
            broker_profile_id=broker_profile_id,
            constraint_set_id=constraint_set_id,
            confidence=confidence,
        )

    def _validate_lineage(self) -> None:
        if self.origin is SyntheticEventOrigin.OBSERVED:
            for name in (
                "source_series_id",
                "source_period",
            ):
                if getattr(self, name) is None:
                    raise ValueError(f"observed event requires {name}")
            if self.source_row_id is None:
                raise ValueError("observed event requires source_row_id")
            row_id = _bounded_int(self.source_row_id, "source_row_id")
            if row_id < 1:
                raise ValueError("source_row_id must be positive")
            object.__setattr__(self, "source_row_id", row_id)
            populated = [
                name
                for name in _SYNTHETIC_LINEAGE_FIELDS
                if getattr(self, name) is not None
            ]
            if populated:
                raise ValueError(
                    "observed event cannot carry synthetic lineage: "
                    + ", ".join(populated)
                )
            return

        populated_source = [
            name
            for name in (
                "source_series_id",
                "source_period",
                "source_row_id",
            )
            if getattr(self, name) is not None
        ]
        if populated_source:
            raise ValueError(
                "synthetic event cannot claim observed row identity: "
                + ", ".join(populated_source)
            )
        for name in _REQUIRED_SYNTHETIC_LINEAGE_FIELDS:
            if getattr(self, name) is None:
                raise ValueError(f"synthetic event requires {name}")
        if self.left_anchor_event_id == self.right_anchor_event_id:
            raise ValueError("synthetic event requires distinct anchors")

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the canonical fields used to derive the event ID."""
        common: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "origin": self.origin.value,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "source_version_id": self.source_version_id,
        }
        if self.origin is SyntheticEventOrigin.OBSERVED:
            common.update(
                {
                    "source_series_id": self.source_series_id,
                    "source_period": self.source_period,
                    "source_row_id": self.source_row_id,
                }
            )
            return common
        common.update(
            {
                "run_id": self.run_id,
                "ensemble_member_id": self.ensemble_member_id,
                "anchor_interval_id": self.anchor_interval_id,
                "left_anchor_event_id": self.left_anchor_event_id,
                "right_anchor_event_id": self.right_anchor_event_id,
                "generator_id": self.generator_id,
                "generator_version": self.generator_version,
                "generator_config_id": self.generator_config_id,
                "reference_id": self.reference_id,
                "motif_id": self.motif_id,
                "feed_epoch_id": self.feed_epoch_id,
                "broker_profile_id": self.broker_profile_id,
                "constraint_set_id": self.constraint_set_id,
            }
        )
        return common

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the stable, flat, JSON-compatible event representation."""
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "origin": self.origin.value,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "source_version_id": self.source_version_id,
            "source_series_id": self.source_series_id,
            "source_period": self.source_period,
            "source_row_id": self.source_row_id,
            "anchor_interval_id": self.anchor_interval_id,
            "left_anchor_event_id": self.left_anchor_event_id,
            "right_anchor_event_id": self.right_anchor_event_id,
            "generator_id": self.generator_id,
            "generator_version": self.generator_version,
            "generator_config_id": self.generator_config_id,
            "reference_id": self.reference_id,
            "motif_id": self.motif_id,
            "feed_epoch_id": self.feed_epoch_id,
            "broker_profile_id": self.broker_profile_id,
            "constraint_set_id": self.constraint_set_id,
            "confidence": self.confidence,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return canonical_contract_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticEventV1":
        """Restore a version-one event and verify its deterministic ID."""
        _require_schema(data, SYNTHETIC_EVENT_SCHEMA_VERSION)
        return cls(
            origin=SyntheticEventOrigin.from_value(str(data.get("origin"))),
            symbol=str(data.get("symbol", "")),
            event_time_ns=cast(int, data.get("event_time_ns")),
            event_sequence=cast(int, data.get("event_sequence")),
            bid=cast(float, data.get("bid")),
            ask=cast(float, data.get("ask")),
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            source_version_id=str(data.get("source_version_id", "")),
            source_series_id=_mapping_optional_text(data, "source_series_id"),
            source_period=_mapping_optional_text(data, "source_period"),
            source_row_id=cast(int | None, data.get("source_row_id")),
            anchor_interval_id=_mapping_optional_text(
                data, "anchor_interval_id"
            ),
            left_anchor_event_id=_mapping_optional_text(
                data, "left_anchor_event_id"
            ),
            right_anchor_event_id=_mapping_optional_text(
                data, "right_anchor_event_id"
            ),
            generator_id=_mapping_optional_text(data, "generator_id"),
            generator_version=_mapping_optional_text(data, "generator_version"),
            generator_config_id=_mapping_optional_text(
                data, "generator_config_id"
            ),
            reference_id=_mapping_optional_text(data, "reference_id"),
            motif_id=_mapping_optional_text(data, "motif_id"),
            feed_epoch_id=_mapping_optional_text(data, "feed_epoch_id"),
            broker_profile_id=_mapping_optional_text(data, "broker_profile_id"),
            constraint_set_id=_mapping_optional_text(data, "constraint_set_id"),
            confidence=cast(float | None, data.get("confidence")),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "SyntheticEventV1":
        """Restore an event from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class SyntheticEventStreamV1:
    """One deterministically ordered symbol/member event stream."""

    run_id: str
    ensemble_member_id: str
    symbol: str
    events: tuple[SyntheticEventV1, ...]
    source_version_ids: tuple[str, ...] = ()
    stream_id: str = ""
    schema_version: str = SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION:
            raise ValueError("unsupported synthetic event stream schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _required_text(self.ensemble_member_id),
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        ordered = tuple(sorted(tuple(self.events), key=_event_order_key))
        object.__setattr__(self, "events", ordered)

        event_ids: set[str] = set()
        positions: set[tuple[int, int]] = set()
        event_sources: set[str] = set()
        for event in ordered:
            if event.run_id != self.run_id:
                raise ValueError("event run_id does not match stream")
            if event.ensemble_member_id != self.ensemble_member_id:
                raise ValueError("event ensemble member does not match stream")
            if event.symbol != self.symbol:
                raise ValueError("event symbol does not match stream")
            if event.event_id in event_ids:
                raise ValueError("duplicate event_id in stream")
            position = (event.event_time_ns, event.event_sequence)
            if position in positions:
                raise ValueError(
                    "duplicate event_time_ns/event_sequence in stream"
                )
            event_ids.add(event.event_id)
            positions.add(position)
            event_sources.add(event.source_version_id)

        sources = _normalized_id_tuple(self.source_version_ids)
        if not sources:
            sources = tuple(sorted(event_sources))
        if not sources:
            raise ValueError("stream requires at least one source version")
        if not event_sources.issubset(set(sources)):
            raise ValueError("stream source versions do not cover events")
        object.__setattr__(self, "source_version_ids", sources)

        expected = _stable_id("stream", self.identity_payload())
        supplied = _optional_text(self.stream_id)
        if supplied is not None and supplied != expected:
            raise ValueError("stream_id does not match deterministic content")
        object.__setattr__(self, "stream_id", expected)

    @classmethod
    def merge(
        cls,
        *,
        run_id: str,
        ensemble_member_id: str,
        symbol: str,
        observed_events: Iterable[SyntheticEventV1],
        synthetic_events: Iterable[SyntheticEventV1],
        source_version_ids: Iterable[str] = (),
    ) -> "SyntheticEventStreamV1":
        """Merge immutable observations and zero-or-more generated events."""
        observed = tuple(observed_events)
        generated = tuple(synthetic_events)
        if any(
            event.origin is not SyntheticEventOrigin.OBSERVED
            for event in observed
        ):
            raise ValueError("observed_events contains a synthetic event")
        if any(
            event.origin is not SyntheticEventOrigin.SYNTHETIC
            for event in generated
        ):
            raise ValueError("synthetic_events contains an observed event")
        return cls(
            run_id=run_id,
            ensemble_member_id=ensemble_member_id,
            symbol=symbol,
            events=observed + generated,
            source_version_ids=tuple(source_version_ids),
        )

    @property
    def observed_event_count(self) -> int:
        """Return the number of immutable observed events."""
        return sum(
            event.origin is SyntheticEventOrigin.OBSERVED
            for event in self.events
        )

    @property
    def synthetic_event_count(self) -> int:
        """Return the number of generated events."""
        return len(self.events) - self.observed_event_count

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return canonical stream identity fields."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "source_version_ids": list(self.source_version_ids),
            "event_ids": [event.event_id for event in self.events],
        }

    def header_dict(self) -> dict[str, JSONValue]:
        """Return bounded metadata stored in Arrow schema metadata."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "stream_id": self.stream_id,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "source_version_ids": list(self.source_version_ids),
            "event_count": len(self.events),
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the stable JSON-compatible stream representation."""
        payload = self.header_dict()
        payload["events"] = [event.to_dict() for event in self.events]
        return payload

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return canonical_contract_json(self.to_dict())

    def to_json_header(self) -> str:
        """Return deterministic bounded stream-header JSON."""
        return canonical_contract_json(self.header_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticEventStreamV1":
        """Restore a stream and verify all event/content identities."""
        _require_schema(data, SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION)
        _require_derived_schema(
            data,
            "event_schema_version",
            SYNTHETIC_EVENT_SCHEMA_VERSION,
        )
        rows = _mapping_sequence(data, "events")
        stream = cls(
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbol=str(data.get("symbol", "")),
            events=tuple(SyntheticEventV1.from_dict(row) for row in rows),
            source_version_ids=tuple(
                str(value)
                for value in _value_sequence(
                    data.get("source_version_ids"),
                    "source_version_ids",
                )
            ),
            stream_id=str(data.get("stream_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _validate_stream_counts(data, stream)
        return stream

    @classmethod
    def from_json(cls, text: str) -> "SyntheticEventStreamV1":
        """Restore a stream from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class SyntheticEnsembleMemberV1:
    """Compact manifest record for one materialized ensemble member."""

    member_id: str
    stream_id: str
    event_count: int
    observed_event_count: int
    synthetic_event_count: int
    content_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "member_id", _required_text(self.member_id))
        object.__setattr__(self, "stream_id", _required_text(self.stream_id))
        for name in (
            "event_count",
            "observed_event_count",
            "synthetic_event_count",
        ):
            value = _bounded_int(getattr(self, name), name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)
        if self.event_count != (
            self.observed_event_count + self.synthetic_event_count
        ):
            raise ValueError("ensemble member event counts do not reconcile")
        digest = _required_text(self.content_sha256)
        if not _is_sha256_id(digest):
            raise ValueError("content_sha256 must be a sha256 identifier")
        object.__setattr__(self, "content_sha256", digest)

    @classmethod
    def from_stream(
        cls, stream: SyntheticEventStreamV1
    ) -> "SyntheticEnsembleMemberV1":
        """Build compact member evidence from one stream."""
        content = canonical_contract_json(
            [event.to_dict() for event in stream.events]
        ).encode("utf-8")
        return cls(
            member_id=stream.ensemble_member_id,
            stream_id=stream.stream_id,
            event_count=len(stream.events),
            observed_event_count=stream.observed_event_count,
            synthetic_event_count=stream.synthetic_event_count,
            content_sha256="sha256:" + hashlib.sha256(content).hexdigest(),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return stable JSON-compatible member evidence."""
        return {
            "member_id": self.member_id,
            "stream_id": self.stream_id,
            "event_count": self.event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "content_sha256": self.content_sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SyntheticEnsembleMemberV1":
        """Restore compact member evidence."""
        return cls(
            member_id=str(data.get("member_id", "")),
            stream_id=str(data.get("stream_id", "")),
            event_count=cast(int, data.get("event_count")),
            observed_event_count=cast(int, data.get("observed_event_count")),
            synthetic_event_count=cast(int, data.get("synthetic_event_count")),
            content_sha256=str(data.get("content_sha256", "")),
        )


@dataclass(frozen=True, slots=True)
class SyntheticEnsembleManifestV1:
    """Deterministic manifest for a set of reconstructed member streams."""

    run_id: str
    primary_member_id: str
    members: tuple[SyntheticEnsembleMemberV1, ...]
    source_version_ids: tuple[str, ...]
    configuration_ids: tuple[str, ...]
    ensemble_id: str = ""
    schema_version: str = SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported synthetic ensemble manifest schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "primary_member_id",
            _required_text(self.primary_member_id),
        )
        members = tuple(sorted(tuple(self.members), key=lambda x: x.member_id))
        if not members:
            raise ValueError("ensemble manifest requires at least one member")
        member_ids = [member.member_id for member in members]
        stream_ids = [member.stream_id for member in members]
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("ensemble member IDs must be unique")
        if len(set(stream_ids)) != len(stream_ids):
            raise ValueError("ensemble stream IDs must be unique")
        if self.primary_member_id not in member_ids:
            raise ValueError("primary_member_id is not present in members")
        object.__setattr__(self, "members", members)
        sources = _normalized_id_tuple(self.source_version_ids)
        configs = _normalized_id_tuple(self.configuration_ids)
        if not sources:
            raise ValueError("ensemble manifest requires source versions")
        if not configs:
            raise ValueError("ensemble manifest requires configurations")
        object.__setattr__(self, "source_version_ids", sources)
        object.__setattr__(self, "configuration_ids", configs)

        expected = _stable_id("ensemble", self.identity_payload())
        supplied = _optional_text(self.ensemble_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "ensemble_id does not match deterministic manifest"
            )
        object.__setattr__(self, "ensemble_id", expected)

    @classmethod
    def from_streams(
        cls,
        streams: Iterable[SyntheticEventStreamV1],
        *,
        primary_member_id: str,
        configuration_ids: Iterable[str],
        source_version_ids: Iterable[str] = (),
    ) -> "SyntheticEnsembleManifestV1":
        """Build a compact deterministic manifest from member streams."""
        materialized = tuple(streams)
        if not materialized:
            raise ValueError("ensemble manifest requires member streams")
        run_ids = {stream.run_id for stream in materialized}
        if len(run_ids) != 1:
            raise ValueError("ensemble streams must share one run_id")
        sources = set(source_version_ids)
        configs = _normalized_id_tuple(configuration_ids)
        event_configs: set[str] = set()
        for stream in materialized:
            sources.update(stream.source_version_ids)
            event_configs.update(
                event.generator_config_id
                for event in stream.events
                if event.generator_config_id is not None
            )
        if not event_configs.issubset(set(configs)):
            raise ValueError(
                "ensemble configurations do not cover generated events"
            )
        return cls(
            run_id=materialized[0].run_id,
            primary_member_id=primary_member_id,
            members=tuple(
                SyntheticEnsembleMemberV1.from_stream(stream)
                for stream in materialized
            ),
            source_version_ids=tuple(sources),
            configuration_ids=configs,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return canonical ensemble identity fields."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "stream_schema_version": SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
            "run_id": self.run_id,
            "primary_member_id": self.primary_member_id,
            "members": [member.to_dict() for member in self.members],
            "source_version_ids": list(self.source_version_ids),
            "configuration_ids": list(self.configuration_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the stable JSON-compatible ensemble manifest."""
        payload = self.identity_payload()
        payload["ensemble_id"] = self.ensemble_id
        return payload

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return canonical_contract_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "SyntheticEnsembleManifestV1":
        """Restore and verify a version-one ensemble manifest."""
        _require_schema(data, SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION)
        _require_derived_schema(
            data,
            "event_schema_version",
            SYNTHETIC_EVENT_SCHEMA_VERSION,
        )
        _require_derived_schema(
            data,
            "stream_schema_version",
            SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
        )
        return cls(
            run_id=str(data.get("run_id", "")),
            primary_member_id=str(data.get("primary_member_id", "")),
            members=tuple(
                SyntheticEnsembleMemberV1.from_dict(row)
                for row in _mapping_sequence(data, "members")
            ),
            source_version_ids=tuple(
                str(value)
                for value in _value_sequence(
                    data.get("source_version_ids"),
                    "source_version_ids",
                )
            ),
            configuration_ids=tuple(
                str(value)
                for value in _value_sequence(
                    data.get("configuration_ids"),
                    "configuration_ids",
                )
            ),
            ensemble_id=str(data.get("ensemble_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "SyntheticEnsembleManifestV1":
        """Restore a manifest from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


def derive_anchor_interval_id(
    left_anchor_event_id: str,
    right_anchor_event_id: str,
) -> str:
    """Return a stable identity for one ordered anchor interval."""
    left = _required_text(left_anchor_event_id)
    right = _required_text(right_anchor_event_id)
    if left == right:
        raise ValueError("anchor interval requires distinct event IDs")
    return _stable_id(
        "anchor-interval",
        {
            "left_anchor_event_id": left,
            "right_anchor_event_id": right,
        },
    )


def canonical_contract_json(value: Any) -> str:
    """Return the canonical JSON encoding used for IDs and wire payloads."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def synthetic_event_arrow_schema() -> Any:
    """Return the exact flat Arrow schema for version-one event rows."""
    pa, _ = _arrow_modules()

    def required_text(name: str) -> Any:
        return pa.field(name, pa.string(), nullable=False)

    def optional_text(name: str) -> Any:
        return pa.field(name, pa.string(), nullable=True)

    return pa.schema(
        [
            required_text("schema_version"),
            required_text("event_id"),
            required_text("origin"),
            required_text("symbol"),
            pa.field("event_time_ns", pa.int64(), nullable=False),
            pa.field("event_sequence", pa.int64(), nullable=False),
            pa.field("bid", pa.float64(), nullable=False),
            pa.field("ask", pa.float64(), nullable=False),
            required_text("run_id"),
            required_text("ensemble_member_id"),
            required_text("source_version_id"),
            optional_text("source_series_id"),
            optional_text("source_period"),
            pa.field("source_row_id", pa.int64(), nullable=True),
            optional_text("anchor_interval_id"),
            optional_text("left_anchor_event_id"),
            optional_text("right_anchor_event_id"),
            optional_text("generator_id"),
            optional_text("generator_version"),
            optional_text("generator_config_id"),
            optional_text("reference_id"),
            optional_text("motif_id"),
            optional_text("feed_epoch_id"),
            optional_text("broker_profile_id"),
            optional_text("constraint_set_id"),
            pa.field("confidence", pa.float64(), nullable=True),
        ],
        metadata={
            EVENT_SCHEMA_METADATA_KEY: SYNTHETIC_EVENT_SCHEMA_VERSION.encode(),
            EVENT_TIME_UNIT_METADATA_KEY: b"UTC epoch nanoseconds",
        },
    )


def synthetic_event_stream_to_arrow(stream: SyntheticEventStreamV1) -> Any:
    """Serialize a stream to one exact-schema Arrow table."""
    pa, _ = _arrow_modules()
    schema = synthetic_event_arrow_schema()
    metadata = dict(schema.metadata or {})
    metadata[STREAM_METADATA_KEY] = stream.to_json_header().encode("utf-8")
    schema = schema.with_metadata(metadata)
    return pa.Table.from_pylist(
        [event.to_dict() for event in stream.events],
        schema=schema,
    )


def synthetic_event_stream_from_arrow(table: Any) -> SyntheticEventStreamV1:
    """Restore and validate a stream from an exact-schema Arrow table."""
    expected = synthetic_event_arrow_schema()
    actual = table.schema
    if not actual.remove_metadata().equals(expected.remove_metadata()):
        raise ValueError("Arrow event schema does not match version one")
    metadata = dict(actual.metadata or {})
    if metadata.get(EVENT_SCHEMA_METADATA_KEY) != (
        SYNTHETIC_EVENT_SCHEMA_VERSION.encode()
    ):
        raise ValueError("Arrow event schema metadata is missing or invalid")
    stream_bytes = metadata.get(STREAM_METADATA_KEY)
    if stream_bytes is None:
        raise ValueError("Arrow stream metadata is missing")
    header = _json_mapping(stream_bytes.decode("utf-8"))
    _require_schema(header, SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION)
    _require_derived_schema(
        header,
        "event_schema_version",
        SYNTHETIC_EVENT_SCHEMA_VERSION,
    )
    events = tuple(SyntheticEventV1.from_dict(row) for row in table.to_pylist())
    stream = SyntheticEventStreamV1(
        run_id=str(header.get("run_id", "")),
        ensemble_member_id=str(header.get("ensemble_member_id", "")),
        symbol=str(header.get("symbol", "")),
        events=events,
        source_version_ids=tuple(
            str(value)
            for value in _value_sequence(
                header.get("source_version_ids"),
                "source_version_ids",
            )
        ),
        stream_id=str(header.get("stream_id", "")),
        schema_version=str(header.get("schema_version", "")),
    )
    _validate_stream_counts(header, stream, context="Arrow stream")
    return stream


def synthetic_event_stream_to_parquet_bytes(
    stream: SyntheticEventStreamV1,
) -> bytes:
    """Return deterministic Parquet bytes for a stream under one runtime."""
    pa, pq = _arrow_modules()
    sink = pa.BufferOutputStream()
    _write_parquet_table(
        pq,
        synthetic_event_stream_to_arrow(stream),
        sink,
    )
    return bytes(sink.getvalue())


def synthetic_event_stream_from_parquet_bytes(
    payload: bytes | bytearray | memoryview,
) -> SyntheticEventStreamV1:
    """Restore a stream from Parquet bytes."""
    pa, pq = _arrow_modules()
    table = pq.read_table(pa.BufferReader(bytes(payload)))
    return synthetic_event_stream_from_arrow(table)


def write_synthetic_event_stream_parquet(
    stream: SyntheticEventStreamV1,
    path: str | Path,
) -> Path:
    """Write one non-atomic Parquet file; #446 owns atomic publication."""
    _, pq = _arrow_modules()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_parquet_table(
        pq,
        synthetic_event_stream_to_arrow(stream),
        target,
    )
    return target


def read_synthetic_event_stream_parquet(
    path: str | Path,
) -> SyntheticEventStreamV1:
    """Read and validate one version-one stream Parquet file."""
    _, pq = _arrow_modules()
    return synthetic_event_stream_from_arrow(pq.read_table(Path(path)))


def _write_parquet_table(pq: Any, table: Any, destination: Any) -> None:
    pq.write_table(
        table,
        destination,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        row_group_size=max(1, table.num_rows),
    )


def _arrow_modules() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except (ImportError, ModuleNotFoundError) as err:
        raise ModuleNotFoundError(
            "synthetic Arrow/Parquet contracts require histdatacom[arrow]"
        ) from err
    return pa, pq


def _event_order_key(event: SyntheticEventV1) -> tuple[int, int, str]:
    return (event.event_time_ns, event.event_sequence, event.event_id)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError("required text value is empty")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalized_symbol(value: Any) -> str:
    return _required_text(value).lower()


def _bounded_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not INT64_MIN <= value <= INT64_MAX:
        raise ValueError(f"{name} is outside signed 64-bit range")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    return _optional_text(data.get(name))


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError(f"unsupported schema version; expected {expected}")


def _require_derived_schema(
    data: Mapping[str, Any],
    name: str,
    expected: str,
) -> None:
    if str(data.get(name, "")) != expected:
        raise ValueError(f"{name} does not match {expected}")


def _validate_stream_counts(
    data: Mapping[str, Any],
    stream: SyntheticEventStreamV1,
    *,
    context: str = "stream",
) -> None:
    expected = (
        ("event_count", len(stream.events)),
        ("observed_event_count", stream.observed_event_count),
        ("synthetic_event_count", stream.synthetic_event_count),
    )
    for name, actual in expected:
        declared = _bounded_int(data.get(name), name)
        if actual != declared:
            label = name.replace("_", " ")
            raise ValueError(f"{context} {label} does not match metadata")


def _json_mapping(text: str) -> Mapping[str, Any]:
    loaded = json.loads(text)
    if not isinstance(loaded, Mapping):
        raise ValueError("contract JSON must contain an object")
    return cast(Mapping[str, Any], loaded)


def _mapping_sequence(
    data: Mapping[str, Any], name: str
) -> tuple[Mapping[str, Any], ...]:
    values = _value_sequence(data.get(name), name)
    if not all(isinstance(value, Mapping) for value in values):
        raise ValueError(f"{name} must contain objects")
    return tuple(cast(Mapping[str, Any], value) for value in values)


def _value_sequence(value: Any, name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise ValueError(f"{name} must be a sequence")
    return tuple(value)


def _normalized_id_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _is_sha256_id(value: str) -> bool:
    prefix = "sha256:"
    if not value.startswith(prefix):
        return False
    digest = value.removeprefix(prefix)
    return len(digest) == 64 and all(
        character in "0123456789abcdef" for character in digest
    )


_OPTIONAL_EVENT_TEXT_FIELDS = (
    "source_series_id",
    "source_period",
    "anchor_interval_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
    "generator_id",
    "generator_version",
    "generator_config_id",
    "reference_id",
    "motif_id",
    "feed_epoch_id",
    "broker_profile_id",
    "constraint_set_id",
)

_SYNTHETIC_LINEAGE_FIELDS = (
    "anchor_interval_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
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

_REQUIRED_SYNTHETIC_LINEAGE_FIELDS = (
    "anchor_interval_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
    "generator_id",
    "generator_version",
    "generator_config_id",
    "constraint_set_id",
)


__all__ = [
    "SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION",
    "SYNTHETIC_EVENT_ARROW_COLUMNS",
    "SYNTHETIC_EVENT_SCHEMA_VERSION",
    "SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION",
    "SyntheticEnsembleManifestV1",
    "SyntheticEnsembleMemberV1",
    "SyntheticEventOrigin",
    "SyntheticEventStreamV1",
    "SyntheticEventV1",
    "canonical_contract_json",
    "derive_anchor_interval_id",
    "read_synthetic_event_stream_parquet",
    "synthetic_event_arrow_schema",
    "synthetic_event_stream_from_arrow",
    "synthetic_event_stream_from_parquet_bytes",
    "synthetic_event_stream_to_arrow",
    "synthetic_event_stream_to_parquet_bytes",
    "write_synthetic_event_stream_parquet",
]
