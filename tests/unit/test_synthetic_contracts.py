"""Tests for versioned variable-cardinality synthetic event contracts."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

from hypothesis import given, settings
from hypothesis import strategies as st
import pyarrow as pa
import pytest

from histdatacom.synthetic import (
    SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION,
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
    SyntheticEnsembleManifestV1,
    SyntheticEnsembleMemberV1,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    derive_anchor_interval_id,
    read_synthetic_event_stream_parquet,
    synthetic_event_arrow_schema,
    synthetic_event_stream_from_arrow,
    synthetic_event_stream_from_parquet_bytes,
    synthetic_event_stream_to_arrow,
    synthetic_event_stream_to_parquet_bytes,
    write_synthetic_event_stream_parquet,
)
from histdatacom.synthetic.contracts import SYNTHETIC_EVENT_ARROW_COLUMNS

BASE_TIME_NS = 1_700_000_000_000_000_000
RUN_ID = "run-contract-v1"
MEMBER_ID = "member-000"
SYMBOL = "eurusd"
OBSERVED_SOURCE = "source-artifact:sha256:observed-v1"
GENERATED_SOURCE = "source-manifest:sha256:inputs-v1"


def _observed(
    row_id: int,
    *,
    event_time_ns: int | None = None,
    event_sequence: int = 0,
    member_id: str = MEMBER_ID,
    bid: float | None = None,
) -> SyntheticEventV1:
    selected_bid = bid if bid is not None else 1.08 + row_id / 100_000
    return SyntheticEventV1.observed(
        symbol=SYMBOL,
        event_time_ns=(
            event_time_ns
            if event_time_ns is not None
            else BASE_TIME_NS + row_id * 1_000
        ),
        event_sequence=event_sequence,
        bid=selected_bid,
        ask=selected_bid + 0.0001,
        run_id=RUN_ID,
        ensemble_member_id=member_id,
        source_version_id=OBSERVED_SOURCE,
        source_series_id="ascii:T:eurusd",
        source_period="202401",
        source_row_id=row_id,
    )


def _generated(
    left: SyntheticEventV1,
    right: SyntheticEventV1,
    *,
    ordinal: int = 1,
    member_id: str = MEMBER_ID,
) -> SyntheticEventV1:
    return SyntheticEventV1.generated(
        symbol=SYMBOL,
        event_time_ns=left.event_time_ns + ordinal,
        event_sequence=0,
        bid=1.08005 + ordinal / 1_000_000_000,
        ask=1.08015 + ordinal / 1_000_000_000,
        run_id=RUN_ID,
        ensemble_member_id=member_id,
        source_version_id=GENERATED_SOURCE,
        left_anchor_event_id=left.event_id,
        right_anchor_event_id=right.event_id,
        generator_id="empirical-motif",
        generator_version="1.0.0",
        generator_config_id="config:sha256:motif-v1",
        constraint_set_id="constraint:sha256:historical-v1",
        confidence=0.8,
        reference_id="reference:modern-2026",
        motif_id="motif:quiet-london-001",
        feed_epoch_id="feed-epoch:modern",
        broker_profile_id="broker-profile:demo-v1",
    )


def _stream(
    *,
    member_id: str = MEMBER_ID,
    generated_count: int = 1,
) -> SyntheticEventStreamV1:
    left = _observed(1, member_id=member_id)
    right = _observed(
        2,
        event_time_ns=left.event_time_ns + 1_000,
        member_id=member_id,
    )
    generated = tuple(
        _generated(left, right, ordinal=ordinal, member_id=member_id)
        for ordinal in range(1, generated_count + 1)
    )
    return SyntheticEventStreamV1.merge(
        run_id=RUN_ID,
        ensemble_member_id=member_id,
        symbol=SYMBOL,
        observed_events=(right, left),
        synthetic_events=reversed(generated),
    )


def test_event_python_and_json_round_trip_verifies_identity() -> None:
    left = _observed(1)
    right = _observed(2)
    generated = _generated(left, right)

    for event in (left, generated):
        assert SyntheticEventV1.from_dict(event.to_dict()) == event
        assert SyntheticEventV1.from_json(event.to_json()) == event
        assert event.event_id.startswith("event:sha256:")
        assert len(event.to_dict()) == len(SYNTHETIC_EVENT_ARROW_COLUMNS)

    assert left.origin is SyntheticEventOrigin.OBSERVED
    assert generated.origin is SyntheticEventOrigin.SYNTHETIC
    assert generated.anchor_interval_id == derive_anchor_interval_id(
        left.event_id,
        right.event_id,
    )


def test_event_reader_accepts_unknown_json_but_rejects_schema_or_id_drift() -> (
    None
):
    payload = _observed(1).to_dict()
    payload["future_envelope_key"] = "ignored"
    assert SyntheticEventV1.from_dict(payload) == _observed(1)

    wrong_schema = dict(payload)
    wrong_schema["schema_version"] = "histdatacom.synthetic-event.v2"
    with pytest.raises(ValueError, match="unsupported schema version"):
        SyntheticEventV1.from_dict(wrong_schema)

    wrong_id = dict(payload)
    wrong_id["event_id"] = "event:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="event_id does not match"):
        SyntheticEventV1.from_dict(wrong_id)


@given(
    field=st.sampled_from(
        (
            "anchor_interval_id",
            "left_anchor_event_id",
            "right_anchor_event_id",
            "generator_id",
            "generator_version",
            "generator_config_id",
            "constraint_set_id",
        )
    ),
    invalid_value=st.sampled_from((None, "", "   ")),
)
@settings(max_examples=30, deadline=None)
def test_synthetic_event_rejects_missing_reproducibility_lineage(
    field: str,
    invalid_value: str | None,
) -> None:
    event = _generated(_observed(1), _observed(2))
    payload = event.to_dict()
    payload[field] = invalid_value
    payload["event_id"] = ""

    with pytest.raises(ValueError, match=f"requires {field}"):
        SyntheticEventV1.from_dict(payload)


def test_origin_identity_cannot_be_misrepresented() -> None:
    observed = _observed(1)
    generated = _generated(observed, _observed(2))

    with pytest.raises(ValueError, match="synthetic lineage"):
        replace(observed, generator_id="not-observed", event_id="")
    with pytest.raises(ValueError, match="observed row identity"):
        replace(
            generated,
            source_series_id="fake-series",
            source_period="202401",
            source_row_id=99,
            event_id="",
        )


def test_synthetic_confidence_is_optional_but_bounded_when_present() -> None:
    event = _generated(_observed(1), _observed(2))
    without_confidence = replace(event, confidence=None, event_id="")

    assert without_confidence.confidence is None
    assert SyntheticEventV1.from_dict(without_confidence.to_dict()) == (
        without_confidence
    )
    with pytest.raises(ValueError, match="between zero and one"):
        replace(event, confidence=1.1, event_id="")


@given(
    row_id=st.integers(min_value=1, max_value=1_000_000),
    event_time_ns=st.integers(
        min_value=BASE_TIME_NS,
        max_value=BASE_TIME_NS + 1_000_000_000,
    ),
    event_sequence=st.integers(min_value=0, max_value=100_000),
    price_units=st.integers(min_value=1, max_value=10_000_000),
    spread_units=st.integers(min_value=0, max_value=10_000),
)
@settings(max_examples=50, deadline=None)
def test_observed_identity_and_values_round_trip_as_a_property(
    row_id: int,
    event_time_ns: int,
    event_sequence: int,
    price_units: int,
    spread_units: int,
) -> None:
    bid = price_units / 1_000_000
    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=event_time_ns,
        event_sequence=event_sequence,
        bid=bid,
        ask=bid + spread_units / 1_000_000,
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        source_version_id=OBSERVED_SOURCE,
        source_series_id="series-property",
        source_period="202401",
        source_row_id=row_id,
    )

    restored = SyntheticEventV1.from_json(event.to_json())
    assert restored == event
    assert restored.event_id == event.event_id
    assert restored.source_row_id == row_id
    assert restored.bid == bid


@given(order=st.permutations((0, 1, 2, 3, 4)))
@settings(max_examples=30, deadline=None)
def test_duplicate_timestamp_ordering_is_deterministic_property(
    order: list[int],
) -> None:
    events = tuple(
        _observed(
            sequence + 1,
            event_time_ns=BASE_TIME_NS,
            event_sequence=sequence,
        )
        for sequence in order
    )
    stream = SyntheticEventStreamV1(
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        symbol=SYMBOL,
        events=events,
    )

    assert [event.event_sequence for event in stream.events] == list(range(5))


def test_stream_rejects_ambiguous_duplicate_position() -> None:
    first = _observed(1, event_time_ns=BASE_TIME_NS, event_sequence=0)
    second = _observed(2, event_time_ns=BASE_TIME_NS, event_sequence=0)

    with pytest.raises(ValueError, match="duplicate event_time_ns"):
        SyntheticEventStreamV1(
            run_id=RUN_ID,
            ensemble_member_id=MEMBER_ID,
            symbol=SYMBOL,
            events=(first, second),
        )


@pytest.mark.parametrize("generated_count", [0, 1, 4])
def test_stream_permits_variable_cardinality_and_preserves_observations(
    generated_count: int,
) -> None:
    stream = _stream(generated_count=generated_count)
    observed = tuple(
        event
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )

    assert stream.observed_event_count == 2
    assert stream.synthetic_event_count == generated_count
    assert observed == (_observed(1), _observed(2))
    assert SyntheticEventStreamV1.from_json(stream.to_json()) == stream


def test_generated_event_id_is_partition_and_retry_independent() -> None:
    left = _observed(1)
    right = _observed(2, event_time_ns=left.event_time_ns + 1_000)
    first = _generated(left, right)
    retry = _generated(left, right)
    earlier = _observed(3, event_time_ns=left.event_time_ns - 1_000)
    later = _observed(4, event_time_ns=right.event_time_ns + 1_000)

    narrow = SyntheticEventStreamV1(
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        symbol=SYMBOL,
        events=(left, first, right),
    )
    wider = SyntheticEventStreamV1(
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        symbol=SYMBOL,
        events=(earlier, left, retry, right, later),
    )

    assert first.event_id == retry.event_id
    assert narrow.stream_id != wider.stream_id
    assert first.event_id in {event.event_id for event in wider.events}


@given(
    ordinal=st.integers(min_value=1, max_value=999),
    surrounding_count=st.integers(min_value=0, max_value=20),
)
@settings(max_examples=50, deadline=None)
def test_generated_event_id_stability_is_a_partition_context_property(
    ordinal: int,
    surrounding_count: int,
) -> None:
    left = _observed(1)
    right = _observed(2, event_time_ns=left.event_time_ns + 1_000)
    generated = _generated(left, right, ordinal=ordinal)
    retry = _generated(left, right, ordinal=ordinal)
    surrounding = tuple(
        _observed(
            row_id + 10,
            event_time_ns=right.event_time_ns + (row_id + 1) * 1_000,
        )
        for row_id in range(surrounding_count)
    )

    narrow = SyntheticEventStreamV1(
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        symbol=SYMBOL,
        events=(left, generated, right),
    )
    wider = SyntheticEventStreamV1(
        run_id=RUN_ID,
        ensemble_member_id=MEMBER_ID,
        symbol=SYMBOL,
        events=(left, retry, right, *surrounding),
    )

    assert generated.event_id == retry.event_id
    assert narrow.stream_id != wider.stream_id or not surrounding
    assert generated.event_id in {event.event_id for event in wider.events}


def test_arrow_and_parquet_round_trip_without_loss(tmp_path: Path) -> None:
    stream = _stream(generated_count=3)
    table = synthetic_event_stream_to_arrow(stream)

    assert synthetic_event_stream_from_arrow(table) == stream
    assert table.schema.names == list(SYNTHETIC_EVENT_ARROW_COLUMNS)
    assert table.schema.field("event_time_ns").type == pa.int64()
    assert table.schema.field("bid").type == pa.float64()
    assert not {
        name
        for name in table.schema.names
        if name.startswith(("dq_", "cm_", "synth_"))
    }

    first_bytes = synthetic_event_stream_to_parquet_bytes(stream)
    second_bytes = synthetic_event_stream_to_parquet_bytes(stream)
    assert first_bytes == second_bytes
    assert synthetic_event_stream_from_parquet_bytes(first_bytes) == stream

    output = write_synthetic_event_stream_parquet(
        stream,
        tmp_path / "nested" / "events.parquet",
    )
    assert read_synthetic_event_stream_parquet(output) == stream


def test_arrow_schema_drift_and_metadata_tampering_fail_closed() -> None:
    stream = _stream()
    table = synthetic_event_stream_to_arrow(stream)
    drifted = table.drop(["confidence"])
    with pytest.raises(ValueError, match="schema does not match"):
        synthetic_event_stream_from_arrow(drifted)

    metadata = dict(table.schema.metadata or {})
    header = json.loads(
        metadata[b"histdatacom.synthetic_event_stream"].decode("utf-8")
    )
    header["event_count"] += 1
    metadata[b"histdatacom.synthetic_event_stream"] = json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    tampered = table.replace_schema_metadata(metadata)
    with pytest.raises(ValueError, match="event count"):
        synthetic_event_stream_from_arrow(tampered)


def test_stream_json_derived_schema_and_counts_fail_closed() -> None:
    payload = _stream().to_dict()
    payload["event_count"] = 99
    with pytest.raises(ValueError, match="event count"):
        SyntheticEventStreamV1.from_dict(payload)

    payload = _stream().to_dict()
    payload["event_schema_version"] = "histdatacom.synthetic-event.v2"
    with pytest.raises(ValueError, match="event_schema_version"):
        SyntheticEventStreamV1.from_dict(payload)


def test_ensemble_manifest_is_compact_deterministic_and_reconciling() -> None:
    primary = _stream(member_id="member-000", generated_count=1)
    alternate = _stream(member_id="member-001", generated_count=3)
    manifest = SyntheticEnsembleManifestV1.from_streams(
        (alternate, primary),
        primary_member_id="member-000",
        configuration_ids=("config:sha256:motif-v1",),
    )

    assert manifest.schema_version == (
        SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION
    )
    assert [member.member_id for member in manifest.members] == [
        "member-000",
        "member-001",
    ]
    assert manifest.members[0].event_count == 3
    assert manifest.members[1].event_count == 5
    assert "events" not in manifest.to_dict()
    assert SyntheticEnsembleManifestV1.from_json(manifest.to_json()) == (
        manifest
    )

    duplicate = SyntheticEnsembleMemberV1(
        member_id="member-duplicate",
        stream_id=manifest.members[0].stream_id,
        event_count=manifest.members[0].event_count,
        observed_event_count=manifest.members[0].observed_event_count,
        synthetic_event_count=manifest.members[0].synthetic_event_count,
        content_sha256=manifest.members[0].content_sha256,
    )
    with pytest.raises(ValueError, match="stream IDs must be unique"):
        SyntheticEnsembleManifestV1(
            run_id=RUN_ID,
            primary_member_id="member-000",
            members=(manifest.members[0], duplicate),
            source_version_ids=(OBSERVED_SOURCE,),
            configuration_ids=("config:sha256:motif-v1",),
        )

    with pytest.raises(ValueError, match="do not cover generated events"):
        SyntheticEnsembleManifestV1.from_streams(
            (primary,),
            primary_member_id="member-000",
            configuration_ids=("config:other",),
        )

    payload = manifest.to_dict()
    payload["stream_schema_version"] = "histdatacom.synthetic-event-stream.v2"
    with pytest.raises(ValueError, match="stream_schema_version"):
        SyntheticEnsembleManifestV1.from_dict(payload)


def test_contract_schema_versions_are_explicit_and_stable() -> None:
    assert SYNTHETIC_EVENT_SCHEMA_VERSION.endswith(".v1")
    assert SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION.endswith(".v1")
    assert SYNTHETIC_ENSEMBLE_MANIFEST_SCHEMA_VERSION.endswith(".v1")
    assert synthetic_event_arrow_schema().names == list(
        SYNTHETIC_EVENT_ARROW_COLUMNS
    )


def test_importing_synthetic_contracts_does_not_import_optional_arrow() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import histdatacom.synthetic; "
                "assert 'pyarrow' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_example_stream_artifact_is_valid_stable_and_narrow() -> None:
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "synthetic_event_stream_v1.json"
    )
    text = fixture.read_text(encoding="utf-8")
    stream = SyntheticEventStreamV1.from_dict(json.loads(text))

    assert stream.observed_event_count == 2
    assert stream.synthetic_event_count == 1
    assert json.dumps(stream.to_dict(), indent=2, sort_keys=True) + "\n" == text
    assert not any(
        name.startswith(("dq_", "cm_", "synth_"))
        for name in stream.events[0].to_dict()
    )
