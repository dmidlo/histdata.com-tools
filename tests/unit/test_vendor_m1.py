"""Invented CSV evidence only; no provider acquisition or corpus qualification."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path

import pytest

from histdatacom.data_quality import vendor_m1 as native
from histdatacom.data_quality.vendor_m1 import (
    VendorM1DifferenceV1,
    VendorM1MinuteV1,
    VendorM1PolicyV1,
    VendorM1ReferenceV1,
    VendorM1ReportV1,
    compare_histdata_m1_to_ticks,
    load_histdata_m1_reference,
    replay_histdata_m1_comparison,
)
from histdatacom.data_quality.vendor_m1_contracts import (
    CLOCK_POLICY,
    MAX_SOURCE_BYTES,
    MINUTE_NS,
    PRICE_POLICY,
    VOLUME_STATUS,
    canonical,
    decimal_value,
    reserve_report,
)

START = 1_704_085_200_000_000_000  # 2024-01-01 05:00:00 UTC.
M1 = "20240101 000000;1.0000;1.2000;1.0000;1.2000;0\n"
TICKS = (
    "20240101 000000000,1.0000,1.0100,0\n20240101 000059999,1.2000,1.2100,0\n"
)


def _file(root: Path, name: str, text: str) -> tuple[Path, str]:
    path = root / name
    path.write_bytes(text.encode("ascii"))
    return path, hashlib.sha256(text.encode("ascii")).hexdigest()


def _inputs(
    tmp_path: Path,
    *,
    m1: str = M1,
    ticks: str = TICKS,
    policy: VendorM1PolicyV1 | None = None,
    available_at_ns: int | None = None,
):
    vendor, vendor_hash = _file(tmp_path, "vendor.csv", m1)
    tick, tick_hash = _file(tmp_path, "ticks.csv", ticks)
    reference = load_histdata_m1_reference(
        vendor,
        expected_sha256=vendor_hash,
        symbol="EURUSD",
        source_period="202401",
        available_at_ns=available_at_ns,
    )
    selected = policy or VendorM1PolicyV1(
        "EURUSD", "202401", START, START + MINUTE_NS, "0.0001"
    )
    return vendor, vendor_hash, tick, tick_hash, reference, selected


def _report(inputs):
    _, _, tick, tick_hash, reference, policy = inputs
    return compare_histdata_m1_to_ticks(
        reference, tick, expected_tick_sha256=tick_hash, policy=policy
    )


def _replay(report, inputs, **overrides):
    vendor, vendor_hash, tick, tick_hash, _, _ = inputs
    kwargs = {
        "expected_report_id": report.report_id,
        "expected_reference_sha256": vendor_hash,
        "expected_tick_sha256": tick_hash,
        **overrides,
    }
    return replay_histdata_m1_comparison(report, vendor, tick, **kwargs)


def test_actual_reader_native_bars_and_exact_replay_preserve_inputs(tmp_path):
    inputs = _inputs(tmp_path)
    originals = tuple(path.read_bytes() for path in (inputs[0], inputs[2]))
    report = _report(inputs)
    assert report.status_counts == {
        "exact_match": 1,
        "bounded_rounding_match": 0,
        "material_mismatch": 0,
        "missing_m1": 0,
        "missing_tick_support": 0,
        "partial_query_refused": 0,
    }
    minute = report.minutes[0]
    assert minute.tick_count == 2
    assert (
        minute.reference_ohlc
        == minute.tick_ohlc
        == ("1.0000", "1.2000", "1.0000", "1.2000")
    )
    assert minute.native_bid_ohlc == ("1.0", "1.2", "1.0", "1.2")
    assert minute.native_bar_id.startswith("derived-bar:sha256:")
    assert (
        tuple(x.signed_difference for x in minute.differences) == ("0/1",) * 4
    )
    assert report.availability_decision == "historical_ex_post_only"
    assert report.to_dict()["volume_status"] == VOLUME_STATUS
    assert report.to_dict()["clock_policy"] == CLOCK_POLICY
    assert report.to_dict()["price_policy"] == PRICE_POLICY
    assert report.to_dict()["native_tick_parser"] == "actual_parse_ascii_lines"
    assert _replay(report, inputs) == report
    assert (
        tuple(path.read_bytes() for path in (inputs[0], inputs[2])) == originals
    )


def test_five_required_outcomes_and_exact_pip_differences(tmp_path):
    m1 = (
        M1
        + "20240101 000100;1.00001;1.00001;1.00001;1.00001;7\n20240101 000200;1.1;1.1;1.1;1.1;0\n20240101 000300;1;1;1;1;0\n"
    )
    ticks = (
        TICKS
        + "20240101 000100000,1,1.01,0\n20240101 000200000,1,1.01,0\n20240101 000400000,1,1.01,9\n"
    )
    policy = VendorM1PolicyV1(
        "EURUSD", "202401", START, START + 5 * MINUTE_NS, "0.0001", "0.1"
    )
    report = _report(_inputs(tmp_path, m1=m1, ticks=ticks, policy=policy))
    assert tuple(x.status for x in report.minutes) == (
        "exact_match",
        "bounded_rounding_match",
        "material_mismatch",
        "missing_tick_support",
        "missing_m1",
    )
    assert sum(report.status_counts.values()) == 5
    delta = report.minutes[1].differences[0]
    assert delta.signed_difference == "-1/100000"
    assert delta.absolute_difference == "1/100000"
    assert delta.pip_difference == "-1/10"
    assert delta.to_dict()["difference_convention"] == "tick_minus_vendor_v1"
    assert report.minutes[1].raw_volume == 7
    assert report.minutes[3].native_bar_id is None
    assert report.minutes[4].reference_ohlc is None
    assert all(x.differences == () for x in report.minutes[3:])


def test_native_float_is_not_decimal_equality_authority(tmp_path):
    m1 = "20240101 000000;10000000000000001;10000000000000001;10000000000000001;10000000000000001;0\n"
    ticks = "20240101 000000000,10000000000000000,10000000000000002,0\n"
    report = _report(_inputs(tmp_path, m1=m1, ticks=ticks))
    assert float(report.minutes[0].reference_ohlc[0]) == float(
        report.minutes[0].tick_ohlc[0]
    )
    assert report.minutes[0].status == "material_mismatch"
    assert report.minutes[0].differences[0].signed_difference == "-1/1"


@pytest.mark.parametrize("which", ("start", "end", "both"))
def test_partial_query_never_claims_price_agreement(tmp_path, which):
    policy = VendorM1PolicyV1(
        "EURUSD",
        "202401",
        START + (1 if which != "end" else 0),
        START + MINUTE_NS - (1 if which != "start" else 0),
        "0.0001",
    )
    report = _report(_inputs(tmp_path, policy=policy))
    assert report.minutes[0].status == "partial_query_refused"
    assert report.minutes[0].differences == ()


def test_half_open_boundary_never_belongs_to_prior_minute(tmp_path):
    report = _report(
        _inputs(tmp_path, ticks=TICKS + "20240101 000100000,9,9.1,0\n")
    )
    assert report.tick_row_count == 3
    assert report.selected_tick_count == 2
    assert report.minutes[0].tick_ohlc[-1] == "1.2000"


def test_same_timestamp_ticks_keep_original_row_ordinals(tmp_path):
    m1 = "20240101 000000;1.2;1.2;1;1;0\n"
    ticks = "20240101 000010000,1.2,1.3,0\n20240101 000010000,1,1.1,0\n"
    report = _report(_inputs(tmp_path, m1=m1, ticks=ticks))
    assert report.minutes[0].status == "exact_match"
    assert report.minutes[0].tick_count == 2
    assert (
        report.minutes[0].ordered_tick_rows_sha256
        == hashlib.sha256(b"vendor-m1-ordered-tick-rows-v1\n1\n2\n").hexdigest()
    )


def test_regressions_require_explicit_bounded_policy_and_remain_reported(
    tmp_path,
):
    ticks = "20240101 000050000,1.2,1.3,0\n20240101 000010000,1,1.1,0\n"
    inputs = _inputs(tmp_path, ticks=ticks)
    with pytest.raises(ValueError, match="regression"):
        _report(inputs)
    policy = replace(
        inputs[-1],
        tick_order="bounded_regressions_event_time_row",
        artifact_id="",
    )
    report = _report((*inputs[:-1], policy))
    assert report.tick_regression_count == 1
    assert report.maximum_tick_regression_ms == 40_000
    assert report.minutes[0].status == "exact_match"
    assert (
        report.minutes[0].ordered_tick_rows_sha256
        == hashlib.sha256(b"vendor-m1-ordered-tick-rows-v1\n2\n1\n").hexdigest()
    )


def test_excess_regression_refused_before_native_bar_work(
    tmp_path, monkeypatch
):
    policy = VendorM1PolicyV1(
        "EURUSD",
        "202401",
        START,
        START + 3 * MINUTE_NS,
        "0.0001",
        tick_order="bounded_regressions_event_time_row",
    )
    inputs = _inputs(
        tmp_path,
        ticks="20240101 020000000,1,1.1,0\n20240101 005959000,1,1.1,0\n",
        policy=policy,
    )
    monkeypatch.setattr(
        native,
        "_native_bars",
        lambda *a, **k: pytest.fail("native work reached"),
    )
    with pytest.raises(ValueError, match="regression"):
        _report(inputs)


@pytest.mark.parametrize(
    "text",
    (
        "20240101 000000;1;1;1;1\n",
        "20240101 000001;1;1;1;1;0\n",
        "20240101 000000;1;0.9;1;1;0\n",
        "20240101 000000;NaN;1;1;1;0\n",
        "20240101 000000;1e0;1;1;1;0\n",
        "20240101 000000; 1;1;1;1;0\n",
        "20240101 000000;1;1;1;1;0.0\n",
        "20240101 000000;1;1;1;1;9223372036854775808\n",
        "20240101 000000;1;1;1;1;0\n\n",
        "20240101 000000;1;1;1;1;0\r",
        "20240201 000000;1;1;1;1;0\n",
        "20240132 000000;1;1;1;1;0\n",
        "20240101 000000;1;1;1;1;0\n20240101 000000;1;1;1;1;0\n",
        "20240101 000100;1;1;1;1;0\n20240101 000000;1;1;1;1;0\n",
    ),
)
def test_malformed_or_ambiguous_reference_refused(tmp_path, text):
    path, digest = _file(tmp_path, "m1.csv", text)
    with pytest.raises(ValueError):
        load_histdata_m1_reference(
            path,
            expected_sha256=digest,
            symbol="EURUSD",
            source_period="202401",
        )


@pytest.mark.parametrize(
    "ticks",
    (
        "20240101 000000000,2,1,0\n",
        "20240101 000000000,NaN,1,0\n",
        "20240101 000000000,1,1.1\n",
        "20240201 000000000,1,1.1,0\n",
        "20240101 000000,1,1.1,0\n",
        "20240101 000000000,1,1.1,0\n\n",
    ),
)
def test_raw_tick_ineligibility_is_not_repaired(tmp_path, ticks):
    with pytest.raises(ValueError):
        _report(_inputs(tmp_path, ticks=ticks))


@pytest.mark.parametrize(
    "m1,ticks,status",
    (("", TICKS, "missing_m1"), (M1, "", "missing_tick_support")),
)
def test_empty_input_is_missing_support_not_exact_match(
    tmp_path, m1, ticks, status
):
    report = _report(_inputs(tmp_path, m1=m1, ticks=ticks))
    assert report.minutes[0].status == status
    assert report.minutes[0].differences == ()
    assert report.status_counts["exact_match"] == 0


def test_both_empty_is_zero_denominator_not_dense_fabrication(tmp_path):
    report = _report(_inputs(tmp_path, m1="", ticks=""))
    assert report.minutes == ()
    assert sum(report.status_counts.values()) == 0
    assert (
        report.to_dict()["native_tick_parser"] == "empty_source_no_native_batch"
    )


@pytest.mark.parametrize(
    "month,local,utc",
    (
        ("202403", "20240310 020000", (2024, 3, 10, 7, 0)),
        ("202411", "20241103 010000", (2024, 11, 3, 6, 0)),
        ("202401", "20240131 235900", (2024, 2, 1, 4, 59)),
        ("202402", "20240229 235900", (2024, 3, 1, 4, 59)),
    ),
)
def test_fixed_est_ignores_dst_and_retains_source_month_spill(
    tmp_path, month, local, utc
):
    stamp = int(datetime(*utc, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    m1, m1_sha = _file(tmp_path, "m1.csv", f"{local};1;1;1;1;0\n")
    ticks, tick_sha = _file(tmp_path, "ticks.csv", f"{local}000,1,1.01,0\n")
    reference = load_histdata_m1_reference(
        m1, expected_sha256=m1_sha, symbol="EURUSD", source_period=month
    )
    report = compare_histdata_m1_to_ticks(
        reference,
        ticks,
        expected_tick_sha256=tick_sha,
        policy=VendorM1PolicyV1(
            "EURUSD", month, stamp, stamp + MINUTE_NS, "0.0001"
        ),
    )
    assert report.minutes[0].start_ns == stamp
    assert report.minutes[0].status == "exact_match"


@pytest.mark.parametrize(
    "reference_time,tick_time,asof,decision",
    (
        (None, None, None, "historical_ex_post_only"),
        (None, START + MINUTE_NS, START + MINUTE_NS, "availability_unknown"),
        (
            START + MINUTE_NS,
            START + MINUTE_NS,
            START + MINUTE_NS,
            "declared_available_as_of_not_independently_attested",
        ),
        (
            START + 2 * MINUTE_NS,
            START + MINUTE_NS,
            START + MINUTE_NS,
            "declared_unavailable_as_of",
        ),
        (
            START + MINUTE_NS,
            START + MINUTE_NS,
            START,
            "declared_unavailable_as_of",
        ),
    ),
)
def test_availability_decision_is_explicit_not_inferred_release(
    tmp_path, reference_time, tick_time, asof, decision
):
    policy = VendorM1PolicyV1(
        "EURUSD",
        "202401",
        START,
        START + MINUTE_NS,
        "0.0001",
        as_of_ns=asof,
        tick_available_at_ns=tick_time,
    )
    report = _report(
        _inputs(tmp_path, policy=policy, available_at_ns=reference_time)
    )
    assert report.availability_decision == decision
    assert report.to_dict()["availability_decision"] == decision
    assert report.as_of_eligible is (
        decision == "declared_available_as_of_not_independently_attested"
    )
    assert report.to_dict()["as_of_eligible"] is report.as_of_eligible


@pytest.mark.parametrize("which", ("m1", "ticks"))
def test_availability_cannot_precede_late_rows_outside_query(
    tmp_path, monkeypatch, which
):
    m1 = M1 + ("20240101 000200;1;1;1;1;0\n" if which == "m1" else "")
    ticks = TICKS + ("20240101 000200000,1,1.1,0\n" if which == "ticks" else "")
    policy = VendorM1PolicyV1(
        "EURUSD",
        "202401",
        START,
        START + MINUTE_NS,
        "0.0001",
        as_of_ns=START + MINUTE_NS,
        tick_available_at_ns=START + MINUTE_NS,
    )
    monkeypatch.setattr(
        native,
        "_verify_native_ticks",
        lambda *a, **k: pytest.fail("native parser reached"),
    )
    monkeypatch.setattr(
        native,
        "_native_bars",
        lambda *a, **k: pytest.fail("native bars reached"),
    )
    with pytest.raises(ValueError, match="availability precedes full snapshot"):
        _report(
            _inputs(
                tmp_path,
                m1=m1,
                ticks=ticks,
                policy=policy,
                available_at_ns=START + MINUTE_NS,
            )
        )


@pytest.mark.parametrize("asof_minutes,eligible", ((1, False), (3, True)))
def test_whole_snapshot_availability_is_retained_not_filtered(
    tmp_path, asof_minutes, eligible
):
    m1 = M1 + "20240101 000200;1;1;1;1;0\n"
    ticks = TICKS + "20240101 000200000,1,1.1,0\n"
    policy = VendorM1PolicyV1(
        "EURUSD",
        "202401",
        START,
        START + MINUTE_NS,
        "0.0001",
        as_of_ns=START + asof_minutes * MINUTE_NS,
        tick_available_at_ns=START + 3 * MINUTE_NS,
    )
    report = _report(
        _inputs(
            tmp_path,
            m1=m1,
            ticks=ticks,
            policy=policy,
            available_at_ns=START + 3 * MINUTE_NS,
        )
    )
    assert report.as_of_eligible is eligible
    assert report.reference_row_count == 2
    assert report.reference_latest_minute_end_ns == START + 3 * MINUTE_NS
    assert report.tick_row_count == 3
    assert report.tick_latest_event_ns == START + 2 * MINUTE_NS
    assert report.selected_tick_count == 2
    assert len(report.minutes) == 1
    assert report.minutes[0].status == "exact_match"
    with pytest.raises(ValueError, match="availability precedes full snapshot"):
        replace(
            report, reference_available_at_ns=START + MINUTE_NS, artifact_id=""
        )


def test_all_concrete_wires_round_trip_and_derived_claims_are_checked(tmp_path):
    inputs = _inputs(tmp_path)
    report = _report(inputs)
    records = (
        inputs[-1],
        inputs[-2],
        report.minutes[0].differences[0],
        report.minutes[0],
        report,
    )
    for record in records:
        cls = type(record)
        assert cls.from_dict(record.to_dict()) == record
        assert cls.from_json(record.to_json()) == record
        payload = record.to_dict()
        payload["schema_version"] += "-unknown"
        with pytest.raises(ValueError, match="schema"):
            cls.from_dict(payload)
        payload = record.to_dict()
        payload["extra"] = True
        with pytest.raises(ValueError, match="fields"):
            cls.from_dict(payload)
    for key, value in (
        ("volume_status", "traded_volume"),
        ("claim_kind", "qualified_source"),
        ("status_counts", {}),
        ("availability_decision", "released"),
        ("as_of_eligible", 0),
    ):
        payload = report.to_dict()
        payload[key] = value
        with pytest.raises(ValueError, match="derived wire claim"):
            VendorM1ReportV1.from_dict(payload)


def test_resealed_numerically_valid_forgery_requires_actual_source_replay(
    tmp_path,
):
    inputs = _inputs(tmp_path)
    report = _report(inputs)
    # Different internally consistent source claim cannot substitute for the
    # selected independent report root, even if all IDs are recomputed.
    forged = replace(
        report, tick_size_bytes=report.tick_size_bytes + 1, artifact_id=""
    )
    with pytest.raises(ValueError, match="independently selected"):
        _replay(forged, inputs, expected_report_id=report.report_id)
    with pytest.raises(ValueError, match="actual source replay"):
        _replay(forged, inputs)


@pytest.mark.parametrize("which", ("vendor", "ticks"))
def test_source_byte_tamper_is_refused(tmp_path, which):
    inputs = _inputs(tmp_path)
    report = _report(inputs)
    path = inputs[0] if which == "vendor" else inputs[2]
    path.write_bytes(path.read_bytes().replace(b"1.0000", b"1.0001"))
    with pytest.raises(ValueError, match="SHA-256"):
        _replay(report, inputs)


def test_wrong_root_refuses_before_source_read(tmp_path, monkeypatch):
    inputs = _inputs(tmp_path)
    report = _report(inputs)
    monkeypatch.setattr(
        native,
        "_read_source",
        lambda *a, **k: pytest.fail("source read reached"),
    )
    with pytest.raises(ValueError, match="identity"):
        _replay(report, inputs, expected_report_id="other:sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="independently selected"):
        _replay(report, inputs, expected_tick_sha256="0" * 64)


def test_subclasses_and_bypass_mutated_nested_values_refuse_before_callback(
    tmp_path,
):
    inputs = _inputs(tmp_path)
    report = _report(inputs)

    class Poison(VendorM1MinuteV1):
        def to_dict(self):
            pytest.fail("untrusted serializer")

    bad = Poison(
        **{
            key: getattr(report.minutes[0], key)
            for key in report.minutes[0].__dataclass_fields__
            if key not in ("schema_version", "PREFIX")
        }
    )
    object.__setattr__(report, "minutes", (bad,))
    with pytest.raises(ValueError, match="exact validation wire"):
        report.to_json()


def test_native_parser_drift_is_detected(tmp_path, monkeypatch):
    from histdatacom import histdata_ascii

    inputs = _inputs(tmp_path)
    original = histdata_ascii.parse_ascii_lines

    def wrong(*args):
        result = original(*args)
        return replace(result, rows=((0, 1.0, 1.0, 0),))

    monkeypatch.setattr(histdata_ascii, "parse_ascii_lines", wrong)
    with pytest.raises(ValueError, match="native T parser"):
        _report(inputs)


def test_native_bar_drift_is_detected(tmp_path, monkeypatch):
    inputs = _inputs(tmp_path)
    original = native._native_bars

    def wrong(*args):
        policy, bars = original(*args)
        object.__setattr__(bars[START], "event_count", 999)
        return policy, bars

    monkeypatch.setattr(native, "_native_bars", wrong)
    with pytest.raises(ValueError, match="native bid OHLC/count"):
        _report(inputs)


def test_prospective_output_reservation_before_both_native_consumers(
    tmp_path, monkeypatch
):
    # Fixed many-reference/empty-tick fixture; no scientific search/iteration.
    text = "".join(
        f"202401{1 + i // 1440:02d} {i % 1440 // 60:02d}{i % 60:02d}00;1;1;1;1;0\n"
        for i in range(8192)
    )
    policy = VendorM1PolicyV1(
        "EURUSD", "202401", START, START + 8192 * MINUTE_NS, "0.0001"
    )
    inputs = _inputs(tmp_path, m1=text, ticks="", policy=policy)
    monkeypatch.setattr(
        native,
        "_native_bars",
        lambda *a, **k: pytest.fail("native bars reached"),
    )
    monkeypatch.setattr(
        native,
        "_verify_native_ticks",
        lambda *a, **k: pytest.fail("native parser reached"),
    )
    with pytest.raises(ValueError, match="prospective report"):
        _report(inputs)


@pytest.mark.parametrize(
    "value",
    (
        "NaN",
        "Infinity",
        "1e-4",
        "-1",
        "0",
        "1.",
        ".1",
        "01",
        "1.0000000000000001",
        "1" * 33,
    ),
)
def test_bad_positive_decimal_lexemes_refuse(value):
    with pytest.raises(ValueError):
        decimal_value(value)


def test_exact_arithmetic_and_pip_sign():
    delta = VendorM1DifferenceV1(
        "close", "1.02", "1.01", "0.01", "-1/100", "1/100", "-1/1"
    )
    assert Fraction(delta.pip_difference) == -1
    with pytest.raises(ValueError, match="reduced"):
        replace(delta, signed_difference="-2/200", artifact_id="")
    with pytest.raises(ValueError, match="differences disagree"):
        replace(delta, absolute_difference="-1/100", artifact_id="")


@pytest.mark.parametrize(
    "which", ("symlink", "directory", "fifo", "oversize", "hardlink")
)
def test_source_file_admission_before_open(tmp_path, monkeypatch, which):
    path, digest = _file(tmp_path, "regular.csv", M1)
    selected = tmp_path / "bad"
    if which == "symlink":
        selected.symlink_to(path)
    elif which == "directory":
        selected.mkdir()
    elif which == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO unavailable on this platform")
        os.mkfifo(selected)
    elif which == "oversize":
        with selected.open("wb") as stream:
            stream.truncate(MAX_SOURCE_BYTES + 1)
    else:
        os.link(path, selected)
    monkeypatch.setattr(
        native.os, "open", lambda *a, **k: pytest.fail("open reached")
    )
    with pytest.raises(ValueError):
        load_histdata_m1_reference(
            selected,
            expected_sha256=digest,
            symbol="EURUSD",
            source_period="202401",
        )


def test_source_inode_change_before_open_refuses(tmp_path, monkeypatch):
    path, digest = _file(tmp_path, "regular.csv", M1)
    replacement, _ = _file(tmp_path, "replacement.csv", M1)
    original = os.open

    def swap(selected, flags):
        replacement.replace(path)
        return original(selected, flags)

    monkeypatch.setattr(native.os, "open", swap)
    with pytest.raises(ValueError, match="identity changed before"):
        load_histdata_m1_reference(
            path,
            expected_sha256=digest,
            symbol="EURUSD",
            source_period="202401",
        )


def test_byte_count_and_row_limits_before_expansion():
    with pytest.raises(ValueError, match="row bound"):
        native._lines("x\n" * 4, 3)
    with pytest.raises(ValueError, match="oversized"):
        native._lines("x" * 513, 3)
    with pytest.raises(ValueError, match="prospective report"):
        reserve_report(8192)


def test_reference_whitespace_and_json_noncanonical_refusal(tmp_path):
    reference = _inputs(tmp_path)[-2]
    with pytest.raises(ValueError, match="canonical"):
        VendorM1ReferenceV1.from_json(reference.to_json() + "\n")
    text = reference.to_json().replace(
        '"symbol":"EURUSD"', '"symbol":"EURUSD","symbol":"EURUSD"'
    )
    with pytest.raises(ValueError, match="duplicate"):
        VendorM1ReferenceV1.from_json(text)
    with pytest.raises(ValueError, match="nesting"):
        VendorM1ReferenceV1.from_json("[" * 21 + "0" + "]" * 21)
    with pytest.raises(ValueError, match="unsupported wire value"):
        canonical({"not_a_float_wire": 0.5})


def test_json_structural_expansion_refuses_before_decoder(monkeypatch):
    from histdatacom.data_quality import vendor_m1_contracts as wire

    # Slightly over one million separators is about two MiB, below byte bound.
    text = "[" + "0," * wire.MAX_JSON_STRUCTURAL_TOKENS + "0]"
    monkeypatch.setattr(
        wire.json, "loads", lambda *a, **k: pytest.fail("decoder reached")
    )
    with pytest.raises(ValueError, match="structural token bound"):
        VendorM1ReportV1.from_json(text)


def test_json_huge_integer_refuses_before_integer_conversion(monkeypatch):
    from histdatacom.data_quality import vendor_m1_contracts as wire

    monkeypatch.setattr(
        wire,
        "int",
        lambda *a, **k: pytest.fail("integer conversion reached"),
        raising=False,
    )
    with pytest.raises(ValueError, match="integer token"):
        VendorM1PolicyV1.from_json('{"x":' + "1" * 81 + "}")


@pytest.mark.parametrize("token", ("1.0", "1e1000000", "-0.1"))
def test_json_float_tokens_refused_without_conversion(token):
    with pytest.raises(ValueError, match="floating JSON"):
        VendorM1PolicyV1.from_json('{"x":' + token + "}")


def test_t_only_legacy_ingestion_is_not_broadened():
    from histdatacom.datasets.adapters import HistDataProviderAdapter
    from histdatacom.histdata_ascii import (
        columns_for_timeframe,
        parse_ascii_lines,
    )

    with pytest.raises(ValueError, match="unsupported ASCII timeframe"):
        columns_for_timeframe("M1")
    with pytest.raises(ValueError, match="unsupported ASCII timeframe"):
        parse_ascii_lines("M1", [M1])
    assert HistDataProviderAdapter().descriptor.granularities == ("T",)


def test_all_nominal_integer_volume_values_stay_unqualified(tmp_path):
    for value in (0, 7, -1):
        inputs = _inputs(tmp_path, m1=M1.replace(";0\n", f";{value}\n"))
        report = _report(inputs)
        assert report.minutes[0].raw_volume == value
        assert report.to_dict()["volume_status"] == VOLUME_STATUS
        assert "qualification" not in json.dumps(report.to_dict())
