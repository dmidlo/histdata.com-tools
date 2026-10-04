"""Independent strict-wire tests; decoding is never native evidence."""

from dataclasses import replace
from fractions import Fraction
import json

import pytest

from histdatacom.cross_feed._wire import RationalV1
from histdatacom.cross_feed.capture_contracts import NativeCaptureV1
from histdatacom.cross_feed.clock_contracts import ClockFitPolicyV1


@pytest.mark.parametrize(
    "numerator,denominator",
    [
        ("01", "1"),
        ("-0", "1"),
        ("2", "2"),
        ("1", "0"),
        ("+1", "2"),
        ("1", "-2"),
        ("9" * 257, "1"),
        (True, "1"),
    ],
)
def test_reject_noncanonical_rationals(numerator, denominator):
    with pytest.raises(ValueError):
        RationalV1(numerator, denominator)


def test_large_reduced_rational_roundtrips_exactly():
    value = RationalV1.from_fraction(Fraction(2**100 + 1, 2**99))
    assert RationalV1.from_payload(value.to_payload()).value == value.value


def test_artifact_identity_and_unknown_nested_field():
    value = ClockFitPolicyV1()
    assert ClockFitPolicyV1.from_json(value.to_json()) == value
    data = value.to_dict()
    data["payload"]["invisible_override"] = True
    with pytest.raises(ValueError):
        ClockFitPolicyV1.from_dict(data)
    assert (
        replace(value, maximum_prediction_horizon_ns=1).artifact_id
        != value.artifact_id
    )


@pytest.mark.parametrize(
    "wire",
    [
        "[" * 21 + "]" * 21,
        '{"n":' + "9" * 2000 + "}",
        '{"n":1.5}',
        '{"n":NaN}',
        '{"a":0,"a":1}',
    ],
)
def test_prospective_wire_refusals(wire):
    with pytest.raises(ValueError):
        ClockFitPolicyV1.from_json(wire)


def test_json_pretty_or_changed_identity_refuses():
    value = ClockFitPolicyV1()
    with pytest.raises(ValueError):
        ClockFitPolicyV1.from_json(json.dumps(value.to_dict(), indent=2))
    data = value.to_dict()
    data["artifact_id"] = "cross-feed-clock-fit-policy:sha256:" + "0" * 64
    with pytest.raises(ValueError):
        ClockFitPolicyV1.from_dict(data)


@pytest.mark.parametrize(
    "wire",
    [
        "[" + ",".join("0" for _ in range(4097)) + "]",
        "["
        + ",".join(
            "[" + ",".join("0" for _ in range(4096)) + "]" for _ in range(33)
        )
        + "]",
    ],
)
def test_node_and_collection_bounds_precede_json_allocation(monkeypatch, wire):
    def poison(*args, **kwargs):
        raise AssertionError("decoder reached before prospective bound")

    monkeypatch.setattr("histdatacom.cross_feed._wire.json.loads", poison)
    with pytest.raises(ValueError, match="bound"):
        ClockFitPolicyV1.from_json(wire)


def test_capture_projection_does_not_claim_native_qualification():
    value = NativeCaptureV1(
        "root", "manifest", "audit", "unavailable", (), (), "0" * 64, 0
    )
    assert value.source_family == "synthetic_kernel_input"
    assert NativeCaptureV1.from_json(value.to_json()) == value


def test_nested_record_subclass_does_not_enter_constructor():
    class Pretender(RationalV1):
        pass

    with pytest.raises(ValueError):
        ClockFitPolicyV1(residual_band_multiplier=Pretender("3", "1"))
