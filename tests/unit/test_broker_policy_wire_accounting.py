"""Exact scalar budget equivalence without provider data or decision caching."""

import json

import pytest
from hypothesis import given, strategies as st

from histdatacom.broker_plugin_policy import _wire

SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=200),
)


@given(SCALARS)
def test_scalar_budget_matches_stdlib_encoder_exactly(value):
    encoded = json.dumps(value, ensure_ascii=True, allow_nan=False)
    assert _wire.canonical_json(value) == encoded
    original = _wire.MAX_BYTES
    try:
        _wire.MAX_BYTES = len(encoded)
        assert _wire.canonical_json(value) == encoded
        _wire.MAX_BYTES -= 1
        with pytest.raises(ValueError, match="byte bound"):
            _wire.plain(value)
    finally:
        _wire.MAX_BYTES = original


@pytest.mark.parametrize(
    "value",
    [
        "\ud800",
        "\udfff",
        "\U00010000",
        "\U0010ffff",
        '\\"\n\t\b\f\r',
        "\x00\x1f",
        "é",
    ],
)
def test_surrogates_and_escape_accounting_match_stdlib(value):
    assert _wire.canonical_json(value) == json.dumps(value, ensure_ascii=True)


def test_repeated_aliases_are_still_charged(monkeypatch):
    value = ["é" * 100]
    monkeypatch.setattr(_wire, "MAX_BYTES", 1000)
    assert _wire.plain(value) == value
    with pytest.raises(ValueError, match="byte bound"):
        _wire.plain([value, value])
