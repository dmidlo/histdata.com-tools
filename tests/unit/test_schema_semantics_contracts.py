"""Bounded exact scalar rules and metadata-only import isolation."""

import json
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.schema_semantics import (
    exact_unit_conversion,
    project_semantics,
    semantic_profile,
)
from histdatacom.schema_semantics import canonical, profiles
from histdatacom.schema_semantics.canonical import (
    canonical_json,
    exact_tree,
    load_json,
)
from tests.fixtures.schema_semantics_graph import graph_fixture
from histdatacom.synthetic.contracts import SyntheticEventV1


@pytest.mark.parametrize(
    "value,numerator,denominator,expected",
    [
        (1_000_000, 1, 1_000_000, 1),
        (-1_000_000, 1, 1_000_000, -1),
        (1, 1_000_000, 1, 1_000_000),
        (0, 3, 7, 0),
    ],
)
def test_exact_integer_units_invert_without_rounding(
    value, numerator, denominator, expected
):
    assert exact_unit_conversion(value, numerator, denominator) == expected
    assert exact_unit_conversion(expected, denominator, numerator) == value


@pytest.mark.parametrize(
    "args",
    [
        (1_000_001, 1, 1_000_000),
        (2**62, 4, 1),
        (True, 1, 1),
        (1.0, 1, 1),
        (1, 0, 1),
        (1, 1, 0),
    ],
)
def test_lossy_overflow_and_coercing_unit_conversions_refuse(args):
    with pytest.raises(ValueError):
        exact_unit_conversion(*args)


def test_tagged_numeric_null_order_and_unicode_rules_are_collision_free():
    values = [True, 1, 1.0, "1", None, [], {}, 0.0, -0.0]
    encoded = {canonical_json(exact_tree(value)) for value in values}
    assert len(encoded) == len(values)
    assert exact_tree({"b": 2, "a": 1}) == exact_tree({"a": 1, "b": 2})
    assert exact_tree([1, 2]) != exact_tree([2, 1])
    assert exact_tree("é") != exact_tree("e\u0301")
    assert exact_tree({}) != exact_tree({"missing": None})
    assert exact_tree("2020-01-01T00:00:00Z") != exact_tree(
        "2019-12-31T19:00:00-05:00"
    )


@pytest.mark.parametrize(
    "source",
    [
        '{"x":1,"x":2}',
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":1e9999}',
        '{"value":18446744073709551616}',
    ],
)
def test_malformed_nonfinite_duplicate_and_integer_overflow(source):
    with pytest.raises(ValueError):
        load_json(source)


def test_alias_frontier_and_tagged_expansion_are_bounded_before_materialization(
    monkeypatch,
):
    monkeypatch.setattr(canonical, "MAX_NODES", 1000)
    value = [1] * 900
    aliased = [value] * 900
    with pytest.raises(ValueError, match="pending expansion"):
        canonical_json(aliased)
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError):
        canonical_json(cycle)
    with pytest.raises(ValueError, match="tagged expansion"):
        exact_tree([1] * 500)


def test_escaped_cost_is_checked_before_encoder_allocation(monkeypatch):
    def never_encode(*args, **kwargs):
        raise AssertionError("encoder must not see over-budget input")

    monkeypatch.setattr(canonical.json, "dumps", never_encode)
    with pytest.raises(ValueError, match="escaped string"):
        canonical_json("\U00010000" * 1000, maximum=2000)
    with pytest.raises(ValueError, match="escaped string"):
        canonical_json({"\u0000" * 1000: 1}, maximum=2000)
    with pytest.raises(ValueError, match="escaped string"):
        canonical_json("\u007f" * 1000, maximum=2000)


def test_exact_escaped_boundary_and_tagged_depth():
    value = "\U00010000" * 10
    assert len(canonical_json(value, maximum=122)) == 122
    with pytest.raises(ValueError):
        canonical_json(value, maximum=121)
    for _ in range(33):
        value = [value]
    with pytest.raises(ValueError, match="tagged depth"):
        exact_tree(value)


def test_profile_pins_do_not_auto_admit_changed_native_code(monkeypatch):
    monkeypatch.setitem(profiles._SOURCES, "synthetic/contracts.py", "0" * 64)
    with pytest.raises(ValueError, match="profile review required"):
        project_semantics("synthetic-event-v1", "{}")


def test_immutable_profiles_and_structural_proof_roundtrip():
    profile = semantic_profile("synthetic-event-v1")
    with pytest.raises(FrozenInstanceError):
        profile.name = "other"
    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=10,
        event_sequence=0,
        bid=1.0,
        ask=1.001,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="source-1",
        source_series_id="series-1",
        source_period="202001",
        source_row_id=1,
    )
    _, (first, _, _) = graph_fixture(event.to_json())
    with pytest.raises(ValueError):
        type(first).from_json(json.dumps(first.to_dict(), indent=2))
    with pytest.raises(ValueError):
        type(first).from_dict({**first.to_dict(), "future": 1})
    with pytest.raises(ValueError):
        replace(first, implementation_sha256="invalid")


def test_nested_proof_aliases_refuse_before_wire_expansion(monkeypatch):
    from histdatacom.schema_semantics import SemanticCompositionProofV1
    from histdatacom.schema_semantics import contracts

    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=10,
        event_sequence=0,
        bid=1.0,
        ask=1.001,
        run_id="run-1",
        ensemble_member_id="member-1",
        source_version_id="source-1",
        source_series_id="series-1",
        source_period="202001",
        source_row_id=1,
    )
    _, (first, _, _) = graph_fixture(event.to_json())
    assert first.source_json == first.destination_json
    monkeypatch.setattr(contracts, "MAX_PROOF_BYTES", 16_000)

    def never_wire(*args):
        raise AssertionError("aggregate proof must refuse before expansion")

    monkeypatch.setattr(type(first), "to_dict", never_wire)
    with pytest.raises(ValueError, match="aggregate byte bound"):
        SemanticCompositionProofV1((first,) * 16)


def test_semantic_metadata_and_legacy_queries_import_no_native_producers():
    program = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        forbidden = ("histdatacom.synthetic", "histdatacom.forecasting", "histdatacom.broker_plugins", "histdatacom.broker_plugin_lifecycle", "histdatacom.market_context", "histdatacom.data_quality", "histdatacom.reconstruction_evidence", "numpy", "polars", "pyarrow")
        if any(fullname == prefix or fullname.startswith(prefix + ".") for prefix in forbidden):
            raise RuntimeError("unwanted producer import: " + fullname)
sys.meta_path.insert(0, Block())
from histdatacom.schema_compatibility import schema_compatibility_registry
from histdatacom.schema_semantics import semantic_profiles, validate_lossless_evidence
assert len(semantic_profiles()) == 9
assert not validate_lossless_evidence(schema_compatibility_registry()).lossless_subject_ids
"""
    subprocess.run([sys.executable, "-c", program], check=True, timeout=30)
