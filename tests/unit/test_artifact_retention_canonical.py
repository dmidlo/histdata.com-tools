"""Tiny synthetic codec controls, not authority to delete any artifact."""

from dataclasses import FrozenInstanceError, dataclass
from enum import Enum
import hashlib
import json
from typing import ClassVar, Literal

import pytest

from histdatacom.artifact_retention import canonical as codec


class Mode(str, Enum):
    KEEP = "keep"
    INSPECT = "inspect"


@dataclass(frozen=True, slots=True)
class Leaf(codec.RetentionContract):
    SCHEMA: ClassVar[str] = "synthetic.retention-leaf.v1"
    KIND: ClassVar[str] = "test-leaf"
    label: str
    count: int
    enabled: bool = True

    def _validate(self):
        if not self.label or self.count < 0:
            raise ValueError("invalid generated leaf")


@dataclass(frozen=True, slots=True)
class Envelope(codec.RetentionContract):
    SCHEMA: ClassVar[str] = "synthetic.retention-envelope.v1"
    KIND: ClassVar[str] = "test-envelope"
    leaf: Leaf
    children: tuple[Leaf, ...] = ()
    mode: Mode = Mode.KEEP
    note: str | None = None
    revision: Literal[1] = 1

    def _validate(self):
        if len({child.label for child in self.children}) != len(self.children):
            raise ValueError("duplicate generated child")


@dataclass(frozen=True, slots=True)
class Chain(codec.RetentionContract):
    SCHEMA: ClassVar[str] = "synthetic.retention-chain.v1"
    KIND: ClassVar[str] = "test-chain"
    label: str
    child: "Chain | None" = None

    def _validate(self):
        if not self.label:
            raise ValueError("empty generated chain label")


class LeafSubclass(Leaf):
    def to_dict(self):
        raise AssertionError("subclass serializer must not run")


class FieldPoisonLeaf(Leaf):
    def __getattribute__(self, name):
        if name in {"label", "count", "enabled"}:
            raise AssertionError("subclass field access must not run")
        return super().__getattribute__(name)


class StringSubclass(str):
    pass


class IntegerSubclass(int):
    pass


LEAF_JSON = (
    '{"count":2,"enabled":true,"label":"generated",'
    '"schema_version":"synthetic.retention-leaf.v1"}'
)


def test_exact_canonical_golden_and_independent_content_identity():
    leaf = Leaf("generated", 2)
    assert leaf.to_json() == LEAF_JSON
    assert leaf.artifact_id == (
        "retention-test-leaf:sha256:"
        + hashlib.sha256(LEAF_JSON.encode("ascii")).hexdigest()
    )
    assert Leaf.from_json(LEAF_JSON) == leaf
    assert Leaf.from_dict(json.loads(LEAF_JSON)) == leaf


def test_nested_roundtrip_enum_optional_and_detached_wire():
    value = Envelope(
        Leaf("generated", 2), (Leaf("child", 0, False),), Mode.INSPECT, "note"
    )
    encoded = value.to_json()
    assert Envelope.from_json(encoded) == value
    wire = value.to_dict()
    assert type(wire["children"]) is list
    assert wire["mode"] == "inspect"
    wire["children"][0]["label"] = "changed"
    assert value.children[0].label == "child"
    assert value.to_json() == encoded
    with pytest.raises(FrozenInstanceError):
        value.note = "changed"


@pytest.mark.parametrize(
    "field,value",
    [
        ("label", StringSubclass("label")),
        ("label", 1),
        ("count", True),
        ("count", IntegerSubclass(1)),
        ("count", 1.0),
        ("enabled", 1),
        ("enabled", "true"),
        ("count", codec.MAX_INTEGER + 1),
        ("count", -1),
        ("label", ""),
    ],
)
def test_constructor_rejects_aliases_subclasses_and_domain_invalidity(
    field, value
):
    values = {"label": "generated", "count": 2, "enabled": True}
    values[field] = value
    with pytest.raises(ValueError):
        Leaf(**values)


@pytest.mark.parametrize(
    "field,value",
    [
        ("leaf", {"label": "not a typed leaf"}),
        ("children", []),
        ("mode", "keep"),
        ("note", False),
        ("revision", True),
        ("revision", 1.0),
        ("revision", 2),
    ],
)
def test_envelope_constructor_exact_types(field, value):
    values = {"leaf": Leaf("generated", 2), field: value}
    with pytest.raises(ValueError):
        Envelope(**values)


@pytest.mark.parametrize(
    "change",
    [
        {"extra": 1},
        {"schema_version": "synthetic.retention-leaf.v2"},
        {"count": True},
        {"enabled": 1},
        {"label": ""},
    ],
)
def test_from_dict_rejects_unknown_schema_fields_and_scalar_aliases(change):
    payload = json.loads(LEAF_JSON)
    payload.update(change)
    with pytest.raises(ValueError):
        Leaf.from_dict(payload)


@pytest.mark.parametrize(
    "field", ["label", "count", "enabled", "schema_version"]
)
def test_from_dict_rejects_missing_fields(field):
    payload = json.loads(LEAF_JSON)
    del payload[field]
    with pytest.raises(ValueError):
        Leaf.from_dict(payload)


@pytest.mark.parametrize(
    "change",
    [
        {"children": ()},
        {"children": [None]},
        {"mode": "delete"},
        {"mode": Mode.KEEP},
        {"revision": True},
        {"leaf": Leaf("typed-not-wire", 0)},
    ],
)
def test_nested_wire_types_are_not_constructor_types(change):
    payload = Envelope(Leaf("generated", 2)).to_dict()
    payload.update(change)
    with pytest.raises(ValueError):
        Envelope.from_dict(payload)


@pytest.mark.parametrize(
    "text",
    [
        LEAF_JSON + "\n",
        " " + LEAF_JSON,
        LEAF_JSON.replace(":2,", ": 2,"),
        LEAF_JSON.replace(
            '"count":2,"enabled":true', '"enabled":true,"count":2'
        ),
        LEAF_JSON.replace("generated", "\\u0067enerated"),
    ],
)
def test_contract_reader_rejects_semantically_equal_noncanonical_bytes(text):
    assert codec.load_json(text) == json.loads(LEAF_JSON)
    with pytest.raises(ValueError, match="noncanonical"):
        Leaf.from_json(text)


@pytest.mark.parametrize(
    "text",
    [
        '{"x":1,"x":2}',
        '{"x":1,"\\u0078":2}',
        '{"nested":{"x":0,"x":0}}',
        "1.0",
        "1e0",
        "NaN",
        "Infinity",
        "-Infinity",
        str(codec.MAX_INTEGER + 1),
        str(-codec.MAX_INTEGER - 1),
        "1" * 21,
        "[] trailing",
        "{",
        '"]" ]',
        '"literal non-ASCII é"',
    ],
)
def test_parser_rejects_duplicate_keys_nonintegers_and_malformed_json(text):
    with pytest.raises(ValueError):
        codec.load_json(text)


def test_integer_boundaries_and_type_separation_are_exact():
    for value in (-codec.MAX_INTEGER, 0, codec.MAX_INTEGER):
        assert codec.load_json(str(value)) == value
        assert codec.canonical_json(value) == str(value)
    assert codec.canonical_json(True) == "true"
    assert codec.canonical_json(1) == "1"
    assert codec.load_json("true") is True
    assert type(codec.load_json("1")) is int


@pytest.mark.parametrize("value", [1.0, float("nan"), {1: "x"}, {"x"}, b"x"])
def test_encoder_rejects_non_wire_values(value):
    with pytest.raises(ValueError):
        codec.canonical_json(value)


@pytest.mark.parametrize("value", [bytearray(b"x"), memoryview(b"x"), "x", 1])
def test_hashing_rejects_nonexact_bytes(value):
    with pytest.raises(ValueError):
        codec.sha256(value)


def test_hashing_preserves_exact_bytes():
    assert codec.sha256(b"x\n") == hashlib.sha256(b"x\n").hexdigest()
    assert codec.sha256(b"x\n") != codec.sha256(b"x")


@pytest.mark.parametrize(
    "field,value", [("count", True), ("count", -1), ("label", "")]
)
@pytest.mark.parametrize(
    "boundary", ["constructor", "to_dict", "to_json", "artifact_id"]
)
def test_nested_frozen_tampering_is_readmitted(field, value, boundary):
    leaf = Leaf("generated", 2)
    parent = Envelope(leaf)
    object.__setattr__(leaf, field, value)
    with pytest.raises(ValueError):
        if boundary == "constructor":
            Envelope(leaf)
        elif boundary == "artifact_id":
            _ = parent.artifact_id
        else:
            getattr(parent, boundary)()


@pytest.mark.parametrize("boundary", ["to_dict", "to_json", "artifact_id"])
def test_outer_frozen_tampering_is_readmitted(boundary):
    parent = Envelope(Leaf("generated", 2))
    object.__setattr__(parent, "revision", True)
    with pytest.raises(ValueError):
        if boundary == "artifact_id":
            _ = parent.artifact_id
        else:
            getattr(parent, boundary)()


@pytest.mark.parametrize("kind", [LeafSubclass, FieldPoisonLeaf])
@pytest.mark.parametrize("boundary", ["constructor", "to_dict", "from_dict"])
def test_nested_subclass_rejected_before_field_or_serializer_dispatch(
    kind, boundary
):
    alien = object.__new__(kind)
    for key, value in (("label", "generated"), ("count", 2), ("enabled", True)):
        object.__setattr__(alien, key, value)
    with pytest.raises(ValueError):
        if boundary == "constructor":
            Envelope(alien)
        elif boundary == "from_dict":
            payload = Envelope(Leaf("generated", 2)).to_dict()
            payload["leaf"] = alien
            Envelope.from_dict(payload)
        else:
            parent = Envelope(Leaf("generated", 2))
            object.__setattr__(parent, "leaf", alien)
            parent.to_dict()


def test_cyclic_forged_frozen_graph_fails_boundedly():
    parent = Chain("generated")
    object.__setattr__(parent, "child", parent)
    with pytest.raises(ValueError, match="structural bound"):
        parent.to_json()


def test_cyclic_raw_tree_fails_boundedly():
    value = []
    value.append(value)
    with pytest.raises(ValueError):
        codec.canonical_json(value)


@pytest.mark.parametrize("text", ["\ud800", "\udfff"])
def test_lone_surrogates_rejected_in_raw_and_escaped_wire(text):
    with pytest.raises(ValueError):
        codec.canonical_json(text)
    with pytest.raises(ValueError):
        codec.load_json(json.dumps(text))


def test_unicode_scalar_escaping_and_quoted_structural_characters():
    text = 'é\x7f😀\\"[{}]\\n'
    expected = '"\\u00e9\\u007f\\ud83d\\ude00\\\\\\"[{}]\\\\n"'
    assert codec.canonical_json(text) == expected
    assert codec.load_json(expected) == text
    assert Leaf.from_json(Leaf(text, 0).to_json()) == Leaf(text, 0)


def test_default_resource_constants_are_explicit():
    assert codec.MAX_WIRE_BYTES == 16 * 1024 * 1024
    assert codec.MAX_WIRE_NODES == 262_144
    assert codec.MAX_WIRE_DEPTH == 32
    assert codec.MAX_COLLECTION_ITEMS == 8192
    assert codec.MAX_INTEGER == 2**63 - 1


def test_real_collection_limit_boundary():
    values = [0] * codec.MAX_COLLECTION_ITEMS
    encoded = codec.canonical_json(values)
    assert codec.load_json(encoded) == values
    with pytest.raises(ValueError):
        codec.canonical_json([*values, 0])
    with pytest.raises(ValueError):
        codec.load_json(
            "[" + ",".join("0" for _ in range(len(values) + 1)) + "]"
        )


def test_structural_depth_rejected_before_json_parser(monkeypatch):
    text = (
        "[" * (codec.MAX_WIRE_DEPTH + 1)
        + "0"
        + "]" * (codec.MAX_WIRE_DEPTH + 1)
    )
    monkeypatch.setattr(
        codec.json, "loads", lambda *_a, **_k: pytest.fail("parser reached")
    )
    with pytest.raises(ValueError, match="structural bound"):
        codec.load_json(text)


def test_node_count_rejected_before_parser_with_small_synthetic_budget(
    monkeypatch,
):
    monkeypatch.setattr(codec, "MAX_WIRE_NODES", 5)
    monkeypatch.setattr(
        codec.json, "loads", lambda *_a, **_k: pytest.fail("parser reached")
    )
    with pytest.raises(ValueError, match="structural bound"):
        codec.load_json("[0,0,0,0,0]")


@pytest.mark.parametrize("text", ["x" * 65, "é"])
def test_raw_size_and_encoding_rejected_before_parser(monkeypatch, text):
    monkeypatch.setattr(codec, "MAX_WIRE_BYTES", 64)
    monkeypatch.setattr(
        codec.json, "loads", lambda *_a, **_k: pytest.fail("parser reached")
    )
    with pytest.raises(ValueError):
        codec.load_json(text)


@pytest.mark.parametrize("char", ["\x7f", "é", "😀", "\\", '"', "\n"])
def test_expanded_escape_budget_rejected_before_serializer(monkeypatch, char):
    monkeypatch.setattr(codec, "MAX_WIRE_BYTES", 64)
    monkeypatch.setattr(
        codec.json, "dumps", lambda *_a, **_k: pytest.fail("serializer reached")
    )
    with pytest.raises(ValueError):
        codec.canonical_json(char * 40)


def test_collection_preflight_rejects_before_serializer(monkeypatch):
    monkeypatch.setattr(
        codec.json, "dumps", lambda *_a, **_k: pytest.fail("serializer reached")
    )
    with pytest.raises(ValueError):
        codec.canonical_json([0] * (codec.MAX_COLLECTION_ITEMS + 1))
