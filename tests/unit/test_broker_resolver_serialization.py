"""Same-call byte reuse, never source qualification or authority caching."""

import hashlib
import json
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from histdatacom.broker_plugin_policy import bindings, derived
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyDataClass as DataClass,
)
from tests.fixtures.broker_derived_policy import generated_fingerprint
from tests.fixtures.broker_resolver_serialization import artifacts

GOLDEN = json.loads(
    (
        Path(__file__).parents[1]
        / "fixtures/broker_resolver_serialization_v1.json"
    ).read_text()
)["artifacts"]
NAMES = tuple(GOLDEN)
JSON_NAMES = tuple(
    name
    for name in NAMES
    if name
    not in {
        "selection_refused",
        "manifest_refused",
    }
)


@pytest.fixture(scope="module")
def examples():
    return artifacts()


def _resolve(name, artifact):
    if name.startswith("fingerprint"):
        result = bindings._resolve_native(artifact)
        return (
            result.artifact_json,
            result.subject.native_ref.native_id,
            result.subject,
        )
    _, text, identity = derived._native_snapshot(artifact)
    return text, identity, None


@pytest.mark.parametrize("name", NAMES)
def test_prechange_canonical_bytes_ids_and_subjects(examples, name):
    expected = GOLDEN[name]
    text, identity, subject = _resolve(name, examples[name])
    assert text == expected["native_json"]
    assert identity == expected["native_id"]
    assert len(text.encode("utf-8")) == expected["bytes"]
    assert (
        hashlib.sha256(text.encode("utf-8")).hexdigest() == expected["sha256"]
    )
    if subject is not None:
        assert subject.to_json() == expected["subject_json"]
        assert subject.native_ref.sha256 == expected["sha256"]
        assert subject.native_ref.byte_length == expected["bytes"]
        assert subject.native_ref.kind == type(examples[name]).__name__
        assert len(subject.bindings) == 1
        assert subject.bindings[0].provider_id == examples[name].adapter_id
        classes = {
            DataClass.FINGERPRINTS,
            DataClass.CONTENT_HASHES,
            DataClass.HEALTH,
        }
        if name == "fingerprint_v1_private_opaque":
            classes |= {DataClass.PRIVATE_ACCOUNT, DataClass.RAW_PAYLOAD}
        assert set(subject.data_classes) == classes
    else:
        assert expected["subject_json"] is None
        assert examples[name].status.value == "refused"


@pytest.mark.parametrize("name", NAMES)
def test_only_redundant_post_validation_encodings_removed(examples, name):
    artifact = examples[name]
    is_dict = name not in JSON_NAMES
    method = "to_dict" if is_dict else "to_json"
    original = getattr(type(artifact), method)
    calls = Counter()

    def encode(self):
        caller = sys._getframe(1).f_code
        calls[Path(caller.co_filename).name + ":" + caller.co_name] += 1
        return original(self)

    with patch.object(type(artifact), method, encode):
        text, _, _ = _resolve(name, artifact)
    assert text == GOLDEN[name]["native_json"]
    before = GOLDEN[name]["serializer_calls"]
    if is_dict:
        # The separate dictionary-reader branch is completely unchanged.
        assert dict(calls) == before
    else:
        expected = dict(before)
        expected["bindings.py:_restore_native_with_json"] = expected.pop(
            "bindings.py:_restore_native"
        )
        if name.startswith("fingerprint"):
            assert expected.pop("bindings.py:_native_ref") == 1
            assert expected.pop("bindings.py:_resolve_native") == 1
            removed = 2
        else:
            assert expected.pop("derived.py:_native_snapshot") == 1
            removed = 1
        assert dict(calls) == expected
        assert sum(calls.values()) == sum(before.values()) - removed
        assert calls["bindings.py:_restore_native_with_json"] == 2


@pytest.mark.parametrize("name", JSON_NAMES)
def test_compatibility_wrapper_returns_detached_native_object(examples, name):
    artifact = examples[name]
    restored = bindings._restore_native(artifact, {type(artifact)})
    paired, text = bindings._restore_native_with_json(
        artifact, {type(artifact)}
    )
    assert type(restored) is type(paired) is type(artifact)
    assert restored is not artifact and paired is not artifact
    assert restored is not paired
    assert (
        restored.to_json()
        == paired.to_json()
        == text
        == GOLDEN[name]["native_json"]
    )


def test_checked_snapshot_detaches_mutable_children_and_next_call_is_fresh():
    original = generated_fingerprint()
    restored, text = bindings._restore_native_with_json(
        original, {type(original)}
    )
    first = bindings._resolve_native(original)
    original.cells[0].metrics[0].quantiles["q0.5"] = 0.0003
    assert restored.cells[0].metrics[0].quantiles == {}
    assert restored.to_json() == text == first.artifact_json
    # The old ID cannot legitimize newly mutated children on a later call.
    with pytest.raises(ValueError, match="metric_id does not match"):
        bindings._resolve_native(original)
    # Mutating the returned object cannot alter its retained immutable text.
    restored.cells[0].metrics[0].quantiles["q0.5"] = 0.0004
    assert text == first.artifact_json
    with pytest.raises(ValueError, match="metric_id does not match"):
        bindings._restore_native_with_json(restored, {type(restored)})


def test_derived_snapshot_detaches_and_rechecks_mutated_child(examples):
    cls = type(examples["proposal_refused"])
    original = cls.from_json(GOLDEN["proposal_refused"]["native_json"])
    restored, text, identity = derived._native_snapshot(original)
    original.metrics_before["spread"] = 0.0003
    assert restored.metrics_before["spread"] == 0.0002
    assert restored.to_json() == text
    assert identity == GOLDEN["proposal_refused"]["native_id"]
    with pytest.raises(ValueError, match="broker proposal_id differs"):
        derived._native_snapshot(original)


def test_later_valid_resolution_is_not_cached():
    original = generated_fingerprint()
    changed = replace(original, server_id="different-server", fingerprint_id="")
    first = bindings._resolve_native(original)
    second = bindings._resolve_native(changed)
    assert first.artifact_json != second.artifact_json
    assert first.subject.native_ref != second.subject.native_ref
    assert first.subject.bindings != second.subject.bindings
    assert second.artifact_json == changed.to_json()


@pytest.mark.parametrize("entrypoint", ["paired", "compatibility", "derived"])
def test_unknown_exact_types_and_subclasses_fail_before_serialization(
    monkeypatch, entrypoint
):
    original = generated_fingerprint()
    subclass = type("UntrustedFingerprint", (type(original),), {})
    value = subclass.from_json(original.to_json())
    monkeypatch.setattr(
        subclass,
        "to_json",
        lambda _: pytest.fail("untrusted serializer called"),
    )
    monkeypatch.setattr(
        bindings, "_preflight_native", lambda _: pytest.fail("exact type first")
    )
    if entrypoint == "derived":

        def call(item):
            return derived._native_snapshot(item)

        message = "unsupported exact broker-derived native artifact"
    else:
        method = (
            bindings._restore_native_with_json
            if entrypoint == "paired"
            else bindings._restore_native
        )

        def call(item):
            return method(item, {type(original)})

        message = "unsupported native provider artifact type"
    for item in (value, object()):
        with pytest.raises(ValueError) as exc:
            call(item)
        assert str(exc.value) == message


@pytest.mark.parametrize(
    "mutation,message",
    [
        ("text", "native provider byte bound"),
        ("depth", "native provider expanded node/depth bound"),
        ("nodes", "native provider expanded node/depth bound"),
        ("wide", "native provider collection bound"),
        ("key", "invalid native provider object key"),
        ("integer", "native provider integer bound"),
        ("nonfinite", "unsupported native provider field"),
        ("object", "unsupported native provider field"),
    ],
)
@pytest.mark.parametrize("entrypoint", ["fingerprint", "derived"])
def test_expanded_preflight_still_precedes_serializer(
    examples, monkeypatch, mutation, message, entrypoint
):
    artifact = (
        generated_fingerprint()
        if entrypoint == "fingerprint"
        else type(examples["proposal_refused"]).from_json(
            GOLDEN["proposal_refused"]["native_json"]
        )
    )
    if mutation == "text":
        value = "x" * (8 * 1024 * 1024 + 1)
    elif mutation == "depth":
        value = {}
        value["cycle"] = value
    elif mutation == "nodes":
        value = [[None] * 4096] * 64
    elif mutation == "wide":
        value = [None] * 4097
    elif mutation == "key":
        value = {1: None}
    elif mutation == "integer":
        value = 2**65
    elif mutation == "nonfinite":
        value = float("nan")
    else:
        value = object()
    field = "limitations" if entrypoint == "fingerprint" else "metrics_before"
    object.__setattr__(artifact, field, value)
    monkeypatch.setattr(
        type(artifact),
        "to_json",
        lambda _: pytest.fail("preflight must run first"),
    )
    with pytest.raises(ValueError) as exc:
        _resolve(
            (
                "fingerprint"
                if entrypoint == "fingerprint"
                else "proposal_refused"
            ),
            artifact,
        )
    assert str(exc.value) == message


@pytest.mark.parametrize(
    "encoding", ["not_text", "str_subclass", "ascii_overflow", "utf8_overflow"]
)
def test_serialized_envelope_is_checked_before_reader(monkeypatch, encoding):
    artifact = generated_fingerprint()

    class Text(str):
        pass

    values = {
        "not_text": None,
        "str_subclass": Text("{}"),
        "ascii_overflow": "x" * (8 * 1024 * 1024 + 1),
        "utf8_overflow": "é" * (4 * 1024 * 1024 + 1),
    }
    monkeypatch.setattr(type(artifact), "to_json", lambda _: values[encoding])
    monkeypatch.setattr(
        type(artifact),
        "from_json",
        classmethod(lambda *_: pytest.fail("envelope must precede reader")),
    )
    with pytest.raises(ValueError) as exc:
        bindings._restore_native_with_json(artifact, {type(artifact)})
    assert (
        str(exc.value)
        == "native provider artifact exceeds bounded identity envelope"
    )


@pytest.mark.parametrize(
    "name", ["fingerprint_v1", "fingerprint_v2_constructed", "proposal_refused"]
)
def test_forged_identity_is_still_rejected_by_native_reader(examples, name):
    cls = type(examples[name])
    artifact = cls.from_json(GOLDEN[name]["native_json"])
    field = (
        "fingerprint_id" if name.startswith("fingerprint") else "proposal_id"
    )
    object.__setattr__(artifact, field, "forged-id")
    with pytest.raises(ValueError, match="identity|proposal_id differs"):
        _resolve(name, artifact)


@pytest.mark.parametrize("name", ["fingerprint_v1", "proposal_refused"])
def test_first_roundtrip_comparison_cannot_be_skipped(
    examples, monkeypatch, name
):
    artifact = examples[name]
    cls = type(artifact)
    original = cls.to_json
    calls = []

    def encode(self):
        calls.append(self is artifact)
        # The actual reader accepts whitespace; checked bytes must still match.
        return original(self) + (" " if self is artifact else "")

    monkeypatch.setattr(cls, "to_json", encode)
    with pytest.raises(ValueError) as exc:
        _resolve(name, artifact)
    assert (
        str(exc.value)
        == "native provider artifact does not round-trip byte-for-byte"
    )
    assert calls == [True, False]


def test_resolving_metadata_does_not_read_or_grant_current_rights(monkeypatch):
    from histdatacom.broker_plugin_policy import scope

    monkeypatch.setattr(
        scope,
        "read_current_provider_policy_context",
        lambda: pytest.fail("metadata is not a rights check"),
    )
    monkeypatch.setattr(
        scope,
        "require_provider_operation",
        lambda *_args, **_kwargs: pytest.fail("metadata is not a rights grant"),
    )
    native = generated_fingerprint()
    assert (
        bindings.resolve_provider_subject(native).native_ref.native_id
        == native.fingerprint_id
    )
    assert bindings.native_provider_artifact_json(native) == native.to_json()
