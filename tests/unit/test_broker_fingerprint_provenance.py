"""Generated native capture roots, never a fabricated verification claim."""

import hashlib
import json
import shutil
from dataclasses import replace

import pytest

from histdatacom.broker_capture import (
    BrokerDeliveryFingerprintV2,
    broker_fingerprint_sources,
    fit_broker_delivery_fingerprint,
    fingerprint_sources,
    parse_broker_delivery_fingerprint,
    verify_broker_fingerprint_sources,
)
from histdatacom.broker_capture.fingerprint_v2 import (
    require_qualified_broker_fingerprint,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyDataClass,
    BrokerPolicyError,
    BrokerPolicyRevocationV1,
    require_provider_operation,
    resolve_provider_subject,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyOperation as Operation,
)
from tests.fixtures.broker_derived_policy import (
    generated_fingerprint,
    generated_qualified_fingerprint,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope


@pytest.fixture(scope="module")
def qualified(tmp_path_factory):
    root = tmp_path_factory.mktemp("fingerprint-native-provenance")
    return root, generated_qualified_fingerprint(root)


def test_historical_v1_bytes_stay_readable_but_cannot_admit_new_science():
    historical = generated_fingerprint()
    text = historical.to_json()
    restored = parse_broker_delivery_fingerprint(text)
    assert type(restored) is type(historical)
    assert restored.to_json() == text
    assert (
        parse_broker_delivery_fingerprint(
            json.dumps(historical.to_dict(), indent=2)
        )
        == historical
    )
    assert resolve_provider_subject(restored).bindings
    with (
        generated_provider_scope(restored),
        pytest.raises(ValueError, match="V2 fingerprint"),
    ):
        require_provider_operation(restored, Operation.DERIVE)
    assert historical.to_json() == text


def test_unknown_capture_family_cannot_be_projected_to_legacy_science(
    tmp_path,
):
    class NotLegacy:
        @property
        def session(self):
            pytest.fail(
                "unsupported family must refuse before reading or adapting it"
            )

    with pytest.raises(
        ValueError, match="SDK-native scientific fitting is unavailable"
    ):
        fit_broker_delivery_fingerprint(tmp_path, (NotLegacy(),))
    assert list(tmp_path.iterdir()) == []


def test_actual_fit_persists_exact_roots_and_native_v1_statistics(qualified):
    root, fingerprint = qualified
    native = fingerprint.statistics.to_json()
    restored = parse_broker_delivery_fingerprint(fingerprint.to_json())
    assert type(restored) is BrokerDeliveryFingerprintV2
    assert (
        parse_broker_delivery_fingerprint(fingerprint.to_json() + "\n")
        == fingerprint
    )
    assert restored.statistics.to_json() == native
    assert restored.fingerprint_id != restored.statistics.fingerprint_id
    assert restored.capture_roots == fingerprint.capture_roots
    reference = restored.capture_roots[0]
    assert (
        reference.manifest.manifest_id
        == reference.seal.terminal.native_manifest_id
    )
    assert reference.header.artifact_id == reference.seal.header_id
    with generated_provider_scope(restored, capture_roots=(root,)):
        assert verify_broker_fingerprint_sources(restored) == fingerprint


@pytest.mark.parametrize("level", ["outer", "source", "header", "seal"])
def test_v2_unknown_fields_are_not_silently_dropped(qualified, level):
    _, fingerprint = qualified
    payload = json.loads(fingerprint.to_json())
    target = payload
    if level != "outer":
        target = payload["capture_roots"][0]
    if level in {"header", "seal"}:
        target = target[level]
    target["unknown_retained_claim"] = True
    with pytest.raises(ValueError):
        BrokerDeliveryFingerprintV2.from_dict(payload)


@pytest.mark.parametrize(
    "field", ["manifest_id", "partition_hashes_sha256", "event_count"]
)
def test_statistics_cannot_rebind_different_native_capture_inventory(
    qualified, field
):
    _, fingerprint = qualified
    original = fingerprint.capture_evidence[0]
    value = original.event_count + 1 if field == "event_count" else "0" * 64
    with pytest.raises(ValueError):
        evidence = replace(original, **{field: value})
        statistics = replace(
            fingerprint.statistics,
            capture_evidence=(evidence,),
            fingerprint_id="",
        )
        replace(fingerprint, statistics=statistics, fingerprint_id="")


def test_retained_constructor_is_not_current_replay_or_provider_authority(
    qualified,
):
    root, fingerprint = qualified
    restored = BrokerDeliveryFingerprintV2.from_json(fingerprint.to_json())
    assert require_qualified_broker_fingerprint(restored) == fingerprint
    with (
        generated_provider_scope(restored),
        pytest.raises(ValueError, match="locators"),
    ):
        require_provider_operation(restored, Operation.MATERIAL_USE)
    with broker_fingerprint_sources(root), pytest.raises(BrokerPolicyError):
        require_provider_operation(restored, Operation.MATERIAL_USE)


def test_ambiguous_source_copies_fail_before_source_admission(
    qualified, tmp_path
):
    root, fingerprint = qualified
    copied = tmp_path / "copied"
    shutil.copytree(root, copied)
    with (
        generated_provider_scope(fingerprint, capture_roots=(root, copied)),
        pytest.raises(ValueError, match="absent or ambiguous"),
    ):
        verify_broker_fingerprint_sources(fingerprint)


def test_explicit_source_scope_is_process_bound_and_append_only(
    qualified, tmp_path, monkeypatch
):
    root, fingerprint = qualified
    with broker_fingerprint_sources(root):
        outer = fingerprint_sources._CURRENT.get()
        with broker_fingerprint_sources(root, tmp_path):
            assert fingerprint_sources._CURRENT.get().roots == (root, tmp_path)
        assert fingerprint_sources._CURRENT.get() == outer
        monkeypatch.setattr(fingerprint_sources.os, "getpid", lambda: -1)
        with pytest.raises(ValueError, match="current-process"):
            verify_broker_fingerprint_sources(fingerprint)
        with (
            pytest.raises(ValueError, match="another process"),
            broker_fingerprint_sources(tmp_path),
        ):
            pass


def test_resealed_root_label_is_not_verified_native_evidence(qualified):
    root, fingerprint = qualified
    original = fingerprint.capture_roots[0]
    substituted = replace(
        original, seal=replace(original.seal, root_sha256="0" * 64)
    )
    forged = replace(
        fingerprint, capture_roots=(substituted,), fingerprint_id=""
    )
    assert forged.fingerprint_id != fingerprint.fingerprint_id
    assert BrokerDeliveryFingerprintV2.from_json(forged.to_json()) == forged
    with (
        generated_provider_scope(forged, capture_roots=(root,)),
        pytest.raises(ValueError),
    ):
        require_provider_operation(forged, Operation.DERIVE)


@pytest.mark.parametrize("name", ["seal.json", "chain.jsonl", "header.json"])
def test_native_source_loss_refuses_previously_retained_fingerprint(
    qualified, tmp_path, name
):
    root, fingerprint = qualified
    copied = tmp_path / "copy"
    shutil.copytree(root, copied)
    paths = tuple(
        path for path in copied.rglob(name) if "provenance" in path.parent.name
    )
    assert len(paths) == 1
    paths[0].unlink()
    with (
        generated_provider_scope(fingerprint, capture_roots=(copied,)),
        pytest.raises(ValueError),
    ):
        require_provider_operation(fingerprint, Operation.MATERIAL_USE)


def test_retained_root_does_not_survive_fresh_provider_revocation(qualified):
    root, fingerprint = qualified
    with generated_provider_scope(fingerprint, capture_roots=(root,)) as source:
        require_provider_operation(fingerprint, Operation.MATERIAL_USE)
        context = source.current
        source.current = replace(
            context,
            revocations=(
                BrokerPolicyRevocationV1(
                    context.selected_policy_ids[0],
                    20,
                    20,
                    (context.evidence[0].artifact_id,),
                    "withdrawn",
                ),
            ),
        )
        with pytest.raises(BrokerPolicyError):
            require_provider_operation(fingerprint, Operation.DERIVE)


def test_stale_mutable_legacy_statistics_cannot_hide_behind_outer_identity(
    qualified,
):
    _, fingerprint = qualified
    detached = BrokerDeliveryFingerprintV2.from_json(fingerprint.to_json())
    object.__setattr__(detached.statistics, "effective_start_utc_ns", 1)
    with pytest.raises(ValueError):
        require_qualified_broker_fingerprint(detached)


def test_retained_manifest_opaque_text_cannot_be_declassified_by_statistics(
    qualified,
):
    """Structural metadata review is conservative; this is not a valid replay."""
    _, fingerprint = qualified
    reference = fingerprint.capture_roots[0]
    manifest = replace(
        reference.manifest,
        limitations=("opaque retained provider text",),
        manifest_id="",
    )
    terminal = replace(
        reference.seal.terminal,
        native_manifest_id=manifest.manifest_id,
        native_manifest_sha256=hashlib.sha256(
            manifest.to_json().encode()
        ).hexdigest(),
    )
    changed_root = replace(
        reference,
        manifest=manifest,
        seal=replace(reference.seal, terminal=terminal),
    )
    decision = replace(
        fingerprint.eligibility_decisions[0],
        manifest_id=manifest.manifest_id,
        decision_id="",
    )
    evidence = replace(
        fingerprint.capture_evidence[0],
        manifest_id=manifest.manifest_id,
        eligibility_decision_id=decision.decision_id,
        evidence_id="",
    )
    statistics = replace(
        fingerprint.statistics,
        capture_evidence=(evidence,),
        eligibility_decisions=(decision,),
        fingerprint_id="",
    )
    declared = replace(
        fingerprint,
        statistics=statistics,
        capture_roots=(changed_root,),
        fingerprint_id="",
    )
    assert (
        BrokerPolicyDataClass.RAW_PAYLOAD
        not in resolve_provider_subject(fingerprint).data_classes
    )
    assert (
        BrokerPolicyDataClass.RAW_PAYLOAD
        in resolve_provider_subject(declared).data_classes
    )
