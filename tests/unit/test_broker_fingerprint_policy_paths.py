"""Synthetic fingerprint host paths and immutable paired admission evidence."""

import os
from pathlib import Path

import pytest

from histdatacom.broker_capture.fingerprints import (
    BrokerDeliveryFingerprintArtifactError,
    load_broker_delivery_fingerprint,
    write_broker_delivery_fingerprint,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyDataClass,
    BrokerPolicyError,
    BrokerPolicyOperation,
    BrokerPolicyStatus,
    provider_policy_scope,
    read_broker_policy_receipt,
    resolve_provider_subject,
    scope,
    verify_broker_policy_receipt,
)
from tests.fixtures.broker_derived_policy import generated_fingerprint
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_provider_scope,
    policy_context,
)


def _symlink(path, destination, *, directory=False):
    try:
        path.symlink_to(destination, target_is_directory=directory)
    except NotImplementedError:
        pytest.skip("symlinks unavailable")
    except OSError as error:
        if os.name == "nt" and getattr(error, "winerror", None) == 1314:
            pytest.skip("symlink privilege unavailable")
        raise


def _sidecar(path):
    return path.with_name(path.name + ".provider-policy.json")


def _files(root):
    return {
        path.name: path.read_bytes()
        for path in root.iterdir()
        if path.is_file()
    }


@pytest.mark.parametrize("style", ("canonical", "relative", "parent_alias"))
def test_fingerprint_host_paths_preserve_native_and_admission_on_retry(
    tmp_path, monkeypatch, style
):
    fingerprint = generated_fingerprint()
    actual = tmp_path / "real"
    actual.mkdir()
    if style == "parent_alias":
        alias = tmp_path / "alias"
        _symlink(alias, actual, directory=True)
        target = alias / "fingerprint.json"
    elif style == "relative":
        monkeypatch.chdir(tmp_path)
        target = Path("real/fingerprint.json")
    else:
        target = actual / "fingerprint.json"
    clock = [100]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    with generated_provider_scope(fingerprint):
        first = write_broker_delivery_fingerprint(target, fingerprint)
        before = _files(actual)
        clock[0] = 200
        second = write_broker_delivery_fingerprint(target, fingerprint)
    expected = actual.resolve() / "fingerprint.json"
    assert first == second
    assert first.path == str(expected)
    assert before == _files(actual)
    assert expected.read_bytes() == (fingerprint.to_json() + "\n").encode()
    assert load_broker_delivery_fingerprint(target) == fingerprint
    receipt = read_broker_policy_receipt(_sidecar(expected))
    assert receipt.retention_admission.request.decision_at_ns == 100
    verify_broker_policy_receipt(receipt, fingerprint, expected)


@pytest.mark.parametrize("missing", ("native", "receipt"))
def test_fingerprint_orphan_is_never_repaired(tmp_path, missing):
    fingerprint = generated_fingerprint()
    target = tmp_path / "fingerprint.json"
    with generated_provider_scope(fingerprint):
        write_broker_delivery_fingerprint(target, fingerprint)
        (target if missing == "native" else _sidecar(target)).unlink()
        before = _files(tmp_path)
        with pytest.raises(
            BrokerDeliveryFingerprintArtifactError,
            match="incomplete; no repair",
        ):
            write_broker_delivery_fingerprint(target, fingerprint)
    assert _files(tmp_path) == before


@pytest.mark.parametrize("tampered", ("native", "receipt"))
def test_fingerprint_invalid_pair_is_not_rewritten(tmp_path, tampered):
    fingerprint = generated_fingerprint()
    target = tmp_path / "fingerprint.json"
    with generated_provider_scope(fingerprint):
        write_broker_delivery_fingerprint(target, fingerprint)
        leaf = target if tampered == "native" else _sidecar(target)
        leaf.write_bytes(b"{}\n")
        before = _files(tmp_path)
        with pytest.raises(BrokerDeliveryFingerprintArtifactError):
            write_broker_delivery_fingerprint(target, fingerprint)
    assert _files(tmp_path) == before


@pytest.mark.parametrize("aliased", ("native", "receipt"))
def test_fingerprint_parent_normalization_never_follows_leaf_alias(
    tmp_path, aliased
):
    fingerprint = generated_fingerprint()
    target = tmp_path / "fingerprint.json"
    with generated_provider_scope(fingerprint):
        write_broker_delivery_fingerprint(target, fingerprint)
        leaf = target if aliased == "native" else _sidecar(target)
        preserved = leaf.with_name("retained-" + leaf.name)
        leaf.rename(preserved)
        _symlink(leaf, preserved.name)
        before = _files(tmp_path)
        with pytest.raises(BrokerDeliveryFingerprintArtifactError):
            write_broker_delivery_fingerprint(target, fingerprint)
    assert leaf.is_symlink()
    assert _files(tmp_path) == before


def test_fingerprint_missing_scope_refuses_before_directory_creation(tmp_path):
    target = tmp_path / "must-not-exist" / "fingerprint.json"
    with pytest.raises(BrokerPolicyError):
        write_broker_delivery_fingerprint(target, generated_fingerprint())
    assert not target.parent.exists()


@pytest.mark.parametrize(
    "status", (BrokerPolicyStatus.UNKNOWN, BrokerPolicyStatus.DENIED)
)
def test_fingerprint_existing_pair_still_needs_current_retention(
    tmp_path, status
):
    fingerprint = generated_fingerprint()
    target = tmp_path / "fingerprint.json"
    with generated_provider_scope(fingerprint):
        write_broker_delivery_fingerprint(target, fingerprint)
    before = _files(tmp_path)
    (binding,) = resolve_provider_subject(fingerprint).bindings
    source = MutablePolicySource(
        policy_context(
            binding,
            changes=(
                (
                    BrokerPolicyOperation.RETAIN_LOCAL,
                    BrokerPolicyDataClass.FINGERPRINTS,
                    status,
                ),
            ),
        )
    )
    with provider_policy_scope(source), pytest.raises(BrokerPolicyError):
        write_broker_delivery_fingerprint(target, fingerprint)
    assert _files(tmp_path) == before
