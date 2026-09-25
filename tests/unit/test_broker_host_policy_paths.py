"""Generated host paths normalize parents, never native or receipt leaves."""

from dataclasses import replace
import errno
import os
from pathlib import Path

import pytest

from histdatacom.broker_capture import (
    AppendOnlyBrokerCaptureWriterV1,
    BrokerCaptureReplaySourceV1,
)
from histdatacom.broker_capture.storage import SESSION_MANIFEST_FILENAME
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleState,
)
from histdatacom.broker_plugin_policy import read_broker_policy_receipt
from histdatacom.broker_plugin_security import (
    BrokerSecurityError,
    read_security_receipt,
    write_security_receipt,
)
from tests.fixtures.broker_provider_policy import (
    generated_legacy_request,
    generated_provider_scope,
    generated_sdk_request,
    legacy_policy_inputs,
)
from tests.unit.test_broker_plugin_security_contracts import receipt, request


def _symlink(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except NotImplementedError:
        pytest.skip("symlink primitive is unavailable")
    except OSError as error:
        if os.name == "nt" and getattr(error, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is unavailable")
        raise


def _host_root(tmp_path, monkeypatch, kind):
    actual = tmp_path / "actual"
    actual.mkdir()
    if kind == "relative":
        monkeypatch.chdir(tmp_path)
        return Path("actual") / "output"
    alias = tmp_path / "alias"
    _symlink(alias, actual, directory=True)
    # The alias is an ancestor. Security's existing immediate-parent
    # no-symlink rule remains intact, independently of policy path handling.
    return alias / "output"


def _capture(root):
    inputs = legacy_policy_inputs()
    provider_request = generated_legacy_request(inputs.session)
    events = inputs.events[:4]
    with generated_provider_scope(provider_request):
        writer = AppendOnlyBrokerCaptureWriterV1(
            root,
            session=inputs.session,
            storage_policy=inputs.storage_policy,
            provider_request=provider_request,
        )
        for event in events:
            writer.append(event)
        manifest = writer.close()
    return provider_request, manifest, events


def _security_inputs():
    inventory, plan, _ = request()
    provider_request = generated_sdk_request(plan)
    manifest = BrokerLifecycleManifestV1(
        BrokerLifecycleHeaderV1(
            inventory, plan, BrokerLifecyclePolicyV1(), (), "e" * 32, "3.10.19"
        ),
        BrokerLifecycleState.DISCOVERED,
        BrokerLifecycleCompletion.OPEN,
    )
    # Actual native OPEN metadata, not a fabricated completed provider run.
    value = replace(receipt(), native_manifest_id=manifest.artifact_id)
    return provider_request, manifest, value


@pytest.mark.parametrize("kind", ("relative", "ancestor_alias"))
def test_legacy_capture_host_parent_roundtrip(tmp_path, monkeypatch, kind):
    root = _host_root(tmp_path, monkeypatch, kind)
    provider_request, manifest, events = _capture(root)
    native = root / manifest.session.session_id / SESSION_MANIFEST_FILENAME
    sidecar = native.with_name(native.name + ".provider-policy.json")
    original_native, original_sidecar = (
        native.read_bytes(),
        sidecar.read_bytes(),
    )
    with generated_provider_scope(provider_request):
        for selected in (root, root.resolve()):
            replay = BrokerCaptureReplaySourceV1(
                selected, manifest, provider_request=provider_request
            )
            assert tuple(replay.iter_events()) == events
    assert native.read_bytes() == original_native
    assert sidecar.read_bytes() == original_sidecar
    assert (
        read_broker_policy_receipt(
            sidecar.parent.resolve() / sidecar.name
        ).native_artifact_name
        == native.name
    )


@pytest.mark.parametrize("leaf", ("native", "receipt"))
def test_legacy_capture_replay_refuses_symlink_evidence_leaf(tmp_path, leaf):
    root = tmp_path / "capture"
    provider_request, manifest, _ = _capture(root)
    native = root / manifest.session.session_id / SESSION_MANIFEST_FILENAME
    target = (
        native
        if leaf == "native"
        else native.with_name(native.name + ".provider-policy.json")
    )
    backup = target.with_name(target.name + ".original")
    target.rename(backup)
    _symlink(target, backup)
    original = backup.read_bytes()
    with (
        generated_provider_scope(provider_request),
        pytest.raises((ValueError, OSError)) as refused,
    ):
        tuple(
            BrokerCaptureReplaySourceV1(
                root, manifest, provider_request=provider_request
            ).iter_events()
        )
    if isinstance(refused.value, OSError):
        assert refused.value.errno == errno.ELOOP
    assert target.is_symlink()
    assert backup.read_bytes() == original


@pytest.mark.parametrize("kind", ("relative", "ancestor_alias"))
def test_security_receipt_host_parent_roundtrip(tmp_path, monkeypatch, kind):
    root = _host_root(tmp_path, monkeypatch, kind)
    root.mkdir()
    path = root / "security.json"
    provider_request, manifest, value = _security_inputs()
    with generated_provider_scope(provider_request):
        write_security_receipt(
            value, path, provider_request=provider_request, manifest=manifest
        )
        sidecar = path.with_name(path.name + ".provider-policy.json")
        original_native = path.read_bytes()
        original_sidecar = sidecar.read_bytes()
        assert read_security_receipt(path) == value
        for selected in (path, path.parent.resolve() / path.name):
            write_security_receipt(
                value,
                selected,
                provider_request=provider_request,
                manifest=manifest,
            )
        assert path.read_bytes() == original_native
        assert sidecar.read_bytes() == original_sidecar


@pytest.mark.parametrize("leaf", ("native", "receipt"))
def test_security_receipt_refuses_symlink_evidence_leaf(tmp_path, leaf):
    provider_request, manifest, value = _security_inputs()
    path = tmp_path / "security.json"
    with generated_provider_scope(provider_request):
        write_security_receipt(
            value, path, provider_request=provider_request, manifest=manifest
        )
        target = (
            path
            if leaf == "native"
            else path.with_name(path.name + ".provider-policy.json")
        )
        backup = target.with_name(target.name + ".original")
        target.rename(backup)
        _symlink(target, backup)
        original = backup.read_bytes()
        with pytest.raises(BrokerSecurityError):
            write_security_receipt(
                value,
                path,
                provider_request=provider_request,
                manifest=manifest,
            )
        assert target.is_symlink()
        assert backup.read_bytes() == original


def test_security_keeps_immediate_parent_symlink_refusal(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    _symlink(alias, actual, directory=True)
    provider_request, manifest, value = _security_inputs()
    with (
        generated_provider_scope(provider_request),
        pytest.raises(BrokerSecurityError),
    ):
        write_security_receipt(
            value,
            alias / "security.json",
            provider_request=provider_request,
            manifest=manifest,
        )
    assert list(actual.iterdir()) == []
