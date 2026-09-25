"""New synthetic native values only; no provider or historical inputs."""

import os
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerPolicyArtifactReceiptV1,
    BrokerPolicyError,
    BrokerPolicyFileSourceV1,
    BrokerPolicyOperation,
    BrokerPolicyStatus,
    capture_request_for,
    legacy_capture_binding,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_policy_receipt,
    scope,
    storage,
    verify_broker_policy_receipt,
    write_broker_policy_context,
    write_broker_policy_receipt,
)
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    legacy_output_contract,
    legacy_policy_inputs,
    policy_context,
)


@pytest.fixture
def values(monkeypatch):
    inputs = legacy_policy_inputs()
    context = policy_context(legacy_capture_binding(inputs.session))
    source = MutablePolicySource(context)
    native = BrokerLegacyRecordV1(
        inputs.session, inputs.events[0], legacy_output_contract()
    )
    encoded = inputs.events[0].to_json().encode() + b"\n"
    monkeypatch.setattr(scope, "_now_ns", lambda: 1000)
    return inputs, context, source, native, encoded


def test_file_source_is_canonical_fresh_and_immutable_writer(tmp_path, values):
    _, context, _, _, _ = values
    path = tmp_path / "review.json"
    write_broker_policy_context(path, context)
    source = BrokerPolicyFileSourceV1(path)
    assert source.read_policy_context() == context
    write_broker_policy_context(path, context)
    other = replace(
        context, execution=replace(context.execution, geography="different")
    )
    with pytest.raises(ValueError, match="different bytes"):
        write_broker_policy_context(path, other)
    assert path.read_bytes() == context.to_json().encode()
    assert not list(tmp_path.glob(".provider-review-*"))
    path.write_text(context.to_json() + "\n")
    with pytest.raises(ValueError):
        source.read_policy_context()


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory"])
def test_source_refuses_nonregular_or_alias_without_blocking(
    tmp_path, values, kind
):
    _, context, _, _, _ = values
    target = tmp_path / "review.json"
    if kind == "symlink":
        original = tmp_path / "original.json"
        original.write_text(context.to_json())
        target.symlink_to(original)
    elif kind == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    with pytest.raises((OSError, ValueError)):
        BrokerPolicyFileSourceV1(target).read_policy_context()


def test_no_policy_scope_has_no_sidecar_effects(tmp_path, values):
    _, _, _, native, encoded = values
    with pytest.raises(BrokerPolicyError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
    assert list(tmp_path.iterdir()) == []


def test_receipt_binds_actual_native_bytes_but_grants_no_current_right(
    tmp_path, values
):
    _, _, source, native, encoded = values
    path = tmp_path / "event.provider-policy.json"
    with provider_policy_scope(source):
        receipt = write_broker_policy_receipt(
            path,
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
    restored = read_broker_policy_receipt(path)
    assert restored == receipt
    target = tmp_path / "event.json"
    with pytest.raises(FileNotFoundError):
        verify_broker_policy_receipt(restored, native, target)
    target.write_bytes(encoded)
    verify_broker_policy_receipt(restored, native, target)
    assert target.read_bytes() == encoded
    assert (
        receipt.operation_admission.request.operation
        is BrokerPolicyOperation.RETAIN_LOCAL
    )
    with pytest.raises(BrokerPolicyError):
        write_broker_policy_receipt(
            path,
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
            replace_existing=True,
        )
    target.write_bytes(encoded + b" ")
    with pytest.raises(ValueError):
        verify_broker_policy_receipt(restored, native, target)


@pytest.mark.parametrize(
    "status", [BrokerPolicyStatus.DENIED, BrokerPolicyStatus.UNKNOWN]
)
def test_denied_receipt_does_not_create_temporary_files(
    tmp_path, values, status
):
    inputs, _, _, native, encoded = values
    source = MutablePolicySource(
        policy_context(legacy_capture_binding(inputs.session), status=status)
    )
    with provider_policy_scope(source), pytest.raises(BrokerPolicyError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("bad_bytes", [b"{}", b"unrelated payload", b"", b"\n"])
def test_unrelated_bytes_cannot_borrow_native_policy(
    tmp_path, values, bad_bytes
):
    _, _, source, native, _ = values
    with provider_policy_scope(source), pytest.raises(ValueError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=bad_bytes,
        )
    assert list(tmp_path.iterdir()) == []


def test_expected_capture_shape_is_not_a_persistable_native_artifact(
    tmp_path, values
):
    inputs, _, source, _, encoded = values
    request = BrokerLegacyCaptureV1(inputs.session, legacy_output_contract())
    with provider_policy_scope(source), pytest.raises(ValueError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            request,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("budget", [0, 1, 100])
def test_sidecar_byte_budget_refuses_before_temporary_io(
    tmp_path, values, budget
):
    _, _, source, native, encoded = values
    with (
        provider_policy_scope(source),
        pytest.raises(ValueError, match="storage budget"),
    ):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
            maximum_bytes=budget,
        )
    assert list(tmp_path.iterdir()) == []


def test_expiry_after_temporary_write_refuses_promotion(
    tmp_path, monkeypatch, values
):
    inputs, _, _, native, encoded = values
    source = MutablePolicySource(
        policy_context(
            legacy_capture_binding(inputs.session), expires_at_ns=1001
        )
    )
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    real_sync = storage.os.fsync

    def expire_after_sync(fd):
        real_sync(fd)
        clock[0] = 1001

    monkeypatch.setattr(storage.os, "fsync", expire_after_sync)
    with provider_policy_scope(source), pytest.raises(BrokerPolicyError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
    assert list(tmp_path.iterdir()) == []


def test_sidecar_replacement_is_explicit_and_preserves_unrelated_file(
    tmp_path, values
):
    _, _, source, native, encoded = values
    path = tmp_path / "event.provider-policy.json"
    with provider_policy_scope(source):
        write_broker_policy_receipt(
            path,
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
        )
        original = path.read_bytes()
        with pytest.raises(ValueError):
            write_broker_policy_receipt(
                path,
                native,
                native_artifact_name="event.json",
                native_file_bytes=encoded,
            )
        assert path.read_bytes() == original
        with pytest.raises(ValueError):
            write_broker_policy_receipt(
                path,
                native,
                native_artifact_name="different.json",
                native_file_bytes=encoded,
                replace_existing=True,
            )
        assert path.read_bytes() == original
        write_broker_policy_receipt(
            path,
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
            replace_existing=True,
        )
        path.write_bytes(b"unrelated user file")
        with pytest.raises(ValueError):
            write_broker_policy_receipt(
                path,
                native,
                native_artifact_name="event.json",
                native_file_bytes=encoded,
                replace_existing=True,
            )
        assert path.read_bytes() == b"unrelated user file"


def test_native_input_roots_are_explicit_detached_and_not_permission(values):
    inputs, _, _, native, _ = values
    request = BrokerLegacyCaptureV1(inputs.session, legacy_output_contract())
    with pytest.raises(ValueError, match="native inputs"):
        capture_request_for(inputs.session)
    with provider_native_inputs(request):
        restored = capture_request_for(inputs.session)
        assert restored.session.to_json() == inputs.session.to_json()
        assert restored.session is not inputs.session
        with pytest.raises(BrokerPolicyError):
            scope.require_provider_operation(
                native, BrokerPolicyOperation.RETAIN_LOCAL
            )
        with (
            pytest.raises(ValueError, match="nested"),
            provider_native_inputs(request),
        ):
            pass
        with pytest.raises(ValueError, match="absent or differs"):
            capture_request_for(
                replace(inputs.session, server_id="foreign", session_id="")
            )
    with pytest.raises(ValueError), provider_native_inputs(request, request):
        pass


def test_native_input_scope_refuses_fork_and_unknown_root(monkeypatch, values):
    inputs, _, _, _, _ = values
    request = BrokerLegacyCaptureV1(inputs.session, legacy_output_contract())
    from histdatacom.broker_plugin_policy import native_inputs

    with provider_native_inputs(request):
        original = os.getpid()
        monkeypatch.setattr(native_inputs.os, "getpid", lambda: original + 1)
        with pytest.raises(ValueError, match="current-process"):
            capture_request_for(inputs.session)
    with (
        pytest.raises(ValueError, match="unsupported"),
        provider_native_inputs({"provider_id": "fixture.provider-policy"}),
    ):
        pass


@pytest.mark.parametrize("refuse_call", [1, 2])
def test_private_input_guard_sees_full_receipt_and_refuses_without_leaking(
    tmp_path, values, refuse_call
):
    _, context, source, native, encoded = values
    seen = []

    def reject_private_text(text):
        seen.append(text)
        if len(seen) == refuse_call:
            raise RuntimeError("synthetic-private-text-must-not-leak")

    with (
        provider_policy_scope(source),
        pytest.raises(ValueError, match="private-input check") as failure,
    ):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name="event.json",
            native_file_bytes=encoded,
            before_persist=reject_private_text,
        )
    assert len(seen) == refuse_call
    assert all(text == seen[0] for text in seen)
    restored = BrokerPolicyArtifactReceiptV1.from_json(seen[0])
    assert restored.operation_admission.context == context
    assert restored.retention_admission.context == context
    assert "synthetic-private-text" not in str(failure.value)
    assert failure.value.__suppress_context__
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "name",
    [
        "\x00event.json",
        "event\n.json",
        " event.json",
        "event.json ",
        "event\x7f.json",
    ],
)
def test_receipt_refuses_ambiguous_native_basename(tmp_path, values, name):
    _, _, source, native, encoded = values
    with provider_policy_scope(source), pytest.raises(ValueError):
        write_broker_policy_receipt(
            tmp_path / "event.provider-policy.json",
            native,
            native_artifact_name=name,
            native_file_bytes=encoded,
        )
    assert list(tmp_path.iterdir()) == []
