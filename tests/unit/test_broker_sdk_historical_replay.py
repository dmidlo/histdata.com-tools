"""Exact historical-helper assertions; synthetic contracts, no native replay."""

from dataclasses import fields, replace
import hashlib
import stat
import sys
from types import ModuleType

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    provider_policy_scope,
    require_provider_operation,
    sdk_invocation_binding,
)
from histdatacom.broker_plugin_policy import scope
from histdatacom.broker_plugin_policy.bindings import resolve_provider_subject
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyOperation,
    BrokerPolicyRequestV1,
    BrokerPolicyRevocationV1,
    BrokerPolicyStatus,
)
from histdatacom.broker_plugin_policy.decisions import decide_provider_operation
from tests.fixtures import broker_sdk_historical_replay as helper
from tests.fixtures.broker_provider_policy import sdk_policy_invocation


def _decision(*, revoked=True, allowed=True, operation=None):
    invocation = sdk_policy_invocation()
    context = helper.policy_context(
        sdk_invocation_binding(invocation),
        status=(
            BrokerPolicyStatus.ALLOWED if allowed else BrokerPolicyStatus.DENIED
        ),
    )
    if revoked:
        context, _ = helper.revoke_selected_policy(context, at_ns=1000)
    return decide_provider_operation(
        context,
        BrokerPolicyRequestV1(
            resolve_provider_subject(invocation),
            operation or BrokerPolicyOperation.MATERIAL_USE,
            1000,
        ),
    )


def test_same_selection_revocation_is_reread_and_exactly_denied(monkeypatch):
    # Synthetic policy-clock input only; main() continues using real time_ns().
    monkeypatch.setattr(scope, "_now_ns", lambda: 1000)
    invocation = sdk_policy_invocation()
    current = helper.policy_context(sdk_invocation_binding(invocation))
    source = helper.MutablePolicySource(current)
    with provider_policy_scope(source):
        assert require_provider_operation(
            invocation, BrokerPolicyOperation.MATERIAL_USE
        ).allowed
        reads_before = source.reads
        source.current, revoked = helper.revoke_selected_policy(
            current, at_ns=1000
        )
        assert source.current.selected_policy_ids == current.selected_policy_ids
        assert source.current.policies == current.policies
        assert source.current.execution == current.execution
        with pytest.raises(BrokerPolicyError) as caught:
            require_provider_operation(
                invocation, BrokerPolicyOperation.MATERIAL_USE
            )
        helper.assert_revoked_denial(caught.value)
        assert source.reads > reads_before
        assert revoked.target_id == current.selected_policy_ids[0]


def test_revocation_builder_preserves_prior_ledger_and_selected_not_first():
    invocation = sdk_policy_invocation()
    original = helper.policy_context(sdk_invocation_binding(invocation))
    old = original.policies[0]
    successor = replace(old, version="1.0.1", predecessor_id=old.artifact_id)
    policies = tuple(
        sorted((old, successor), key=lambda value: value.artifact_id)
    )
    prior = BrokerPolicyRevocationV1(
        original.acknowledgements[0].artifact_id,
        900,
        2000,
        old.evidence_ids,
        "review_invalidated",
    )
    current = replace(
        original,
        policies=policies,
        selected_policy_ids=(policies[-1].artifact_id,),
        revocations=(prior,),
    )
    updated, revoked = helper.revoke_selected_policy(current, at_ns=1000)
    assert revoked.target_id == policies[-1].artifact_id
    assert revoked.target_id != policies[0].artifact_id
    assert revoked.recorded_at_ns == revoked.effective_at_ns == 1000
    assert set(updated.revocations) == {prior, revoked}
    assert updated.revocations == tuple(
        sorted(updated.revocations, key=lambda value: value.artifact_id)
    )
    for field in fields(current):
        if field.name != "revocations":
            assert getattr(updated, field.name) == getattr(current, field.name)
    assert current.revocations == (prior,)


@pytest.mark.parametrize(
    "reason",
    (
        "current_clock_regressed",
        "selected_policy_or_execution_context_changed",
        "invalid_current_policy_source",
    ),
)
def test_other_refusal_reasons_are_not_revocation_success(reason):
    with pytest.raises(AssertionError):
        helper.assert_revoked_denial(BrokerPolicyError(reason, _decision()))


@pytest.mark.parametrize("kind", ("missing", "allowed", "not-revoked"))
def test_missing_allowed_or_nonrevoked_decision_is_not_success(kind):
    decision = (
        None
        if kind == "missing"
        else _decision(revoked=False, allowed=kind == "allowed")
    )
    with pytest.raises(AssertionError):
        helper.assert_revoked_denial(
            BrokerPolicyError("operation_not_allowed", decision)
        )


@pytest.mark.parametrize(
    "operation", (BrokerPolicyOperation.CAPTURE, BrokerPolicyOperation.INVOKE)
)
def test_wrong_denied_operation_is_not_material_use_revocation(operation):
    with pytest.raises(AssertionError):
        helper.assert_revoked_denial(
            BrokerPolicyError(
                "operation_not_allowed", _decision(operation=operation)
            )
        )


def test_subtyped_and_unrelated_errors_are_not_revocation_success():
    class DerivedPolicyError(BrokerPolicyError):
        pass

    for error in (
        DerivedPolicyError("operation_not_allowed", _decision()),
        ValueError("provider policy refused: operation_not_allowed"),
    ):
        with pytest.raises(AssertionError):
            helper.assert_revoked_denial(error)


@pytest.mark.parametrize(
    "present", ("none", "distribution", "module", "loaded")
)
def test_selected_distribution_and_entry_module_must_both_be_absent(
    monkeypatch, present
):
    module = "histdatacom_generated_historical_absence_canary"

    def distribution(name):
        assert name == "histdatacom-generated-historical-absence-canary"
        if present == "distribution":
            return object()
        raise helper.metadata.PackageNotFoundError(name)

    def find_spec(name):
        assert name == module
        return object() if present == "module" else None

    monkeypatch.setattr(helper.metadata, "distribution", distribution)
    monkeypatch.setattr(helper.util, "find_spec", find_spec)
    if present == "loaded":
        monkeypatch.setitem(
            sys.modules, module + ".child", ModuleType(module + ".child")
        )
    if present == "none":
        helper.assert_plugin_absent(
            "histdatacom-generated-historical-absence-canary", module
        )
    else:
        with pytest.raises(AssertionError):
            helper.assert_plugin_absent(
                "histdatacom-generated-historical-absence-canary", module
            )


@pytest.mark.parametrize("mutation", ("bytes", "mode", "added", "removed"))
def test_all_retained_input_bytes_modes_and_inventory_are_observed(
    tmp_path, mutation
):
    source = tmp_path / "source"
    source.mkdir()
    first, other = source / "manifest.json", source / "other.json"
    first.write_bytes(b"generated A")
    other.write_bytes(b"generated B")
    before = helper.retained_input_files(source)
    assert before["other.json"] == (
        hashlib.sha256(b"generated B").hexdigest(),
        len(b"generated B"),
        stat.S_IMODE(other.stat().st_mode),
    )
    if mutation == "bytes":
        other.write_bytes(b"generated C")
    elif mutation == "mode":
        other.chmod(stat.S_IMODE(other.stat().st_mode) ^ stat.S_IXUSR)
    elif mutation == "added":
        (source / "extra.json").write_bytes(b"generated C")
    else:
        other.unlink()
    assert helper.retained_input_files(source) != before


def test_retained_input_symlink_is_refused_before_target_read(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "outside.json"
    target.write_bytes(b"outside generated input")
    (source / "linked.json").symlink_to(target)
    with pytest.raises(AssertionError):
        helper.retained_input_files(source)
