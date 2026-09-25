"""Process-local freshness tests; native resolver is explicitly instrumented.

The resolver stand-in tests only core orchestration. Independent integration
tests own genuine closed native classification and provider effect boundaries.
"""

from dataclasses import replace
import os
import sys
from types import ModuleType

import pytest

from histdatacom.broker_plugin_policy import scope
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyContextV1,
    BrokerPolicyOperation,
    BrokerPolicyRevocationV1,
)
from tests.fixtures.broker_policy_core import (
    CoreMutableSource,
    core_context,
    core_subject,
)


@pytest.fixture
def instrumented_native_resolver(monkeypatch):
    # Not a runtime escape hatch: this temporary module exists only in tests.
    native = object()
    context = core_context()
    calls = []
    module = ModuleType("histdatacom.broker_plugin_policy.bindings")

    def resolve(value):
        calls.append(value)
        if value is not native:
            raise ValueError("unsupported synthetic stand-in")
        return core_subject(context)

    module.resolve_provider_subject = resolve
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(scope, "_now_ns", lambda: 10)
    return native, CoreMutableSource(context), calls


def test_guard_reads_current_context_each_time_and_returns_replayed_decision(
    instrumented_native_resolver,
):
    native, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        assert source.reads == 1
        first = scope.require_provider_operation(
            native, BrokerPolicyOperation.MATERIAL_USE
        )
        second = scope.require_provider_operation(
            native, BrokerPolicyOperation.MATERIAL_USE
        )
        assert source.reads == 3
        assert first == second
        assert first.context is not source.context
        assert first.allowed
    assert calls == [native, native]
    with pytest.raises(scope.BrokerPolicyError, match="current_process"):
        scope.require_provider_operation(
            native, BrokerPolicyOperation.MATERIAL_USE
        )


def test_parent_ipc_context_helper_is_fresh_detached_but_grants_no_operation(
    instrumented_native_resolver,
):
    _, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        first = scope.read_current_provider_policy_context()
        assert first == source.context and first is not source.context
        assert source.reads == 2
        ack = replace(source.context.acknowledgements[0], acknowledged_at_ns=4)
        source.context = replace(
            source.context,
            acknowledgements=tuple(
                sorted(
                    source.context.acknowledgements + (ack,),
                    key=lambda a: a.artifact_id,
                )
            ),
        )
        second = scope.read_current_provider_policy_context()
        assert len(second.acknowledgements) == 2
        assert source.reads == 3
        assert calls == []
    with pytest.raises(scope.BrokerPolicyError, match="current_process"):
        scope.read_current_provider_policy_context()


def test_revocation_between_operations_refuses_before_second_native_effect(
    instrumented_native_resolver,
):
    native, source, _ = instrumented_native_resolver
    effects = []
    with scope.provider_policy_scope(source):
        scope.require_provider_operation(native, BrokerPolicyOperation.CAPTURE)
        effects.append("first admitted operation")
        context = source.context
        source.context = replace(
            context,
            revocations=(
                BrokerPolicyRevocationV1(
                    context.policies[0].artifact_id,
                    10,
                    10,
                    context.policies[0].evidence_ids,
                    "withdrawn",
                ),
            ),
        )
        with pytest.raises(
            scope.BrokerPolicyError, match="operation_not_allowed"
        ) as captured:
            scope.require_provider_operation(
                native, BrokerPolicyOperation.CAPTURE
            )
            effects.append("must not happen")
        assert captured.value.decision is not None
        assert not captured.value.decision.allowed
    assert effects == ["first admitted operation"]


@pytest.mark.parametrize(
    "mutation", ["policy", "execution", "ack_removal", "ack_rewrite"]
)
def test_pinned_selection_and_append_only_review_inventory(
    instrumented_native_resolver, mutation
):
    _, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        scope.read_current_provider_policy_context()
        context = source.context
        if mutation == "policy":
            policy = replace(context.policies[0], version="1.1.0")
            source.context = replace(
                context,
                policies=(policy,),
                acknowledgements=(),
                selected_policy_ids=(policy.artifact_id,),
            )
        elif mutation == "execution":
            source.context = replace(
                context,
                execution=replace(context.execution, geography="region-b"),
            )
        elif mutation == "ack_removal":
            source.context = replace(context, acknowledgements=())
        else:
            source.context = replace(
                context,
                acknowledgements=(
                    replace(context.acknowledgements[0], expires_at_ns=101),
                ),
            )
        with pytest.raises(
            scope.BrokerPolicyError, match="changed|removed_or_rewritten"
        ):
            scope.read_current_provider_policy_context()
    assert calls == []


def test_rollback_of_newly_observed_append_also_refuses(
    instrumented_native_resolver,
):
    _, source, _ = instrumented_native_resolver
    original = source.context
    with scope.provider_policy_scope(source):
        extra = replace(
            original.evidence[0],
            reference=replace(
                original.evidence[0].reference,
                native_id="another-generated-evidence",
            ),
        )
        source.context = replace(
            original,
            evidence=tuple(
                sorted(
                    original.evidence + (extra,), key=lambda e: e.artifact_id
                )
            ),
        )
        scope.read_current_provider_policy_context()
        source.context = original
        with pytest.raises(
            scope.BrokerPolicyError, match="removed_or_rewritten"
        ):
            scope.read_current_provider_policy_context()


@pytest.mark.parametrize("clock", [True, -1, 2**63, 9])
def test_current_clock_invalid_or_regressed_before_native_resolution(
    instrumented_native_resolver, monkeypatch, clock
):
    native, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        scope.read_current_provider_policy_context()
        monkeypatch.setattr(scope, "_now_ns", lambda: clock)
        with pytest.raises(scope.BrokerPolicyError, match="clock"):
            scope.require_provider_operation(
                native, BrokerPolicyOperation.INVOKE
            )
    assert calls == []


def test_clock_rechecked_after_native_resolution(
    instrumented_native_resolver, monkeypatch
):
    native, source, _ = instrumented_native_resolver
    clocks = iter((99, 100))
    monkeypatch.setattr(scope, "_now_ns", lambda: next(clocks))
    with scope.provider_policy_scope(source):
        with pytest.raises(
            scope.BrokerPolicyError, match="operation_not_allowed"
        ) as captured:
            scope.require_provider_operation(
                native, BrokerPolicyOperation.INVOKE
            )
    assert captured.value.decision.request.decision_at_ns == 100
    assert "policy_expired" in captured.value.decision.cells[0].reasons


def test_inherited_pid_is_not_a_new_process_scope(
    instrumented_native_resolver, monkeypatch
):
    native, source, calls = instrumented_native_resolver
    actual_pid = os.getpid()
    with scope.provider_policy_scope(source):
        monkeypatch.setattr(scope.os, "getpid", lambda: actual_pid + 1)
        with pytest.raises(scope.BrokerPolicyError, match="current_process"):
            scope.require_provider_operation(
                native, BrokerPolicyOperation.INVOKE
            )
        with pytest.raises(scope.BrokerPolicyError, match="current_process"):
            scope.read_current_provider_policy_context()
    assert calls == []


def test_nested_and_reentrant_scopes_refuse_without_deadlock(
    instrumented_native_resolver,
):
    native, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        with pytest.raises(scope.BrokerPolicyError, match="nested"):
            with scope.provider_policy_scope(source):
                pytest.fail("nested scope must not replace selection")
        state = scope._CURRENT.get()
        with state.lock:
            with pytest.raises(
                scope.BrokerPolicyError, match="concurrent_or_reentrant"
            ):
                scope.require_provider_operation(
                    native, BrokerPolicyOperation.INVOKE
                )
        assert scope.read_current_provider_policy_context() == source.context
    assert calls == []


@pytest.mark.parametrize(
    "failure", ["secret_exception", "system_exit", "wrong_type", "mutated"]
)
def test_source_failures_and_deep_mutation_are_closed_and_redacted(
    instrumented_native_resolver, failure
):
    native, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):

        def read():
            if failure == "secret_exception":
                raise RuntimeError("password=do-not-include-this")
            if failure == "system_exit":
                raise SystemExit("password=do-not-include-this")
            if failure == "wrong_type":
                return True
            # Simulate hostile mutation, not a supported public API operation.
            object.__setattr__(
                source.context.policies[0], "declared_at_ns", True
            )
            return source.context

        source.read_policy_context = read
        with pytest.raises(
            scope.BrokerPolicyError, match="invalid_current_policy_source"
        ) as captured:
            scope.require_provider_operation(
                native, BrokerPolicyOperation.INVOKE
            )
        assert "password" not in str(captured.value)
        assert captured.value.__suppress_context__
    assert calls == []


def test_pure_subject_is_not_a_host_resolver_result(
    instrumented_native_resolver,
):
    _, source, calls = instrumented_native_resolver
    with scope.provider_policy_scope(source):
        with pytest.raises(
            scope.BrokerPolicyError, match="native_subject_required"
        ):
            scope.require_provider_operation(
                core_subject(source.context), BrokerPolicyOperation.INVOKE
            )
    assert calls == []


def test_exception_restores_scope_and_each_later_scope_revalidates(
    instrumented_native_resolver,
):
    _, source, _ = instrumented_native_resolver
    with pytest.raises(RuntimeError, match="generated error"):
        with scope.provider_policy_scope(source):
            raise RuntimeError("generated error")
    assert scope._CURRENT.get() is None
    with scope.provider_policy_scope(source):
        assert (
            BrokerPolicyContextV1.from_json(
                scope.read_current_provider_policy_context().to_json()
            )
            == source.context
        )
    assert source.reads == 3
