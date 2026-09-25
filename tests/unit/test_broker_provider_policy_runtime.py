"""Fresh parent-ledger IPC and runtime-only authorization boundary canaries."""

import os
import time
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_lifecycle import worker
from histdatacom.broker_plugin_lifecycle.contracts import BrokerLifecycleError
from tests.fixtures.broker_policy_core import core_context
from tests.fixtures.broker_runtime_policy import runtime_request, runtime_scope
from tests.unit.test_broker_plugin_lifecycle import (
    installed as installed,  # noqa: PLC0414 - deliberate pytest fixture export
)
from tests.unit.test_broker_plugin_lifecycle import (
    request_data as request_data,  # noqa: PLC0414 - pytest fixture export
)


def test_lifecycle_replay_canonicalizes_parent_but_not_receipt_leaf(
    request_data, tmp_path
):
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleCompletion,
    )
    from tests.unit.test_broker_plugin_lifecycle import (
        replay_broker_lifecycle,
        run,
    )

    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)
    result = run(request_data, alias)
    assert result.manifest.completion is BrokerLifecycleCompletion.COMPLETE
    records = tuple(replay_broker_lifecycle(result.directory))
    assert len(records) == result.manifest.appended_records
    assert sum(record.kind == "event" for record in records) == 2

    receipt = physical / "run" / "manifest.json.provider-policy.json"
    original = tmp_path / "original-receipt.json"
    receipt.rename(original)
    receipt.symlink_to(original)
    with pytest.raises(BrokerLifecycleError, match="integrity_failure"):
        tuple(replay_broker_lifecycle(result.directory))


def _source(monkeypatch, replies):
    controls = worker.Controls()
    emitted = []
    monkeypatch.setattr(controls, "receive", lambda timeout: replies.pop(0))
    monkeypatch.setattr(
        worker,
        "write_frame",
        lambda descriptor, frame, maximum: emitted.append(frame),
    )
    source = worker._PolicySource(controls, 10, "invocation", 2, 524_288, 1)
    return source, controls, emitted


def _reply(context=None, **changes):
    return {
        "type": "policy_context",
        "invocation_id": "invocation",
        "epoch": 2,
        "sequence": 0,
        "context": (context or core_context()).to_json(),
        **changes,
    }


def test_each_read_requires_a_new_exact_parent_response(monkeypatch):
    first = core_context()
    second = replace(
        first, execution=replace(first.execution, commercial_use=True)
    )
    source, _, emitted = _source(
        monkeypatch, [_reply(first), _reply(second, sequence=1)]
    )
    assert source.read_policy_context() == first
    assert source.read_policy_context() == second
    assert [frame["sequence"] for frame in emitted] == [0, 1]
    assert all(
        frame["epoch"] == 2 and frame["invocation_id"] == "invocation"
        for frame in emitted
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"invocation_id": "other"},
        {"epoch": 1},
        {"epoch": True},
        {"sequence": 1},
        {"sequence": False},
        {"extra": None},
        {"type": "permit"},
        {"context": "{}"},
        {"context": None},
    ],
)
def test_context_rpc_refuses_mismatched_or_malformed_reply(
    monkeypatch, changes
):
    source, _, _ = _source(monkeypatch, [{**_reply(), **changes}])
    with pytest.raises(ValueError):
        source.read_policy_context()
    assert source.sequence == 0


@pytest.mark.parametrize("reply", [None, {"type": "stop"}])
def test_context_rpc_timeout_or_stop_never_reuses_a_context(monkeypatch, reply):
    source, _, emitted = _source(monkeypatch, [_reply(), reply])
    source.read_policy_context()
    with pytest.raises(BrokerLifecycleError):
        source.read_policy_context()
    assert len(emitted) == 2


def test_only_acknowledged_old_deliveries_can_precede_policy_reply(monkeypatch):
    source, controls, _ = _source(
        monkeypatch,
        [
            {"type": "ack", "delivery": 0},
            _reply(),
            {"type": "ack", "delivery": 1},
        ],
    )
    controls.acknowledged_delivery = 0
    assert source.read_policy_context() == core_context()
    with pytest.raises(BrokerLifecycleError):
        source.read_policy_context()


def test_noncanonical_context_is_not_an_accepted_parent_ledger(monkeypatch):
    reply = _reply()
    reply["context"] += " "
    source, _, _ = _source(monkeypatch, [reply])
    with pytest.raises(ValueError):
        source.read_policy_context()


@pytest.mark.parametrize(
    "changes",
    [
        {"invocation_id": "different-invocation"},
        {"epoch": True},
        {"epoch": 1},
        {"sequence": False},
        {"sequence": 1},
        {"extra": "not-an-authority"},
    ],
)
def test_parent_rejects_unbound_context_request_before_identity(
    request_data, tmp_path, monkeypatch, changes
):
    from histdatacom.broker_plugin_lifecycle import supervisor
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleCompletion,
        BrokerLifecycleReason,
    )
    from tests.unit.test_broker_plugin_lifecycle import (
        replay_broker_lifecycle,
        run,
    )

    decoder = supervisor.FrameDecoder
    altered = []

    class UnboundDecoder(decoder):
        def feed(self, data):
            for frame, size in super().feed(data):
                if frame.get("type") == "policy_context_request":
                    assert not altered
                    altered.append(True)
                    frame = {**frame, **changes}
                yield frame, size

    monkeypatch.setattr(supervisor, "FrameDecoder", UnboundDecoder)
    result = run(request_data, tmp_path)
    assert altered == [True]
    assert result.reason is BrokerLifecycleReason.MALFORMED_IPC
    assert result.manifest.completion is BrokerLifecycleCompletion.PARTIAL
    assert result.manifest.worker_reaped
    assert result.manifest.appended_events == 0
    assert all(
        record.kind == "transition"
        for record in replay_broker_lifecycle(result.directory)
    )


def test_real_worker_rereads_parent_revocation_before_another_event(
    request_data,
    tmp_path,
    monkeypatch,
):
    from histdatacom.broker_plugin_lifecycle import (
        inspect_broker_lifecycle,
        supervisor,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleCompletion,
        BrokerLifecyclePolicyV1,
    )
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyRevocationV1,
    )
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError

    python, inventory, plan = request_data
    configuration = {"mode": "finite"}
    request = runtime_request(plan, configuration)
    output = tmp_path / "revoked"
    delivered = []
    processes = []
    original_append = supervisor._Run.append
    original_popen = supervisor.subprocess.Popen

    def record_process(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        processes.append(process)
        return process

    with runtime_scope(request) as source:

        def revoke_after_first(
            self, kind, payload, delivery=None, *, ingress_sequence=None
        ):
            original_append(
                self,
                kind,
                payload,
                delivery,
                ingress_sequence=ingress_sequence,
            )
            if kind == "event":
                delivered.append(delivery)
                assert len(delivered) == 1
                context = source.current
                now = time.time_ns()
                source.current = replace(
                    context,
                    revocations=(
                        BrokerPolicyRevocationV1(
                            context.policies[0].artifact_id,
                            now,
                            now,
                            context.policies[0].evidence_ids,
                            "withdrawn",
                        ),
                    ),
                )

        monkeypatch.setattr(supervisor._Run, "append", revoke_after_first)
        monkeypatch.setattr(supervisor.subprocess, "Popen", record_process)
        with pytest.raises(BrokerPolicyError):
            supervisor.run_broker_plugin_lifecycle(
                inventory,
                plan,
                configuration,
                ("EURUSD",),
                output,
                authorize=lambda _: True,
                provider_request=request,
                worker_python=str(python),
                policy=BrokerLifecyclePolicyV1(
                    startup_timeout_ms=15000,
                    run_timeout_ms=30000,
                    acknowledgement_timeout_ms=5000,
                    shutdown_timeout_ms=200,
                ),
            )
        assert source.reads > 10
    assert delivered == [0]
    assert (
        inspect_broker_lifecycle(output).manifest.completion
        is BrokerLifecycleCompletion.OPEN
    )
    assert len(processes) == 1 and processes[0].poll() is not None
    with pytest.raises(ChildProcessError):
        os.waitpid(processes[0].pid, os.WNOHANG)
