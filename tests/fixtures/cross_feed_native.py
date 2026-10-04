"""Real, tiny native captures of invented feeds; no verifier success doubles.

The SDK wheel is the existing offline health fixture. It is unpacked only into
a new pip-free environment, never installed into the test interpreter. Host
observations, lifecycle records and provenance are produced by the real host.
Known source-time correspondences are fixture truth, not real-feed truth.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from importlib import invalidate_caches
from pathlib import Path
import subprocess
import sys
from typing import Any
import venv
from zipfile import ZipFile

from histdatacom.broker_capture import (
    BrokerAdapterMessageV1,
    BrokerCaptureEventKind,
    BrokerCapturePriceTextSemantics,
    BrokerCaptureReplaySourceV1,
    BrokerCaptureSourceTimestampSemantics,
    SequenceBrokerCaptureAdapterV1,
)
from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
from histdatacom.broker_plugin_health.runtime_legacy import (
    capture_legacy_with_host_health,
)
from histdatacom.broker_plugin_policy.bindings import (
    resolve_provider_subject,
)
from histdatacom.broker_plugin_policy.contracts import BrokerPolicyContextV1
from histdatacom.broker_plugin_policy.scope import provider_policy_scope
from tests.fixtures.broker_host_health import SyntheticHostHealthClock
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_legacy_request,
    generated_sdk_request,
    legacy_policy_inputs,
    policy_context,
)

SECOND = 1_000_000_000
SHIFT = 12_000_000


@dataclass(frozen=True)
class NativeFixture:
    directory: Path
    request: Any
    result: Any
    family: str
    authority: Any = None
    resources: Any = None

    @property
    def health(self):
        return (
            self.result.audit if self.family == "legacy" else self.result.health
        )

    @property
    def seal(self):
        return self.result.provenance

    def reference(self):
        from histdatacom.cross_feed.native import NativeCaptureRefV1

        return NativeCaptureRefV1(
            self.directory,
            self.result.manifest,
            self.request,
            self.seal,
            self.health.artifact_id,
        )


@contextmanager
def capture_scope(*captures: NativeFixture, additional_subjects=()):
    """Explicit generated terms and actual wheel permissions, freshly read.

    Returned mutable policy source permits genuine revocation controls. These
    fixture declarations assert no real provider rights or source authenticity.
    """
    subjects = tuple(capture.request for capture in captures) + tuple(
        additional_subjects
    )
    bindings = {}
    for subject in subjects:
        for binding in resolve_provider_subject(subject).bindings:
            bindings[binding.artifact_id] = binding
    contexts = tuple(policy_context(bindings[key]) for key in sorted(bindings))
    if not contexts:
        raise ValueError("explicit native fixture subject required")

    def union(field):
        entries = {
            value.artifact_id: value
            for context in contexts
            for value in getattr(context, field)
        }
        return tuple(entries[key] for key in sorted(entries))

    source = MutablePolicySource(
        BrokerPolicyContextV1(
            union("policies"),
            union("evidence"),
            union("acknowledgements"),
            (),
            tuple(
                sorted(
                    identity
                    for context in contexts
                    for identity in context.selected_policy_ids
                )
            ),
            contexts[0].execution,
        )
    )
    sdk = tuple(capture for capture in captures if capture.family == "sdk")
    if len(sdk) > 1:
        raise ValueError("fixture scope supports one actual SDK authority")
    with ExitStack() as stack:
        stack.enter_context(provider_policy_scope(source))
        if sdk:
            from histdatacom.broker_plugin_permissions import (
                broker_permission_scope,
            )

            stack.enter_context(
                broker_permission_scope(
                    sdk[0].authority, resources=sdk[0].resources
                )
            )
        yield source


def build_legacy_capture(
    directory: Path,
    *,
    source_times: tuple[int, ...] = tuple(
        SECOND * n + SHIFT for n in (1, 2, 3)
    ),
    prices: tuple[tuple[str, str], ...] | None = None,
    label: str = "fixed-shift",
    precision_ns: int = 1,
    clock=None,
    fsync_each_event: bool = False,
) -> NativeFixture:
    """Record generated adapter ingress through the real durability pipeline."""
    import hashlib

    inputs = legacy_policy_inputs()
    session = replace(
        inputs.session,
        adapter_config_sha256=hashlib.sha256(label.encode("ascii")).hexdigest(),
        session_id="",
    )
    request = generated_legacy_request(session)
    if prices is None:
        prices = tuple(
            (f"1.100{n}", f"1.200{n}") for n in range(len(source_times))
        )
    if len(prices) != len(source_times):
        raise ValueError("fixture quote/time cardinality differs")
    quotes = tuple(
        BrokerAdapterMessageV1(
            kind=BrokerCaptureEventKind.QUOTE,
            connection_id="connection-1",
            source_event_time_ns=timestamp,
            source_timestamp_semantics=BrokerCaptureSourceTimestampSemantics.BROKER_EVENT,
            source_timestamp_precision_ns=precision_ns,
            source_sequence=index,
            source_message_id=f"{label}-quote-{index}",
            source_batch_id=f"{label}-batch-{index // 2}",
            symbol="EURUSD",
            bid=float(bid),
            ask=float(ask),
            bid_text=bid,
            ask_text=ask,
            price_text_semantics=BrokerCapturePriceTextSemantics.SOURCE_LEXEME,
        )
        for index, (timestamp, (bid, ask)) in enumerate(
            zip(source_times, prices)
        )
    )
    adapter = SequenceBrokerCaptureAdapterV1(
        session.adapter_id,
        session.adapter_version,
        inputs.messages[:3] + quotes + inputs.messages[-1:],
    )
    pending = NativeFixture(directory, request, None, "legacy")
    with capture_scope(pending):
        result = capture_legacy_with_host_health(
            directory,
            provider_request=request,
            adapter=adapter,
            storage_policy=replace(
                inputs.storage_policy,
                fsync_each_event=fsync_each_event,
                policy_id=None,
            ),
            symbols=("EURUSD",),
            clock=clock or SyntheticHostHealthClock(session),
        )
    return replace(pending, result=result)


def legacy_quotes(capture: NativeFixture):
    with capture_scope(capture):
        return tuple(
            event
            for event in BrokerCaptureReplaySourceV1(
                capture.directory,
                capture.result.manifest,
                provider_request=capture.request,
            ).iter_events()
            if event.kind is BrokerCaptureEventKind.QUOTE
        )


@contextmanager
def installed_sdk_capture(directory: Path, *, mode: str = "honest"):
    """Use an actual wheel and worker in a new isolated fixture environment."""
    from histdatacom.broker_plugin_capabilities import (
        BrokerCapabilityWorkflowV1,
        negotiate_broker_capabilities,
    )
    from histdatacom.broker_plugin_lifecycle import (
        BrokerLifecycleCompletion,
        BrokerLifecyclePolicyV1,
        run_broker_plugin_lifecycle,
    )
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionAuthorityV1,
        BrokerPermissionBindingV1,
        BrokerPermissionContextV1,
        BrokerPermissionGrantV1,
        BrokerPermissionResourcesV1,
        read_installed_broker_permissions,
    )
    from histdatacom.broker_plugin_registry import discover_broker_plugins
    from tests.fixtures.broker_host_health_wheel import build_host_health_wheel
    from tests.fixtures.broker_permission_wheel import permission_fixture_schema

    environment = directory / "isolated-worker"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    python = environment / "bin/python"
    site = Path(
        subprocess.check_output(
            [
                str(python),
                "-B",
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ],
            text=True,
        ).strip()
    )
    wheel = build_host_health_wheel(directory / "wheel")
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    # Discovery reads real distribution/RECORD bytes. No metadata or verifier
    # monkeypatch is involved; the worker independently imports its installation.
    sys.path.insert(0, str(site))
    invalidate_caches()
    try:
        inventory = discover_broker_plugins()
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(
                tuple(
                    sorted(
                        (
                            "configuration_schema",
                            "open_session",
                            "instruments",
                            "subscribe",
                            "iter_events",
                            "unsubscribe",
                        )
                    )
                ),
                optional=(
                    "gaps.v1",
                    "heartbeat.v1",
                    "timestamps.broker-event.v1",
                ),
            ),
            plugin_id="org.example.permissions",
        )
        configuration = {"mode": mode}
        request = generated_sdk_request(
            plan,
            schema=permission_fixture_schema(),
            public_configuration=configuration,
        )
        manifest = read_installed_broker_permissions(plan.candidate)
        binding = BrokerPermissionBindingV1(
            plan.candidate.artifact_id,
            manifest.artifact_id,
            "1.0.0",
            "fixture",
            request.configuration_profile.artifact_id,
        )
        grant = BrokerPermissionGrantV1(
            binding,
            manifest.declared_atoms,
            "synthetic-cross-feed-operator",
            0,
            2**63 - 1,
            "c" * 32,
        )

        class Grants:
            def read_context(self):
                return BrokerPermissionContextV1((grant,))

        authority = BrokerPermissionAuthorityV1(
            manifest, binding, grant.artifact_id, Grants()
        )
        resources = BrokerPermissionResourcesV1(
            authority, provider_request=request
        )
        pending = NativeFixture(
            directory / "capture", request, None, "sdk", authority, resources
        )
        with capture_scope(pending):
            result = run_broker_plugin_lifecycle(
                inventory,
                plan,
                configuration,
                ("EURUSD",),
                pending.directory,
                authorize=lambda _: True,
                provider_request=request,
                worker_python=str(python),
                policy=BrokerLifecyclePolicyV1(
                    startup_timeout_ms=15000,
                    run_timeout_ms=120000,
                    # Fixed offline validation allowance: the floor runtime's
                    # genuine durable admission exceeded the former 5s ACK
                    # budget and retried. Keep all health expectations intact;
                    # this is not a clock-quality or loss-policy relaxation.
                    acknowledgement_timeout_ms=10000,
                ),
                health_policy=BrokerHostHealthPolicyV1(
                    bucket_width_ns=60_000_000_000,
                    max_clock_jump_ns=60_000_000_000,
                    max_persistence_p95_ns=60_000_000_000,
                    require_known_upstream_loss=True,
                ),
            )
        if (
            not result.manifest.worker_reaped
            or result.manifest.completion
            is not BrokerLifecycleCompletion.COMPLETE
        ):
            raise AssertionError(
                f"actual generated SDK capture did not complete: {result.reason}"
            )
        yield replace(pending, result=result)
    finally:
        sys.path.remove(str(site))
        invalidate_caches()
