# Broker-plugin lifecycle and durable native capture

Issue #618 adds a versioned, caller-authorized host lifecycle on top of the
unchanged SDK, metadata registry and capability gates. It does not activate a
live provider, grant credentials/rights, retrofit legacy capture adapters, or
provide the hostile-code sandbox planned separately in #619.

## Public API and platform boundary

`histdatacom.broker_plugin_lifecycle` exports immutable `*V1` policy, header,
identity, session, transition, record, partition and manifest artifacts;
`run_broker_plugin_lifecycle`, `inspect_broker_lifecycle` and
`replay_broker_lifecycle`; and closed `BrokerLifecycleReason` errors. Lifecycle
policy version `1.0.0` is independent of package and broker SDK versions.
Canonical JSON includes its schema and content hash. Unknown fields, duplicate
JSON keys, wrong scalar types, noncanonical nested evidence, depth/item/byte
overflow and inconsistent identities are refused. Final encoded envelopes,
including identity overhead and escaped strings, must fit 524,288 bytes.

Execution and filesystem readers currently support **POSIX only** (qualified on
macOS). Other platforms are explicitly refused before worker/output mutation;
this does not remove the host package's Windows support for other APIs. The
supervisor uses process groups, inherited pipe descriptors, nonblocking I/O,
regular-file/no-symlink reads, atomic local renames and filesystem fsync. Caller
callbacks, the Python runtime, selected interpreter and local filesystem remain
trusted. Blocking kernel filesystem calls and hostile descendants escaping a
process group are not covered by a hard sandbox guarantee.

```python
from pathlib import Path
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1, negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecyclePolicyV1, run_broker_plugin_lifecycle,
    replay_broker_lifecycle,
)

inventory = discover_broker_plugins()  # metadata only, no provider imports
workflow = BrokerCapabilityWorkflowV1(tuple(sorted((
    "configuration_schema", "open_session", "instruments", "subscribe",
    "iter_events", "unsubscribe",
))))
plan = negotiate_broker_capabilities(
    inventory, workflow, plugin_id="org.example.lifecycle",
)
# This callback must represent a caller-owned authorization decision. An
# admitted capability plan is NOT an authorization or source-quality proof.
result = run_broker_plugin_lifecycle(
    inventory, plan, {"mode": "finite"}, ("EURUSD",), Path("new-run"),
    authorize=lambda selected: caller_has_authorized(selected),
    policy=BrokerLifecyclePolicyV1(),
)
for record in replay_broker_lifecycle(result.directory):
    consume_native_record(record)
```

The complete workflow and explicit boolean authorization are checked before
directory creation or worker launch. Configuration must be a bounded primitive
mapping (32 KiB transport limit), then must satisfy the selected plugin's exact
public schema before opening a session. Values are passed through an ephemeral
pipe, not arguments, artifact fields, hashes or exception messages. This is not
arbitrary secret detection: trusted plugin code still receives configuration,
and the SDK's public-field hygiene cannot prove that arbitrary extension text
contains no secrets. Do not grant access to an untrusted plugin on this basis.

Each fresh worker revalidates the selected installed distribution, descriptor,
entry-module origin and bytes through the #617 installed-loader API immediately
before invoking it. Only that registered entry point is imported. The parent
never imports the provider module. Entry-module association is not attestation
of the complete dependency tree or an atomic supply-chain snapshot.

## Deterministic state and sequence ownership

The normal sequence is `discovered → configured → starting → active → stopping
→ stopped`. Loss follows `active → degraded → reconnecting → starting`; terminal
failures use `stopping → failed`. Every transition is a canonical native health
record with previous/current state, a closed valid reason and host epoch. The
public transition validator rejects contradictory reason/state combinations.
`configured` means a structurally valid host request; the worker subsequently
checks the declared configuration schema before `active` can be reached.

The parent revalidates worker identity, capability receipts, session identity,
instrument/subscription membership, contiguous SDK sequence, receive clocks and
event semantics. It assigns a separate contiguous `capture_sequence` to every
durable native record. A new worker/session attempt creates a host connection
epoch. The provider's `connection_id`, SDK sequence and original source/receive
timestamps remain unchanged; host receipt UTC/monotonic clocks are separate.
Native SDK health/gap/clock evidence is never squeezed into legacy capture
events or relabeled as a collector clock. Existing legacy collectors, frames,
capture identity schemas and their trusted caller-owned adapter seams are
unchanged.

`SOURCE_GAP` diagnostics mark unknown loss even on a `health` event. Other
ERROR-severity diagnostics degrade the host with `plugin_error_health`, increment
`error_diagnostics`, and prohibit complete status, but do not assert lost market
data without gap evidence. Other INFO/WARNING diagnostics remain visible without
being automatically relabeled as data loss. Replay recomputes these decisions.

Only exact IPC redelivery `(host epoch, delivery sequence, admitted-event hash)`
is deduplicated. An identical quote at a new sequence is still a new event.
Sequences may reset across connections: no cross-epoch quote comparison or
provider sequence heuristic certifies continuity. Every reconnect records
unknown continuity/loss; repeated identical quotes remain visible in all epochs.
Retry delays and maximum attempts are frozen in the policy (at most eight
retries). No automatic parent-process resume or merged-run continuity claim is
made; every run requires a new output directory and public nonce.

## Backpressure, bounds and deadlines

The transport is a length-prefixed JSON protocol, never pickle or unbounded
line reads. A worker has **one outstanding event** and waits for the host's
durable acknowledgement before asking the SDK iterator for another. This
explicit backpressure bounds in-flight production at the host boundary; it
does not assert that an upstream network/source cannot drop data.

The parent also enforces pending-frame item and byte limits, a bounded recent
delivery-history window, per-frame bytes, event count, health-transition count,
partition count/bytes and total retained bytes. Bootstrap/control bytes obey
the byte budget too. Queue saturation refuses the run and marks unknown loss;
it never silently expands memory. Confirmed unique received-but-unpersisted
events are counted separately from unknown upstream/undecodable loss. Output
is drained and discarded in bounded chunks, with separate cumulative stdout
and stderr budgets; its text and provider exception text are never retained.
Output counters describe bytes actually drained, not an unknowable total after
forced termination. No arrival/service rates, utilization or M/M/1 latency
claims are invented.

Startup (including import/open), overall run and shutdown deadlines use the
host monotonic clock, independent of injected evidence timestamps. Timeout or
cancellation sends a cooperative stop. Blocked import, open, next or close is
then terminated by SIGTERM, escalated to SIGKILL and reaped with bounded waits
(each escalation wait is at most the configured shutdown interval). A watchdog
attempts cleanup after abrupt parent death without touching SDK objects. This
Python watchdog is best-effort: a provider native extension holding the GIL can
prevent its execution. Live-parent external process signaling remains the hard
plugin-call boundary, subject to the OS assumptions above. This assumes the
trusted runtime/OS schedules the supervisor/watchdog; it is not
containment of actively hostile Python code. Reconnect backoff remains
cancellable. Wall-clock scheduling is not claimed byte-identical between runs;
the retained transitions and their replay are deterministic evidence.

Cancellation/startup/run deadlines can race with a worker's startup reply.
Late identity/session replies still pass the full ordering and binding checks
and are retained as observed readiness, but never promote the host to `active`
after a stop request. Late events are semantically validated in bounded transient
state and counted as unpersisted, never ACKed or appended as active capture.
Valid draining preserves the initiating cancellation/deadline reason; genuinely
malformed, duplicate-handshake or wrong-binding evidence still refuses.

## Durable ACKs, partial partitions and completion meaning

Every accepted record is written and fsynced before the host ACK. A lost ACK can
cause bounded exact retransmission, but not another persisted canonical event.
Partition rollover uses exclusive creation, fsync and no-clobber publication.
An atomically replaced manifest binds the exact header, partition SHA-256,
byte count, record/event counts and host sequence range. SDK identity, field
admission, session/epoch and clock checks are reapplied during replay.

`complete_local_finite_run` requires observed finite iterator EOF, reported
session cleanup, successful worker exit/reaping, all events durably committed,
and no gap/reconnect, known drop, unknown loss or forced termination. Reaching
the event budget is a refusal, not an extra EOF probe. Cancellation and failed
or forced shutdown remain `partial`; the current hash-bound `.partial` file is
not renamed into a complete partition. Earlier sealed prefixes may remain.
If the parent dies or storage cannot finish, the original `open` manifest and
unadvertised partial tail remain detectably incomplete. Unfinished tails are
not silently repaired, resumed or admitted for replay.

The health budget reserves the maximum configured retry storm plus terminal
transitions; impossible low-health policies refuse at construction. Capture
bytes/partition capacity may still be exhausted by valid large events. If
terminal evidence cannot fit, execution raises the closed `persistence_failure`
reason and leaves `open` evidence instead of exceeding the configured budget.
An actual short/failed write poisons that journal: no subsequent append or final
manifest may hide the unknown tail. Inspection remains available; replay refuses.

`source_continuity_verified` and `configuration_material_retained` are always
false. Even successful local finite completion is **not** a provider-feed
completeness, source authenticity, market-data permission or production-readiness
certificate. Configuration is intentionally not reproducibility evidence;
broader provider provenance remains separate work (#626).

`inspect_broker_lifecycle(...).complete` is only the manifest's claimed local
completion plus directory shape. It does **not** hash or authenticate partition
contents. `replay_broker_lifecycle` verifies all referenced partitions and
semantic bindings before yielding its first record, then reopens and rehashes
bounded partitions on the yielding pass. Checks prove local retained consistency,
not an external signature or that an attacker could not rewrite all artifacts.
Concurrent external filesystem mutation is outside the trusted-directory model.

## Offline qualification

The maintained `broker_lifecycle_external.py` fixture imports only the public
SDK. Its separate wheel includes a real entry point, descriptor and RECORD
hashes. With an installed host wheel and pip already available, run:

```text
python tests/fixtures/broker_lifecycle_qualification.py /absolute/lifecycle-fixture.whl
```

The script asserts installed host imports, pip-installs only the offline
fixture (`--no-index --no-deps`), runs real finite/blocked-open/blocked-next/
blocked-close/reconnect workers, verifies durable replay and no parent provider
imports, uninstalls the fixture and checks exact inventory restoration in the
same process. No live provider is contacted. Public positive/negative typing
fixtures qualify the installed package without source paths or MYPYPATH.

Unit conformance additionally covers every state edge/reason, cancellation,
lost ACK/retransmission, saturation, malformed events/IPC, output floods,
blocked import, parent death before/after durable append, forced reaping,
unchanged identical quotes across epochs, partition tamper and nonregular file
refusal. Parent-owned repository hooks/full suites remain separate final gates.
