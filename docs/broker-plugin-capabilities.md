# Broker-plugin capability gates

The host-side `histdatacom.broker_plugin_capabilities` public package completes
the SDK-v1 operation boundary introduced by #615 and the installed registry
introduced by #616. Its immutable catalog has independent SemVer `1.0.0`;
capability meanings carry `.v1` suffixes. Incompatible meanings need a new atom
or version, not a provider-name conditional or an edited interpretation.

Capabilities describe evidence a plugin can supply. They are **not permissions**,
license grants, credential policy, scientific admission, live deployment approval,
or a guarantee of capture completeness. Invocation always requires an explicit
caller-owned authorization callback after complete preflight. No command starts
a plugin automatically, and this package neither changes legacy capture wire
identities nor activates a production broker.

## Preflight and explicit invocation

```python
from histdatacom.broker_plugin_registry import discover_broker_plugins
from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    invoke_authorized_installed_broker_plugin,
    negotiate_broker_capabilities,
)

inventory = discover_broker_plugins()  # metadata only, no plugin imports
workflow = BrokerCapabilityWorkflowV1(
    operations=tuple(sorted((
        "open_session", "instruments", "subscribe", "iter_events",
        "unsubscribe", "close_session",
    ))),
    required=("timestamps.broker-event.v1",),
)
plan = negotiate_broker_capabilities(
    inventory, workflow, plugin_id="org.example.mybroker"
)
plan.require_admitted()  # typed unsupported_capability / unsupported_operation

# Supply a real caller-owned authorization decision. A capability receipt is
# not sufficient authority. This example intentionally does not authorize.
invocation = invoke_authorized_installed_broker_plugin(
    inventory, plan, authorize=lambda approved_plan: False
)
```

Required capabilities are the union of declared operation requirements and
workflow requirements. Admission requires that set to be contained in the
selected plugin's advertised set and understood by this catalog. Unknown
required versions refuse; unknown optional atoms are not enabled. Adding optional
capabilities cannot revoke admission. No capability is inferred from provider ID.
Selection still uses the registry's SDK-compatibility and duplicate-candidate
refusals. Plan identity binds the entire inventory, exact candidate, catalog,
workflow, and all derived decisions. `verify_broker_capability_plan` recomputes
them; restored receipt bytes alone are not execution authorization.

Every SDK-v1 operation has a declaration:

| Operation | Required capabilities | Declared prerequisite operations |
| --- | --- | --- |
| `metadata` | none; public context is optional | none |
| `configuration_schema` | none | none |
| `open_session` | `session.v1` | none |
| `instruments` | `instruments.v1` | `open_session` |
| `subscribe` | `instruments.v1`, `quotes.v1` | `open_session`, `instruments` |
| `unsubscribe` | `quotes.v1` | `open_session`, `instruments`, `subscribe` |
| `iter_events` | `events.v1` | `open_session` |
| `close_session` | none | always available for cleanup |

An explicitly required quote stream also needs instrument/subscription operations.
Impossible operation sets refuse before authorization or factory/import calls.
A health-only stream does not need a quote subscription. An optional quote
declaration does not add prerequisites or permit unsolicited quote delivery:
runtime quote events still require an actual discovered, subscribed instrument.
Configuration-only and metadata-only inspection remain possible. The invocation
enforces session/subscription ordering and rejects undeclared methods before
calling the plugin; prerequisites authorize no automatic method calls.

`history.replay.v1` and `history.backfill.v1` are versioned declarations, but SDK
v1 has no historical execution method. `history_replay` and `history_backfill`
therefore explicitly refuse as `unsupported_operation`, even when advertised.
`offline_capture_replay` also refuses through this API: legacy replay reads stored
capture evidence and is not an installed-plugin operation. Nothing fabricates
history or claims to gate every historical artifact with today's plugin inventory.

`legacy_live_capture` likewise refuses plugin negotiation. The frozen
`broker_capture.adapters.LiveBrokerCaptureSourceV1` accepts a trusted,
caller-supplied legacy adapter, not an SDK plugin, and remains outside this new
invocation path. Its direct calls have not silently acquired these capability
gates. All new SDK-plugin host methods are gated here; retrofit of the legacy live
adapter would require an explicit lossless versioned integration, not relabeling
SDK events or pretending the old adapter supplied a registration.

## Exact field and event meanings

The public catalog enumerates quotes, source-time variants, plugin receive time,
instrument precision, connection/subscription events, heartbeat, health, reconnect,
gaps, clock corrections, raw-message hashes, and public metadata. All SDK event
kinds are checked before delivery; none is silently relabeled into a legacy kind.

- `timestamps.broker-event.v1`, `timestamps.exchange-event.v1`, and
  `timestamps.adapter-receive.v1` are distinct source-time meanings. A required
  broker event time cannot be satisfied by an optionally enabled adapter time.
- `timestamps.receive.v1` is the plugin's declared UTC/monotonic observation;
  it is never a synthesized host capture clock or source timestamp.
- `sizes.quoted.v1` and `sizes.broker-specific.v1` preserve decimal lexemes and
  exact units. Required size semantics apply to both bid and ask. Activity atoms
  distinguish integer message counts, broker measurements, and liquidity proxies.
  None becomes centralized traded volume.
- Simultaneous required alternatives for a single source-time, size, or activity
  field are contradictory and refuse during preflight. Enabled optional
  alternatives do not weaken a required meaning.
- Required quote fields are checked on quote events; absent quote-only fields on
  control messages are `not_applicable`. Required receive time applies to every
  event. Required price increment is checked on every discovered instrument.
- `raw-hashes.v1` preserves the SDK's SHA, byte count, MIME type and external
  policy pointer. The caller must separately establish collection/retention rights.
- Public context uses the SDK extension namespace
  `org.histdatacom.broker-public-context`. Keys `account_class`, `feed_class`, and
  `server_label` have closed class vocabularies; these are not account IDs, private
  endpoints or credentials. Account classes: `demo`, `retail`, `professional`,
  `institutional`, `unknown`; feed classes: `indicative`, `executable`,
  `consolidated`, `single_dealer`, `unknown`; server labels: `demo`, `test`,
  `production`, `unknown`. Other extensions remain opaque SDK data and convey no
  additional scientific meaning or capability authority.

Admitted events, metadata and instruments preserve native SDK artifacts plus
explicit field receipts: `present`, `unsupported`, `supported_not_reported`,
`not_requested`, or `not_applicable`. No missing value is populated. An unsupported
non-null value refuses rather than being silently dropped. Present receipts name
the exact capability atom; absent receipts use a field-group label such as
`source_time.v1`, which is provenance notation, **not** a catalog capability.
`verify_broker_admitted_event` recomputes restored event receipts against the plan.

## Installed association and runtime limits

After admission and explicit authorization, installed invocation re-discovers
the inventory, validates the exact registered distribution/entry point and
descriptor/entry-module RECORD bytes, checks parent/entry origins using explicit
`PathFinder` searches without importing parents, reads bounded regular source
files, and rechecks the inventory before execution. It compiles the freshly
verified entry-module bytes rather than trusting a same-size/same-mtime `.pyc`.
Runtime metadata must match the selected plugin ID/version/SDK interval. Source
registration never comes from a user-supplied module path. `sys.path` shadow
packages and conflicting loaded module origins refuse before their import.
Equivalent resolved path aliases are allowed. Namespace, extension-module and
custom-loader package layouts are currently unsupported and fail closed.

Previously loaded entry modules refuse, even if their path matches: their mutable
runtime state is not disk evidence. Use a fresh caller-owned process for another
installed invocation. Existing correctly located parent packages may be reused.
The source hash attests **only the entry module**, not parent package code,
dependencies, the entire wheel, supply chain, licensing or permissions. Metadata
providers, interpreter/import hooks and the Python runtime are trusted. Filesystem
checks are not an atomic filesystem snapshot; package code can execute arbitrary
Python. This API is not a hostile-code sandbox or TOCTOU-proof isolation boundary.

`invoke_authorized_broker_factory` is a separately labeled caller-owned seam.
Its `caller_factory_identity_only` binding verifies runtime metadata but **does
not verify installed association**; it cannot select the installed verification
label. Public direct invocation construction refuses, and its plan/binding
properties are read-only. This is a supported API invariant, not protection
against hostile mutation of Python internals. A persisted binding is a receipt,
not independently signed proof that execution took place.

An invocation owns at most one SDK session, one finite instrument snapshot and
one active iterator, on one caller thread. Configuration is validated immediately
before `open_session`, passed ephemerally, and not retained in receipts. No source
event array is accumulated. Stream calls default to 1,024 events and permit a
caller bound of 1–1,000,000. Reaching the exact budget raises `resource_limit`
without consuming an extra event to guess completeness. Ordered sequence and
receive-clock checks persist across fragments; invalid output is not delivered.
Closing remains available after failure and is idempotent. Dropping/closing a
generator releases its stream guard. Plugin failures produce closed reason codes,
never raw exception/configuration strings.

No transport deadline, process isolation, retry/backpressure scheduler, credential
manager, or automatic capture persistence is supplied. Plugin calls may block;
caller-owned cancellation/containment remains necessary. Those concerns stay with
#618/#619/#623/#624. HEALTH and plugin CLOCK_CORRECTION events retain their native
meaning; a lossy bridge into legacy capture events is deliberately not supplied.

## Qualification

The focused tests cover generated capability subsets, optional monotonicity,
every required-capability removal, method prerequisites, all SDK methods/event
kinds and field semantics, native receipt replay, malformed artifacts, bounded
streams, exact identity, zero-call refusals, shadow modules and stale bytecode.
The separately installed offline wheel fixture imports only public SDK contracts;
it needs no host source edits and exercises metadata-only discovery through
authorized execution and uninstall in the same process.

```sh
PYTHONPATH=src python -m pytest -q \
  tests/unit/test_broker_plugin_capabilities.py \
  tests/unit/test_broker_plugin_capability_loading.py
# In a fresh environment with the built host wheel installed (no PYTHONPATH):
python tests/fixtures/qualify_broker_capabilities.py /absolute/fixture.whl
mypy --strict --python-executable /absolute/venv/bin/python \
  tests/fixtures/broker_capability_typing.py \
  tests/fixtures/broker_capability_external.py
# Expected failure: incompatible operations argument, not an untyped import.
mypy --strict --python-executable /absolute/venv/bin/python \
  tests/fixtures/broker_capability_typing_invalid.py
```

The scoped `py.typed` marker supports third-party installed-wheel type checking
without declaring the entire host typed. The corresponding negative type fixture
must reject an invalid workflow field or return type rather than degrading to
`Any`. Packaging follows the registry's existing
[entry-point specification](https://packaging.python.org/en/latest/specifications/entry-points/)
and [installed-file RECORD convention](https://packaging.python.org/en/latest/specifications/recording-installed-packages/).
