# Canonical generated FX fixture ABI v1

This is the external developer specification for `canonical-fx-v1`, used by
conformance catalog version `2.0.0`, identity
`broker-conformance-catalog:sha256:7c4965dad269a282167e08b1b04a7caaa6feaf4fc78da4d87b6229aef008b705`.
It describes synthetic test behavior, not a provider protocol or a production
mode. It requires no broker account, production endpoint or market dataset.
The reviewed host wheel and its public catalog remain pinned qualification
inputs; the installed plugin/permission/driver identity is the candidate subject.
A future catalog identity needs explicit compatibility review of this specification.

## Driver and installed subject

The public `BrokerConformanceDriverV1` binds the exact discovered candidate,
installed permission manifest, provider ID, canonical configuration schema,
symbols `("EURUSD",)`, and sorted `BrokerConformanceScenarioV1` mappings. Each
mapping translates a scenario below into the plugin's own public configuration.
The configuration key need not be named `mode`. No mapping supplies a factory,
transport, expected verdict, selected passing cases, or replacement host object.
Unknown scenario IDs refuse. The catalog, requested capabilities and profile
determine the complete case denominator; missing required scenarios never pass.

A generated implementation may support only an honestly declared capability
subset. It must still implement every common case and every case required by
that subset/profile. A driver mapping does not advertise a capability. Production
endpoints, including optional declarations, are not accepted for fixture runs;
declared network origins, if needed, must be literal loopback addresses.

The prescribed quote-fixture vectors themselves require these runtime
declarations: `session.v1`, `events.v1`, `instruments.v1`, `quotes.v1`,
`health.v1`, `timestamps.broker-event.v1`, and `timestamps.receive.v1`.
Native/dual common reconnect also requires `connection.v1`. These are emission
prerequisites even when only quote certification is requested: the common
finite vector contains HEALTH, while duplicate/unchanged use source timestamps.
Their presence does not mean a quote report separately certifies health or
timestamps. To qualify health's native scenarios, also declare and implement
`gaps.v1` and `heartbeat.v1`. Additional vectors need their matching atoms:
`sizes.quoted.v1`, `raw-hashes.v1`, or `instruments.price-increment.v1` when
those values are emitted. Optional data may instead be omitted consistently.
The permission manifest independently needs actual emission grants, including
`emit:health` and `emit:quotes`; capability declarations are not grants.

Discover one instrument: canonical `EURUSD`, provider symbol `EUR/USD`, base
`EUR`, quote `USD`, exact price increment `0.00001` if that capability is declared.
Subscribe/unsubscribe accept the exact canonical symbol, not an inferred alias.
Use the public SDK structural protocol and match metadata to registration. A new
session binds that metadata and its own public instance/receive-clock identity.
The session nonce and physical host clocks are not fixed golden market values.
Cleanup must be idempotent except the deliberately blocked-close test below.

Every successful opening must use a fresh `BrokerSessionV1.instance_nonce`,
including independent executions and reconnect openings. A changed session ID
or opening timestamp does not excuse a reused nonce. The named common
`session.freshness` gate reuses the `finite` scenario; catalog 2 keeps the same
24 scenario IDs. Replaying one retained bundle preserves its original nonce
and is not a new opening. Freshness is checked separately from semantic-vector
equality. Observed distinct nonces do not prove entropy, global uniqueness,
absence of cross-process caches, or future behavior.

Catalog-1 reports remain historical evidence under their exact catalog and a
compatible verifier. They are not upgraded or relabelled as catalog-2 passes;
complete new physical execution and independent verification are required.

## Baseline vectors

Unless a row below overrides them, emit three QUOTE events and finite EOF. Use
contiguous local event sequences from zero across all emitted event kinds,
matching session and connection IDs. A repeated source message is a new local
event, not a repeated local sequence.

- Instrument is `EURUSD`; bid is exactly `"1.10000"`, ask `"1.20000"`. Preserve
  these decimal strings and trailing zeros; do not round-trip through floats.
- Source times are `(1_000_000_000, 2_000_000_000, 3_000_000_000)` UTC ns,
  precision `1`, semantics `broker_event`.
- A deterministic receive vector may use wall UTC ns `(10, 11, 12) * 10**9`
  and monotonic ns `(10, 11, 12)` under the session receive-clock identity.
  These are generated evidence vectors, not actual transport latency claims.
- Optional sizes, activity, raw provenance and opaque extensions are absent
  unless explicitly called for and declared. Null does not mean zero.
- Fixed source/receive vectors do not replace the host's independently measured
  ingress, persistence or close clocks.

The `finite` stream is exactly HEALTH, QUOTE, QUOTE, QUOTE. The HEALTH diagnostic
is SDK `HEALTHY`/`INFO`, with a stable authored public summary. The supplied
no-raw operator example uses exactly `BrokerReasonCode.HEALTHY.value` as that
summary: any other free-form diagnostic text is conservatively classified as
raw payload by provider policy, even when its author considers it harmless.
Explain the advisory generated meaning in documentation rather than widening
the example's raw rights. No CONNECTED/SUBSCRIBED/DISCONNECTED events are
added to this particular four-event fixture vector. Subscription state still
uses the normal SDK methods; the host retains its own lifecycle/control records.
This exact case vector does not redefine general SDK event-kind support.

## Complete scenario vocabulary

| Scenario | Required generated stimulus |
| --- | --- |
| `finite` | The exact four-event vector above, followed by normal finite EOF and successful close. |
| `exception` | Raise a synthetic ordinary exception from `open_session`, after valid metadata/schema. The host must report the actual open-stage `plugin_failure`, without raw exception text. |
| `malformed` | Open normally, then return a non-SDK event object from iteration, such as a generated dictionary. The actual host event-type gate must refuse it; a plugin-raised error is not a substitute. |
| `invalid-spread` | Open normally and return an otherwise shaped SDK quote event whose bid exceeds ask. In this adversarial fixture only, construct a valid frozen quote/event then deliberately corrupt its bid with `object.__setattr__` before returning it. A constructor exception inside the plugin does not test the host spread gate. Never use this technique for real data. |
| `bad-sequence` | Return valid first quote at local sequence zero, then skip sequence one (return two). The host must identify the sequence gate, not an unrelated error. |
| `bad-clock` | Return a first quote with receive monotonic10, then monotonic1 on the second, with the same receive-clock identity and otherwise valid data. The host must identify monotonic regression. |
| `duplicate` | Exactly three quotes with source times `(1,1,2) * 10**9`. First two have equal quote/instrument/connection but distinct local event identities. Do not suppress either. |
| `unchanged` | Exactly three equal quote payloads with three distinct source times. Preserve all three events. |
| `stale` | Exactly three quotes with source times `(1,1,2) * 10**9`, bids `"1.10000"`, `"1.10001"`, `"1.10002"`, fixed ask `"1.20000"`; pause at least150ms before the second quote. This changes the quote while its source stamp stays old, avoiding an exact-duplicate explanation. |
| `burst` | Four immediate baseline quotes (cycle the three source offsets), then EOF. Respect ordinary host stop-and-wait delivery; do not implement private ACK injection. |
| `next-block` | Open successfully, then block before the first event for longer than the fixture cancellation window (300seconds is the reference stimulus). The host requests cancellation after45seconds, must retain zero events and reap the worker. Never run this mode directly in an unbounded developer test. |
| `close-block` | Exactly three quotes and EOF, then block in `close_session` for longer than shutdown bounds (300seconds is the reference stimulus). The host must record shutdown deadline, force termination and reap. |
| `reconnect` | In each fresh host epoch, exactly three baseline quotes then DISCONNECTED. The kit supplies one zero-delay retry, requiring two sessions/epochs and eventual retry exhaustion. Do not emit a healthy prelude or duplicate the first source stamp within an epoch. Equal quotes across distinct epochs are retained, not merged. |
| `secret-leak` | Raise an ordinary synthetic exception during open with exact test-only canary text `generated-conformance-secret-not-for-retention`. The host must retain only the closed open-stage plugin failure and omit the canary from evidence. This is not a real credential and must never be replaced by one. |
| `honest-health` | HEALTH then three quotes. For these quotes omit source time entirely and use actual current UTC/monotonic receive pairs; make no source-clock or upstream-continuity claim. The host, not the plugin, determines `qualified_host_boundary_only`. |
| `healthy-duplicate` | Three quotes with source offsets `(1,1,2)` and equal first two quote payloads, but no falsely healthy diagnostic. The host must observe duplicates without a healthy-claim discrepancy. |
| `healthy-gap` | GAP with known missing count5 followed by exactly three quotes, no falsely healthy diagnostic. The GAP is SOURCE scope, e.g. start100/end200 UTC ns, with SOURCE_GAP/WARNING. The native result is intentionally partial. |
| `healthy-reorder` | Three quotes with source offsets `(3,1,2)` seconds, preserving arrival order, and no falsely healthy diagnostic. Host source-time reordering must remain visible. |
| `healthy-delay` | HEARTBEAT before each of three quotes, with at least150ms before the second heartbeat/quote pair, and no falsely healthy claim. The host must measure an observed heartbeat gap above50ms. |
| `heartbeat` | HEARTBEAT before each of three baseline quotes, contiguous local sequences across both kinds, then EOF. |
| `gap` | SOURCE GAP with missing count5 and SOURCE_GAP/WARNING, then exactly three baseline quotes and EOF. A valid terminal chain does not turn the intentionally partial capture into complete data. |
| `sizes` | Three quotes with both sizes `"2"`, semantics `quoted_size`, unit `base-units`, when declared. The kit separately removes `emit:sizes` to require the actual permission refusal. Do not catch and disguise that refusal. |
| `raw` | Three quotes each carrying generated raw provenance: SHA256 of ASCII `generated`, byte count9, MIME `application/json`, public synthetic policy reference. Actual raw/retention rights remain required. The kit separately removes `raw_payload:emit` to require the actual permission refusal. No raw body is stored in the SDK event. |
| `overflow` | A bounded96-quote generated stream, cycling baseline source offsets. This scenario is used by the separate host-reference adversary, which controls ACK loss/persistence and must reach queue saturation. A candidate's ordinary burst is not an overflow test, and reference success cannot certify the candidate. |

The `healthy-*` names describe host-observability checks. A conforming fixture
must expose the stimulus without lying about it. The separate deliberately
broken false-health plugin adds a contradictory healthy claim and must fail.
Similarly, deliberate broken distributions are different installed subjects,
not a driver's self-reported fault flag or a way to certify broken production
behavior. The optional `open-block` helper sometimes used in host development
is not a scenario in this catalog and must not appear in its driver.

## Intended gates and execution profiles

The catalog contains 44 cases. Obtain its exact per-capability/profile inventory
with the public `catalog`/`plan` commands rather than constructing a shortened
case list. Quote certification includes common discovery, permission, rights,
lifecycle, secret and replay obligations as well as quote-specific cases.

Negative payload cases use actual installed SDK probes and require closed host
diagnostics: `event_type`, `invalid_spread`, `sequence`, or
`monotonic_regression`. Exceptions require open-stage `plugin_failure`.
Permission cases require actual current decisions with the intended denied
atom. Generic timeouts, arbitrary exceptions, producer-created error codes and
missing evidence cannot stand in for these intended gates.

The kit owns required-permission denial, revocation and provider invoke,
retention and publication denials. A plugin implements ordinary SDK behavior;
it does not create grants, terms approvals, expected refusals or result records.
Generated terms have no effect outside their exact generated invocation.

`trusted_contract_v1` uses fresh bounded trusted processes, not a kernel
isolation claim. `isolated_contract_v1` uses native/kernel cases plus explicitly
identified trusted negative probes. `dual_contract_v1` also compares independent
trusted and isolated executions. Kernel execution is currently qualified only
on macOS; an unavailable backend must remain unsupported, never silently fall
back or count as a pass. Native fixture policy is startup30s, run120s (240s for
reconnect), acknowledgement10s and shutdown100ms. Reference overflow instead
uses acknowledgement10ms and eight retries. These are test policy values, not
production defaults or a throughput qualification.

All successful native predicates also require the exact pinned closed journal,
matching native terminal state, no unrelated refusal/error, and worker reaping.
Ordinary finite cases require COMPLETE with normal EOF/CLOSED. Reconnect, gap,
cancellation, forced-close and reference overflow retain their prescribed
PARTIAL outcomes; these are test success, not complete market observations.

Determinism and dual equivalence require two actual executions. They compare
ordered SDK semantic tuples `(kind, instrument, quote, source_time, gap,
diagnostic, raw_provenance)`, preserving each physical run's clocks and roots
separately. Keep the finite semantic vectors stable across fresh sessions;
do not make market values depend on current time, worker identity or scheduling.
Re-verifying one retained bundle is deterministic report replay, not a second
physical execution and not proof of cross-backend equivalence.

Keep the whole generated bundle and independently retained report/root identity.
Use the public `verify` command to replay it. Do not rewrite its evidence to
produce green rows. Required `unsupported`, `not_run`, errors and skipped cases
are not passes. Certification is per capability and exact subject/profile;
host-reference self-tests cannot certify an installed candidate. None of these
results proves provider truth, rights, source completeness or scientific
fitness, and SDK-native scientific fitting remains a separate obligation.

## Specification qualification status

This draft has been derived from the current actual catalog, runner and verifier.
Before shipping it, both standalone projects must reconcile every implemented
scenario to this table and complete their real clean installed qualification.
These words are not execution evidence, and do not claim those runs occurred.
