# Live broker delivery capture contracts

> **Later milestone:** these provider-neutral research contracts are not an
> executable v2.4 data source. Current reconstruction is qualified only for
> HistData.com ASCII/T caches. No OANDA or other live broker adapter is
> selected, and broker-specific work remains blocked on feed capability.

The `histdatacom.broker_capture` domain records how a broker feed was delivered
to one collector. It is measurement evidence for a later broker-delivery
fingerprint; it is not historical reconstruction, synthetic output, a claim
about the whole FX market, or the final Parquet product.

Version one deliberately has no real broker implementation. A real adapter
requires an explicit broker, protocol, SDK, licensing, and configuration
decision. That adapter implements the frozen public message iterator without
changing the capture, storage, replay, or downstream consumer contracts.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `BrokerAdapterMessageV1` | Credential-free quote, lifecycle, health, source-time, precision, size/activity, batch, and raw-message-hash evidence emitted by an adapter. |
| `BrokerCaptureSessionV1` | Adapter/protocol/collector/clock identity, hashed account and host identity, public environment/server identity, and configuration hash. |
| `BrokerCaptureEventV1` | Collector-assigned contiguous sequence plus adjacent UTC wall-clock and monotonic receive timestamps. |
| `BrokerCaptureStoragePolicyV1` | Partition rotation, session quota, high watermark, immutable retention ceiling, manifest reserve, and fsync behavior. |
| `BrokerCapturePartitionManifestV1` | Completed JSONL artifact hash, size, sequence/time bounds, and event-kind counts. |
| `BrokerCaptureSessionManifestV1` | Atomic catalog of completed partitions and explicit open/completed/failed capture health. |
| `BrokerCaptureReplaySummaryV1` | Reconciled replay counts, sequence bounds, and logical event-content hash. |

Every durable contract carries a schema version and verifies its derived ID on
read. Contract JSON is canonical, bounded, and contains no event batch inside a
manifest.

## Adapter and collector boundary

`BrokerCaptureAdapterV1` exposes only:

- a public adapter ID;
- an adapter semantic version;
- `iter_messages()`, which yields `BrokerAdapterMessageV1` values in observed
  order.

The adapter may privately own credentials and network state, but the collector
does not inspect adapter attributes, configuration dictionaries, exceptions,
or raw payloads. A blocking WebSocket, SDK, file-descriptor, or bridge-backed
implementation can all satisfy the iterator seam. Async/thread integration is
an adapter concern and does not change downstream contracts.

`LiveBrokerCaptureSourceV1` samples `BrokerCaptureClockV1` at the collector
boundary for each adapter message and assigns a zero-based contiguous capture
sequence. It rejects adapter-generated clock corrections: only the collector
can assert that its wall and monotonic clocks diverged.

## Time and ordering evidence

Each captured event records two different receive clocks:

- `receive_time_utc_ns` is an epoch timestamp suitable for alignment;
- `receive_time_monotonic_ns` is the ordering/duration clock that cannot be
  corrected by NTP or an operator clock change.

The collector compares the change in wall time with the change in monotonic
time. A difference at or above the configured threshold inserts a first-class
`clock_correction` event with the measured offset change before the triggering
message. UTC values are therefore allowed to move backward; monotonic values
and capture sequence are not.

An optional source timestamp is separately labeled as `broker_event`,
`exchange_event`, or `adapter_receive` and requires an explicit precision in
nanoseconds. Absence is `unavailable`, not zero or an inferred broker time.

## Quote precision, sizes, and activity

Numeric bid/ask values support downstream calculations. Optional `bid_text`
and `ask_text` preserve the exact plain-decimal lexemes needed to measure feed
precision and trailing-zero/rounding behavior. Their provenance is either
`source_lexeme` or `adapter_rendered`; absent text is explicitly
`unavailable`.

Optional bid/ask sizes must declare `quoted_size` or `broker_specific` meaning.
Optional activity must declare `message_count`, `broker_activity`, or
`liquidity_proxy`. No field is labeled as centralized FX transaction volume.

`source_batch_id`, source sequence, source message identity, and raw-message
SHA-256 are retained where supplied. Exact duplicate messages therefore share
a message ID while their distinct capture sequences produce distinct capture
event IDs.

## Lifecycle and capture health

Lifecycle and health are data, not log-only side effects. Version one includes:

- process start, stop, and explicit restart;
- connection open and close;
- reconnect;
- subscription add and remove;
- heartbeat;
- known gap;
- outage start and end;
- collector clock correction;
- quote.

This lets the fingerprint fitter distinguish a quiet market, a known broker
outage, a collector failure, a reconnect discontinuity, and a clock problem. A session manifest is
`open`, `completed`, or `failed`; a failed session retains bounded reason codes
without copying exception messages or private adapter state.

## Credential and publication safety

The public contracts have no token, password, credential, cookie,
authorization header, private key, or raw account-ID field. Account and host
identity are optional SHA-256 digests. Adapter configuration is represented
only by a SHA-256 digest computed over an operator-approved public
configuration projection.

All public metadata is recursively checked for sensitive key names, bearer
tokens, credential-bearing URLs, private-key headers, non-JSON values,
non-finite numbers, and size overflow. Error messages identify only the field
location; they never echo the rejected value. Raw message bodies are not
stored. An adapter may supply a content hash when licensing and source behavior
allow it.

This is stricter than the repository's path-oriented publication sanitizer:
capture contracts prevent credential-shaped values from entering rows or
manifests in the first place.

## Append-only storage and crash behavior

Capture data uses dependency-free canonical UTF-8 JSON Lines. This preserves
raw delivery evidence without coupling the base package to Arrow; #446
implements the separate final reconstructed Parquet product.

One session directory has this shape:

```text
broker-capture-session-<sha256>/
  session.manifest.json
  partition-000000.jsonl
  partition-000000.manifest.json
  partition-000001.jsonl.partial
```

The writer performs each rotation in this order:

1. append and fsync events to a `.jsonl.partial` file;
2. fsync and rename the data file to its final name;
3. hash it and atomically publish its partition manifest;
4. atomically replace the session manifest so the partition becomes
   discoverable.

A crash before step four leaves partial or orphan evidence, never an advertised
completed partition. `inspect_broker_capture_session()` reports partial data,
unadvertised final data, and orphan sidecars. Discovery reads only atomically
published session manifests. Replay verifies every sidecar plus data size,
SHA-256, UTF-8/line completeness, contract IDs, session, contiguous sequence,
monotonic ordering, counts, and time bounds.

## Rotation, quota, retention, and backpressure

Rotation can occur on event count, bytes, or monotonic duration. Before an
append, the writer conservatively accounts for current disk use, the next
canonical line, and manifest reserve.

- hard session quota raises `BrokerCaptureQuotaError`;
- the high watermark raises `BrokerCaptureBackpressureError`;
- the partition ceiling raises `BrokerCaptureRetentionError`;
- v1 retention never deletes committed evidence to make room;
- a single overlarge row refuses before a partial file is created.

The synchronous v1 collector therefore fails predictably rather than buffering
without bound. A future production control plane may pause/reconnect an adapter,
but it must honor these limits and cannot silently drop or overwrite captured
events.

## Live/replay parity and the fingerprint interface

Both `LiveBrokerCaptureSourceV1` and `BrokerCaptureReplaySourceV1` implement
`BrokerCaptureEventSourceV1`. `consume_broker_capture_source()` sends either
source through the same `BrokerCaptureEventConsumerV1.on_event()` interface.
The persistence sink is called before consumers, so an event that failed to
persist is never presented as captured fingerprint input.

The synthetic fixture covers exact duplicates, unchanged/stale quotes, a burst,
a quiet gap, outage start/end, disconnect/reconnect, subscriptions, process
lifecycle, heartbeat, source batching, exact price lexemes, and wall-clock
drift. The same consumer sees byte-replayed events equal to those observed
during live fixture collection.

Before the fitter creates a broker delivery fingerprint, it requires:

- a supported schema and adapter/collector version;
- a completed session manifest;
- clean partial/orphan inspection;
- verified sidecars and data hashes;
- acceptable clock-correction and capture-health evidence;
- sufficient conditioned support;
- an immutable capture and configuration identity.

An open or failed capture may be replayed for diagnosis, but it is not silently
eligible for fingerprint fitting.

The implemented two-pass fitter, condition cells, support/backoff policy,
compact statistics, drift evidence, supersession, and immutable artifact rules
are specified in
[`broker-delivery-fingerprint-contracts.md`](broker-delivery-fingerprint-contracts.md).

## Issue boundaries

- #431 supplies the separate reconstructed-event contract.
- #433 governs information safety for reconstruction; broker capture remains
  timestamped external evidence with explicit availability.
- #443 implements capture and verified replay only.
- #444 supplies immutable broker-delivery fingerprints and stratified drift
  detection from qualified captures.
- #445 applies a selected fingerprint during proposal and delivery rendering.
- #446 implements the final reconstructed Parquet product publisher.

Changing timestamp meaning, message identity, quote-lexeme semantics,
credential boundary, storage publication order, replay integrity, or consumer
interface requires a new schema or collector version.
