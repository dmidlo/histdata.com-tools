# Public broker-plugin SDK v1

Issue #615 freezes `histdatacom.broker_plugins` as the public namespace for
third-party broker evidence producers. Its first release has SDK version
`1.0.0`, independently of the host application's version and each plugin's
SemVer version. Plugins import only this namespace and the standard library
or their own provider dependencies. The SDK itself imports only the standard
library and its own modules; it performs no network, filesystem, registration,
or provider discovery side effects.

## Ownership and compatibility

The plugin supplies broker-neutral measurements. The host owns validation,
capture clocks, storage, fingerprinting, reconstruction, certification,
experiment state, retries and resource policy. No SDK method receives those
host objects or a product writer. A plugin may keep its own private connection
state; this does not grant it access to host internals. SDK contracts are
deeply immutable, versioned evidence artifacts. An incompatible field meaning
requires a new schema, never an altered interpretation of a v1 field.

`BrokerPluginMetadataV1` declares a namespaced plugin ID, plugin SemVer version,
public display name, and the exact SDK version it implements. This release
admits `sdk_version="1.0.0"`; unknown versions fail explicitly. Future SDK
compatibility policy must admit new versions deliberately. Metadata and
configuration declarations contain no credentials or live connection handles.

## Mandatory structural protocol

`BrokerPluginV1` is a runtime-checkable structural protocol. Inheritance from
an SDK implementation class is unnecessary. Every implementation provides:

| Surface | Required behavior |
| --- | --- |
| `metadata` | Immutable `BrokerPluginMetadataV1`, stable for this plugin build. |
| `configuration_schema` | Immutable `BrokerConfigurationSchemaV1`; no configuration values. |
| `open_session(configuration)` | Validate ephemeral configuration, open the connection, return a new `BrokerSessionV1`, or raise a bounded `BrokerPluginError`. |
| `instruments(session)` | Return a complete bounded tuple of supported `BrokerInstrumentV1` records or refuse; never present truncated discovery as complete. |
| `subscribe(session, symbols)` | Subscribe using exact canonical symbols from discovery; emit ordered `subscribed` evidence. |
| `unsubscribe(session, symbols)` | Remove subscriptions and emit ordered `unsubscribed` evidence. |
| `iter_events(session)` | Yield `BrokerEventV1` measurements in contiguous per-session sequence order starting at zero. |
| `close_session(session)` | Release resources, emit terminal disconnection evidence when observable, and make repeated close harmless. |

All methods are synchronous in v1. A plugin must refuse unsupported operations
with a typed diagnostic instead of silently dropping them. A caller consumes
the event iterator from one owner; parallel calls, async execution, automatic
retry and process supervision are not implied. The host must close sessions
in a `finally` block when iteration fails or is cancelled. It must not infer
successful closure from the absence of a final event. Detailed runtime
discovery, capability negotiation, isolation and lifecycle orchestration have
separate issues #616–#619; they are not hidden requirements on a v1 plugin.

Configuration fields declare string, integer, finite number, or boolean type;
required/optional and secret status; description; and optional non-secret
string choices. `validate_configuration` rejects unknown/missing fields,
wrong exact types (including booleans used as integers), invalid choices and
unbounded values. It does not serialize configuration or echo failing values.
There are no persisted defaults, especially secret defaults. Plugins own
provider authentication and must never copy credentials into public evidence.

## Session and instrument identity

A session binds `metadata_id`, a fresh public 128-bit instance nonce, opening
UTC nanoseconds and the plugin process's receive-clock identity. The nonce is
not an authentication token. Reopening after a process restart creates a new
session and receive-clock identity. A reconnect inside one session keeps the
local sequence increasing and emits `reconnecting` evidence; connection IDs
are public opaque labels, never account credentials or secret-bearing URLs.

Instrument discovery declares the exact provider symbol, canonical six-letter
FX symbol, explicit base/quote currencies, and optional exact decimal price
increment. Unknown increments remain null. `normalize_broker_instrument`
resolves only this discovery mapping. It rejects duplicate/ambiguous mappings
and undeclared symbols; it never guesses suffixes, punctuation, currencies,
or pip sizes. V1 covers FX pairs; other instrument classes require a successor
contract rather than overloading these fields.

## Event and measurement semantics

Every event binds the session artifact ID, local sequence, kind and connection
ID. Source timestamps and source ordering are evidence, not the local ordering
key. Duplicate source messages and source timestamps moving backward are
retained when their local sequence differs.

| Kind | Required additional evidence |
| --- | --- |
| `connected`, `disconnected` | Public connection identity; optional diagnostic. |
| `reconnecting` | Diagnostic explaining the interruption. |
| `subscribed`, `unsubscribed` | Canonical instrument symbol. |
| `quote` | Matching instrument and `BrokerQuoteV1`; no gap payload. |
| `heartbeat` | No fabricated quote or gap; receipt evidence when available. |
| `health` | Bounded diagnostic, including explicit healthy status. |
| `gap` | `BrokerGapV1` and diagnostic; quote inactivity alone is not a gap. |
| `clock_correction` | Receive-clock pair and `clock_discontinuity` diagnostic. |

`BrokerSourceTimeV1` records a nonnegative UTC nanosecond value, positive
precision, and `broker_event`, `exchange_event` or `adapter_receive` semantics.
Absent source time is null; it is not replaced with receive time.
`BrokerReceiveTimeV1` pairs wall UTC and monotonic nanoseconds under one clock
identity. It is the plugin transport's observation. The **host boundary**
must independently stamp its own capture clocks; plugin times cannot claim
host measurement authority. Missing plugin receipt evidence remains null.
Source and receive clocks may disagree; the SDK preserves that discrepancy.

`validate_broker_event_stream` checks session identity, contiguous local
sequence, fixed receive-clock domain and nondecreasing monotonic time. A
backward wall-clock step requires an explicit correction event. Source-clock
regressions remain permitted evidence. For a verified resumed fragment, the
host may supply its independently established next `starting_sequence`; the
validator does not fabricate continuity across an unverified missing fragment.
The function streams without retaining the corpus or certifying source health.

Quotes retain bid/ask as exact unsigned decimal lexemes, including trailing
zeros. Positive prices and `bid <= ask` are required. Optional bid/ask size
uses a declared unit and `quoted_size` or `broker_specific` meaning. Optional
activity uses `message_count`, `broker_activity` or `liquidity_proxy` and its
unit. Message counts are integral and use the `message` unit. Missing size
or activity is null, never zero by assumption. These fields do not establish
centralized FX trading volume, executable liquidity or actual transactions.

Gap evidence distinguishes source, transport and collector scope, records
the known UTC start, optional end and optional missing-message count. An open
gap retains a null end; an unknown count remains null. A gap is not silently
closed because a later quote exists.

`BrokerRawProvenanceV1` optionally records a permitted raw-message SHA-256,
byte count, MIME type and governing policy reference. It contains no raw body
and grants no retention or redistribution rights. The caller must establish
the applicable policy before hashing; if hashing is not permitted, omit this
artifact. Provider rights and capture certification remain host concerns.

## Diagnostics and refusal

`BrokerDiagnosticV1` records an enumerated reason, severity, bounded public
summary and retryable hint. Reasons distinguish configuration, authentication,
unsupported instruments/operations, transport failure, timeout, source gap,
clock discontinuity, policy refusal, resource limits, malformed messages and
internal failure. The plugin's retry hint never overrides host policy.

`BrokerPluginError` carries this diagnostic. Summaries are at most 512
characters, single-line, and reject common credential-bearing URL, bearer
token and private-key patterns. They must be authored public summaries, not
raw exception dumps. Pattern checks cannot prove arbitrary text is free of
secrets; plugins remain responsible for redaction. Unknown provider details
belong in an optional approved extension, not a new unversioned reason code.

## Strict canonical serialization

Every SDK artifact has `schema_version`, `artifact_id`, `to_dict`, `to_json`,
`from_dict` and `from_json`. Its ID is SHA-256 over sorted compact ASCII JSON
of all fields including the schema but excluding the ID. Prefixes identify
the artifact type. Nested artifacts retain and verify their own IDs. Text,
decimals, nulls, optional flags and extension contents all affect identity.

Decoders require exactly the schema's field set, including explicit nulls and
empty arrays. They reject unknown fields, wrong scalar types, unknown enum
values, unsupported schemas, duplicate JSON keys, identity tampering, NaN,
infinities and excessive sizes/depth. No coercion turns a string into a number
or a boolean into a timestamp. Serialization returns detached dictionaries;
mutating them cannot modify the original artifact. Collections become tuples
and nested artifacts remain frozen.

Each artifact is bounded to 65,536 serialized bytes; collections have at most
128 items and JSON nesting at most 16 levels. Extension payloads are at most
8,192 bytes. `BrokerExtensionV1` stores an immutable canonical JSON-object
string under a unique namespaced identifier, for example
`org.example.transport`. The `histdatacom` and `broker-plugin`/`broker.plugin`
namespace roots are reserved, including dotted and hyphenated descendants.
`payload()` returns a detached copy. Unknown extensions round-trip unchanged
and may be ignored;
their keys cannot override any core event field, identity, permission or
scientific meaning. An extension that changes a required behavior needs a
separately versioned negotiated capability, not silent reinterpretation.

Extensions reject common credential-bearing text patterns and known credential
keys (including password, token, API-key and authorization spellings), including
nested objects and arrays. This is a bounded hygiene check, not a guarantee that
arbitrary extension content is secret-free. Plugins must redact and the host
must approve extension content before persistence; unfamiliar credentials must
never be disguised under an innocuous key.

## Independent acceptance evidence

`tests/fixtures/broker_sdk_external.py` is a standalone structural plugin that
imports only `histdatacom.broker_plugins` and standard-library modules. It
exercises configuration, session opening, discovery/normalization,
subscription, quote/heartbeat delivery, unsubscription and idempotent closure.
Its deterministic sample events round-trip canonically with no credentials.
It is a fixture, not evidence of an integrated or qualified real broker feed.

The SDK tests audit every SDK import, execute the external plugin in a fresh
interpreter with private host imports blocked, and test schema tampering,
strict scalar handling, immutable nested data, extension bounds, all event
kinds, sequence/clock failures and explicit optional-data semantics. Strict
type checking covers the SDK and the external structural implementation:

```console
python -m mypy --strict --follow-imports=skip --no-incremental \
  src/histdatacom/broker_plugins tests/fixtures/broker_sdk_external.py
python -m pytest tests/unit/test_broker_plugin_sdk.py
```

The SDK subpackage contains a PEP 561 `py.typed` marker, included in both the
wheel and source distribution. This marks the public SDK, not the entire host
package. Checking the SDK sources directly is not sufficient packaging
acceptance: also build a wheel, install it in a fresh environment, and check
the external fixture without repository source or repository mypy settings:

```console
python -m build --outdir /tmp/broker-sdk-dist
python3.10 -m venv /tmp/broker-sdk-consumer
/tmp/broker-sdk-consumer/bin/python -m pip install mypy
/tmp/broker-sdk-consumer/bin/python -m pip install --no-deps "$SDK_WHEEL"
cd /tmp
env -u PYTHONPATH -u MYPYPATH /tmp/broker-sdk-consumer/bin/python \
  -m mypy --config-file= --strict --no-incremental \
  "$SDK_FIXTURES/broker_sdk_external.py"
env -u PYTHONPATH -u MYPYPATH /tmp/broker-sdk-consumer/bin/python \
  -m mypy --config-file= --strict --no-incremental \
  "$SDK_FIXTURES/broker_sdk_invalid_external.py"
```

Set `SDK_WHEEL` to the absolute built-wheel path and `SDK_FIXTURES` to this
checkout's absolute `tests/fixtures` path before running the commands. The
positive fixture must pass. The negative fixture must fail with both an
`arg-type` error for a numeric quote price and an `assignment` error for a
nonconforming plugin; an `import-untyped` failure does not count as acceptance.
The environment installs no host dependencies: SDK imports and the positive
fixture also run there with Python's `-I` isolated mode. No `MYPYPATH`, editable
install, `--follow-imports=skip`, or directly supplied SDK source is used in
this installed-consumer check.

No plugin discovery registry, provider network adapter, capture publication,
broker fingerprint fit or scientific qualification is claimed by this SDK.
