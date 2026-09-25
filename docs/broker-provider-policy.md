# Declared provider rights (unreleased v3)

Issue #623 introduces a breaking, default-deny runtime boundary. The host's MIT
license, a plugin's software license, successful offline conformance, and a
community registry entry do **not** grant permission to use a provider API or
retain, derive, redistribute, or publish its data.

This is an operator-reviewed declaration system, not legal advice, automated
interpretation of terms, account authentication, or scientific qualification.
The implementation and its tests use generated inputs; they do not approve any
real provider, account, historical capture, or empirical study.

## Review workflow

Before activating a provider, an operator must obtain appropriate review and
record an exact provider/configuration binding. Record software licensing
separately from provider terms: terms identity, version, date, issuer, evidence
locator and byte reference; account/feed eligibility; commercial, geographic
and account-class restrictions; attribution obligations; and the operator's
acknowledgement of the selected policy and evidence. Hashes identify retained
evidence; they do not prove its issuer, accuracy, or legal effect.

The declaration contains an explicit decision for every operation/class pair.
There are seven operations (`invoke`, `capture`, `material_use`, `derive`,
`retain_local`, `redistribute`, `publish`) and seven classes:

- Credentials or private account metadata.
- Raw provider payloads, including unrecognized opaque provider metadata.
- Normalized canonical quotes.
- Content hashes.
- Bounded health metadata.
- Derived features or statistical fingerprints.
- Broker-conditioned synthetic products.

All 49 cells must be present. `unknown` and `denied` refuse the requested
operation; missing evidence or acknowledgement also refuses it. The synthetic
conformance evidence kind is descriptive and never authorizes provider work.
Tests explicitly declare generated test terms; there is no fixture-name or
adapter-name exception in production gates.

Permission for normalized quotes or fingerprints does not imply permission for
raw payloads, and vice versa. Public dissemination has its own operation and
recipient scope. Writing to local storage is not evidence of permission to
redistribute those files or upload them to a public service.

## Explicit execution context

`provider_policy_scope(source)` requires a source whose `read_policy_context()`
returns the complete reviewed ledger. `BrokerPolicyFileSourceV1` rereads one
absolute, canonical regular file; it does not discover accounts, search for
terms, follow symlink aliases, or infer a permissive configuration.
`write_broker_policy_context()` publishes a no-clobber immutable snapshot.
Changing a review file is an operator action, not a task performed automatically
by a denied operation.

Each host operation rereads the context and checks the current clock, exact
selected configuration, effective policy interval, evidence, acknowledgements,
and revocations. An active scope refuses selection changes, ledger rollback,
process inheritance, and nested scopes. A receipt from an earlier operation is
not a reusable permit. Worker processes request a fresh parent context for each
guard; startup admission is not an unlimited session authorization.

SDK activation requires `BrokerSDKInvocationV1` via `provider_request`, binding
the exact negotiated candidate, reviewed configuration schema, public values,
opaque account/profile identity, and permitted output shape. Private values are
neither retained nor hashed into that profile. The existing security boundary
also checks known private material before persisting policy receipts.

Legacy capture and replay require `BrokerLegacyCaptureV1`, likewise supplied as
`provider_request`. Expected output shapes permit refusal before invoking a
provider; actual returned native records are independently classified. Opaque
metadata cannot be reclassified as bounded health by a caller-supplied label.

`provider_native_inputs(...)` carries detached, bounded native capture requests
or fingerprints and broker product manifests needed by downstream ID-only
contracts. This is metadata-only lineage, not permission: it performs no path
discovery and does not install a policy scope. A bare fingerprint ID cannot
substitute for its exact native parent. Neither native identities nor this
registry authenticate external provider claims.

`provider_reconstruction_inputs(...)` explicitly adds exact broker product
metadata for one source-driven downstream operation. It retains and bounds the
complete inherited root inventory, checks process identity, and grants no
rights. Standalone reuse of ID-only broker bars requires their actual product
and fingerprint roots; a manifest ID cannot substitute for those native values.

## Retention and audit receipts

Local retention needs an explicit duration policy. Existing immutable stores do
not implement scheduled deletion: they therefore refuse finite retention terms,
including requests that merely supply a deadline. Declaring a finite TTL does
not create a deletion service or establish that copies elsewhere will expire.

New native artifacts retain their existing native JSON schema, identities and
bytes. Separate `.provider-policy.json` receipts bind the actual native file's
name, byte length and digest to its closed native classification, as-of operation
decision, local retention decision, reviewed terms and evidence. Sidecars count
toward bounded storage. Review fields must never contain credentials.

Native files and individual sidecars are separate atomic writes, not a two-file
transaction. A sidecar alone is not capture completion. Verification requires
the matched native bytes; a present mismatched or invalid sidecar is refused.
Older files without sidecars have **unknown historical admission**, not
retroactively manufactured permission. Material reuse still requires a fresh
current declaration. Newly staged broker product publications require the pair
before directory promotion.

Broker-bearing bars, activity summaries, and training batches likewise require
fresh derivation and retention admission. Their original native bytes remain
unchanged; new persisted artifacts carry separate matched receipts. Training
publication still replays the complete original source and ownership inventory,
including products not selected by a row projection. Native parent bindings are
not replacements for that physical and numerical replay. Truncated activity
provenance cannot authorize omission of a used broker parent: broker aggregation
refuses when its bounded output cannot retain the complete parent inventory.
Training and activity writers refuse existing unpaired native files or sidecars
instead of silently repairing incomplete admission evidence. Historical material
may still be read or replayed under current rights, with its original admission
remaining unknown.

Retained bar-state fingerprint comparisons also require the exact broker product
and its matching embedded fingerprint. Fingerprint-only permission does not
authorize the associated product states; a source product ID is not sufficient.

Pure native JSON readers and receipt readers remain historical/structural
readers. Reading an old decision does not authorize a new capture, fit, material
read, write, or publication. Integrity checking does not prove authenticity.

## Migration and limits

This changes previously published behavior and is a SemVer-major change, not an
additive patch. Callers must supply explicit provider requests and reviewed
contexts; downstream broker products also need their exact native parent roots.
Pure offline SDK conformance and generic non-broker reconstruction are distinct
from provider activation.

Broker-backed Polars queries materialize through guarded batch reads before
returning an in-memory lazy frame. They do not return deferred disk readers that
could start provider reads after the scope ends. Generic non-broker products
retain their lazy disk scan behavior.

These are cooperative host workflow boundaries, not a hostile same-user sandbox
or universal information-flow tracker. The host cannot revoke ordinary values
already returned to a caller, authenticate a manually declared broker account,
control an unrelated filesystem reader, or prove a caller has not removed
provenance outside the host. Current admission is checked at host operation
boundaries; it is not a claim that permissions cannot change during one bounded
I/O call. Interrupted persistence can leave incomplete evidence, never a claim
of successful completion.

Integration requires qualification of all affected runtime routes and
migrations with synthetic source and installed-package tests. This document
alone does not certify those gates or authorize a production release.
