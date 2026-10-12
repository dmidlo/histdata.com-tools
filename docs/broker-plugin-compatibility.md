# Broker ecosystem compatibility governance (unreleased v3)

Issue #622 separates installed software, executable SDK admission, canonical
event compatibility, provider declarations, and scientific qualification.
Successful installation, a parseable document, or a directory listing proves
none of the others.

## Executable SDK generations

The host implements SDK **1.0.0 and 1.1.0**. The default remains 1.0.0 so existing
metadata, inventories, sessions, captures, and serializers retain their bytes.
SDK 1.1 is an additive protocol extension; the canonical event schema remains
`histdatacom.broker-plugin.event.v1`. It is not an event migration or a new claim
about an old capture's semantics.

Registration declares `[sdk_min_version, sdk_max_version)`. The pure
`negotiate_broker_sdk` API intersects this interval with explicit executable host
generations, selecting the highest supported member unless an exact generation
is requested. A missing intersection refuses admission. SemVer build labels do
not change precedence; interval endpoints are not a license to invent an
implementation of an unknown minor or major version.

Native callers select the actual generation with
`discover_broker_plugins(sdk_version="1.1.0")`, then create the usual capability
plan. The installed permission reader accepts the same `sdk_version` keyword.
The inventory, runtime plugin metadata, permission manifest and binding, software
provenance, lifecycle header and capture compatibility decision must agree.
Admission never silently substitutes another SDK to make a package work.

SDK 1.1 adds the optional `provider.protocol.v1` capability. A plugin advertising
it implements `provider_protocol() -> BrokerProviderProtocolV1` and retains the
same typed declaration in its metadata extension namespace
`org.histdatacom.provider-protocol`. The result contains provider ID, public
protocol name and the provider's version string (not necessarily SemVer).
During actual host construction the method executes and its exact typed result
must equal the declaration. Missing methods, unknown/changed results, and a
runtime SDK mismatch refuse admission. Permission and provider-rights checks run
before and after the callback. Existing metadata-extension emission permission
remains necessary; this method grants no provider access.

SDK 1.0 never calls the new method. A historical SDK 1.0 extension using this
namespace remains opaque, byte-for-byte metadata, not typed protocol evidence.
Cached metadata and protocol access recheck current output/resource rights
without calling the plugin again. An SDK 1.1 plugin may omit this optional
capability; requesting it through the gated wrapper then reports
`unsupported_operation`, rather than inventing a protocol version. Provider
protocol declarations describe what the plugin reported, not authenticated
provider compatibility. The existing reference and starter packages remain
SDK 1.0 examples; separate installed fixtures exercise SDK 1.1 behavior.

## Deprecation and retained replay

The current content-addressed policy supports both generations for live capture
and retained replay. **No removal is currently announced.** An explicit future
live deprecation must name its announcement time and host version. Removal may
not occur before both 180 days and two host minor releases (or the next major),
and requires its declared host/time boundaries. Decisions expose machine-readable
`sdk_deprecated:<version>` and `sdk_retained_replay_only:<version>` warning codes.

Ending live admission does not end offline replay. This policy format does not
permit removing a retained replay contract. Such removal requires a separately
reviewed policy/schema successor reconciled with #635's golden corpus and #633's
compatibility registry. V1 pins both original generations; omitting one from a
custom policy is not a removal mechanism. No current registry lookup, installed
plugin, credentials or provider contact is needed to read an old capture. Current material-use rights
still apply; retained historical permission evidence is not a present grant.
These notice requirements apply to supported public SDK symbols as well as
whole generations. Removing or changing a required method is not an optional
addition: it requires a new major contract or an explicit, tested adapter.

## Capture-bound version axes

New native SDK captures retain an `environment.v2` document with the exact SDK
decision, support policy, host version, chosen SDK version, candidate registration,
plugin and distribution versions, native header/invocation identities, provider,
and event schema. The unchanged provenance header hashes the **entire** document;
the native chain and final seal bind that header. It is not an unbound sidecar.

At startup provider protocol information is explicitly not yet observed. Actual
SDK 1.1 declarations appear in the sealed lifecycle identity records after the
typed method handshake. `read_lifecycle_capture_compatibility` verifies actual
native provenance and returns those exact runtime metadata bytes and observed
protocol versions, without activation or discovery. A package upgrade changes
the candidate, decision and capture identity even if its event output is equal.

Original `environment.v1` captures use the original reconstruction branch. The
new accessor returns `None` for their unavailable historical governance evidence;
it does not infer a retrospective decision or rewrite old bytes. Later replay
reconstructs new decisions from the **retained** policy and capture-time facts,
not today's catalog or installed distributions.

Native callers may supply the bounded `conformance_reference` keyword to
`run_broker_plugin_lifecycle`. Its exact catalog/version, report and evidence
inventory must identify the actual candidate before a capture directory is
created. Those bytes enter the sealed compatibility declaration. The default is
honestly unavailable, not the latest report found in a registry. Parsing a report
is not independent requalification, and no host self-test is relabelled as plugin
certification. The historical V1 header's certification fields retain their old
meaning and are not promoted by a supplied report; use the explicit new reference.
Historical reference reads validate the exact report identity, case inventory,
and complete-denominator assessment against its retained catalog, not a later
kit's case list. That read model has no current certification interface; ordinary
conformance report assessment still requires the current kit catalog.

## Directory and schema evolution

`BrokerPluginDirectoryEntryV1` is an offline metadata record, not a network
registry service. It retains maintainer, public package/source URLs, the exact
candidate declaration (provider IDs, SDK range and capabilities), latest exact
conformance evidence when available, and community/first-party/deprecated/
unverified status. Every status retains the explicit nonclaim: no endorsement,
provider authorization, security audit or scientific qualification. A first-party
label does not bypass any native permission, provenance or scientific gate.

Canonical schema direct-read relations, migration reachability and semantic
equivalence remain separate #632–#636 contracts. SDK 1.1 keeps event V1; it does
not register an event migration. Any future migration must retain predecessor
identity and the #636 receipt; a lossless claim needs #634's independently proven
semantic projection/hash invariant. Lossy conversion cannot promote historical
captures into a new semantic contract.
