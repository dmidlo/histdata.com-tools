# Host-owned capture provenance (unreleased v3)

Issue #626 adds an ordered, tamper-evident host journal to actual SDK lifecycle
and legacy host-health capture execution. It does not authenticate a broker's
market data, recover invisible upstream omissions, certify throughput, or protect
against a compromised recording host. Historical native V1 files are unchanged;
old captures without this journal remain inspectable but cannot acquire a new
scientific qualification by reconstructing observations afterward.

## Recording and identity

The host writes a sibling `<capture>-provenance` directory containing canonical
`header.json`, `chain.jsonl`, `checkpoints.jsonl`, `seal.json`, and exact native,
health, authority, invocation and public-environment metadata. The closed file
inventory is checked during replay. Files must be regular, singly linked and
opened without following symlinks. Nonblocking opens refuse FIFOs without
waiting for a writer. Interrupted tails and missing seals are not repaired.

The header binds the actual plugin/provider/configuration, native capture and
schema, host version/public runtime environment and start clocks. SDK captures
also bind the installed distribution, entry-module and registration hashes,
SDK version, permission manifest/context/decision/grant, and provider-policy
decision. Legacy captures retain their actual adapter/session identity; they do
not invent installed-distribution or SDK permission facts. Feed identity is
explicitly absent where the native contract declares no separate feed identity.
The environment is a public runtime description, not a dependency SBOM or source
tree attestation. It contains no local paths, hostname, environment variables or
credentials. Configuration digests cover the existing reviewed public
configuration, never opaque secret resource contents.

The header's conformance status is currently `unavailable`, with no kit version
or receipt claimed. Running a conformance test does not retroactively certify
the capture that supplied its evidence. Versioned status fields distinguish
unavailable evidence from a candidate qualification; a successful hash replay
does not manufacture the latter.

Each entry carries a contiguous host chain sequence, explicit connection epoch,
closed kind and exact canonical payload. The journal interleaves:

- admitted ingress, before transport duplicate suppression;
- actual fsynced native event/control records;
- actual host ingress, persistence, refusal, queue and close observations;
- the terminal native-manifest, health-audit and permission-execution bindings.

Provider sequence numbers, native capture sequence and host chain sequence are
different quantities. Exact retries remain observable; equal-price new quotes
are not deleted. Reconnects introduce new epochs, including honest interleaving
of already-observed older ingress. Native persisted epochs cannot regress.
Malformed or forbidden input is represented by its permitted refusal evidence,
not an invented raw payload or hash. Raw provenance and optional fields retain
their actual source classifications: a hash/health label is not a rights waiver.

For algorithm `sha256-canonical-json-chain-v1`, with canonical ASCII JSON:

```text
h0 = SHA256(header_bytes)
hi = SHA256(bytes.fromhex(h_previous) + entry_bytes)
```

The fixed 32-byte preceding digest makes concatenation unambiguous. The header
binds the algorithm/version. Checkpoints bind exact prefix membership; the final
seal binds the root, entry/checkpoint inventory, final epoch and terminal record.
Stop UTC/monotonic clocks come from the actual host CLOSE observation, not replay
time. A partial native run remains partial even if its retained chain is intact.

The default policy checkpoints every 64 entries and bounds the journal to
1,000,000 entries, 65,536 checkpoints, 8 MiB per encoded entry and 256 MiB total.
Native/health readers have independent, sometimes smaller limits; enlarging one
policy does not enlarge another. Verification is bounded sequential replay, not
a Merkle random-access service. Overflow refuses instead of truncating evidence.

## Independent offline verification

`histdatacom.broker_plugin_provenance` exposes canonical contracts and pure
`verify_provenance_chain`. Pure verification checks the ordered cryptographic
structure; it does not perform current-rights checks or native replay. Both an
expected header ID and expected seal ID are needed for its anchored result.

Native readers additionally replay actual native partitions and host-health
observations, verify historical authority bindings, and compare their ordered
bytes and terminal identities with the chain. They validate ingress-to-native
persistence associations and exact delivery retries. They require current
provider material-use rights before access and again before returning, without
activating the provider, acquiring broker credentials or accessing the network.
Retained permission/provider decisions are historical evidence, not a current
grant installed into the caller's process.

```python
from histdatacom.broker_plugin_provenance import (
    read_lifecycle_capture_provenance,
)
from histdatacom.broker_plugin_policy import provider_policy_scope

# All arguments below are caller-supplied reviewed or retained native values.
with provider_policy_scope(current_reviewed_policy_source):
    proof = read_lifecycle_capture_provenance(
        native_capture_directory,
        retained_native_manifest,
        provider_request=reviewed_invocation,
        expected_root=independently_retained_seal,
    )
    assert proof.complete and proof.anchored
```

For legacy captures, use `require_legacy_capture_provenance(root, manifest,
provider_request=..., expected_root=...)`; `root` is the parent of the session
directory. The SDK reader takes the native capture directory itself.

Without an expected seal, a complete chain is only
`self_consistent_unanchored`. Obtaining the expected seal from the same mutable
directory immediately before checking it establishes no independent trust.
Keep the original seal/header identity under the caller's separate retention
controls. Even independently pinned roots prove retained identity/integrity,
not broker truth. The trusted-directory model excludes concurrent hostile
filesystem mutation and a host capable of rewriting both data and trusted roots.

## Scientific admission and retained lineage

New legacy fitting returns `BrokerDeliveryFingerprintV2`: unchanged V1
statistics plus every exact source manifest/output contract/provenance header
and seal. Its outer fingerprint identity binds that complete inventory.
Historical V1 parsing and numerical inspection remain supported; a V1 artifact
cannot authorize new broker-conditioned science.

`broker_fingerprint_sources(*roots)` declares explicit current-process source
locations for downstream replay. It grants no rights and supplies no trusted
verification boolean. Each source must occur below exactly one declared root;
missing or ambiguous locations refuse. Every material boundary replays the
actual sources against the retained expected seals and current provider rights.
New retained products carry their complete V2 fingerprint parents, so reopening
them cannot rely solely on an earlier process-local scope.

New native publications retain their provider-policy receipt and complete
scientific-parent companion. Material readers require the applicable companions;
deleting both is not a way to downgrade a V2 artifact to historical metadata.
If authority expires before native publication, already-authorized companions
may remain without a native output. Such an incomplete publication is not
repaired or accepted as an idempotent success. Pure historical metadata parsing
remains distinct from material reuse.

```python
from histdatacom.broker_capture import broker_fingerprint_sources

with provider_policy_scope(current_reviewed_policy_source):
    with broker_fingerprint_sources(explicit_capture_root):
        # Invoke the desired scientific consumer with its retained V2 parent.
        consume_qualified_fingerprint(retained_v2_fingerprint)
```

Successful fitting also requires replayed host-health qualification. A valid
chain cannot turn degraded/insufficient health into qualified observations.
SDK lifecycle captures support provenance verification but remain explicitly
unavailable for scientific fingerprint fitting: no conversion invents legacy
session metadata, clock roles or price representations. That separate
same-interface fitter obligation remains in parent #614.

Validation uses generated captures and deliberate failures only. It does not
establish provider representativeness or authorize an empirical campaign.
Ordinary generated lifecycle fixtures use a 120-second run budget; the
three-epoch reconnect fixture uses 180 seconds. Fresh authority checks and
fsynced journals must complete before their intended assertions, including on
the supported Python floor. Production deadlines and dedicated timeout tests
are unchanged; these test allowances are not throughput or latency guarantees.
