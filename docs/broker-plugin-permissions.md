# Broker plugin permissions (unreleased v3)

Issue #624 separates host-resource permissions from SDK capabilities and
[provider-data rights](broker-provider-policy.md). Activation requires all three.
This is an unreleased major behavior change: live invocation requires an exact
permission scope; old plaintext-secret configuration is refused. Existing SDK,
registration, lifecycle and security V1 serialized bytes are not rewritten.

## Offline declaration

Ship canonical `BrokerPermissionManifestV1` JSON at
`permission_resource_path(plugin_id, entry_point)` in the registration's wheel:
a sibling `_histdatacom_broker_permissions/<plugin-id>.json` resource, with its
SHA-256 and byte count in `RECORD`. It binds registration and implementation
hashes, distribution name/version, SDK, providers and the resource ABI.

```console
histdatacom broker-plugins permissions --plugin-id org.example.feed --json
```

This reads verified metadata without importing the plugin, loading configuration,
resolving a credential or applying grants. Missing, ambiguous, substituted or
incorrectly recorded declarations fail closed.

The operator's `BrokerPermissionBindingV1` selects the exact candidate, manifest,
SDK and `BrokerProviderConfigurationV1`. A `BrokerPermissionGrantV1` grants a
bounded time interval. Required permissions must be a subset of granted ones;
effective permissions are the intersection of granted and declared ones.
Denied optional permissions remain explicit, never fabricated as fields.

`BrokerPermissionAuthorityV1` rereads the current supplied ledger before effects.
It pins process/binding/grant and rejects expiry, revocation, backward clocks,
removed/rewritten history, and concurrent/reentrant checks. Establish
`broker_permission_scope` alongside the independently reviewed provider-policy
scope. A stored receipt is historical evidence, not a new operation permit.

## Explicit resource ABI

`resource_abi="none"` is emission-only and retains the zero-argument factory.
`resource_abi="host_resources_v1"` calls `factory(resources)` with the public,
stdlib-only `BrokerHostResourcesV1`. No objects or plaintext credentials are
inserted into primitive V1 session configuration.

| Permission | Host boundary |
| --- | --- |
| `network:provider:<id>` | Exact declared provider/origin, path-segment prefix, methods, request/response bounds and deadline; no redirects, arbitrary headers or proxy inheritance. |
| `secrets:read:<profile>` | Opaque name selects host-side endpoint authentication; no plaintext export or enumeration. |
| `cache:plugin:<id>` | Bounded ephemeral memory namespace; no filesystem/scientific-store paths. |
| `emit:quotes`, `emit:health` | Actual event kind, checked in the facade and parent persistence. |
| `emit:sizes` | Bid/ask sizes and activity/count fields. |
| `raw_payload:emit` | Raw provenance and opaque event/metadata extensions; provider rights remain required. |
| `subprocess:requested` | Reserved vocabulary; execution currently refuses even when granted. |

`BrokerPermissionResourcesV1` requires the actual reviewed provider invocation.
Rights and grants are checked freshly before transport/cache effects, including
after secret resolver callbacks. Operator-only `BrokerHostSecretProfileV1` maps
an opaque profile to a resolver handle. Known private material is refused in
responses/native outputs; this is not covert-channel DLP.

The trusted host HTTP helper bounds DNS, TLS and response reads with a parent
deadline. HTTPS verifies certificates. HTTP is allowed only for explicitly
declared loopback fixtures. This first ABI deliberately excludes query strings,
percent escapes and dot segments. Isolated workers use bounded, canonical,
invocation/epoch/sequence-bound parent RPC, not their own network access.
Operator-supplied secret resolvers, policy sources and clock callbacks remain
trusted synchronous host callbacks. The transport deadline is not a promise to
interrupt a hanging host callback or contain malicious trusted in-process code.

## Isolation and evidence

The macOS path starts trusted Python with `-I -S -B`, seals the kernel profile
before site/plugin imports, and restores reviewed import roots without running
`.pth` or `sitecustomize`. Fork/spawn/exec, writes and direct plugin networking
are denied. Approved transport occurs in the parent. Unsupported enforcement
refuses rather than falling back to unsandboxed execution.

Trusted in-process Python remains trusted, not OS-confined. The SDK exposes no
generic filesystem, product-store, experiment or certification mutation API.
Neither isolation nor grants establish provider terms or source continuity.

`BrokerPermissionExecutionV1` retains exact authority and native artifact bytes;
its reader recomputes the historical decision and native association. Lifecycle
results persist this proof beside [host-health evidence](broker-host-health.md).
Trusted results wrap the unchanged V1 receipt with the proof. Grant/manifest/
binding changes alter lifecycle identity even with the same caller nonce.

Qualification uses generated wheels, permission-set tests, actual kernel probes
and caller-owned loopback endpoints only. No live provider or release is implied.
