# Broker-plugin trust, privacy and execution security

Issue #619 adds the opt-in `histdatacom.broker_plugin_security` API. Its policy
version is `1.0.0`, independent of the host and SDK versions. It preserves the
SDK, registry, capability and native lifecycle wire contracts. Installation is
a software supply-chain decision: neither installation nor these receipts grants
provider rights, certifies market data, or activates a production broker.

## Threat model and trust tiers

`BrokerSecurityPolicyV1` binds an exact selected candidate to one of
`first_party`, `reviewed_third_party`, or `untrusted_local_development`.
First-party/reviewed labels are explicit operator assertions, not automatic
conclusions from package names, signatures or a community listing.

There are two deliberately different execution modes:

- `kernel_isolated`: the host qualifies a macOS kernel profile **before**
  resolving credentials or starting the plugin. Unsupported systems, failed
  probes and unsupported network policies refuse; there is no unsandboxed
  fallback. Development/untrusted plugins require this mode.
- `trusted_in_process`: an explicitly authorized first-party/reviewed plugin
  runs synchronously in the caller's Python process. It has the caller's ambient
  OS/Python authority. Python stdout/stderr and ordinary logging are discarded
  during calls, but native descriptors, additional threads, native extensions,
  hangs and arbitrary Python introspection are not contained. Use a quiescent,
  caller-owned process. A policy requiring kernel protection cannot use it.

The trusted computing base includes the host, selected Python interpreter,
installed runtime/code roots, import machinery, macOS kernel and filesystem.
The boundary is not a claim of safety against a malicious local user, kernel
vulnerability, hostile privileged process or arbitrary covert channels. A user
who can replace the host/interpreter or forge all evidence can forge a receipt.

## Actual kernel enforcement and its limits

The qualified backend uses `/usr/bin/sandbox-exec` before Python startup. Apple
marks this facility deprecated; its availability and profile behavior must be
rechecked at invocation. Apple's supported application packaging alternative is
[App Sandbox](https://developer.apple.com/documentation/security/protecting-user-data-with-app-sandbox),
not a portable promise supplied by this Python package. Linux and Windows
strict execution are currently unsupported; portable policy/read/refusal APIs
remain available.

The profile is default-deny with Apple's `system.sb` runtime support, and adds:

- Readable selected interpreter/venv roots, the host package source or installed
  site-packages root, `/System/Library`, `/usr/lib`, and `/opt/local/lib` for a
  MacPorts interpreter. MacPorts also admits the exact existing, resolved
  `/opt/local/libexec/openssl3/lib` runtime-library directory, never its broader
  `libexec` parent. The fresh working directory itself is readable so
  ordinary `getcwd` works. **These readable roots must contain no credentials.**
  Installation metadata and other packages inside them are readable too.
- Filesystem metadata reads are allowed separately; content confidentiality does
  not include hiding file names, sizes, existence or metadata.
- All new filesystem writes are denied, including cache/temp writes. The host
  does not describe cleanup, polling or deadlines as a disk quota. The public
  `BrokerHostResources` cache is bounded **memory only**, with item/byte limits;
  it is not a filesystem grant or an automatically exposed IPC service.
- Only required inherited transport descriptors survive worker launch. Such
  already-open descriptors remain intentionally usable; the profile is not an
  assertion that all inherited descriptors become inaccessible.
- Process descendants inherit kernel restrictions. Lifecycle process-group
  cleanup/deadlines remain the independent host termination boundary. Neither
  that mechanism nor this profile is a blanket hostile-code sandbox guarantee.

Runtime and protected paths are resolved, and overlapping/aliased protected
roots are refused. The host tests actual content reads, new writes, truncation,
rename, hard links, symlinks, unlink, aliases and descendant writes, including
the real scientific artifact writer against disposable protected stores.
Store contents remain unchanged. No test reads real host credentials.

Only `HOME`, `TMPDIR` and `LANG` are supplied to the worker environment. Home and
cwd refer to a fresh host-owned temporary directory, not the operator's home.
Ambient credential variables, proxy variables and loader variables are not
forwarded. Raw credentials are never placed in process arguments/environment.
Runtime path/profile strings are ephemeral host launch details, not receipt
identity; no public artifact retains a user-specific installation path.

## Endpoint, proxy and TLS ownership

Strict mode supports network **off**, or outbound access to an exact declared
set of loopback ports. Other loopback ports and direct external endpoints are
denied. There is no wildcard external-network fallback. A declared local server
or transport proxy is a separate, caller-owned authority: the host neither
creates it silently nor assumes that it validates provider TLS.

`transport_owner` is the plugin. `tls_owner` is the plugin or an explicitly
caller-owned proxy. `proxy_owner` is `none` or `caller`. These declarations do
not assert that a TLS handshake or certificate check was performed:
`provider_tls_verified_by_host` is always false. Trusted in-process mode may
declare provider-owned transport, but does not claim kernel endpoint control.
Plugins/proxies must configure explicit endpoints, proxies, certificate
verification and trust roots; disabling TLS checks is not an implication of
software compatibility. Real provider deployment and additional network
backends are outside this offline qualification.

## Explicit secrets and private identifiers

The host receives public configuration separately from opaque secret handles.
A caller-owned `BrokerSecretProvider.resolve(handle)` returns a bounded string.
Every delivered field must be named in policy and marked `secret=True` in the
actual SDK configuration schema before `open_session`. Undeclared/missing
bindings and public values under a secret field refuse. Numeric secrets should
be declared and delivered as strings, not silently coerced into scientific
numbers. Handles, secret values and known private account identifiers are not
public policy/provenance and are never hashed into scientific identity.

The worker receives values through the bounded ephemeral configuration pipe.
Known-secret guards run before worker frame serialization and before host
persistence. The parent checks the **entire** inventory/plan/header, including
unselected candidates and diagnostics, plus metadata, session, events and the
security sidecar. Nested JSON strings are traversed with depth/size bounds;
known plaintext, JSON escaping, URL encoding, common base64 and SHA-256 forms
are refused. A scientific value containing private material is refused, never
silently rewritten/redacted into a different market observation.

This is bounded known-material detection, not proof that arbitrary encodings,
substrings, exfiltration over an admitted transport, or covert channels can be
recognized. Public metadata fields are declarations and must not contain
unclassified private account data. Pass known private identifiers explicitly.
Do not authorize an untrusted provider with a real credential merely because a
canary suite passes.

Errors use closed `BrokerSecurityReason` values and suppress exception chaining;
standard formatted tracebacks do not echo provider/resolver messages. This does
not erase Python memory, exception-context objects, debugger locals or caller
references. Ephemeral dictionaries are cleared on exit, not claimed zeroized.
Strict worker stdout/stderr are drained, counted and discarded under lifecycle
bounds; no plugin traceback or output stream is copied into host logs.

## Software provenance and publication boundary

`installed_software_provenance` freshly verifies the selected package candidate,
then records normalized distribution/version, SDK identity, exact registration
and entry-module SHA-256, a closed installer classification, and sanitized origin
kind. Raw `direct_url.json`, credentials/query strings, local paths and URL
digests are not retained. Available declared archive hashes and VCS commit IDs
are distinguished from unavailable wheel hashes. Entry-module verification is
not whole-wheel/dependency-tree verification or an atomic filesystem snapshot.

An optional attestation is bounded public software evidence whose hash may be
retained. `attestation_verified` remains false: a hash is not a signature check.
No plugin download/install occurs during scientific execution. Installation
metadata is untrusted declaration, not proof of publisher trust or rights.

The SDK and worker IPC expose no scientific/product publication operation or
product-store descriptor. `BrokerHostResources.publish_scientific_product`
explicitly refuses; unknown resource operations also refuse. In strict mode,
direct calls to host scientific writers and direct filesystem operations are
additionally denied by the kernel. Trusted in-process mode's API restriction is
not a promise to stop a trusted caller from bypassing it with ordinary Python.
Permission-set grants and provider legal/data-rights admission remain #624/#623,
not inferred from this security policy.

## Running and retaining evidence

Use `run_secure_broker_plugin(inventory, plan, policy, public_configuration,
symbols, output_directory, authorize=..., secret_handles=...,
secret_provider=..., protected_paths=...)` for the strict native lifecycle.
Authorization, backend availability and capability/policy checks precede secret
resolution. Kernel qualification precedes provider import and capture output.
Before a security sidecar may retain public configuration, full native replay
must prove a verified configuration-schema handshake for **every** started
worker epoch. Schema secret classification is checked before value validation
and independently reconciled by the parent. A blocked import, factory/schema
failure or early cancellation without a verified handshake refuses the security
receipt entirely; only sanitized native partial health evidence remains.

The result contains the unchanged native lifecycle result and a versioned
`BrokerSecurityReceiptV1`. A create-once sidecar named
`<output-directory-name>-security.json` is written beside the native directory;
it does not add an unexpected file inside a legacy V1 capture. The sidecar binds
the exact native manifest, security policy, sanitized software provenance and
canonical **public** configuration. Different credentials producing identical
native evidence do not change identity. Real clocks and observed behavior can
change evidence; secrets are not replaced with secret fingerprints to hide it.

`read_security_receipt` validates exact canonical bytes and self-consistency.
`verify_security_capture` cross-checks the authoritative native manifest and
selected software identity. `replay_broker_lifecycle` separately verifies every
native partition and its semantics before yielding records. Recomputed hashes
do not authenticate the executing host: these are not unforgeable kernel
attestations, signed execution certificates or scientific admissions.

`run_trusted_broker_plugin` returns a bounded `BrokerTrustedSecurityReceiptV1`
with exact native admitted events, plan, metadata, session and installed-module
binding. Its reader recomputes admission and stream identity checks. It makes
no asynchronous shutdown or capture-completeness claim. Deterministic fixture
event bytes match strict-process results without relabeling SDK timestamps or
squeezing events into the older legacy capture schema.

## Qualification

The maintained external fixture imports only the public SDK on its positive
path. Runtime-generated canaries prove successful explicit credential delivery
to a caller-owned offline loopback transport as well as secret leak refusal.
Tests cover trusted/native parity, credential-independent identity, sanitized
installation provenance, malformed payloads, excessive output, exceptions,
blocked import/open/next/close, subprocess death, cancellation, protected-store
attacks, exact loopback admission, unsupported platforms and receipt tampering.
They use synthetic assets and do not require a broker, secret manager, legal
terms approval, external network service or real account.

The standalone installed-wheel qualification is
`tests/fixtures/broker_security_qualification.py`. Public typing fixtures are
checked against an installed wheel without `PYTHONPATH`/`MYPYPATH`. These focused
checks do not claim the future independently published #620 certification kit,
provider quality, continuity or empirical #488/#601 qualification.
