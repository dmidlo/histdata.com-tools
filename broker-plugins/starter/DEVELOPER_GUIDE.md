# Develop and qualify a standalone broker plugin

Draft for issue #621. The reference and starter are independent source projects,
not plugins loaded from a host checkout. This guide must ship inside each project
together with `CONFORMANCE_ABI.md`, metadata tools, tests and the public-API
capture/replay example. Their clean installed qualification is still pending;
this document is not a conformance certificate or release announcement.

## Before starting

Use Python3.10 or newer and a reviewed development host wheel containing the
public SDK, discovery, capability, permission, provider-policy, lifecycle,
security, provenance and conformance APIs described here. The current work is
on the unreleased v3 host track; the SDK is independently versioned `1.0.0`
and each example distribution/plugin starts at `0.1.0`.

The project declares a normal `histdatacom` dependency. That is an installation
dependency, not an assertion that every historical host provides these APIs.
The current development wheel still reports host metadata `2.5.0`; that number
alone is insufficient compatibility evidence. Run the explicit public SDK/API
preflight and qualify the exact reviewed host wheel. Do not invent a released
3.x dependency, ignore an unsatisfied requirement, or infer SDK compatibility
from the application's version. Registration declares the separate SDK interval.

This source snapshot targets conformance catalog `2.0.0` and its exact identity
in the included fixture ABI. The earlier catalog-1 host wheel SHA256
`857eece608736afd8097a475868d4b09849650eaa7a0c6ad251bb6a319b00128`
is historical evidence only, not compatible qualification for this snapshot.
No catalog-2 host wheel or installed qualification is supplied here. Obtain a
reviewed artifact that passes the updated preflight, then record the wheel
actually used for every execution. Do not relabel catalog-1 reports or silently
upgrade their meaning; retain their exact artifacts and compatible verifier.

Work in a disposable directory without production credentials. Copy just the
chosen standalone project outside the host repository; it must not need the
other example, repository `tests`, a source `PYTHONPATH`, or editable host code.
The offline examples below assume the operator has supplied `HOST_WHEEL` and a
reviewed `WHEELHOUSE` containing the host's declared dependencies plus the
project's build/test tools, including setuptools, wheel and the pytest test extra.
Both paths must be absolute. The package-index settings below also apply to
PEP 517's isolated build environment; an earlier pip command's flags alone do
not constrain that later environment. These commands do not download packages
or authorize publishing.

```sh
python3.10 -m venv .venv
. .venv/bin/activate
export PIP_NO_INDEX=1
export PIP_FIND_LINKS="$WHEELHOUSE"
python -m pip install --no-index --find-links "$WHEELHOUSE" "$HOST_WHEEL" build
python -m pip check
python tools/check_host.py
```

Keep packaging/build execution separate from plugin runtime. Host-side metadata
and operator tools may use documented public host package roots. The plugin
implementation itself imports only `histdatacom.broker_plugins`, stdlib and its
own declared provider dependencies.

## 1. Implement the SDK protocol

The public `BrokerPluginV1` protocol is structural: no private base class is
required. Implement `metadata`, `configuration_schema`, `open_session`,
`instruments`, `subscribe`, `unsubscribe`, `iter_events`, and idempotent
`close_session`. Match runtime plugin ID/version/SDK to the installed declaration.
Validate exact configuration types and reject unknown values without echoing
their contents. Preserve exact decimal strings, clock roles and absent values.

One session owns one ordered stream with contiguous local sequence numbers.
Source sequence duplicates and source-clock regressions remain observations,
not reasons to erase quotes or rewrite them into local order. Normalize symbols
only through explicit discovered instruments. Do not assume suffixes, pip sizes,
sizes, volume, history or a missing source timestamp.

Keep generated fixture behavior separate from a future provider implementation.
The included `CONFORMANCE_ABI.md` specifies every canonical fixture scenario,
including deliberately malformed or blocked behaviors used only by bounded
tests. A malformed returned object in an adversarial fixture is not a technique
for normal market data. Never run blocking fixture modes without the kit's
supervised deadlines.

## 2. Declare capabilities honestly

Registration advertises what the implementation can emit or do, not what it has
been certified to do. The host negotiates operations and optional/required fields
separately. An undeclared non-null field is refused rather than dropped.

Even quote-only fixture certification needs the baseline emission declarations
listed in `CONFORMANCE_ABI.md`: common finite tests emit HEALTH, and quote tests
use source/receive times. Native reconnect additionally emits connection evidence.
Health qualification has further gap/heartbeat scenarios. Declare those actual
outputs without claiming that a quote-only report independently certifies them.
Qualify every claimed certification capability against its complete catalog
inventory. Leave sizes/raw/history unsupported when the implementation cannot
provide them honestly; a driver mapping cannot confer support.

SDKv1 has no historical execution method. Capture/replay of retained host
artifacts does not mean the plugin supports provider history or backfill.
Source time, receive time and host ingress/persistence time are different facts.
Passing fixed generated vectors is not a latency or throughput qualification.

## 3. Register an ordinary wheel entry point

Use the public entry-point group `histdatacom.broker_plugins.v1`, mapping a
stable namespaced plugin ID to its package module and factory. The reference ID
is `org.histdatacom.reference-broker`; the starter uses `org.example.starter`.
Keep distribution name, plugin ID, Python import package and provider ID distinct.
Change every corresponding declaration when adapting the template.

The project metadata tool creates canonical registration, candidate-bound
permission resources and the declarative conformance driver using public APIs:

```sh
python tools/generate_metadata.py
python tools/check_host.py
python tools/generate_metadata.py --check
python -m build
```

Registration belongs at the public `registration_resource_path` under the plugin
package; permissions use `permission_resource_path`. Both must be included in
the wheel with normal SHA256/size RECORD entries. Candidate identity binds the
registration and exact entry-module bytes. Regenerate after changing entry
source, identity, versions, permissions or configuration. The build freshness
check must refuse stale metadata; do not disable it to ship an old driver.

Use a normal backend-generated sdist/wheel, not the host kit's fixture-wheel
generator as the external project. Rebuild the sdist outside the repository to
prove it contains its own build tools/resources. Wheel/source hashes identify
bytes, not publisher trust, rights or a complete dependency attestation.
The standalone tests require an installed plugin; run them after the clean
installation in step5, not against an uninstalled source tree.

## 4. Run the complete public conformance kit

The project ships `conformance/driver.json` and the full fixture ABI. The driver
binds one installed candidate and permission manifest; changing either requires
a newly generated driver. It maps public scenario configuration, not executable
callbacks, expected verdicts or a hand-picked passing case set.

After installing the project wheel in the fresh runtime environment described
in step5, inspect the public catalog and create a complete candidate plan:

```sh
histdatacom-broker-conformance catalog
histdatacom-broker-conformance plan --driver conformance/driver.json --profile trusted_contract_v1 --capability quotes.v1 --subject installed_candidate --output quote-plan.json
histdatacom-broker-conformance run --plan quote-plan.json --output new-quote-run --authorize-generated-execution
histdatacom-broker-conformance verify new-quote-run
```

Use fresh plan/run paths. Add repeated `--capability` flags for the other
certification claims; do not reduce the generated case inventory. Keep the
report's exact subject, profile, capabilities, catalog, host and driver identities.
Required skips, errors, unsupported backends and unrun cases are not passes.
The target reference/starter qualification also includes native/dual profiles
for their supported claims on the supported macOS qualification host.

`trusted_contract_v1` does not prove kernel containment. Native/dual profiles
include named trusted negative probes as well as isolated native cases; the
report preserves actual backend labels. Unsupported isolation refuses without
fallback. A `host_reference_selftest` report is not candidate certification.
No production endpoint, real credential or real-provider rights are authorized
by `--authorize-generated-execution`.

Retain and independently verify the entire bundle. Re-reading one bundle is
report replay, not a fresh physical execution. Determinism and dual equivalence
require the kit's two actual runs, while retaining their distinct clocks/roots.

## 5. Install alongside a clean host

Use a second fresh environment and normal dependency resolution with the exact
reviewed host wheel and the independently built project wheel. Set absolute
`PLUGIN_WHEEL` and `PROJECT_COPY` paths; the latter is the standalone source
project copied outside the host checkout. Preserve discovery before installing
the plugin, then test only the installed runtime:

```sh
python3.10 -m venv runtime-venv
. runtime-venv/bin/activate
python -m pip install --no-index --find-links "$WHEELHOUSE" "$HOST_WHEEL"
histdatacom broker-plugins list --json > host-before-plugin.json
python -m pip install --no-index --find-links "$WHEELHOUSE" "${PLUGIN_WHEEL}[test]"
python -m pip check
python -I -m pytest -q "$PROJECT_COPY/tests"
```

Run from a neutral directory without source import paths. Check actual module
origins are installed site-packages, not the copied project or host checkout.
Do not qualify an editable install or use `--no-deps` to conceal an unsatisfied
dependency contract. The host application stays separately installed; the
plugin wheel does not replace host files or install privileged internal hooks.

Before plugin installation retain a metadata-only host inventory. Later uninstall
the exact plugin distribution and compare fresh discovery with that baseline:
`histdatacom-reference-broker` for the reference, `histdatacom-broker-starter`
for the template. Do not uninstall dependencies or delete captured evidence.
The host must remain usable and retained replay must work after plugin removal.

## 6. Inspect and discover without activation

For the reference project:

```sh
histdatacom broker-plugins list --json
histdatacom broker-plugins inspect --plugin-id org.histdatacom.reference-broker --json
histdatacom broker-plugins select --plugin-id org.histdatacom.reference-broker --version '==0.1.0'
histdatacom broker-plugins permissions --plugin-id org.histdatacom.reference-broker --json
```

Use `org.example.starter` for the starter. Discovery verifies static installed
metadata and recorded implementation bytes without importing/activating the
plugin. `list` and `inspect` expose diagnostic and compatibility information,
including duplicate or incompatible declarations; inspect those results rather
than treating their presence as admission. Selection and activation enforce
fail-closed compatibility and identity gates. Discovery commands do not accept
execution configuration or provide a
capture subcommand. Their success grants neither invocation nor data rights.

## 7. Keep secrets outside configuration and artifacts

These projects need no secret. Public configuration contains only generated
scenario choices; never add account IDs, tokens, passwords or private URLs to
registration, driver, CLI arguments, logs or provenance. Avoid raw exception
messages and provider payload dumps. SDK diagnostic strings are authored public
summaries; pattern-based hygiene is not proof that arbitrary text is secret-free.
The finite no-raw example uses the exact closed diagnostic code value as its
summary. Other free-form diagnostic text is conservatively treated as raw
payload by provider policy; a harmless-looking message is not an exemption.

A future reviewed authenticated integration uses the public host-resource ABI
and opaque secret-profile names. The operator owns secret resolution and exact
network origin/path/method/time bounds; plaintext secret values are not exported
to the worker. Legacy secret fields in primitive configuration are refused for
new activation. The fixture's public test canary is not a real credential.

Permissions, provider terms and capabilities are separate. Emit permission does
not grant raw retention, and an API license does not authorize publication.
Never turn generated fixture declarations into reusable live-provider policy.

## 8. Capture and replay through the public host API

Use the included operator example, not a direct call to the plugin factory.
The example must use only public host package roots and require explicit
generated-execution authorization. It must construct and explain:

1. Fresh installed inventory and an admitted capability workflow.
2. Exact `BrokerProviderConfigurationV1`, output shape and
   `BrokerSDKInvocationV1` for the installed schema and reviewed public values.
3. Generated-only operator terms/evidence/acknowledgement and a complete current
   provider-policy matrix, denying publication/redistribution. Local retention
   must be explicitly unbounded because this immutable store is not a TTL service.
4. Exact candidate/configuration-bound permission authority granting only needed
   declared emissions, plus normal resources for a resource-ABI factory.
5. Both current `provider_policy_scope` and `broker_permission_scope`, and an
   explicit caller-owned authorization decision.
6. Public `run_secure_broker_plugin` with reviewed finite lifecycle bounds and
   supported kernel isolation. An unavailable backend is not silently weakened.
7. Normal complete terminal state, actual worker cleanup, retained permissions,
   health, security and provenance, followed by full `replay_broker_lifecycle`.
8. `read_lifecycle_capture_provenance` with an independently retained expected
   seal, requiring verified, complete, anchored replay under fresh current rights.
   Separately retain and compare the original security receipt as well: the
   native seal does not itself pin that separately written receipt's bytes.

The host-side script is `tools/capture_example.py`. Its current construction
and pure stub tests are separate from pending physical capture qualification.
After installing the reviewed host and plugin, use fresh leaf paths whose
operator-owned parent directories already exist:

```sh
python tools/capture_example.py capture --driver conformance/driver.json --native-dir /absolute/captures/generated-run --anchor-dir /absolute/operator-anchors/generated-run --authorize-generated-execution
python tools/capture_example.py replay --native-dir /absolute/captures/generated-run --anchor-dir /absolute/operator-anchors/generated-run --authorize-generated-material-use
```

The two authorization flags are distinct, explicit local operator declarations.
Capture grants only `emit:health` and `emit:quotes`; even optional network,
secret, cache and subprocess declarations refuse. Optional size/raw emissions
are not granted. The example supports only the supplied single nonsecret
`mode` schema, finite mapping and EURUSD symbol, not every general SDK driver.
It accepts both projects' declared factory resource ABIs without substituting
a factory or changing their manifests.

Permission grants and provider reviews expire after one hour and are read from
current operator-owned in-memory sources, not an external revocation service.
Replay creates a fresh material-use-only review and no plugin grant. Failed or
partial attempts remain at their original paths; the script never repairs,
retries, overwrites or deletes them. Protect the separately retained anchor:
tampering with both it and the native bundle is outside the integrity guarantee.
See the accompanying operator notes for exact budgets, output scope and limits.

Structural `inspect_broker_lifecycle(...).complete` does not hash partitions.
Reading an expected seal from the same mutable bundle immediately before
verification supplies no independent anchor. Retain the original invocation
and seal separately; a past decision or conformance report is not current
permission. Replay should require no plugin activation, network or broker
credential and must remain possible after uninstall under fresh material-use
rights. Intentionally partial conformance scenarios remain partial data.

## 9. Keep software qualification separate from provider science

Conformance proves exact software-contract behavior for the retained subject,
profile and capability set. Provenance proves retained integrity relative to
its independently held anchor, not broker truth or upstream completeness.
Neither grants data rights, measures production throughput, qualifies a provider
account, supplies historical availability, or proves representative market data.

SDK-native scientific fitting remains unavailable here; do not convert an SDK
capture into a legacy fit by inventing adapter/session metadata. Real provider
deployment and empirical qualification remain separately gated by issue #488
and the applicable source-rights/study work. This task performs generated tests
only and does not authorize a release, publishing or a workflow dispatch.

## Before calling a project ready

Use the included release checklist and CI example, with an explicitly supplied
compatible reviewed host. Validate complete project tests and fresh sdist/wheel
builds, metadata freshness refusals, normal dependency resolution and `pip check`,
installed-only imports, complete claimed conformance profiles, actual public API
capture/pinned replay, intended permission/integrity refusals and independent
uninstall. Repeat on Python3.10 floor and the qualified current interpreter.
Record exact artifacts, platforms, terminal exits and report identities; preserve
failed attempts. No release or issue completion follows from this draft alone.
