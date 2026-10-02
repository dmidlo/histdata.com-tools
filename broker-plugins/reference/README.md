# Deterministic reference broker

This is an independently buildable, credential-free broker plugin, not a host
fixture-wheel generator. Its runtime imports only `histdatacom.broker_plugins`
and the Python standard library. It does not contact a provider, accept secrets,
read market data, or establish scientific/provider qualification.

Distribution `histdatacom-reference-broker` and plugin version are **0.1.0**.
The entry point is `org.histdatacom.reference-broker`, provider identity is
`generated-reference`, and the public SDK interval is `[1.0.0, 2.0.0)`.
Permission resources pin SDK **1.0.0** and the exact installed candidate.

## Development host requirement

These samples target the **unreleased v3 broker boundary**, whose reviewed
development wheel currently has distribution metadata **2.5.0**. That version
label alone does not establish compatibility with any historical 2.5.0 release.
The ordinary package dependency is `histdatacom`; use the reviewed development
wheel and run the explicit public API preflight. This source snapshot requires
catalog **2.0.0**, pinned exactly in `tools/check_host.py` and the fixture ABI.
No catalog-2 host wheel or installed qualification is supplied by this snapshot.
The earlier catalog-1 host artifact, SHA256
`857eece608736afd8097a475868d4b09849650eaa7a0c6ad251bb6a319b00128`
is historical only and is rejected by this snapshot's catalog preflight.
Its physical results cannot qualify catalog 2. Obtain and record a separately
reviewed compatible host artifact before the build/install commands below.

## Build and install normally

Start with a fresh virtual environment, Python 3.10 or newer. Install the
reviewed host wheel and its dependencies from an approved offline wheelhouse;
do not use editable installs or `--no-deps` to hide unsatisfied metadata.
The shared [developer guide](DEVELOPER_GUIDE.md) describes the complete public
boundary; [OPERATOR.md](OPERATOR.md) documents the companion capture/replay tool.

```sh
export PIP_NO_INDEX=1
export PIP_FIND_LINKS=/approved/wheelhouse
python -m pip install /approved/histdatacom-2.5.0-py3-none-any.whl build setuptools wheel
python tools/check_host.py
python tools/generate_metadata.py
python tools/generate_metadata.py --check
python -m build
python -m pip install "dist/histdatacom_reference_broker-0.1.0-py3-none-any.whl[test]"
python -m pip check
python -I -m pytest tests
```

For fully offline PEP 517 isolation, configure `PIP_NO_INDEX=1` and
`PIP_FIND_LINKS=/approved/wheelhouse` with the declared setuptools/wheel build
requirements available there. Alternatively, in a dedicated environment where
these requirements are already installed, `python -m build --no-isolation`
uses the same conventional build backend. The build creates both sdist and
wheel and refuses stale source/resource fingerprints. Regenerate metadata after
editing source, entry points, capabilities, configuration, or packaging.

`tools/generate_metadata.py` uses public artifact APIs without importing the
plugin. It writes canonical registration and permission package resources plus
`conformance/driver.json`; setuptools records their hashes in the wheel RECORD.
The build-time checker imports only the standard library, not the host.

Run the complete pytest suite after installing the built wheel, including the
operator-tool tests; unittest discovery alone does not run pytest-style cases.
The isolated interpreter and importlib test mode do not add the source package
to the import path. The runtime tests also require an actual `site-packages`
installation.

## Discovery and conformance

Catalog-2 source metadata requires the following complete candidate scopes.
These are planned denominators, not executed results or certification:

| Profile | Required cases | Requested catalog capabilities |
| --- | ---: | ---: |
| `trusted_contract_v1` | 28 | 7 |
| `isolated_contract_v1` | 41 | 8 |
| `dual_contract_v1` | 42 | 8 |

Each includes `session.freshness`. Trusted mode cannot certify native-only
`health.v1`; its declaration is still required for the finite health prelude.
Generate a fresh installed plan after building, rather than carrying forward a
catalog-1 plan, shortening its case list, or treating unavailable cases as passes.

```sh
python -m histdatacom.broker_plugin_registry list
python -m histdatacom.broker_plugin_conformance plan --driver conformance/driver.json --profile dual_contract_v1 --capability quotes.v1 --capability timestamps.broker-event.v1 --capability timestamps.receive.v1 --capability health.v1 --capability heartbeat.v1 --capability gaps.v1 --capability sizes.quoted.v1 --capability raw-hashes.v1 --output qualification/plan.json
```

Run the resulting complete plan in a fresh directory and independently verify it:

```sh
python -m histdatacom.broker_plugin_conformance run --plan qualification/plan.json --output qualification/run --authorize-generated-execution
python -m histdatacom.broker_plugin_conformance verify qualification/run
```

The clone-local [CONFORMANCE_ABI.md](CONFORMANCE_ABI.md) specifies all 24 scenarios.
Isolated/dual qualification requires a supported kernel backend;
unsupported is not a pass. Candidate certification is distinct from the kit's
reference-host selftests. Pure unit tests are not a replacement for conformance.

The plugin implements canonical finite, receive-only honest-health, optional
size/raw-hash, heartbeat, gap, duplicate, stale, reorder, reconnect, burst and
explicit negative scenarios. Finite output is one advisory HEALTH followed by
three EURUSD quotes with decimal lexemes `1.10000`/`1.20000`; deterministic source
timestamps are 1/2/3 seconds. `honest-health` instead uses current receive clocks
and no source-time claim. Gap output declares five missing messages; reconnect
emits three quotes then DISCONNECTED each epoch. The `next-block` and
`close-block` scenarios deliberately sleep for 300 seconds and must only be
executed through a bounded supervisor. Malformed/spread/sequence/clock, exception
and synthetic secret-canary scenarios deliberately exercise host refusal.
Every successful session opening has a fresh public 128-bit nonce, including
reopening the same plugin instance. Determinism compares the documented semantic
event tuple, not equal session IDs or complete event-envelope bytes.

## Permissions, secrets, capture and replay

Required grants are `emit:health` and `emit:quotes`; sizes/raw payload-hash
emission are explicitly optional. The factory receives public
`BrokerHostResourcesV1` under `host_resources_v1`, but this plugin requests no
network endpoints, caches, secret profiles, or subprocesses. Declarations are
not operator grants. The host must supply fresh exact candidate/configuration
permission authority and separately declared generated-provider rights.

Normal capture/replay uses the public lifecycle/security and provenance APIs,
not this metadata-only discovery command or internal fixture authority helpers.
Use the companion third-party guide's complete operator example, choose
`{"mode":"finite"}`, retain native/health/permission/provenance artifacts, and
pin the returned seal when reopening. A constructed seal or conformance report
does not authorize capture, replay or scientific material use. SDK-native
scientific fitting remains unavailable until the separate supported interface
exists; never relabel this capture as a legacy capture.

The operator commands and protected external anchor requirements are in
[OPERATOR.md](OPERATOR.md). Keep that anchor separately from the capture tree;
it pins both the provenance seal and the exact independent security receipt.

No configuration field carries a real credential. Never log payloads, raw
exceptions, configuration values, or secret material. The secret-canary mode
uses a public, synthetic test string solely to exercise host redaction. Real
plugins must use host-mediated opaque secret profiles and reviewed bounded
endpoints; they must not export credential values into artifacts.

Independent uninstall is ordinary:

```sh
python -m pip uninstall histdatacom-reference-broker
python -m histdatacom.broker_plugin_registry list
```

The plugin candidate should disappear while the host and unrelated plugins
remain. Retained artifacts remain evidence; uninstall is not artifact deletion.

## Release checklist

The manual [CI example](.github/workflows/ci.yml) requires an operator-supplied
reviewed host wheel URL and independently verified SHA256. It performs normal
dependency installation, a conventional build, all installed-only pytest tests,
and a complete trusted plan for every advertised catalog capability available
in that profile. Native-only `health.v1` qualification is deliberately excluded
there; it still requires the complete supported-kernel dual plan above. This
CI example is not an isolated-runtime certification and has no publishing job.

- Check independent plugin SemVer, SDK interval and exact public API support.
- Regenerate/check metadata, then build wheel and sdist without host source.
- Install into a clean environment; `pip check`, pure tests and full public kit
  must pass, including independently replayed complete planned reports.
- Exercise normal public capture, pinned replay, tamper refusal and uninstall.
- Preserve exact host/plugin wheel hashes, Python/backend identities and logs.
- Do not publish this staging project or claim empirical provider quality from
  generated fixtures. Real provider work remains gated separately.

Current staging status: integrated-source qualification is recorded separately
in `INTEGRATED_QUALIFICATION.md`, with exact artifact identities and test scope.
Earlier pure/build qualification applies only to its preserved prior candidate,
not these changed source bytes. Full conformance and normal
capture/replay/uninstall acceptance remain pending until explicitly recorded.
