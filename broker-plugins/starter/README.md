# Standalone broker-plugin starter

Copy this directory alone into a new repository. It contains source, metadata
generation, SDK-only example, tests, CI and documentation; no parent checkout,
host fixture factory, private runtime, broker account or credential is required.
This is generated software-test data, NOT a broker integration or empirical feed.

## Three independent versions

- Plugin/distribution: `histdatacom-broker-starter` **0.1.0**.
- Public SDK implemented: **1.0.0**, declared compatibility **[1.0.0, 2.0.0)**.
- Host boundary: **unreleased v3**. The reviewed development host wheel currently
  has distribution metadata **2.5.0**; the number 2.5.0 alone does NOT establish
  compatibility with permissions, conformance or provenance APIs used here.

This source snapshot requires catalog **2.0.0**, pinned exactly in
`tools/check_host.py` and the fixture ABI. No catalog-2 host wheel or installed
qualification is supplied by this snapshot. The earlier catalog-1 host SHA256
`857eece608736afd8097a475868d4b09849650eaa7a0c6ad251bb6a319b00128`
is historical only and is rejected by this snapshot's catalog preflight. Its
physical results cannot qualify catalog 2. Obtain and record a separately
reviewed compatible development artifact from your operator/host project. This
starter does not download a substitute or invent a future released version.
`tools/check_host.py` rejects missing APIs, unsupported SDK versions, and changes
to the reviewed public conformance catalog. API presence is not a conformance
certificate. Normal project dependency is
`histdatacom`; no unsatisfied version pin or `--no-deps` workaround is required.

## Build and inspect (no plugin activation)

Catalog-2 source metadata requires these complete candidate scopes. They are
planned denominators, not executed results or certification:

| Profile | Required cases | Requested catalog capabilities |
| --- | ---: | ---: |
| `trusted_contract_v1` | 24 | 5 |
| `isolated_contract_v1` | 37 | 6 |
| `dual_contract_v1` | 38 | 6 |

Each includes `session.freshness`. Trusted mode cannot certify native-only
`health.v1`; its declaration is still needed for the finite health prelude.
Generate a fresh installed plan after building. Do not reuse catalog-1 plans,
shorten required case lists, or treat unavailable cases as passes.

Create a fresh Python 3.10+ environment. Set `HOST_WHEEL` to the reviewed absolute
host wheel path and `WHEELHOUSE` to a prepared offline directory containing that
wheel and its ordinary runtime/build/test dependencies. Verify its SHA256 first:

```sh
python -m venv .venv
.venv/bin/python -m pip install --no-index --find-links "$WHEELHOUSE" "$HOST_WHEEL" build setuptools wheel
.venv/bin/python tools/check_host.py
.venv/bin/python tools/generate_metadata.py
.venv/bin/python tools/generate_metadata.py --check
.venv/bin/python -m build --no-isolation
.venv/bin/python -m pip install --no-index --find-links "$WHEELHOUSE" 'dist/histdatacom_broker_starter-0.1.0-py3-none-any.whl[test]'
.venv/bin/python -m pip check
.venv/bin/python -I -m pytest -q tests
.venv/bin/histdatacom broker-plugins inspect --plugin-id org.example.starter --json
.venv/bin/histdatacom broker-plugins permissions --plugin-id org.example.starter --json
```

Ordinary `python -m build` also works when its isolated build environment can
resolve setuptools and wheel. Build hooks use only stdlib to reject stale
declarations before creating a wheel/sdist. They never import the plugin.
Regenerate after changing source, identities, schema or project metadata. Test
installed wheels, not editable installs: metadata-only discovery requires actual
wheel RECORD ownership/hashes. No entry-point implementation is imported by
discovery/inspection.

Run the installed SDK example directly, without pretending this is host capture:

```python
from histdatacom_broker_starter.plugin import factory
from histdatacom.broker_plugins import validate_broker_event_stream

plugin = factory()
session = plugin.open_session({"mode": "finite"})
try:
    plugin.instruments(session)
    plugin.subscribe(session, ("EURUSD",))
    for event in validate_broker_event_stream(plugin.iter_events(session), session):
        print(event.to_json())  # generated public fixture data only
    plugin.unsubscribe(session, ("EURUSD",))
finally:
    plugin.close_session(session)
```

## Public conformance and actual host capture

`conformance/driver.json` is generated from the same candidate/permission hashes
as the wheel. Do not edit it independently or reuse it after changing source.
The configured finite data and negative modes are isolated test fixtures, not
provider output. [CONFORMANCE_ABI.md](docs/CONFORMANCE_ABI.md) specifies the exact
public scenario vectors. All scenarios required by this example's declared
capabilities are implemented; only raw/sizes and host-reference overflow are
absent. Implementation and unit tests are not a claim that the full installed
conformance matrix has passed; archive its actual report before asserting that.

```sh
histdatacom-broker-conformance catalog
histdatacom-broker-conformance plan --driver conformance/driver.json --profile trusted_contract_v1 --capability quotes.v1 --output plan.json
# Explicit authorization for generated testing only; no broker/provider grants.
histdatacom-broker-conformance run --plan plan.json --output generated-run --authorize-generated-execution
histdatacom-broker-conformance verify generated-run
```

The quote-only report certifies only its exact requested subset. To exercise
all advertised evidence capabilities, include `--capability health.v1`,
`--capability heartbeat.v1`, `--capability gaps.v1`,
`--capability timestamps.broker-event.v1` and
`--capability timestamps.receive.v1` when planning, and select a native-capable
profile on a supported macOS host for health/stale/reconnect/cancellation gates.
The package never advertises sizes, activity, raw hashes, history or precision.

Missing scenarios, `unsupported`, `not_run`, and generic errors are not passes.
Use a fresh process for each installed invocation. The host's normal public
capture/replay API requires reviewed independent provider-rights and permission
scopes; plugin installation or this driver supplies neither. There is no broker
capture command in the discovery CLI. The [developer guide](DEVELOPER_GUIDE.md)
and [operator example](OPERATOR.md) cover public-API capture/replay without
private authority helpers. Operator tests explicitly stub native calls; they
do not establish actual capture, isolation, or conformance qualification.
SDK events are not silently converted into legacy scientific captures.

## Adapt the template

1. Rename distribution, Python package, plugin ID, provider ID, entry point and
   matching build tooling together. Keep plugin SemVer separate from SDK/host.
2. Replace only the generated transport portion of `StarterPlugin`. Preserve
   exact session, symbol, subscription, sequence, optional-field and cleanup
   semantics. Unsupported data stays `None`; do not invent volume/source time.
3. Declare only implemented capabilities. The example requests no network,
   subprocess, cache, sizes, raw payload or secrets and uses zero-argument factory
   `resource_abi="none"`. Capabilities are not resource grants or data rights.
4. For approved resources use public `BrokerHostResourcesV1` and explicitly
   change the resource ABI/declaration; pass opaque secret profile IDs to host
   HTTP authentication, never plaintext secrets in session configuration.
5. Read [fixture ABI](docs/CONFORMANCE_ABI.md), [logging/redaction](docs/LOGGING.md)
   and the [release checklist](docs/RELEASE_CHECKLIST.md).
6. Regenerate, build, install normally, run unit/typing/conformance/capture/replay
   gates and archive exact identities. Uninstall only this plugin distribution:
   `python -m pip uninstall histdatacom-broker-starter`.

RECORD is integrity metadata, not a signature. Its implementation hash covers
the entry module, not arbitrary dependencies. Passing conformance validates
software-contract behavior under the tested profile; it does not establish
real feed quality, provider permission, scientific fitness or live deployment.
