# Installed broker-plugin discovery

Issue #616 introduces the public host-side `histdatacom.broker_plugin_registry`
namespace. It discovers **declarations**, not activated plugins. Neither an
installed wheel, compatible SDK interval, selected declaration, nor inventory
receipt establishes permission, capability admission, legal/data rights, feed
quality, or scientific qualification. Those gates remain in #617–#626 and the
real-feed workstreams. No broker connection, credential loading, plugin download,
or capture execution occurs here.

The reserved entry-point group is **`histdatacom.broker_plugins.v1`**. This page
freezes it for the first time: #616's original reference to a group frozen in
#615 was premature. The independent public SDK remains version `1.0.0`; its
contracts and dependency isolation are unchanged.

## Wheel registration

Use a normal Python entry point, with the stable lowercase namespaced plugin ID
as its name. A distribution may advertise multiple IDs/implementations, and one
implementation may declare multiple provider IDs. Example package configuration:

```toml
[project.entry-points."histdatacom.broker_plugins.v1"]
"org.example.feed" = "example_feed.plugin:factory"

[tool.setuptools.package-data]
example_feed = ["_histdatacom_broker_plugins/*.json"]
```

The module must live inside a package, with a pure-Python `.py` module or package
`__init__.py` listed in that distribution's wheel `RECORD`. Its entry-point
object reference is recorded but **not resolved or imported** by discovery.
The symbol's existence and runtime conformance must be tested during separately
authorized activation/conformance, not inferred from metadata.

Ship a static JSON declaration at the deterministic path returned by
`registration_resource_path(plugin_id, entry_point)`. For the example this is
`example_feed/_histdatacom_broker_plugins/org.example.feed.json`. Generate its
complete schema and content ID from the public contract:

```python
from pathlib import Path
from histdatacom.broker_plugin_registry import BrokerPluginRegistrationV1

registration = BrokerPluginRegistrationV1(
    plugin_id="org.example.feed",
    plugin_version="1.2.0",
    display_name="Example feed",
    distribution_name="example-feed",
    distribution_version="1.2.0",
    entry_point="example_feed.plugin:factory",
    sdk_min_version="1.0.0",
    sdk_max_version="2.0.0",
    provider_ids=("example",),
    capabilities=("health.v1", "quotes.v1"),
    source_revision="abcdef1234567",
    source_repository="https://example.org/example-feed",
    build_id="build-42",
)
Path("src/example_feed/_histdatacom_broker_plugins/org.example.feed.json").write_text(
    registration.to_json(), encoding="ascii"
)
```

Create the resource's parent directory when scaffolding the package. Provider
and capability tuples must be sorted, unique, bounded lowercase identifiers.
Capability strings are advertised declarations only; this registry does not
define their operational meaning or negotiate requirements. The public source
revision, repository, and build fields are optional (empty string means absent).
Repository URLs must use HTTPS without credentials, query strings, or fragments.
Do not put private account identifiers, credentials, or configuration values in
any declaration field. Arbitrary raw installation-origin URLs are not copied.

Distribution names normalize runs of `.`, `_`, and `-` to lowercase `-`.
Distribution versions must exactly match installed package metadata and remain
distinct from the plugin's SemVer version. Both the descriptor and implementation
module must belong to the same concrete distribution, carry SHA-256/size entries
in its `RECORD`, and match their recorded bytes. Namespace packages are not
assumed to have a single distribution owner.

This implementation uses distribution-scoped entry-point metadata rather than
`EntryPoint.load()`. See the primary
[Python metadata documentation](https://docs.python.org/3.10/library/importlib.metadata.html),
[entry-point specification](https://packaging.python.org/en/latest/specifications/entry-points/),
and [installed-project RECORD specification](https://packaging.python.org/en/latest/specifications/recording-installed-packages/).

## Offline CLI and API

```console
histdatacom broker-plugins list --json
histdatacom broker-plugins inspect --plugin-id org.example.feed --json
histdatacom broker-plugins inspect --provider example --json
histdatacom broker-plugins select --plugin-id org.example.feed --version '>=1.0.0,<2.0.0'
histdatacom broker-plugins permissions --plugin-id org.example.feed --json
histdatacom broker-plugins list --snapshot ./plugin-inventory.json
python -m histdatacom.broker_plugin_registry list --json
```

`--json` and `--snapshot` work before or after the subcommand. Output is JSON by
default. `--config` is deliberately unsupported: these commands do not read the
host's YAML settings or any secret configuration. Invalid arguments produce a
closed reason code rather than echoing arbitrary user input. Failures return
exit status 2; inspection of a complete incompatible inventory can succeed
while clearly reporting no compatible candidates.

```python
from histdatacom.broker_plugin_registry import (
    discover_broker_plugins, inspect_broker_plugins, select_broker_plugin,
)

inventory = discover_broker_plugins()
declarations = inspect_broker_plugins(inventory, provider_id="example")
selected = select_broker_plugin(
    inventory, plugin_id="org.example.feed", version_constraint="==1.2.0"
)
```

Each discovery call rescans installed metadata and verifies current resources.
There is no persistent discovery cache; installing/uninstalling a wheel can
change the next inventory in the same process. Inspection returns all matching
valid declarations, including duplicates and SDK-incompatible versions. The
CLI inventory also retains bounded diagnostics for malformed declarations.
Selection refuses an incomplete inventory, unknown selection, incompatible SDK,
or ambiguity. Errors identify installed candidate IDs; CLI diagnostics also
include validated plugin/distribution identities and compatibility.

Stable IDs use the SDK's lowercase namespaced grammar; arbitrary module paths
are not selectors. SDK compatibility is the half-open SemVer interval
`[sdk_min_version, sdk_max_version)`. Supported plugin-version constraints are
comma-separated `==`, `>=`, `<=`, `>`, and `<` comparisons (at most eight).
SemVer numeric prerelease ordering is respected. Build labels do not affect
range precedence, while `==` matches the exact version including its build
label. No implicit newest-version, filesystem-order, import-order, or provider
preference exists. Explicit constraints must leave exactly one compatible
candidate. A provider filter cannot conceal another active build with the same
plugin ID; indistinguishable same-version duplicates remain refused.

Candidates sort by plugin ID, SemVer precedence, exact version, normalized
distribution identity, entry point, and content identity. Metadata is read and
validated before this ordering; no plugin code is evaluated during either
stage. Diagnostic ordering is deterministic as well.

## Inventory persistence and existing manifest linkage

The immutable `BrokerPluginInventoryV1` snapshot lists **discovered**, not used,
plugins. Candidate identities bind canonical registration plus exact descriptor
and entry-point-module hashes. `registration_sha256` hashes the external JSON
file's actual bytes: pretty versus compact equivalent JSON can therefore produce
different installed candidate identities even when the declaration ID matches.

```python
from histdatacom.broker_plugin_registry import (
    capture_plugin_inventory_metadata, read_plugin_inventory,
    verify_capture_plugin_inventory, write_plugin_inventory,
)

reference = write_plugin_inventory(inventory, "./plugin-inventory.json")
restored = read_plugin_inventory(reference)
metadata = capture_plugin_inventory_metadata(
    restored, selected_candidate_ids=(selected.artifact_id,)
)
# Pass metadata as BrokerCaptureSessionV1.public_metadata when constructing
# an actual capture session. Later, with that authoritative session:
# verify_capture_plugin_inventory(session, read_plugin_inventory(reference))
```

Writes atomically create a new local file, allow identical-byte retries, and
never overwrite different evidence. Readers check regular-file/no-symlink
admission, byte count, SHA-256, artifact kind/reference ID, canonical bytes, and
all nested content IDs. Directory/FIFO inputs fail without blocking. Restoring
a retained snapshot does **not** prove those distributions remain installed;
rediscover to inspect the current environment.

Capture metadata binds the full persisted inventory's ID and hash plus explicit
selected candidate IDs. Omitted selections mean none are asserted selected.
Binding rechecks exact-version selection, SDK compatibility, and duplicate
refusal; it does not assert those selected plugins were activated or used.
Verification reconstructs and validates the authoritative capture session, so
mutating metadata without a new session identity is rejected.

Legacy reconstruction experiment v2.4 explicitly rejects broker/provider
artifact domains. This restriction is unchanged, not bypassed through a renamed
domain or fake executable-module hash. Instead, the separate versioned
`BrokerPluginExperimentInventoryV1` software-environment envelope contains the
exact original experiment ID, complete inventory, and explicit selections:

```python
from histdatacom.broker_plugin_registry import (
    BrokerPluginExperimentInventoryV1, bind_plugin_inventory_to_experiment,
    verify_plugin_experiment_inventory,
)

# experiment is an authoritative ReconstructionExperimentManifestV1,
# e.g. obtained from read_reconstruction_experiment(...).
envelope = bind_plugin_inventory_to_experiment(experiment, inventory)
reference = write_plugin_inventory(envelope, "./experiment-plugin-inventory.json")
restored_envelope = read_plugin_inventory(
    reference, artifact_type=BrokerPluginExperimentInventoryV1
)
verify_plugin_experiment_inventory(restored_envelope, experiment)
```

Binding/verification independently reconstruct the supplied existing experiment
through its public contract, not merely a syntax-valid ID. Reading an envelope
alone verifies its own bytes; external linkage requires that authoritative
manifest. No legacy manifest bytes, identities, release/campaign state, or
broker-data admission policy change. An envelope is not a scientific certificate.

## Bounds, trust, and qualification

V1 admits ordinary unpacked wheels with concrete standard-library
`PathDistribution` metadata. Editable/zip/custom-provider registrations, missing
RECORD ownership/hashes, native-only entry-point modules, symlinks, malformed
metadata, and oversized inputs fail closed. Bounds are 128 declarations or
diagnostics, 64 KiB descriptor bytes, 8 MiB implementation-module bytes, 16,384
distribution file records, and 256 KiB canonical inventory/envelope bytes.
Canonical payloads reject unknown fields, duplicate JSON keys, nonfinite/floating
numbers, excessive depth/collections, wrong scalar types, and altered identities.

Python's metadata finder machinery is trusted host infrastructure and can itself
be extended with code. Metadata-only inspection is not an OS sandbox. Installed
files/metadata are read sequentially, not as an atomic filesystem snapshot;
concurrent installation or mutation can cause refusal and warrants a fresh scan.
The implementation digest covers the entry-point module only, not its imported
dependencies or the complete distribution supply chain. RECORD is local
integrity metadata, not an independent signature or package authenticity proof.
No automatic downloads occur. Stronger supply-chain and process-isolation policy
remains #619; operational capabilities/permissions remain separate gates.

The maintained wheel fixture deliberately raises if imported, proving discovery
and inspection do not execute its module. Focused tests exercise real metadata
and RECORDs, ordering/version/duplicate refusals, mutation/size/path failures,
canonical persistence, and actual capture/experiment contracts.
`tests/fixtures/qualify_broker_plugin_installation.py` runs inside a clean
installed-host environment: pass its separately built fixture-wheel path; it
installs with `pip --no-index --no-deps`, discovers/selects/inspects in that same
process, uninstalls, and verifies the original empty inventory returns. Build
the host wheel first; do not place checkout `src` on `PYTHONPATH` for this gate.
