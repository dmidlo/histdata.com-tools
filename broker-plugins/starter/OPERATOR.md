# Standalone generated-only operator example

Draft for issue #621. Copy `tools/capture_example.py` into either standalone
project. It imports only documented public host package roots and stdlib, not
the project implementation, sibling projects, repository tests or private host
helpers. It requires the reviewed host APIs; a host version string is not an
API compatibility guarantee. Physical qualification is pending.

Use a clean installed environment without production credentials. Install host
and plugin wheels normally, run `pip check`, and run from a neutral directory.
Use the project's canonical installed-candidate `conformance/driver.json`.
Create the parents of the following paths yourself; the example creates fresh
leaf outputs only:

```sh
python tools/capture_example.py capture --driver conformance/driver.json --native-dir /absolute/captures/generated-run --anchor-dir /absolute/operator-anchors/generated-run --authorize-generated-execution
python tools/capture_example.py replay --native-dir /absolute/captures/generated-run --anchor-dir /absolute/operator-anchors/generated-run --authorize-generated-material-use
```

Capture resolves the installed candidate and manifest, negotiates an exact
finite workflow, and explicitly grants only quote/health emissions. Any network,
endpoint, secret, cache or subprocess declaration is refused, even if optional.
Optional size/raw emission declarations receive no grant. This example supports
only the two projects' single nonsecret `mode` schema and exact `finite` mapping;
it does not claim to handle every possible public conformance driver schema.
The HEALTH prelude must use `HEALTHY`/`INFO` and the exact closed summary
`BrokerReasonCode.HEALTHY.value`; other diagnostic summary text is classified as
raw payload by the public native resolver and is intentionally not authorized.

The operator declaration has all 49 rights cells. It allows invoke/capture/
material-use/local-retention only for normalized quotes, health and content
hashes, and denies credentials, raw bodies, derived products, dissemination and
publication. Local retention is explicitly unbounded; this immutable evidence
store is not a TTL service. Operator declarations are not verified legal rights.
The permission and policy sources are current operator-owned in-memory ledgers
re-read by the host; this is not an external revocation service. Grants/reviews
expire after one hour. A replay command creates a fresh material-use-only review
and never reinstalls a historical permission grant.

The native path is the public kernel-isolated runner with network off and no
fallback. It retains physical clocks, permission execution, health, security and
provenance. The reviewed fixture policy uses 30-second startup, 120-second run,
10-second acknowledgement, 100-millisecond shutdown, no delivery retries and
no reconnects. These are example-specific bounds, not production defaults or a
throughput claim. Unsupported isolation refuses.

The external anchor directory is separate from every native artifact and is
create-once. It retains driver, manifest and invocation before activation, and
the returned original seal, exact security receipt and outcome afterwards, even
if a returned run is partial. Security receipts are separately anchored because
the native provenance seal does not bind that sibling receipt's full contents.
Each returned seal/security/outcome write freshly checks retention rights on the
actual returned native capture/security wrapper. If rights are revoked before
those writes, only already-permitted files remain; even a partial seal is not
written under an old decision. Initial metadata is the operator's authored input.
Anchor durability failure can leave an incomplete directory. No error removes,
repairs, retries, overwrites or adopts prior outputs. Preserve failures and choose
new paths for a deliberate new attempt. CLI errors are closed and never print
raw plugin exceptions. Secure these independently retained anchors; same-user
tampering with both anchors and capture is outside this integrity guarantee.

Replay uses the retained invocation and external expected seal, not discovery or
plugin activation, so the plugin can be uninstalled. A structural inspection is
not enough: the example fully replays partitions, checks finite HQQQ vectors,
normal EOF/CLOSED, complete status/worker reaping, security receipt binding, and
anchored complete native provenance under fresh current material-use rights.
This does not assert health qualification, source continuity, scientific fitness
or provider truth. Fixed source vectors do not describe physical host latency.

Pure boundary tests:

```sh
python -m pytest -q tests/test_capture_example.py
```

Tests use generated public contracts; tests that reach native/installed calls
label their stubs explicitly. Their passing status is not capture, isolation,
conformance, packaging or scientific qualification. No physical capture or kit
execution is included in this test module.
