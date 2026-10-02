# Broker-plugin conformance kit (unreleased v3)

Issue #620 supplies an installed-package test kit for generated software-contract
fixtures. It requires no source checkout or broker credentials. It does not grant
provider rights, certify a broker feed, qualify scientific fitting, or run an
empirical campaign. Kit, package, SDK and provider versions are separate.

## Exact subject and complete denominator

`BrokerConformanceDriverV1` binds one installed candidate and permission manifest,
a reviewed public configuration schema, provider ID, symbols and a
scenario-to-configuration mapping. It contains no executable callback,
replacement factory, transport override, expected verdict or passing-test list.
Discovery is metadata-only; execution rechecks the actual installed entry point
and its bytes. Changed installed code/configuration is a changed test subject.

The current `canonical-fx-v1` fixture ABI uses `EURUSD` and explicit generated
scenarios. External plugins implement that fixture mode through their declared
configuration. Production origins are refused; resource declarations must use
literal loopback origins. Generated authority is not real provider permission.
Use a separate test environment without production credentials.

The catalog fixes the required cases per capability/profile. Every planned case
remains in the report as `pass`, `fail`, `error`, `unsupported` or `not_run`.
Missing scenarios, unavailable backends, cancellation and runner deadlines never
pass. A capability passes only when its nonempty required inventory passes
completely. Quotes do not certify sizes, raw hashes or history. SDK-v1 has no
executable history interface, so history remains explicitly unsupported.

Reports distinguish `installed_candidate` from `host_reference_selftest`.
Reference tests cannot certify another candidate. A compliant stop-and-wait SDK
has only one outstanding delivery: an ordinary burst cannot demonstrate host
overflow. The separate `host-adversarial.v1` reference case injects bounded ACK
loss and slow persistence and requires an actual queue-saturation refusal, not
a timeout. Provenance mutation cases likewise test retained evidence explicitly.

## Profiles and evidence

| Profile | Scope |
| --- | --- |
| `trusted_contract_v1` | Installed SDK execution and negative probes in fresh bounded processes; no kernel/native-chain certification. |
| `isolated_contract_v1` | Kernel-isolated native lifecycle, health and provenance cases, plus explicitly identified trusted SDK negative probes. |
| `dual_contract_v1` | The combined scope plus separate trusted and isolated executions for semantic equivalence. |

These are explicit mixed case inventories, not a claim that every SDK probe ran
inside a kernel sandbox. Native kernel execution is currently qualified on
macOS; unavailable backends never silently fall back to weaker execution.
Trusted candidate code remains trusted code. A subprocess alone is not a
hostile-code isolation guarantee.

The versioned `catalog` command lists discovery/SDK/capability, permissions,
provider-policy refusals, symbols/quotes/clocks, duplicates/stale messages,
gaps/heartbeats/reconnects, backpressure, malformed input, exceptions,
cancellation/shutdown, secret hygiene, health, replay and provenance gates.
Negative cases require their intended host gate: a generic exception is not a
spread, sequence, clock or permission test pass. The actual gated SDK exposes
closed `BrokerCapabilityError.event_failure` codes where the host can establish
the exact predicate. Producer-thrown errors cannot impersonate those codes.
No rejected payload, derived hash, configuration or raw exception is retained
in that diagnostic. Historical error `to_dict()` bytes remain unchanged.

Native evidence includes actual manifests, health audits, permission execution
and [provenance seals](broker-plugin-provenance.md). Verification independently
replays those sources and expected roots. Integrity is not provider truth.
Same retained evidence produces identical canonical report bytes; fresh runs
retain their actual clocks, nonces and roots. Deterministic-fixture repeatability
and cross-backend equivalence compare declared SDK semantics without erasing
the two physical executions' identities.

Catalog `2.0.0` adds the required `session.freshness` gate. It compares the
public `instance_nonce` values from two independently executed finite runs,
not just session artifact IDs: changing the opening timestamp does not hide
nonce reuse. Verified native captures also require distinct nonces across
their distinct reconnect openings. Replay determinism and dual equivalence
continue to require equal declared event semantics and now also require fresh
physical session identities. The named freshness case does not replace the
separate semantic checks.

Reading the same retained bundle twice is not opening a new session. Both reads
keep its original nonce, and no global historical nonce cache is maintained.
This gate establishes observed uniqueness in the tested executions, not random
entropy or guaranteed uniqueness in all future runs. The generated fixture uses
fresh UUID nonces; its `reused-session-nonce` and
`reused-session-nonce-changing-time` faults deliberately violate that contract.

The catalog major version is independent of the host package, SDK and artifact
wire versions. Complete plans and certificates pinned to the earlier catalog
are not silently upgraded. Preserve their original evidence and use their exact
compatible reader; build new plans and qualify the complete new denominator.

Passing isolated native cases also require confirmed worker reaping and replay
against the retained expected seal. Ordinary positive finite scenarios require
normal completion and a complete anchored capture. Deliberately partial
scenarios, including reported gaps, reconnects and cancellation, must instead
match their expected terminal outcome and closed pinned journal; a useful event
followed by an unrelated failure is not a passing test.

The separate `honest-health` fixture reports actual generated receive clocks
but deliberately does not claim a source timestamp. The SDK retains
`supported_not_reported` and the host audit retains missing-source-clock and
unknown-upstream evidence. Its advisory healthy claim concerns the measured
receive/host boundary only; it does not certify source continuity, timestamp
coverage or scientific use. Source-timestamp and drift tests remain separate.
Deterministic replay/equivalence fixtures retain their fixed source vectors;
those vectors are not asserted to match the host's physical persistence pace.

Native qualification uses an explicit fixture policy: 30-second startup,
120-second run (240 seconds for reconnect), 10-second acknowledgement and
100-millisecond shutdown limits. The reference overflow case deliberately uses
a 10-millisecond acknowledgement interval with eight retries. These settings
are retained in each native manifest; they are not the public runtime defaults.
Passing this kit does not qualify latency or throughput under the default
500-millisecond acknowledgement interval, or under a caller's different policy.

## Installed CLI

In an environment with the intended host wheel already installed:

```text
histdatacom-broker-conformance catalog
histdatacom-broker-conformance fixture --output generated-fixture
python -m pip install --no-index --no-deps generated-fixture/histdatacom_conformance_fixture-1.0.0-py3-none-any.whl
histdatacom-broker-conformance plan --driver generated-fixture/driver.json --profile trusted_contract_v1 --capability quotes.v1 --output plan.json
histdatacom-broker-conformance run --plan plan.json --output new-generated-run --authorize-generated-execution
histdatacom-broker-conformance verify new-generated-run
```

`python -m histdatacom.broker_plugin_conformance` is the equivalent module entry
point. The execution flag authorizes generated testing, not live broker use.
Existing run directories are not overwritten. `run`/`verify` print canonical
JSON and return nonzero for noncertified results; inspect their per-case reasons.

`fixture --fault ...` builds genuinely different installed code/declarations;
available names are listed by `fixture --help`. Use a fresh fixture directory
and environment per subject. Faults cover fabricated optional fields, missing
capabilities, overprivilege, secret leaks, reconnect duplication, false health,
malformed events and nondeterministic output. Host fault injection and retained
chain mutations are separate reference cases, not no-op plugin fault labels.
Use `plan --subject host_reference_selftest --capability host-adversarial.v1`
with a native-capable profile for that separate reference inventory; its results
cannot produce installed-candidate certification.

## Library use and limits

`histdatacom.broker_plugin_conformance` exports the catalog and canonical
driver/scenario/plan/report contracts, `plan_broker_conformance`,
`run_broker_conformance`, `verify_broker_conformance`, and
`build_broker_conformance_fixture`. Planning derives the entire denominator.
Cancellation leaves unexecuted cases visible. Readers bound canonical evidence
and reject unknown fields, incorrect types, duplicate keys and overflows.

Reports are reproducible diagnostics, not signatures authenticating their
producer. JSON parsing or manually constructed passing rows are not execution
proof: reopen the retained bundle with `verify_broker_conformance`. Independently
retain trusted report/root identities if substitution detection matters. Native
trusted-directory/host threat limits still apply.

Conformance does not upgrade historical scientific fingerprints or turn SDK
captures into a legacy fit. Current provider policy, permissions, host-health
qualification and scientific capture-root replay remain separate requirements.
