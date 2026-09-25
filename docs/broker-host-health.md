# Host-owned capture health (unreleased v3)

Issue #625 separates host observations from a plugin's “healthy” claim. The host
records canonical ingress, queue state/refusals, and persistence completion after
native fsync. Native V1 bytes remain unchanged; evidence lives in a separate
`<capture>-host-health` directory. Historical missing observations are unavailable,
never reconstructed from a final manifest as if measured live.

`BrokerHostHealthPolicyV1` supplies versioned SLO thresholds, bucket/sample bounds
and evidence requirements. `HostHealthRecorder` accepts trusted host clock samples
and closed observation kinds/reasons, not plugin counters. Observation retention
is bounded by the policy count and a 64 MiB byte ceiling.
Independent native replay has a separate 64 MiB canonical-input ceiling, and
individual canonical audit/metadata artifacts are bounded at 8 MiB. Oversized
evidence refuses; a larger native capture policy does not increase these limits
or authorize truncating health evidence.

The per-bucket `max_reported_source_gap_events`, `max_reconnect_events`, and
`max_clock_correction_events` ceilings default to zero. An explicitly declared
calibration policy may admit bounded, retained source outage/reconnect events
as model inputs; it does not recast them as uninterrupted source data or permit
host drops, malformed records, source reorder, or false healthy claims. Collector
corrections are counted separately from measured host/source clock jumps, whose
magnitude threshold still applies. The synthetic fingerprint fixture declares
one source outage, one reconnect, and at most one collector correction because
those events are the parameters under test. SDK native PARTIAL/incomplete runs
remain insufficient even when their source-event counts fit these ceilings.
`max_queue_saturation_ns` permits at most the declared observed full-queue
duration per bucket; its zero default refuses every observed full-queue episode.
An explicit overflow observation or overflow refusal always fails, even if a
later queue snapshot is empty or a duration allowance would otherwise apply.

Headers bind the actual plugin/provider/configuration, capture, epochs, symbols,
permission snapshot and provider-policy decision. Independent replay verifies
the entire ordered native partition inventory, ingress/persistence associations,
and observation hash. Identical prices alone do not establish a delivery retry.

Metrics include received/persisted/refused events, duplicate versus unchanged and
stale quotes, gaps/reconnects/heartbeats, queue occupancy/saturation, persistence
lag, source-clock diagnostics and known host drops, by epoch/symbol/time bucket.
Malformed or unidentified content is not assigned an invented symbol.

Rates retain denominators and Wilson intervals: 25 known drops among 10,000
eligible events yield 0.0025 with a 95% interval approximately [0.001694, 0.003688].
Timing summaries have explicit sample bounds. Little's-law diagnostics require
an explicitly declared approximately stationary window: 5,000/s × 0.012 s = 60
mean in-system events. Neither calculation assumes a Poisson/M/M/1 process.

Source timestamps are provider evidence, not authenticated clocks; differences
from host receipt are not identified network latency. Pre-boundary provider loss
remains unknown. A host cannot detect invisible upstream omissions merely
because a plugin claims health. Policies requiring identified upstream loss
therefore refuse qualification when that evidence is absent.

States distinguish qualified **host-boundary-only**, degraded, insufficient and
historical-unavailable evidence. False healthy claims are cross-checked against
observed violations. Qualification never repairs native events, invents fields,
or establishes market truth/source continuity.

`replay_lifecycle_host_health` and `replay_legacy_host_health` use concrete native
and authority artifacts. Persisted readers additionally check no-follow file
inventories and compare claimed audits with independent replay. Provider rights
remain separate. New legacy scientific fitting requires measured, replayed
health; historical absence is not a pass. Structural historical inspection
remains available without inventing observations.

A successful new native fingerprint fit writes an immutable
`BrokerHostHealthQualificationV1` under the explicit capture root's
`host-health-qualifications` directory. It binds the fingerprint bytes and the
complete sorted inventory of capture manifest, audit and SLO-policy IDs. A
different audit for the same V1 fingerprint is a conflict, not an overwrite.
`read_current_broker_health_qualification` replays the supplied actual source
captures and current provider rights; JSON deserialization alone is not
admission. This new fitting side effect requires a writable capture root. Pure
numerical fingerprint comparison is not a health certification or provenance
chain verifier; broader end-to-end chain work remains separate in #626.

Tests use synthetic captures, deliberately broken/lying plugins and independent
mathematical calculations. No provider capture or empirical campaign is run.
The installed synthetic diagnostic fixture declares a 120-second lifecycle
budget: observed fresh per-write authorization and fsync overhead can take a
seven-event Python 3.10 run beyond 30 seconds. This fixture budget is not an API
timing guarantee; production defaults and diagnostic health SLO assertions remain
unchanged. Synthetic qualification establishes neither throughput nor
real-provider readiness.
