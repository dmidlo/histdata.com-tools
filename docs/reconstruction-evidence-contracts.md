# Point-in-time reconstruction evidence contracts

`histdatacom.reconstruction_evidence` converts deterministic source-quality
facts into bounded, versioned constraints that can condition reconstruction.
The contracts are provider-neutral; the only executable compiler in the
current milestone is the HistData.com ASCII tick (`ASCII/T`) adapter.

> **Current milestone boundary:** HistData.com cache data is the only admitted
> source. OANDA, other historical providers, live broker feeds, and
> broker-conditioned delivery remain later-milestone work. Naming a provider
> in a policy does not qualify it: the current public planner accepts exactly
> `supported_provider_ids=["histdata.com"]`.

## Why the evidence is a sidecar

Quality evidence has several incompatible grains. An exact invalid quote can
be attached to one immutable source-row identity, but a suspicious gap belongs
to an interval and a fingerprint normally describes a partition, period, or
series. Copying the latter values onto every row would falsely increase their
support and could leak evidence from later observations.

`ReconstructionEvidenceRecordV1` therefore labels each scalar as one of:

- `row_fact`;
- `interval_finding`;
- `series_fingerprint`;
- `advisory`;
- `default`; or
- `unavailable`.

Each record retains its source artifact ID and SHA-256, source partition,
metric/rule, calculation basis, source and target grain, support interval,
availability and as-of timestamps, information mode, projection method,
readiness, confidence, row identity when exact, and explicit limitations.
`PointInTimeEvidenceProjectionV1` binds those records to one source partition
and reconstruction window without embedding complete tick rows or reports.

## Point-in-time semantics

The compiler uses half-open support, `[support_start_ns, support_end_ns)`, and
rejects supplied events outside it. Duplicate timestamps retain distinct
source-row IDs and source order. Observed timestamps, bid, ask, volume, and row
identity are never rewritten.

Legacy four-column caches are labeled explicitly and their unavailable
row-quality columns become a limitation, not an implicit clean result. Complete
`histdatacom.ascii-tick-training-features.v1` caches contribute sparse true
row flags by source-row identity. Objective duplicate, ordering, and spread
facts are reconciled against the immutable quotes; disagreement refuses the
projection. Matching and additional cached finding counts remain bounded
sidecars.

In `ex_post_reconstruction`, evidence available for the completed historical
window may be used. In `ex_ante_simulation`, future rows, aggregate values, and
finding counts are withheld. An unavailable marker describes what was hidden;
it does not retain the hidden value. Exact quality-report findings project to a
row only when symbol, period, timestamp, row location, support, and availability
all agree with the immutable source row. A decision also ignores any record
whose declared as-of time is later than the stage use time. Aggregate findings
remain sidecars.

## Policy and threshold precedence

`ReconstructionEvidencePolicyV1` is content-addressed and part of plan
identity. Threshold resolution is deterministic:

1. classification profile;
2. time-series fingerprint;
3. bounded quality payload;
4. quality report; then
5. an explicit policy fallback.

The suspicious-gap fallback is versioned rather than hidden in feature code.
The wide-spread threshold uses an explicit supplied threshold when available;
otherwise it derives from the observed non-negative spread median and the
policy multiplier. If no baseline exists, its state is `unavailable` unless a
policy minimum explicitly permits a fallback. A failed supplied quality status
refuses use; a warning applies the policy's versioned source-quality penalty.
The policy also bounds every projection to 256 records, including at most 64
row records; truncation changes readiness to `limited` and records the omitted
count. A policy must reserve at least 24 sidecar slots so row findings cannot
crowd out mandatory thresholds, hard counts, cache reconciliation, and
availability evidence.

## First-party flow

The public `ReconstructionPlanSpecV1` carries the policy through
`ReconstructionClient.construct_plan()`. Planning writes and hash-binds the
policy, declares it in the information manifest, and refuses a provider set
other than HistData.com.

At runtime:

1. source enrichment compiles one HistData projection per participating
   partition, scores usable source evidence, and refuses hard or unavailable
   evidence according to policy;
2. proposal conditioning receives the source-quality score and propagates the
   projection/use lineage;
3. carving applies the resolved anchor-gap and spread constraints and writes a
   bounded decision into every carved-batch ledger record;
4. synchronized reconciliation and identity delivery preserve projection and
   decision IDs; and
5. validation re-evaluates availability at its declared use time, stores the
   resulting use/refusal decision in the staging descriptor, and binds the
   bounded projection and decision IDs into the committed delivery-quality
   manifest and its content hash.

This makes a quality constraint auditable from the source artifact through the
publication gate. It does not turn a quality report into historical truth, and
it does not repair source values.

## Typed use

```python
from dataclasses import replace

from histdatacom.reconstruction import (
    ReconstructionClient,
    ReconstructionEvidencePolicyV1,
)

policy = ReconstructionEvidencePolicyV1(
    suspicious_gap_fallback_ms=86_400_000,
    wide_spread_multiplier=4.0,
)

spec = replace(existing_histdata_plan_spec, evidence_policy=policy)
client = ReconstructionClient()
plan_ref = client.construct_plan(spec)
```

Adapters for a future source may compile the same record and projection
contracts. They must be separately implemented, qualified, and admitted by a
future planner milestone; no OANDA behavior is inferred by these contracts.
