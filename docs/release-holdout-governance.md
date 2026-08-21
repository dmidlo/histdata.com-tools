# Sealed release-holdout governance

The v2.5 release decision uses a fresh protected holdout. The benchmark
`final_holdout` is not release evidence because its identity and aggregate
results informed earlier implementation rounds. A release holdout is eligible
only when its manifest was committed while sealed and before a release
candidate was fitted or evaluated.

The executable contracts live in
`histdatacom.synthetic.release_holdout`. They are deliberately row-free: a
manifest records periods, half-open nanosecond intervals, counts, source
partition identities and hashes, exact signatures, 64-bit neighbor sketches,
cohesion groups, and coverage strata. It never records quote or event rows.

## Lifecycle

```text
development/validation identity units
              |
              v
  exact + near-neighbor + temporal audit ---- fail ---> replace split
              |
              v
     predeclared coverage audit -------- insufficient -> reduce claim
              |
              v
      sealed content-addressed manifest
              |
              | candidate inputs may use calibration, validation,
              | and public policy only
              v
       immutable candidate graph
              |
              v
        one-time authorization
              |
              v
 atomic opened-and-consumed reservation
              |
              v
       one evaluation report/receipt
              |
              v
 mandatory retirement; a non-pass requires a fresh successor
```

Creating an evaluation reservation happens before the evaluator callback. If
the callback raises, the holdout remains consumed and receives an operational
failure receipt. It cannot be retried. The caller must use one durable,
authoritative ledger directory for a release program; copying the manifest or
choosing another ledger is not a valid governance operation.

## Split and leakage rules

Each protected window is a whole split unit. Anchor neighborhoods, context
events, and declared cohesion groups cannot cross development, validation, or
holdout roles. Every release window must begin after the development source
cutoff. Protected windows must not overlap or fall within the predeclared
temporal-neighbor guard.

The row-free leakage audit checks:

- repeated source partition identities or source hashes;
- exact source, motif, and context signatures;
- near source, motif, and context identities;
- overlapping or guarded-neighbor time intervals; and
- reused cohesion, anchor-neighborhood, or context-event groups.

Near-neighbor identity uses a frozen 64-bit hexadecimal sketch. For sketches
$a$ and $b$, the distance is

$$
d_H(a,b)=\operatorname{popcount}(a\mathbin{\mathtt{xor}}b).
$$

A pair is a near duplicate when $d_H$ is no larger than the policy threshold.
Exact SHA-256 signatures remain the authoritative exact-identity evidence;
the sketch only adds a conservative near-neighbor screen.

## Coverage and claims

Before authorization, the manifest must cover every policy value on these
axes:

| Axis | Required v2.5 strata |
| --- | --- |
| Feed epoch | early, qualified transition, modern |
| Session | Asia, London, New York, overlap/closure |
| Event context | ordinary, event |
| Observation uncertainty | high-retention/low-infill, central, low-retention/high-infill |
| Alignment | exact, bounded-nearest |
| Deficit | low, median, high |

Missing coverage produces `insufficient_evidence`. It never relaxes the split,
neighbor, or cohesion rules. A narrower scientific claim requires a new
predeclared policy and manifest; it is not inferred after results are opened.

## Candidate independence

`ReleaseCandidateFreezeV1` binds strong artifact references for every surface
that could otherwise learn from the holdout:

- fitting;
- preprocessing;
- support tuning;
- smoothing;
- engine selection;
- observation-scenario policy; and
- adaptive policy.

Each artifact declares only calibration, validation, or public-policy input
roles. The graph also binds the validation-only Hawkes product-selection
dossier and the already-sealed manifest identity. Authorization fails unless
the graph, selection dossier, leakage audit, coverage audit, and strong file
references all agree.

The holdout report has no candidate-selection role. A pass may support the one
frozen release decision. A scientific failure, insufficient result, or
operational failure forbids tuning on the same holdout and requires a new
successor manifest. Every outcome ends with a retirement marker, and retired
holdouts cannot be reused for a later release.
