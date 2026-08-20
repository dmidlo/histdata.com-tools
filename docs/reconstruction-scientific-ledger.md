# Reconstruction scientific ledger

`histdatacom.reconstruction_science` is the authoritative scientific target for
regime-conditioned constrained infill. It replaces distributed prose as the
identity-bearing source of the estimand, assumptions, context missingness
semantics, generated-row claims, validity boundary, and legacy replay
treatment.

The current milestone is deliberately narrow: it uses only the qualified
HistData.com ASCII/T intersection for EURGBP, EURUSD, and GBPUSD. The contract
is provider-neutral so later evidence adapters can bind the same domain
semantics, but OANDA, alternate historical providers, live feeds, and broker
conditioning remain later milestones. They are not inferred, substituted, or
silently admitted here.

## Research estimand

Let `X` be an unobserved market-event process, `O_{phi,e}` an observation
operator with parameters `phi` and feed epoch `e`, `C` point-in-time context,
and `Y` the surviving HistData feed:

\[
Y=O_{\phi,e}(X).
\]

The proposal layer samples the conditional counterfactual distribution

\[
X_{\mathrm{miss}}\sim q_\theta\left(
X_{\mathrm{miss}}\mid Y_{\mathrm{anchors}},C,e,\phi
\right).
\]

Carving and reconciliation then produce

\[
X_{\mathrm{product}}=R\left(
Y_{\mathrm{anchors}}\cup K(X_{\mathrm{miss}})
\right).
\]

The final product law is the pushforward `(R o K)_# q_theta`, not the raw
proposal law. Proposal-engine qualification is therefore necessary but not
sufficient evidence for final-product validity.

> Output is a plausible counterfactual ensemble conditioned on declared
> artifacts and constraints; it is not recovered historical truth.

This formalizes the practical question: what might a historical episode have
looked like under a selected qualified observation regime while its surviving
historical anchors and declared context remain fixed? It does not claim to
recover missing ticks or latent market truth.

## Required assumptions

Every ledger contains exactly these machine-readable assumptions. Each entry
has its own content ID and applicability scopes, and the ordered set is part of
the ledger ID.

| Assumption ID key | Boundary |
|---|---|
| `modern-reference-is-not-latent-truth` | Stable modern HistData epochs are reference observations, not latent truth. |
| `observation-and-market-processes-may-be-confounded` | A feed change and a market change may not be separately identifiable. |
| `operator-transport-is-support-bounded` | Observation-operator estimates transport backward only inside declared support and uncertainty. |
| `anchor-conditioning-does-not-identify-path` | Anchors constrain but do not identify the internal path. |
| `context-completeness-varies` | Completeness varies by source, period, field, and information mode. |
| `asynchronous-triangle-support-is-assumed` | Triangle alignment is an explicit observation assumption, not a simultaneity claim. |
| `ex-post-products-are-invalid-for-backtest` | Ex-post products are not newly observed point-in-time evidence. |

Changing an equation, assumption statement, applicability scope, limitation,
taxonomy definition, migration rule, or validity label changes its child ID
and the parent `ledger_id`.

## Context missingness taxonomy

Provider-row absence never proves that no market-moving event occurred. A
complete calendar with a confirmed no-match is distinct from an incomplete
corpus, and both differ from a matched row whose fields or publication time
are incomplete.

| Category | Meaning and treatment |
|---|---|
| `complete_calendar_no_matching_event` | The declared calendar is complete and the bounded query confirms no matching event. Condition on the no-match state without claiming no other event occurred. |
| `matched_complete_contemporaneous_fields` | The matched event has the declared forecast, previous, and revision fields. |
| `matched_missing_contemporaneous_fields` | The event matches, but one or more contemporaneous fields are explicitly absent. Use only present fields. |
| `incomplete_corpus_coverage` | Timeline, coverage, or calendar evidence is incomplete. Required conditioning fails closed unless separately qualified. |
| `event_known_only_ex_post` | The event/value crosses the relevant first-known or availability boundary. Ex-post use stays labeled. |
| `uncertain_first_known_or_publication_time` | Timing is approximate, window-only, or ambiguous. Exact point-in-time claims are forbidden. |
| `cftc_limited_availability` | CFTC state is pre-coverage, stale, unavailable, missing, or restatement-incomplete. The runtime does not impute it. |
| `explicit_qualified_unconditioned_mode` | A strong powered qualification permits an eligible engine to run without unavailable CFTC context. The omission and qualification remain explicit. |

`ReconstructionConditioningStateV1` binds each bounded market-context and CFTC
query to the ledger, information mode, completeness, one or more taxonomy
categories, reason codes, missing fields, and a deterministic state ID. Source
enrichment writes both states. Final validation re-derives them before
publication and carries their IDs into product scientific evidence.

## Information and strategy validity

An `ex_post_reconstruction` state always carries
`invalid-for-backtest`. Strategy-sensitivity contracts preserve the same label
at case, plan, window-result, and report boundaries. Such products may support
counterfactual sensitivity analysis, reverse degradation, and stress testing;
they cannot be promoted as newly observed evidence for prospective strategy
claims.

An `ex_ante_simulation` state must not carry that reason, but it remains subject
to the stricter point-in-time information audit. This distinction does not
convert an ex-post reconstruction into ex-ante evidence.

## Identity and runtime lineage

Every newly constructed plan writes a strong
`reconstruction_scientific_ledger_v1` artifact and binds it through:

1. the frozen experiment artifact graph and implementation hashes;
2. proposal context references, configuration hashes, the information
   manifest, top-level plan identity, and execution manifest;
3. explicit source and validation stage input references;
4. source-stage conditioning states and final product quality evidence;
5. the product's experiment identity and scientific-ledger artifact digest;
6. campaign product reconciliation and the synthetic dataset version's direct
   qualification evidence; and
7. certification requirements for the ledger, lineage, complete conditioning
   states, generated origin, and backtest-invalid labeling.

Generated rows use only `origin="synthetic"` and required generator/anchor
lineage. `SyntheticEventV1` rejects observed-row identity on generated events
and rejects synthetic lineage on observed anchors. The current ledger also
forbids generated-row claims of `observed`, `recovered truth`, or `broker
history`.

The installed inspection surfaces are:

```sh
histdatacom reconstruction --json science
histdatacom reconstruction science --ledger PATH
```

Python callers use `ReconstructionClient.scientific_ledger()`,
`current_histdata_reconstruction_scientific_ledger()`, or
`read_reconstruction_scientific_ledger()` from
`histdatacom.reconstruction`.

## Retained v2.4 artifacts

Retained v2.4 identities remain readable and replayable as their original
contracts. They are classified `legacy-unbound`: verification of the old
identity does not manufacture a current scientific binding. They cannot
execute under the current pipeline without replanning from their original
inputs, which produces a new experiment, run, plan, and ledger-bound identity.
No in-place identity rewrite or implicit migration is allowed.

## Methodological basis

The observation/process separation follows the original self- and mutually
exciting point-process formulation in [Hawkes (1971)](https://doi.org/10.1093/biomet/58.1.83).
Point-process fit and residual checks are distinct from this estimand and use
the time-rescaling basis described by [Brown et al. (2002)](https://doi.org/10.1162/08997660252741149).
The explicit missingness ledger is conservative because missing-data mechanisms
are ignorable only under specific conditions; see [Rubin (1976)](https://doi.org/10.1093/biomet/63.3.581).

These references motivate the contracts. They do not by themselves validate a
campaign, select an engine, or establish transport into an unsupported epoch.
