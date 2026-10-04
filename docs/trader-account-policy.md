# Fictional account FIFO and netting policy

The account contracts and ledger under `histdatacom.synthetic.traders` model a
bounded fictional retail-FX account. They enforce one account-wide, same-pair
offset boundary across strategy labels. They do not implement a broker,
strategy catalog, margin engine, financing, executable price discovery, or a
production trader engine. Successful replay is not legal or scientific
certification and does not authorize an empirical campaign.

## Frozen policy, not inferred account eligibility

The default software profile records a review completed on
2026-10-04 at 03:09:34 UTC. Its current primary anchor is
[NFA Rule 2-43(b)](https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=RULE+2-43&Section=4):
ordinary offsets use FIFO; an explicitly customer-directed same-size offset
must still select the oldest matching transaction. It is not permission to
choose any tax lot. The profile's account/product scope is limited to its
declared fictional FDM retail, non-ECP, leveraged off-exchange assumptions.
See [NFA Bylaw 1507(b)](https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=BYLAW+1507&Section=3)
and [17 CFR 5.1](https://www.ecfr.gov/current/title-17/chapter-I/part-5/section-5.1).

Source references and review timestamps are retained declarations, not a fresh
legal review at every execution. The applicability interval describes the
software profile, not a guarantee that future law is unchanged. Whole-rule
amendment dates do not establish historical paragraph versions. Historical
execution clocks therefore retain the explicit `frozen_modern_counterfactual`
assumption; they are not evidence of historical legal compliance.

Policy revision, review/source fields, applicability, account label and account
currency participate in account identity. A replacement policy creates a new
account and dataset identity; it cannot reinterpret a retained old ledger.
Unsupported jurisdiction/account/product or close-rule variants refuse instead
of silently inheriting this profile. Citizenship and currency pairs do not
establish actual account eligibility.

## One execution boundary

`build_account_ledger(spec, executions)` creates a new fictional execution
history. `replay_account_ledger(spec, ledger, expected_dataset_id=...)` verifies
an existing history against both an independently retained account specification
and dataset ID. `apply_account_execution(spec, ledger, execution,
expected_dataset_id=...)` verifies that selected history and appends one fill.

Retain the expected dataset ID when producing or accepting the original ledger.
Do not recover the expected ID from a potentially modified artifact immediately
before verifying it. A consistently rewritten history has a different identity;
mathematical self-consistency alone cannot establish the original history.

Every boundary re-admits exact bounded record types and recomputes the complete
history iteratively. Passive constructors, successful JSON decoding and a
caller-supplied hash do not verify allocations. Receipt references bind each
fill and before/after state without embedding a quadratic series of full states.
The ledger retains the account specification, fills, receipts and final state.

These APIs are pure and immutable. There is no global latest-head pointer,
durable compare-and-swap, source-authenticity proof or external-completeness
claim. A valid selected older prefix can produce a different fictional branch;
it does not overwrite the retained original. Durable publication and broader
trader verification are separate work.

Execution sequence is a contiguous account-local ordinal starting at zero,
not a venue sequence. Event timestamps may tie but cannot run backward. Lots
retain opening order and age after partial consumption. Different strategy
labels in the same account/pair share that order; strategies cannot supply a
custom close callback or select a later matching lot.

Ordinary FIFO consumes opposite-side quantity first. An oversized reversal
opens only its residual position. The lot retains the full original execution
quantity for provenance: selling eight against a long five leaves a short
three whose original execution quantity is minus eight. It is not relabeled
as an untouched three-unit transaction.

For an invented, pre-cost account with explicit USD conversion:

```python
from histdatacom.synthetic.traders import (
    AccountFillV1,
    AccountSpecV1,
    ExactAmountV1 as Amount,
    build_account_ledger,
    frozen_us_fifo_policy,
    replay_account_ledger,
)

policy = frozen_us_fifo_policy()
spec = AccountSpecV1("invented example", "USD", policy, policy.effective_from_ns)
fills = tuple(
    AccountFillV1(
        spec.account_id, sequence, sequence, "EURUSD",
        Amount(quantity), Amount.from_decimal(price), Amount(1),
        "invented identity conversion v1",
    )
    for sequence, (quantity, price) in enumerate(
        ((2, "1.10"), (3, "1.20"), (-4, "1.30"))
    )
)
ledger = build_account_ledger(spec, fills)
retained_root = ledger.dataset_id  # save independently with the original
assert ledger.final_state.realized_account_pnl == Amount(3, 5)
replay_account_ledger(spec, ledger, expected_dataset_id=retained_root)
```

The optional same-size mode requires an explicit customer-direction reference
and an intact full matching opposite lot. It chooses the oldest such lot and
does not partially close, reverse, or fall back to FIFO. If an opposite partial
lot has an original **or** remaining magnitude matching the requested size,
the request refuses, including when another intact match exists. The inspected
rule does not resolve original-versus-remaining size after partial consumption;
this restriction is a conservative software choice, not an added regulatory
interpretation. A `fifo_only` profile refuses every exception request.

## Exact accounting and explicit units

`ExactAmountV1` uses reduced rational integers. Decimal text is parsed exactly,
without binary floats or the process-wide Decimal context. Invalid or excessive
values fail rather than round. Quantities are **base-currency units**, not lots,
contracts or pips; prices are quote-currency units per base unit.

For each closing allocation, realized quote P/L is the positive quantity closed
times the consumed position's sign (long +1, short -1), times the exit-minus-entry
price difference. The closing fill's explicit positive,
versioned quote-to-account conversion factor produces account-currency P/L.
Same-currency conversion must be exactly one. Conversion references are modeled
inputs, not authenticated historical FX rates. Totals are pre-cost: financing,
commissions, taxes, margin, cash balances and solvency are not inferred.

The independent fixture buys two at 1.10, buys three at 1.20, and sells four at
1.30. FIFO closes two from each lot, realizes exactly 0.60 before conversion and
costs, and leaves one at 1.20. Short-side, losing, partial and reversal cases
must reconcile by the same arithmetic, not by approximate floating comparison.

Pairs remain separate. Each remaining position contributes its signed base
principal and opposite quote entry-notional principal. Currency exposures retain
net and gross amounts in each currency's own units. A zero-net, positive-gross
currency remains present; amounts in different currencies are never summed
into a scalar risk number. This is book-cost exposure, not mark-to-market risk.
Correlated cross-pair positions do not themselves establish a legal hedging
exemption under [NFA Rule 2-36](https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=RULE+2-36&Section=4).

## Bounds and nonclaims

Admission is bounded to 1,024 fills/lots/allocations per sequence and 2,048
allocations across a whole ledger, 32 pairs,
256-bit rational components, 4 MiB of canonical wire, depth 20 and 262,144 tree
nodes. Combined shapes must fit every bound; these are not a guarantee that
every maximum-sized field can coexist. Arithmetic result growth also refuses.
Unknown or duplicate fields, unsupported schemas, numeric booleans, floats and
noncanonical wire are not accepted through coercion.

This feature does not upgrade the full execution/account-ledger maturity stage,
complete the 1,000-strategy population, qualify its inputs, or deliver downstream
Luigi integration. Validation uses invented fills and independent arithmetic
oracles, not market capture or historical account processing.
