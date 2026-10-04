"""Independent fictional-account arithmetic, not provider or legal evidence.

The reference model uses Fraction and plain lists, never ledger arithmetic or
selection helpers. Mutated-record vectors prove replay refusal, not new fills.
"""

from dataclasses import fields, replace
from decimal import Inexact, ROUND_DOWN, ROUND_UP, localcontext
from fractions import Fraction

import pytest

from histdatacom.synthetic.traders.account_contracts import (
    AccountFillV1,
    AccountLedgerV1,
    AccountPolicyV1,
    AccountSpecV1,
    ExactAmountV1,
    PolicySourceV1,
)
from histdatacom.synthetic.traders.account_ledger import (
    apply_account_execution,
    build_account_ledger,
    replay_account_ledger,
)


def amount(value):
    value = Fraction(value)
    return ExactAmountV1(value.numerator, value.denominator)


def fraction(value):
    return Fraction(value.numerator, value.denominator)


def policy(**changes):
    values = {
        "policy_revision": "1.0.0",
        "label": "Fictional oracle profile, not legal certification",
        "effective_from_ns": 100,
        "effective_until_ns": 1000,
        "sources": (
            PolicySourceV1(
                "https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=RULE+2-43&Section=4",
                "NFA Compliance Rule 2-43; fictional software fixture",
                100,
            ),
        ),
    }
    return AccountPolicyV1(**(values | changes))


def spec(**changes):
    values = {
        "account_label": "independent-oracle",
        "account_currency": "USD",
        "policy": policy(),
        "policy_as_of_ns": 100,
    }
    return AccountSpecV1(**(values | changes))


def fill(account, sequence, quantity, price, **changes):
    values = {
        "account_id": account.account_id,
        "sequence": sequence,
        "event_time_ns": sequence + 1,
        "symbol": "EURUSD",
        "signed_quantity": amount(quantity),
        "price": amount(price),
        "quote_to_account": amount(1),
        "conversion_reference": "fictional-quote-to-account:identity",
        "strategy_id": "strategy-a",
    }
    return AccountFillV1(**(values | changes))


def reseal(record, **changes):
    """Use ordinary constructors; no bypass of native structural admission."""
    payload = {
        field.name: getattr(record, field.name)
        for field in fields(record)
        if field.init and field.name != "artifact_id"
    }
    return type(record)(**(payload | changes))


def oracle(executions):
    """Small independent FIFO reference in base units and entry-notional legs."""
    lots = []
    receipts = []
    total = Fraction(0)
    for execution in executions:
        remaining = fraction(execution.signed_quantity)
        price = fraction(execution.price)
        conversion = fraction(execution.quote_to_account)
        opposite = [
            lot
            for lot in lots
            if lot["symbol"] == execution.symbol and lot["q"] * remaining < 0
        ]
        if execution.close_mode == "customer_directed_same_size":
            size = abs(remaining)
            if any(
                lot["q"] != lot["original"]
                and size in (abs(lot["q"]), abs(lot["original"]))
                for lot in opposite
            ):
                raise ValueError("ambiguous partial-lot size")
            opposite = [
                lot
                for lot in opposite
                if lot["q"] == lot["original"] and abs(lot["q"]) == size
            ][:1]
            if not opposite:
                raise ValueError("no eligible complete same-size lot")
        allocations = []
        delta = Fraction(0)
        for lot in opposite:
            if not remaining:
                break
            quantity = min(abs(remaining), abs(lot["q"]))
            side = 1 if lot["q"] > 0 else -1
            pnl = quantity * (price - lot["price"]) * side * conversion
            allocations.append(
                (lot["opening"], quantity, lot["price"], price, pnl)
            )
            delta += pnl
            lot["q"] -= side * quantity
            remaining += side * quantity
        lots = [lot for lot in lots if lot["q"]]
        if remaining:
            lots.append(
                {
                    "symbol": execution.symbol,
                    "opening": execution.fill_id,
                    "sequence": execution.sequence,
                    "time": execution.event_time_ns,
                    "original": fraction(execution.signed_quantity),
                    "q": remaining,
                    "price": price,
                }
            )
        total += delta
        receipts.append((allocations, delta))
    exposures = {}
    for lot in lots:
        for currency, quantity in (
            (lot["symbol"][:3], lot["q"]),
            (lot["symbol"][3:], -lot["q"] * lot["price"]),
        ):
            net, gross = exposures.get(currency, (Fraction(0), Fraction(0)))
            exposures[currency] = net + quantity, gross + abs(quantity)
    return lots, total, exposures, receipts


def assert_oracle(ledger):
    lots, pnl, exposures, receipts = oracle(ledger.fills)
    state = ledger.final_state
    assert fraction(state.realized_account_pnl) == pnl
    assert state.next_sequence == len(ledger.fills)
    assert state.last_event_time_ns == (
        ledger.fills[-1].event_time_ns if ledger.fills else None
    )
    assert [
        (
            lot.symbol,
            lot.opening_fill_id,
            lot.opened_sequence,
            lot.opened_at_ns,
            fraction(lot.original_signed_quantity),
            fraction(lot.remaining_signed_quantity),
            fraction(lot.entry_price),
        )
        for lot in state.open_lots
    ] == [
        (
            lot["symbol"],
            lot["opening"],
            lot["sequence"],
            lot["time"],
            lot["original"],
            lot["q"],
            lot["price"],
        )
        for lot in lots
    ]
    assert {
        item.currency: (fraction(item.net_units), fraction(item.gross_units))
        for item in state.currency_exposures
    } == exposures
    assert len(ledger.receipts) == len(receipts)
    for actual, (allocations, delta) in zip(ledger.receipts, receipts):
        assert fraction(actual.realized_account_pnl_delta) == delta
        assert [
            (
                item.opening_fill_id,
                fraction(item.quantity_closed),
                fraction(item.entry_price),
                fraction(item.exit_price),
                fraction(item.realized_account_pnl),
            )
            for item in actual.allocations
        ] == allocations
    for symbol in {lot.symbol for lot in state.open_lots}:
        signs = {
            fraction(lot.remaining_signed_quantity) > 0
            for lot in state.open_lots
            if lot.symbol == symbol
        }
        assert len(signs) == 1


@pytest.mark.parametrize("side", [1, -1])
@pytest.mark.parametrize("profitable", [True, False])
def test_literal_fifo_profit_and_loss_in_both_directions(side, profitable):
    account = spec()
    prices = ("1.10", "1.20", "1.30")
    if (side < 0) == profitable:
        prices = tuple(reversed(prices))
    executions = tuple(
        fill(account, i, quantity * side, prices[i])
        for i, quantity in enumerate((2, 3, -4))
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    assert fraction(ledger.final_state.realized_account_pnl) == Fraction(
        3 if profitable else -3, 5
    )
    assert len(ledger.final_state.open_lots) == 1
    remaining = ledger.final_state.open_lots[0]
    assert fraction(remaining.remaining_signed_quantity) == side
    assert fraction(remaining.entry_price) == Fraction(6, 5)


@pytest.mark.parametrize("side", [1, -1])
def test_partial_lot_keeps_original_age_and_consumed_state_identity(side):
    account = spec()
    executions = tuple(
        fill(account, i, quantity * side, price)
        for i, (quantity, price) in enumerate(
            ((2, "1.10"), (3, "1.20"), (-1, "1.30"), (-2, "1.40"))
        )
    )
    prefix = build_account_ledger(account, executions[:3])
    assert_oracle(prefix)
    partial = prefix.final_state.open_lots[0]
    assert partial.opened_sequence == 0
    assert fraction(partial.original_signed_quantity) == 2 * side
    assert fraction(partial.remaining_signed_quantity) == side
    result = apply_account_execution(
        account, prefix, executions[3], expected_dataset_id=prefix.dataset_id
    )
    assert_oracle(result)
    first, second = result.receipts[-1].allocations
    assert first.lot_id == partial.lot_id
    assert first.opening_fill_id == executions[0].fill_id
    assert second.opening_fill_id == executions[1].fill_id
    assert result.receipts[2].allocations[0].lot_id != first.lot_id


@pytest.mark.parametrize("side", [1, -1])
def test_reversal_opens_only_residual_and_later_flatten_is_exact(side):
    account = spec()
    executions = tuple(
        fill(account, i, quantity * side, price)
        for i, (quantity, price) in enumerate(
            ((2, "1.10"), (3, "1.20"), (-7, "1.30"), (2, "1.20"))
        )
    )
    reversed_ledger = build_account_ledger(account, executions[:3])
    assert_oracle(reversed_ledger)
    remaining = reversed_ledger.final_state.open_lots[0]
    assert fraction(remaining.remaining_signed_quantity) == -2 * side
    assert fraction(remaining.original_signed_quantity) == -7 * side
    assert remaining.opening_fill_id == executions[2].fill_id
    final = apply_account_execution(
        account,
        reversed_ledger,
        executions[3],
        expected_dataset_id=reversed_ledger.dataset_id,
    )
    assert_oracle(final)
    assert final.final_state.open_lots == ()
    assert final.final_state.currency_exposures == ()
    assert fraction(final.final_state.realized_account_pnl) == side * Fraction(
        9, 10
    )


@pytest.mark.parametrize("side", [1, -1])
def test_customer_direction_chooses_oldest_intact_same_size_not_fifo(side):
    account = spec()
    opened = tuple(
        fill(account, i, quantity * side, price)
        for i, (quantity, price) in enumerate(
            ((2, "1.10"), (3, "1.20"), (3, "1.25"))
        )
    )
    ordinary = fill(account, 3, -3 * side, "1.30")
    directed = fill(
        account,
        3,
        -3 * side,
        "1.30",
        close_mode="customer_directed_same_size",
        customer_direction_id="fictional-customer-direction:1",
    )
    fifo = build_account_ledger(account, (*opened, ordinary))
    chosen = build_account_ledger(account, (*opened, directed))
    assert_oracle(fifo)
    assert_oracle(chosen)
    assert [x.opening_fill_id for x in fifo.receipts[-1].allocations] == [
        opened[0].fill_id,
        opened[1].fill_id,
    ]
    assert [x.opening_fill_id for x in chosen.receipts[-1].allocations] == [
        opened[1].fill_id
    ]
    assert fifo.dataset_id != chosen.dataset_id


@pytest.mark.parametrize("side", [1, -1])
@pytest.mark.parametrize("requested", [2, 3])
def test_matching_partial_original_or_remaining_blocks_even_intact_match(
    side, requested
):
    account = spec()
    opened = (
        fill(account, 0, 3 * side, "1.10"),
        fill(account, 1, -side, "1.20"),
        fill(account, 2, requested * side, "1.30"),
    )
    ledger = build_account_ledger(account, opened)
    directed = fill(
        account,
        3,
        -requested * side,
        "1.40",
        close_mode="customer_directed_same_size",
        customer_direction_id="fictional-customer-direction:ambiguous",
    )
    before = ledger.to_json()
    with pytest.raises((ValueError, TypeError)):
        apply_account_execution(
            account, ledger, directed, expected_dataset_id=ledger.dataset_id
        )
    assert ledger.to_json() == before
    with pytest.raises((ValueError, TypeError)):
        build_account_ledger(account, (*opened, directed))


@pytest.mark.parametrize("quantity", [1, 4, -2])
def test_same_size_never_partially_closes_reverses_or_opens(quantity):
    account = spec()
    opened = fill(account, 0, 2, "1.10")
    directed = fill(
        account,
        1,
        -quantity,
        "1.30",
        close_mode="customer_directed_same_size",
        customer_direction_id="fictional-customer-direction:invalid",
    )
    with pytest.raises((ValueError, TypeError)):
        build_account_ledger(account, (opened, directed))


@pytest.mark.parametrize("requested", [3, 8])
def test_reversal_residual_keeps_full_execution_size_and_is_ambiguous(
    requested,
):
    account = spec()
    executions = (
        fill(account, 0, 5, "1.10"),
        fill(account, 1, -8, "1.20"),
        fill(account, 2, -requested, "1.30"),
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    residual = ledger.final_state.open_lots[0]
    assert fraction(residual.original_signed_quantity) == -8
    assert fraction(residual.remaining_signed_quantity) == -3
    directed = fill(
        account,
        3,
        requested,
        "1.40",
        close_mode="customer_directed_same_size",
        customer_direction_id="fictional-customer-direction:reversal",
    )
    with pytest.raises((ValueError, TypeError)):
        apply_account_execution(
            account, ledger, directed, expected_dataset_id=ledger.dataset_id
        )
    ordinary = fill(account, 3, requested, "1.40")
    assert_oracle(
        apply_account_execution(
            account, ledger, ordinary, expected_dataset_id=ledger.dataset_id
        )
    )


def test_strategy_labels_cannot_partition_account_book():
    account = spec()
    executions = (
        fill(account, 0, 2, "1.10", strategy_id="alpha"),
        fill(account, 1, -2, "1.30", strategy_id="beta"),
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    assert ledger.final_state.open_lots == ()
    assert fraction(ledger.final_state.realized_account_pnl) == Fraction(2, 5)


def test_correlated_pairs_keep_books_and_explicit_currency_legs_separate():
    account = spec()
    executions = (
        fill(account, 0, 2, "1.10", symbol="EURUSD"),
        fill(account, 1, -3, "1.20", symbol="GBPUSD"),
        fill(
            account,
            2,
            4,
            "0.80",
            symbol="EURGBP",
            quote_to_account=amount("1.25"),
            conversion_reference="fictional:GBP-to-USD",
        ),
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    assert len(ledger.final_state.open_lots) == 3
    assert fraction(ledger.final_state.realized_account_pnl) == 0
    assert {
        x.currency: (fraction(x.net_units), fraction(x.gross_units))
        for x in ledger.final_state.currency_exposures
    } == {
        "EUR": (Fraction(6), Fraction(6)),
        "USD": (Fraction(7, 5), Fraction(29, 5)),
        "GBP": (Fraction(-31, 5), Fraction(31, 5)),
    }


def test_zero_net_currency_with_nonzero_gross_is_not_dropped():
    account = spec()
    executions = (
        fill(account, 0, 2, "1.50", symbol="EURUSD"),
        fill(account, 1, -3, "1.00", symbol="GBPUSD"),
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    usd = next(
        x for x in ledger.final_state.currency_exposures if x.currency == "USD"
    )
    assert fraction(usd.net_units) == 0
    assert fraction(usd.gross_units) == 6
    assert len(ledger.final_state.open_lots) == 2


def test_realized_conversion_uses_closing_fill_factor_without_rounding():
    account = spec(account_currency="JPY")
    executions = (
        fill(account, 0, 2, "1.10", quote_to_account=amount(100)),
        fill(account, 1, 3, "1.20", quote_to_account=amount(110)),
        fill(
            account,
            2,
            -4,
            "1.30",
            quote_to_account=amount(Fraction(301, 2)),
        ),
    )
    ledger = build_account_ledger(account, executions)
    assert_oracle(ledger)
    assert fraction(ledger.final_state.realized_account_pnl) == Fraction(
        903, 10
    )


@pytest.mark.parametrize("case", range(16))
def test_generated_small_histories_match_independent_oracle_at_every_fill(case):
    account = spec()
    ledger = build_account_ledger(account, ())
    assert_oracle(ledger)
    fills = []
    for index in range(6):
        side = 1 if (case >> (index % 4)) & 1 else -1
        execution = fill(
            account,
            index,
            side * (1 + (case + index) % 3),
            Fraction(100 + 7 * index + case % 5, 100),
            strategy_id=f"strategy-{index % 3}",
        )
        previous = ledger
        fills.append(execution)
        ledger = apply_account_execution(
            account,
            previous,
            execution,
            expected_dataset_id=previous.dataset_id,
        )
        assert_oracle(ledger)
        assert ledger == build_account_ledger(account, tuple(fills))


def test_resealed_omitted_history_requires_original_independent_dataset_root():
    account = spec()
    fills = (
        fill(account, 0, 2, "1.10"),
        fill(account, 1, -2, "1.20"),
    )
    original = build_account_ledger(account, fills)
    prefix = build_account_ledger(account, fills[:1])
    with pytest.raises((ValueError, TypeError)):
        replay_account_ledger(
            account, prefix, expected_dataset_id=original.dataset_id
        )
    with pytest.raises((ValueError, TypeError)):
        apply_account_execution(
            account, prefix, fills[1], expected_dataset_id=original.dataset_id
        )
    # A separately identified prefix is a new fictional dataset, not proof of
    # completeness/latest-head authority; the API does not promise durable CAS.
    assert (
        replay_account_ledger(
            account, prefix, expected_dataset_id=prefix.dataset_id
        )
        == prefix
    )
    assert (
        apply_account_execution(
            account, prefix, fills[1], expected_dataset_id=prefix.dataset_id
        )
        == original
    )


def test_forged_and_resealed_arithmetic_cannot_become_replay_authority():
    account = spec()
    ledger = build_account_ledger(
        account,
        (fill(account, 0, 2, "1.10"), fill(account, 1, -1, "1.30")),
    )
    # Exact frozen record mutation is hostile input, not structural authority.
    poisoned = AccountLedgerV1.from_json(ledger.to_json())
    object.__setattr__(poisoned.final_state, "realized_account_pnl", amount(99))
    with pytest.raises((ValueError, TypeError)):
        replay_account_ledger(
            account, poisoned, expected_dataset_id=ledger.dataset_id
        )
    # Even if every affected structural identity is normally recomputed, the
    # full arithmetic replay must disagree; constructor refusal is also sound.
    with pytest.raises((ValueError, TypeError)):
        state = reseal(ledger.final_state, realized_account_pnl=amount(99))
        receipt = reseal(
            ledger.receipts[-1],
            after_state_id=state.state_id,
            realized_account_pnl_delta=amount(99),
        )
        forged = reseal(
            ledger,
            final_state=state,
            receipts=(*ledger.receipts[:-1], receipt),
        )
        replay_account_ledger(
            account, forged, expected_dataset_id=forged.dataset_id
        )


def test_resealed_newer_lot_allocation_refuses_even_with_equal_prices_and_pnl():
    account = spec()
    executions = (
        fill(account, 0, 2, "1.10"),
        fill(account, 1, 2, "1.10"),
        fill(account, 2, -1, "1.30"),
    )
    before = build_account_ledger(account, executions[:2])
    ledger = build_account_ledger(account, executions)
    old, newer = before.final_state.open_lots
    replacement_lots = (
        old,
        reseal(newer, remaining_signed_quantity=amount(1)),
    )
    bad_state = reseal(ledger.final_state, open_lots=replacement_lots)
    bad_allocation = reseal(
        ledger.receipts[-1].allocations[0],
        lot_id=newer.lot_id,
        opening_fill_id=newer.opening_fill_id,
    )
    bad_receipt = reseal(
        ledger.receipts[-1],
        after_state_id=bad_state.state_id,
        allocations=(bad_allocation,),
    )
    forged = reseal(
        ledger,
        final_state=bad_state,
        receipts=(*ledger.receipts[:-1], bad_receipt),
    )
    assert forged.dataset_id != ledger.dataset_id
    assert (
        forged.final_state.realized_account_pnl
        == ledger.final_state.realized_account_pnl
    )
    assert (
        forged.final_state.currency_exposures
        == ledger.final_state.currency_exposures
    )
    # A new content address and unchanged aggregate arithmetic cannot authorize
    # a younger same-pair lot; fresh replay must enforce the selected FIFO law.
    with pytest.raises((ValueError, TypeError)):
        replay_account_ledger(
            account, forged, expected_dataset_id=forged.dataset_id
        )


def test_policy_changes_identity_without_reinterpreting_retained_old_history():
    original_spec = spec()
    original = build_account_ledger(
        original_spec, (fill(original_spec, 0, 2, "1.10"),)
    )
    newer_spec = spec(policy=policy(policy_revision="2.0.0"))
    newer = build_account_ledger(newer_spec, (fill(newer_spec, 0, 2, "1.10"),))
    assert newer_spec.account_id != original_spec.account_id
    assert newer.dataset_id != original.dataset_id
    with pytest.raises((ValueError, TypeError)):
        replay_account_ledger(
            newer_spec, original, expected_dataset_id=original.dataset_id
        )
    assert (
        replay_account_ledger(
            original_spec, original, expected_dataset_id=original.dataset_id
        )
        == original
    )


@pytest.mark.parametrize("prec,rounding", [(2, ROUND_DOWN), (50, ROUND_UP)])
def test_global_decimal_context_cannot_change_arithmetic_or_identity(
    prec, rounding
):
    account = spec()
    executions = (
        fill(account, 0, Fraction(1, 3), Fraction(7, 11)),
        fill(account, 1, Fraction(2, 7), Fraction(13, 17)),
        fill(account, 2, -Fraction(1, 2), Fraction(19, 23)),
    )
    expected = build_account_ledger(account, executions)
    with localcontext() as context:
        context.prec = prec
        context.rounding = rounding
        context.traps[Inexact] = True
        actual = build_account_ledger(account, executions)
        assert actual.to_json() == expected.to_json()
        assert (
            replay_account_ledger(
                account, actual, expected_dataset_id=actual.dataset_id
            )
            == expected
        )
        assert fraction(ExactAmountV1.from_decimal("0.123456789")) == Fraction(
            123456789, 1_000_000_000
        )
    assert_oracle(actual)


@pytest.mark.parametrize("bad", [True, 1.0, "1", None])
def test_execution_numeric_aliases_are_not_admitted(bad):
    account = spec()
    valid = fill(account, 0, 1, "1.10")
    with pytest.raises((ValueError, TypeError)):
        build_account_ledger(account, (replace(valid, signed_quantity=bad),))


def test_hostile_exact_nested_fields_fail_before_subclass_serializer():
    account = spec()
    valid = fill(account, 0, 1, "1.10")

    class PoisonAmount(ExactAmountV1):
        def to_dict(self):
            pytest.fail("hostile serializer reached before exact admission")

    poison = object.__new__(PoisonAmount)
    object.__setattr__(poison, "numerator", 1)
    object.__setattr__(poison, "denominator", 1)
    poisoned = AccountFillV1.from_json(valid.to_json())
    object.__setattr__(poisoned, "signed_quantity", poison)
    with pytest.raises((ValueError, TypeError)):
        build_account_ledger(account, (poisoned,))


def test_oversized_history_refuses_before_reading_nonrecord_properties():
    account = spec()

    class PoisonFill:
        @property
        def account_id(self):
            pytest.fail("oversized history inspected before count admission")

    with pytest.raises((ValueError, TypeError)):
        build_account_ledger(account, (PoisonFill(),) * 1025)


@pytest.mark.parametrize(
    "text", ["1e999999999", "NaN", "Infinity", "9" * 10000]
)
def test_numeric_lexemes_refuse_without_rounding_or_unbounded_exponents(text):
    with pytest.raises((ValueError, TypeError)):
        ExactAmountV1.from_decimal(text)


def test_bounded_input_arithmetic_growth_refuses_instead_of_rounding():
    maximum = ExactAmountV1(2**256 - 1)
    with pytest.raises((ValueError, TypeError)):
        maximum + ExactAmountV1(1)
    with pytest.raises((ValueError, TypeError)):
        maximum * ExactAmountV1(2)
    assert fraction(ExactAmountV1(2**255) - ExactAmountV1(2**255)) == 0
