"""Fixed synthetic transition/replay controls; no source/legal qualification."""

from __future__ import annotations

import pytest

from histdatacom.synthetic.traders import account_ledger as module
from histdatacom.synthetic.traders.account_contracts import (
    AccountAllocationV1,
    AccountFillReceiptV1,
    AccountFillV1,
    AccountLedgerV1,
    AccountPolicyV1,
    AccountSpecV1,
    AccountStateV1,
    ExactAmountV1,
    PolicySourceV1,
)
from histdatacom.synthetic.traders.account_ledger import (
    apply_account_execution,
    build_account_ledger,
    replay_account_ledger,
)


def amount(text: str) -> ExactAmountV1:
    return ExactAmountV1.from_decimal(text)


def spec(
    *,
    revision: str = "1.0.0",
    rule: str = "fifo_with_customer_directed_same_size",
) -> AccountSpecV1:
    policy = AccountPolicyV1(
        revision,
        "Fixed fictional test policy, not legal advice",
        100,
        None,
        (
            PolicySourceV1(
                "https://www.nfa.futures.org/", "Review reference only", 100
            ),
        ),
        close_rule=rule,
    )
    return AccountSpecV1("generated-test-account", "USD", policy, 100)


def fill(
    account: AccountSpecV1,
    sequence: int,
    quantity: str,
    price: str = "1",
    *,
    symbol: str = "EURUSD",
    time: int = 100,
    conversion: str = "1",
    strategy: str = "strategy-a",
    directed: bool = False,
) -> AccountFillV1:
    return AccountFillV1(
        account.account_id,
        sequence,
        time,
        symbol,
        amount(quantity),
        amount(price),
        amount(conversion),
        "fictional-exact-conversion:v1",
        close_mode="customer_directed_same_size" if directed else "fifo",
        strategy_id=strategy,
        customer_direction_id=(
            "synthetic-customer-request" if directed else None
        ),
    )


def replay(account: AccountSpecV1, ledger: AccountLedgerV1) -> AccountLedgerV1:
    return replay_account_ledger(
        account, ledger, expected_dataset_id=ledger.dataset_id
    )


def test_independent_issue_fixture_and_linear_receipts() -> None:
    account = spec()
    fills = (
        fill(account, 0, "2", "1.10"),
        fill(account, 1, "3", "1.20"),
        fill(account, 2, "-4", "1.30"),
    )
    result = build_account_ledger(account, fills)
    assert result.final_state.realized_account_pnl == amount("0.60")
    assert len(result.final_state.open_lots) == 1
    lot = result.final_state.open_lots[0]
    assert lot.remaining_signed_quantity == amount("1")
    assert lot.original_signed_quantity == amount("3")
    assert lot.entry_price == amount("1.20")
    assert lot.opening_fill_id == fills[1].fill_id
    assert lot.opened_sequence == 1
    assert [
        item.quantity_closed for item in result.receipts[-1].allocations
    ] == [
        amount("2"),
        amount("2"),
    ]
    assert result.receipts[-1].after_state_id == result.final_state.state_id
    assert not hasattr(result.receipts[-1], "after_state")
    assert replay(account, result).to_json() == result.to_json()


@pytest.mark.parametrize("direction", (1, -1))
def test_partials_preserve_age_and_reversal_original_execution(
    direction: int,
) -> None:
    account = spec()
    first = fill(account, 0, str(direction * 5), "2", time=90)
    second = fill(account, 1, str(direction * -2), "3", time=90)
    partial = build_account_ledger(account, (first, second))
    lot = partial.final_state.open_lots[0]
    assert lot.opened_sequence == 0 and lot.opened_at_ns == 90
    assert lot.original_signed_quantity == amount(str(direction * 5))
    assert lot.remaining_signed_quantity == amount(str(direction * 3))
    reversed_result = apply_account_execution(
        account,
        partial,
        fill(account, 2, str(direction * -8), "4"),
        expected_dataset_id=partial.dataset_id,
    )
    residual = reversed_result.final_state.open_lots[0]
    assert residual.original_signed_quantity == amount(str(direction * -8))
    assert residual.remaining_signed_quantity == amount(str(direction * -5))
    assert residual.opened_sequence == 2
    assert reversed_result.final_state.realized_account_pnl == amount(
        str(direction * 8)
    )
    assert replay(account, reversed_result) == reversed_result


def test_same_size_skips_different_size_but_uses_oldest_matching() -> None:
    account = spec()
    fills = (
        fill(account, 0, "2", "1"),
        fill(account, 1, "3", "2"),
        fill(account, 2, "3", "4"),
    )
    before = build_account_ledger(account, fills)
    result = apply_account_execution(
        account,
        before,
        fill(account, 3, "-3", "5", directed=True),
        expected_dataset_id=before.dataset_id,
    )
    allocation = result.receipts[-1].allocations[0]
    assert allocation.opening_fill_id == fills[1].fill_id
    assert allocation.lot_id == before.final_state.open_lots[1].lot_id
    assert result.final_state.realized_account_pnl == amount("9")
    assert [lot.opening_fill_id for lot in result.final_state.open_lots] == [
        fills[0].fill_id,
        fills[2].fill_id,
    ]


@pytest.mark.parametrize("requested", ("-2", "-3"))
def test_matching_modified_original_or_remaining_refuses_even_intact_match(
    requested: str,
) -> None:
    account = spec()
    fills = (
        fill(account, 0, "3"),
        fill(account, 1, "-1"),
        fill(account, 2, "2"),
        fill(account, 3, "3"),
    )
    before = build_account_ledger(account, fills)
    with pytest.raises(ValueError, match="ambiguous"):
        apply_account_execution(
            account,
            before,
            fill(account, 4, requested, directed=True),
            expected_dataset_id=before.dataset_id,
        )


@pytest.mark.parametrize("requested", ("3", "8"))
def test_reversal_residual_cannot_be_exceptional_full_transaction(
    requested: str,
) -> None:
    account = spec()
    before = build_account_ledger(
        account, (fill(account, 0, "5"), fill(account, 1, "-8"))
    )
    with pytest.raises(ValueError, match="ambiguous"):
        apply_account_execution(
            account,
            before,
            fill(account, 2, requested, directed=True),
            expected_dataset_id=before.dataset_id,
        )


@pytest.mark.parametrize("quantity", ("-1", "-4", "2"))
def test_exception_no_match_never_falls_back_or_reverses(quantity: str) -> None:
    account = spec()
    with pytest.raises(ValueError, match="no intact matching"):
        build_account_ledger(
            account,
            (fill(account, 0, "2"), fill(account, 1, quantity, directed=True)),
        )


def test_fifo_only_policy_refuses_direction() -> None:
    account = spec(rule="fifo_only")
    with pytest.raises(ValueError, match="does not permit"):
        build_account_ledger(
            account,
            (fill(account, 0, "2"), fill(account, 1, "-2", directed=True)),
        )


def test_all_strategies_share_same_pair_fifo_and_distinct_pairs_survive() -> (
    None
):
    account = spec()
    fills = (
        fill(account, 0, "2", strategy="a"),
        fill(account, 1, "3", strategy="b"),
        fill(account, 2, "7", symbol="GBPUSD"),
        fill(account, 3, "-4", strategy="b"),
    )
    result = build_account_ledger(account, fills)
    assert [a.opening_fill_id for a in result.receipts[-1].allocations] == [
        fills[0].fill_id,
        fills[1].fill_id,
    ]
    assert [
        (lot.symbol, lot.remaining_signed_quantity)
        for lot in result.final_state.open_lots
    ] == [("EURUSD", amount("1")), ("GBPUSD", amount("7"))]


def test_currency_units_keep_zero_net_positive_gross_without_mark_to_market() -> (
    None
):
    account = spec()
    result = build_account_ledger(
        account,
        (
            fill(account, 0, "1", "2"),
            fill(account, 1, "2", "0.5", symbol="USDEUR", conversion="2"),
        ),
    )
    exposure = {
        item.currency: item for item in result.final_state.currency_exposures
    }
    assert set(exposure) == {"EUR", "USD"}
    assert exposure["EUR"].net_units == amount("0")
    assert exposure["EUR"].gross_units == amount("2")
    assert exposure["USD"].net_units == amount("0")
    assert exposure["USD"].gross_units == amount("4")


def test_foreign_quote_pnl_conversion_is_exact_and_fill_bound() -> None:
    account = spec()
    result = build_account_ledger(
        account,
        (
            fill(account, 0, "3", "2", symbol="EURGBP", conversion="1.25"),
            fill(account, 1, "-2", "2.5", symbol="EURGBP", conversion="1.5"),
        ),
    )
    assert result.final_state.realized_account_pnl == amount("1.5")
    assert result.receipts[-1].allocations[0].realized_account_pnl == amount(
        "1.5"
    )
    with pytest.raises(ValueError, match="conversion"):
        build_account_ledger(account, (fill(account, 0, "1", conversion="2"),))


@pytest.mark.parametrize("sequence", (1, 3))
def test_contiguous_account_local_sequence_required(sequence: int) -> None:
    account = spec()
    with pytest.raises(ValueError, match="sequence"):
        build_account_ledger(account, (fill(account, sequence, "1"),))


def test_clock_regression_refused_equal_times_allowed() -> None:
    account = spec()
    with pytest.raises(ValueError, match="time"):
        build_account_ledger(
            account,
            (fill(account, 0, "1", time=20), fill(account, 1, "1", time=19)),
        )
    assert (
        len(
            build_account_ledger(
                account,
                (
                    fill(account, 0, "1", time=20),
                    fill(account, 1, "1", time=20),
                ),
            ).fills
        )
        == 2
    )


def test_other_account_and_policy_swap_rejected() -> None:
    account, other = spec(), spec(revision="1.0.1")
    ledger = build_account_ledger(account, (fill(account, 0, "1"),))
    assert account.account_id != other.account_id
    with pytest.raises(ValueError, match="different account"):
        build_account_ledger(account, (fill(other, 0, "1"),))
    with pytest.raises(ValueError, match="specification"):
        replay_account_ledger(
            other, ledger, expected_dataset_id=ledger.dataset_id
        )


def test_omitted_history_resealed_root_rejected_but_explicit_branch_is_valid() -> (
    None
):
    account = spec()
    first = fill(account, 0, "2")
    prefix = build_account_ledger(account, (first,))
    full = apply_account_execution(
        account,
        prefix,
        fill(account, 1, "-1"),
        expected_dataset_id=prefix.dataset_id,
    )
    with pytest.raises(ValueError, match="retained root"):
        replay_account_ledger(
            account, prefix, expected_dataset_id=full.dataset_id
        )
    with pytest.raises(ValueError, match="retained root"):
        apply_account_execution(
            account,
            prefix,
            fill(account, 1, "1"),
            expected_dataset_id=full.dataset_id,
        )
    branch = apply_account_execution(
        account,
        prefix,
        fill(account, 1, "3"),
        expected_dataset_id=prefix.dataset_id,
    )
    assert branch.dataset_id != full.dataset_id
    assert replay(account, prefix) == prefix
    assert replay(account, branch) == branch


def test_resealed_arithmetic_forgery_is_not_replay_authority() -> None:
    account = spec()
    valid = build_account_ledger(
        account, (fill(account, 0, "2", "1"), fill(account, 1, "-2", "2"))
    )
    original = valid.final_state
    false_state = AccountStateV1(
        original.account_id,
        original.next_sequence,
        original.last_event_time_ns,
        original.open_lots,
        amount("99"),
        original.currency_exposures,
    )
    old = valid.receipts[-1]
    old_allocation = old.allocations[0]
    false_allocation = AccountAllocationV1(
        old_allocation.lot_id,
        old_allocation.opening_fill_id,
        old_allocation.quantity_closed,
        old_allocation.entry_price,
        old_allocation.exit_price,
        amount("99"),
    )
    false_receipt = AccountFillReceiptV1(
        old.account_id,
        old.fill_id,
        old.before_state_id,
        false_state.state_id,
        (false_allocation,),
        amount("99"),
    )
    forged = AccountLedgerV1(
        account,
        valid.fills,
        valid.receipts[:-1] + (false_receipt,),
        false_state,
    )
    with pytest.raises(ValueError, match="actual replay"):
        replay(account, forged)
    with pytest.raises(ValueError, match="actual replay"):
        apply_account_execution(
            account,
            forged,
            fill(account, 2, "1"),
            expected_dataset_id=forged.dataset_id,
        )


def test_empty_history_and_detached_replay() -> None:
    account = spec()
    empty = build_account_ledger(account, ())
    assert empty.final_state.next_sequence == 0
    assert empty.final_state.last_event_time_ns is None
    assert not empty.receipts and not empty.final_state.open_lots
    assert empty.final_state.realized_account_pnl == amount("0")
    restored = replay(account, empty)
    assert restored == empty and restored is not empty
    assert restored.spec is not account


def test_readmission_rejects_bypass_mutation() -> None:
    account = spec()
    ledger = build_account_ledger(account, (fill(account, 0, "1"),))
    object.__setattr__(ledger.final_state, "realized_account_pnl", amount("99"))
    with pytest.raises((ValueError, TypeError)):
        replay(account, ledger)


def test_single_iterative_replay_for_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = spec()
    ledger = build_account_ledger(
        account, tuple(fill(account, i, "1") for i in range(8))
    )
    original = module._step
    seen: list[int] = []

    def counted(*args: object, **kwargs: object) -> object:
        seen.append(args[2].sequence)  # type: ignore[attr-defined]
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(module, "_step", counted)
    apply_account_execution(
        account,
        ledger,
        fill(account, 8, "-2"),
        expected_dataset_id=ledger.dataset_id,
    )
    assert seen == list(range(9))


def test_overbound_history_refuses_before_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = spec()

    def forbidden(*args: object) -> object:
        raise AssertionError("transition must not run")

    monkeypatch.setattr(module, "_step", forbidden)
    with pytest.raises(ValueError, match="bounded exact tuple"):
        build_account_ledger(account, (fill(account, 0, "1"),) * 1025)
    with pytest.raises(ValueError, match="bounded exact tuple"):
        build_account_ledger(account, [])  # type: ignore[arg-type]


def test_rational_overflow_does_not_round() -> None:
    account = spec()
    value = ExactAmountV1(2**255)
    oversized = AccountFillV1(
        account.account_id,
        0,
        100,
        "EURUSD",
        value,
        value,
        amount("1"),
        "fixed-conversion:v1",
    )
    with pytest.raises(ValueError):
        build_account_ledger(account, (oversized,))


def test_pair_inventory_bound_is_prospective(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = spec()
    executions = tuple(
        fill(
            account,
            index,
            "1",
            symbol=f"A{chr(65 + index // 26)}{chr(65 + index % 26)}USD",
        )
        for index in range(33)
    )
    allowed = build_account_ledger(account, executions[:32])
    assert len(allowed.final_state.open_lots) == 32

    def forbidden(*args: object) -> object:
        raise AssertionError("pair inventory must preflight before transitions")

    monkeypatch.setattr(module, "_step", forbidden)
    with pytest.raises(ValueError, match="currency-pair bound"):
        build_account_ledger(account, executions)


def test_partial_allocation_binds_consumed_state_not_opening_state() -> None:
    account = spec()
    first = build_account_ledger(account, (fill(account, 0, "3"),))
    second = apply_account_execution(
        account,
        first,
        fill(account, 1, "-1"),
        expected_dataset_id=first.dataset_id,
    )
    third = apply_account_execution(
        account,
        second,
        fill(account, 2, "-1"),
        expected_dataset_id=second.dataset_id,
    )
    assert (
        third.receipts[-1].allocations[0].lot_id
        == second.final_state.open_lots[0].lot_id
    )
    assert (
        third.receipts[-1].allocations[0].lot_id
        != first.final_state.open_lots[0].lot_id
    )
    assert (
        third.receipts[-1].allocations[0].opening_fill_id
        == first.fills[0].fill_id
    )


def test_maximum_linear_history_conservation_replay_and_append_bound() -> None:
    account = spec()
    executions = tuple(
        fill(account, index, "1" if index % 2 == 0 else "-1")
        for index in range(1024)
    )
    result = build_account_ledger(account, executions)
    assert len(result.fills) == len(result.receipts) == 1024
    assert result.final_state.next_sequence == 1024
    assert not result.final_state.open_lots
    assert not result.final_state.currency_exposures
    assert result.final_state.realized_account_pnl == amount("0")
    allocations = tuple(
        allocation
        for receipt in result.receipts
        for allocation in receipt.allocations
    )
    assert len(allocations) == 512
    assert {allocation.opening_fill_id for allocation in allocations} == {
        execution.fill_id for execution in executions[::2]
    }
    assert all(
        allocation.quantity_closed == amount("1") for allocation in allocations
    )
    assert all(
        allocation.realized_account_pnl == amount("0")
        for allocation in allocations
    )
    assert all(
        receipt.realized_account_pnl_delta == amount("0")
        for receipt in result.receipts
    )
    assert replay(account, result).to_json() == result.to_json()
    # The full-history refusal precedes inspecting any proposed next execution.
    with pytest.raises(ValueError, match="execution bound"):
        apply_account_execution(
            account,
            result,
            executions[0],
            expected_dataset_id=result.dataset_id,
        )


@pytest.mark.parametrize(
    "root", ("", "account-spec:sha256:" + "0" * 64, "x" * 1000)
)
def test_malformed_anchor_refuses_before_record_readmission(
    monkeypatch: pytest.MonkeyPatch, root: str
) -> None:
    def forbidden(*args: object) -> object:
        raise AssertionError("malformed anchor must precede record admission")

    monkeypatch.setattr(module, "readmit", forbidden)
    with pytest.raises(ValueError, match="content ID"):
        replay_account_ledger(None, None, expected_dataset_id=root)  # type: ignore[arg-type]
