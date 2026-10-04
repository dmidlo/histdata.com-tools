"""Exact, bounded fictional-account FIFO transitions and anchored replay.

This is a pure account-local accounting law, not execution simulation, source
authenticity, a durable latest-head service, or legal compliance certification.
All strategies share the account/pair book. No caller can select a closing lot.
"""

from __future__ import annotations

from histdatacom.synthetic.traders.account_codec import (
    MAX_EVENTS,
    MAX_PAIRS,
    MAX_WIRE_BYTES,
    require_id,
)
from histdatacom.synthetic.traders.account_contracts import (
    AccountAllocationV1,
    AccountFillReceiptV1,
    AccountFillV1,
    AccountLedgerV1,
    AccountLotV1,
    AccountSpecV1,
    AccountStateV1,
    CurrencyExposureV1,
    ExactAmountV1,
    readmit,
)

__all__ = [
    "apply_account_execution",
    "build_account_ledger",
    "replay_account_ledger",
]

_ZERO = ExactAmountV1(0)
_ONE = ExactAmountV1(1)


def _sign(value: ExactAmountV1) -> ExactAmountV1:
    return _ONE if value > _ZERO else -_ONE


def _exposures(
    lots: tuple[AccountLotV1, ...],
) -> tuple[CurrencyExposureV1, ...]:
    # Separate currency units: a zero net with positive gross must remain.
    totals: dict[str, tuple[ExactAmountV1, ExactAmountV1]] = {}
    for lot in lots:
        quantity = lot.remaining_signed_quantity
        for currency, leg in (
            (lot.symbol[:3], quantity),
            (lot.symbol[3:], -(quantity * lot.entry_price)),
        ):
            net, gross = totals.get(currency, (_ZERO, _ZERO))
            totals[currency] = (net + leg, gross + abs(leg))
    return tuple(
        CurrencyExposureV1(currency, net, gross)
        for currency, (net, gross) in sorted(totals.items())
        if net != _ZERO or gross != _ZERO
    )


def _empty(spec: AccountSpecV1) -> AccountStateV1:
    return AccountStateV1(spec.account_id, 0, None, (), _ZERO, ())


def _allocation(
    lot: AccountLotV1, quantity: ExactAmountV1, fill: AccountFillV1
) -> AccountAllocationV1:
    pnl = (
        quantity
        * (fill.price - lot.entry_price)
        * _sign(lot.remaining_signed_quantity)
        * fill.quote_to_account
    )
    return AccountAllocationV1(
        lot.lot_id,
        lot.opening_fill_id,
        quantity,
        lot.entry_price,
        fill.price,
        pnl,
    )


def _remaining_lot(lot: AccountLotV1, remaining: ExactAmountV1) -> AccountLotV1:
    return AccountLotV1(
        lot.account_id,
        lot.symbol,
        lot.opening_fill_id,
        lot.opened_sequence,
        lot.opened_at_ns,
        lot.original_signed_quantity,
        remaining,
        lot.entry_price,
    )


def _opposite(lot: AccountLotV1, fill: AccountFillV1) -> bool:
    return lot.symbol == fill.symbol and (
        (lot.remaining_signed_quantity > _ZERO)
        != (fill.signed_quantity > _ZERO)
    )


def _directed_index(
    spec: AccountSpecV1,
    lots: tuple[AccountLotV1, ...],
    fill: AccountFillV1,
) -> int:
    if spec.policy.close_rule != "fifo_with_customer_directed_same_size":
        raise ValueError("account policy does not permit same-size direction")
    quantity = abs(fill.signed_quantity)
    selected: int | None = None
    for index, lot in enumerate(lots):
        if not _opposite(lot, fill):
            continue
        original = abs(lot.original_signed_quantity)
        remaining = abs(lot.remaining_signed_quantity)
        if original != remaining and quantity in (original, remaining):
            raise ValueError(
                "same-size direction is ambiguous for a modified lot"
            )
        if original == remaining == quantity and selected is None:
            selected = index
    if selected is None:
        raise ValueError(
            "same-size direction has no intact matching opposite lot"
        )
    return selected


def _step(
    spec: AccountSpecV1, state: AccountStateV1, fill: AccountFillV1
) -> tuple[AccountStateV1, AccountFillReceiptV1]:
    """Internal step over already admitted inputs, never public state authority."""
    if fill.account_id != spec.account_id:
        raise ValueError("execution belongs to a different account")
    if fill.sequence != state.next_sequence:
        raise ValueError(
            "execution sequence must be contiguous and start at zero"
        )
    if (
        state.last_event_time_ns is not None
        and fill.event_time_ns < state.last_event_time_ns
    ):
        raise ValueError("execution time must not regress")
    if (
        fill.symbol[3:] == spec.account_currency
        and fill.quote_to_account != _ONE
    ):
        raise ValueError("same-currency P/L conversion must equal one")

    remaining = abs(fill.signed_quantity)
    allocations: list[AccountAllocationV1] = []
    lots: list[AccountLotV1] = []
    if fill.close_mode == "customer_directed_same_size":
        index = _directed_index(spec, state.open_lots, fill)
        allocations.append(_allocation(state.open_lots[index], remaining, fill))
        lots.extend(state.open_lots[:index])
        lots.extend(state.open_lots[index + 1 :])
        remaining = _ZERO
    else:
        for lot in state.open_lots:
            if remaining == _ZERO or not _opposite(lot, fill):
                lots.append(lot)
                continue
            quantity = min(remaining, abs(lot.remaining_signed_quantity))
            allocations.append(_allocation(lot, quantity, fill))
            remaining = remaining - quantity
            residual = abs(lot.remaining_signed_quantity) - quantity
            if residual != _ZERO:
                lots.append(
                    _remaining_lot(
                        lot, residual * _sign(lot.remaining_signed_quantity)
                    )
                )
        if remaining != _ZERO:
            lots.append(
                AccountLotV1(
                    spec.account_id,
                    fill.symbol,
                    fill.fill_id,
                    fill.sequence,
                    fill.event_time_ns,
                    # Full execution quantity, including a reversal's closing
                    # portion: a residual is not an intact same-size transaction.
                    fill.signed_quantity,
                    remaining * _sign(fill.signed_quantity),
                    fill.price,
                )
            )
    delta = _ZERO
    for allocation in allocations:
        delta = delta + allocation.realized_account_pnl
    open_lots = tuple(lots)
    after = AccountStateV1(
        spec.account_id,
        fill.sequence + 1,
        fill.event_time_ns,
        open_lots,
        state.realized_account_pnl + delta,
        _exposures(open_lots),
    )
    receipt = AccountFillReceiptV1(
        spec.account_id,
        fill.fill_id,
        state.state_id,
        after.state_id,
        tuple(allocations),
        delta,
    )
    return after, receipt


def _admit_fills(
    spec: AccountSpecV1, executions: tuple[AccountFillV1, ...]
) -> tuple[AccountFillV1, ...]:
    if type(executions) is not tuple or len(executions) > MAX_EVENTS:
        raise ValueError("executions require a bounded exact tuple")
    admitted: list[AccountFillV1] = []
    pairs: set[str] = set()
    size = len(spec.to_json().encode("ascii"))
    for execution in executions:
        fill = readmit(execution, AccountFillV1)
        size += len(fill.to_json().encode("ascii"))
        if size > MAX_WIRE_BYTES:
            raise ValueError("account execution inputs exceed byte bound")
        pairs.add(fill.symbol)
        if len(pairs) > MAX_PAIRS:
            raise ValueError("account history exceeds currency-pair bound")
        admitted.append(fill)
    return tuple(admitted)


def _same_state(actual: AccountStateV1, expected: AccountStateV1) -> None:
    if actual.to_json() != expected.to_json():
        raise ValueError("account final state does not match actual replay")


def _reconstruct(
    spec: AccountSpecV1,
    fills: tuple[AccountFillV1, ...],
    expected: AccountLedgerV1 | None = None,
) -> AccountLedgerV1:
    state = _empty(spec)
    receipts: list[AccountFillReceiptV1] = []
    pairs: set[str] = set()
    prefix_length = len(expected.fills) if expected is not None else 0
    for index, fill in enumerate(fills):
        if expected is not None and index == prefix_length:
            _same_state(state, expected.final_state)
        pairs.add(fill.symbol)
        if len(pairs) > MAX_PAIRS:
            raise ValueError("account history exceeds currency-pair bound")
        state, receipt = _step(spec, state, fill)
        if (
            expected is not None
            and index < prefix_length
            and receipt.to_json() != expected.receipts[index].to_json()
        ):
            raise ValueError("account receipt does not match actual replay")
        receipts.append(receipt)
    if expected is not None and len(fills) == prefix_length:
        _same_state(state, expected.final_state)
    return AccountLedgerV1(spec, fills, tuple(receipts), state)


def _anchored(
    spec: AccountSpecV1, expected: AccountLedgerV1, expected_dataset_id: str
) -> tuple[AccountSpecV1, AccountLedgerV1]:
    require_id(
        expected_dataset_id, "expected_dataset_id", kind="account-ledger"
    )
    account = readmit(spec, AccountSpecV1)
    ledger = readmit(expected, AccountLedgerV1)
    if (
        type(expected_dataset_id) is not str
        or ledger.dataset_id != expected_dataset_id
    ):
        raise ValueError(
            "account dataset does not match independently retained root"
        )
    if account.to_json() != ledger.spec.to_json():
        raise ValueError("account specification differs from retained dataset")
    return account, ledger


def build_account_ledger(
    spec: AccountSpecV1, executions: tuple[AccountFillV1, ...]
) -> AccountLedgerV1:
    """Build a new fictional history; establish no external history completeness."""
    account = readmit(spec, AccountSpecV1)
    return _reconstruct(account, _admit_fills(account, executions))


def replay_account_ledger(
    spec: AccountSpecV1,
    expected: AccountLedgerV1,
    *,
    expected_dataset_id: str,
) -> AccountLedgerV1:
    """Recompute a history against an independently retained spec and root."""
    account, ledger = _anchored(spec, expected, expected_dataset_id)
    actual = _reconstruct(account, ledger.fills, ledger)
    if actual.to_json() != ledger.to_json():
        raise ValueError("account dataset does not match actual replay")
    return actual


def apply_account_execution(
    spec: AccountSpecV1,
    ledger: AccountLedgerV1,
    execution: AccountFillV1,
    *,
    expected_dataset_id: str,
) -> AccountLedgerV1:
    """Replay a selected immutable parent once, then append one execution.

    Branching from an explicitly selected older root is allowed. This function
    neither overwrites that parent nor promises a global latest-head/CAS check.
    """
    account, prior = _anchored(spec, ledger, expected_dataset_id)
    if len(prior.fills) >= MAX_EVENTS:
        raise ValueError("account history exceeds execution bound")
    fills = _admit_fills(account, prior.fills + (execution,))
    return _reconstruct(account, fills, prior)
