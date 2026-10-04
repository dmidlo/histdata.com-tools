"""Immutable fictional retail-FX account records; replay is separate authority.

The frozen modern software profile is not legal advice, historical compliance,
real-account eligibility, empirical qualification or a margin/financing model.
Quantities are base-currency units, and currency exposures are entry-notional
principal legs rather than marked-to-market valuations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar

from .account_codec import (
    MAX_EVENTS,
    MAX_PAIRS,
    AccountRecord,
    ExactAmountV1,
    readmit,
    require_amount,
    require_clock,
    require_id,
    require_symbol,
    require_text,
)

__all__ = [
    "AccountAllocationV1",
    "AccountFillReceiptV1",
    "AccountFillV1",
    "AccountLedgerV1",
    "AccountLotV1",
    "AccountPolicyV1",
    "AccountSpecV1",
    "AccountStateV1",
    "CurrencyExposureV1",
    "ExactAmountV1",
    "PolicySourceV1",
    "frozen_us_fifo_policy",
    "readmit",
]

ZERO = ExactAmountV1(0)
ONE = ExactAmountV1(1)
FROZEN_REVIEW_TIME_NS = 1791083374000000000


def _currency(value: Any) -> None:
    if type(value) is not str or re.fullmatch(r"[A-Z]{3}", value) is None:
        raise ValueError("currency requires an uppercase three-letter code")


def _sequence(value: Any, *, final: bool = False) -> None:
    if type(value) is not int or not 0 <= value <= MAX_EVENTS - (not final):
        raise ValueError("account-local sequence exceeds bounds")


def _nested(value: Any, cls: type[AccountRecord]) -> None:
    if type(value) is not cls:
        raise TypeError(f"nested record requires exact {cls.__name__}")
    cls._validate(value)


def _tuple(
    value: Any, cls: type[AccountRecord], maximum: int = MAX_EVENTS
) -> None:
    if type(value) is not tuple or len(value) > maximum:
        raise ValueError("account records require a bounded exact tuple")
    for item in value:
        _nested(item, cls)


def _positive(value: Any, name: str) -> None:
    require_amount(value, name)
    if value <= ZERO:
        raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class PolicySourceV1(AccountRecord):
    source_url: str
    source_title: str
    reviewed_at_ns: int
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-policy-source.v1"
    KIND: ClassVar[str] = "account-policy-source"

    def _validate(self) -> None:
        require_text(self.source_url, "source_url")
        if not self.source_url.startswith("https://"):
            raise ValueError("policy review reference requires HTTPS")
        require_text(self.source_title, "source_title")
        require_clock(self.reviewed_at_ns, "reviewed_at_ns")


@dataclass(frozen=True, slots=True)
class AccountPolicyV1(AccountRecord):
    policy_revision: str
    label: str
    effective_from_ns: int
    effective_until_ns: int | None
    sources: tuple[PolicySourceV1, ...]
    close_rule: str = "fifo_with_customer_directed_same_size"
    jurisdiction: str = "US"
    account_class: str = "fictional_retail_fdm_non_ecp"
    product_scope: str = "leveraged_off_exchange_forex"
    historical_assumption: str = "frozen_modern_counterfactual"
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-policy.v1"
    KIND: ClassVar[str] = "account-policy"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "sources": (PolicySourceV1, True)
    }
    MARKERS: ClassVar[dict[str, str]] = {
        "applicability": "software_profile_not_historical_or_future_law",
        "same_size_semantics": "oldest_full_unmodified_refuse_matching_partial_original_or_remaining_v1",
    }

    @property
    def policy_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        if (
            type(self.policy_revision) is not str
            or len(self.policy_revision) > 32
            or re.fullmatch(
                r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)",
                self.policy_revision,
            )
            is None
        ):
            raise ValueError("policy revision requires bounded SemVer core")
        require_text(self.label, "policy label")
        require_clock(self.effective_from_ns, "effective_from_ns")
        if self.effective_until_ns is not None:
            require_clock(self.effective_until_ns, "effective_until_ns")
            if self.effective_until_ns <= self.effective_from_ns:
                raise ValueError(
                    "policy applicability interval must be positive"
                )
        _tuple(self.sources, PolicySourceV1, 8)
        if not self.sources or len(
            {item.source_url for item in self.sources}
        ) != len(self.sources):
            raise ValueError("policy needs distinct retained review references")
        if any(
            item.reviewed_at_ns > self.effective_from_ns
            for item in self.sources
        ):
            raise ValueError(
                "software applicability cannot precede source review"
            )
        if type(self.close_rule) is not str or self.close_rule not in (
            "fifo_only",
            "fifo_with_customer_directed_same_size",
        ):
            raise ValueError("unsupported account close rule")
        expected = (
            (self.jurisdiction, "US"),
            (self.account_class, "fictional_retail_fdm_non_ecp"),
            (self.product_scope, "leveraged_off_exchange_forex"),
            (self.historical_assumption, "frozen_modern_counterfactual"),
        )
        if any(
            type(value) is not str or value != target
            for value, target in expected
        ):
            raise ValueError("unsupported fictional account policy scope")


@dataclass(frozen=True, slots=True)
class AccountSpecV1(AccountRecord):
    account_label: str
    account_currency: str
    policy: AccountPolicyV1
    policy_as_of_ns: int
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-spec.v1"
    KIND: ClassVar[str] = "account-spec"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "policy": (AccountPolicyV1, False)
    }
    MARKERS: ClassVar[dict[str, str]] = {
        "quantity_unit": "base_currency_units",
        "eligibility": "fictional_assumption_not_verified_real_account",
    }

    @property
    def account_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_text(self.account_label, "account_label", maximum=128)
        _currency(self.account_currency)
        _nested(self.policy, AccountPolicyV1)
        require_clock(self.policy_as_of_ns, "policy_as_of_ns")
        if self.policy_as_of_ns < self.policy.effective_from_ns or (
            self.policy.effective_until_ns is not None
            and self.policy_as_of_ns >= self.policy.effective_until_ns
        ):
            raise ValueError(
                "policy_as_of outside software applicability interval"
            )


@dataclass(frozen=True, slots=True)
class AccountFillV1(AccountRecord):
    account_id: str
    sequence: int
    event_time_ns: int
    symbol: str
    signed_quantity: ExactAmountV1
    price: ExactAmountV1
    quote_to_account: ExactAmountV1
    conversion_reference: str
    close_mode: str = "fifo"
    strategy_id: str = "unspecified"
    customer_direction_id: str | None = None
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-fill.v1"
    KIND: ClassVar[str] = "account-fill"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "signed_quantity": (ExactAmountV1, False),
        "price": (ExactAmountV1, False),
        "quote_to_account": (ExactAmountV1, False),
    }
    MARKERS: ClassVar[dict[str, str]] = {
        "conversion_semantics": "quote_currency_to_account_currency_at_fill_v1",
        "quantity_unit": "base_currency_units",
    }

    @property
    def fill_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_id(self.account_id, "account_id", kind="account-spec")
        _sequence(self.sequence)
        require_clock(self.event_time_ns, "event_time_ns")
        require_symbol(self.symbol)
        require_amount(self.signed_quantity, "signed_quantity")
        if self.signed_quantity == ZERO:
            raise ValueError("fill quantity cannot be zero")
        _positive(self.price, "price")
        _positive(self.quote_to_account, "quote_to_account")
        require_text(
            self.conversion_reference, "conversion_reference", maximum=256
        )
        require_text(self.strategy_id, "strategy_id", maximum=128)
        if type(self.close_mode) is not str or self.close_mode not in (
            "fifo",
            "customer_directed_same_size",
        ):
            raise ValueError("unsupported close mode")
        if self.close_mode == "fifo":
            if self.customer_direction_id is not None:
                raise ValueError(
                    "FIFO does not accept a customer lot direction"
                )
        else:
            require_text(
                self.customer_direction_id, "customer_direction_id", maximum=256
            )


@dataclass(frozen=True, slots=True)
class AccountLotV1(AccountRecord):
    account_id: str
    symbol: str
    opening_fill_id: str
    opened_sequence: int
    opened_at_ns: int
    original_signed_quantity: ExactAmountV1
    remaining_signed_quantity: ExactAmountV1
    entry_price: ExactAmountV1
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-lot.v1"
    KIND: ClassVar[str] = "account-lot"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "original_signed_quantity": (ExactAmountV1, False),
        "remaining_signed_quantity": (ExactAmountV1, False),
        "entry_price": (ExactAmountV1, False),
    }

    @property
    def lot_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_id(self.account_id, "account_id", kind="account-spec")
        require_id(self.opening_fill_id, "opening_fill_id", kind="account-fill")
        require_symbol(self.symbol)
        _sequence(self.opened_sequence)
        require_clock(self.opened_at_ns, "opened_at_ns")
        require_amount(
            self.original_signed_quantity, "original_signed_quantity"
        )
        require_amount(
            self.remaining_signed_quantity, "remaining_signed_quantity"
        )
        if (
            self.original_signed_quantity == ZERO
            or self.remaining_signed_quantity == ZERO
            or (self.original_signed_quantity < ZERO)
            != (self.remaining_signed_quantity < ZERO)
            or abs(self.remaining_signed_quantity)
            > abs(self.original_signed_quantity)
        ):
            raise ValueError(
                "lot remaining quantity must preserve original side and bound"
            )
        _positive(self.entry_price, "entry_price")


@dataclass(frozen=True, slots=True)
class AccountAllocationV1(AccountRecord):
    lot_id: str
    opening_fill_id: str
    quantity_closed: ExactAmountV1
    entry_price: ExactAmountV1
    exit_price: ExactAmountV1
    realized_account_pnl: ExactAmountV1
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-allocation.v1"
    KIND: ClassVar[str] = "account-allocation"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        name: (ExactAmountV1, False)
        for name in (
            "quantity_closed",
            "entry_price",
            "exit_price",
            "realized_account_pnl",
        )
    }

    @property
    def allocation_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_id(self.lot_id, "lot_id", kind="account-lot")
        require_id(self.opening_fill_id, "opening_fill_id", kind="account-fill")
        _positive(self.quantity_closed, "quantity_closed")
        _positive(self.entry_price, "entry_price")
        _positive(self.exit_price, "exit_price")
        require_amount(self.realized_account_pnl, "realized_account_pnl")


@dataclass(frozen=True, slots=True)
class CurrencyExposureV1(AccountRecord):
    currency: str
    net_units: ExactAmountV1
    gross_units: ExactAmountV1
    SCHEMA: ClassVar[str] = "histdatacom.trader-currency-exposure.v1"
    KIND: ClassVar[str] = "currency-exposure"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "net_units": (ExactAmountV1, False),
        "gross_units": (ExactAmountV1, False),
    }

    @property
    def exposure_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        _currency(self.currency)
        require_amount(self.net_units, "net_units")
        require_amount(self.gross_units, "gross_units")
        if self.gross_units <= ZERO or self.gross_units < abs(self.net_units):
            raise ValueError(
                "gross exposure must cover absolute net and be positive"
            )


@dataclass(frozen=True, slots=True)
class AccountStateV1(AccountRecord):
    account_id: str
    next_sequence: int
    last_event_time_ns: int | None
    open_lots: tuple[AccountLotV1, ...]
    realized_account_pnl: ExactAmountV1
    currency_exposures: tuple[CurrencyExposureV1, ...]
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-state.v1"
    KIND: ClassVar[str] = "account-state"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "open_lots": (AccountLotV1, True),
        "realized_account_pnl": (ExactAmountV1, False),
        "currency_exposures": (CurrencyExposureV1, True),
    }
    MARKERS: ClassVar[dict[str, str]] = {
        "exposure_basis": "open_lot_entry_notional_currency_principals_v1"
    }

    @property
    def state_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_id(self.account_id, "account_id", kind="account-spec")
        _sequence(self.next_sequence, final=True)
        if self.last_event_time_ns is not None:
            require_clock(self.last_event_time_ns, "last_event_time_ns")
        _tuple(self.open_lots, AccountLotV1)
        _tuple(self.currency_exposures, CurrencyExposureV1, MAX_PAIRS * 2)
        require_amount(self.realized_account_pnl, "realized_account_pnl")
        if self.next_sequence == 0:
            if (
                self.last_event_time_ns is not None
                or self.open_lots
                or self.currency_exposures
                or self.realized_account_pnl != ZERO
            ):
                raise ValueError("initial state must be empty and zero")
        elif self.last_event_time_ns is None:
            raise ValueError("noninitial state requires last event time")
        if len({lot.symbol for lot in self.open_lots}) > MAX_PAIRS:
            raise ValueError("account pair count exceeds bound")
        previous = -1
        previous_time = -1
        sides: dict[str, bool] = {}
        opening_ids: set[str] = set()
        for lot in self.open_lots:
            if (
                lot.account_id != self.account_id
                or not previous < lot.opened_sequence < self.next_sequence
                or not previous_time
                <= lot.opened_at_ns
                <= (self.last_event_time_ns or 0)
                or lot.opening_fill_id in opening_ids
            ):
                raise ValueError(
                    "lot account, immutable age or ordering mismatch"
                )
            previous, previous_time = lot.opened_sequence, lot.opened_at_ns
            opening_ids.add(lot.opening_fill_id)
            side = lot.remaining_signed_quantity > ZERO
            if lot.symbol in sides and sides[lot.symbol] != side:
                raise ValueError("opposing same-pair lots cannot coexist")
            sides[lot.symbol] = side
        currencies = tuple(item.currency for item in self.currency_exposures)
        if currencies != tuple(sorted(set(currencies))):
            raise ValueError("currency exposure order or uniqueness mismatch")
        if not self.open_lots and self.currency_exposures:
            raise ValueError("flat account cannot retain open-lot exposures")


@dataclass(frozen=True, slots=True)
class AccountFillReceiptV1(AccountRecord):
    account_id: str
    fill_id: str
    before_state_id: str
    after_state_id: str
    allocations: tuple[AccountAllocationV1, ...]
    realized_account_pnl_delta: ExactAmountV1
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-fill-receipt.v1"
    KIND: ClassVar[str] = "account-fill-receipt"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "allocations": (AccountAllocationV1, True),
        "realized_account_pnl_delta": (ExactAmountV1, False),
    }

    @property
    def receipt_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        require_id(self.account_id, "account_id", kind="account-spec")
        require_id(self.fill_id, "fill_id", kind="account-fill")
        require_id(
            self.before_state_id, "before_state_id", kind="account-state"
        )
        require_id(self.after_state_id, "after_state_id", kind="account-state")
        _tuple(self.allocations, AccountAllocationV1)
        require_amount(
            self.realized_account_pnl_delta, "realized_account_pnl_delta"
        )
        if len({item.opening_fill_id for item in self.allocations}) != len(
            self.allocations
        ):
            raise ValueError("receipt consumes one opening lot more than once")


@dataclass(frozen=True, slots=True)
class AccountLedgerV1(AccountRecord):
    spec: AccountSpecV1
    fills: tuple[AccountFillV1, ...]
    receipts: tuple[AccountFillReceiptV1, ...]
    final_state: AccountStateV1
    SCHEMA: ClassVar[str] = "histdatacom.trader-account-ledger.v1"
    KIND: ClassVar[str] = "account-ledger"
    NESTED: ClassVar[dict[str, tuple[type[Any], bool]]] = {
        "spec": (AccountSpecV1, False),
        "fills": (AccountFillV1, True),
        "receipts": (AccountFillReceiptV1, True),
        "final_state": (AccountStateV1, False),
    }
    MARKERS: ClassVar[dict[str, str]] = {
        "status": "fictional_unqualified_requires_replay",
        "history_scope": "new_or_independently_anchored_account_local_history",
    }

    @property
    def dataset_id(self) -> str:
        return self.artifact_id

    def _validate(self) -> None:
        _nested(self.spec, AccountSpecV1)
        _tuple(self.fills, AccountFillV1)
        _tuple(self.receipts, AccountFillReceiptV1)
        _nested(self.final_state, AccountStateV1)
        account_id = self.spec.account_id
        if (
            len(self.fills) != len(self.receipts)
            or self.final_state.next_sequence != len(self.fills)
            or self.final_state.account_id != account_id
        ):
            raise ValueError(
                "account ledger denominator or final state mismatch"
            )
        if len({fill.symbol for fill in self.fills}) > MAX_PAIRS:
            raise ValueError("account pair count exceeds bound")
        prior = AccountStateV1(account_id, 0, None, (), ZERO, ()).state_id
        previous_time = -1
        seen: dict[str, AccountFillV1] = {}
        for index, (fill, receipt) in enumerate(zip(self.fills, self.receipts)):
            if (
                fill.account_id != account_id
                or fill.sequence != index
                or fill.event_time_ns < previous_time
            ):
                raise ValueError("fill account, sequence or timestamp mismatch")
            if (
                fill.symbol[3:] == self.spec.account_currency
                and fill.quote_to_account != ONE
            ):
                raise ValueError(
                    "same-currency conversion factor must equal one"
                )
            if (
                self.spec.policy.close_rule == "fifo_only"
                and fill.close_mode != "fifo"
            ):
                raise ValueError("policy does not admit same-size exception")
            if (
                receipt.account_id != account_id
                or receipt.fill_id != fill.fill_id
                or receipt.before_state_id != prior
            ):
                raise ValueError(
                    "receipt account, fill or state chain mismatch"
                )
            for allocation in receipt.allocations:
                opening = seen.get(allocation.opening_fill_id)
                if (
                    opening is None
                    or opening.symbol != fill.symbol
                    or allocation.entry_price != opening.price
                    or allocation.exit_price != fill.price
                ):
                    raise ValueError(
                        "allocation opening fill or price binding mismatch"
                    )
            seen[fill.fill_id] = fill
            prior, previous_time = receipt.after_state_id, fill.event_time_ns
        if (
            self.final_state.state_id != prior
            or self.final_state.last_event_time_ns
            != (self.fills[-1].event_time_ns if self.fills else None)
        ):
            raise ValueError("final state does not match receipt chain")
        for lot in self.final_state.open_lots:
            opening = seen.get(lot.opening_fill_id)
            if opening is None or (
                lot.symbol,
                lot.opened_sequence,
                lot.opened_at_ns,
                lot.original_signed_quantity,
                lot.entry_price,
            ) != (
                opening.symbol,
                opening.sequence,
                opening.event_time_ns,
                opening.signed_quantity,
                opening.price,
            ):
                raise ValueError(
                    "open lot immutable opening attribution mismatch"
                )


ACCOUNT_RECORD_TYPES = (
    PolicySourceV1,
    AccountPolicyV1,
    AccountSpecV1,
    AccountFillV1,
    AccountLotV1,
    AccountAllocationV1,
    CurrencyExposureV1,
    AccountStateV1,
    AccountFillReceiptV1,
    AccountLedgerV1,
)


def frozen_us_fifo_policy() -> AccountPolicyV1:
    """Reviewed modern fictional profile, not historical/future law assurance."""
    references = (
        (
            "https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=RULE+2-43&Section=4",
            "NFA Compliance Rule 2-43(b)",
        ),
        (
            "https://www.nfa.futures.org/rulebooksql/rules.aspx?RuleID=BYLAW+1507&Section=3",
            "NFA Bylaw 1507 forex dealer member scope",
        ),
        (
            "https://www.ecfr.gov/current/title-17/chapter-I/part-5/section-5.1",
            "17 CFR 5.1 retail forex definitions",
        ),
    )
    return AccountPolicyV1(
        "1.0.0",
        "Frozen modern fictional U.S. retail-FDM FIFO profile",
        FROZEN_REVIEW_TIME_NS,
        None,
        tuple(
            PolicySourceV1(url, title, FROZEN_REVIEW_TIME_NS)
            for url, title in references
        ),
    )
