"""Pure account contract controls, not ledger execution or legal evidence."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from decimal import Inexact, ROUND_DOWN, localcontext

import pytest

from histdatacom.synthetic.traders import account_codec as codec
from histdatacom.synthetic.traders.account_contracts import (
    ACCOUNT_RECORD_TYPES,
    FROZEN_REVIEW_TIME_NS,
    AccountAllocationV1,
    AccountFillReceiptV1,
    AccountFillV1,
    AccountLedgerV1,
    AccountLotV1,
    AccountSpecV1,
    AccountStateV1,
    CurrencyExposureV1,
    ExactAmountV1 as A,
    frozen_us_fifo_policy,
    readmit,
)


def _records() -> tuple[codec.AccountRecord, ...]:
    """Handwritten structure only; genuine transitions belong to ledger tests."""
    policy = frozen_us_fifo_policy()
    spec = AccountSpecV1(
        "invented account", "USD", policy, FROZEN_REVIEW_TIME_NS
    )
    initial = AccountStateV1(spec.account_id, 0, None, (), A(0), ())
    fill = AccountFillV1(
        spec.account_id, 0, 1, "EURUSD", A(2), A(11, 10), A(1), "fixture:USD"
    )
    lot = AccountLotV1(
        spec.account_id, "EURUSD", fill.fill_id, 0, 1, A(2), A(2), A(11, 10)
    )
    exposure = CurrencyExposureV1("EUR", A(2), A(2))
    state = AccountStateV1(
        spec.account_id,
        1,
        1,
        (lot,),
        A(0),
        (exposure, CurrencyExposureV1("USD", A(-11, 5), A(11, 5))),
    )
    receipt = AccountFillReceiptV1(
        spec.account_id,
        fill.fill_id,
        initial.state_id,
        state.state_id,
        (),
        A(0),
    )
    allocation = AccountAllocationV1(
        lot.lot_id, fill.fill_id, A(1), A(11, 10), A(6, 5), A(1, 10)
    )
    ledger = AccountLedgerV1(spec, (fill,), (receipt,), state)
    return (
        policy.sources[0],
        policy,
        spec,
        fill,
        lot,
        allocation,
        exposure,
        state,
        receipt,
        ledger,
    )


@pytest.mark.parametrize("index", range(10))
def test_closed_records_roundtrip_exact_detached_immutable(index):
    record = _records()[index]
    assert type(record) is ACCOUNT_RECORD_TYPES[index]
    wire = record.to_json()
    restored = type(record).from_json(wire)
    assert restored == record
    assert restored is not record
    assert restored.to_json() == wire
    assert record.artifact_id.startswith(record.KIND + ":sha256:")
    assert readmit(record, type(record)) == record
    assert wire.isascii() and not wire.endswith("\n")
    field = next(iter(record.__dataclass_fields__))
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        setattr(record, field, None)


@pytest.mark.parametrize("index", range(10))
@pytest.mark.parametrize("change", ("unknown", "missing", "id", "schema"))
def test_record_fields_schema_and_identity_are_strict(index, change):
    record = _records()[index]
    data = record.to_dict()
    if change == "unknown":
        data["extra"] = None
    elif change == "missing":
        data.pop("artifact_id")
    elif change == "id":
        data["artifact_id"] = record.KIND + ":sha256:" + "0" * 64
    else:
        data["schema_version"] = "histdatacom.unknown.v1"
    with pytest.raises((TypeError, ValueError)):
        type(record).from_dict(data)


@pytest.mark.parametrize(
    ("text", "numerator", "denominator"),
    (
        ("0", 0, 1),
        ("-0.00", 0, 1),
        ("1.10", 11, 10),
        ("-12.345", -2469, 200),
        ("0.00001", 1, 100000),
    ),
)
def test_plain_decimal_is_exact_and_reduced(text, numerator, denominator):
    amount = A.from_decimal(text)
    assert amount == A(numerator, denominator)
    assert A.from_json(amount.to_json()) == amount


@pytest.mark.parametrize(
    "value", (True, False, 1.0, float("inf"), float("nan"), "1", None)
)
def test_amount_rejects_numeric_aliases(value):
    with pytest.raises((TypeError, ValueError)):
        A(value)
    with pytest.raises((TypeError, ValueError)):
        A(1, value)


@pytest.mark.parametrize(
    "text",
    (
        "1e2",
        "NaN",
        "Infinity",
        "+1",
        "01",
        ".1",
        "1.",
        " 1",
        "1 ",
        "1_0",
        1,
        True,
        "9" * 161,
    ),
)
def test_decimal_lexeme_rejects_coercion_and_unbounded_input(text):
    with pytest.raises((TypeError, ValueError)):
        A.from_decimal(text)


@pytest.mark.parametrize(
    "pair", ((2, 4), (0, 2), (1, -1), (1, 0), (2**256, 1), (1, 2**256))
)
def test_fraction_constructor_requires_reduced_bounded_form(pair):
    with pytest.raises((TypeError, ValueError)):
        A(*pair)


def test_ratio_and_arithmetic_independent_of_decimal_context():
    with localcontext() as context:
        context.prec = 1
        context.rounding = ROUND_DOWN
        context.traps[Inexact] = True
        assert A.from_ratio(2, 4) == A(1, 2)
        assert A.from_ratio(1, -2) == A(-1, 2)
        assert A.from_decimal("1.10") + A.from_decimal("0.20") == A(13, 10)
        assert A(1, 3) + A(1, 6) == A(1, 2)
        assert A(2, 3) * A(9, 4) == A(3, 2)
        assert A(2, 3) / A(4, 5) == A(5, 6)
        assert A(2) * (A(13, 10) - A(11, 10)) + A(2) * (
            A(13, 10) - A(6, 5)
        ) == A(3, 5)
        assert abs(-A(2, 3)) == A(2, 3)
        assert A(-1) < A(0) < A(1, 2)


def test_arithmetic_refuses_overflow_and_zero_divisor():
    maximum = A(2**256 - 1)
    with pytest.raises(ValueError):
        maximum + A(1)
    with pytest.raises(ValueError):
        maximum * A(2)
    with pytest.raises(ZeroDivisionError):
        A(1) / A(0)
    assert maximum / maximum == A(1)


def test_foreign_arithmetic_operand_is_rejected_before_callback():
    class Poison:
        def __neg__(self):
            raise AssertionError("foreign negation executed")

    for operation in (
        lambda: A(1) - Poison(),
        lambda: A(1) + Poison(),
        lambda: A(1) * Poison(),
        lambda: A(1) / Poison(),
    ):
        with pytest.raises(TypeError):
            operation()


@pytest.mark.parametrize(
    "wire",
    (
        '{"numerator":1,"numerator":1,"denominator":1}',
        '{"denominator":1,"numerator":1.0}',
        '{"denominator":1,"numerator":true}',
        '{"denominator":1,"numerator":NaN}',
        '{"denominator":1,"numerator":1}\n',
        '{"numerator":1,"denominator":1}',
    ),
)
def test_amount_json_rejects_duplicate_noncanonical_or_numeric_alias(wire):
    with pytest.raises(ValueError):
        A.from_json(wire)


def test_policy_sources_are_review_references_and_scope_is_explicit():
    policy = frozen_us_fifo_policy()
    assert (
        datetime.fromtimestamp(
            FROZEN_REVIEW_TIME_NS // 10**9, timezone.utc
        ).isoformat()
        == "2026-10-04T03:09:34+00:00"
    )
    assert policy.sources[0].source_url.endswith("RuleID=RULE+2-43&Section=4")
    assert "sha256" not in policy.sources[0].to_dict()
    assert (
        policy.MARKERS["applicability"]
        == "software_profile_not_historical_or_future_law"
    )
    assert policy.historical_assumption == "frozen_modern_counterfactual"
    assert policy.effective_from_ns == FROZEN_REVIEW_TIME_NS


@pytest.mark.parametrize(
    "kwargs",
    (
        {"jurisdiction": "EU"},
        {"account_class": "actual_eligible"},
        {"product_scope": "futures"},
        {"historical_assumption": "historical_law_verified"},
        {"close_rule": "caller_picks_lot"},
        {"policy_revision": "01.0.0"},
        {"policy_revision": "1.0"},
        {"sources": ()},
        {"sources": []},
        {"effective_until_ns": FROZEN_REVIEW_TIME_NS},
    ),
)
def test_policy_scope_and_version_are_closed(kwargs):
    with pytest.raises((TypeError, ValueError)):
        replace(frozen_us_fifo_policy(), **kwargs)


def test_policy_revision_binds_account_identity_and_half_open_applicability():
    policy = frozen_us_fifo_policy()
    changed = replace(policy, policy_revision="1.0.1")
    spec = AccountSpecV1("fictional", "USD", policy, FROZEN_REVIEW_TIME_NS)
    assert spec.account_id != replace(spec, policy=changed).account_id
    bounded = replace(policy, effective_until_ns=FROZEN_REVIEW_TIME_NS + 1)
    assert replace(spec, policy=bounded).policy == bounded
    for instant in (FROZEN_REVIEW_TIME_NS - 1, FROZEN_REVIEW_TIME_NS + 1):
        with pytest.raises(ValueError):
            replace(spec, policy=bounded, policy_as_of_ns=instant)


@pytest.mark.parametrize(
    "kwargs",
    (
        {"sequence": True},
        {"sequence": -1},
        {"sequence": 1024},
        {"event_time_ns": True},
        {"event_time_ns": 2**63},
        {"symbol": "eurusd"},
        {"symbol": "USDUSD"},
        {"signed_quantity": A(0)},
        {"price": A(0)},
        {"quote_to_account": A(-1)},
        {"close_mode": "arbitrary"},
        {"customer_direction_id": "pick-lot"},
        {"close_mode": "customer_directed_same_size"},
        {"strategy_id": "x" * 129},
    ),
)
def test_fill_numeric_scope_and_direction_controls(kwargs):
    with pytest.raises((TypeError, ValueError)):
        replace(_records()[3], **kwargs)


def test_strategy_and_direction_are_recorded_not_book_partition_keys():
    fill = _records()[3]
    directed = replace(
        fill,
        close_mode="customer_directed_same_size",
        customer_direction_id="customer-request-1",
    )
    assert directed.fill_id != fill.fill_id
    other_strategy = replace(fill, strategy_id="other")
    assert other_strategy.account_id == fill.account_id
    assert other_strategy.fill_id != fill.fill_id


@pytest.mark.parametrize(
    "kwargs",
    (
        {"remaining_signed_quantity": A(0)},
        {"remaining_signed_quantity": A(-1)},
        {"remaining_signed_quantity": A(3)},
        {"original_signed_quantity": A(0)},
        {"entry_price": A(0)},
    ),
)
def test_lot_quantity_side_and_original_bounds(kwargs):
    with pytest.raises(ValueError):
        replace(_records()[4], **kwargs)


def test_partial_lot_changes_state_id_but_preserves_opening_identity_age():
    lot = _records()[4]
    partial = replace(lot, remaining_signed_quantity=A(1))
    assert partial.lot_id != lot.lot_id
    assert (
        partial.opening_fill_id,
        partial.opened_sequence,
        partial.opened_at_ns,
        partial.original_signed_quantity,
    ) == (
        lot.opening_fill_id,
        lot.opened_sequence,
        lot.opened_at_ns,
        lot.original_signed_quantity,
    )
    reversal = replace(
        lot, original_signed_quantity=A(-8), remaining_signed_quantity=A(-3)
    )
    assert (
        reversal.original_signed_quantity != reversal.remaining_signed_quantity
    )


@pytest.mark.parametrize(
    "net,gross", ((A(1), A(0)), (A(2), A(1)), (A(-2), A(1)), (A(0), A(-1)))
)
def test_exposure_gross_is_not_absolute_net(net, gross):
    with pytest.raises(ValueError):
        CurrencyExposureV1("USD", net, gross)
    assert CurrencyExposureV1("USD", A(0), A(2)).gross_units == A(2)


def test_nested_exact_type_poison_is_rejected_without_serializer():
    class Poison:
        def to_dict(self):
            raise AssertionError("foreign serializer called")

    spec = _records()[2]
    object.__setattr__(spec, "policy", Poison())
    with pytest.raises(TypeError):
        readmit(spec, AccountSpecV1)


@pytest.mark.parametrize(
    "field,value", (("fills", []), ("fills", (None,) * 1025), ("spec", None))
)
def test_mutated_ledger_raw_admission_precedes_serialization(
    field, value, monkeypatch
):
    ledger = _records()[-1]
    object.__setattr__(ledger, field, value)
    monkeypatch.setattr(
        codec.json,
        "dumps",
        lambda *a, **k: pytest.fail("JSON allocation before raw rejection"),
    )
    with pytest.raises((TypeError, ValueError)):
        ledger.to_json()


def test_aggregate_astral_bound_is_before_serialization(monkeypatch):
    ledger = _records()[-1]
    fill = replace(ledger.fills[0], conversion_reference="😀" * 256)
    object.__setattr__(ledger, "fills", (fill,) * 1024)
    object.__setattr__(ledger, "receipts", (ledger.receipts[0],) * 1024)
    monkeypatch.setattr(
        codec.json,
        "dumps",
        lambda *a, **k: pytest.fail("unbounded JSON allocation"),
    )
    with pytest.raises(ValueError, match="bound"):
        ledger.to_json()


def test_raw_text_limit_precedes_json_parser(monkeypatch):
    monkeypatch.setattr(
        codec.json,
        "loads",
        lambda *a, **k: pytest.fail("oversize parser called"),
    )
    with pytest.raises(ValueError):
        AccountSpecV1.from_json(" " * (codec.MAX_WIRE_BYTES + 1))
    with pytest.raises(ValueError):
        AccountSpecV1.from_json("é")


def test_duplicate_nested_record_key_is_rejected():
    text = _records()[2].to_json()
    replaced = text.replace(
        '"account_currency":"USD"',
        '"account_currency":"USD","account_currency":"USD"',
        1,
    )
    with pytest.raises(ValueError, match="duplicate"):
        AccountSpecV1.from_json(replaced)


def test_wire_bounds_depth_nodes_and_del_escape(monkeypatch):
    value = None
    for _ in range(22):
        value = [value]
    with pytest.raises(ValueError, match="bounds"):
        codec.canonical(value)
    monkeypatch.setattr(codec, "MAX_WIRE_BYTES", 20)
    monkeypatch.setattr(
        codec.json,
        "dumps",
        lambda *a, **k: pytest.fail("DEL escaped bound checked too late"),
    )
    with pytest.raises(ValueError, match="bound"):
        codec.canonical("\x7f" * 4)


def test_linear_receipts_do_not_embed_fills_or_states():
    ledger = _records()[-1]
    data = ledger.to_dict()
    receipt = data["receipts"][0]
    assert {"fill_id", "before_state_id", "after_state_id"} <= receipt.keys()
    assert not {"fill", "before_state", "after_state"} & receipt.keys()
    assert data["status"] == "fictional_unqualified_requires_replay"
    assert len(data["fills"]) == len(data["receipts"]) == 1


def test_dataset_rejects_account_conversion_chain_and_opening_substitution():
    ledger = _records()[-1]
    with pytest.raises(ValueError, match="denominator"):
        replace(ledger, receipts=())
    with pytest.raises(ValueError, match="conversion"):
        replace(
            ledger, fills=(replace(ledger.fills[0], quote_to_account=A(2)),)
        )
    with pytest.raises(ValueError, match="chain"):
        replace(
            ledger,
            receipts=(
                replace(
                    ledger.receipts[0],
                    before_state_id=ledger.final_state.state_id,
                ),
            ),
        )
    changed_policy = replace(ledger.spec.policy, policy_revision="1.0.1")
    with pytest.raises(ValueError, match="final state"):
        replace(ledger, spec=replace(ledger.spec, policy=changed_policy))


def test_structural_wire_is_not_arithmetic_replay_authority():
    ledger = _records()[-1]
    state = replace(ledger.final_state, realized_account_pnl=A(99))
    receipt = replace(
        ledger.receipts[0],
        after_state_id=state.state_id,
        realized_account_pnl_delta=A(99),
    )
    resealed = replace(ledger, receipts=(receipt,), final_state=state)
    assert resealed.dataset_id != ledger.dataset_id
    assert AccountLedgerV1.from_json(resealed.to_json()) == resealed
    assert (
        resealed.to_dict()["status"] == "fictional_unqualified_requires_replay"
    )
    # Only actual replay with the independently retained dataset ID can verify.


def test_no_pass_or_arbitrary_close_callback_fields():
    ledger = _records()[-1]
    value = json.loads(ledger.to_json())
    value["verified"] = True
    with pytest.raises(ValueError):
        AccountLedgerV1.from_dict(value)
    with pytest.raises(TypeError):
        replace(ledger.fills[0], select_lot=lambda: None)


@pytest.mark.parametrize("wire", (False, True))
def test_aggregate_allocation_lengths_checked_before_rows(wire, monkeypatch):
    ledger = _records()[-1]
    receipt = ledger.receipts[0]
    if wire:
        value = ledger.to_dict()
        value["receipts"] = [{"allocations": [None] * 1024}] * 3

        def operation():
            return AccountLedgerV1.from_dict(value)

    else:
        object.__setattr__(receipt, "allocations", (None,) * 1024)
        object.__setattr__(ledger, "receipts", (receipt,) * 3)
        operation = ledger.to_json
    monkeypatch.setattr(
        codec,
        "tree_bound",
        lambda *a: pytest.fail("rows expanded before length admission"),
    )
    with pytest.raises(ValueError, match="aggregate allocation"):
        operation()
