"""Public exports and the invented documentation example stay executable."""

from pathlib import Path

import histdatacom.synthetic.traders as traders
from histdatacom.synthetic.traders import account_contracts, account_ledger


def test_public_account_exports_are_canonical_owners():
    names = (
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
    )
    for name in names:
        assert name in traders.__all__
        assert getattr(traders, name) is getattr(account_contracts, name)
    for name in account_ledger.__all__:
        assert name in traders.__all__
        assert getattr(traders, name) is getattr(account_ledger, name)
    assert len(traders.__all__) == len(set(traders.__all__))


def test_documentation_invented_account_example():
    root = Path(__file__).resolve().parents[2]
    document = (root / "docs/trader-account-policy.md").read_text()
    example = document.split("```python\n", 1)[1].split("```", 1)[0]
    namespace = {}
    exec(compile(example, "docs/trader-account-policy.md", "exec"), namespace)
    ledger = namespace["ledger"]
    assert ledger.final_state.open_lots[0].remaining_signed_quantity == (
        traders.ExactAmountV1(1)
    )
