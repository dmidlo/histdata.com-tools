"""Tests for shared data-quality bounded limit normalization."""

from __future__ import annotations

from histdatacom.data_quality import bounded_report_limit


def test_bounded_report_limit_uses_default_when_none() -> None:
    """Omitted limits should retain default and effective semantics."""
    limit = bounded_report_limit(None, default_limit=5)

    assert limit.requested_limit is None
    assert limit.default_limit == 5
    assert limit.effective_limit == 5
    assert limit.unbounded is False
    assert limit.slice([1, 2, 3]) == [1, 2, 3]
    assert limit.count_payload(7) == {
        "limit": 5,
        "effective_limit": 5,
        "requested_limit": None,
        "default_limit": 5,
        "minimum_limit": 0,
        "maximum_limit": None,
        "unbounded": False,
        "total_count": 7,
        "included_count": 5,
        "omitted_count": 2,
        "truncated": True,
    }


def test_bounded_report_limit_clamps_zero_when_minimum_is_one() -> None:
    """Count-bucket limits can require a non-empty minimum."""
    limit = bounded_report_limit(
        0,
        default_limit=8,
        minimum_limit=1,
        allow_unbounded=False,
    )

    assert limit.requested_limit == 0
    assert limit.effective_limit == 1
    assert limit.minimum_limit == 1
    assert limit.slice(["a", "b"]) == ["a"]


def test_bounded_report_limit_treats_negative_as_unbounded() -> None:
    """Negative report limits preserve the existing unbounded convention."""
    limit = bounded_report_limit(-1, default_limit=8)

    assert limit.requested_limit == -1
    assert limit.effective_limit == -1
    assert limit.unbounded is True
    assert limit.slice([1, 2, 3]) == [1, 2, 3]
    assert limit.count_payload(3)["omitted_count"] == 0


def test_bounded_report_limit_allows_one_and_large_limits() -> None:
    """Explicit positive limits should round-trip as effective limits."""
    one = bounded_report_limit(1, default_limit=8)
    large = bounded_report_limit(10_000, default_limit=8)

    assert one.slice([1, 2, 3]) == [1]
    assert one.count_payload(3)["truncated"] is True
    assert large.slice([1, 2, 3]) == [1, 2, 3]
    assert large.count_payload(3)["included_count"] == 3


def test_bounded_report_limit_clamps_to_maximum() -> None:
    """Optional maximums should cap the effective limit without hiding requests."""
    limit = bounded_report_limit(25, default_limit=8, maximum_limit=10)

    assert limit.requested_limit == 25
    assert limit.maximum_limit == 10
    assert limit.effective_limit == 10
    assert limit.count_payload(12)["omitted_count"] == 2
