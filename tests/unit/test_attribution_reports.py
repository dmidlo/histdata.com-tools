"""Exact report math from retained synthetic attribution outputs."""

from dataclasses import replace
from fractions import Fraction

import pytest

from histdatacom.attribution.reference import explain_reference
from histdatacom.attribution.reports import (
    AttributionStratumV1,
    StratifiedAttributionV1,
    build_attribution_report,
    render_attribution_report,
)
from tests.fixtures.decision_attribution import reference_inputs


def sample(
    *, x=1.0, domain="synthetic_fixture", era="fixture-era", unit="unit-1"
):
    model, policy, snapshot, background = reference_inputs()
    snapshot = replace(
        snapshot,
        domain=domain,
        values=(replace(snapshot.values[0], value=x), snapshot.values[1]),
    )
    background = replace(
        background, snapshots=(replace(background.snapshots[0], domain=domain),)
    )
    attribution = explain_reference(
        model, policy, snapshot, background, generated_at_ns=20
    )
    stratum = AttributionStratumV1(
        "EURUSD",
        "fixture-regime",
        era,
        domain,
        "supported",
        snapshot.session,
        "fixture-broker-state",
    )
    return StratifiedAttributionV1(attribution, stratum, unit)


def test_group_hhi_exact_math_and_signed_distributions():
    report = build_attribution_report((sample(),))
    summary = report.summaries[0]
    assert [g.mean_absolute for g in summary.groups] == [5.0, 4.0]
    assert summary.hhi == float(Fraction(41, 81))
    assert summary.effective_contributing_groups == float(Fraction(81, 41))
    assert summary.groups[0].signed_distribution == (5.0,)
    assert type(report).from_json(report.to_json()) == report
    rendered = render_attribution_report(report)
    assert "model attribution != causal effect" in rendered
    assert "not historical or live qualification" in rendered


def test_every_stratum_and_domain_difference_is_retained_not_pooled():
    report = build_attribution_report(
        (
            sample(domain="observed_fixture"),
            sample(
                x=2.0,
                domain="synthetic_fixture",
                era="other-era",
                unit="unit-2",
            ),
        )
    )
    assert len(report.summaries) == 2 and len(report.differences) == 2
    assert {s.stratum.domain for s in report.summaries} == {
        "observed_fixture",
        "synthetic_fixture",
    }
    assert report.differences[0].mean_absolute_difference != 0


def test_summary_or_nonclaim_tampering_refuses_even_with_new_outer_id():
    report = build_attribution_report((sample(),))
    with pytest.raises(ValueError, match="replay"):
        replace(report, summaries=(replace(report.summaries[0], hhi=1.0),))
    with pytest.raises(ValueError, match="causal"):
        replace(report, causal_nonclaim="causal market effect")
    with pytest.raises(ValueError, match="duplicate"):
        build_attribution_report((sample(), sample()))


def test_zero_total_attribution_has_unavailable_concentration_not_infinite():
    entry = sample(x=0.0)
    # Explain the all-zero background point so both group contributions vanish.
    model, policy, snapshot, background = reference_inputs()
    point = replace(
        snapshot, values=tuple(replace(v, value=0.0) for v in snapshot.values)
    )
    attribution = explain_reference(
        model, policy, point, background, generated_at_ns=20
    )
    report = build_attribution_report(
        (replace(entry, attribution=attribution),)
    )
    assert report.summaries[0].hhi is None
    assert report.summaries[0].effective_contributing_groups is None
