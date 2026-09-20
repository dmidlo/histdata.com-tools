"""Independent deterministic triangle fixtures, not empirical qualification."""

from dataclasses import replace

from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAvailabilityBasis,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    CausalBarSnapshotV1,
    _cells as bar_cells,
)
from histdatacom.synthetic.bars import (
    STANDARD_DERIVED_BAR_INTERVALS,
    DerivedBarPolicyV1,
    derive_reconstruction_bars,
    publish_derived_bars,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyValidationStage,
    eurusd_triangle_reconciliation_config,
    reconcile_cross_currency_window,
    validate_cross_currency_output,
)
from histdatacom.synthetic.delivery import project_modern_reference_delivery
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    commit_delivery_reconstruction_publication,
    estimate_reconstruction_retention,
    stage_delivery_reconstruction_publication,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)
from histdatacom.synthetic.triangle_bar_features import (
    TRIANGLE_SYMBOLS,
    TriangleBarPolicyV1,
    TriangleBarSnapshotV1,
    TriangleBarSourceV1,
    TriangleBarStratumV1,
    _cells,
    event_order,
    scope_contains,
)
from histdatacom.synthetic.triangle_projection_features import (
    TriangleProjectionEvidenceV1,
)

BASE = 20_000 * STANDARD_DERIVED_BAR_INTERVALS["1d"]
PRODUCT_ID = "reconstruction-manifest:sha256:" + "a" * 64
BAR_MANIFEST_ID = "derived-bar-manifest:sha256:" + "b" * 64
MEMBER = "triangle-feature-fixture-member"
SOURCE = "source:triangle-feature-fixture"
POST = InformationMode.EX_POST_RECONSTRUCTION


def quote(run, symbol, time, sequence, bid, ask, *, generated=False):
    event = SyntheticEventV1.observed(
        symbol=symbol,
        event_time_ns=time,
        event_sequence=sequence,
        bid=bid,
        ask=ask,
        run_id=run.run_id,
        ensemble_member_id=MEMBER,
        source_version_id=SOURCE,
        source_series_id="fixture:" + symbol,
        source_period="202410",
        source_row_id=sequence + 1,
    )
    if generated:
        event = SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=time,
            event_sequence=sequence,
            bid=bid,
            ask=ask,
            run_id=run.run_id,
            ensemble_member_id=MEMBER,
            source_version_id=SOURCE,
            left_anchor_event_id="event:left",
            right_anchor_event_id="event:right",
            generator_id="fixture",
            generator_version="1.0.0",
            generator_config_id="fixture:config",
            constraint_set_id="fixture:constraints",
        )
    return (
        replace(event, feed_epoch_id="fixture:epoch:" + symbol, event_id="")
        if generated
        else event
    )


def run_for(config=None):
    config = config or eurusd_triangle_reconciliation_config()
    return ReconstructionRunV1(
        symbols=TRIANGLE_SYMBOLS,
        source_version_ids=(SOURCE,),
        configuration_ids=(config.config_id,),
        ensemble_member_ids=(MEMBER,),
        base_seed=651,
    )


def math_events(interval="1m"):
    width = STANDARD_DERIVED_BAR_INTERVALS[interval]
    run = run_for()
    events = []
    for leg, symbol in enumerate(TRIANGLE_SYMBOLS):
        base = (2.0, 10.0, 5.0)[leg]
        for bar in range(4):
            # Every scope gets both endpoint geometry and its own exact IDs.
            for point, offset in enumerate((0.0, 0.5, -0.25, 0.25)):
                mid = base + bar * (leg + 1) * 0.1 + offset
                time = BASE + bar * width + (point + 1) * width // 5
                for generated in (False, True):
                    events.append(
                        quote(
                            run,
                            symbol,
                            time + int(generated),
                            10 * bar + 2 * point + int(generated),
                            mid - 0.01,
                            mid + 0.01,
                            generated=generated,
                        )
                    )
    return tuple(sorted(events, key=event_order))


def math_snapshot(
    interval="1m",
    scope=ActivitySliceScope.OBSERVED,
    *,
    events=None,
    policy=None,
    missing=(),
    cutoff=None,
    strata=True,
    partial=False,
):
    width = STANDARD_DERIVED_BAR_INTERVALS[interval]
    events = math_events(interval) if events is None else events
    cutoff = BASE + 4 * width if cutoff is None else cutoff
    bar_policy = (
        policy.bar_policy
        if policy
        else BarFeaturePolicyV1(
            POST,
            intervals=(interval,),
            scopes=(scope,),
            allow_prior_generated_state=True,
        )
    )
    policy = policy or TriangleBarPolicyV1(
        bar_policy, BASE, max_endpoint_age_ns=width
    )
    legs, all_bars, declarations = [], [], []
    for symbol in TRIANGLE_SYMBOLS:
        selected = tuple(e for e in events if e.symbol.upper() == symbol)
        rows = derive_reconstruction_bars(
            selected,
            source_product_manifest_id=PRODUCT_ID,
            run_id=selected[0].run_id,
            ensemble_member_id=MEMBER,
            policy=DerivedBarPolicyV1(intervals=(interval,), scopes=(scope,)),
            start_ns=BASE,
            end_ns=BASE + 4 * width - int(partial),
        )
        bars = tuple(
            b
            for b in rows
            if (symbol, b.bar_start_ns) not in missing
            and b.bar_end_ns <= cutoff
            and b.bar_start_ns >= cutoff // width * width - 4 * width
        )
        availability = tuple(
            sorted(
                (
                    BarAvailabilityDeclarationV1(
                        b.bar_id,
                        b.bar_end_ns,
                        b.bar_end_ns,
                        BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
                        "fixture assertion",
                        generated_at_ns=(
                            b.bar_end_ns
                            if scope is not ActivitySliceScope.OBSERVED
                            else None
                        ),
                    )
                    for b in bars
                ),
                key=lambda a: a.bar_id,
            )
        )
        legs.append(
            CausalBarSnapshotV1(
                symbol,
                cutoff,
                bar_policy,
                bars,
                availability,
                (),
                bar_cells(symbol, cutoff, bar_policy, bars, availability, ()),
                PRODUCT_ID,
                BAR_MANIFEST_ID,
            )
        )
        all_bars.extend(bars)
        declarations.extend(
            TriangleBarStratumV1(
                b.bar_id,
                "fixture-session",
                BASE,
                "fixture session assertion",
                "fixture:epoch:" + symbol,
            )
            for b in bars
        )
    retained = tuple(
        e
        for e in events
        if any(
            e.symbol == b.symbol
            and b.bar_start_ns <= e.event_time_ns < b.bar_end_ns
            and scope_contains(scope, e)
            for b in all_bars
        )
    )
    labels = (
        tuple(sorted(declarations, key=lambda s: s.bar_id)) if strata else ()
    )
    return TriangleBarSnapshotV1(
        policy,
        tuple(legs),
        tuple(e.to_json() for e in retained),
        labels,
        _cells(tuple(legs), policy, retained, labels),
    )


def published_triangle(tmp_path, interval="1m"):
    width = STANDARD_DERIVED_BAR_INTERVALS[interval]
    config = replace(
        eurusd_triangle_reconciliation_config(),
        max_projection_relative=0.5,
        config_id="",
    )
    run = run_for(config)
    window = ReconstructionWindowV1(
        run.run_id, MEMBER, run.symbols, BASE, BASE + 4 * width
    )
    rows = {s: [] for s in TRIANGLE_SYMBOLS}
    for bar in range(4):
        for point in range(4):
            generated = point in (1, 2)
            time = (
                BASE
                + bar * width
                + (
                    1_000_000
                    if point == 0
                    else width - 1_000_000 if point == 3 else point * width // 3
                )
            )
            quotes = {"EURUSD": (1.1999, 1.2001), "GBPUSD": (1.4999, 1.5001)}
            quotes["EURGBP"] = (
                quotes["EURUSD"][0] / quotes["GBPUSD"][1],
                quotes["EURUSD"][1] / quotes["GBPUSD"][0],
            )
            if point == 1:
                quotes["EURGBP"] = tuple(v * 1.001 for v in quotes["EURGBP"])
            for symbol, (bid, ask) in quotes.items():
                rows[symbol].append(
                    quote(
                        run,
                        symbol,
                        time,
                        bar * 4 + point,
                        bid,
                        ask,
                        generated=generated,
                    )
                )
    proposals = tuple(
        SyntheticEventStreamV1(run.run_id, MEMBER, symbol, tuple(rows[symbol]))
        for symbol in TRIANGLE_SYMBOLS
    )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={s.symbol: s for s in proposals},
        config=config,
    )
    delivered = project_modern_reference_delivery(
        group, delivery_profile_id="modern-reference:triangle-feature-fixture"
    )
    anchors = tuple(
        e
        for s in delivered.streams
        for e in s.events
        if e.origin.value == "observed"
    )
    validation = validate_cross_currency_output(
        run=run,
        window=window,
        streams={s.symbol: s for s in delivered.streams},
        config=config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=anchors,
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=MEMBER,
        retained_member_event_counts={
            MEMBER: sum(len(s.events) for s in proposals)
        },
        estimated_partition_count=12,
        storage_policy=run.storage_policy,
    )
    staged = stage_delivery_reconstruction_publication(
        tmp_path / "archive",
        delivered,
        final_validation=validation,
        benchmark_artifact_ids=("fixture:benchmark",),
        benchmark_evidence={"gate": "passed"},
        point_in_time_evidence_projection_ids=("fixture:projection",),
        point_in_time_evidence_decision_ids=("fixture:decision",),
        cross_series_constraint_bundle_ids=("fixture:bundle",),
        cross_series_constraint_window_ids=("fixture:window",),
        cross_series_constraint_decision_ids=("fixture:constraint-decision",),
        immutable_source_anchors=anchors,
        symbol_group_id=window.synchronization_unit_id,
        retention_plan=retention,
        storage_policy=run.storage_policy,
        staging_root=tmp_path / "scratch",
    )
    product = commit_delivery_reconstruction_publication(staged)
    bars = publish_derived_bars(
        tmp_path / "bars",
        product.manifest_path,
        policy=DerivedBarPolicyV1(
            intervals=(interval,), scopes=tuple(ActivitySliceScope)
        ),
        start_ns=BASE,
        end_ns=BASE + 4 * width,
    )
    source = TriangleBarSourceV1(
        BarFeatureSourceV1(str(product.manifest_path), str(bars.manifest_path))
    )
    policy = TriangleBarPolicyV1(
        BarFeaturePolicyV1(
            POST, intervals=(interval,), allow_prior_generated_state=True
        ),
        BASE,
        max_endpoint_age_ns=1_000_000,
    )
    all_bars = tuple(
        b
        for symbol in TRIANGLE_SYMBOLS
        for b in source.bar_source.verified_bars(symbol, policy.bar_policy)
    )
    availability = tuple(
        BarAvailabilityDeclarationV1(
            b.bar_id,
            b.bar_end_ns,
            b.bar_end_ns,
            BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
            "fixture assertion",
            generated_at_ns=(
                b.bar_end_ns
                if b.scope is not ActivitySliceScope.OBSERVED
                else None
            ),
        )
        for b in all_bars
    )
    strata = tuple(
        TriangleBarStratumV1(
            b.bar_id,
            "fixture-session",
            BASE,
            "fixture session assertion",
            "fixture:epoch:" + b.symbol.upper(),
        )
        for b in all_bars
    )
    snapshot = source.snapshot(
        decision_time_ns=BASE + 4 * width,
        policy=policy,
        availability=availability,
        strata=strata,
    )
    evidence = TriangleProjectionEvidenceV1.retain(
        run=run,
        window=window,
        config=config,
        proposals=proposals,
        delivery_profile_id=delivered.manifest.delivery_profile_id,
        known_at_ns=BASE + 4 * width,
        generated_at_ns=BASE + 4 * width,
    )
    return source, snapshot, evidence, delivered
