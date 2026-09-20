"""Synthetic, source-bound reconciliation at all five public trader seams."""

from __future__ import annotations

import ast
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetContractError,
    DatasetDescriptorV1,
    DatasetOrigin,
    DatasetParentV1,
    DatasetQueryScopeV1,
    DatasetVersionManifestV1,
    FixtureProviderAdapter,
)
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarAsKnownReaderV1 as CanonicalCalendarReader,
    EconomicCalendarQueryV1,
)
from histdatacom.market_context.positioning import (
    CftcPositioningQueryStatus,
    CftcReportFamily,
    CftcReportScope,
    CftcRestatementStatus,
    query_cftc_positioning_corpus,
)
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAvailabilityBasis,
    BarFeatureState,
)
from histdatacom.synthetic.cross_currency import CrossCurrencyJoinPolicy
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_PRODUCT_DIRECTORY,
    ReconstructionPersistenceError,
    load_reconstruction_manifest,
    read_reconstruction_streams,
)
from histdatacom.synthetic.traders import (
    CatalogTraderDatasetReaderV1,
    CftcTraderPositioningReaderV1,
    CommittedTraderTriangleReaderV1,
    EconomicCalendarAsKnownReaderV1,
    TraderBarActivityReaderV1,
    TraderDatasetReaderV1,
    TraderPositioningReaderV1,
    TraderQuoteAvailabilityV1,
    TraderTriangleReaderV1,
    TraderTriangleRequestV1,
    TraderTriangleState,
)
from histdatacom.synthetic.traders import triangle as triangle_module
from tests.fixtures.triangle_bar_features_v1 import (
    BASE,
    MEMBER,
    published_triangle,
)
from tests.fixtures import triangle_bar_features_v1 as triangle_fixture
from tests.unit.test_cftc_positioning import (
    _window_ns,
    corpus_build as _cftc_fixture,
)
from tests.unit.test_datasets import _catalog, _write_fixture_csv
from tests.unit.test_economic_calendar import (
    SECOND_NS,
    TARGET_TIME,
    _corpus,
)

ANTE = InformationMode.EX_ANTE_SIMULATION
POST = InformationMode.EX_POST_RECONSTRUCTION
OBS = ActivitySliceScope.OBSERVED
EXACT = CrossCurrencyJoinPolicy.EXACT_EVENT_TIME_NO_FORWARD_FILL
PRIOR = CrossCurrencyJoinPolicy.NEAREST_PRIOR_BOUNDED_NO_FORWARD_FILL
MINUTE = 60_000_000_000


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    return published_triangle(tmp_path_factory.mktemp("trader-interfaces"))


def _request(**changes):
    return replace(
        TraderTriangleRequestV1(
            BASE,
            BASE + 4 * MINUTE,
            BASE + 4 * MINUTE,
            BASE,
            MEMBER,
            POST,
        ),
        **changes,
    )


def _triangle_inputs(published, request=None):
    source, _, _, _ = published
    reader = CommittedTraderTriangleReaderV1(
        source.bar_source.reconstruction_manifest_path
    )
    request = request or _request()
    streams = read_reconstruction_streams(reader.manifest_path)
    clocks = tuple(
        TraderQuoteAvailabilityV1(
            e.event_id,
            request.decision_at_ns,
            request.decision_at_ns,
            "Synthetic fixture availability assertion, not historical proof",
            generated_at_ns=(
                request.decision_at_ns if e.origin.value != "observed" else None
            ),
        )
        for s in streams
        for e in s.events
    )
    return reader, request, clocks, streams


def test_public_protocols_have_canonical_owners(published, tmp_path):
    assert EconomicCalendarAsKnownReaderV1 is CanonicalCalendarReader
    assert isinstance(_corpus(), EconomicCalendarAsKnownReaderV1)
    assert isinstance(published[0].bar_source, TraderBarActivityReaderV1)
    assert isinstance(_triangle_inputs(published)[0], TraderTriangleReaderV1)
    assert isinstance(
        CftcTraderPositioningReaderV1(_cftc_fixture.__wrapped__().corpus),
        TraderPositioningReaderV1,
    )
    assert isinstance(
        CatalogTraderDatasetReaderV1(DatasetCatalog((), (), (), ())),
        TraderDatasetReaderV1,
    )


def test_calendar_protocol_preserves_actual_previous_and_forecast_provenance():
    reader: EconomicCalendarAsKnownReaderV1 = _corpus()

    def query(cutoff):
        return reader.as_known_at(
            start_ns=TARGET_TIME,
            end_ns=TARGET_TIME + SECOND_NS,
            decision_at_ns=cutoff,
            symbols=("EURUSD",),
        )

    before = query(TARGET_TIME - SECOND_NS)
    now = query(TARGET_TIME)
    later = query(TARGET_TIME + 3 * SECOND_NS)
    assert before.events[0].actual_initial is None
    assert now.events[0].actual_initial == now.events[0].actual_latest == 2.0
    assert later.events[0].actual_initial == 2.0
    assert later.events[0].actual_latest == 2.1
    assert now.events[0].previous_value == 1.1
    assert now.events[0].observed_consensus.value == 1.9
    assert now.events[0].machine_projection.value == 1.8
    assert query(TARGET_TIME).to_json() == now.to_json()
    assert EconomicCalendarQueryV1.from_json(now.to_json()) == now
    with pytest.raises(ValueError, match="coverage"):
        reader.as_known_at(start_ns=0, end_ns=1, decision_at_ns=0)


def test_positioning_preserves_strict_native_refusals_and_positive_vintage():
    corpus = _cftc_fixture.__wrapped__().corpus
    reader: TraderPositioningReaderV1 = CftcTraderPositioningReaderV1(corpus)
    args = dict(
        start_ns=_window_ns("2025-11-24"),
        end_ns=_window_ns("2025-11-25"),
        decision_at_ns=_window_ns("2025-11-22"),
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
        max_staleness_days=60,
    )
    refusal = reader.as_known_at(**args)
    assert refusal.status is CftcPositioningQueryStatus.RESTATEMENT_INCOMPLETE
    early = reader.as_known_at(
        **{**args, "decision_at_ns": _window_ns("2025-11-18")}
    )
    assert early.status is CftcPositioningQueryStatus.NOT_AVAILABLE
    snapshots = tuple(
        (
            replace(
                s,
                restatement_status=CftcRestatementStatus.ORIGINAL_VERIFIED,
                snapshot_id="",
            )
            if s.report_date == "2025-10-07"
            else s
        )
        for s in corpus.snapshots
    )
    original = replace(corpus, snapshots=snapshots, corpus_id="")
    positive = CftcTraderPositioningReaderV1(original).as_known_at(**args)
    assert positive.status is CftcPositioningQueryStatus.READY
    native_args = {k: v for k, v in args.items() if k != "decision_at_ns"}
    native = query_cftc_positioning_corpus(
        original,
        **native_args,
        as_of_ns=args["decision_at_ns"],
        information_mode=ANTE,
    )
    assert positive.to_dict() == native.to_dict()
    with pytest.raises(ValueError, match="decision"):
        reader.as_known_at(**{**args, "decision_at_ns": args["end_ns"]})
    with pytest.raises(ValueError, match="int64"):
        reader.as_known_at(**{**args, "decision_at_ns": True})


def test_dataset_registration_resolution_replay_lineage_and_byte_refusal(
    tmp_path,
):
    root = tmp_path / "source"
    source = _write_fixture_csv(root)
    catalog, version = _catalog(FixtureProviderAdapter(), root, tmp_path)
    reader: TraderDatasetReaderV1 = CatalogTraderDatasetReaderV1(
        DatasetCatalog((), (), (), ())
    ).register(catalog)
    scope = DatasetQueryScopeV1(symbols=("EURUSD",), periods=("202001",))
    receipt = reader.resolve("latest-qualified", query_scope=scope)
    assert reader.replay(receipt) == receipt
    assert reader.lineage(receipt) == (version,)
    assert reader.verify(receipt) == catalog.verify(receipt)
    assert (
        reader.register(catalog).resolve("latest-qualified", query_scope=scope)
        == receipt
    )
    changed = replace(catalog.aliases[0], revision=2, alias_id="")
    moved = replace(catalog, aliases=(changed,), catalog_id="")
    assert CatalogTraderDatasetReaderV1(moved).replay(receipt) == receipt
    with pytest.raises(ValueError, match="conflicts"):
        reader.register(moved)
    source.write_bytes(source.read_bytes() + b"\n")
    with pytest.raises(DatasetContractError):
        reader.verify(receipt)


def test_bar_activity_is_the_native_verified_snapshot_not_a_second_projection(
    published,
):
    source, native, _, _ = published
    reader: TraderBarActivityReaderV1 = source.bar_source
    leg = native.leg_snapshots[1]
    query = dict(
        symbol=leg.symbol,
        decision_time_ns=leg.decision_time_ns,
        policy=leg.policy,
        availability=leg.availability,
        absences=leg.absences,
    )
    snapshot = reader.snapshot(**query)
    assert snapshot.to_json() == leg.to_json()
    cell = snapshot.cell(OBS, "1m")
    assert cell.state is BarFeatureState.AVAILABLE
    assert cell.feature("event_count").value == 2
    assert cell.feature("tick_intensity_per_second").value is not None
    assert snapshot.historical_availability_verified is False
    missing = reader.snapshot(**{**query, "availability": ()})
    assert missing.cell(OBS, "1m").state is not BarFeatureState.AVAILABLE
    assert (
        missing.source_product_manifest_id
        == snapshot.source_product_manifest_id
    )


def test_dataset_diamond_lineage_preserves_exact_parents_once(tmp_path):
    root = tmp_path / "source"
    _write_fixture_csv(root)
    catalog, observed = _catalog(FixtureProviderAdapter(), root, tmp_path)

    def derived(name, parents):
        return DatasetVersionManifestV1(
            dataset_id=name,
            origin=DatasetOrigin.DERIVED,
            normalization_policy_id="fixture-identity-only-v1",
            qualification_status=observed.qualification_status,
            parents=tuple(
                DatasetParentV1(v.dataset_version_id, "input", i)
                for i, v in enumerate(parents)
            ),
            qualification_evidence=observed.qualification_evidence,
        )

    left = derived("left", (observed,))
    right = derived("right", (observed,))
    top = derived("top", (left, right))
    versions = (observed, left, right, top)
    catalog = replace(
        catalog,
        datasets=catalog.datasets
        + tuple(
            DatasetDescriptorV1(
                v.dataset_id, v.dataset_id, "fixture", (v.origin,)
            )
            for v in (left, right, top)
        ),
        versions=versions,
        catalog_id="",
    )
    reader = CatalogTraderDatasetReaderV1(catalog)
    receipt = reader.resolve(
        top.dataset_version_id, query_scope=DatasetQueryScopeV1()
    )
    assert reader.lineage(receipt) == tuple(
        sorted(versions, key=lambda v: v.dataset_version_id)
    )
    # Content-bound parents prevent constructing a valid cycle by resealing
    # one node: its descendants still reference the old, now absent ID.
    with pytest.raises(DatasetContractError, match="unknown parent"):
        replace(catalog, versions=(left, right, top), catalog_id="")
    with pytest.raises(
        DatasetContractError, match="identity|immutable|differs"
    ):
        replace(
            left, parents=(DatasetParentV1(top.dataset_version_id, "input", 0),)
        )


@pytest.mark.parametrize("join,age", [(EXACT, 0), (PRIOR, MINUTE)])
@pytest.mark.parametrize("scope", tuple(ActivitySliceScope))
def test_committed_triangle_positive_native_identity_and_replay(
    published, join, age, scope
):
    request = _request(join_policy=join, max_quote_age_ns=age, scope=scope)
    reader, request, clocks, streams = _triangle_inputs(published, request)
    result = reader.query(request, availability=clocks)
    assert result.state is TraderTriangleState.READY
    expected = 8 if scope is not ActivitySliceScope.MERGED else 16
    assert len(result.tuples) == expected
    assert (
        result.window_event_count == result.admitted_event_count == 3 * expected
    )
    native = {e.event_id: e.to_json() for s in streams for e in s.events}
    assert all(
        native[e.event_id] == e.to_json()
        for t in result.tuples
        for e in t.events
    )
    assert all(
        e.event_time_ns < request.end_ns
        for t in result.tuples
        for e in t.events
    )
    assert reader.replay(result) == result
    assert not result.historical_availability_verified
    with pytest.raises(ValueError, match="differs"):
        reader.replay(replace(result, source_product_manifest_id="forged"))


def test_arbitrary_half_open_event_window_is_not_an_ohlc_bar(published):
    request = _request(
        start_ns=BASE + 1_000_000, end_ns=BASE + MINUTE - 1_000_000
    )
    reader, request, clocks, _ = _triangle_inputs(published, request)
    result = reader.query(request, availability=clocks)
    assert result.state is TraderTriangleState.READY
    assert len(result.tuples) == 1
    assert result.tuples[0].probe_time_ns == request.start_ns
    assert result.tuples[0].endpoint_ns == request.end_ns


def test_availability_cutoff_is_not_event_time_or_source_digest(published):
    reader, request, clocks, _ = _triangle_inputs(published)
    unknown = reader.query(request)
    assert unknown.state is TraderTriangleState.UNAVAILABLE
    assert unknown.admitted_event_count == 0 and not unknown.tuples
    future = tuple(
        replace(c, known_at_ns=request.decision_at_ns + 1) for c in clocks
    )
    assert reader.query(request, availability=future) == unknown
    assert reader.replay(unknown) == unknown
    admitted = reader.query(request, availability=clocks)
    assert admitted.state is TraderTriangleState.READY
    assert (
        admitted.source_product_manifest_id
        == unknown.source_product_manifest_id
    )
    with pytest.raises(ValueError, match="duplicate"):
        reader.query(request, availability=(clocks[0], clocks[0]))
    with pytest.raises(ValueError, match="precedes"):
        reader.query(
            request, availability=(replace(clocks[0], available_at_ns=0),)
        )


def test_bounded_prior_uses_prior_known_event_with_inclusive_age_not_future(
    published,
):
    request = _request(
        scope=ActivitySliceScope.MERGED,
        join_policy=PRIOR,
        max_quote_age_ns=20_000_000_000,
    )
    reader, request, clocks, streams = _triangle_inputs(published, request)
    # Remove EURGBP's first generated event; EURUSD's fixed probe must use
    # the earlier observed EURGBP event, never the later generated quote.
    x = next(s for s in streams if s.symbol.upper() == "EURGBP")
    removed = x.events[1]
    clocks = tuple(c for c in clocks if c.event_id != removed.event_id)
    result = reader.query(request, availability=clocks)
    assert result.state is TraderTriangleState.UNAVAILABLE
    matched = next(
        t for t in result.tuples if t.probe_time_ns == removed.event_time_ns
    )
    assert matched.events[0].event_id == x.events[0].event_id
    age = removed.event_time_ns - x.events[0].event_time_ns
    assert matched.ages_ns[0] == age
    equal = reader.query(
        replace(request, max_quote_age_ns=age), availability=clocks
    )
    stale = reader.query(
        replace(request, max_quote_age_ns=age - 1), availability=clocks
    )
    assert any(t.probe_time_ns == removed.event_time_ns for t in equal.tuples)
    assert not any(
        t.probe_time_ns == removed.event_time_ns for t in stale.tuples
    )
    no_carry = reader.query(
        replace(request, start_ns=removed.event_time_ns), availability=clocks
    )
    assert not any(
        t.probe_time_ns == removed.event_time_ns for t in no_carry.tuples
    )


def test_source_budget_and_member_fail_before_deep_read(published, monkeypatch):
    reader, request, _, _ = _triangle_inputs(published)

    def forbidden(*args, **kwargs):
        pytest.fail("deep source replay was called")

    monkeypatch.setattr(
        triangle_module, "read_reconstruction_streams", forbidden
    )
    with pytest.raises(ValueError, match="total source"):
        reader.query(replace(request, max_source_events=1))
    with pytest.raises(ValueError, match="member"):
        reader.query(replace(request, ensemble_member_id="another"))


@pytest.mark.parametrize(
    "scope", [ActivitySliceScope.SYNTHETIC, ActivitySliceScope.MERGED]
)
def test_ex_ante_generated_state_refuses_before_any_io(scope, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("source metadata was read")

    monkeypatch.setattr(
        triangle_module, "load_reconstruction_manifest", forbidden
    )
    with pytest.raises(ValueError, match="ex-ante"):
        _request(information_mode=ANTE, scope=scope)


def test_observed_ex_ante_keeps_assertion_limitation_and_rejects_replay_clock(
    published,
):
    reader, request, clocks, _ = _triangle_inputs(
        published, _request(information_mode=ANTE)
    )
    result = reader.query(request, availability=clocks)
    assert result.state is TraderTriangleState.READY
    assert not result.historical_availability_verified
    changed = tuple(
        replace(c, basis=BarAvailabilityBasis.REPLAY_CLOCK_ASSUMPTION)
        for c in clocks
    )
    with pytest.raises(ValueError, match="replay clock"):
        reader.query(request, availability=changed)


def test_generated_quotes_require_known_generation_and_empty_is_explicit(
    published,
):
    reader, request, clocks, _ = _triangle_inputs(
        published, _request(scope=ActivitySliceScope.SYNTHETIC)
    )
    with pytest.raises(ValueError, match="generation cutoff"):
        reader.query(
            request,
            availability=tuple(
                replace(c, generated_at_ns=None) for c in clocks
            ),
        )
    empty = reader.query(replace(request, start_ns=BASE, end_ns=BASE + 1))
    assert empty.state is TraderTriangleState.EMPTY_WINDOW
    assert empty.window_event_count == empty.admitted_event_count == 0
    assert reader.replay(empty) == empty


def test_exact_no_support_is_not_zero_opportunity(tmp_path, monkeypatch):
    native_quote = triangle_fixture.quote

    def shifted_quote(run, symbol, time, *args, **kwargs):
        # Genuine committed fixture: the first observed leg arrives one ns
        # later, while all later tuples still pass canonical publication.
        if symbol == "EURGBP" and time == BASE + 1_000_000:
            time += 1
        return native_quote(run, symbol, time, *args, **kwargs)

    monkeypatch.setattr(triangle_fixture, "quote", shifted_quote)
    published = published_triangle(tmp_path)
    request = _request(start_ns=BASE + 1_000_000, end_ns=BASE + 1_000_002)
    reader, request, clocks, _ = _triangle_inputs(published, request)
    result = reader.query(request, availability=clocks)
    assert result.state is TraderTriangleState.NO_SUPPORT
    assert result.window_event_count == result.admitted_event_count == 3
    assert result.tuples == ()
    assert reader.replay(result) == result


def test_replay_refuses_native_tuple_resealing_and_changed_source_bytes(
    published, tmp_path
):
    reader, request, clocks, _ = _triangle_inputs(published)
    original = reader.query(request, availability=clocks)
    first = original.tuples[0]
    mutated_event = replace(
        first.events[0], bid=first.events[0].bid - 0.01, event_id=""
    )
    mutated_tuple = replace(
        first,
        event_json=(mutated_event.to_json(), *first.event_json[1:]),
    )
    assert mutated_tuple.artifact_id != first.artifact_id
    with pytest.raises(ValueError, match="differs"):
        reader.replay(
            replace(original, tuples=(mutated_tuple, *original.tuples[1:]))
        )
    # Copy only this synthetic publication to keep the shared fixture immutable.
    old_path = Path(reader.manifest_path)
    old_root = next(
        p
        for p in old_path.parents
        if p.name == RECONSTRUCTION_PRODUCT_DIRECTORY
    )
    copied = tmp_path / old_root.name / old_path.parent.relative_to(old_root)
    shutil.copytree(old_path.parent, copied)
    fresh = CommittedTraderTriangleReaderV1(str(copied / old_path.name))
    assert fresh.replay(original) == original
    manifest = load_reconstruction_manifest(fresh.manifest_path)
    partition = copied / manifest.partitions[0].relative_path
    partition.write_bytes(partition.read_bytes()[:32])
    with pytest.raises(ReconstructionPersistenceError):
        fresh.replay(original)


@pytest.mark.parametrize(
    "changes",
    [
        {"decision_at_ns": True},
        {"start_ns": -1},
        {"end_ns": BASE + 4 * MINUTE + 1},
        {"policy_known_at_ns": BASE + 4 * MINUTE + 1},
        {"join_policy": PRIOR},
        {"max_quote_age_ns": 1},
        {"max_retained_events": 4097},
        {"max_source_events": True},
        {"probe_symbol": "USDJPY"},
        {"ensemble_member_id": ""},
    ],
)
def test_query_clock_policy_type_and_bound_refusals(changes):
    with pytest.raises((ValueError, TypeError)):
        _request(**changes)


def test_architecture_uses_only_canonical_owners_and_no_retired_calendar_api():
    import histdatacom.synthetic.traders as package

    root = Path(package.__file__).parent
    allowed = {
        "histdatacom.datasets.catalog",
        "histdatacom.datasets.contracts",
        "histdatacom.market_context.economic_calendar",
        "histdatacom.market_context.positioning",
        "histdatacom.synthetic.activity",
        "histdatacom.synthetic.bar_features",
        "histdatacom.synthetic.contracts",
        "histdatacom.synthetic.cross_currency",
        "histdatacom.synthetic.information",
        "histdatacom.synthetic.persistence",
        "histdatacom.synthetic.triangle_bar_features",
    }
    forbidden = {
        "TradingEconomicsCalendarAdapterV1",
        "EconomicCalendarFetchProfileV1",
        "build_live_economic_calendar_corpus",
        "query_context_corpus",
    }
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("histdatacom."):
                    assert node.module in allowed or node.module.startswith(
                        "histdatacom.synthetic.traders."
                    )
                    assert not {n.name for n in node.names} & forbidden
                    assert all(not n.name.startswith("_") for n in node.names)
            if isinstance(node, ast.Import):
                assert all(
                    not n.name.startswith("histdatacom.") for n in node.names
                )
    assert (root / "py.typed").is_file()
